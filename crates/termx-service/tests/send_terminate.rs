//! M2c2 集成测试：`terminal.send` 的 lease 门禁、`resize`、`signal` 与
//! `terminal.terminate` 的真实后端行为及无孤儿清理。
//!
//! Windows 用 ConPTY + `cmd.exe`，Unix 用 `openpty` + `/bin/sh`。

#![cfg(any(windows, unix))]

mod common;

use std::time::{Duration, Instant};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use common::{
    Client, TERMINATE_WAIT, TestService, call, call_collect, connect, create_terminal, handshake,
    next_event, next_event_matching,
};
use tempfile::tempdir;
use termx_protocol::{CreateResult, Event, EventKind, LifecycleState, StatusResult};
use tokio::time::sleep;

/// 交互 shell 创建参数。
#[cfg(windows)]
fn shell_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "cmd.exe",
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 交互 shell 创建参数。
#[cfg(unix)]
fn shell_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "/bin/sh",
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 长运行命令创建参数：1 个 ping/sleep 不自行退出。
#[cfg(windows)]
fn long_running_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "cmd.exe",
        "args": ["/C", "ping -n 30 127.0.0.1 > nul"],
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 长运行命令创建参数：`sleep 30` 不自行退出。
#[cfg(unix)]
fn long_running_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "/bin/sh",
        "args": ["-c", "sleep 30"],
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 给创建参数加上 `force=true`。
fn with_force(mut params: serde_json::Value) -> serde_json::Value {
    params["force"] = serde_json::json!(true);
    params
}

/// 回显命令的文本与 `enter` 取值。
#[cfg(windows)]
fn echo_request() -> (&'static str, bool) {
    ("echo hi_termx", true)
}

/// 回显命令的文本与 `enter` 取值。
#[cfg(unix)]
fn echo_request() -> (&'static str, bool) {
    ("echo hi_termx\n", false)
}

/// 检查进程是否仍存活。
#[cfg(windows)]
fn pid_alive(pid: u32) -> bool {
    let filter = format!("PID eq {pid}");
    let Ok(output) = std::process::Command::new("tasklist")
        .args(["/FI", &filter, "/NH", "/FO", "CSV"])
        .output()
    else {
        return false;
    };
    String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
}

/// 检查进程是否仍存活。
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

/// 从事件流中累积输出，直到包含 `needle`。
async fn wait_output_contains(
    client: &mut Client,
    mut events: Vec<Event>,
    needle: &str,
) -> Vec<u8> {
    let mut output = Vec::new();
    loop {
        let event = if events.is_empty() {
            next_event(client).await
        } else {
            events.remove(0)
        };
        match event.kind {
            EventKind::Output { data, .. } => {
                output.extend(STANDARD.decode(data).expect("output 不是合法 base64"));
                if String::from_utf8_lossy(&output).contains(needle) {
                    return output;
                }
            }
            EventKind::Exited { exit_code } => {
                panic!(
                    "命令提前退出: {exit_code}，输出: {:?}",
                    String::from_utf8_lossy(&output)
                )
            }
            EventKind::Interrupted { signal, source } => {
                panic!("命令被中断: {signal}/{source}")
            }
            _ => {}
        }
    }
}

/// 轮询 status，直到根进程与至少一个子进程出现；返回最后一次快照。
async fn wait_for_tree(client: &mut Client, terminal_id: &str) -> StatusResult {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = call(
            client,
            "c-status",
            "terminal.status",
            serde_json::json!({ "terminal_id": terminal_id }),
        )
        .await;
        assert!(response.ok, "terminal.status 应成功: {response:?}");
        let status: StatusResult =
            serde_json::from_value(response.result.expect("缺少 status result"))
                .expect("StatusResult 解析失败");
        if status.snapshot.tree.len() >= 2 {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "5s 内未观察到子进程树: {:?}",
            status.snapshot.tree
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// 等待一组 pid 全部消失；超时 panic。
async fn wait_pids_gone(pids: &[u32]) {
    let deadline = Instant::now() + TERMINATE_WAIT;
    loop {
        let alive: Vec<u32> = pids.iter().copied().filter(|pid| pid_alive(*pid)).collect();
        if alive.is_empty() {
            return;
        }
        assert!(Instant::now() < deadline, "终止后仍有残留进程: {alive:?}");
        sleep(Duration::from_millis(100)).await;
    }
}

/// 未 claim / 非 owner 的 send 被拒绝；owner 可写；resize 生效；终态后拒绝写。
#[tokio::test]
async fn send_requires_lease_and_resize_applies() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut owner = connect(&service).await;
    handshake(&mut owner, &service.token).await;
    let created = create_terminal(&mut owner, "c-create", shell_params("ses-io", "shell")).await;
    let terminal_id = created.terminal_id.clone();

    let (attach, _) = call_collect(
        &mut owner,
        "c-attach",
        "terminal.attach",
        serde_json::json!({ "terminal_id": terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    // 未 claim：owner 自己也不能写。
    let response = call(
        &mut owner,
        "c-send-unclaimed",
        "terminal.send",
        serde_json::json!({ "terminal_id": terminal_id, "text": "x" }),
    )
    .await;
    assert_eq!(
        response.error.expect("缺少 error").code,
        termx_protocol::ErrorCode::PermissionDenied
    );

    // owner claim 后，另一条连接作为非 owner 被拒绝。
    let claim = call(
        &mut owner,
        "c-claim",
        "terminal.claim",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(claim.ok, "claim 应成功: {claim:?}");

    let mut other = connect(&service).await;
    handshake(&mut other, &service.token).await;
    let response = call(
        &mut other,
        "c-send-non-owner",
        "terminal.send",
        serde_json::json!({ "terminal_id": terminal_id, "text": "x" }),
    )
    .await;
    assert_eq!(
        response.error.expect("缺少 error").code,
        termx_protocol::ErrorCode::PermissionDenied
    );

    // owner 发送回显命令，输出应包含标记。
    let (text, enter) = echo_request();
    let (send, send_events) = call_collect(
        &mut owner,
        "c-send",
        "terminal.send",
        serde_json::json!({ "terminal_id": terminal_id, "text": text, "enter": enter }),
    )
    .await;
    assert!(send.ok, "owner send 应成功: {send:?}");
    wait_output_contains(&mut owner, send_events, "hi_termx").await;

    // resize 成功且快照反映新尺寸。
    let resize = call(
        &mut owner,
        "c-resize",
        "terminal.resize",
        serde_json::json!({ "terminal_id": terminal_id, "cols": 100, "rows": 40 }),
    )
    .await;
    assert!(resize.ok, "resize 应成功: {resize:?}");
    let status = call(
        &mut owner,
        "c-status-size",
        "terminal.status",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    let status: StatusResult = serde_json::from_value(status.result.expect("缺少 status result"))
        .expect("StatusResult 解析失败");
    assert_eq!((status.snapshot.cols, status.snapshot.rows), (100, 40));

    // 未知信号映射为 bad_request。
    let response = call(
        &mut owner,
        "c-signal-bad",
        "terminal.signal",
        serde_json::json!({ "terminal_id": terminal_id, "signal": "SIGBOGUS" }),
    )
    .await;
    assert_eq!(
        response.error.expect("缺少 error").code,
        termx_protocol::ErrorCode::BadRequest
    );

    // 终止后写操作返回 already_terminal。
    let terminate = call(
        &mut owner,
        "c-terminate",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(terminate.ok, "terminate 应成功: {terminate:?}");

    let response = call(
        &mut owner,
        "c-send-terminal",
        "terminal.send",
        serde_json::json!({ "terminal_id": terminal_id, "text": "x" }),
    )
    .await;
    assert_eq!(
        response.error.expect("缺少 error").code,
        termx_protocol::ErrorCode::AlreadyTerminal
    );
}

/// 终止长运行命令：收到 `interrupted{source:"terminate"}`，整棵树在 10s 内消失。
#[tokio::test]
async fn terminate_long_running_command_leaves_no_orphans() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "c-create",
        long_running_params("ses-term", "long"),
    )
    .await;
    let terminal_id = created.terminal_id.clone();

    let (attach, _) = call_collect(
        &mut client,
        "c-attach",
        "terminal.attach",
        serde_json::json!({ "terminal_id": terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    // 先记录运行中的进程树（含根进程），终止后逐个核对消失。
    let before = wait_for_tree(&mut client, &terminal_id).await;
    let mut pids: Vec<u32> = before.snapshot.tree.iter().map(|node| node.pid).collect();
    if let Some(pid) = before.snapshot.pid {
        pids.push(pid);
    }
    pids.sort_unstable();
    pids.dedup();
    assert!(pids.len() >= 2, "运行中应能看到根进程与子进程: {pids:?}");

    let (terminate, events) = call_collect(
        &mut client,
        "c-terminate",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(terminate.ok, "terminate 应成功: {terminate:?}");

    // 事件可能在响应前已收集，也可能仍在队列中。
    let interrupted = match events
        .into_iter()
        .find(|event| matches!(event.kind, EventKind::Interrupted { .. }))
    {
        Some(event) => event,
        None => {
            next_event_matching(&mut client, |event| {
                matches!(event.kind, EventKind::Interrupted { .. })
            })
            .await
        }
    };
    assert!(matches!(
        interrupted.kind,
        EventKind::Interrupted { ref source, .. } if source == "terminate"
    ));

    wait_pids_gone(&pids).await;

    let status = call(
        &mut client,
        "c-status-terminal",
        "terminal.status",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    let status: StatusResult = serde_json::from_value(status.result.expect("缺少 status result"))
        .expect("StatusResult 解析失败");
    assert_eq!(status.snapshot.state, LifecycleState::Interrupted);
    assert!(status.snapshot.tree.is_empty(), "终态进程树应为空");

    // 幂等：重复 terminate 仍返回 ok。
    let again = call(
        &mut client,
        "c-terminate-again",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(again.ok, "重复 terminate 应幂等成功: {again:?}");

    // 终态后 send 被拒绝。
    let response = call(
        &mut client,
        "c-send-terminal",
        "terminal.send",
        serde_json::json!({ "terminal_id": terminal_id, "text": "x" }),
    )
    .await;
    assert_eq!(
        response.error.expect("缺少 error").code,
        termx_protocol::ErrorCode::AlreadyTerminal
    );
}

/// force 替换同名长驻终端：旧终端收到 `interrupted{source:"terminate"}`，
/// 不能因为先终止后端而变成 `exited`。
#[tokio::test]
async fn force_replace_emits_interrupted_for_old_terminal() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let first = create_terminal(
        &mut client,
        "c-create-old",
        long_running_params("ses-force", "dup"),
    )
    .await;
    let (attach, attach_events) = call_collect(
        &mut client,
        "c-attach-old",
        "terminal.attach",
        serde_json::json!({ "terminal_id": first.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    let (replaced, replace_events) = call_collect(
        &mut client,
        "c-create-new",
        "terminal.create",
        with_force(long_running_params("ses-force", "dup")),
    )
    .await;
    assert!(replaced.ok, "force create 应成功: {replaced:?}");
    let second: CreateResult = serde_json::from_value(replaced.result.expect("缺少 create result"))
        .expect("CreateResult 解析失败");
    assert_ne!(first.terminal_id, second.terminal_id);

    let mut events = attach_events;
    events.extend(replace_events);
    let interrupted = match events.iter().find(|event| {
        event.terminal_id == first.terminal_id
            && matches!(event.kind, EventKind::Interrupted { .. })
    }) {
        Some(event) => event.clone(),
        None => {
            next_event_matching(&mut client, |event| {
                event.terminal_id == first.terminal_id
                    && matches!(event.kind, EventKind::Interrupted { .. })
            })
            .await
        }
    };
    assert!(matches!(
        interrupted.kind,
        EventKind::Interrupted { ref source, .. } if source == "terminate"
    ));
    assert!(
        !events.iter().any(|event| {
            event.terminal_id == first.terminal_id && matches!(event.kind, EventKind::Exited { .. })
        }),
        "旧终端不应先收到 exited: {events:?}"
    );

    // 旧终端已从索引摘除。
    let status = call(
        &mut client,
        "c-status-old",
        "terminal.status",
        serde_json::json!({ "terminal_id": first.terminal_id }),
    )
    .await;
    assert!(!status.ok, "旧终端应已不存在: {status:?}");
    assert_eq!(
        status.error.expect("缺少 error").code,
        termx_protocol::ErrorCode::NotFound
    );

    let _ = call(
        &mut client,
        "c-terminate-new",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": second.terminal_id }),
    )
    .await;
}

/// Windows：`terminal.signal("SIGTERM")` 终止 Job 后应落
/// `interrupted{signal:"SIGTERM", source:"signal"}`（而非 exited），且进程消失。
#[cfg(windows)]
#[tokio::test]
async fn windows_sigterm_maps_to_interrupted_source_signal() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "c-create-win-signal",
        long_running_params("ses-win-signal", "win-signal"),
    )
    .await;

    let (attach, _) = call_collect(
        &mut client,
        "c-attach-win-signal",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    // 记录根进程与子进程，信号终止后逐个核对消失。
    let before = wait_for_tree(&mut client, &created.terminal_id).await;
    let mut pids: Vec<u32> = before.snapshot.tree.iter().map(|node| node.pid).collect();
    if let Some(pid) = before.snapshot.pid {
        pids.push(pid);
    }
    pids.sort_unstable();
    pids.dedup();
    assert!(pids.len() >= 2, "运行中应能看到根进程与子进程: {pids:?}");

    let (signal, mut events) = call_collect(
        &mut client,
        "c-signal-win",
        "terminal.signal",
        serde_json::json!({ "terminal_id": created.terminal_id, "signal": "SIGTERM" }),
    )
    .await;
    assert!(signal.ok, "terminal.signal 应成功: {signal:?}");

    loop {
        let event = if events.is_empty() {
            next_event(&mut client).await
        } else {
            events.remove(0)
        };
        match event.kind {
            EventKind::Interrupted { signal, source } => {
                assert_eq!(signal, "SIGTERM");
                assert_eq!(source, "signal");
                break;
            }
            EventKind::Exited { exit_code } => {
                panic!("SIGTERM 终止不应映射为 exited: {exit_code}")
            }
            _ => {}
        }
    }

    wait_pids_gone(&pids).await;
}

/// Unix：进程被 SIGTERM 终止且无显式中断意图 → `interrupted{signal:"SIGTERM",
/// source:"signal"}`。
#[cfg(unix)]
#[tokio::test]
async fn signaled_process_maps_to_interrupted_source_signal() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "c-create-signaled",
        serde_json::json!({
            "mode": "pty",
            "cmd": "/bin/sh",
            "args": ["-c", "kill -TERM $$"],
            "root_session_id": "ses-signal",
            "name": "signaled",
        }),
    )
    .await;

    let (attach, mut events) = call_collect(
        &mut client,
        "c-attach-signaled",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    loop {
        let event = if events.is_empty() {
            next_event(&mut client).await
        } else {
            events.remove(0)
        };
        match event.kind {
            EventKind::Interrupted { signal, source } => {
                assert_eq!(signal, "SIGTERM");
                assert_eq!(source, "signal");
                return;
            }
            EventKind::Exited { exit_code } => {
                panic!("信号终止不应映射为 exited: {exit_code}")
            }
            _ => {}
        }
    }
}
