//! M2 收尾集成测试：`idle_timeout` 后台 ticker 与 `terminal.prune`。
//!
//! Windows 用 ConPTY + `cmd.exe`，Unix 用 `openpty` + `/bin/sh`；空闲超时取
//! 1s、ticker 周期取 25ms：既覆盖后台并发路径，又给“创建→attach→采集进程树”
//! 留出足够余量，避免慢机器上先超时后断言导致的 flaky。

#![cfg(any(windows, unix))]

mod common;

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use common::{
    Client, TERMINATE_WAIT, TestService, call, call_collect, connect, create_terminal, handshake,
    next_event_matching,
};
use futures_util::StreamExt;
use tempfile::tempdir;
use termx_core::TerminalId;
use termx_protocol::{EventKind, LifecycleState, ProcNode, PruneResult, StatusResult, Stream};
use termx_service::ServiceConfig;
use tokio::time::{sleep, timeout};

/// 后台 ticker 周期（测试用）。
const TICK_INTERVAL: Duration = Duration::from_millis(25);
/// `idle_timeout` 数值（毫秒）；比 tick 大两个数量级，避免创建/采集阶段误触发。
const IDLE_MS: u64 = 2000;
/// 等待进程树出现后代的上限。
const TREE_WAIT: Duration = Duration::from_millis(1000);
/// detach 后确认“不再收到事件”的观察窗口。
const QUIET_WINDOW: Duration = Duration::from_millis(300);

/// 长运行命令创建参数；Windows 用 ping，Unix 用 sh 后台 sleep 形成父子进程树。
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

/// 长运行命令创建参数。
///
/// 用 `sleep 30 & wait` 而不是 `sleep 30`：后者可能被 shell `exec` 复用 PID，
/// 无法稳定覆盖“根进程 + 后代”的杀进程树语义（AC-8）。
#[cfg(unix)]
fn long_running_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "/bin/sh",
        "args": ["-c", "sleep 30 & wait"],
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 追加 `idle_timeout` 配置。
fn with_idle_timeout(
    mut params: serde_json::Value,
    action: &str,
    value_ms: u64,
) -> serde_json::Value {
    params["idle_timeout"] = serde_json::json!({
        "value": value_ms,
        "unit": "ms",
        "action": action,
    });
    params
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

/// 从 `status.tree` 收集根进程及其全部后代 PID。
///
/// 按 `ppid` 链做闭包，避免把 ToolHelp/`/proc` 快照里父 PID 复用的无关进程
/// 误记为后代。
fn collect_tree_pids(root: u32, tree: &[ProcNode]) -> Vec<u32> {
    let mut pids = BTreeSet::from([root]);
    loop {
        let added: Vec<u32> = tree
            .iter()
            .filter(|node| pids.contains(&node.ppid) && !pids.contains(&node.pid))
            .map(|node| node.pid)
            .collect();
        if added.is_empty() {
            break;
        }
        pids.extend(added);
    }
    pids.into_iter().collect()
}

/// 轮询 `terminal.status`，直到进程树至少收集到 `min` 个 PID（含根）。
///
/// `terminal.status` 不刷新 `last_activity`，因此这里不会干扰空闲计时。
async fn wait_tree_pids(client: &mut Client, terminal_id: &str, min: usize) -> Vec<u32> {
    let deadline = Instant::now() + TREE_WAIT;
    loop {
        let snapshot = status(client, "c-status-tree", terminal_id).await;
        let root = snapshot.snapshot.pid.expect("运行中应有 pid");
        let pids = collect_tree_pids(root, &snapshot.snapshot.tree);
        if pids.len() >= min {
            return pids;
        }
        assert!(
            Instant::now() < deadline,
            "进程树未达到 {min} 个 PID: {pids:?}"
        );
        sleep(Duration::from_millis(25)).await;
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

/// 以短 tick 启动服务。
async fn start_short_tick(config_dir: &std::path::Path) -> TestService {
    TestService::start_with_config(
        ServiceConfig::new(config_dir.to_path_buf()).with_idle_tick_interval(TICK_INTERVAL),
    )
    .await
}

/// 读取 `terminal.status` 快照。
async fn status(client: &mut Client, id: &str, terminal_id: &str) -> StatusResult {
    let response = call(
        client,
        id,
        "terminal.status",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(response.ok, "terminal.status 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 status result"))
        .expect("StatusResult 解析失败")
}

/// detach：收到 `detached{idle_timeout}`，进程仍存活，且该客户端不再收到后续事件。
#[tokio::test]
async fn idle_timeout_detach_keeps_terminal_running() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = start_short_tick(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "c-create-detach",
        with_idle_timeout(
            long_running_params("ses-idle-detach", "detach"),
            "detach",
            IDLE_MS,
        ),
    )
    .await;
    let terminal_id = created.terminal_id.clone();

    let (attach, _) = call_collect(
        &mut client,
        "c-attach-detach",
        "terminal.attach",
        serde_json::json!({ "terminal_id": terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    // 超时前记录根进程（status 不刷新 last_activity）。
    let pids = wait_tree_pids(&mut client, &terminal_id, 1).await;
    let root = pids[0];

    let detached = next_event_matching(&mut client, |event| {
        event.terminal_id == terminal_id
            && matches!(
                &event.kind,
                EventKind::Detached { reason, .. } if reason == "idle_timeout"
            )
    })
    .await;
    assert!(matches!(
        detached.kind,
        EventKind::Detached { ref reason, .. } if reason == "idle_timeout"
    ));
    assert!(pid_alive(root), "detach 后进程应继续存活: {root}");

    let snapshot = status(&mut client, "c-status-detach", &terminal_id).await;
    assert_eq!(
        snapshot.snapshot.state,
        LifecycleState::Running,
        "detach 动作不应终止终端"
    );

    // detach 已清订阅：直接向 registry 注入事件，客户端不应再收到该终端事件。
    let terminal = TerminalId::new(terminal_id.clone());
    {
        let mut registry = service.registry.lock().expect("registry 锁被污染");
        registry
            .report_event(
                &terminal,
                EventKind::Output {
                    data: "cG9zdC1kZXRhY2g=".to_string(),
                    stream: Stream::Pty,
                },
            )
            .expect("注入事件应成功");
    }
    let quiet = timeout(QUIET_WINDOW, client.next()).await;
    assert!(
        quiet.is_err(),
        "detach 后不应再收到该终端事件，却收到: {quiet:?}"
    );

    // 清理：显式终止后进程退出。
    let terminate = call(
        &mut client,
        "c-terminate-detach",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(terminate.ok, "清理 terminate 应成功: {terminate:?}");
    wait_pids_gone(&pids).await;
}

/// terminate：收到 `interrupted{source:timeout}`，根进程与全部后代都消失。
#[tokio::test]
async fn idle_timeout_terminate_kills_process_tree() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = start_short_tick(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "c-create-terminate",
        with_idle_timeout(
            long_running_params("ses-idle-terminate", "terminate"),
            "terminate",
            IDLE_MS,
        ),
    )
    .await;
    let terminal_id = created.terminal_id.clone();

    let (attach, _) = call_collect(
        &mut client,
        "c-attach-terminate",
        "terminal.attach",
        serde_json::json!({ "terminal_id": terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    // 超时前从 tree 收集根 + 后代；至少 2 个 PID 才真正覆盖 AC-8 杀进程树。
    let pids = wait_tree_pids(&mut client, &terminal_id, 2).await;

    let interrupted = next_event_matching(&mut client, |event| {
        event.terminal_id == terminal_id
            && matches!(
                &event.kind,
                EventKind::Interrupted { source, .. } if source == "timeout"
            )
    })
    .await;
    assert!(matches!(
        interrupted.kind,
        EventKind::Interrupted {
            ref signal,
            ref source
        } if signal == "SIGTERM" && source == "timeout"
    ));

    wait_pids_gone(&pids).await;
    let after = status(&mut client, "c-status-after", &terminal_id).await;
    assert_eq!(after.snapshot.state, LifecycleState::Interrupted);
}

/// 活跃终端：持续 `terminal.send` 续期，不应被判定为空闲超时。
#[tokio::test]
async fn active_terminal_is_not_pruned_while_active() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = start_short_tick(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "c-create-active",
        with_idle_timeout(
            long_running_params("ses-idle-active", "active"),
            "terminate",
            IDLE_MS,
        ),
    )
    .await;
    let terminal_id = created.terminal_id.clone();

    let (attach, _) = call_collect(
        &mut client,
        "c-attach-active",
        "terminal.attach",
        serde_json::json!({ "terminal_id": terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    let claim = call(
        &mut client,
        "c-claim-active",
        "terminal.claim",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(claim.ok, "claim 应成功: {claim:?}");

    // 每 200ms 写一次，累计 2.4s > IDLE_MS；若未续期会被误判超时。
    for index in 0..12 {
        let send = call(
            &mut client,
            &format!("c-send-active-{index}"),
            "terminal.send",
            serde_json::json!({ "terminal_id": terminal_id, "text": "keep-alive" }),
        )
        .await;
        assert!(send.ok, "send 应成功: {send:?}");
        sleep(Duration::from_millis(200)).await;
    }

    let snapshot = status(&mut client, "c-status-active", &terminal_id).await;
    assert_eq!(
        snapshot.snapshot.state,
        LifecycleState::Running,
        "持续 send 的终端不应被空闲超时终止"
    );

    // 清理：终止仍运行的终端。
    let terminate = call(
        &mut client,
        "c-terminate-active",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(terminate.ok, "清理 terminate 应成功: {terminate:?}");
}

/// prune：只删除终态终端并返回被删 ID；运行中终端保留。
#[tokio::test]
async fn prune_removes_terminal_terminals_only() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = start_short_tick(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let terminated = create_terminal(
        &mut client,
        "c-create-prune-old",
        long_running_params("ses-prune", "old"),
    )
    .await;
    let running = create_terminal(
        &mut client,
        "c-create-prune-live",
        long_running_params("ses-prune", "live"),
    )
    .await;

    let terminate = call(
        &mut client,
        "c-terminate-prune-old",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": terminated.terminal_id }),
    )
    .await;
    assert!(terminate.ok, "terminate 应成功: {terminate:?}");

    let (prune, _) = call_collect(
        &mut client,
        "c-prune",
        "terminal.prune",
        serde_json::json!({}),
    )
    .await;
    assert!(prune.ok, "terminal.prune 应成功: {prune:?}");
    let result: PruneResult = serde_json::from_value(prune.result.expect("缺少 prune result"))
        .expect("PruneResult 解析失败");
    assert_eq!(result.removed, vec![terminated.terminal_id.clone()]);
    assert_eq!(
        result.removed.len(),
        1,
        "removed 数量应等于被删除 terminal_id 列表长度"
    );

    let gone = call(
        &mut client,
        "c-status-pruned",
        "terminal.status",
        serde_json::json!({ "terminal_id": terminated.terminal_id }),
    )
    .await;
    assert!(!gone.ok, "已清理终端应 not_found: {gone:?}");

    let live = status(&mut client, "c-status-live", &running.terminal_id).await;
    assert_eq!(live.snapshot.state, LifecycleState::Running);

    // 清理：终止仍运行的终端。
    let terminate = call(
        &mut client,
        "c-terminate-prune-live",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": running.terminal_id }),
    )
    .await;
    assert!(terminate.ok, "清理 terminate 应成功: {terminate:?}");
}
