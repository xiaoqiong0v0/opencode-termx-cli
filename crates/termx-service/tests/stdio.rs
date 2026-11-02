//! M3a 集成测试：`terminal.create{mode:"stdio"}` 走真实管道后端。
//!
//! 验证点：output 事件带正确的 `stream`（stdout/stderr）、`exited` 终态、
//! attach 回放，以及 claim 后 `terminal.send` 可写入子进程 stdin。

#![cfg(any(windows, unix))]

mod common;

use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use tempfile::tempdir;
use termx_protocol::{ErrorCode, Event, EventKind, StatusResult, Stream};
use termx_service::ServiceConfig;

use common::{
    Client, TestService, call, call_collect, connect, create_terminal, handshake, next_event,
};

/// 平台相关命令：stdout/stderr 各写一个标记后以 5 退出。
fn echo_params() -> serde_json::Value {
    #[cfg(windows)]
    {
        serde_json::json!({
            "mode": "stdio",
            "cmd": "cmd.exe",
            "args": ["/C", "echo out_hi & echo err_hi 1>&2 & exit 5"],
            "root_session_id": "ses-stdio",
            "name": "echo",
        })
    }
    #[cfg(unix)]
    {
        serde_json::json!({
            "mode": "stdio",
            "cmd": "/bin/sh",
            "args": ["-c", "echo out_hi; echo err_hi 1>&2; exit 5"],
            "root_session_id": "ses-stdio",
            "name": "echo",
        })
    }
}

/// 平台相关交互命令：读一行 stdin 并回显 `got:<行>`。
///
/// Windows 必须开启延迟展开（`/V:ON` + `!X!`）：`cmd /C` 会在执行 `set /p`
/// 前展开整行的 `%X%`，普通写法只能得到空值。
fn interactive_params() -> serde_json::Value {
    #[cfg(windows)]
    {
        serde_json::json!({
            "mode": "stdio",
            "cmd": "cmd.exe",
            "args": ["/V:ON", "/C", "set /p X=& echo got:!X!"],
            "root_session_id": "ses-stdio",
            "name": "interactive",
        })
    }
    #[cfg(unix)]
    {
        serde_json::json!({
            "mode": "stdio",
            "cmd": "/bin/sh",
            "args": ["-c", "read line; echo got:$line"],
            "root_session_id": "ses-stdio",
            "name": "interactive",
        })
    }
}

/// 收集输出直到终态，返回 `(stream, bytes)` 列表与退出码。
async fn collect_until_exit(
    client: &mut Client,
    mut pending: Vec<Event>,
    output: &mut Vec<(Stream, Vec<u8>)>,
) -> i32 {
    loop {
        let event = if pending.is_empty() {
            next_event(client).await
        } else {
            pending.remove(0)
        };
        match event.kind {
            EventKind::Output { data, stream } => {
                output.push((
                    stream,
                    STANDARD.decode(data).expect("output 不是合法 base64"),
                ));
            }
            EventKind::Exited { exit_code } => return exit_code,
            EventKind::Interrupted { signal, .. } => panic!("进程被信号终止: {signal}"),
            _ => {}
        }
    }
}

/// 把指定流的输出拼成文本。
fn stream_text(output: &[(Stream, Vec<u8>)], stream: Stream) -> String {
    let bytes: Vec<u8> = output
        .iter()
        .filter(|(source, _)| *source == stream)
        .flat_map(|(_, data)| data.iter().copied())
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[tokio::test]
async fn stdio_create_streams_stdout_and_stderr_and_exits() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(&mut client, "c-create", echo_params()).await;

    // cursor=0 回放 started/output/exited，避免 create→attach 空窗。
    let (attached, replay) = call_collect(
        &mut client,
        "c-attach",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attached.ok, "terminal.attach 应成功: {attached:?}");

    let mut output = Vec::new();
    let exit_code = collect_until_exit(&mut client, replay, &mut output).await;
    assert_eq!(exit_code, 5, "退出码应为 5");

    let stdout = stream_text(&output, Stream::Stdout);
    let stderr = stream_text(&output, Stream::Stderr);
    assert!(stdout.contains("out_hi"), "stdout 缺少标记: {stdout:?}");
    assert!(stderr.contains("err_hi"), "stderr 缺少标记: {stderr:?}");
    assert!(
        !stdout.contains("err_hi"),
        "stdout 不应混入 stderr: {stdout:?}"
    );
    assert!(
        !stderr.contains("out_hi"),
        "stderr 不应混入 stdout: {stderr:?}"
    );
}

#[tokio::test]
async fn stdio_claim_then_send_reaches_stdin() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(&mut client, "c-create", interactive_params()).await;

    let (attached, replay) = call_collect(
        &mut client,
        "c-attach",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attached.ok, "terminal.attach 应成功: {attached:?}");

    let claim = call(
        &mut client,
        "c-claim",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(claim.ok, "terminal.claim 应成功: {claim:?}");

    // stdio 的 enter=true 必须追加 \n（不是 pty 的 \r）。
    let (sent, send_events) = call_collect(
        &mut client,
        "c-send",
        "terminal.send",
        serde_json::json!({ "terminal_id": created.terminal_id, "text": "abc", "enter": true }),
    )
    .await;
    assert!(sent.ok, "terminal.send 应成功: {sent:?}");

    let mut pending = replay;
    pending.extend(send_events);
    let mut output = Vec::new();
    let exit_code = collect_until_exit(&mut client, pending, &mut output).await;
    assert_eq!(exit_code, 0, "交互脚本应正常退出");

    let stdout = stream_text(&output, Stream::Stdout);
    assert!(stdout.contains("got:abc"), "stdout 缺少回显: {stdout:?}");
}

/// 平台相关的长驻 stdio 命令。
fn long_running_params() -> serde_json::Value {
    #[cfg(windows)]
    {
        serde_json::json!({
            "mode": "stdio",
            "cmd": "cmd.exe",
            "args": ["/C", "ping -n 30 127.0.0.1 > nul"],
            "root_session_id": "ses-stdio",
            "name": "long",
        })
    }
    #[cfg(unix)]
    {
        serde_json::json!({
            "mode": "stdio",
            "cmd": "/bin/sh",
            "args": ["-c", "sleep 30"],
            "root_session_id": "ses-stdio",
            "name": "long",
        })
    }
}

/// 按 pid 判断进程是否仍存活（僵尸不算存活）。
fn pid_alive(pid: u32) -> bool {
    #[cfg(windows)]
    {
        let filter = format!("PID eq {pid}");
        let Ok(output) = std::process::Command::new("tasklist")
            .args(["/FI", &filter, "/NH", "/FO", "CSV"])
            .output()
        else {
            return false;
        };
        String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
    }
    #[cfg(unix)]
    {
        let Ok(raw) = std::fs::read(format!("/proc/{pid}/stat")) else {
            return false;
        };
        let Some(close) = raw.iter().rposition(|&byte| byte == b')') else {
            return false;
        };
        let Ok(rest) = std::str::from_utf8(&raw[close + 1..]) else {
            return false;
        };
        rest.split_whitespace().next() != Some("Z")
    }
}

/// 子进程不读 stdin 时，连续 `terminal.send` 必须最终返回
/// `limit_exceeded`（连接不被阻塞，响应仍按请求 `id` 返回）。
#[tokio::test]
async fn send_queue_full_returns_limit_exceeded_without_blocking() {
    let dir = tempdir().expect("创建临时目录失败");
    let mut service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf()).with_stdio_write_queue_capacity(1),
    )
    .await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(&mut client, "c-full-create", long_running_params()).await;
    let claim = call(
        &mut client,
        "c-full-claim",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(claim.ok, "terminal.claim 应成功: {claim:?}");

    // 单次 256 KiB（base64 后约 341 KiB，低于 1 MiB 帧上限）。
    let payload = STANDARD.encode(vec![b'x'; 256 * 1024]);
    let outcome = tokio::time::timeout(Duration::from_secs(20), async {
        for index in 0..128 {
            let id = format!("c-full-send-{index}");
            let response = call(
                &mut client,
                &id,
                "terminal.send",
                serde_json::json!({ "terminal_id": created.terminal_id, "data": payload }),
            )
            .await;
            assert_eq!(response.id, id, "响应 id 应回显请求 id");
            if let Some(error) = &response.error {
                assert_eq!(error.code, ErrorCode::LimitExceeded);
                assert!(error.retryable, "limit_exceeded 应可重试");
                return true;
            }
            assert!(response.ok, "未满时 send 应成功: {response:?}");
        }
        false
    })
    .await;

    assert!(
        outcome.expect("写队列满用例超时（疑似阻塞）"),
        "连续写入后应返回 limit_exceeded"
    );
    service.shutdown().await;
}

#[tokio::test]
async fn service_shutdown_terminates_stdio_children() {
    let dir = tempdir().expect("创建临时目录失败");
    let mut service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(&mut client, "c-long", long_running_params()).await;
    let (status, _) = call_collect(
        &mut client,
        "c-long-status",
        "terminal.status",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(status.ok, "status 应成功: {status:?}");
    let status: StatusResult = serde_json::from_value(status.result.expect("缺少 status result"))
        .expect("StatusResult 解析失败");
    let pid = status.snapshot.pid.expect("stdio 后端应有 pid");
    assert!(pid_alive(pid), "启动后进程应存活");

    service.shutdown().await;
    drop(client);

    tokio::time::timeout(Duration::from_secs(10), async {
        while pid_alive(pid) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("service 退出后 stdio 子进程仍存活");
}
