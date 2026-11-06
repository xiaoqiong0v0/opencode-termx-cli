//! `terminal.transcript` 集成测试：命令记录、时间/状态/输出、鉴权隔离与管理员例外。
#![cfg(any(windows, unix))]

mod common;

use std::time::Duration;

use common::{Client, TestService, call, connect, create_terminal, handshake, handshake_root};
use tempfile::tempdir;
use termx_protocol::{ErrorCode, TranscriptResult, TranscriptSource, TranscriptStatus};
use tokio::time::sleep;

/// 长运行 shell 参数。
#[cfg(windows)]
fn shell_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "cmd.exe",
        "args": [],
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 长运行 shell 参数。
#[cfg(unix)]
fn shell_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "/bin/sh",
        "args": [],
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 支持 OSC marker 的真实 shell；不可用时返回 `None`，测试跳过。
#[cfg(windows)]
fn marker_shell() -> Option<(String, Vec<String>)> {
    for name in ["pwsh.exe", "powershell.exe"] {
        let available = std::process::Command::new(name)
            .args(["-NoLogo", "-NoProfile", "-Command", "exit 0"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if available {
            return Some((
                name.to_string(),
                vec!["-NoLogo".to_string(), "-NoProfile".to_string()],
            ));
        }
    }
    None
}

/// 支持 OSC marker 的真实 shell；不可用时返回 `None`，测试跳过。
#[cfg(unix)]
fn marker_shell() -> Option<(String, Vec<String>)> {
    std::path::Path::new("/bin/sh")
        .exists()
        .then(|| ("/bin/sh".to_string(), Vec::new()))
}

/// 显式输出 in-band OSC D 且返回 7 的单行命令。
///
/// 该测试覆盖通用 shell 的 stdout OSC 解析；termx-pwsh 已改用带外通道，
/// 因此不能依赖宿主自动输出 marker。
#[cfg(windows)]
const MARKER_EXIT_COMMAND: &str = "cmd /c exit 7; Write-Host \"$([char]27)]133;D;7$([char]7)\"";
/// 显式输出 in-band OSC D 且返回 7 的单行命令。
#[cfg(unix)]
const MARKER_EXIT_COMMAND: &str = "printf '\\033]133;D;7\\007'; (exit 7)";

/// 调用 transcript 并解析结果。
async fn transcript(client: &mut Client, id: &str, params: serde_json::Value) -> TranscriptResult {
    let response = call(client, id, "terminal.transcript", params).await;
    assert!(response.ok, "terminal.transcript 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 transcript result"))
        .expect("TranscriptResult 解析失败")
}

/// 取得输入 lease。
async fn claim(client: &mut Client, id: &str, terminal_id: &str) {
    let response = call(
        client,
        id,
        "terminal.claim",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(response.ok, "claim 应成功: {response:?}");
}

#[tokio::test]
async fn transcript_records_command_with_time_status_and_clean_output() {
    let dir = tempdir().expect("创建临时目录失败");
    let mut service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(&mut client, "c-create", shell_params("ses-a", "t1")).await;
    claim(&mut client, "c-claim", &created.terminal_id).await;

    let sent = call(
        &mut client,
        "c-send",
        "terminal.send",
        serde_json::json!({
            "terminal_id": created.terminal_id,
            "text": "echo transcript_marker",
            "enter": true,
        }),
    )
    .await;
    assert!(sent.ok, "send 应成功: {sent:?}");

    // 输出异步：轮询直到清洗后的 output 出现 marker。
    let mut result = None;
    for _ in 0..40 {
        let current = transcript(
            &mut client,
            "c-transcript",
            serde_json::json!({ "terminal_id": created.terminal_id }),
        )
        .await;
        let matched = current.commands.last().is_some_and(|cmd| {
            cmd.command == "echo transcript_marker" && cmd.output.contains("transcript_marker")
        });
        if matched {
            result = Some(current);
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let result = result.expect("应记录命令与清洗后的输出");
    let command = result.commands.last().expect("应有命令记录");
    assert!(
        matches!(
            command.status,
            TranscriptStatus::Running | TranscriptStatus::Done
        ),
        "命令状态应为 running/done，实际 {:?}",
        command.status
    );
    assert!(!command.started_at.is_empty());
    assert!(
        !command.output.contains("echo transcript_marker"),
        "输出不应包含命令回显: {:?}",
        command.output
    );
    assert!(result.revision > 0);

    // limit=1 只返回最近一条。
    let latest = transcript(
        &mut client,
        "c-limit",
        serde_json::json!({ "terminal_id": created.terminal_id, "limit": 1 }),
    )
    .await;
    assert_eq!(latest.commands.len(), 1);
    assert_eq!(latest.commands[0].command, "echo transcript_marker");

    let terminated = call(
        &mut client,
        "c-terminate",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(terminated.ok, "terminate 应成功: {terminated:?}");
    service.shutdown().await;
}

#[tokio::test]
async fn transcript_records_shell_integration_exit_code() {
    let Some((cmd, args)) = marker_shell() else {
        return;
    };

    let dir = tempdir().expect("创建临时目录失败");
    let mut service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "c-create",
        serde_json::json!({
            "mode": "pty",
            "cmd": cmd,
            "args": args,
            "root_session_id": "ses-marker",
            "name": "m1",
            "cols": 120,
            "rows": 30,
        }),
    )
    .await;
    claim(&mut client, "c-claim", &created.terminal_id).await;

    let sent = call(
        &mut client,
        "c-send",
        "terminal.send",
        serde_json::json!({
            "terminal_id": created.terminal_id,
            "text": MARKER_EXIT_COMMAND,
            "enter": true,
        }),
    )
    .await;
    assert!(sent.ok, "send 应成功: {sent:?}");

    let mut matched = None;
    for _ in 0..100 {
        let current = transcript(
            &mut client,
            "c-transcript",
            serde_json::json!({ "terminal_id": created.terminal_id, "limit": 1 }),
        )
        .await;
        if let Some(command) = current.commands.last() {
            if command.command == MARKER_EXIT_COMMAND && command.exit_code.is_some() {
                matched = Some(command.clone());
                break;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }

    let command = matched.expect("应在超时前收到 shell integration 退出码");
    assert_eq!(command.status, TranscriptStatus::Done);
    assert_eq!(command.exit_code, Some(7));
    assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
    assert!(!command.best_effort);
    assert_eq!(
        command.command, MARKER_EXIT_COMMAND,
        "transcript 应保留原始命令"
    );

    let terminated = call(
        &mut client,
        "c-terminate",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(terminated.ok);
    service.shutdown().await;
}

#[tokio::test]
async fn transcript_isolated_by_root_and_admin_exception() {
    let dir = tempdir().expect("创建临时目录失败");
    let mut service = TestService::start(dir.path()).await;

    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;
    let created = create_terminal(&mut admin, "c-create", shell_params("ses-a", "t1")).await;

    // 其它会话：无权限，且不泄露终端内容。
    let mut other = connect(&service).await;
    handshake_root(&mut other, &service.token, "ses-b").await;
    let denied = call(
        &mut other,
        "c-other",
        "terminal.transcript",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(!denied.ok, "跨会话读取应失败");
    assert_eq!(
        denied.error.expect("应有错误").code,
        ErrorCode::PermissionDenied
    );

    // 所属会话：可读。
    let mut owner = connect(&service).await;
    handshake_root(&mut owner, &service.token, "ses-a").await;
    let allowed = transcript(
        &mut owner,
        "c-owner",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(allowed.commands.is_empty());

    // 管理员：可读任意会话。
    let admin_view = transcript(
        &mut admin,
        "c-admin",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(admin_view.commands.is_empty());

    let terminated = call(
        &mut admin,
        "c-terminate",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(terminated.ok);
    service.shutdown().await;
}
