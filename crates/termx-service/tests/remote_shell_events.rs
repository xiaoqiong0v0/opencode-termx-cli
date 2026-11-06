//! T5 远端链路集成测试：service + sshd + helper + 远端 `termx-pwsh` 探针。
//!
//! 环境变量：`TERMX_TEST_SSH_HOST` / `TERMX_TEST_SSH_PORT` / `TERMX_TEST_SSH_USER` /
//! `TERMX_TEST_SSH_PASSWORD`、`TERMX_TEST_SSH_HELPER_BIN`（容器内 helper 路径）。
//! 远端程序默认 `/usr/local/bin/termx-pwsh`，可用 `TERMX_TEST_REMOTE_PROGRAM` 覆盖。
//! 任一必需变量缺失时跳过而非失败。
//!
//! 密码/token 只用于请求参数与远端进程内存，不写入断言消息或日志。

mod common;

use std::{env, time::Duration};

use common::{Client, TestService};
use futures_util::{SinkExt, StreamExt};
use tempfile::tempdir;
use termx_protocol::{Response, TranscriptResult, TranscriptSource, TranscriptStatus, from_json};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

/// 建连 + helper 上传 + 握手的等待上限。
const CREATE_WAIT: Duration = Duration::from_secs(90);
/// transcript 轮询次数。
const POLL_ATTEMPTS: usize = 150;
/// transcript 轮询间隔。
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// 默认远端探针路径。
const DEFAULT_REMOTE_PROGRAM: &str = "/usr/local/bin/termx-pwsh";

/// 测试环境；缺失任一必需项时返回 `None`。
struct RemoteEnv {
    /// SSH 主机。
    host: String,
    /// SSH 端口。
    port: u16,
    /// SSH 用户。
    user: String,
    /// SSH 密码（不打印）。
    password: String,
    /// 本地 helper 二进制（容器内由 cargo 构建）。
    helper_bin: String,
    /// 远端探针路径。
    program: String,
}

/// 读取环境变量；缺失时给出跳过提示。
fn remote_env() -> Option<RemoteEnv> {
    let host = env::var("TERMX_TEST_SSH_HOST").ok()?;
    let port = env::var("TERMX_TEST_SSH_PORT").ok()?.parse::<u16>().ok()?;
    let user = env::var("TERMX_TEST_SSH_USER").ok()?;
    let password = env::var("TERMX_TEST_SSH_PASSWORD").ok()?;
    let helper_bin = match env::var("TERMX_TEST_SSH_HELPER_BIN") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!("跳过远端 shell 事件测试：未设置 TERMX_TEST_SSH_HELPER_BIN");
            return None;
        }
    };
    let program = env::var("TERMX_TEST_REMOTE_PROGRAM")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_REMOTE_PROGRAM.to_string());
    Some(RemoteEnv {
        host,
        port,
        user,
        password,
        helper_bin,
        program,
    })
}

/// 发送请求并等待同 id 响应；慢操作（SSH 建连）使用独立超时，忽略中途事件。
async fn call_long(
    client: &mut Client,
    id: &str,
    method: &str,
    params: serde_json::Value,
    wait: Duration,
) -> Response {
    let request = serde_json::json!({ "id": id, "method": method, "params": params }).to_string();
    client
        .send(Message::Text(request.into()))
        .await
        .expect("发送请求失败");
    loop {
        let message = timeout(wait, client.next())
            .await
            .expect("等待响应超时")
            .expect("连接已关闭")
            .expect("读取响应失败");
        let value: serde_json::Value =
            from_json(message.to_text().expect("响应不是文本帧")).expect("响应 JSON 非法");
        if value.get("id").is_none() {
            continue;
        }
        let response: Response = serde_json::from_value(value).expect("Response 解析失败");
        if response.id == id {
            return response;
        }
        panic!("收到非预期响应: {}", response.id);
    }
}

/// 读取 transcript。
async fn transcript(client: &mut Client, id: &str, terminal_id: &str) -> TranscriptResult {
    let response = common::call(
        client,
        id,
        "terminal.transcript",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(response.ok, "terminal.transcript 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 transcript result"))
        .expect("TranscriptResult 解析失败")
}

/// 轮询 transcript，返回指定命令结束后的记录。
async fn wait_for_command(
    client: &mut Client,
    terminal_id: &str,
    expected_command: &str,
) -> termx_protocol::TranscriptCommand {
    for _ in 0..POLL_ATTEMPTS {
        let current = transcript(client, "t5-transcript", terminal_id).await;
        if let Some(command) = current
            .commands
            .iter()
            .find(|command| command.command == expected_command && command.exit_code.is_some())
        {
            return command.clone();
        }
        sleep(POLL_INTERVAL).await;
    }
    panic!("超时未收到 shell integration 退出码");
}

#[tokio::test]
async fn remote_shell_events_finish_transcript_with_exit_code() {
    let Some(env) = remote_env() else {
        eprintln!("跳过远端 shell 事件测试：未设置 TERMX_TEST_SSH_* 必需环境变量");
        return;
    };
    let command_text = env::var("TERMX_TEST_REMOTE_COMMAND")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "run".to_string());
    let expected_output = env::var("TERMX_TEST_REMOTE_OUTPUT")
        .ok()
        .filter(|value| !value.trim().is_empty());

    let dir = tempdir().expect("临时目录");
    let mut service = TestService::start(dir.path()).await;
    let mut client = common::connect(&service).await;
    common::handshake(&mut client, &service.token).await;

    let params = serde_json::json!({
        "mode": "pty",
        "cmd": env.program,
        "args": [],
        "root_session_id": "ses-t5",
        "name": "remote-events",
        // 本次请求显式确认远端注入（上传 helper 需要授权）。
        "allow_injection": true,
        "ssh": {
            "host": env.host,
            "port": env.port,
            "user": env.user,
            "auth": { "type": "password", "password": env.password },
            "helper_local_path": env.helper_bin,
            "upload_helper": true,
            "accept_any_host_key": true
        }
    });
    let response = call_long(
        &mut client,
        "t5-create",
        "terminal.create",
        params,
        CREATE_WAIT,
    )
    .await;
    assert!(response.ok, "terminal.create 应成功: {response:?}");
    let created: termx_protocol::CreateResult =
        serde_json::from_value(response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let claimed = common::call(
        &mut client,
        "t5-claim",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(claimed.ok, "claim 应成功: {claimed:?}");

    let sent = common::call(
        &mut client,
        "t5-send",
        "terminal.send",
        serde_json::json!({
            "terminal_id": created.terminal_id,
            "text": command_text.clone(),
            "enter": true,
        }),
    )
    .await;
    assert!(sent.ok, "send 应成功: {sent:?}");

    let command = wait_for_command(&mut client, &created.terminal_id, &command_text).await;
    assert_eq!(command.status, TranscriptStatus::Done);
    assert_eq!(command.exit_code, Some(7), "应收到远端退出码 7");
    assert_eq!(
        command.source,
        Some(TranscriptSource::ShellIntegration),
        "退出码应来自带外 shell 事件"
    );
    assert!(!command.best_effort);

    // 输出断言可选；真实 pwsh 可能没有探针的固定输出。
    if let Some(expected) = expected_output.as_deref() {
        let mut saw_output = false;
        for _ in 0..10 {
            let latest = transcript(&mut client, "t5-output", &created.terminal_id).await;
            saw_output = latest
                .commands
                .iter()
                .find(|item| item.command == command_text)
                .is_some_and(|item| item.output.contains(expected));
            if saw_output {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
        assert!(saw_output, "transcript 输出应包含 {expected:?}");
    }

    service.shutdown().await;
}
