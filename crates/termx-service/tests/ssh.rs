//! M6b-1 集成测试：`terminal.create` 带 `ssh` 走真实 SSH（russh fallback）。
//!
//! 依赖 `TERMX_TEST_SSH_HOST/PORT/USER/PASSWORD`；任一缺失时跳过而非失败。
//! 密码只从环境读取，不写入断言消息。

mod common;

use std::{env, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use common::{Incoming, TestService};
use tempfile::tempdir;
use termx_protocol::{AttachResult, Event, EventKind, LifecycleState};
use tokio::time::timeout;

/// SSH 事件等待上限（建连 + 输出 + 退出）。
const SSH_WAIT: Duration = Duration::from_secs(30);

/// 读取 SSH 测试环境；缺失时返回 `None`。
struct SshEnv {
    /// 主机。
    host: String,
    /// 端口。
    port: u16,
    /// 用户。
    user: String,
    /// 密码（仅用于请求参数，不打印）。
    password: String,
}

/// 从环境变量读取测试配置。
fn ssh_env() -> Option<SshEnv> {
    let host = env::var("TERMX_TEST_SSH_HOST").ok()?;
    let port = env::var("TERMX_TEST_SSH_PORT").ok()?.parse::<u16>().ok()?;
    let user = env::var("TERMX_TEST_SSH_USER").ok()?;
    let password = env::var("TERMX_TEST_SSH_PASSWORD").ok()?;
    Some(SshEnv {
        host,
        port,
        user,
        password,
    })
}

/// 等待事件直到看到目标输出与退出码。
async fn wait_for_output_and_exit(
    client: &mut common::Client,
    terminal_id: &str,
    buffered: Vec<Event>,
) -> (Vec<u8>, Option<i32>) {
    let mut output = Vec::new();
    let mut exit = None;
    let mut pending: std::collections::VecDeque<Event> = buffered.into();
    loop {
        let event = match pending.pop_front() {
            Some(event) => event,
            None => match timeout(SSH_WAIT, common::next_incoming(client)).await {
                Ok(Incoming::Event(event)) => event,
                Ok(Incoming::Response(response)) => {
                    panic!("等待 SSH 事件时收到非预期响应: {response:?}")
                }
                Err(_) => break,
            },
        };
        if event.terminal_id != terminal_id {
            continue;
        }
        match event.kind {
            EventKind::Output { data, .. } => {
                if let Ok(bytes) = STANDARD.decode(data) {
                    output.extend_from_slice(&bytes);
                }
            }
            EventKind::Exited { exit_code } => {
                exit = Some(exit_code);
                break;
            }
            EventKind::Interrupted { signal, .. } => {
                panic!("SSH exec 不应被信号中断: {signal}");
            }
            _ => {}
        }
    }
    (output, exit)
}

#[tokio::test]
async fn ssh_create_reports_capabilities_output_and_exit() {
    let Some(env) = ssh_env() else {
        eprintln!("跳过 SSH service 集成测试：未设置 TERMX_TEST_SSH_* 环境变量");
        return;
    };

    let dir = tempdir().expect("临时目录");
    let mut service = TestService::start(dir.path()).await;
    let mut client = common::connect(&service).await;
    common::handshake(&mut client, &service.token).await;

    let params = serde_json::json!({
        "mode": "pty",
        "cmd": "sh",
        "args": ["-c", "echo hello_ssh; exit 3"],
        "root_session_id": "ses-ssh",
        "name": "remote",
        "ssh": {
            "host": env.host,
            "port": env.port,
            "user": env.user,
            "auth": { "type": "password", "password": env.password },
            "accept_any_host_key": true
        }
    });
    let created = common::create_terminal(&mut client, "ssh-create", params).await;

    // attach 快照暴露 per-terminal capabilities（helper=false / bestEffort=true）。
    let attach = common::call(
        &mut client,
        "ssh-attach",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");
    let attach: AttachResult =
        serde_json::from_value(attach.result.expect("attach 缺少 result")).expect("AttachResult");
    // exec 可能在 attach 前就结束，状态允许已是终态；capabilities 必须存在。
    assert!(matches!(
        attach.snapshot.state,
        LifecycleState::Running | LifecycleState::Exited | LifecycleState::Interrupted
    ));
    let capabilities = attach
        .snapshot
        .capabilities
        .expect("SSH 快照应带 capabilities");
    assert!(!capabilities.helper, "fallback 模式不应声称 helper 可用");
    assert!(capabilities.best_effort, "fallback 模式应标记 bestEffort");

    // 从 cursor=0 订阅，回放 started + 输出 + 终态。
    let (subscribed, buffered) = common::call_collect(
        &mut client,
        "ssh-sub",
        "subscribe",
        serde_json::json!({
            "terminals": [{ "terminal_id": created.terminal_id, "cursor": 0 }]
        }),
    )
    .await;
    assert!(subscribed.ok, "subscribe 应成功: {subscribed:?}");

    let (output, exit) =
        wait_for_output_and_exit(&mut client, &created.terminal_id, buffered).await;
    assert!(
        String::from_utf8_lossy(&output).contains("hello_ssh"),
        "应回放远端输出，实际 {} 字节",
        output.len()
    );
    assert_eq!(exit, Some(3), "应收到远端退出码 3");

    service.shutdown().await;
}
