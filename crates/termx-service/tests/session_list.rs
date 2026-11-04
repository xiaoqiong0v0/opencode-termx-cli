//! `session.list` 集成测试：合并心跳与终端 root、`live` 标记、终端计数与 TTL。
#![cfg(any(windows, unix))]

mod common;

use std::time::Duration;

use common::{Client, TestService, call, connect, create_terminal, handshake};
use tempfile::tempdir;
use termx_protocol::{SessionInfo, SessionListResult};
use termx_service::ServiceConfig;
use tokio::time::sleep;

/// 长运行 shell 参数（pty 需要 cols/rows）。
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

/// 长运行 shell 参数（pty 需要 cols/rows）。
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

/// 发送一次 `session.list` 并解析结果。
async fn list(client: &mut Client, id: &str) -> SessionListResult {
    let response = call(client, id, "session.list", serde_json::json!({})).await;
    assert!(response.ok, "session.list 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 session.list result"))
        .expect("SessionListResult 解析失败")
}

/// 按 `root_session_id` 查找条目。
fn find<'a>(result: &'a SessionListResult, root: &str) -> Option<&'a SessionInfo> {
    result
        .sessions
        .iter()
        .find(|session| session.root_session_id == root)
}

/// 发送新式会话心跳。
async fn heartbeat(client: &mut Client, root_session_id: &str) {
    let response = call(
        client,
        "heartbeat",
        "heartbeat",
        serde_json::json!({ "root_session_id": root_session_id }),
    )
    .await;
    assert!(response.ok, "heartbeat 应成功: {response:?}");
}

#[tokio::test]
async fn session_list_merges_heartbeats_and_terminal_roots() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    heartbeat(&mut client, "ses-live").await;
    create_terminal(&mut client, "c-create", shell_params("ses-term", "main")).await;

    let result = list(&mut client, "c-list-1").await;
    let live = find(&result, "ses-live").expect("心跳会话应出现在列表");
    assert!(live.live, "刚发送心跳的会话应为 live");
    assert!(live.last_seen_ms.is_some(), "存活会话应带 last_seen_ms");
    assert_eq!(live.terminals, 0, "无终端的会话 terminals 应为 0");

    let terminal_only = find(&result, "ses-term").expect("终端 root 应出现在列表");
    assert!(!terminal_only.live, "没有心跳的终端 root 不应标记 live");
    assert!(
        terminal_only.last_seen_ms.is_none(),
        "非存活会话不应带 last_seen_ms"
    );
    assert_eq!(terminal_only.terminals, 1, "终端计数应为 1");

    // 同一 root 同时有终端与心跳：去重为一条，live=true 且保留终端计数。
    heartbeat(&mut client, "ses-term").await;
    let result = list(&mut client, "c-list-2").await;
    let merged = find(&result, "ses-term").expect("合并后仍应只有一条");
    assert!(merged.live, "有心跳时终端 root 应标记 live");
    assert_eq!(merged.terminals, 1, "合并后终端计数不应累加");

    // session_closed 立即移除仅靠心跳存在的会话。
    let closed = call(
        &mut client,
        "c-session-closed",
        "session_closed",
        serde_json::json!({ "root_session_id": "ses-live" }),
    )
    .await;
    assert!(closed.ok, "session_closed 应成功: {closed:?}");
    let result = list(&mut client, "c-list-3").await;
    assert!(
        find(&result, "ses-live").is_none(),
        "关闭后的会话不应再出现在列表"
    );
    assert!(find(&result, "ses-term").is_some(), "终端 root 应继续保留");
}

#[tokio::test]
async fn expired_heartbeat_is_not_listed() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf()).with_session_ttl(Duration::from_millis(100)),
    )
    .await;

    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    heartbeat(&mut client, "ses-expire").await;
    let result = list(&mut client, "c-list-before").await;
    assert!(find(&result, "ses-expire").is_some());

    sleep(Duration::from_millis(200)).await;
    let result = list(&mut client, "c-list-after").await;
    assert!(
        find(&result, "ses-expire").is_none(),
        "超过 TTL 的心跳会话不应再列出"
    );
}
