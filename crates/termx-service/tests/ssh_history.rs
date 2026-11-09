//! SSH 连接历史：成功/失败写入、去重计数、limit、损坏文件与敏感字段。
//!
//! 使用 `MockFactory` 子类模拟 SSH 成功/失败，不依赖真实 SSH 服务。

mod common;

use std::{fs, sync::Arc};

use common::{TestService, call, connect, handshake, handshake_root};
use tempfile::tempdir;
use termx_core::{BackendFactory, CoreError, MockFactory, SpawnedBackend};
use termx_protocol::{
    CreateParams, ErrorCode, SshHistoryListResult, SshHistoryMutationResult, SshHistoryRemoveParams,
};
use termx_service::ServiceConfig;

/// 指定 host 返回 `bad_request`，其它 host 走 Mock 成功，用于模拟失败不记录。
struct SelectiveFactory {
    /// 会失败的 host。
    fail_host: &'static str,
}

impl BackendFactory for SelectiveFactory {
    fn spawn(&self, params: &CreateParams) -> Result<SpawnedBackend, CoreError> {
        if params
            .ssh
            .as_ref()
            .is_some_and(|ssh| ssh.host == self.fail_host)
        {
            return Err(CoreError::BadRequest("模拟 SSH 连接失败".to_string()));
        }
        <MockFactory as BackendFactory>::spawn(&MockFactory, params)
    }
}

/// 构造 SSH 创建参数；密码/路径只用于验证不落盘。
fn ssh_params(host: &str, port: u16, user: &str, auth_type: &str, name: &str) -> serde_json::Value {
    let auth = if auth_type == "key" {
        serde_json::json!({
            "type": "key",
            "path": "/home/alice/.ssh/id_ed25519",
            "passphrase": "passphrase-secret",
        })
    } else {
        serde_json::json!({ "type": "password", "password": "password-secret" })
    };
    serde_json::json!({
        "mode": "pty",
        "cmd": "shell",
        "root_session_id": "ses-history",
        "name": name,
        "ssh": {
            "host": host,
            "port": port,
            "user": user,
            "auth": auth,
            "accept_any_host_key": true
        }
    })
}

/// 调用 `ssh.history.list` 并解析条目。
async fn list_history(client: &mut common::Client) -> Vec<termx_protocol::SshHistoryEntry> {
    let response = call(
        client,
        "history-list",
        "ssh.history.list",
        serde_json::json!({}),
    )
    .await;
    assert!(response.ok, "ssh.history.list 应成功: {response:?}");
    let result: SshHistoryListResult =
        serde_json::from_value(response.result.expect("缺少 list 结果"))
            .expect("ssh.history.list 结果非法");
    result.entries
}

/// 创建终端并返回响应。
async fn create(
    client: &mut common::Client,
    id: &str,
    params: serde_json::Value,
) -> termx_protocol::Response {
    call(client, id, "terminal.create", params).await
}

#[tokio::test]
async fn successful_create_records_and_failure_does_not() {
    let dir = tempdir().expect("临时目录");
    let factory = Arc::new(SelectiveFactory {
        fail_host: "fail.example",
    });
    let service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(factory),
    )
    .await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create(
        &mut client,
        "ok-1",
        ssh_params("example.com", 22, "alice", "password", "ssh-1"),
    )
    .await;
    assert!(created.ok, "首次创建应成功: {created:?}");
    let entries = list_history(&mut client).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].host, "example.com");
    assert_eq!(entries[0].port, 22);
    assert_eq!(entries[0].user, "alice");
    assert_eq!(entries[0].auth_type, "password");
    assert_eq!(entries[0].use_count, 1);

    // 同 host+port+user 再次成功：去重并计数；认证方式刷新为最近一次。
    let created = create(
        &mut client,
        "ok-2",
        ssh_params("example.com", 22, "alice", "key", "ssh-2"),
    )
    .await;
    assert!(created.ok, "重复创建应成功: {created:?}");
    let entries = list_history(&mut client).await;
    assert_eq!(entries.len(), 1, "host+port+user 应去重");
    assert_eq!(entries[0].use_count, 2);
    assert_eq!(entries[0].auth_type, "key");

    // 失败创建不记录。
    let failed = create(
        &mut client,
        "fail-1",
        ssh_params("fail.example", 22, "alice", "password", "ssh-fail"),
    )
    .await;
    assert!(!failed.ok, "模拟失败应返回错误");
    let entries = list_history(&mut client).await;
    assert_eq!(entries.len(), 1, "失败创建不得写入历史");

    // 密码、口令与私钥路径都不落盘。
    let raw = fs::read_to_string(dir.path().join("ssh_history.json")).expect("读取历史文件");
    for secret in [
        "password-secret",
        "passphrase-secret",
        "id_ed25519",
        "/home/alice",
    ] {
        assert!(!raw.contains(secret), "历史文件不得包含敏感值 {secret}");
    }
    assert!(raw.contains("\"auth_type\": \"key\""));
}

#[tokio::test]
async fn history_list_is_admin_only() {
    let dir = tempdir().expect("临时目录");
    let service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;

    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;
    let created = create(
        &mut admin,
        "ok",
        ssh_params("example.com", 22, "alice", "password", "ssh"),
    )
    .await;
    assert!(created.ok);

    let mut regular = connect(&service).await;
    handshake_root(&mut regular, &service.token, "ses-history").await;
    let response = call(
        &mut regular,
        "regular-list",
        "ssh.history.list",
        serde_json::json!({}),
    )
    .await;
    assert!(!response.ok);
    assert_eq!(
        response.error.expect("缺少错误").code,
        ErrorCode::PermissionDenied
    );
}

#[tokio::test]
async fn limit_four_evicts_smallest_use_count_then_oldest() {
    let dir = tempdir().expect("临时目录");
    let service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()).with_ssh_history_limit(4),
        Some(Arc::new(MockFactory)),
    )
    .await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    for index in 0..5 {
        let created = create(
            &mut client,
            &format!("ok-{index}"),
            ssh_params(
                "example.com",
                2200 + index,
                "alice",
                "password",
                &format!("ssh-{index}"),
            ),
        )
        .await;
        assert!(created.ok, "创建 {index} 应成功: {created:?}");
    }

    let entries = list_history(&mut client).await;
    assert_eq!(entries.len(), 4, "limit=4 应只保留 4 条");
    let ports: Vec<u16> = entries.iter().map(|entry| entry.port).collect();
    assert!(
        !ports.contains(&2200),
        "count 相同时应淘汰最早写入的 2200: {ports:?}"
    );
}

#[tokio::test]
async fn corrupt_json_does_not_block_create_and_clear_works() {
    let dir = tempdir().expect("临时目录");
    fs::write(dir.path().join("ssh_history.json"), "{broken").expect("写入损坏 JSON");

    let service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    assert!(
        list_history(&mut client).await.is_empty(),
        "损坏文件按空历史"
    );

    let created = create(
        &mut client,
        "ok",
        ssh_params("example.com", 22, "alice", "password", "ssh"),
    )
    .await;
    assert!(created.ok, "损坏历史不得阻断创建: {created:?}");
    assert_eq!(list_history(&mut client).await.len(), 1);

    let removed = call(
        &mut client,
        "remove",
        "ssh.history.remove",
        serde_json::to_value(SshHistoryRemoveParams {
            host: "example.com".to_string(),
            port: 22,
            user: "alice".to_string(),
        })
        .expect("序列化 remove 参数"),
    )
    .await;
    assert!(removed.ok);
    let removed: SshHistoryMutationResult =
        serde_json::from_value(removed.result.expect("缺少 remove 结果")).unwrap();
    assert_eq!(removed.removed, 1);

    let created = create(
        &mut client,
        "ok-2",
        ssh_params("example.com", 22, "alice", "password", "ssh-2"),
    )
    .await;
    assert!(created.ok);
    let cleared = call(
        &mut client,
        "clear",
        "ssh.history.clear",
        serde_json::json!({}),
    )
    .await;
    assert!(cleared.ok);
    let cleared: SshHistoryMutationResult =
        serde_json::from_value(cleared.result.expect("缺少 clear 结果")).unwrap();
    assert_eq!(cleared.removed, 1);
    assert!(list_history(&mut client).await.is_empty());
}
