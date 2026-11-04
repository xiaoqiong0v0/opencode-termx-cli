//! 会话访问隔离与迁移授权集成测试。
#![cfg(any(windows, unix))]

mod common;

use common::{Client, TestService, call, connect, handshake, handshake_root};
use tempfile::tempdir;
use termx_protocol::{CreateResult, ErrorCode, ListItem, Response};

/// 长运行 shell 的创建参数。
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

/// 长运行 shell 的创建参数。
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

/// 断言响应失败且错误码匹配。
fn expect_error(response: Response, code: ErrorCode) {
    assert!(!response.ok, "应失败: {response:?}");
    assert_eq!(response.error.expect("缺少 error").code, code);
}

/// 解析 `terminal.list` 结果。
fn parse_list(response: Response) -> Vec<ListItem> {
    assert!(response.ok, "terminal.list 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 list result")).expect("ListItem 解析失败")
}

/// 解析 `terminal.create` 结果。
fn parse_create(response: Response) -> CreateResult {
    assert!(response.ok, "terminal.create 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 create result"))
        .expect("CreateResult 解析失败")
}

/// 建立一条普通会话连接并完成握手。
async fn connect_as(service: &TestService, root_session_id: &str) -> Client {
    let mut client = connect(service).await;
    handshake_root(&mut client, &service.token, root_session_id).await;
    client
}

#[tokio::test]
async fn sessions_only_see_and_operate_their_own_terminals() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut a = connect_as(&service, "ses-a").await;
    let created = parse_create(
        call(
            &mut a,
            "a-create",
            "terminal.create",
            shell_params("ses-a", "shared"),
        )
        .await,
    );
    let terminal_id = created.terminal_id.clone();

    // A：可见、可 status。
    let list = parse_list(call(&mut a, "a-list", "terminal.list", serde_json::json!({})).await);
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].terminal_id, terminal_id);
    let status = call(
        &mut a,
        "a-status",
        "terminal.status",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(status.ok, "同会话 status 应成功: {status:?}");

    // B：list 看不到，也不能按 terminal_id 访问。
    let mut b = connect_as(&service, "ses-b").await;
    let list = parse_list(call(&mut b, "b-list", "terminal.list", serde_json::json!({})).await);
    assert!(list.is_empty(), "B 不应看到 A 的终端: {list:?}");

    for (id, method, params) in [
        (
            "b-status",
            "terminal.status",
            serde_json::json!({ "terminal_id": terminal_id }),
        ),
        (
            "b-read",
            "terminal.read",
            serde_json::json!({ "terminal_id": terminal_id, "cursor": 0 }),
        ),
        (
            "b-send",
            "terminal.send",
            serde_json::json!({ "terminal_id": terminal_id, "text": "x" }),
        ),
        (
            "b-resize",
            "terminal.resize",
            serde_json::json!({ "terminal_id": terminal_id, "cols": 80, "rows": 24 }),
        ),
        (
            "b-signal",
            "terminal.signal",
            serde_json::json!({ "terminal_id": terminal_id, "signal": "SIGINT" }),
        ),
        (
            "b-terminate",
            "terminal.terminate",
            serde_json::json!({ "terminal_id": terminal_id }),
        ),
        (
            "b-claim",
            "terminal.claim",
            serde_json::json!({ "terminal_id": terminal_id }),
        ),
        (
            "b-release",
            "terminal.release",
            serde_json::json!({ "terminal_id": terminal_id }),
        ),
        (
            "b-attach",
            "terminal.attach",
            serde_json::json!({ "terminal_id": terminal_id }),
        ),
        (
            "b-detach",
            "terminal.detach",
            serde_json::json!({ "terminal_id": terminal_id }),
        ),
    ] {
        let response = call(&mut b, id, method, params).await;
        expect_error(response, ErrorCode::PermissionDenied);
    }

    // 跨会话心跳/关闭、session.list、subscribe(all) 均拒绝。
    expect_error(
        call(
            &mut b,
            "b-hb",
            "heartbeat",
            serde_json::json!({ "root_session_id": "ses-a" }),
        )
        .await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        call(
            &mut b,
            "b-closed",
            "session_closed",
            serde_json::json!({ "root_session_id": "ses-a" }),
        )
        .await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        call(&mut b, "b-sessions", "session.list", serde_json::json!({})).await,
        ErrorCode::PermissionDenied,
    );
    expect_error(
        call(
            &mut b,
            "b-sub-all",
            "terminal.subscribe",
            serde_json::json!({ "all": true }),
        )
        .await,
        ErrorCode::PermissionDenied,
    );

    // 迁移授权：B 不能把终端迁到第三会话；迁到自己的会话（目标会话拉取）可以。
    expect_error(
        call(
            &mut b,
            "b-rebind-a",
            "terminal.rebind",
            serde_json::json!({ "terminal_id": terminal_id, "root_session_id": "ses-a" }),
        )
        .await,
        ErrorCode::PermissionDenied,
    );
    let rebound = call(
        &mut b,
        "b-rebind-b",
        "terminal.rebind",
        serde_json::json!({ "terminal_id": terminal_id, "root_session_id": "ses-b" }),
    )
    .await;
    assert!(rebound.ok, "目标会话拉取应成功: {rebound:?}");

    // 迁移后 B 可访问、A 不可。
    let status = call(
        &mut b,
        "b-status-after",
        "terminal.status",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(status.ok, "迁移后 B status 应成功: {status:?}");
    let list = parse_list(
        call(
            &mut b,
            "b-list-after",
            "terminal.list",
            serde_json::json!({}),
        )
        .await,
    );
    assert_eq!(list.len(), 1);
    expect_error(
        call(
            &mut a,
            "a-status-after",
            "terminal.status",
            serde_json::json!({ "terminal_id": terminal_id }),
        )
        .await,
        ErrorCode::PermissionDenied,
    );
    let list = parse_list(
        call(
            &mut a,
            "a-list-after",
            "terminal.list",
            serde_json::json!({}),
        )
        .await,
    );
    assert!(list.is_empty(), "A 迁移后不应再看到该终端: {list:?}");
}

#[tokio::test]
async fn admin_sees_all_terminals_and_lists_sessions() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut owner = connect_as(&service, "ses-owner").await;
    let created = parse_create(
        call(
            &mut owner,
            "o-create",
            "terminal.create",
            shell_params("ses-owner", "owned"),
        )
        .await,
    );
    let terminal_id = created.terminal_id.clone();

    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;

    let list = parse_list(
        call(
            &mut admin,
            "adm-list",
            "terminal.list",
            serde_json::json!({}),
        )
        .await,
    );
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].terminal_id, terminal_id);

    let sessions = call(
        &mut admin,
        "adm-sessions",
        "session.list",
        serde_json::json!({}),
    )
    .await;
    assert!(sessions.ok, "管理员 session.list 应成功: {sessions:?}");
    let sessions: termx_protocol::SessionListResult =
        serde_json::from_value(sessions.result.expect("缺少 session result"))
            .expect("SessionListResult 解析失败");
    assert!(
        sessions
            .sessions
            .iter()
            .any(|session| session.root_session_id == "ses-owner")
    );

    let status = call(
        &mut admin,
        "adm-status",
        "terminal.status",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(status.ok, "管理员 status 应成功: {status:?}");

    let claim = call(
        &mut admin,
        "adm-claim",
        "terminal.claim",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(claim.ok, "管理员 claim 应成功: {claim:?}");
    let send = call(
        &mut admin,
        "adm-send",
        "terminal.send",
        serde_json::json!({ "terminal_id": terminal_id, "text": "x" }),
    )
    .await;
    assert!(send.ok, "管理员 send 应成功: {send:?}");

    let terminated = call(
        &mut admin,
        "adm-terminate",
        "terminal.terminate",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(terminated.ok, "管理员 terminate 应成功: {terminated:?}");
}

#[tokio::test]
async fn prune_only_touches_caller_session() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    // 两端都创建终态终端：用 stdio 直接退出。
    #[cfg(windows)]
    let exited = serde_json::json!({
        "mode": "stdio",
        "cmd": "cmd.exe",
        "args": ["/C", "exit 0"],
        "root_session_id": "ses-a",
        "name": "done",
    });
    #[cfg(unix)]
    let exited = serde_json::json!({
        "mode": "stdio",
        "cmd": "/bin/sh",
        "args": ["-c", "exit 0"],
        "root_session_id": "ses-a",
        "name": "done",
    });

    let mut a = connect_as(&service, "ses-a").await;
    parse_create(call(&mut a, "a-create", "terminal.create", exited).await);

    let mut other = connect_as(&service, "ses-other").await;
    parse_create(
        call(
            &mut other,
            "o-create",
            "terminal.create",
            shell_params("ses-other", "keep"),
        )
        .await,
    );

    // 等待 A 的 stdio 终端进入终态；失败时显式报错，避免后续 prune 空跑。
    let mut exited = false;
    for attempt in 0..50 {
        let response = call(
            &mut a,
            &format!("a-status-{attempt}"),
            "terminal.status",
            serde_json::json!({ "root_session_id": "ses-a", "name": "done" }),
        )
        .await;
        if response.ok {
            let status: termx_protocol::StatusResult =
                serde_json::from_value(response.result.expect("缺少 status"))
                    .expect("StatusResult 解析失败");
            if status.snapshot.state.is_terminal() {
                exited = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(exited, "A 的 stdio 终端应进入终态");

    // `all=true` 优先于 `states`：即使 states 只列 running，也应清理全部终态，
    // 且清理范围仍限制在调用者会话内。
    let pruned = call(
        &mut a,
        "a-prune",
        "terminal.prune",
        serde_json::json!({ "states": ["running"], "all": true }),
    )
    .await;
    assert!(pruned.ok, "本会话 prune 应成功: {pruned:?}");
    let pruned: termx_protocol::PruneResult =
        serde_json::from_value(pruned.result.expect("缺少 prune result"))
            .expect("PruneResult 解析失败");
    assert_eq!(pruned.removed.len(), 1, "all=true 应忽略 states 清理终态");

    let own_list = parse_list(call(&mut a, "a-list", "terminal.list", serde_json::json!({})).await);
    assert!(own_list.is_empty(), "本会话终态应已清理: {own_list:?}");
    let other_list =
        parse_list(call(&mut other, "o-list", "terminal.list", serde_json::json!({})).await);
    assert_eq!(other_list.len(), 1, "不应清理其它会话的终端");
    assert_eq!(other_list[0].name, "keep");
}
