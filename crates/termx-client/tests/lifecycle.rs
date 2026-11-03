//! 连接/握手、真实 PTY 生命周期与错误映射。

mod common;

use std::time::Duration;

use common::{TestService, locator, next_event_matching, pty_params, wait_output_contains};
use futures_util::{SinkExt, StreamExt};
use termx_client::{Client, ClientConfig, ClientError};
use termx_protocol::{
    AttachParams, ClaimParams, ErrorCode, EventKind, SendParams, StatusParams, TerminateParams,
};
use tokio::{net::TcpListener, sync::broadcast::error::RecvError, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// connect + hello 成功后可正常发请求；close 后事件通道关闭。
#[tokio::test]
async fn connect_and_hello_succeeds() {
    let dir = tempfile::tempdir().expect("临时目录");
    let service = TestService::start(dir.path()).await;

    let client = Client::connect(service.client_config())
        .await
        .expect("连接并握手应成功");
    assert!(!client.is_closed());

    let terminals = client
        .list(Default::default())
        .await
        .expect("hello 后 list 应成功");
    assert!(terminals.is_empty(), "新服务不应有终端");

    let mut events = client.events();
    client.close().await;
    assert!(client.is_closed());
    assert!(
        matches!(events.recv().await, Err(RecvError::Closed)),
        "close 后事件通道应关闭"
    );
}

/// 错误 token 映射为 `Auth`。
#[tokio::test]
async fn wrong_token_maps_to_auth() {
    let dir = tempfile::tempdir().expect("临时目录");
    let service = TestService::start(dir.path()).await;

    let mut config = service.client_config();
    config.token = "definitely-wrong".to_string();
    let error = Client::connect(config)
        .await
        .expect_err("错误 token 应失败");
    assert!(
        matches!(error, ClientError::Auth),
        "应映射为 Auth，实际: {error:?}"
    );
}

/// 非环回 endpoint 在建立连接前就被拒绝，token 不会被发送。
#[tokio::test]
async fn non_loopback_endpoint_is_rejected_before_connect() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定本地监听");
    let port = listener.local_addr().expect("本地地址").port();
    let config = ClientConfig::new(
        format!("ws://0.0.0.0:{port}"),
        "token-must-not-leak",
        "client-test",
    );

    let error = Client::connect(config)
        .await
        .expect_err("非环回 host 应被拒绝");
    assert!(
        matches!(error, ClientError::Connect(_)),
        "应返回 Connect，实际: {error:?}"
    );

    let accepted = timeout(Duration::from_millis(200), listener.accept()).await;
    assert!(
        accepted.is_err(),
        "拒绝必须发生在 TCP/WebSocket 连接与 token 发送之前"
    );
}

/// 服务端返回不同 proto 时映射为 `VersionMismatch`。
#[tokio::test]
async fn hello_version_mismatch_is_rejected() {
    let (endpoint, server) = spawn_fake_server(
        serde_json::json!({
            "id": "hello",
            "ok": true,
            "result": {
                "proto": "termx/2",
                "server": "fake",
                "capabilities": { "helper": false, "version": "termx-helper/1", "bestEffort": false }
            }
        }),
        None,
    )
    .await;

    let error = Client::connect(ClientConfig::new(endpoint, "token", "client-test"))
        .await
        .expect_err("proto 不一致应失败");
    assert!(
        matches!(error, ClientError::VersionMismatch),
        "应返回 VersionMismatch，实际: {error:?}"
    );
    server.await.expect("fake 服务任务不应 panic");
}

/// `id == ""` 的连接级错误不被丢弃：后续请求返回该错误，事件通道关闭。
#[tokio::test]
async fn connection_level_error_is_retained_for_later_requests() {
    let (endpoint, server) = spawn_fake_server(
        serde_json::json!({
            "id": "hello",
            "ok": true,
            "result": {
                "proto": "termx/1",
                "server": "fake",
                "capabilities": { "helper": false, "version": "termx-helper/1", "bestEffort": false }
            }
        }),
        Some(serde_json::json!({
            "id": "",
            "ok": false,
            "error": {
                "code": "limit_exceeded",
                "message": "订阅队列已满",
                "retryable": true
            }
        })),
    )
    .await;

    let client = Client::connect(ClientConfig::new(endpoint, "token", "client-test"))
        .await
        .expect("hello 应成功");
    let mut events = client.events();
    let closed = timeout(Duration::from_secs(5), events.recv()).await;
    assert!(
        matches!(closed, Ok(Err(RecvError::Closed))),
        "连接级错误后事件通道应关闭，实际: {closed:?}"
    );

    let error = client
        .list(Default::default())
        .await
        .expect_err("连接级错误后请求应失败");
    match error {
        ClientError::Protocol(protocol) => assert_eq!(protocol.code, ErrorCode::LimitExceeded),
        other => panic!("应返回 Protocol(limit_exceeded)，实际: {other:?}"),
    }
    assert!(client.last_error().is_some(), "last_error 应保留连接级错误");
    server.await.expect("fake 服务任务不应 panic");
}

/// 显式请求 ID 在连接内只能使用一次（完成后也不释放）。
#[tokio::test]
async fn explicit_request_id_cannot_be_reused() {
    let dir = tempfile::tempdir().expect("临时目录");
    let service = TestService::start(dir.path()).await;
    let client = Client::connect(service.client_config())
        .await
        .expect("连接并握手应成功");

    client
        .request(
            Some("fixed-id".to_string()),
            "terminal.list",
            serde_json::json!({}),
        )
        .await
        .expect("首次显式 ID 应成功");
    let error = client
        .request(
            Some("fixed-id".to_string()),
            "terminal.list",
            serde_json::json!({}),
        )
        .await
        .expect_err("复用显式 ID 应失败");
    match error {
        ClientError::Protocol(protocol) => assert_eq!(protocol.code, ErrorCode::BadRequest),
        other => panic!("应返回 Protocol(bad_request)，实际: {other:?}"),
    }

    client.close().await;
}

/// 未知终端映射为 `Protocol(not_found)`。
#[tokio::test]
async fn unknown_terminal_maps_to_not_found() {
    let dir = tempfile::tempdir().expect("临时目录");
    let service = TestService::start(dir.path()).await;
    let client = Client::connect(service.client_config())
        .await
        .expect("连接并握手应成功");

    let error = client
        .status(StatusParams {
            locator: locator("missing-terminal"),
        })
        .await
        .expect_err("未知终端应失败");
    match error {
        ClientError::Protocol(error) => assert_eq!(error.code, ErrorCode::NotFound),
        other => panic!("应映射为 Protocol(not_found)，实际: {other:?}"),
    }
}

/// create → attach(cursor=0) 收到 started；claim 后 send 收到 output；terminate 收到 interrupted。
#[tokio::test]
async fn create_attach_send_terminate_round_trip() {
    let dir = tempfile::tempdir().expect("临时目录");
    let service = TestService::start(dir.path()).await;
    let client = Client::connect(service.client_config())
        .await
        .expect("连接并握手应成功");

    // 先订阅事件，避免 attach 响应返回前回放的 started 丢失。
    let mut events = client.events();

    let created = client
        .create(pty_params("ses-lifecycle", "term"))
        .await
        .expect("create 应成功");
    assert_eq!(created.cursor, 0, "create 的初始游标固定为 0");
    let terminal_id = created.terminal_id.clone();

    let attached = client
        .attach(AttachParams {
            locator: locator(&terminal_id),
            cursor: Some(0),
        })
        .await
        .expect("attach 应成功");
    assert_eq!(attached.snapshot.terminal_id, terminal_id);

    let started = next_event_matching(&mut events, |event| {
        event.terminal_id == terminal_id && matches!(&event.kind, EventKind::Started { .. })
    })
    .await;
    assert!(started.seq >= 1);

    let claim = client
        .claim(ClaimParams {
            locator: locator(&terminal_id),
            force: false,
        })
        .await
        .expect("claim 应成功");
    assert!(!claim.owner.is_empty(), "claim 应返回 owner");

    client
        .send(SendParams {
            locator: locator(&terminal_id),
            data: None,
            text: Some("echo hi_termx".to_string()),
            enter: true,
        })
        .await
        .expect("claim 后 send 应成功");
    wait_output_contains(&mut events, &terminal_id, b"hi_termx").await;

    client
        .terminate(TerminateParams {
            locator: locator(&terminal_id),
            force: false,
        })
        .await
        .expect("terminate 应成功");
    let interrupted = next_event_matching(&mut events, |event| {
        event.terminal_id == terminal_id && matches!(&event.kind, EventKind::Interrupted { .. })
    })
    .await;
    match interrupted.kind {
        EventKind::Interrupted { source, .. } => assert_eq!(source, "terminate"),
        other => panic!("应为 interrupted 事件，实际: {other:?}"),
    }

    client.close().await;
}

/// 启动只回答一次 `hello` 的假 WebSocket 服务。
///
/// `follow_up` 在 hello 响应后延迟发送，避免被客户端握手读取阶段吞掉。
async fn spawn_fake_server(
    hello_reply: serde_json::Value,
    follow_up: Option<serde_json::Value>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 fake 监听");
    let port = listener.local_addr().expect("fake 本地地址").port();
    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept fake 连接");
        let mut ws = accept_async(stream).await.expect("fake WS 握手");
        let _ = ws.next().await;
        ws.send(Message::Text(hello_reply.to_string().into()))
            .await
            .expect("发送 hello 响应");
        if let Some(follow_up) = follow_up {
            tokio::time::sleep(Duration::from_millis(150)).await;
            ws.send(Message::Text(follow_up.to_string().into()))
                .await
                .expect("发送连接级错误");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    (format!("ws://127.0.0.1:{port}"), handle)
}
