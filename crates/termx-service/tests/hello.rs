//! F1/F2 集成测试：环回 WebSocket、token/hello 鉴权与 `terminal.*` RPC。

use std::{
    fs,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use tempfile::tempdir;
use termx_core::{MockFactory, Registry, TerminalId};
use termx_protocol::{
    AttachResult, ClaimResult, Confidence, CreateResult, ErrorCode, Event, EventKind,
    HELPER_PROTOCOL_VERSION, HelloResult, LifecycleState, ListItem, Mode, PROTOCOL_VERSION,
    RebindResult, Response, StatusResult, Stream, SubscribeResult, from_json,
};
use termx_service::{PORT_FILE_NAME, Service, ServiceConfig, ServiceError, TOKEN_FILE_NAME};
use tokio::{net::TcpStream, task::JoinHandle, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

/// 测试用 WebSocket 客户端类型。
type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// 测试守卫：负责在测试结束时停止 accept 循环。
struct TestService {
    /// 实际监听地址。
    addr: SocketAddr,
    /// 从 token 文件读出的 token。
    token: String,
    /// 与运行中服务共享的 registry。
    registry: Arc<Mutex<Registry>>,
    /// `Service::run` 的任务句柄。
    handle: JoinHandle<Result<(), ServiceError>>,
}

impl TestService {
    /// 用临时配置目录启动服务并读出 token。
    async fn start(config_dir: &Path) -> Self {
        Self::start_with_config(ServiceConfig::new(config_dir.to_path_buf())).await
    }

    /// 用给定配置启动服务并读出 token；注入 mock 工厂避免创建真实进程。
    async fn start_with_config(config: ServiceConfig) -> Self {
        let service = Service::bind_with_factory(config, Arc::new(MockFactory))
            .await
            .expect("服务绑定失败");
        let addr = service.local_addr();
        let token = fs::read_to_string(service.config().token_path())
            .expect("token 文件缺失")
            .trim()
            .to_string();
        let registry = service.registry();
        let handle = tokio::spawn(service.run());

        Self {
            addr,
            token,
            registry,
            handle,
        }
    }

    /// 构造客户端连接地址（IPv4 / IPv6 都正确加括号）。
    fn url(&self) -> String {
        match self.addr {
            SocketAddr::V4(addr) => format!("ws://{addr}"),
            SocketAddr::V6(addr) => format!("ws://[{addr}]"),
        }
    }
}

impl Drop for TestService {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// 连接测试服务。
async fn connect(service: &TestService) -> Client {
    let (client, _) = connect_async(service.url())
        .await
        .expect("连接测试服务失败");
    client
}

/// 带 `Authorization: Bearer <token>` 头连接测试服务。
async fn connect_with_bearer(service: &TestService, token: &str) -> Client {
    let mut request = service.url().into_client_request().expect("构造请求失败");
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("构造鉴权头失败"),
    );
    let (client, _) = connect_async(request).await.expect("连接测试服务失败");
    client
}

/// 发送一个文本帧。
async fn send_text(client: &mut Client, text: String) {
    client
        .send(Message::Text(text.into()))
        .await
        .expect("发送请求失败");
}

/// 读取并解析一个响应。
async fn next_response(client: &mut Client) -> Response {
    let message = timeout(Duration::from_secs(5), client.next())
        .await
        .expect("等待响应超时")
        .expect("连接已关闭")
        .expect("读取响应失败");
    let text = message.to_text().expect("响应不是文本帧");
    from_json(text).expect("响应 JSON 非法")
}

/// 断言服务端在超时内发送 Close 或直接断开。
async fn expect_closed(client: &mut Client) {
    let closed = timeout(Duration::from_secs(5), async {
        loop {
            match client.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await
    .is_ok();
    assert!(closed, "服务端未在超时内关闭连接");
}

/// 构造带指定客户端标识的 `hello` 请求 JSON。
fn hello_request_for(id: &str, token: &str, proto: &str, client: &str) -> String {
    serde_json::json!({
        "id": id,
        "method": "hello",
        "params": {
            "proto": proto,
            "token": token,
            "client": client,
            "admin": true,
        }
    })
    .to_string()
}

/// 构造 `hello` 请求 JSON。
fn hello_request(id: &str, token: &str, proto: &str) -> String {
    hello_request_for(id, token, proto, "termx-service-test")
}

/// 在已连接客户端上完成 `hello` 握手（默认客户端标识）。
async fn handshake(client: &mut Client, token: &str) {
    handshake_as(client, token, "termx-service-test").await;
}

/// 用指定客户端标识完成 `hello` 握手。
async fn handshake_as(client: &mut Client, token: &str, client_id: &str) {
    send_text(
        client,
        hello_request_for("h-001", token, PROTOCOL_VERSION, client_id),
    )
    .await;
    let response = next_response(client).await;
    assert!(response.ok, "hello 应成功: {response:?}");
}

/// 下一帧：响应或事件。
enum Incoming {
    /// RPC 响应。
    Response(Response),
    /// 服务端推送事件。
    Event(Event),
}

/// 读取下一个帧并按 `id` 字段区分响应/事件。
async fn next_incoming(client: &mut Client) -> Incoming {
    let message = timeout(Duration::from_secs(5), client.next())
        .await
        .expect("等待帧超时")
        .expect("连接已关闭")
        .expect("读取帧失败");
    let text = message.to_text().expect("帧不是文本");
    let value: serde_json::Value = from_json(text).expect("帧 JSON 非法");

    if value.get("id").is_some() {
        Incoming::Response(serde_json::from_value(value).expect("Response 解析失败"))
    } else {
        Incoming::Event(serde_json::from_value(value).expect("Event 解析失败"))
    }
}

/// 发送请求并读到对应响应，顺带收集之前到达的事件帧。
async fn call_collect(
    client: &mut Client,
    id: &str,
    method: &str,
    params: serde_json::Value,
) -> (Response, Vec<Event>) {
    let request = serde_json::json!({
        "id": id,
        "method": method,
        "params": params,
    })
    .to_string();
    send_text(client, request).await;

    let mut events = Vec::new();
    loop {
        match next_incoming(client).await {
            Incoming::Event(event) => events.push(event),
            Incoming::Response(response) if response.id == id => return (response, events),
            Incoming::Response(other) => panic!("收到非预期响应: {other:?}"),
        }
    }
}

/// 等待下一个事件（响应帧视为测试失败）。
async fn next_event(client: &mut Client) -> Event {
    match next_incoming(client).await {
        Incoming::Event(event) => event,
        Incoming::Response(response) => panic!("本应收到事件，却收到响应: {response:?}"),
    }
}

/// 补足到 `count` 个事件；响应可能先于事件到达，因此允许已收集部分事件。
async fn ensure_events(client: &mut Client, mut events: Vec<Event>, count: usize) -> Vec<Event> {
    while events.len() < count {
        events.push(next_event(client).await);
    }
    events
}

/// 等待下一个满足条件的事件，跳过不相关事件。
async fn next_event_matching<F>(client: &mut Client, mut predicate: F) -> Event
where
    F: FnMut(&Event) -> bool,
{
    loop {
        let event = next_event(client).await;
        if predicate(&event) {
            return event;
        }
    }
}

/// 通过共享 registry 注入一条 output 事件。
fn report_output(service: &TestService, terminal_id: &str, data: &str) -> u64 {
    let mut registry = service.registry.lock().expect("registry 锁中毒");
    registry
        .report_event(
            &TerminalId::new(terminal_id),
            EventKind::Output {
                data: data.to_string(),
                stream: Stream::Stdout,
            },
        )
        .expect("注入 output 事件失败")
}

/// 发送一个 RPC 请求并等待响应。
async fn call(client: &mut Client, id: &str, method: &str, params: serde_json::Value) -> Response {
    let request = serde_json::json!({
        "id": id,
        "method": method,
        "params": params,
    })
    .to_string();
    send_text(client, request).await;
    next_response(client).await
}

/// 构造 `terminal.create` 的 pty 参数。
fn create_params(root_session_id: &str, name: &str, force: bool) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "mock",
        "root_session_id": root_session_id,
        "name": name,
        "force": force,
    })
}

#[tokio::test]
async fn hello_with_valid_token_succeeds() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;

    send_text(
        &mut client,
        hello_request("c-001", &service.token, PROTOCOL_VERSION),
    )
    .await;
    let response = next_response(&mut client).await;

    assert!(response.ok, "hello 应成功: {response:?}");
    assert_eq!(response.id, "c-001");
    let result: HelloResult = serde_json::from_value(response.result.expect("缺少 result"))
        .expect("HelloResult 解析失败");
    assert_eq!(result.proto, PROTOCOL_VERSION);
    assert_eq!(result.proto, "termx/1");
    assert!(!result.capabilities.helper);
    assert_eq!(result.capabilities.version, HELPER_PROTOCOL_VERSION);
    assert!(!result.capabilities.best_effort);
}

#[tokio::test]
async fn wrong_token_is_auth_failed_and_closes() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;

    send_text(
        &mut client,
        hello_request("c-002", "definitely-wrong-token", PROTOCOL_VERSION),
    )
    .await;
    let response = next_response(&mut client).await;

    assert!(!response.ok);
    assert_eq!(
        response.error.expect("缺少 error").code,
        ErrorCode::AuthFailed
    );
    expect_closed(&mut client).await;
}

#[tokio::test]
async fn wrong_proto_is_version_mismatch_and_closes() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;

    send_text(
        &mut client,
        hello_request("c-003", &service.token, "termx/999"),
    )
    .await;
    let response = next_response(&mut client).await;

    assert!(!response.ok);
    assert_eq!(
        response.error.expect("缺少 error").code,
        ErrorCode::VersionMismatch
    );
    expect_closed(&mut client).await;
}

#[tokio::test]
async fn first_message_must_be_hello() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;

    let request = serde_json::json!({
        "id": "c-004",
        "method": "terminal.list",
        "params": {},
    })
    .to_string();
    send_text(&mut client, request).await;
    let response = next_response(&mut client).await;

    assert!(!response.ok);
    assert_eq!(
        response.error.expect("缺少 error").code,
        ErrorCode::ProtocolError
    );
    expect_closed(&mut client).await;
}

#[tokio::test]
async fn malformed_first_frame_is_protocol_error() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;

    send_text(&mut client, "这不是 JSON".to_string()).await;
    let response = next_response(&mut client).await;

    assert!(!response.ok);
    assert_eq!(response.id, "");
    assert_eq!(
        response.error.expect("缺少 error").code,
        ErrorCode::ProtocolError
    );
    expect_closed(&mut client).await;
}

#[tokio::test]
async fn bind_writes_token_and_port_files() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let token = fs::read_to_string(dir.path().join(TOKEN_FILE_NAME)).expect("token 文件缺失");
    assert!(!token.trim().is_empty());
    assert_eq!(token.trim(), service.token);

    let port = fs::read_to_string(dir.path().join(PORT_FILE_NAME)).expect("port 文件缺失");
    assert_eq!(
        port.trim().parse::<u16>().expect("port 内容不是端口号"),
        service.addr.port()
    );
    assert!(service.addr.ip().is_loopback());
}

#[cfg(unix)]
#[tokio::test]
async fn token_file_permissions_are_0600() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempdir().expect("创建临时目录失败");
    let _service = TestService::start(dir.path()).await;

    let metadata = fs::metadata(dir.path().join(TOKEN_FILE_NAME)).expect("token 文件缺失");
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
}

#[tokio::test]
async fn terminal_create_returns_id_and_cursor() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let response = call(
        &mut client,
        "c-101",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;

    assert!(response.ok, "terminal.create 应成功: {response:?}");
    let result: CreateResult = serde_json::from_value(response.result.expect("缺少 result"))
        .expect("CreateResult 解析失败");
    assert_eq!(result.terminal_id.len(), 26);
    // create 返回 started 之前的游标，便于 attach 回放 started。
    assert_eq!(result.cursor, 0);
}

#[tokio::test]
async fn terminal_list_contains_created_terminal() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created_response = call(
        &mut client,
        "c-201",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let response = call(&mut client, "c-202", "terminal.list", serde_json::json!({})).await;
    assert!(response.ok, "terminal.list 应成功: {response:?}");
    let items: Vec<ListItem> =
        serde_json::from_value(response.result.expect("缺少 result")).expect("ListItem 解析失败");

    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item.terminal_id, created.terminal_id);
    assert_eq!(item.root_session_id, "ses-1");
    assert_eq!(item.name, "build");
    assert_eq!(item.state, LifecycleState::Running);
    assert_eq!(item.mode, Mode::Pty);
    assert!(item.pid.is_some(), "mock 后端应提供 pid");
    assert!(item.idle_timeout.is_none());
    assert!(item.last_activity.is_some());
    assert!(!item.created_at.is_empty());
}

#[tokio::test]
async fn duplicate_without_force_returns_session_exists_with_old_id() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created_response = call(
        &mut client,
        "c-301",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let response = call(
        &mut client,
        "c-302",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;

    assert!(!response.ok, "同名且无 force 应失败");
    let error = response.error.expect("缺少 error");
    assert_eq!(error.code, ErrorCode::SessionExists);
    assert_eq!(error.terminal_id, Some(created.terminal_id));
    assert!(!error.retryable);
}

#[tokio::test]
async fn force_replaces_terminal_and_keeps_single_entry() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let first_response = call(
        &mut client,
        "c-401",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let first: CreateResult =
        serde_json::from_value(first_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let second_response = call(
        &mut client,
        "c-402",
        "terminal.create",
        create_params("ses-1", "build", true),
    )
    .await;
    assert!(
        second_response.ok,
        "force create 应成功: {second_response:?}"
    );
    let second: CreateResult =
        serde_json::from_value(second_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");
    assert_ne!(first.terminal_id, second.terminal_id);

    let list_response = call(&mut client, "c-403", "terminal.list", serde_json::json!({})).await;
    let items: Vec<ListItem> =
        serde_json::from_value(list_response.result.expect("缺少 list result"))
            .expect("ListItem 解析失败");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].terminal_id, second.terminal_id);
}

#[tokio::test]
async fn terminal_rebind_moves_key_and_preserves_terminal_id() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created_response = call(
        &mut client,
        "rb-101",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    assert!(created_response.ok, "create 应成功: {created_response:?}");
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");
    let created = created.terminal_id;

    let response = call(
        &mut client,
        "rb-102",
        "terminal.rebind",
        serde_json::json!({
            "terminal_id": created,
            "root_session_id": "ses-2",
            "name": "verify"
        }),
    )
    .await;
    assert!(response.ok, "rebind 应成功: {response:?}");
    let result: RebindResult = serde_json::from_value(response.result.expect("缺少 rebind result"))
        .expect("RebindResult 解析失败");
    assert_eq!(result.terminal_id, created);
    assert_eq!(result.root_session_id, "ses-2");
    assert_eq!(result.name, "verify");

    let old_status = call(
        &mut client,
        "rb-103",
        "terminal.status",
        serde_json::json!({ "root_session_id": "ses-1", "name": "build" }),
    )
    .await;
    assert!(!old_status.ok, "旧键应解析失败");
    assert_eq!(
        old_status.error.expect("缺少 error").code,
        ErrorCode::NotFound
    );

    let new_status = call(
        &mut client,
        "rb-104",
        "terminal.status",
        serde_json::json!({ "root_session_id": "ses-2", "name": "verify" }),
    )
    .await;
    assert!(new_status.ok, "新键应可解析: {new_status:?}");
    let snapshot: StatusResult =
        serde_json::from_value(new_status.result.expect("缺少 status result"))
            .expect("StatusResult 解析失败");
    assert_eq!(snapshot.snapshot.terminal_id, created);
    assert_eq!(snapshot.snapshot.state, LifecycleState::Running);
}

#[tokio::test]
async fn terminal_rebind_conflict_and_force() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let source_response = call(
        &mut client,
        "rb-201",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    assert!(source_response.ok, "create 应成功: {source_response:?}");
    let source: CreateResult =
        serde_json::from_value(source_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");
    let source = source.terminal_id;

    let target_response = call(
        &mut client,
        "rb-202",
        "terminal.create",
        create_params("ses-2", "build", false),
    )
    .await;
    assert!(target_response.ok, "create 应成功: {target_response:?}");
    let target: CreateResult =
        serde_json::from_value(target_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");
    let target = target.terminal_id;

    // 同名且无 force：session_exists 携带目标旧终端 ID。
    let conflict = call(
        &mut client,
        "rb-203",
        "terminal.rebind",
        serde_json::json!({ "terminal_id": source, "root_session_id": "ses-2" }),
    )
    .await;
    assert!(!conflict.ok, "目标键已存在且无 force 应失败");
    let error = conflict.error.expect("缺少 error");
    assert_eq!(error.code, ErrorCode::SessionExists);
    assert_eq!(error.terminal_id, Some(target.clone()));

    // force：目标旧终端被终止，源终端迁移到目标键。
    let forced = call(
        &mut client,
        "rb-204",
        "terminal.rebind",
        serde_json::json!({ "terminal_id": source, "root_session_id": "ses-2", "force": true }),
    )
    .await;
    assert!(forced.ok, "force rebind 应成功: {forced:?}");
    let result: RebindResult = serde_json::from_value(forced.result.expect("缺少 rebind result"))
        .expect("RebindResult 解析失败");
    assert_eq!(result.terminal_id, source);

    let list = call(
        &mut client,
        "rb-205",
        "terminal.list",
        serde_json::json!({}),
    )
    .await;
    let items: Vec<ListItem> =
        serde_json::from_value(list.result.expect("缺少 list result")).expect("ListItem 解析失败");
    assert_eq!(items.len(), 1, "被置换的目标终端应已移除");
    assert_eq!(items[0].terminal_id, source);
    assert_eq!(items[0].root_session_id, "ses-2");
    assert_eq!(items[0].name, "build");
}

#[tokio::test]
async fn terminal_status_returns_running_snapshot_with_pid() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created_response = call(
        &mut client,
        "c-501",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let response = call(
        &mut client,
        "c-502",
        "terminal.status",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(response.ok, "terminal.status 应成功: {response:?}");
    let status: StatusResult = serde_json::from_value(response.result.expect("缺少 result"))
        .expect("StatusResult 解析失败");

    assert_eq!(status.snapshot.state, LifecycleState::Running);
    assert_eq!(status.snapshot.mode, Mode::Pty);
    assert_eq!(status.snapshot.cursor, 1);
    assert!(status.snapshot.pid.is_some());
    assert!(status.snapshot.pgid.is_some());
}

/// F3：attach 回放 started、事件推送与 detach 停止推送。
#[tokio::test]
async fn attach_replays_started_and_receives_output_until_detach() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake_as(&mut client, &service.token, "client-a").await;

    let created_response = call(
        &mut client,
        "c-601",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let (attach_response, attach_events) = call_collect(
        &mut client,
        "c-602",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach_response.ok, "attach 应成功: {attach_response:?}");
    let result: AttachResult =
        serde_json::from_value(attach_response.result.expect("缺少 attach result"))
            .expect("AttachResult 解析失败");
    assert_eq!(result.cursor, 1);
    assert_eq!(result.snapshot.cursor, 1);
    assert!(result.reset.is_none());
    let attach_events = ensure_events(&mut client, attach_events, 2).await;
    assert_eq!(attach_events.len(), 2);
    assert!(matches!(attach_events[0].kind, EventKind::Started { .. }));
    assert_eq!(attach_events[0].seq, 1);
    // client_id 由服务端按连接生成，不再等于 hello.client 标签。
    assert!(matches!(
        attach_events[1].kind,
        EventKind::Attached { ref client_id } if !client_id.is_empty()
    ));
    assert_eq!(attach_events[1].seq, 2);

    assert_eq!(report_output(&service, &created.terminal_id, "YQ=="), 3);
    let output = next_event(&mut client).await;
    assert_eq!(output.seq, 3);
    assert!(matches!(
        output.kind,
        EventKind::Output { ref data, .. } if data == "YQ=="
    ));

    let (detach_response, detach_events) = call_collect(
        &mut client,
        "c-603",
        "terminal.detach",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(detach_response.ok, "detach 应成功: {detach_response:?}");
    assert!(detach_events.is_empty(), "detach 不应回推自己的 detached");

    assert_eq!(report_output(&service, &created.terminal_id, "Yg=="), 5);
    let extra = timeout(Duration::from_millis(400), client.next()).await;
    assert!(extra.is_err(), "detach 后不应再收到事件: {extra:?}");
}

/// F3：subscribe all 能收到订阅之后新建终端的 started。
#[tokio::test]
async fn subscribe_all_receives_started_of_new_terminal() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut watcher = connect(&service).await;
    handshake_as(&mut watcher, &service.token, "client-b").await;
    let (subscribe_response, subscribe_events) = call_collect(
        &mut watcher,
        "c-701",
        "terminal.subscribe",
        serde_json::json!({ "all": true }),
    )
    .await;
    assert!(
        subscribe_response.ok,
        "subscribe 应成功: {subscribe_response:?}"
    );
    let result: SubscribeResult =
        serde_json::from_value(subscribe_response.result.expect("缺少 subscribe result"))
            .expect("SubscribeResult 解析失败");
    assert!(result.cursors.is_empty());
    assert!(subscribe_events.is_empty());

    let mut creator = connect(&service).await;
    handshake_as(&mut creator, &service.token, "client-a").await;
    let created_response = call(
        &mut creator,
        "c-702",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let event = next_event(&mut watcher).await;
    assert_eq!(event.terminal_id, created.terminal_id);
    assert_eq!(event.seq, 1);
    assert!(matches!(
        event.kind,
        EventKind::Started { ref mode, .. } if *mode == Mode::Pty
    ));
}

/// F3：两个客户端收到同一批 output，且各自保持 seq 顺序。
#[tokio::test]
async fn two_clients_receive_same_outputs_in_order() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut first = connect(&service).await;
    handshake_as(&mut first, &service.token, "client-a").await;
    let created_response = call(
        &mut first,
        "c-801",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");
    let (attach_response, attach_events) = call_collect(
        &mut first,
        "c-802",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach_response.ok, "attach 应成功: {attach_response:?}");
    let attach_events = ensure_events(&mut first, attach_events, 2).await;
    assert_eq!(attach_events.len(), 2);

    let mut second = connect(&service).await;
    handshake_as(&mut second, &service.token, "client-b").await;
    let (subscribe_response, _) = call_collect(
        &mut second,
        "c-803",
        "terminal.subscribe",
        serde_json::json!({ "all": true }),
    )
    .await;
    assert!(
        subscribe_response.ok,
        "subscribe 应成功: {subscribe_response:?}"
    );

    assert_eq!(report_output(&service, &created.terminal_id, "YQ=="), 3);
    assert_eq!(report_output(&service, &created.terminal_id, "Yg=="), 4);

    let first_one = next_event_matching(&mut first, |event| {
        matches!(event.kind, EventKind::Output { .. })
    })
    .await;
    let first_two = next_event_matching(&mut first, |event| {
        matches!(event.kind, EventKind::Output { .. })
    })
    .await;
    let second_one = next_event_matching(&mut second, |event| {
        matches!(event.kind, EventKind::Output { .. })
    })
    .await;
    let second_two = next_event_matching(&mut second, |event| {
        matches!(event.kind, EventKind::Output { .. })
    })
    .await;

    assert_eq!((first_one.seq, first_two.seq), (3, 4));
    assert_eq!((second_one.seq, second_two.seq), (3, 4));
    for event in [&first_one, &first_two, &second_one, &second_two] {
        assert_eq!(event.terminal_id, created.terminal_id);
    }
}

/// F4：status/attach 支持以 `(root_session_id, name)` 定位。
#[tokio::test]
async fn locator_supports_session_key_for_status_and_attach() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created_response = call(
        &mut client,
        "c-901",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let locator = serde_json::json!({
        "root_session_id": "ses-1",
        "name": "build",
    });

    let status_response = call(&mut client, "c-902", "terminal.status", locator.clone()).await;
    assert!(
        status_response.ok,
        "按会话键 status 应成功: {status_response:?}"
    );
    let status: StatusResult =
        serde_json::from_value(status_response.result.expect("缺少 status result"))
            .expect("StatusResult 解析失败");
    assert_eq!(status.snapshot.terminal_id, created.terminal_id);

    let mut attach_params = locator;
    attach_params["cursor"] = serde_json::json!(0);
    let (attach_response, attach_events) =
        call_collect(&mut client, "c-903", "terminal.attach", attach_params).await;
    assert!(
        attach_response.ok,
        "按会话键 attach 应成功: {attach_response:?}"
    );
    let result: AttachResult =
        serde_json::from_value(attach_response.result.expect("缺少 attach result"))
            .expect("AttachResult 解析失败");
    assert_eq!(result.snapshot.terminal_id, created.terminal_id);
    assert_eq!(result.cursor, 1);

    let attach_events = ensure_events(&mut client, attach_events, 2).await;
    assert!(matches!(attach_events[0].kind, EventKind::Started { .. }));
    assert!(matches!(attach_events[1].kind, EventKind::Attached { .. }));
}

/// F4：相同 hello.client 的两条连接使用不同连接级 client_id。
#[tokio::test]
async fn same_hello_client_label_yields_distinct_client_ids() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut first = connect(&service).await;
    handshake_as(&mut first, &service.token, "same-label").await;
    let created_response = call(
        &mut first,
        "c-911",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let (first_attach, first_events) = call_collect(
        &mut first,
        "c-912",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(first_attach.ok, "首次 attach 应成功: {first_attach:?}");
    let first_events = ensure_events(&mut first, first_events, 2).await;
    let first_client_id = match &first_events[1].kind {
        EventKind::Attached { client_id } => client_id.clone(),
        other => panic!("期望 attached 事件，实际 {other:?}"),
    };

    let mut second = connect(&service).await;
    handshake_as(&mut second, &service.token, "same-label").await;
    let (second_attach, second_events) = call_collect(
        &mut second,
        "c-913",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(second_attach.ok, "第二次 attach 应成功: {second_attach:?}");
    let second_events = ensure_events(&mut second, second_events, 3).await;
    let second_client_id = match &second_events[2].kind {
        EventKind::Attached { client_id } => client_id.clone(),
        other => panic!("期望 attached 事件，实际 {other:?}"),
    };
    assert_ne!(
        first_client_id, second_client_id,
        "hello.client 相同也必须分配不同的连接级 client_id"
    );

    // 第一次连接也能观察第二次连接附着的 attached 事件，且其 ID 与自身不同。
    let observed = next_event_matching(&mut first, |event| {
        matches!(event.kind, EventKind::Attached { .. })
    })
    .await;
    assert!(matches!(
        observed.kind,
        EventKind::Attached { ref client_id } if client_id == &second_client_id
    ));
}

/// F4：claim 冲突返回 permission_denied；force 抢占后旧连接收到 lease_revoked。
#[tokio::test]
async fn claim_conflict_and_force_revoke() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut first = connect(&service).await;
    handshake_as(&mut first, &service.token, "claimer-a").await;
    let created_response = call(
        &mut first,
        "c-921",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let claim_response = call(
        &mut first,
        "c-922",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(claim_response.ok, "首次 claim 应成功: {claim_response:?}");
    let first_claim: ClaimResult =
        serde_json::from_value(claim_response.result.expect("缺少 claim result"))
            .expect("ClaimResult 解析失败");

    let mut second = connect(&service).await;
    handshake_as(&mut second, &service.token, "claimer-b").await;

    let conflict_response = call(
        &mut second,
        "c-923",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(!conflict_response.ok, "被占用且无 force 应失败");
    assert_eq!(
        conflict_response.error.expect("缺少 error").code,
        ErrorCode::PermissionDenied
    );

    // 旧 owner 订阅后才能收到 lease_revoked。
    let (attach_response, attach_events) = call_collect(
        &mut first,
        "c-924",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach_response.ok, "attach 应成功: {attach_response:?}");
    ensure_events(&mut first, attach_events, 2).await;

    let force_response = call(
        &mut second,
        "c-925",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id, "force": true }),
    )
    .await;
    assert!(force_response.ok, "force claim 应成功: {force_response:?}");
    let second_claim: ClaimResult =
        serde_json::from_value(force_response.result.expect("缺少 claim result"))
            .expect("ClaimResult 解析失败");
    assert_ne!(first_claim.owner, second_claim.owner);

    let revoked = next_event_matching(&mut first, |event| {
        matches!(event.kind, EventKind::LeaseRevoked { .. })
    })
    .await;
    assert!(matches!(
        revoked.kind,
        EventKind::LeaseRevoked { ref client_id } if client_id == &first_claim.owner
    ));
}

/// F4：连接断开时释放其 lease，其他连接随后可重新 claim。
#[tokio::test]
async fn disconnect_releases_lease_for_reclaim() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut first = connect(&service).await;
    handshake_as(&mut first, &service.token, "leaver").await;
    let created_response = call(
        &mut first,
        "c-931",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let (attach_response, attach_events) = call_collect(
        &mut first,
        "c-932",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach_response.ok, "attach 应成功: {attach_response:?}");
    ensure_events(&mut first, attach_events, 2).await;

    let claim_response = call(
        &mut first,
        "c-933",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(claim_response.ok, "claim 应成功: {claim_response:?}");
    let first_claim: ClaimResult =
        serde_json::from_value(claim_response.result.expect("缺少 claim result"))
            .expect("ClaimResult 解析失败");

    let mut second = connect(&service).await;
    handshake_as(&mut second, &service.token, "reclaimer").await;
    let (subscribe_response, _) = call_collect(
        &mut second,
        "c-934",
        "terminal.subscribe",
        serde_json::json!({ "all": true }),
    )
    .await;
    assert!(
        subscribe_response.ok,
        "subscribe 应成功: {subscribe_response:?}"
    );

    // 主动关闭第一条连接，等待服务端完成 lease 释放与 detach_all。
    first.close(None).await.expect("关闭连接失败");
    drop(first);

    let detached = next_event_matching(&mut second, |event| {
        matches!(
            event.kind,
            EventKind::Detached { ref client_id, .. } if client_id == &first_claim.owner
        )
    })
    .await;
    assert_eq!(detached.terminal_id, created.terminal_id);

    // detached 已到达，说明清理完成；lease 应已释放并可被再次 claim。
    let reclaim_response = call(
        &mut second,
        "c-935",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(
        reclaim_response.ok,
        "断开后 lease 应可被重新 claim: {reclaim_response:?}"
    );
    let reclaim: ClaimResult =
        serde_json::from_value(reclaim_response.result.expect("缺少 claim result"))
            .expect("ClaimResult 解析失败");
    assert_ne!(reclaim.owner, first_claim.owner);
}

/// 复核项：Authorization Bearer 头优先于 hello.token。
#[tokio::test]
async fn authorization_bearer_header_takes_priority_over_hello_token() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut client = connect_with_bearer(&service, &service.token).await;
    send_text(
        &mut client,
        hello_request("c-1001", "definitely-wrong-token", PROTOCOL_VERSION),
    )
    .await;
    let response = next_response(&mut client).await;
    assert!(
        response.ok,
        "有效 Bearer 头应覆盖错误的 hello.token: {response:?}"
    );

    let mut client = connect_with_bearer(&service, "definitely-wrong-token").await;
    send_text(
        &mut client,
        hello_request("c-1002", &service.token, PROTOCOL_VERSION),
    )
    .await;
    let response = next_response(&mut client).await;
    assert!(!response.ok);
    assert_eq!(
        response.error.expect("缺少 error").code,
        ErrorCode::AuthFailed
    );
    expect_closed(&mut client).await;
}

/// 复核项：非环回监听地址在 bind 阶段被拒绝。
#[tokio::test]
async fn non_loopback_bind_is_rejected() {
    let dir = tempdir().expect("创建临时目录失败");
    let config = ServiceConfig::new(dir.path().to_path_buf())
        .with_bind_addr("0.0.0.0:0".parse().expect("地址字面量"));
    let error = match Service::bind(config).await {
        Ok(_) => panic!("非环回地址应拒绝"),
        Err(error) => error,
    };
    assert!(matches!(error, ServiceError::NonLoopbackBind(_)));
}

/// 复核项：超过单帧上限时回 bad_request 并断开。
#[tokio::test]
async fn oversized_frame_returns_bad_request_and_closes() {
    let dir = tempdir().expect("创建临时目录失败");
    let config = ServiceConfig::new(dir.path().to_path_buf()).with_max_frame_bytes(256);
    let service = TestService::start_with_config(config).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    send_text(&mut client, "x".repeat(1024)).await;
    let response = next_response(&mut client).await;
    assert!(!response.ok);
    assert_eq!(
        response.error.expect("缺少 error").code,
        ErrorCode::BadRequest
    );
    expect_closed(&mut client).await;
}

/// 复核项：subscribe 的 terminals 与 all 重叠时 cursor 去重且事件不重复。
#[tokio::test]
async fn subscribe_explicit_and_all_deduplicates_cursors_and_events() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let first_response = call(
        &mut client,
        "c-1101",
        "terminal.create",
        create_params("ses-1", "first", false),
    )
    .await;
    let first: CreateResult =
        serde_json::from_value(first_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");
    let second_response = call(
        &mut client,
        "c-1102",
        "terminal.create",
        create_params("ses-1", "second", false),
    )
    .await;
    let second: CreateResult =
        serde_json::from_value(second_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let (subscribe_response, subscribe_events) = call_collect(
        &mut client,
        "c-1103",
        "terminal.subscribe",
        serde_json::json!({
            "terminals": [{ "terminal_id": first.terminal_id }],
            "all": true,
        }),
    )
    .await;
    assert!(
        subscribe_response.ok,
        "subscribe 应成功: {subscribe_response:?}"
    );
    assert!(subscribe_events.is_empty());
    let result: SubscribeResult =
        serde_json::from_value(subscribe_response.result.expect("缺少 subscribe result"))
            .expect("SubscribeResult 解析失败");
    assert_eq!(result.cursors.len(), 2, "重叠终端应只出现一次");
    let mut ids: Vec<String> = result
        .cursors
        .iter()
        .map(|c| c.terminal_id.clone())
        .collect();
    ids.sort();
    let mut expected = vec![first.terminal_id.clone(), second.terminal_id.clone()];
    expected.sort();
    assert_eq!(ids, expected);

    assert_eq!(report_output(&service, &first.terminal_id, "YQ=="), 2);
    let output = next_event_matching(&mut client, |event| {
        matches!(event.kind, EventKind::Output { .. })
    })
    .await;
    assert_eq!(output.terminal_id, first.terminal_id);
    let extra = timeout(Duration::from_millis(300), client.next()).await;
    assert!(extra.is_err(), "同一客户端不应收到重复 output: {extra:?}");
}

/// 复核项：detach 后全局订阅排除该终端，attach 后恢复。
#[tokio::test]
async fn detach_excludes_terminal_from_global_subscription_until_attach() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut creator = connect(&service).await;
    handshake(&mut creator, &service.token).await;
    let created_response = call(
        &mut creator,
        "c-1201",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let mut observer = connect(&service).await;
    handshake_as(&mut observer, &service.token, "observer").await;
    let (subscribe_response, _) = call_collect(
        &mut observer,
        "c-1202",
        "terminal.subscribe",
        serde_json::json!({ "all": true }),
    )
    .await;
    assert!(
        subscribe_response.ok,
        "subscribe 应成功: {subscribe_response:?}"
    );

    let detach_response = call(
        &mut observer,
        "c-1203",
        "terminal.detach",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(detach_response.ok, "detach 应成功: {detach_response:?}");

    report_output(&service, &created.terminal_id, "YQ==");
    let extra = timeout(Duration::from_millis(300), observer.next()).await;
    assert!(extra.is_err(), "detach 后不应再收到全局事件: {extra:?}");

    let (attach_response, attach_events) = call_collect(
        &mut observer,
        "c-1204",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(attach_response.ok, "attach 应成功: {attach_response:?}");
    ensure_events(&mut observer, attach_events, 1).await;

    report_output(&service, &created.terminal_id, "Yg==");
    let output = next_event_matching(&mut observer, |event| {
        matches!(event.kind, EventKind::Output { .. })
    })
    .await;
    assert_eq!(output.terminal_id, created.terminal_id);
}

/// 复核项：owner 主动 release 也广播 lease_revoked。
#[tokio::test]
async fn release_emits_lease_revoked() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created_response = call(
        &mut client,
        "c-1301",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let (attach_response, attach_events) = call_collect(
        &mut client,
        "c-1302",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attach_response.ok, "attach 应成功: {attach_response:?}");
    ensure_events(&mut client, attach_events, 2).await;

    let claim_response = call(
        &mut client,
        "c-1303",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    let claim: ClaimResult =
        serde_json::from_value(claim_response.result.expect("缺少 claim result"))
            .expect("ClaimResult 解析失败");

    let release_response = call(
        &mut client,
        "c-1304",
        "terminal.release",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(release_response.ok, "release 应成功: {release_response:?}");

    let revoked = next_event_matching(&mut client, |event| {
        matches!(event.kind, EventKind::LeaseRevoked { .. })
    })
    .await;
    assert!(matches!(
        revoked.kind,
        EventKind::LeaseRevoked { ref client_id } if client_id == &claim.owner
    ));
}

/// 复核项：subscribe/unsubscribe 先校验全部目标，失败时不产生部分变更。
#[tokio::test]
async fn subscribe_and_unsubscribe_are_atomic_on_invalid_target() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created_response = call(
        &mut client,
        "c-1401",
        "terminal.create",
        create_params("ses-1", "build", false),
    )
    .await;
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let invalid = "01JNOTEXIST";
    let subscribe_response = call(
        &mut client,
        "c-1402",
        "terminal.subscribe",
        serde_json::json!({
            "terminals": [
                { "terminal_id": created.terminal_id },
                { "terminal_id": invalid },
            ],
        }),
    )
    .await;
    assert!(!subscribe_response.ok, "含非法目标应整体失败");
    assert_eq!(
        subscribe_response.error.expect("缺少 error").code,
        ErrorCode::NotFound
    );

    // 原子性：合法目标不应被注册。
    report_output(&service, &created.terminal_id, "YQ==");
    let extra = timeout(Duration::from_millis(300), client.next()).await;
    assert!(extra.is_err(), "失败后不应残留订阅: {extra:?}");

    let subscribe_response = call(
        &mut client,
        "c-1403",
        "terminal.subscribe",
        serde_json::json!({ "terminals": [{ "terminal_id": created.terminal_id }] }),
    )
    .await;
    assert!(
        subscribe_response.ok,
        "合法订阅应成功: {subscribe_response:?}"
    );

    let unsubscribe_response = call(
        &mut client,
        "c-1404",
        "terminal.unsubscribe",
        serde_json::json!({
            "terminals": [
                { "terminal_id": created.terminal_id },
                { "terminal_id": invalid },
            ],
        }),
    )
    .await;
    assert!(!unsubscribe_response.ok, "含非法目标应整体失败");
    assert_eq!(
        unsubscribe_response.error.expect("缺少 error").code,
        ErrorCode::NotFound
    );

    // 原子性：合法目标的订阅不应被移除。
    report_output(&service, &created.terminal_id, "Yg==");
    let output = next_event_matching(&mut client, |event| {
        matches!(event.kind, EventKind::Output { .. })
    })
    .await;
    assert_eq!(output.terminal_id, created.terminal_id);
}

/// M3b：慢订阅者状态事件队列满时，服务端回 `limit_exceeded` 并断开，
/// 而不是静默丢弃状态事件。
#[tokio::test]
async fn slow_subscriber_state_overflow_disconnects_with_limit_exceeded() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf()).with_subscriber_queue_capacity(1),
    )
    .await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created_response = call(
        &mut client,
        "c-1501",
        "terminal.create",
        create_params("ses-limit", "slow", false),
    )
    .await;
    assert!(created_response.ok, "create 应成功: {created_response:?}");
    let created: CreateResult =
        serde_json::from_value(created_response.result.expect("缺少 create result"))
            .expect("CreateResult 解析失败");

    let (attach_response, _) = call_collect(
        &mut client,
        "c-1502",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(attach_response.ok, "attach 应成功: {attach_response:?}");

    // 同一把 registry 锁内连续注入状态事件：队列容量 1，第二次必然投递失败。
    let terminal_id = created.terminal_id.clone();
    {
        let mut registry = service.registry.lock().expect("registry 锁被污染");
        for _ in 0..8 {
            registry
                .report_event(
                    &TerminalId::new(terminal_id.clone()),
                    EventKind::WaitingInput {
                        confidence: Confidence::Low,
                        reason: "no_output".to_string(),
                    },
                )
                .expect("状态事件应写入 ring");
        }
    }

    let mut saw_limit = false;
    let outcome = timeout(Duration::from_secs(5), async {
        loop {
            match client.next().await {
                Some(Ok(Message::Text(text))) => {
                    let value: serde_json::Value = from_json(text.as_str()).expect("帧 JSON 非法");
                    if value.get("type").is_some() {
                        continue;
                    }
                    let response: Response =
                        serde_json::from_value(value).expect("Response 解析失败");
                    if let Some(error) = &response.error {
                        // PROTOCOL §4：无法归属到具体请求的错误帧 id 必须为空。
                        assert!(
                            response.id.is_empty(),
                            "连接级错误帧 id 必须为空: {response:?}"
                        );
                        assert_eq!(error.code, ErrorCode::LimitExceeded);
                        assert!(error.retryable);
                        saw_limit = true;
                        break;
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;

    assert!(outcome.is_ok(), "应在超时内收到 limit_exceeded 或断开");
    assert!(
        saw_limit,
        "应收到 limit_exceeded 错误帧，而不是静默丢状态事件"
    );
    expect_closed(&mut client).await;
}

/// AC-16：运行中终端数量达到 `max_terminals` 上限后，create 返回
/// `limit_exceeded`（可重试）。
#[tokio::test]
async fn max_terminals_returns_limit_exceeded() {
    let dir = tempdir().expect("创建临时目录失败");
    let config = ServiceConfig::new(dir.path().to_path_buf()).with_max_terminals(Some(1));
    let service = TestService::start_with_config(config).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let first = call(
        &mut client,
        "c-1601",
        "terminal.create",
        create_params("ses-limit", "first", false),
    )
    .await;
    assert!(first.ok, "首个 create 应成功: {first:?}");

    let second = call(
        &mut client,
        "c-1602",
        "terminal.create",
        create_params("ses-limit", "second", false),
    )
    .await;
    assert!(!second.ok, "超过上限的 create 应失败: {second:?}");
    let error = second.error.expect("缺少 error");
    assert_eq!(error.code, ErrorCode::LimitExceeded);
    assert!(error.retryable, "limit_exceeded 应可重试");

    // 上限只约束运行中终端：force 替换同名终端仍允许。
    let forced = call(
        &mut client,
        "c-1603",
        "terminal.create",
        create_params("ses-limit", "first", true),
    )
    .await;
    assert!(forced.ok, "force 替换不应被上限拒绝: {forced:?}");
}
