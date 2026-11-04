//! M2c1 集成测试：`terminal.create` 走真实 PTY/ConPTY 后端，输出与退出事件
//! 经 registry 推送给客户端，`terminal.status` 反映终态。
//!
//! Windows 用 ConPTY + `cmd.exe`，Unix 用 `openpty` + `/bin/sh`；两者都验证
//! `echo hello_termx` 的输出与 `exit 7` 的退出码。

#![cfg(any(windows, unix))]

use std::{fs, net::SocketAddr, path::Path, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use tempfile::tempdir;
use termx_protocol::{
    AttachResult, CreateResult, Event, EventKind, LifecycleState, PROTOCOL_VERSION, ReadResult,
    ResetReason, Response, StatusResult, from_json,
};
use termx_service::{Service, ServiceConfig, ServiceError};
use tokio::{
    net::TcpStream,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

/// 测试用 WebSocket 客户端类型。
type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// 测试守卫：drop 时停止 accept 循环。
struct TestService {
    /// 实际监听地址。
    addr: SocketAddr,
    /// token 文件内容。
    token: String,
    /// `Service::run` 任务句柄。
    handle: JoinHandle<Result<(), ServiceError>>,
}

impl TestService {
    /// 用临时配置目录启动真实后端服务。
    async fn start(config_dir: &Path) -> Self {
        Self::start_with_config(ServiceConfig::new(config_dir.to_path_buf())).await
    }

    /// 用自定义配置启动真实后端服务。
    async fn start_with_config(config: ServiceConfig) -> Self {
        let service = Service::bind(config).await.expect("服务绑定失败");
        let addr = service.local_addr();
        let token = fs::read_to_string(service.config().token_path())
            .expect("token 文件缺失")
            .trim()
            .to_string();
        let handle = tokio::spawn(service.run());
        Self {
            addr,
            token,
            handle,
        }
    }

    /// WebSocket 连接地址。
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
    let (client, _) = tokio_tungstenite::connect_async(service.url())
        .await
        .expect("连接测试服务失败");
    client
}

/// 发送文本帧。
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
    from_json(message.to_text().expect("响应不是文本帧")).expect("响应 JSON 非法")
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
    let value: serde_json::Value =
        from_json(message.to_text().expect("帧不是文本")).expect("帧 JSON 非法");
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
    let request = serde_json::json!({ "id": id, "method": method, "params": params }).to_string();
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

/// 等待下一个事件。
async fn next_event(client: &mut Client) -> Event {
    match next_incoming(client).await {
        Incoming::Event(event) => event,
        Incoming::Response(response) => panic!("本应收到事件，却收到响应: {response:?}"),
    }
}

/// 完成 `hello` 握手。
async fn handshake(client: &mut Client, token: &str) {
    let request = serde_json::json!({
        "id": "h-001",
        "method": "hello",
        "params": {
            "proto": PROTOCOL_VERSION,
            "token": token,
            "client": "pty-test",
            "root_session_id": "ses-pty",
        }
    })
    .to_string();
    send_text(client, request).await;
    let response = next_response(client).await;
    assert!(response.ok, "hello 应成功: {response:?}");
}

/// 平台相关的真实命令：输出 `hello_termx` 后以退出码 7 结束。
#[cfg(windows)]
fn create_params() -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "cmd.exe",
        "args": ["/C", "echo hello_termx & exit 7"],
        "root_session_id": "ses-pty",
        "name": "build",
    })
}

/// 平台相关的真实命令：输出 `hello_termx` 后以退出码 7 结束。
#[cfg(unix)]
fn create_params() -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "/bin/sh",
        "args": ["-c", "echo hello_termx; exit 7"],
        "root_session_id": "ses-pty",
        "name": "build",
    })
}

/// 消费事件直到终态退出，返回退出码并累积输出字节。
async fn collect_until_exit(
    client: &mut Client,
    mut events: Vec<Event>,
    output: &mut Vec<u8>,
) -> i32 {
    loop {
        let event = if events.is_empty() {
            next_event(client).await
        } else {
            events.remove(0)
        };
        match event.kind {
            EventKind::Output { data, .. } => {
                output.extend(STANDARD.decode(data).expect("output 不是合法 base64"));
            }
            EventKind::Exited { exit_code } => return exit_code,
            EventKind::Interrupted { signal, .. } => panic!("进程被信号终止: {signal}"),
            _ => {}
        }
    }
}

/// 真实 PTY：输出含 hello_termx、退出码为 7、status 落 `exited`。
#[tokio::test]
async fn pty_create_streams_output_and_exit_and_status_is_terminal() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let (created, events) =
        call_collect(&mut client, "c-001", "terminal.create", create_params()).await;
    assert!(created.ok, "terminal.create 应成功: {created:?}");
    assert!(events.is_empty(), "create 不应推送事件");
    let created: CreateResult = serde_json::from_value(created.result.expect("缺少 create result"))
        .expect("CreateResult 解析失败");
    assert_eq!(created.cursor, 0);

    let (attached, attach_events) = call_collect(
        &mut client,
        "c-002",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attached.ok, "terminal.attach 应成功: {attached:?}");
    let _: AttachResult = serde_json::from_value(attached.result.expect("缺少 attach result"))
        .expect("AttachResult 解析失败");

    let mut output = Vec::new();
    let exit_code = collect_until_exit(&mut client, attach_events, &mut output).await;
    assert_eq!(exit_code, 7, "退出码应为 7");
    let text = String::from_utf8_lossy(&output);
    assert!(
        text.contains("hello_termx"),
        "输出应包含 hello_termx，实际: {text:?}"
    );

    let status_response = call_collect(
        &mut client,
        "c-003",
        "terminal.status",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await
    .0;
    assert!(
        status_response.ok,
        "terminal.status 应成功: {status_response:?}"
    );
    let status: StatusResult =
        serde_json::from_value(status_response.result.expect("缺少 status result"))
            .expect("StatusResult 解析失败");
    assert_eq!(status.snapshot.state, LifecycleState::Exited);
    assert!(status.snapshot.pid.is_some());
}

/// 读取 `terminal.read` 结果。
async fn read_terminal(
    client: &mut Client,
    id: &str,
    terminal_id: &str,
    cursor: u64,
    max_bytes: Option<u64>,
) -> ReadResult {
    let mut params = serde_json::json!({ "terminal_id": terminal_id, "cursor": cursor });
    if let Some(max_bytes) = max_bytes {
        params["max_bytes"] = serde_json::json!(max_bytes);
    }
    let (response, _) = call_collect(client, id, "terminal.read", params).await;
    assert!(response.ok, "terminal.read 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 read result")).expect("ReadResult 解析失败")
}

/// 读取 `terminal.status` 快照。
async fn status_of(client: &mut Client, id: &str, terminal_id: &str) -> StatusResult {
    let (response, _) = call_collect(
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

/// M3b：真实 PTY 退出后，`terminal.read` 能回放输出并可增量读取。
#[tokio::test]
async fn terminal_read_returns_output_after_exit_and_is_incremental() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let (created, events) = call_collect(
        &mut client,
        "c-read-create",
        "terminal.create",
        create_params(),
    )
    .await;
    assert!(created.ok, "terminal.create 应成功: {created:?}");
    let created: CreateResult = serde_json::from_value(created.result.expect("缺少 create result"))
        .expect("CreateResult 解析失败");

    let (attached, attach_events) = call_collect(
        &mut client,
        "c-read-attach",
        "terminal.attach",
        serde_json::json!({ "terminal_id": created.terminal_id, "cursor": 0 }),
    )
    .await;
    assert!(attached.ok, "attach 应成功: {attached:?}");
    let mut output = Vec::new();
    let exit_code = collect_until_exit(&mut client, attach_events, &mut output).await;
    assert_eq!(exit_code, 7);
    assert!(events.is_empty());

    let read = read_terminal(&mut client, "c-read-1", &created.terminal_id, 0, None).await;
    assert!(read.reset.is_none(), "默认缓冲不应 reset: {read:?}");
    let mut text = String::new();
    for chunk in &read.chunks {
        text.push_str(&String::from_utf8_lossy(
            &STANDARD.decode(&chunk.data).expect("chunk 不是合法 base64"),
        ));
    }
    assert!(text.contains("hello_termx"), "read 输出缺少标记: {text:?}");
    let status = status_of(&mut client, "c-read-status", &created.terminal_id).await;
    assert_eq!(
        read.cursor, status.snapshot.cursor,
        "read 游标应推进到当前最后 seq"
    );

    let next = read_terminal(
        &mut client,
        "c-read-2",
        &created.terminal_id,
        read.cursor,
        None,
    )
    .await;
    assert!(next.chunks.is_empty(), "增量读取不应重复返回旧输出");
    assert_eq!(next.cursor, read.cursor);
    assert!(next.reset.is_none());
}

/// M3b：小事件缓冲下输出被淘汰，read 返回 too_old + 基线；状态事件仍可读。
#[tokio::test]
async fn terminal_read_resets_when_output_buffer_overflowed() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf()).with_event_buffer_capacity(2),
    )
    .await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = call_collect(
        &mut client,
        "c-small-create",
        "terminal.create",
        create_params(),
    )
    .await
    .0;
    assert!(created.ok, "terminal.create 应成功: {created:?}");
    let created: CreateResult = serde_json::from_value(created.result.expect("缺少 create result"))
        .expect("CreateResult 解析失败");

    // 不 attach，轮询 status 直到进程退出（status 是只读接口）。
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let snapshot = loop {
        let snapshot = status_of(&mut client, "c-small-status", &created.terminal_id).await;
        if snapshot.snapshot.state == LifecycleState::Exited {
            break snapshot;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "等待进程退出超时: {snapshot:?}"
        );
        sleep(Duration::from_millis(20)).await;
    };

    let read = read_terminal(&mut client, "c-small-read", &created.terminal_id, 0, None).await;
    assert_eq!(read.reset, Some(ResetReason::TooOld));
    assert!(read.chunks.is_empty(), "reset 时不返回残缺输出");
    assert_eq!(read.cursor, snapshot.snapshot.cursor, "reset 返回基线游标");

    // 状态事件未被淘汰：终态仍可读，exited 也在 ring 中（status 即快照来源）。
    let after = status_of(&mut client, "c-small-status-2", &created.terminal_id).await;
    assert_eq!(after.snapshot.state, LifecycleState::Exited);
    assert_eq!(after.snapshot.cursor, snapshot.snapshot.cursor);
}
