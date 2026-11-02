//! termx-service 集成测试共享脚手架：启动真实后端服务、握手、收发帧。

#![allow(dead_code)]

use std::{
    fs,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use termx_core::Registry;
use termx_protocol::{CreateResult, Event, PROTOCOL_VERSION, Response, from_json};
use termx_service::{Service, ServiceConfig, ServiceError};
use tokio::{net::TcpStream, task::JoinHandle, time::timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

/// 测试用 WebSocket 客户端类型。
pub type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// 常规等待上限。
pub const WAIT: Duration = Duration::from_secs(5);

/// 终止后等待进程消失的上限。
pub const TERMINATE_WAIT: Duration = Duration::from_secs(10);

/// 测试守卫：drop 时停止 accept 循环。
pub struct TestService {
    /// 实际监听地址。
    pub addr: SocketAddr,
    /// token 文件内容。
    pub token: String,
    /// 与服务共享的 registry；测试可据此直接注入事件或检查订阅状态。
    pub registry: Arc<Mutex<Registry>>,
    /// `Service::run` 任务句柄。
    handle: JoinHandle<Result<(), ServiceError>>,
}

impl TestService {
    /// 用临时配置目录启动真实 PTY/ConPTY 后端服务。
    pub async fn start(config_dir: &Path) -> Self {
        Self::start_with_config(ServiceConfig::new(config_dir.to_path_buf())).await
    }

    /// 用自定义配置启动真实 PTY/ConPTY 后端服务。
    pub async fn start_with_config(config: ServiceConfig) -> Self {
        let service = Service::bind(config).await.expect("服务绑定失败");
        let addr = service.local_addr();
        let registry = service.registry();
        let token = fs::read_to_string(service.config().token_path())
            .expect("token 文件缺失")
            .trim()
            .to_string();
        let handle = tokio::spawn(service.run());
        Self {
            addr,
            token,
            registry,
            handle,
        }
    }

    /// WebSocket 连接地址（IPv4 / IPv6 都正确加括号）。
    pub fn url(&self) -> String {
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
pub async fn connect(service: &TestService) -> Client {
    let (client, _) = tokio_tungstenite::connect_async(service.url())
        .await
        .expect("连接测试服务失败");
    client
}

/// 完成 `hello` 握手。
pub async fn handshake(client: &mut Client, token: &str) {
    let request = serde_json::json!({
        "id": "h-001",
        "method": "hello",
        "params": {
            "proto": PROTOCOL_VERSION,
            "token": token,
            "client": "io-test",
        }
    })
    .to_string();
    client
        .send(Message::Text(request.into()))
        .await
        .expect("发送 hello 失败");
    let response = next_response(client).await;
    assert!(response.ok, "hello 应成功: {response:?}");
}

/// 读取并解析一个响应。
pub async fn next_response(client: &mut Client) -> Response {
    let message = timeout(WAIT, client.next())
        .await
        .expect("等待响应超时")
        .expect("连接已关闭")
        .expect("读取响应失败");
    from_json(message.to_text().expect("响应不是文本帧")).expect("响应 JSON 非法")
}

/// 下一帧：响应或事件。
pub enum Incoming {
    /// RPC 响应。
    Response(Response),
    /// 服务端推送事件。
    Event(Event),
}

/// 读取下一个帧并按 `id` 字段区分响应/事件。
pub async fn next_incoming(client: &mut Client) -> Incoming {
    let message = timeout(WAIT, client.next())
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
pub async fn call_collect(
    client: &mut Client,
    id: &str,
    method: &str,
    params: serde_json::Value,
) -> (Response, Vec<Event>) {
    let request = serde_json::json!({ "id": id, "method": method, "params": params }).to_string();
    client
        .send(Message::Text(request.into()))
        .await
        .expect("发送请求失败");

    let mut events = Vec::new();
    loop {
        match next_incoming(client).await {
            Incoming::Event(event) => events.push(event),
            Incoming::Response(response) if response.id == id => return (response, events),
            Incoming::Response(other) => panic!("收到非预期响应: {other:?}"),
        }
    }
}

/// 发送请求并等待响应（忽略中途事件）。
pub async fn call(
    client: &mut Client,
    id: &str,
    method: &str,
    params: serde_json::Value,
) -> Response {
    call_collect(client, id, method, params).await.0
}

/// 等待下一个事件。
pub async fn next_event(client: &mut Client) -> Event {
    match next_incoming(client).await {
        Incoming::Event(event) => event,
        Incoming::Response(response) => panic!("本应收到事件，却收到响应: {response:?}"),
    }
}

/// 等待下一个满足条件的事件，跳过不相关事件。
pub async fn next_event_matching<F>(client: &mut Client, mut predicate: F) -> Event
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

/// 创建终端并返回 `CreateResult`。
pub async fn create_terminal(
    client: &mut Client,
    id: &str,
    params: serde_json::Value,
) -> CreateResult {
    let response = call(client, id, "terminal.create", params).await;
    assert!(response.ok, "terminal.create 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 create result"))
        .expect("CreateResult 解析失败")
}
