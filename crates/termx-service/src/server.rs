//! 环回 WebSocket 服务、连接级鉴权状态机与 `hello` 握手。

use std::{
    collections::BTreeSet,
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use futures_util::{Sink, SinkExt, StreamExt, stream::SplitStream};
use termx_core::{CoreError, DETACH_REASON_DISCONNECT, Registry, TerminalId};
use termx_protocol::{
    AttachParams, Capabilities, ClaimParams, CreateParams, DetachParams, ErrorCode, Event,
    HELPER_PROTOCOL_VERSION, HelloParams, HelloResult, ListParams, PROTOCOL_VERSION, ProtocolError,
    ReleaseParams, Request, Response, SimpleResult, StatusParams, StatusResult, SubscribeParams,
    SubscribeResult, SubscriptionCursor, TerminalLocator, UnsubscribeParams, from_json, to_json,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{
            ErrorResponse as WsErrorResponse, Request as WsRequest, Response as WsResponse,
        },
    },
};
use ulid::Ulid;

use crate::config::{ServiceConfig, load_or_create_token, write_port_file};

/// 服务端标识，随包版本变化。
const SERVER_ID: &str = concat!("termx-service/", env!("CARGO_PKG_VERSION"));
/// 等待首条 `hello` 的超时。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// 鉴权头名。
const AUTHORIZATION_HEADER: &str = "authorization";
/// Bearer 鉴权方案前缀。
const BEARER_PREFIX: &str = "bearer";

/// 服务启动或运行期错误。
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// 配置目录解析失败。
    #[error("配置错误: {0}")]
    Config(#[from] crate::ConfigError),
    /// 文件或网络 I/O 失败。
    #[error("文件或网络 I/O 失败: {0}")]
    Io(#[from] std::io::Error),
    /// WebSocket 握手/收发失败。
    #[error("WebSocket 错误: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    /// 协议 JSON 编解码失败。
    #[error("协议编解码失败: {0}")]
    Codec(#[from] termx_protocol::CodecError),
    /// JSON 值构造失败。
    #[error("JSON 序列化失败: {0}")]
    Json(#[from] serde_json::Error),
    /// 配置要求绑定非环回地址。
    #[error("拒绝绑定非环回地址: {0}")]
    NonLoopbackBind(SocketAddr),
}

/// 已绑定监听端口的服务实例。
pub struct Service {
    listener: TcpListener,
    local_addr: SocketAddr,
    config: ServiceConfig,
    token: Arc<str>,
    /// 多客户端共享的终端 registry；std Mutex 临界区不跨 `await`。
    registry: Arc<Mutex<Registry>>,
}

impl Service {
    /// 绑定 [`ServiceConfig::bind_addr`] 并写出实际端口。
    ///
    /// 仅允许环回监听；配置目录、token 文件会在绑定前准备好。
    pub async fn bind(config: ServiceConfig) -> Result<Self, ServiceError> {
        if !is_loopback(config.bind_addr) {
            return Err(ServiceError::NonLoopbackBind(config.bind_addr));
        }

        // 端口文件始终写到 config_dir；即使 token 走环境变量或自定义文件也要保证目录存在。
        std::fs::create_dir_all(&config.config_dir)?;
        let token = load_or_create_token(&config)?;
        let listener = TcpListener::bind(config.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        write_port_file(&config, local_addr.port())?;
        let registry = Arc::new(Mutex::new(Registry::new(config.event_buffer_capacity)));

        tracing::info!(
            addr = %local_addr,
            config_dir = %config.config_dir.display(),
            "termx-service 已绑定环回地址"
        );

        Ok(Self {
            listener,
            local_addr,
            config,
            token: Arc::from(token),
            registry,
        })
    }

    /// 实际监听地址（包含由 OS 分配的端口）。
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 服务使用的配置。
    pub fn config(&self) -> &ServiceConfig {
        &self.config
    }

    /// 共享 registry 句柄。
    ///
    /// 供测试直接注入事件，也供后续内置事件源（PTY 读线程、helper）复用；
    /// 不通过 RPC 暴露，避免客户端伪造任意事件。
    pub fn registry(&self) -> Arc<Mutex<Registry>> {
        Arc::clone(&self.registry)
    }

    /// 持续 accept，直到监听器出错。
    ///
    /// 测试可用 `tokio::spawn(service.run())` 后 abort 任务；需要可控退出时用
    /// [`Service::run_until`]。
    pub async fn run(self) -> Result<(), ServiceError> {
        self.run_until(std::future::pending::<()>()).await
    }

    /// 持续 accept，直到 `shutdown` 完成。
    pub async fn run_until<F>(self, shutdown: F) -> Result<(), ServiceError>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!(addr = %self.local_addr, "termx-service 停止接收新连接");
                    return Ok(());
                }
                accepted = self.listener.accept() => {
                    let (stream, peer_addr) = accepted?;
                    if !is_loopback(peer_addr) {
                        tracing::warn!(peer = %peer_addr, "拒绝非环回连接");
                        continue;
                    }

                    let token = Arc::clone(&self.token);
                    let registry = Arc::clone(&self.registry);
                    let max_frame_bytes = self.config.max_frame_bytes;
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_connection(stream, token, registry, max_frame_bytes).await
                        {
                            tracing::debug!(peer = %peer_addr, %error, "连接处理结束");
                        }
                    });
                }
            }
        }
    }
}

/// 处理单个客户端连接：完成 `hello` 鉴权，成功则保持连接。
async fn handle_connection(
    stream: TcpStream,
    token: Arc<str>,
    registry: Arc<Mutex<Registry>>,
    max_frame_bytes: usize,
) -> Result<(), ServiceError> {
    let peer_addr = stream.peer_addr().ok();
    let (mut ws, auth_header) = accept_with_auth_header(stream).await?;

    let Some(first) = read_first_message(&mut ws, peer_addr).await else {
        return Ok(());
    };

    if frame_len(&first) > max_frame_bytes {
        let response = error_response(String::new(), ErrorCode::BadRequest, "帧超过上限");
        send_response(&mut ws, &response).await?;
        let _ = ws.close(None).await;
        return Ok(());
    }

    match evaluate_handshake(&first, &token, auth_header.as_deref()) {
        Handshake::Accepted { id, client } => {
            handle_accepted(ws, peer_addr, id, client, registry, max_frame_bytes).await
        }
        Handshake::Rejected { id, code, message } => {
            handle_rejected(&mut ws, peer_addr, id, code, message).await
        }
    }
}

/// 在 WebSocket 握手回调中捕获 `Authorization` 头。
///
/// 回调签名由 tungstenite 固定，`Err` 变体较大但只在校验失败时构造。
#[allow(clippy::result_large_err)]
async fn accept_with_auth_header(
    stream: TcpStream,
) -> Result<(WebSocketStream<TcpStream>, Option<String>), ServiceError> {
    let slot = Arc::new(Mutex::new(None));
    let capture = Arc::clone(&slot);
    let ws = accept_hdr_async(
        stream,
        move |request: &WsRequest, response: WsResponse| -> Result<WsResponse, WsErrorResponse> {
            if let Some(value) = request.headers().get(AUTHORIZATION_HEADER) {
                if let Ok(value) = value.to_str() {
                    *capture
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(value.to_string());
                }
            }
            Ok(response)
        },
    )
    .await?;
    let auth_header = slot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    Ok((ws, auth_header))
}

/// 单个帧的字节数；控制帧不计入业务帧上限。
fn frame_len(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.len(),
        Message::Binary(bytes) => bytes.len(),
        _ => 0,
    }
}

/// 读取首条消息；超时/断开时已记录原因并关闭连接，返回 `None`。
async fn read_first_message<S>(
    ws: &mut WebSocketStream<S>,
    peer_addr: Option<SocketAddr>,
) -> Option<Message>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match timeout(HANDSHAKE_TIMEOUT, ws.next()).await {
        Ok(Some(Ok(message))) => Some(message),
        Ok(Some(Err(error))) => {
            tracing::debug!(?peer_addr, %error, "读取首帧失败");
            None
        }
        Ok(None) => {
            tracing::debug!(?peer_addr, "客户端在 hello 前断开");
            None
        }
        Err(_) => {
            tracing::warn!(?peer_addr, "等待 hello 超时");
            let _ = ws.close(None).await;
            None
        }
    }
}

/// 鉴权通过：回 `hello` 结果并保持连接。
async fn handle_accepted<S>(
    mut ws: WebSocketStream<S>,
    peer_addr: Option<SocketAddr>,
    id: String,
    client: String,
    registry: Arc<Mutex<Registry>>,
    max_frame_bytes: usize,
) -> Result<(), ServiceError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // 每连接生成唯一的连接级 client_id；`hello.client` 只作为日志标签，
    // 不参与订阅键、attached/detached 事件或 lease 归属，避免同名连接撞键。
    let client_id = Ulid::generate().to_string();
    tracing::info!(
        ?peer_addr,
        client = %sanitize_log_value(&client),
        %client_id,
        "hello 鉴权成功"
    );
    let response = hello_response(id)?;
    send_response(&mut ws, &response).await?;
    serve_connection(ws, peer_addr, client_id, registry, max_frame_bytes).await
}

/// 鉴权失败：回错误响应后主动关闭连接。
async fn handle_rejected<S>(
    ws: &mut WebSocketStream<S>,
    peer_addr: Option<SocketAddr>,
    id: String,
    code: ErrorCode,
    message: &'static str,
) -> Result<(), ServiceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tracing::warn!(?peer_addr, code = ?code, "hello 握手失败");
    let response = error_response(id, code, message);
    send_response(ws, &response).await?;
    let _ = ws.close(None).await;
    Ok(())
}

/// 构造 `hello` 成功响应。
fn hello_response(id: String) -> Result<Response, ServiceError> {
    let result = HelloResult {
        proto: PROTOCOL_VERSION.to_string(),
        server: SERVER_ID.to_string(),
        capabilities: Capabilities {
            helper: false,
            version: HELPER_PROTOCOL_VERSION.to_string(),
            best_effort: false,
        },
    };

    Ok(Response {
        id,
        ok: true,
        result: Some(serde_json::to_value(result)?),
        error: None,
    })
}

/// `hello` 成功后保持连接：读写拆分为 writer / reader，事件经独立通道转发。
///
/// 读循环只负责请求→响应；事件由 core 投递到 `event_tx`，forward task 序列化为
/// WS text frame 后写入同一 out 队列，因此慢事件不会阻塞请求处理。
async fn serve_connection<S>(
    ws: WebSocketStream<S>,
    peer_addr: Option<SocketAddr>,
    client_id: String,
    registry: Arc<Mutex<Registry>>,
    max_frame_bytes: usize,
) -> Result<(), ServiceError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (sink, mut stream) = ws.split();
    let (out_tx, out_rx) = unbounded_channel::<Message>();
    let mut writer = tokio::spawn(write_loop(sink, out_rx));

    let (event_tx, event_rx) = unbounded_channel::<Event>();
    let forwarder = tokio::spawn(forward_events(event_rx, out_tx.clone()));

    let result = read_loop(
        &mut stream,
        peer_addr,
        &client_id,
        &registry,
        &out_tx,
        &event_tx,
        max_frame_bytes,
    )
    .await;

    // 连接结束：先释放该连接持有的 lease（避免终端被永久锁死），再清理订阅，
    // 防止 registry 持有失效 sender。
    {
        let mut registry = lock_registry(&registry);
        registry.release_all(&client_id);
        if let Err(error) = registry.detach_all(&client_id, DETACH_REASON_DISCONNECT) {
            tracing::debug!(?peer_addr, %error, "清理订阅失败");
        }
    }
    drop(event_tx);
    let _ = out_tx.send(Message::Close(None));
    drop(out_tx);

    let _ = (&mut writer).await;
    let _ = forwarder.await;
    tracing::debug!(?peer_addr, "客户端连接结束");
    result
}

/// writer task：串行消费 out 队列并写出，保证帧顺序。
async fn write_loop<W>(mut sink: W, mut out_rx: UnboundedReceiver<Message>)
where
    W: Sink<Message> + Unpin,
{
    while let Some(message) = out_rx.recv().await {
        if sink.send(message).await.is_err() {
            break;
        }
    }
    let _ = sink.close().await;
}

/// forward task：core 事件 → 协议 JSON → out 队列。
async fn forward_events(mut event_rx: UnboundedReceiver<Event>, out_tx: UnboundedSender<Message>) {
    while let Some(event) = event_rx.recv().await {
        match to_json(&event) {
            Ok(text) => {
                if out_tx.send(Message::Text(text.into())).is_err() {
                    break;
                }
            }
            Err(error) => tracing::warn!(%error, "事件序列化失败"),
        }
    }
}

/// 读循环：处理请求并交给同一 out 队列写回响应。
async fn read_loop<S>(
    stream: &mut SplitStream<WebSocketStream<S>>,
    peer_addr: Option<SocketAddr>,
    client_id: &str,
    registry: &Mutex<Registry>,
    out_tx: &UnboundedSender<Message>,
    event_tx: &UnboundedSender<Event>,
    max_frame_bytes: usize,
) -> Result<(), ServiceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(message) = stream.next().await {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                tracing::debug!(?peer_addr, %error, "连接读取失败");
                return Ok(());
            }
        };

        if frame_len(&message) > max_frame_bytes {
            tracing::warn!(?peer_addr, %max_frame_bytes, "入站帧超过上限，断开连接");
            let response = error_response(String::new(), ErrorCode::BadRequest, "帧超过上限");
            let text = to_json(&response)?;
            let _ = out_tx.send(Message::Text(text.into()));
            return Ok(());
        }

        match message {
            Message::Text(text) => {
                handle_rpc_text(text.as_str(), client_id, registry, out_tx, event_tx)?
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

/// 解析请求并分发；非法 JSON 回 `protocol_error`。
fn handle_rpc_text(
    text: &str,
    client_id: &str,
    registry: &Mutex<Registry>,
    out_tx: &UnboundedSender<Message>,
    event_tx: &UnboundedSender<Event>,
) -> Result<(), ServiceError> {
    let response = match from_json::<Request>(text) {
        Ok(request) => dispatch_request(request, client_id, registry, event_tx)?,
        Err(_) => error_response(String::new(), ErrorCode::ProtocolError, "请求 JSON 非法"),
    };

    let text = to_json(&response)?;
    let _ = out_tx.send(Message::Text(text.into()));
    Ok(())
}

/// 分发 `terminal.*` RPC；attach/subscribe 使用连接级 `client_id` 与事件通道。
///
/// 锁内只做 registry 读写（事件投递为非阻塞 send），不跨 `await` 持有锁。
fn dispatch_request(
    request: Request,
    client_id: &str,
    registry: &Mutex<Registry>,
    event_tx: &UnboundedSender<Event>,
) -> Result<Response, ServiceError> {
    let Request { id, method, params } = request;

    match method.as_str() {
        "terminal.create" => {
            let params: CreateParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.create 参数非法: {error}"),
                    ));
                }
            };

            let mut registry = lock_registry(registry);
            match registry.create(params) {
                Ok(result) => {
                    let value = serde_json::to_value(result)?;
                    Ok(ok_response(id, value))
                }
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.list" => {
            let params: ListParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.list 参数非法: {error}"),
                    ));
                }
            };

            let registry = lock_registry(registry);
            let value = serde_json::to_value(registry.list(&params))?;
            Ok(ok_response(id, value))
        }
        "terminal.status" => {
            let params: StatusParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.status 参数非法: {error}"),
                    ));
                }
            };

            let registry = lock_registry(registry);
            match registry.status(&params.locator) {
                Ok(snapshot) => {
                    let value = serde_json::to_value(StatusResult { snapshot })?;
                    Ok(ok_response(id, value))
                }
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.attach" => {
            let params: AttachParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.attach 参数非法: {error}"),
                    ));
                }
            };
            let mut registry = lock_registry(registry);
            match registry.attach(&params.locator, client_id, params.cursor, event_tx.clone()) {
                Ok(result) => Ok(ok_response(id, serde_json::to_value(result)?)),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.detach" => {
            let params: DetachParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.detach 参数非法: {error}"),
                    ));
                }
            };

            let mut registry = lock_registry(registry);
            match detach_terminal(&mut registry, &params.locator, client_id) {
                Ok(()) => Ok(ok_response(
                    id,
                    serde_json::to_value(SimpleResult { ok: true })?,
                )),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.claim" => {
            let params: ClaimParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.claim 参数非法: {error}"),
                    ));
                }
            };

            let mut registry = lock_registry(registry);
            match registry.claim(&params.locator, client_id, params.force) {
                Ok(result) => Ok(ok_response(id, serde_json::to_value(result)?)),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.release" => {
            let params: ReleaseParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.release 参数非法: {error}"),
                    ));
                }
            };

            let mut registry = lock_registry(registry);
            match registry.release(&params.locator, client_id) {
                Ok(()) => Ok(ok_response(
                    id,
                    serde_json::to_value(SimpleResult { ok: true })?,
                )),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.subscribe" | "subscribe" => {
            let params: SubscribeParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.subscribe 参数非法: {error}"),
                    ));
                }
            };
            if params.terminals.is_none() && params.all != Some(true) {
                return Ok(error_response(
                    id,
                    ErrorCode::BadRequest,
                    "terminal.subscribe 需要 terminals 或 all",
                ));
            }

            let mut registry = lock_registry(registry);
            match subscribe_terminals(&mut registry, params, client_id, event_tx) {
                Ok(cursors) => Ok(ok_response(
                    id,
                    serde_json::to_value(SubscribeResult { cursors })?,
                )),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.unsubscribe" | "unsubscribe" => {
            let params: UnsubscribeParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.unsubscribe 参数非法: {error}"),
                    ));
                }
            };

            let mut registry = lock_registry(registry);
            match unsubscribe_terminals(&mut registry, params, client_id) {
                Ok(()) => Ok(ok_response(
                    id,
                    serde_json::to_value(SimpleResult { ok: true })?,
                )),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        _ => Ok(error_response(
            id,
            ErrorCode::BadRequest,
            format!("方法尚未实现: {method}"),
        )),
    }
}

/// 按定位器 detach；定位失败直接返回 core 错误。
fn detach_terminal(
    registry: &mut Registry,
    locator: &TerminalLocator,
    client_id: &str,
) -> Result<(), CoreError> {
    registry.detach(locator, client_id)
}

/// 订阅指定终端；`all=true` 时同时注册全局订阅。
///
/// 两阶段执行：先解析并校验全部目标，任一失败则直接返回、不注册任何订阅、
/// 不回放任何事件；结果按 `terminal_id` 去重（`terminals` 与 `all` 重叠时只保留
/// 显式目标的那条 cursor）。
fn subscribe_terminals(
    registry: &mut Registry,
    params: SubscribeParams,
    client_id: &str,
    event_tx: &UnboundedSender<Event>,
) -> Result<Vec<SubscriptionCursor>, CoreError> {
    let mut explicit = Vec::new();
    let mut seen_targets = BTreeSet::new();
    if let Some(targets) = params.terminals {
        for target in targets {
            let locator = TerminalLocator {
                terminal_id: Some(target.terminal_id),
                root_session_id: None,
                name: None,
            };
            let id = registry.resolve(&locator)?;
            if seen_targets.insert(id.clone()) {
                explicit.push((id, target.cursor));
            }
        }
    }

    // 校验阶段已全部通过，且持有同一把 registry 锁，后续注册不会失败。
    let mut cursors = Vec::new();
    let mut seen_cursors = BTreeSet::new();
    for (id, cursor) in explicit {
        let locator = TerminalLocator {
            terminal_id: Some(id.as_str().to_string()),
            root_session_id: None,
            name: None,
        };
        let result = registry.subscribe(&locator, client_id, cursor, event_tx.clone())?;
        if seen_cursors.insert(result.terminal_id.clone()) {
            cursors.push(result);
        }
    }
    if params.all == Some(true) {
        for cursor in registry.subscribe_all(client_id, event_tx.clone()) {
            if seen_cursors.insert(cursor.terminal_id.clone()) {
                cursors.push(cursor);
            }
        }
    }
    Ok(cursors)
}

/// 取消订阅；未给 terminals 时移除该客户端全部订阅（含全局）。
///
/// 同样先解析校验全部目标，任一失败则整体失败且不改变任何订阅。
fn unsubscribe_terminals(
    registry: &mut Registry,
    params: UnsubscribeParams,
    client_id: &str,
) -> Result<(), CoreError> {
    match params.terminals {
        Some(locators) => {
            let mut ids: Vec<TerminalId> = Vec::new();
            let mut seen = BTreeSet::new();
            for locator in &locators {
                let id = registry.resolve(locator)?;
                if seen.insert(id.clone()) {
                    ids.push(id);
                }
            }
            for id in ids {
                registry.unsubscribe(&id, client_id)?;
            }
        }
        None => registry.unsubscribe_all(client_id),
    }
    Ok(())
}

/// 获取 registry 锁；锁中毒时取回内部值继续（registry 不持有跨调用状态）。
fn lock_registry(registry: &Mutex<Registry>) -> MutexGuard<'_, Registry> {
    registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 构造成功响应。
fn ok_response(id: String, result: serde_json::Value) -> Response {
    Response {
        id,
        ok: true,
        result: Some(result),
        error: None,
    }
}

/// 把 core 错误映射为协议错误响应。
fn response_from_core_error(id: String, error: CoreError) -> Response {
    Response {
        id,
        ok: false,
        result: None,
        error: Some(protocol_error_from_core(error)),
    }
}

/// core 错误 → 协议错误码；`session_exists` 携带旧终端 ID。
fn protocol_error_from_core(error: CoreError) -> ProtocolError {
    let code = match &error {
        CoreError::SessionExists { .. } => ErrorCode::SessionExists,
        CoreError::InvalidName | CoreError::InvalidIdleTimeout(_) | CoreError::BadRequest(_) => {
            ErrorCode::BadRequest
        }
        CoreError::NotFound => ErrorCode::NotFound,
        CoreError::AlreadyTerminal => ErrorCode::AlreadyTerminal,
        CoreError::PermissionDenied => ErrorCode::PermissionDenied,
    };
    let terminal_id = match &error {
        CoreError::SessionExists { terminal_id } => Some(terminal_id.clone()),
        _ => None,
    };

    ProtocolError {
        code,
        message: error.to_string(),
        retryable: code.is_retryable(),
        terminal_id,
    }
}

/// 发送一个协议响应。
async fn send_response<S>(
    ws: &mut WebSocketStream<S>,
    response: &Response,
) -> Result<(), ServiceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let text = to_json(response)?;
    ws.send(Message::Text(text.into())).await?;
    Ok(())
}

/// 构造失败响应。
fn error_response(id: String, code: ErrorCode, message: impl Into<String>) -> Response {
    Response {
        id,
        ok: false,
        result: None,
        error: Some(ProtocolError {
            code,
            message: message.into(),
            retryable: code.is_retryable(),
            terminal_id: None,
        }),
    }
}

/// 首帧鉴权结论。
enum Handshake {
    /// 通过；保留请求 ID 与客户端标识用于日志/响应。
    Accepted {
        /// `hello` 请求 ID。
        id: String,
        /// 客户端标识。
        client: String,
    },
    /// 拒绝；需要回错误响应后关闭连接。
    Rejected {
        /// 请求 ID；首帧无法解析时为空串。
        id: String,
        /// 协议错误码。
        code: ErrorCode,
        /// 不含敏感信息的说明。
        message: &'static str,
    },
}

impl Handshake {
    /// 构造拒绝结论。
    fn rejected(id: impl Into<String>, code: ErrorCode, message: &'static str) -> Self {
        Self::Rejected {
            id: id.into(),
            code,
            message,
        }
    }
}

/// 校验首帧必须是合法 `hello`，且 proto/token 匹配。
fn evaluate_handshake(
    message: &Message,
    expected_token: &str,
    auth_header: Option<&str>,
) -> Handshake {
    match message {
        Message::Text(text) => evaluate_hello_text(text.as_str(), expected_token, auth_header),
        Message::Close(_) => {
            Handshake::rejected("", ErrorCode::ProtocolError, "首帧之前连接已关闭")
        }
        _ => Handshake::rejected("", ErrorCode::ProtocolError, "首帧必须是 hello 文本 JSON"),
    }
}

/// 解析首帧文本并要求 method 为 `hello`。
fn evaluate_hello_text(text: &str, expected_token: &str, auth_header: Option<&str>) -> Handshake {
    let request: Request = match from_json(text) {
        Ok(request) => request,
        Err(_) => {
            return Handshake::rejected("", ErrorCode::ProtocolError, "首帧不是合法的请求 JSON");
        }
    };

    if request.method != "hello" {
        return Handshake::rejected(request.id, ErrorCode::ProtocolError, "首帧必须是 hello");
    }

    validate_hello(request, expected_token, auth_header)
}

/// 校验 `hello` 参数中的 proto 与 token；`Authorization` 头优先于 `hello.token`。
fn validate_hello(request: Request, expected_token: &str, auth_header: Option<&str>) -> Handshake {
    let params: HelloParams = match serde_json::from_value(request.params) {
        Ok(params) => params,
        Err(_) => {
            return Handshake::rejected(request.id, ErrorCode::ProtocolError, "hello 参数非法");
        }
    };

    if params.proto != PROTOCOL_VERSION {
        return Handshake::rejected(request.id, ErrorCode::VersionMismatch, "主协议版本不匹配");
    }

    let provided_token = bearer_token(auth_header).unwrap_or(params.token.as_str());
    if !tokens_match(expected_token, provided_token) {
        return Handshake::rejected(request.id, ErrorCode::AuthFailed, "token 校验失败");
    }

    Handshake::Accepted {
        id: request.id,
        client: params.client,
    }
}

/// 从 `Authorization: Bearer <token>` 中取出 token；非 Bearer 视为缺省。
fn bearer_token(auth_header: Option<&str>) -> Option<&str> {
    let (scheme, token) = auth_header?.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case(BEARER_PREFIX) {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

/// 长度相等时逐字节比较，避免首个差异字节提前返回。
fn tokens_match(expected: &str, provided: &str) -> bool {
    let expected = expected.as_bytes();
    let provided = provided.as_bytes();
    if expected.len() != provided.len() {
        return false;
    }

    let diff = expected
        .iter()
        .zip(provided)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right));
    diff == 0
}

/// 清洗日志字段中的控制字符，避免换行/转义注入。
fn sanitize_log_value(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                '?'
            } else {
                character
            }
        })
        .collect()
}

/// 是否为环回地址。
fn is_loopback(addr: SocketAddr) -> bool {
    addr.ip().is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn loopback_addresses_are_recognized() {
        for addr in [
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 1, 2, 3)), 1),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1),
        ] {
            assert!(is_loopback(addr), "{addr} 应判定为环回");
        }

        for addr in [
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 1),
            SocketAddr::new(IpAddr::V6("2001:db8::1".parse().expect("IPv6 字面量")), 1),
        ] {
            assert!(!is_loopback(addr), "{addr} 不应判定为环回");
        }
    }

    #[test]
    fn tokens_match_only_when_equal() {
        assert!(tokens_match("abc", "abc"));
        assert!(tokens_match("", ""));
        assert!(!tokens_match("abc", "abd"));
        assert!(!tokens_match("abc", "ab"));
        assert!(!tokens_match("", "x"));
    }

    #[test]
    fn bearer_token_accepts_case_insensitive_scheme_only() {
        assert_eq!(bearer_token(Some("Bearer abc")), Some("abc"));
        assert_eq!(bearer_token(Some("bearer abc")), Some("abc"));
        assert_eq!(bearer_token(Some("Bearer   abc  ")), Some("abc"));
        assert_eq!(bearer_token(Some("Basic abc")), None);
        assert_eq!(bearer_token(Some("Bearer")), None);
        assert_eq!(bearer_token(Some("Bearer   ")), None);
        assert_eq!(bearer_token(None), None);
    }

    #[test]
    fn log_values_strip_control_characters() {
        assert_eq!(sanitize_log_value("a\nb\r\tc"), "a?b??c");
        assert_eq!(sanitize_log_value("normal-client"), "normal-client");
    }
}
