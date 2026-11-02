//! 环回 WebSocket 服务、连接级鉴权状态机与 `hello` 握手。

use std::{
    collections::BTreeSet,
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::{Sink, SinkExt, StreamExt, stream::SplitStream};
use termx_core::{
    BackendFactory, CoreError, DETACH_REASON_DISCONNECT, ExitOutcome, IdleAction, PtyFactory,
    Registry, SpawnedBackend, TerminalBackend, TerminalId,
};
use termx_proc::ProcEvent;
use termx_protocol::{
    AttachParams, Capabilities, ClaimParams, CreateParams, DetachParams, ErrorCode, Event,
    EventKind, HELPER_PROTOCOL_VERSION, HelloParams, HelloResult, ListParams, PROTOCOL_VERSION,
    ProtocolError, PruneParams, ReleaseParams, Request, ResizeParams, Response, SendParams,
    SignalParams, SimpleResult, StatusParams, StatusResult, Stream, SubscribeParams,
    SubscribeResult, SubscriptionCursor, TerminalLocator, TerminalSnapshot, TerminateParams,
    UnsubscribeParams, from_json, to_json,
};
use time::OffsetDateTime;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{Receiver, UnboundedReceiver, UnboundedSender, unbounded_channel},
        watch,
    },
    time::{MissedTickBehavior, timeout},
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
    /// 仅允许环回监听；配置目录、token 文件会在绑定前准备好。默认使用
    /// [`PtyFactory`] 创建真实 PTY 后端。
    pub async fn bind(config: ServiceConfig) -> Result<Self, ServiceError> {
        Self::bind_with_factory(config, Arc::new(PtyFactory)).await
    }

    /// 使用指定后端工厂绑定服务；测试可注入 mock 工厂以避免创建真实进程。
    pub async fn bind_with_factory(
        config: ServiceConfig,
        factory: Arc<dyn BackendFactory>,
    ) -> Result<Self, ServiceError> {
        if !is_loopback(config.bind_addr) {
            return Err(ServiceError::NonLoopbackBind(config.bind_addr));
        }

        // 端口文件始终写到 config_dir；即使 token 走环境变量或自定义文件也要保证目录存在。
        std::fs::create_dir_all(&config.config_dir)?;
        let token = load_or_create_token(&config)?;
        let listener = TcpListener::bind(config.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        write_port_file(&config, local_addr.port())?;
        let registry = Arc::new(Mutex::new(Registry::with_factory(
            config.event_buffer_capacity,
            factory,
        )));

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

        // 空闲检查 ticker 与 accept 循环同生命周期：shutdown 时先发停止信号，
        // 再等待 ticker 退出，避免后台任务悬挂。
        let (stop_tx, stop_rx) = watch::channel(false);
        let ticker = tokio::spawn(run_idle_ticker(
            Arc::clone(&self.registry),
            self.config.idle_tick_interval,
            stop_rx,
        ));

        let result = loop {
            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!(addr = %self.local_addr, "termx-service 停止接收新连接");
                    break Ok(());
                }
                accepted = self.listener.accept() => {
                    let (stream, peer_addr) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => break Err(ServiceError::Io(error)),
                    };
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
        };

        let _ = stop_tx.send(true);
        let _ = ticker.await;
        result
    }
}

/// 后台空闲检查：周期调用 [`Registry::expire_idle`]。
///
/// 锁纪律：只在同步块内取 registry 锁调用 `expire_idle`（事件投递为非阻塞
/// send），`Terminate` 返回的后端在锁外通过阻塞线程池终止。
async fn run_idle_ticker(
    registry: Arc<Mutex<Registry>>,
    period: Duration,
    mut stop: watch::Receiver<bool>,
) {
    let period = period.max(Duration::from_millis(1));
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // 跳过 interval 的首次立即触发，启动后按周期扫描。
    interval.tick().await;

    loop {
        tokio::select! {
            changed = stop.changed() => {
                // 发送端被 drop 或收到停止信号都退出。
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                let actions = {
                    let mut registry = lock_registry(&registry);
                    registry.expire_idle(OffsetDateTime::now_utc())
                };
                // 每个待终止后端独立 spawn 一个任务：多个终端同时到期时并行执行，
                // 避免一个慢后端串行拖住整轮扫描，也缩短 shutdown 等待。
                let mut terminations = Vec::new();
                for action in actions {
                    match action {
                        IdleAction::Detach { terminal_id, clients } => {
                            // registry 已在锁内把 detached{idle_timeout} 投递给订阅者。
                            tracing::debug!(
                                terminal_id = %terminal_id,
                                clients = clients.len(),
                                "空闲超时：已断开订阅，终端继续运行"
                            );
                        }
                        IdleAction::Terminate(backend) => {
                            tracing::info!(pid = backend.pid(), "空闲超时：终止终端");
                            terminations.push(tokio::spawn(terminate_backend(backend)));
                        }
                    }
                }
                for handle in terminations {
                    if let Err(error) = handle.await {
                        tracing::debug!(%error, "空闲终止任务失败");
                    }
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
    registry: &Arc<Mutex<Registry>>,
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
                handle_rpc_text(text.as_str(), client_id, registry, out_tx, event_tx).await?
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

/// 解析请求并分发；非法 JSON 回 `protocol_error`。
async fn handle_rpc_text(
    text: &str,
    client_id: &str,
    registry: &Arc<Mutex<Registry>>,
    out_tx: &UnboundedSender<Message>,
    event_tx: &UnboundedSender<Event>,
) -> Result<(), ServiceError> {
    let response = match from_json::<Request>(text) {
        Ok(request) => dispatch_request(request, client_id, registry, event_tx).await?,
        Err(_) => error_response(String::new(), ErrorCode::ProtocolError, "请求 JSON 非法"),
    };

    let text = to_json(&response)?;
    let _ = out_tx.send(Message::Text(text.into()));
    Ok(())
}

/// 分发 `terminal.*` RPC；attach/subscribe 使用连接级 `client_id` 与事件通道。
///
/// 锁内只做 registry 读写（事件投递为非阻塞 send），不跨 `await` 持有锁；
/// `terminal.create` 的 spawn/terminate 都在 `spawn_blocking` 中于锁外执行。
async fn dispatch_request(
    request: Request,
    client_id: &str,
    registry: &Arc<Mutex<Registry>>,
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
            handle_create(id, params, registry).await
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

            let (snapshot, backend) = {
                let registry = lock_registry(registry);
                match registry.status_parts(&params.locator) {
                    Ok(parts) => parts,
                    Err(error) => return Ok(response_from_core_error(id, error)),
                }
            };
            // 后端查询放在锁外，并与其它后端操作一致放进阻塞线程池：
            // `foreground_pgrp` / `tree` 都可能阻塞或读 /proc。
            let snapshot = match tokio::task::spawn_blocking(move || {
                fill_runtime_info(snapshot, backend.as_deref())
            })
            .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::Internal,
                        format!("status 任务失败: {error}"),
                    ));
                }
            };
            let value = serde_json::to_value(StatusResult { snapshot })?;
            Ok(ok_response(id, value))
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
        "terminal.send" => {
            let params: SendParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.send 参数非法: {error}"),
                    ));
                }
            };
            handle_send(id, params, client_id, registry).await
        }
        "terminal.resize" => {
            let params: ResizeParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.resize 参数非法: {error}"),
                    ));
                }
            };
            handle_resize(id, params, registry).await
        }
        "terminal.signal" => {
            let params: SignalParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.signal 参数非法: {error}"),
                    ));
                }
            };
            handle_signal(id, params, registry).await
        }
        "terminal.terminate" => {
            let params: TerminateParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.terminate 参数非法: {error}"),
                    ));
                }
            };
            handle_terminate(id, params, registry).await
        }
        "terminal.prune" => {
            let params: PruneParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.prune 参数非法: {error}"),
                    ));
                }
            };
            let result = {
                let mut registry = lock_registry(registry);
                registry.prune(&params)
            };
            Ok(ok_response(id, serde_json::to_value(result)?))
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

/// 创建终端：预检 → 锁外终止旧后端 → 锁外 spawn → 锁内注册 → 启动事件摄取。
///
/// 并发契约：registry 的 `std::sync::Mutex` 只在同步短操作中持有；
/// `spawn` / `terminate` / `foreground_pgrp` 全部放到 `spawn_blocking`，
/// 且不在持锁期间等待。
async fn handle_create(
    id: String,
    params: CreateParams,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    // 只做参数/冲突预检，不提前终止旧后端：否则旧进程退出的 `exited` 可能
    // 抢在 `register_create` 落 `interrupted` 之前。置换统一在锁内完成。
    let factory = {
        let registry = lock_registry(registry);
        if let Err(error) = registry.prepare_create(&params) {
            return Ok(response_from_core_error(id, error));
        }
        registry.backend_factory()
    };

    let (backend, events) = match spawn_backend(factory, params.clone()).await {
        Ok(spawned) => spawned,
        Err(error) => return Ok(response_from_core_error(id, error)),
    };
    let foreground_pgrp = read_foreground_pgrp(Arc::clone(&backend)).await;

    let registered = {
        let mut registry = lock_registry(registry);
        registry.register_create(params, Arc::clone(&backend), foreground_pgrp)
    };
    let (result, displaced) = match registered {
        Ok(registered) => registered,
        Err(error) => {
            // 竞态导致注册失败：回收刚启动的进程，避免孤儿。
            terminate_backend(backend).await;
            return Ok(response_from_core_error(id, error));
        }
    };
    if let Some(displaced) = displaced {
        terminate_backend(displaced).await;
    }

    tokio::spawn(ingest_proc_events(
        Arc::clone(registry),
        TerminalId::new(result.terminal_id.clone()),
        events,
    ));
    Ok(ok_response(id, serde_json::to_value(result)?))
}

/// 处理 `terminal.send`：锁内做 lease 门禁取后端，锁外阻塞写。
async fn handle_send(
    id: String,
    params: SendParams,
    client_id: &str,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    let payload = match send_payload(&params) {
        Ok(payload) => payload,
        Err(error) => return Ok(response_from_core_error(id, error)),
    };

    let backend = {
        let mut registry = lock_registry(registry);
        match registry.prepare_send(&params.locator, client_id) {
            Ok(backend) => backend,
            Err(error) => return Ok(response_from_core_error(id, error)),
        }
    };

    match tokio::task::spawn_blocking(move || backend.write(&payload)).await {
        Ok(Ok(())) => Ok(ok_response(
            id,
            serde_json::to_value(SimpleResult { ok: true })?,
        )),
        Ok(Err(error)) => Ok(response_from_core_error(id, error)),
        Err(error) => Ok(error_response(
            id,
            ErrorCode::Internal,
            format!("send 任务失败: {error}"),
        )),
    }
}

/// 处理 `terminal.resize`：锁内校验并记录尺寸，锁外调用后端。
async fn handle_resize(
    id: String,
    params: ResizeParams,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    let (cols, rows) = (params.cols, params.rows);
    let backend = {
        let mut registry = lock_registry(registry);
        match registry.prepare_resize(&params.locator, cols, rows) {
            Ok(backend) => backend,
            Err(error) => return Ok(response_from_core_error(id, error)),
        }
    };

    match tokio::task::spawn_blocking(move || backend.resize(cols, rows)).await {
        Ok(Ok(())) => Ok(ok_response(
            id,
            serde_json::to_value(SimpleResult { ok: true })?,
        )),
        Ok(Err(error)) => Ok(response_from_core_error(id, error)),
        Err(error) => Ok(error_response(
            id,
            ErrorCode::Internal,
            format!("resize 任务失败: {error}"),
        )),
    }
}

/// 处理 `terminal.signal`：锁内校验非终态，锁外发送信号。
async fn handle_signal(
    id: String,
    params: SignalParams,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    let prepared = {
        let mut registry = lock_registry(registry);
        match registry.prepare_signal(&params.locator, &params.signal) {
            Ok(prepared) => prepared,
            Err(error) => return Ok(response_from_core_error(id, error)),
        }
    };

    // SIGINT 的 Ctrl-C 字节写入与其它信号发送都在锁外阻塞执行；
    // pending 已在锁内记录。
    match tokio::task::spawn_blocking(move || prepared.execute()).await {
        Ok(Ok(())) => Ok(ok_response(
            id,
            serde_json::to_value(SimpleResult { ok: true })?,
        )),
        Ok(Err(error)) => Ok(response_from_core_error(id, error)),
        Err(error) => Ok(error_response(
            id,
            ErrorCode::Internal,
            format!("signal 任务失败: {error}"),
        )),
    }
}

/// 处理 `terminal.terminate`：锁内落终态并广播，锁外终止后端。
///
/// 重复 terminate 幂等返回 `ok:true`；终态事件只由
/// [`Registry::prepare_terminate`] 发出一次。
async fn handle_terminate(
    id: String,
    params: TerminateParams,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    let backend = {
        let mut registry = lock_registry(registry);
        match registry.prepare_terminate(&params.locator) {
            Ok(backend) => backend,
            Err(error) => return Ok(response_from_core_error(id, error)),
        }
    };
    if let Some(backend) = backend {
        terminate_backend(backend).await;
    }

    Ok(ok_response(
        id,
        serde_json::to_value(SimpleResult { ok: true })?,
    ))
}

/// 组装 `terminal.send` 的输入字节。
///
/// `data` 优先于 `text`（对齐 PROTOCOL §6）；两者都缺省或 base64 非法时返回
/// `bad_request`。`enter=true` 在末尾追加 `\r`（PTY 回车）。
fn send_payload(params: &SendParams) -> Result<Vec<u8>, CoreError> {
    let mut payload = if let Some(data) = &params.data {
        BASE64_STANDARD
            .decode(data)
            .map_err(|error| CoreError::BadRequest(format!("terminal.send.data 非法: {error}")))?
    } else if let Some(text) = &params.text {
        text.as_bytes().to_vec()
    } else {
        return Err(CoreError::BadRequest(
            "terminal.send 需要 data 或 text".to_string(),
        ));
    };

    if params.enter {
        payload.push(b'\r');
    }
    Ok(payload)
}

/// 在阻塞线程池中调用后端 `spawn`，避免占用 async worker。
async fn spawn_backend(
    factory: Arc<dyn BackendFactory>,
    params: CreateParams,
) -> Result<SpawnedBackend, CoreError> {
    match tokio::task::spawn_blocking(move || factory.spawn(&params)).await {
        Ok(result) => result,
        Err(error) => Err(CoreError::SpawnFailed {
            message: format!("spawn 任务失败: {error}"),
        }),
    }
}

/// 在阻塞线程池中调用后端 `terminate`；清理失败只记录。
async fn terminate_backend(backend: Arc<dyn TerminalBackend>) {
    let terminated = tokio::task::spawn_blocking(move || backend.terminate()).await;
    if let Err(error) = terminated {
        tracing::debug!(%error, "终止后端任务失败");
    }
}

/// 在阻塞线程池中读取前台进程组；失败时返回 `None`。
async fn read_foreground_pgrp(backend: Arc<dyn TerminalBackend>) -> Option<u32> {
    tokio::task::spawn_blocking(move || backend.foreground_pgrp())
        .await
        .ok()
        .flatten()
}

/// 把后端运行信息合入 status 快照；终态时后端为 `None`，保留最后快照。
fn fill_runtime_info(
    mut snapshot: TerminalSnapshot,
    backend: Option<&dyn TerminalBackend>,
) -> TerminalSnapshot {
    if let Some(backend) = backend {
        snapshot.foreground_pgrp = backend.foreground_pgrp();
        snapshot.tree = backend.tree();
    }
    snapshot
}

/// 事件摄取任务：把 `termx-proc` 事件翻译为协议事件并写入 registry。
///
/// 每次只在同步块内取 registry 锁；收到终态事件后退出，避免后台任务泄漏。
async fn ingest_proc_events(
    registry: Arc<Mutex<Registry>>,
    terminal_id: TerminalId,
    mut events: Receiver<ProcEvent>,
) {
    while let Some(event) = events.recv().await {
        let (result, terminal) = {
            let mut registry = registry
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match event {
                ProcEvent::Output(bytes) => (
                    registry.report_event(
                        &terminal_id,
                        EventKind::Output {
                            data: BASE64_STANDARD.encode(bytes),
                            stream: Stream::Pty,
                        },
                    ),
                    false,
                ),
                // 退出结果统一交给 report_exit：由 registry 结合 pending 决定
                // 发 interrupted（Ctrl-C / terminate / signal）还是 exited。
                ProcEvent::Exited(exit_code) => (
                    registry.report_exit(&terminal_id, ExitOutcome::Code(exit_code)),
                    true,
                ),
                ProcEvent::Signaled(signal) => (
                    registry.report_exit(&terminal_id, ExitOutcome::Signal(signal)),
                    true,
                ),
            }
        };
        match result {
            Ok(_) => {}
            // 终端被 force 替换后索引已不存在：结束摄取。
            Err(CoreError::NotFound) => break,
            Err(error) => tracing::debug!(%error, terminal_id = %terminal_id, "事件写入失败"),
        }

        if terminal {
            break;
        }
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
fn lock_registry(registry: &Arc<Mutex<Registry>>) -> MutexGuard<'_, Registry> {
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
        CoreError::SpawnFailed { .. } => ErrorCode::SpawnFailed,
        CoreError::UnsupportedSignal(_) => ErrorCode::BadRequest,
        CoreError::BackendFailed(_) => ErrorCode::Internal,
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

    /// 构造只带载荷的 send 参数。
    fn send_params(data: Option<&str>, text: Option<&str>, enter: bool) -> SendParams {
        SendParams {
            locator: TerminalLocator {
                terminal_id: Some("01JABCDEF".to_string()),
                root_session_id: None,
                name: None,
            },
            data: data.map(str::to_string),
            text: text.map(str::to_string),
            enter,
        }
    }

    #[test]
    fn send_payload_prefers_data_and_appends_carriage_return() {
        let payload =
            send_payload(&send_params(Some("aGk="), Some("ignored"), true)).expect("data 应优先");
        assert_eq!(payload, b"hi\r");

        let payload = send_payload(&send_params(None, Some("echo"), false)).expect("text 应可用");
        assert_eq!(payload, b"echo");
    }

    #[test]
    fn send_payload_rejects_missing_and_invalid_data() {
        assert!(matches!(
            send_payload(&send_params(None, None, false)),
            Err(CoreError::BadRequest(_))
        ));
        assert!(matches!(
            send_payload(&send_params(Some("not-base64!"), None, false)),
            Err(CoreError::BadRequest(_))
        ));
    }
}
