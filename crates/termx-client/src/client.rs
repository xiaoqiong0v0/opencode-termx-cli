//! 连接、请求配对与事件广播。
//!
//! 并发模型：单后台 reader 任务读帧；响应按 `id` 交给 pending 表里的
//! `oneshot`，事件经 `broadcast` 广播。写端由互斥的 `SplitSink` 串行发送，
//! 因而「请求发送」串行，但「等待响应」与「事件消费」互不阻塞。
//! 连接断开时统一收尾：置关闭标记、关闭事件通道、所有 pending 返回
//! [`ClientError::Closed`]。

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use termx_protocol::{
    AttachParams, AttachResult, ClaimParams, ClaimResult, CreateParams, CreateResult, DetachParams,
    ErrorCode, Event, HelloParams, HelloResult, ListItem, ListParams, OutputParams, OutputResult,
    PROTOCOL_VERSION, ProtocolError, PruneParams, PruneResult, ReadParams, ReadResult,
    RebindParams, RebindResult, ReleaseParams, Request, ResizeParams, Response, SendParams,
    SignalParams, SimpleResult, StatusParams, StatusResult, SubscribeParams, SubscribeResult,
    TerminateParams, TranscriptParams, TranscriptResult, UnsubscribeParams,
};
use tokio::{
    net::TcpStream,
    sync::{Mutex as AsyncMutex, broadcast, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{ClientConfig, ClientError};

/// WebSocket 流类型（本机明文传输）。
type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
/// 拆分的写端。
type WsSink = SplitSink<WsStream, Message>;
/// 拆分的读端。
type WsSource = SplitStream<WsStream>;
/// 单个请求的响应通道。
type PendingSender = oneshot::Sender<Result<serde_json::Value, ClientError>>;
/// 未完成请求表：请求 ID → 响应通道。
type PendingMap = HashMap<String, PendingSender>;

/// 事件广播通道容量；慢消费者只会收到 `Lagged`，不阻塞 reader。
const EVENT_CHANNEL_CAPACITY: usize = 1024;
/// 建立 WebSocket 连接的超时。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// `hello` 握手的超时。
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// 握手请求 ID；握手阶段连接内唯一即可。
const HELLO_REQUEST_ID: &str = "hello";
/// `hello` 方法名。
const HELLO_METHOD: &str = "hello";
/// 允许连接的环回主机字面量。
const LOOPBACK_HOSTS: [&str; 2] = ["127.0.0.1", "::1"];

/// 连接共享状态；reader 任务与 `Client` 共同持有。
struct Inner {
    /// WebSocket 写端；发送本身串行，避免交错写坏帧。
    sink: AsyncMutex<WsSink>,
    /// 未完成请求表；只在同步短临界区内访问，不跨 `await`。
    pending: Mutex<PendingMap>,
    /// 连接内已使用过的请求 ID（含已完成请求），保证显式复用被拒绝。
    used_ids: Mutex<HashSet<String>>,
    /// 事件广播发送端；关闭时置空以让所有接收者看到 `Closed`。
    events: Mutex<Option<broadcast::Sender<Event>>>,
    /// 连接级错误（`id == ""` 的错误帧）；供后续请求与 adapter 判定。
    last_error: Mutex<Option<ProtocolError>>,
    /// 请求 ID 计数器。
    next_id: AtomicU64,
    /// 连接是否已关闭。
    closed: AtomicBool,
}

impl Inner {
    /// 取 pending 表锁；锁中毒时取回内部数据继续工作。
    fn lock_pending(&self) -> MutexGuard<'_, PendingMap> {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 取已用 ID 表锁；锁中毒时取回内部数据继续工作。
    fn lock_used_ids(&self) -> MutexGuard<'_, HashSet<String>> {
        self.used_ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 生成并登记下一个连接内唯一请求 ID。
    ///
    /// 自动 ID 与显式 ID 共用一张已用表：显式 ID 可能占用 `c-<n>`，
    /// 这里跳过已占用值，保证两种来源不会相撞。
    fn reserve_auto_request_id(&self) -> String {
        let mut used = self.lock_used_ids();
        loop {
            let candidate = format!("c-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
            if used.insert(candidate.clone()) {
                return candidate;
            }
        }
    }

    /// 登记显式请求 ID；连接内重复时返回 `None`。
    fn reserve_explicit_request_id(&self, request_id: &str) -> bool {
        self.lock_used_ids().insert(request_id.to_string())
    }

    /// 连接是否已关闭。
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// 记录连接级错误（`id == ""` 的错误帧）。
    fn set_connection_error(&self, error: ProtocolError) {
        let mut guard = self
            .last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_none() {
            *guard = Some(error);
        }
    }

    /// 取连接级错误克隆；用于后续请求直接返回同一失败。
    fn connection_error(&self) -> Option<ClientError> {
        self.last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .map(ClientError::Protocol)
    }

    /// 订阅事件；已关闭时返回一个立即结束的接收者。
    fn subscribe_events(&self) -> broadcast::Receiver<Event> {
        let guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_ref() {
            Some(sender) => sender.subscribe(),
            None => {
                let (sender, receiver) = broadcast::channel(1);
                drop(sender);
                receiver
            }
        }
    }

    /// 广播一个事件；没有接收者时丢弃，不影响请求流程。
    fn broadcast(&self, event: Event) {
        let sender = {
            let guard = self
                .events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.as_ref().cloned()
        };
        if let Some(sender) = sender {
            let _ = sender.send(event);
        }
    }

    /// 统一收尾：置关闭标记、关闭事件通道、pending 全部返回 `Closed`。
    ///
    /// 幂等；`closed` 的置位与 pending 插入共用同一把锁的先后顺序，
    /// 保证收尾之后不会再有请求挂进 pending 表。
    fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let sender = {
            let mut guard = self
                .events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.take()
        };
        drop(sender);

        let mut pending = self.lock_pending();
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(ClientError::Closed));
        }
    }
}

/// 已建立并完成 `hello` 的客户端连接。
pub struct Client {
    /// 连接共享状态。
    inner: Arc<Inner>,
    /// 单次请求等待响应的超时。
    request_timeout: Duration,
    /// 后台读任务句柄；`close` 或 Drop 时中断。
    reader: Mutex<Option<JoinHandle<()>>>,
}

/// 生成类型化 RPC 方法：序列化参数、调用 [`Client::request`]、反序列化结果。
macro_rules! typed_calls {
    ($(
        $(#[$meta:meta])*
        $name:ident => $method:literal, $params:ty, $result:ty;
    )*) => {
        $(
            $(#[$meta])*
            pub async fn $name(&self, params: $params) -> Result<$result, ClientError> {
                let value = self
                    .request(
                        None,
                        $method,
                        serde_json::to_value(params).map_err(encode_error)?,
                    )
                    .await?;
                serde_json::from_value::<$result>(value).map_err(decode_error)
            }
        )*
    };
}

impl Client {
    /// 连接服务并完成 `hello` 握手。
    ///
    /// - 连接前先校验 endpoint 主机必须是 `127.0.0.1` / `[::1]`，否则直接
    ///   返回 [`ClientError::Connect`]，绝不向外部地址发送 token；
    /// - 连接/传输失败映射为 [`ClientError::Connect`] / [`ClientError::Transport`]；
    /// - 服务端 `auth_failed` → [`ClientError::Auth`]；
    /// - `version_mismatch` 或响应 proto 不符 → [`ClientError::VersionMismatch`]；
    /// - 其余握手错误 → [`ClientError::Protocol`]。
    ///
    /// 调用 [`Client::attach`] / [`Client::subscribe`] 前必须先调用
    /// [`Client::events`]：回放事件经广播通道投递，无接收者会被直接丢弃。
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        ensure_loopback_endpoint(&config.endpoint)?;
        let request_timeout = config.request_timeout;

        let (mut ws, _) = timeout(CONNECT_TIMEOUT, connect_async(config.endpoint.as_str()))
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(|error| ClientError::Connect(error.to_string()))?;

        let hello = HelloParams {
            proto: PROTOCOL_VERSION.to_string(),
            token: config.token,
            client: config.client,
            root_session_id: config.root_session_id,
            admin: config.admin,
        };
        let request = Request {
            id: HELLO_REQUEST_ID.to_string(),
            method: HELLO_METHOD.to_string(),
            params: serde_json::to_value(hello).map_err(encode_error)?,
        };
        let text = serde_json::to_string(&request).map_err(encode_error)?;
        ws.send(Message::Text(text.into()))
            .await
            .map_err(|error| ClientError::Transport(error.to_string()))?;

        let response = timeout(HELLO_TIMEOUT, read_hello_response(&mut ws))
            .await
            .map_err(|_| ClientError::Timeout)??;
        evaluate_hello(response)?;

        // 握手成功后才拆读写：首帧阶段不需要事件通道。
        let (sink, source) = ws.split();
        let (events, _receiver) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let inner = Arc::new(Inner {
            sink: AsyncMutex::new(sink),
            pending: Mutex::new(HashMap::new()),
            used_ids: Mutex::new(HashSet::new()),
            events: Mutex::new(Some(events)),
            last_error: Mutex::new(None),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        });
        let reader = tokio::spawn(run_reader(Arc::clone(&inner), source));

        Ok(Self {
            inner,
            request_timeout,
            reader: Mutex::new(Some(reader)),
        })
    }

    /// 发送原始请求并等待配对响应；`id` 缺省时自动生成连接内唯一 ID。
    ///
    /// 显式 `id` 在整个连接生命周期内只能使用一次（已完成请求也不再释放），
    /// 复用返回 `bad_request` 协议错误。等待响应超过
    /// [`ClientConfig::request_timeout`] 时返回 [`ClientError::Timeout`]。
    /// 连接级错误（`id == ""`）到达后，后续请求直接返回同一错误。
    pub async fn request(
        &self,
        id: Option<String>,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, ClientError> {
        if let Some(error) = self.inner.connection_error() {
            return Err(error);
        }
        if self.inner.is_closed() {
            return Err(ClientError::Closed);
        }

        let request_id = match id {
            Some(request_id) => {
                if !self.inner.reserve_explicit_request_id(&request_id) {
                    return Err(protocol_error(
                        ErrorCode::BadRequest,
                        format!("请求 ID 在连接内已使用: {request_id}"),
                    ));
                }
                request_id
            }
            None => self.inner.reserve_auto_request_id(),
        };
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self.inner.lock_pending();
            // 与 shutdown 的收尾顺序对齐：收尾置位后不再接纳新请求。
            if let Some(error) = self.inner.connection_error() {
                return Err(error);
            }
            if self.inner.is_closed() {
                return Err(ClientError::Closed);
            }
            if pending.contains_key(&request_id) {
                return Err(protocol_error(
                    ErrorCode::BadRequest,
                    format!("请求 ID 重复: {request_id}"),
                ));
            }
            pending.insert(request_id.clone(), sender);
        }

        let request = Request {
            id: request_id.clone(),
            method: method.to_string(),
            params,
        };
        let message = match serde_json::to_string(&request) {
            Ok(text) => Message::Text(text.into()),
            Err(error) => {
                self.inner.lock_pending().remove(&request_id);
                return Err(encode_error(error));
            }
        };

        let sent = {
            let mut sink = self.inner.sink.lock().await;
            sink.send(message).await
        };
        if let Err(error) = sent {
            self.inner.lock_pending().remove(&request_id);
            self.inner.shutdown();
            return Err(ClientError::Transport(error.to_string()));
        }

        match timeout(self.request_timeout, receiver).await {
            Ok(Ok(outcome)) => outcome,
            // 发送端在未发送结果的情况下被丢弃（例如收尾竞态）。
            Ok(Err(_)) => Err(ClientError::Closed),
            Err(_) => {
                // 超时后撤掉 pending，避免迟到的响应写入已弃用通道。
                self.inner.lock_pending().remove(&request_id);
                Err(ClientError::Timeout)
            }
        }
    }

    typed_calls! {
        /// 创建终端（`terminal.create`）。
        create => "terminal.create", CreateParams, CreateResult;
        /// 附着终端并按 `cursor` 回放事件（`terminal.attach`）。
        ///
        /// **调用前必须先 [`Client::events`]**：回放事件写入广播通道，
        /// 没有接收者时会被直接丢弃。
        attach => "terminal.attach", AttachParams, AttachResult;
        /// 断开附着（`terminal.detach`）。
        detach => "terminal.detach", DetachParams, SimpleResult;
        /// 取得输入 lease（`terminal.claim`）。
        claim => "terminal.claim", ClaimParams, ClaimResult;
        /// 释放输入 lease（`terminal.release`）。
        release => "terminal.release", ReleaseParams, SimpleResult;
        /// 列出终端（`terminal.list`）。
        list => "terminal.list", ListParams, Vec<ListItem>;
        /// 查询终端状态快照（`terminal.status`）。
        status => "terminal.status", StatusParams, StatusResult;
        /// 增量读取 output（`terminal.read`）。
        read => "terminal.read", ReadParams, ReadResult;
        /// 对保留的原始输出做尾部截取或倒搜（`terminal.output`）。
        output => "terminal.output", OutputParams, OutputResult;
        /// 读取命令记录（`terminal.transcript`）。
        transcript => "terminal.transcript", TranscriptParams, TranscriptResult;
        /// 向终端写入（`terminal.send`）。
        send => "terminal.send", SendParams, SimpleResult;
        /// 调整终端尺寸（`terminal.resize`）。
        resize => "terminal.resize", ResizeParams, SimpleResult;
        /// 发送信号（`terminal.signal`）。
        signal => "terminal.signal", SignalParams, SimpleResult;
        /// 迁移终端绑定键（`terminal.rebind`），保留 `terminal_id`、进程与 lease。
        rebind => "terminal.rebind", RebindParams, RebindResult;
        /// 终止终端（`terminal.terminate`）。
        terminate => "terminal.terminate", TerminateParams, SimpleResult;
        /// 清理终态终端（`terminal.prune`）；空过滤需显式 `PruneParams::all = true`。
        prune => "terminal.prune", PruneParams, PruneResult;
        /// 订阅事件（`subscribe`）。
        ///
        /// **调用前必须先 [`Client::events`]**：回放事件写入广播通道，
        /// 没有接收者时会被直接丢弃。
        subscribe => "subscribe", SubscribeParams, SubscribeResult;
        /// 取消订阅（`unsubscribe`）。
        unsubscribe => "unsubscribe", UnsubscribeParams, SimpleResult;
    }

    /// 订阅事件广播；连接关闭后接收者会读到 `Closed`。
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.inner.subscribe_events()
    }

    /// 连接是否已关闭。
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// 最近一次连接级错误（`id == ""` 的错误帧）。
    ///
    /// 事件通道关闭后 adapter 可用它区分「正常关闭」与「服务端判定连接级失败」。
    pub fn last_error(&self) -> Option<ClientError> {
        self.inner.connection_error()
    }

    /// 主动关闭：先收尾（pending 全回 `Closed`），再发送 WebSocket Close 帧并中断 reader。
    pub async fn close(&self) {
        self.inner.shutdown();
        {
            let mut sink = self.inner.sink.lock().await;
            let _ = sink.close().await;
        }
        self.abort_reader();
    }

    /// 中断后台读任务；幂等。
    fn abort_reader(&self) {
        let handle = {
            let mut guard = self
                .reader
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.take()
        };
        if let Some(handle) = handle {
            handle.abort();
        }
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Client")
            .field("closed", &self.inner.is_closed())
            .finish()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.inner.shutdown();
        self.abort_reader();
    }
}

/// 后台读循环：响应按 `id` 配对，其余帧按事件广播。
async fn run_reader(inner: Arc<Inner>, mut source: WsSource) {
    while let Some(message) = source.next().await {
        match message {
            Ok(Message::Text(text)) => {
                if !handle_text(&inner, text.as_str()) {
                    break;
                }
            }
            Ok(Message::Close(_)) => break,
            // Ping/Pong 由 tungstenite 内部处理；二进制帧不在 v1 协议内。
            Ok(_) => {}
            Err(_) => break,
        }
    }
    inner.shutdown();
}

/// 处理一个 text frame；返回 `false` 表示应结束读循环。
fn handle_text(inner: &Inner, text: &str) -> bool {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return false,
    };

    if value.get("id").is_some() {
        let response: Response = match serde_json::from_value(value) {
            Ok(response) => response,
            Err(_) => return false,
        };
        if response.id.is_empty() {
            // 协议约定的连接级错误（如订阅队列溢出）：先留存错误，
            // 再交给连接关闭统一收尾，让后续请求能返回同一失败。
            let error = response.error.unwrap_or_else(|| ProtocolError {
                code: ErrorCode::ProtocolError,
                message: "连接级错误缺少 error 字段".to_string(),
                retryable: false,
                terminal_id: None,
                injection: None,
                host_key: None,
            });
            inner.set_connection_error(error);
            return false;
        }
        deliver_response(inner, response);
        true
    } else {
        let event: Event = match serde_json::from_value(value) {
            Ok(event) => event,
            Err(_) => return false,
        };
        inner.broadcast(event);
        true
    }
}

/// 把响应交给对应 `oneshot`；未知 ID 直接丢弃。
fn deliver_response(inner: &Inner, response: Response) {
    let outcome = if response.ok {
        response
            .result
            .ok_or_else(|| protocol_error(ErrorCode::ProtocolError, "成功响应缺少 result 字段"))
    } else {
        Err(response.error.map_or_else(
            || protocol_error(ErrorCode::ProtocolError, "失败响应缺少 error 字段"),
            ClientError::Protocol,
        ))
    };

    let sender = { inner.lock_pending().remove(&response.id) };
    if let Some(sender) = sender {
        let _ = sender.send(outcome);
    }
}

/// 读取握手响应；忽略握手前到达的非响应帧。
async fn read_hello_response(ws: &mut WsStream) -> Result<Response, ClientError> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                let value: serde_json::Value = serde_json::from_str(text.as_str())
                    .map_err(|error| protocol_error(ErrorCode::ProtocolError, error.to_string()))?;
                if value.get("id").is_none() {
                    continue;
                }
                let response: Response = serde_json::from_value(value)
                    .map_err(|error| protocol_error(ErrorCode::ProtocolError, error.to_string()))?;
                if response.id == HELLO_REQUEST_ID {
                    return Ok(response);
                }
            }
            Some(Ok(Message::Close(_))) => {
                return Err(ClientError::Transport(
                    "服务端在 hello 完成前关闭连接".to_string(),
                ));
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(ClientError::Transport(error.to_string())),
            None => {
                return Err(ClientError::Transport(
                    "服务端在 hello 完成前断开连接".to_string(),
                ));
            }
        }
    }
}

/// 校验 `hello` 响应：ok、proto 与错误码映射。
fn evaluate_hello(response: Response) -> Result<HelloResult, ClientError> {
    if !response.ok {
        let error = response.error.ok_or_else(|| {
            protocol_error(ErrorCode::ProtocolError, "hello 失败但缺少 error 字段")
        })?;
        return match error.code {
            ErrorCode::AuthFailed => Err(ClientError::Auth),
            ErrorCode::VersionMismatch => Err(ClientError::VersionMismatch),
            _ => Err(ClientError::Protocol(error)),
        };
    }

    let value = response
        .result
        .ok_or_else(|| protocol_error(ErrorCode::ProtocolError, "hello 成功但缺少 result"))?;
    let result: HelloResult = serde_json::from_value(value).map_err(decode_error)?;
    if result.proto != PROTOCOL_VERSION {
        return Err(ClientError::VersionMismatch);
    }
    Ok(result)
}

/// 校验 endpoint 只指向环回字面量；拒绝外部主机与用户信息。
///
/// 只接受 `ws://127.0.0.1:port` / `ws://[::1]:port`（端口可缺省）。
/// `localhost` 不做 DNS 解析：解析结果可能被 hosts/代理改写，最稳妥的做法
/// 是只放行明确的环回字面量。
fn ensure_loopback_endpoint(endpoint: &str) -> Result<(), ClientError> {
    let rest = endpoint.strip_prefix("ws://").ok_or_else(|| {
        ClientError::Connect(format!(
            "仅支持 ws:// 环回地址（127.0.0.1 / [::1]），收到: {endpoint}"
        ))
    })?;
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .trim();
    if authority.is_empty() || authority.contains('@') {
        return Err(ClientError::Connect(format!(
            "endpoint 主机非法（要求 127.0.0.1 或 [::1]）: {endpoint}"
        )));
    }

    let host = if let Some(inner) = authority.strip_prefix('[') {
        let end = inner.find(']').ok_or_else(|| {
            ClientError::Connect(format!("endpoint IPv6 主机缺少 ']': {endpoint}"))
        })?;
        &inner[..end]
    } else {
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host)
    };

    if LOOPBACK_HOSTS.contains(&host) {
        Ok(())
    } else {
        Err(ClientError::Connect(format!(
            "拒绝连接非环回主机 {host:?}；只允许 127.0.0.1 或 [::1]"
        )))
    }
}

/// 构造 `protocol_error` 错误。
fn protocol_error(code: ErrorCode, message: impl Into<String>) -> ClientError {
    ClientError::Protocol(ProtocolError {
        code,
        message: message.into(),
        retryable: false,
        terminal_id: None,
        injection: None,
        host_key: None,
    })
}

/// 参数序列化失败映射为协议错误。
fn encode_error(error: serde_json::Error) -> ClientError {
    protocol_error(
        ErrorCode::ProtocolError,
        format!("请求参数序列化失败: {error}"),
    )
}

/// 结果反序列化失败映射为协议错误。
fn decode_error(error: serde_json::Error) -> ClientError {
    protocol_error(
        ErrorCode::ProtocolError,
        format!("响应结果解码失败: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_endpoints_are_accepted() {
        for endpoint in [
            "ws://127.0.0.1:45678",
            "ws://127.0.0.1",
            "ws://[::1]:45678",
            "ws://[::1]",
        ] {
            assert!(
                ensure_loopback_endpoint(endpoint).is_ok(),
                "{endpoint} 应被接受"
            );
        }
    }

    #[test]
    fn non_loopback_endpoints_are_rejected() {
        for endpoint in [
            "ws://192.168.1.1:45678",
            "ws://0.0.0.0:45678",
            "ws://localhost:45678",
            "ws://example.com:443",
            "ws://[2001:db8::1]:443",
            "ws://user@127.0.0.1:1",
            "wss://127.0.0.1:1",
            "http://127.0.0.1:1",
        ] {
            assert!(
                matches!(
                    ensure_loopback_endpoint(endpoint),
                    Err(ClientError::Connect(_))
                ),
                "{endpoint} 应被拒绝"
            );
        }
    }
}
