//! 环回 WebSocket 服务、连接级鉴权状态机与 `hello` 握手。

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::{Sink, SinkExt, StreamExt, stream::SplitStream};
use serde::Deserialize;
use termx_core::{
    BackendFactory, CLIENT_TIMEOUT_SOURCE, ClientlessCandidate, CoreError, DEFAULT_QUIET_FINALIZE,
    DETACH_REASON_DISCONNECT, ExitOutcome, IdleAction, InteractionConfig, ModeFactory, Registry,
    SpawnedBackend, TerminalBackend, TerminalId,
};
use termx_proc::{OutputStream, ProcEvent};
use termx_protocol::{
    AttachParams, Capabilities, ClaimParams, CreateParams, DetachParams, ErrorCode, Event,
    EventKind, HELPER_PROTOCOL_VERSION, HelloParams, HelloResult, ListItem, ListParams, Mode,
    OutputParams, PROTOCOL_VERSION, ProtocolError, PruneParams, ReadParams, RebindParams,
    ReleaseParams, Request, ResizeParams, Response, RuntimeInfo, RuntimeListResult, SendParams,
    SessionInfo, SessionListParams, SessionListResult, SignalParams, SimpleResult, SshAuth,
    StatusParams, StatusResult, Stream, SubscribeParams, SubscribeResult, SubscriptionCursor,
    TerminalLocator, TerminalSnapshot, TerminateParams, TranscriptParams, UnsubscribeParams,
    WEB_ROOT_SESSION_ID, from_json, to_json,
};
use termx_runtime::{RuntimeCatalog, RuntimeError};
use time::OffsetDateTime;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
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

use crate::config::{
    ClientIdlePolicy, RemoteInjectionConfig, ServiceConfig, load_or_create_token, write_port_file,
};
use crate::lock::{LockError, ServiceLock};
use crate::marker;
use crate::shell_events::{
    EVENT_ENDPOINT_ENV, EVENT_TOKEN_ENV, ShellEventCancel, ShellEventChannel, ShellEventTracker,
};
use crate::web::WebServer;

/// 服务端标识，随包版本变化。
const SERVER_ID: &str = concat!("termx-service/", env!("CARGO_PKG_VERSION"));
/// 等待首条 `hello` 的超时。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// 命令内 marker 注入开关。
///
/// P1 的 in-band marker 会被 shell 回显并写入历史，污染 raw/历史记录；
/// 等 hook-based shell integration 接上后再启用精确退出码。
const TRANSCRIPT_MARKER_ENABLED: bool = false;
/// 使用带外 shell 事件通道的本地 runtime ID。
const SHELL_EVENT_RUNTIME_ID: &str = "termx-pwsh";
/// 单终端交互采样（`foreground_pgrp` + `tree`）的超时上限。
///
/// 每个终端独立任务并带超时：任一后端的 `tree()` 卡住只影响自身，不会拖住
/// 其它终端的采集，也不会阻塞 ticker 响应停止信号。
const INTERACTION_SAMPLE_TIMEOUT: Duration = Duration::from_secs(2);
/// 鉴权头名。
const AUTHORIZATION_HEADER: &str = "authorization";
/// Bearer 鉴权方案前缀。
const BEARER_PREFIX: &str = "bearer";
/// 订阅队列溢出、状态事件无法投递时发给客户端的说明。
const LIMIT_EXCEEDED_MESSAGE: &str =
    "订阅事件队列已满，状态事件无法保证投递；连接将断开，请重连后按 cursor 补齐";
/// 控制台辅助进程名（Windows ConPTY/winpty 会随终端附带，不计为用户子进程）。
const CONSOLE_HELPER_NAMES: &[&str] = &[
    "conhost.exe",
    "conhost",
    "winpty.exe",
    "winpty",
    "openconsole.exe",
    "openconsole",
];

/// 会话心跳表：`root_session_id -> last_seen`，并记录判定存活的 TTL。
///
/// 与 registry 分开加锁；ticker 只在锁内清理过期项并生成存活集合快照，
/// 不跨 `await` 持锁。`session.list` 复用同一把锁读取心跳快照。
struct SessionBookState {
    /// 各会话最近一次心跳时间。
    entries: Mutex<HashMap<String, Instant>>,
    /// 心跳超过该时长视为会话不再存活。
    ttl: Duration,
}

/// 会话心跳表句柄。
type SessionBook = Arc<SessionBookState>;

/// 新式 `heartbeat` 参数；旧客户端发送 `{}` 时按连接级心跳处理。
#[derive(Debug, Default, Deserialize)]
struct HeartbeatParams {
    /// 所属 opencode 会话 ID；缺失表示旧的连接级心跳。
    #[serde(default)]
    root_session_id: Option<String>,
}

/// `session_closed` 参数。
#[derive(Debug, Deserialize)]
struct SessionClosedParams {
    /// 已删除的 opencode 会话 ID。
    root_session_id: String,
}

/// 连接级调用者身份，来自 `hello.root_session_id` / `hello.admin`。
///
/// 普通连接必须携带 `root_session_id`，只能访问同 root 的终端；
/// 管理员连接（Web 代理）可访问/迁移全部终端。
#[derive(Debug, Clone, PartialEq, Eq)]
struct Caller {
    /// 调用者 opencode 会话 ID；admin 连接为 `None`。
    root_session_id: Option<String>,
    /// 是否为管理员连接。
    admin: bool,
}

impl Caller {
    /// 列表/清理等批量操作的会话范围；`None` 表示不限制（管理员）。
    fn scope(&self) -> Option<&str> {
        if self.admin {
            None
        } else {
            self.root_session_id.as_deref()
        }
    }

    /// 是否可访问指定会话的终端。
    ///
    /// `@web` 是 Web 未绑定终端的保留会话，仅管理员可见/可操作；普通连接
    /// 即使自称该 root 也不能访问。
    fn can_access_root(&self, root_session_id: &str) -> bool {
        self.admin
            || (root_session_id != WEB_ROOT_SESSION_ID
                && self.root_session_id.as_deref() == Some(root_session_id))
    }

    /// 校验可访问指定会话；否则返回 `permission_denied`。
    fn require_root(&self, root_session_id: &str) -> Result<(), CoreError> {
        if self.can_access_root(root_session_id) {
            Ok(())
        } else {
            Err(CoreError::PermissionDenied)
        }
    }
}

/// 解析定位器并鉴权，返回只含 `terminal_id` 的定位器。
///
/// 调用方必须持有同一把 registry 锁，并在返回后立即执行实际操作。
fn authorize_locator(
    registry: &Registry,
    locator: &TerminalLocator,
    caller: &Caller,
) -> Result<TerminalLocator, CoreError> {
    let id = registry.authorize(locator, caller.admin, caller.root_session_id.as_deref())?;
    Ok(TerminalLocator {
        terminal_id: Some(id.to_string()),
        root_session_id: None,
        name: None,
    })
}

/// 新建空会话表。
fn new_session_book(ttl: Duration) -> SessionBook {
    Arc::new(SessionBookState {
        entries: Mutex::new(HashMap::new()),
        ttl,
    })
}

/// 记录一次会话心跳（覆盖为 `now`）。
fn record_session_heartbeat(sessions: &SessionBook, root_session_id: &str, now: Instant) {
    let mut entries = lock_sessions(sessions);
    entries.insert(root_session_id.to_string(), now);
}

/// 显式关闭会话；不存在时幂等。
fn remove_session(sessions: &SessionBook, root_session_id: &str) {
    let mut entries = lock_sessions(sessions);
    entries.remove(root_session_id);
}

/// 心跳是否仍在 TTL 内。
fn is_session_live(last_seen: Instant, now: Instant, ttl: Duration) -> bool {
    now.saturating_duration_since(last_seen) < ttl
}

/// 清理超过 TTL 的会话，并返回当前存活会话集合。
///
/// 在锁内只做 map 增删与克隆，不跨 `await`。调用方用该集合在锁外按
/// `root_session_id` 判断终端是否仍有会话。
fn live_sessions(sessions: &SessionBook, now: Instant) -> HashSet<String> {
    let mut entries = lock_sessions(sessions);
    entries.retain(|_, last_seen| is_session_live(*last_seen, now, sessions.ttl));
    entries.keys().cloned().collect()
}

/// 清理过期心跳后返回存活会话及其最近心跳时间。
fn live_heartbeats(sessions: &SessionBook, now: Instant) -> HashMap<String, Instant> {
    let mut entries = lock_sessions(sessions);
    entries.retain(|_, last_seen| is_session_live(*last_seen, now, sessions.ttl));
    entries.clone()
}

/// 获取会话表锁；中毒时取回内部值，避免一次 panic 永久阻断保活判定。
fn lock_sessions(sessions: &SessionBook) -> MutexGuard<'_, HashMap<String, Instant>> {
    sessions
        .entries
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 合并存活心跳与 registry 中所有终端的 `root_session_id`，生成会话摘要。
///
/// 排序与去重由 `BTreeMap` 保证；`live` 只反映心跳是否在 TTL 内，终端是否
/// 运行不影响该标记。调用方需保证 registry 锁已释放。
fn build_session_list(
    heartbeats: HashMap<String, Instant>,
    terminals: Vec<ListItem>,
    now: Instant,
) -> SessionListResult {
    let mut sessions: BTreeMap<String, SessionInfo> = BTreeMap::new();
    for (root_session_id, last_seen) in heartbeats {
        let last_seen_ms =
            u64::try_from(now.saturating_duration_since(last_seen).as_millis()).unwrap_or(u64::MAX);
        sessions.insert(
            root_session_id.clone(),
            SessionInfo {
                root_session_id,
                live: true,
                last_seen_ms: Some(last_seen_ms),
                terminals: 0,
            },
        );
    }
    for terminal in terminals {
        let entry = sessions
            .entry(terminal.root_session_id.clone())
            .or_insert_with(|| SessionInfo {
                root_session_id: terminal.root_session_id.clone(),
                live: false,
                last_seen_ms: None,
                terminals: 0,
            });
        entry.terminals = entry.terminals.saturating_add(1);
    }
    SessionListResult {
        sessions: sessions.into_values().collect(),
    }
}
/// 判断进程树里是否存在用户子进程。
///
/// 排除终端根进程本身与控制台辅助进程（conhost/winpty/OpenConsole）；
/// 其余任一节点都视为用户子进程。
fn has_user_children(root_pid: Option<u32>, tree: &[termx_protocol::ProcNode]) -> bool {
    tree.iter()
        .filter(|node| Some(node.pid) != root_pid)
        .any(|node| !is_console_helper(&node.cmd))
}

/// 判断命令行是否属于控制台辅助进程（按可执行文件名匹配）。
fn is_console_helper(cmd: &str) -> bool {
    let executable = cmd.split_whitespace().next().unwrap_or(cmd);
    let name = executable
        .rsplit(['\\', '/'])
        .next()
        .unwrap_or(executable)
        .to_ascii_lowercase();
    CONSOLE_HELPER_NAMES.contains(&name.as_str())
}

/// 在锁外采集进程树，判断该终端是否仍有用户子进程。
///
/// 采集带超时：`tree()` 卡住时保守地按「有子进程」处理，避免误杀；
/// 后端返回空树（不支持进程树）时按「无子进程」处理。
async fn candidate_has_user_children(candidate: &ClientlessCandidate) -> bool {
    let pid = candidate.pid;
    let backend = Arc::clone(&candidate.backend);
    let collected = tokio::time::timeout(
        INTERACTION_SAMPLE_TIMEOUT,
        tokio::task::spawn_blocking(move || has_user_children(pid, &backend.tree())),
    )
    .await;
    match collected {
        Ok(Ok(has_children)) => has_children,
        Ok(Err(error)) => {
            tracing::warn!(%error, "采集进程树任务失败，按有子进程处理");
            true
        }
        Err(_) => {
            tracing::warn!(pid = ?pid, "采集进程树超时，按有子进程处理");
            true
        }
    }
}

/// 服务启动或运行期错误。
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
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
    /// 同一配置目录已有另一个 service 实例持锁运行。
    #[error("termx-service 已在运行（单实例锁被占用）: {0}")]
    AlreadyRunning(PathBuf),
}

/// 已绑定监听端口的服务实例。
pub struct Service {
    listener: TcpListener,
    local_addr: SocketAddr,
    config: ServiceConfig,
    /// 单实例锁；持有到 `Service` 被丢弃，进程退出时由 OS 自动释放。
    _lock: ServiceLock,
    token: Arc<str>,
    /// 多客户端共享的终端 registry；std Mutex 临界区不跨 `await`。
    registry: Arc<Mutex<Registry>>,
    /// 启动时读取的本地 runtime catalog；加载失败时为空。
    runtimes: Arc<RuntimeCatalog>,
    /// `web_port > 0` 时预先绑定的 Web 监听器。
    web: Option<WebServer>,
}

impl Service {
    /// 绑定 [`ServiceConfig::bind_addr`] 并写出实际端口。
    ///
    /// 仅允许环回监听；配置目录、token 文件会在绑定前准备好。默认使用
    /// [`ModeFactory`] 按 `mode` 创建真实 PTY / stdio 后端。
    pub async fn bind(config: ServiceConfig) -> Result<Self, ServiceError> {
        let factory = Arc::new(ModeFactory::with_stdio_write_queue_capacity(
            config.stdio_write_queue_capacity,
        ));
        Self::bind_with_factory(config, factory).await
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

        // 顺序契约：先拿单实例锁，再写 token、绑定主监听/Web，最后原子写 port。
        // 锁获取失败（另一个实例在运行）时绝不触碰 token/port 文件。
        let lock_path = config.service_lock_path();
        let lock = ServiceLock::acquire(&lock_path).map_err(|error| match error {
            LockError::Contended => ServiceError::AlreadyRunning(lock_path.clone()),
            LockError::Io(error) => ServiceError::Io(error),
        })?;

        let token = load_or_create_token(&config)?;
        let listener = TcpListener::bind(config.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let token: Arc<str> = Arc::from(token);
        // Web 端口在 bind 阶段就占用，端口冲突会直接启动失败，而不是运行期才暴露。
        let web = if config.web_port > 0 {
            Some(
                WebServer::bind(&config, local_addr, Arc::clone(&token))
                    .await
                    .map_err(ServiceError::Io)?,
            )
        } else {
            None
        };
        // port 是最后一步：主监听与 Web 都成功后才原子替换，失败不会覆盖旧文件。
        write_port_file(&config, local_addr.port())?;
        let mut registry = Registry::with_config(
            config.event_buffer_capacity,
            config.subscriber_queue_capacity,
            factory,
        );
        registry.set_max_terminals(config.max_terminals);
        registry.set_transcript_capacity(config.transcript_capacity);
        registry.set_interaction_config(InteractionConfig {
            enabled: config.interaction_enabled,
            no_output_threshold: config.interaction_no_output_threshold,
            repeat_interval: config.interaction_repeat_interval,
        });
        let registry = Arc::new(Mutex::new(registry));
        // runtime catalog 只读本地目录；缺失/非法时保持空 catalog，不影响旧 cmd/args。
        let (runtimes, runtime_error) = RuntimeCatalog::load(config.runtime_dir());
        if let Some(error) = runtime_error {
            tracing::warn!(
                runtime_dir = %config.runtime_dir().display(),
                %error,
                "runtime catalog 加载失败，按空 catalog 启动"
            );
        } else {
            tracing::info!(
                runtime_dir = %runtimes.dir().display(),
                runtimes = runtimes.len(),
                "runtime catalog 已加载"
            );
        }
        let runtimes = Arc::new(runtimes);

        tracing::info!(
            addr = %local_addr,
            config_dir = %config.config_dir.display(),
            "termx-service 已绑定环回地址"
        );

        Ok(Self {
            listener,
            local_addr,
            config,
            _lock: lock,
            token,
            registry,
            runtimes,
            web,
        })
    }

    /// 实际监听地址（包含由 OS 分配的端口）。
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Web 服务实际监听地址；`web_port = 0` 时为 `None`。
    pub fn web_local_addr(&self) -> Option<SocketAddr> {
        self.web.as_ref().map(WebServer::local_addr)
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

        // Web 服务与主服务同生命周期：run_until 被 abort 时守卫会中止 Web 任务，
        // 正常停机时在 accept 循环结束后立即中止。
        let web_guard = self.web.map(|web| {
            AbortOnDrop(tokio::spawn(async move {
                if let Err(error) = web.serve().await {
                    tracing::warn!(%error, "termx web 服务结束");
                }
            }))
        });

        // 空闲检查与交互识别 ticker 与 accept 循环同生命周期：shutdown 时先发
        // 停止信号，再等待两个 ticker 退出，避免后台任务悬挂。
        let (stop_tx, stop_rx) = watch::channel(false);
        let sessions = new_session_book(self.config.session_ttl);
        let idle_ticker = tokio::spawn(run_idle_ticker(
            Arc::clone(&self.registry),
            self.config.idle_tick_interval,
            self.config.client_idle_timeout,
            self.config.client_idle_policy,
            Arc::clone(&sessions),
            stop_rx.clone(),
        ));
        let interaction_ticker = tokio::spawn(run_interaction_ticker(
            Arc::clone(&self.registry),
            self.config.interaction_tick_interval,
            self.config.interaction_enabled,
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
                    let resources = ConnectionResources {
                        registry: Arc::clone(&self.registry),
                        runtimes: Arc::clone(&self.runtimes),
                        sessions: Arc::clone(&sessions),
                        max_frame_bytes: self.config.max_frame_bytes,
                        subscriber_queue_capacity: self.config.subscriber_queue_capacity,
                        remote_injection: Arc::new(self.config.remote_injection.clone()),
                        known_hosts_path: Arc::new(self.config.known_hosts_path()),
                    };
                    tokio::spawn(async move {
                        if let Err(error) = handle_connection(stream, token, resources).await {
                            tracing::debug!(peer = %peer_addr, %error, "连接处理结束");
                        }
                    });
                }
            }
        };

        let _ = stop_tx.send(true);
        let _ = idle_ticker.await;
        let _ = interaction_ticker.await;
        drop(web_guard);

        // shutdown：先停止 ticker 与新连接，再在锁外 best-effort 终止所有后端，
        // 确保 stdio/PTY 子进程不会随 service 退出而残留。
        let backends = {
            let registry = lock_registry(&self.registry);
            registry.collect_backends()
        };
        for backend in backends {
            terminate_backend(backend).await;
        }
        result
    }
}

/// 任务守卫：离开作用域时中止后台任务，避免主服务退出后 Web 监听残留。
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// 后台空闲检查：周期处理 `idle_timeout` 与「无存活会话」超时。
///
/// 锁纪律：
/// - 会话表锁只用于清理 TTL、生成存活集合快照与记录心跳，绝不跨 `await`；
/// - registry 锁内只做计时/落终态并取出后端 `Arc`，进程树采集与后端终止
///   都在锁外执行。
///
/// 保活条件：终端的 `root_session_id` 在会话表内且未超过 `session_ttl`。
/// Web 附着与终端级 `attach`/`subscribe` 均不参与保活。
async fn run_idle_ticker(
    registry: Arc<Mutex<Registry>>,
    period: Duration,
    client_idle_timeout: Duration,
    client_idle_policy: ClientIdlePolicy,
    sessions: SessionBook,
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
                let now = OffsetDateTime::now_utc();
                let now_std = Instant::now();
                let live = live_sessions(&sessions, now_std);

                let (actions, clientless) = {
                    let mut registry = lock_registry(&registry);
                    // 信息页命令的静默结束与空闲检查共用同一周期。
                    registry.finalize_quiet_transcripts(now, DEFAULT_QUIET_FINALIZE);
                    let actions = registry.expire_idle(now);
                    let clientless = if client_idle_policy == ClientIdlePolicy::Never
                        || client_idle_timeout.is_zero()
                    {
                        Vec::new()
                    } else {
                        registry.candidates_clientless(now_std, client_idle_timeout, |root| {
                            live.contains(root)
                        })
                    };
                    (actions, clientless)
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

                for candidate in clientless {
                    if client_idle_policy == ClientIdlePolicy::NoChildren
                        && candidate_has_user_children(&candidate).await
                    {
                        // 仍有用户子进程：继续保活，子进程退出后重新按超时计时。
                        let mut registry = lock_registry(&registry);
                        let _ = registry.reset_clientless(&candidate.terminal_id, Instant::now());
                        tracing::debug!(
                            terminal_id = %candidate.terminal_id,
                            root_session_id = %candidate.root_session_id,
                            "无存活会话但仍有用户子进程：继续保活"
                        );
                        continue;
                    }

                    // 锁外判定完成后重新取锁落终态；同 ID 可能已被替换或终止。
                    let backend = {
                        let mut registry = lock_registry(&registry);
                        match registry
                            .terminate_with_source(&candidate.terminal_id, CLIENT_TIMEOUT_SOURCE)
                        {
                            Ok(Some(backend)) => {
                                tracing::info!(
                                    terminal_id = %candidate.terminal_id,
                                    root_session_id = %candidate.root_session_id,
                                    policy = client_idle_policy.as_str(),
                                    "无存活会话超时：终止终端"
                                );
                                Some(backend)
                            }
                            // 已被其它路径落终态：幂等跳过。
                            Ok(None) => None,
                            Err(error) => {
                                tracing::debug!(
                                    terminal_id = %candidate.terminal_id,
                                    %error,
                                    "无存活会话超时终止失败"
                                );
                                None
                            }
                        }
                    };
                    if let Some(backend) = backend {
                        terminations.push(tokio::spawn(terminate_backend(backend)));
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

/// 后台交互识别：周期采集运行中终端的前台进程组/进程树并采样。
///
/// 锁纪律：先在锁内取后端 `Arc` 快照，再在 `spawn_blocking` 中采集
/// `foreground_pgrp` / `tree`（可能读 `/proc` 或系统调用），最后重新取锁调用
/// `sample_interaction`。每个终端由独立任务负责，采集带超时且与其它终端并行；
/// 等待采样结果时同时监听停止信号，保证单个卡住的 `tree()` 不会拖死其它终端
/// 或 `run_until` 停机。
async fn run_interaction_ticker(
    registry: Arc<Mutex<Registry>>,
    period: Duration,
    enabled: bool,
    mut stop: watch::Receiver<bool>,
) {
    if !enabled {
        // 关闭时直接等待停止信号，不产生任何后端调用。
        let _ = stop.changed().await;
        return;
    }

    let period = period.max(Duration::from_millis(1));
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // 跳过 interval 的首次立即触发，启动后按周期采样。
    interval.tick().await;

    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                let targets = {
                    let registry = lock_registry(&registry);
                    registry.running_backends()
                };
                let mut samples = Vec::with_capacity(targets.len());
                for (terminal_id, backend) in targets {
                    samples.push(tokio::spawn(sample_terminal_interaction(
                        Arc::clone(&registry),
                        terminal_id,
                        backend,
                    )));
                }
                // 采样阶段同样可被停止信号打断；卡住的任务不等待、不阻塞停机。
                let mut stopping = false;
                for sample in samples {
                    tokio::select! {
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow() {
                                stopping = true;
                                break;
                            }
                        }
                        _ = sample => {}
                    }
                }
                if stopping {
                    break;
                }
            }
        }
    }
}

/// 单个终端的交互采样任务：锁外采集前台进程组/进程树，再回锁内提交。
///
/// 采集放在 `spawn_blocking` 并加 [`INTERACTION_SAMPLE_TIMEOUT`]：超时只放弃
/// 本轮采样，不阻塞 ticker 或其它终端。提交阶段只做同步短操作。
async fn sample_terminal_interaction(
    registry: Arc<Mutex<Registry>>,
    terminal_id: TerminalId,
    backend: Arc<dyn TerminalBackend>,
) {
    let collected = tokio::task::spawn_blocking(move || {
        let fg = backend.foreground_pgrp();
        let tree = backend.tree();
        (fg, tree)
    });
    let (fg, tree) = match timeout(INTERACTION_SAMPLE_TIMEOUT, collected).await {
        Ok(Ok(sample)) => sample,
        Ok(Err(error)) => {
            tracing::debug!(terminal_id = %terminal_id, %error, "交互采样任务失败");
            return;
        }
        Err(_) => {
            tracing::debug!(terminal_id = %terminal_id, "交互采样超时，跳过本轮");
            return;
        }
    };

    let result = {
        let mut registry = lock_registry(&registry);
        registry.sample_interaction(&terminal_id, OffsetDateTime::now_utc(), fg, &tree)
    };
    match result {
        Ok(_) => {}
        // 终端在采样期间退出/被清理：跳过即可。
        Err(CoreError::NotFound) => {}
        Err(error) => {
            tracing::debug!(%error, terminal_id = %terminal_id, "交互采样失败");
        }
    }
}

/// 连接处理共享的资源与上限；在 accept 后按连接克隆引用。
struct ConnectionResources {
    /// 共享 registry。
    registry: Arc<Mutex<Registry>>,
    /// 只读 runtime catalog。
    runtimes: Arc<RuntimeCatalog>,
    /// 共享会话心跳表。
    sessions: SessionBook,
    /// 入站帧上限。
    max_frame_bytes: usize,
    /// 订阅事件队列容量。
    subscriber_queue_capacity: usize,
    /// 远端注入授权与路径配置（只读，不保存任何授权状态）。
    remote_injection: Arc<RemoteInjectionConfig>,
    /// SSH known_hosts 路径；服务端配置目录下，不来自客户端。
    known_hosts_path: Arc<PathBuf>,
}

/// 处理单个客户端连接：完成 `hello` 鉴权，成功则保持连接。
async fn handle_connection(
    stream: TcpStream,
    token: Arc<str>,
    resources: ConnectionResources,
) -> Result<(), ServiceError> {
    let peer_addr = stream.peer_addr().ok();
    let (mut ws, auth_header) = accept_with_auth_header(stream).await?;

    let Some(first) = read_first_message(&mut ws, peer_addr).await else {
        return Ok(());
    };

    if frame_len(&first) > resources.max_frame_bytes {
        let response = error_response(String::new(), ErrorCode::BadRequest, "帧超过上限");
        send_response(&mut ws, &response).await?;
        let _ = ws.close(None).await;
        return Ok(());
    }

    match evaluate_handshake(&first, &token, auth_header.as_deref()) {
        Handshake::Accepted { id, client, caller } => {
            handle_accepted(
                ws,
                AcceptedConnection {
                    peer_addr,
                    id,
                    client,
                    caller,
                    resources,
                },
            )
            .await
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

/// 已完成鉴权、待保持的连接参数（避免过多参数）。
struct AcceptedConnection {
    peer_addr: Option<SocketAddr>,
    id: String,
    client: String,
    caller: Caller,
    resources: ConnectionResources,
}

/// 鉴权通过：回 `hello` 结果并保持连接。
async fn handle_accepted<S>(
    mut ws: WebSocketStream<S>,
    connection: AcceptedConnection,
) -> Result<(), ServiceError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let AcceptedConnection {
        peer_addr,
        id,
        client,
        caller,
        resources,
    } = connection;
    // 每连接生成唯一的连接级 client_id；`hello.client` 只作为日志标签，
    // 不参与订阅键、attached/detached 事件或 lease 归属，避免同名连接撞键。
    let client_id = Ulid::generate().to_string();
    tracing::info!(
        ?peer_addr,
        client = %sanitize_log_value(&client),
        %client_id,
        root_session_id = %sanitize_log_value(caller.root_session_id.as_deref().unwrap_or("-")),
        admin = caller.admin,
        "hello 鉴权成功"
    );
    let response = hello_response(id)?;
    // `hello.capabilities.runtime_catalog` 是服务级声明；具体条目由
    // `runtime.list` 返回，加载失败时该列表为空。
    send_response(&mut ws, &response).await?;
    serve_connection(ws, peer_addr, client_id, caller, resources).await
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
            runtime_catalog: true,
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
    caller: Caller,
    resources: ConnectionResources,
) -> Result<(), ServiceError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let ConnectionResources {
        registry,
        runtimes,
        sessions,
        max_frame_bytes,
        subscriber_queue_capacity,
        remote_injection,
        known_hosts_path,
    } = resources;
    let (sink, mut stream) = ws.split();
    let (out_tx, out_rx) = unbounded_channel::<Message>();
    let mut writer = tokio::spawn(write_loop(sink, out_rx));

    // 订阅事件走有界队列；队列满时的分流规则由 registry 负责（见 ASYNC.md）。
    let (event_tx, event_rx) = channel::<Event>(subscriber_queue_capacity.max(1));
    // registry 无法投递状态事件时通过该 watch 通知 service 断开连接。
    let (throttle_tx, throttle_rx) = watch::channel(false);
    // forwarder 因限流决定断开时，用该 watch 停止读循环。
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    {
        let mut registry = lock_registry(&registry);
        registry.register_client(&client_id, throttle_tx);
    }
    let forwarder = tokio::spawn(forward_events(
        event_rx,
        out_tx.clone(),
        throttle_rx,
        shutdown_tx,
    ));

    let mut context = ReadContext {
        peer_addr,
        client_id: &client_id,
        caller: &caller,
        registry: &registry,
        runtimes: &runtimes,
        sessions: &sessions,
        remote_injection: &remote_injection,
        known_hosts_path: known_hosts_path.as_path(),
        out_tx: &out_tx,
        event_tx: &event_tx,
        shutdown: &mut shutdown_rx,
        max_frame_bytes,
    };
    let result = read_loop(&mut stream, &mut context).await;

    // 连接结束：先释放该连接持有的 lease（避免终端被永久锁死），再清理订阅，
    // 防止 registry 持有失效 sender；最后注销限流通知。
    {
        let mut registry = lock_registry(&registry);
        registry.release_all(&client_id);
        if let Err(error) = registry.detach_all(&client_id, DETACH_REASON_DISCONNECT) {
            tracing::debug!(?peer_addr, %error, "清理订阅失败");
        }
        registry.unregister_client(&client_id);
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
///
/// 同时监听连接级限流信号：registry 无法投递状态类事件时置 `true`，这里先回
/// `error{code:"limit_exceeded",retryable:true}`，再通知读循环收尾并关闭连接。
/// 选择「错误帧 + 断开」而不是 `detached`：状态事件已无法保证送达，必须让
/// 客户端明确重连并按 cursor 补齐，不能伪装成正常 detach。
async fn forward_events(
    mut event_rx: Receiver<Event>,
    out_tx: UnboundedSender<Message>,
    mut throttle: watch::Receiver<bool>,
    shutdown: watch::Sender<bool>,
) {
    loop {
        tokio::select! {
            changed = throttle.changed() => {
                let throttled = *throttle.borrow();
                match (changed, throttled) {
                    // 正常连接收尾时 registry 会 drop 发送端：此时不应误报限流。
                    (Err(_), false) => break,
                    (Ok(()), true) | (Err(_), true) => {
                        send_limit_exceeded(&out_tx);
                        let _ = shutdown.send(true);
                        break;
                    }
                    (Ok(()), false) => {}
                }
            }
            event = event_rx.recv() => {
                let Some(event) = event else {
                    break;
                };
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
    }
}

/// 发送订阅队列溢出的错误帧；无 `id` 表示连接级错误而非请求响应。
fn send_limit_exceeded(out_tx: &UnboundedSender<Message>) {
    let response = Response {
        id: String::new(),
        ok: false,
        result: None,
        error: Some(ProtocolError {
            code: ErrorCode::LimitExceeded,
            message: LIMIT_EXCEEDED_MESSAGE.to_string(),
            retryable: true,
            terminal_id: None,
            injection: None,
            host_key: None,
        }),
    };
    match to_json(&response) {
        Ok(text) => {
            let _ = out_tx.send(Message::Text(text.into()));
        }
        Err(error) => tracing::warn!(%error, "limit_exceeded 帧序列化失败"),
    }
}

/// 读循环：处理请求并交给同一 out 队列写回响应。
/// 读循环所需的连接级上下文，避免参数列表过长。
struct ReadContext<'a> {
    /// 对端地址（仅日志）。
    peer_addr: Option<SocketAddr>,
    /// 连接级客户端 ID。
    client_id: &'a str,
    /// 连接级调用者身份。
    caller: &'a Caller,
    /// 共享 registry。
    registry: &'a Arc<Mutex<Registry>>,
    /// 只读 runtime catalog。
    runtimes: &'a Arc<RuntimeCatalog>,
    /// 共享会话心跳表。
    sessions: &'a SessionBook,
    /// 远端注入配置；`terminal.create` 组装内部计划时只读。
    remote_injection: &'a RemoteInjectionConfig,
    /// 服务端 known_hosts 路径。
    known_hosts_path: &'a Path,
    /// 连接出站队列。
    out_tx: &'a UnboundedSender<Message>,
    /// 订阅事件发送端。
    event_tx: &'a Sender<Event>,
    /// forwarder 触发的连接关闭信号。
    shutdown: &'a mut watch::Receiver<bool>,
    /// 入站帧上限。
    max_frame_bytes: usize,
}

async fn read_loop<S>(
    stream: &mut SplitStream<WebSocketStream<S>>,
    context: &mut ReadContext<'_>,
) -> Result<(), ServiceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = tokio::select! {
            // forwarder 因订阅队列溢出决定断开：停止读取，交由收尾流程关闭连接。
            changed = context.shutdown.changed() => {
                if changed.is_err() || *context.shutdown.borrow() {
                    break;
                }
                continue;
            }
            message = stream.next() => match message {
                Some(Ok(message)) => message,
                Some(Err(error)) => {
                    tracing::debug!(peer_addr = ?context.peer_addr, %error, "连接读取失败");
                    return Ok(());
                }
                None => break,
            },
        };

        if frame_len(&message) > context.max_frame_bytes {
            tracing::warn!(
                peer_addr = ?context.peer_addr,
                max_frame_bytes = context.max_frame_bytes,
                "入站帧超过上限，断开连接"
            );
            let response = error_response(String::new(), ErrorCode::BadRequest, "帧超过上限");
            let text = to_json(&response)?;
            let _ = context.out_tx.send(Message::Text(text.into()));
            return Ok(());
        }

        match message {
            Message::Text(text) => {
                handle_rpc_text(
                    text.as_str(),
                    context.client_id,
                    context.caller,
                    context.registry,
                    context.runtimes,
                    context.sessions,
                    context.remote_injection,
                    context.known_hosts_path,
                    context.out_tx,
                    context.event_tx,
                )
                .await?
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

/// 解析请求并分发；非法 JSON 回 `protocol_error`。
//
// 连接上下文（身份、registry、runtime、会话表、输出通道）都是独立借用，
// 拆成结构体会把生命周期参数扩散到整个分发层；此处保持显式参数。
#[allow(clippy::too_many_arguments)]
async fn handle_rpc_text(
    text: &str,
    client_id: &str,
    caller: &Caller,
    registry: &Arc<Mutex<Registry>>,
    runtimes: &Arc<RuntimeCatalog>,
    sessions: &SessionBook,
    remote_injection: &RemoteInjectionConfig,
    known_hosts_path: &Path,
    out_tx: &UnboundedSender<Message>,
    event_tx: &Sender<Event>,
) -> Result<(), ServiceError> {
    let response = match from_json::<Request>(text) {
        Ok(request) => {
            dispatch_request(
                request,
                client_id,
                caller,
                registry,
                runtimes,
                sessions,
                remote_injection,
                known_hosts_path,
                event_tx,
            )
            .await?
        }
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
//
// 连接级依赖（身份、registry、runtime、会话表、注入配置、事件通道）都是独立
// 借用，拆结构体会把生命周期扩散到整个分发层；与 `handle_rpc_text` 保持一致。
#[allow(clippy::too_many_arguments)]
async fn dispatch_request(
    request: Request,
    client_id: &str,
    caller: &Caller,
    registry: &Arc<Mutex<Registry>>,
    runtimes: &Arc<RuntimeCatalog>,
    sessions: &SessionBook,
    remote_injection: &RemoteInjectionConfig,
    known_hosts_path: &Path,
    event_tx: &Sender<Event>,
) -> Result<Response, ServiceError> {
    let Request { id, method, params } = request;

    match method.as_str() {
        "session.list" => {
            if !caller.admin {
                return Ok(error_response(
                    id,
                    ErrorCode::PermissionDenied,
                    "session.list 仅管理员可用",
                ));
            }
            let _params: SessionListParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("session.list 参数非法: {error}"),
                    ));
                }
            };

            // 会话表与 registry 分别加锁、从不同时持有：先生成心跳快照，
            // 再在 registry 锁内取终端摘要，最后在锁外合并。
            let now = Instant::now();
            let heartbeats = live_heartbeats(sessions, now);
            let terminals = {
                let registry = lock_registry(registry);
                registry.list(&ListParams::default())
            };
            let value = serde_json::to_value(build_session_list(heartbeats, terminals, now))?;
            Ok(ok_response(id, value))
        }
        "runtime.list" => {
            let result = RuntimeListResult {
                runtimes: runtimes
                    .list(
                        &termx_runtime::current_platform(),
                        &termx_runtime::current_arch(),
                    )
                    .into_iter()
                    .map(|runtime| RuntimeInfo {
                        id: runtime.id,
                        name: runtime.name,
                        version: runtime.version,
                        platform: runtime.platform,
                        arch: runtime.arch,
                        available: runtime.available,
                    })
                    .collect(),
            };
            Ok(ok_response(id, serde_json::to_value(result)?))
        }
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
            handle_create(
                id,
                params,
                caller,
                registry,
                runtimes,
                remote_injection,
                known_hosts_path,
            )
            .await
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
            let value = serde_json::to_value(registry.list_for(&params, caller.scope()))?;
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
                let locator = match authorize_locator(&registry, &params.locator, caller) {
                    Ok(locator) => locator,
                    Err(error) => return Ok(response_from_core_error(id, error)),
                };
                match registry.status_parts(&locator) {
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
            let locator = match authorize_locator(&registry, &params.locator, caller) {
                Ok(locator) => locator,
                Err(error) => return Ok(response_from_core_error(id, error)),
            };
            match registry.attach(&locator, client_id, params.cursor, event_tx.clone()) {
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
            let locator = match authorize_locator(&registry, &params.locator, caller) {
                Ok(locator) => locator,
                Err(error) => return Ok(response_from_core_error(id, error)),
            };
            match detach_terminal(&mut registry, &locator, client_id) {
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
            let locator = match authorize_locator(&registry, &params.locator, caller) {
                Ok(locator) => locator,
                Err(error) => return Ok(response_from_core_error(id, error)),
            };
            match registry.claim(&locator, client_id, params.force) {
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
            let locator = match authorize_locator(&registry, &params.locator, caller) {
                Ok(locator) => locator,
                Err(error) => return Ok(response_from_core_error(id, error)),
            };
            match registry.release(&locator, client_id) {
                Ok(()) => Ok(ok_response(
                    id,
                    serde_json::to_value(SimpleResult { ok: true })?,
                )),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.read" => {
            let params: ReadParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.read 参数非法: {error}"),
                    ));
                }
            };

            // 只读扫描 ring：不注册订阅、不推进消费状态，持锁时间与回放长度成正比。
            let registry = lock_registry(registry);
            let locator = match authorize_locator(&registry, &params.locator, caller) {
                Ok(locator) => locator,
                Err(error) => return Ok(response_from_core_error(id, error)),
            };
            match registry.read(&locator, params.cursor, params.max_bytes) {
                Ok(result) => Ok(ok_response(id, serde_json::to_value(result)?)),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.output" => {
            let params: OutputParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.output 参数非法: {error}"),
                    ));
                }
            };

            // 与 read 一致：只读扫描事件环，不注册订阅、不 cross await 持锁。
            let registry = lock_registry(registry);
            let locator = match authorize_locator(&registry, &params.locator, caller) {
                Ok(locator) => locator,
                Err(error) => return Ok(response_from_core_error(id, error)),
            };
            match registry.output_text(&locator, &params) {
                Ok(result) => Ok(ok_response(id, serde_json::to_value(result)?)),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "terminal.transcript" => {
            let params: TranscriptParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.transcript 参数非法: {error}"),
                    ));
                }
            };

            // 只读有界缓冲，锁内复制返回条目；不做订阅或任何 await。
            let registry = lock_registry(registry);
            let locator = match authorize_locator(&registry, &params.locator, caller) {
                Ok(locator) => locator,
                Err(error) => return Ok(response_from_core_error(id, error)),
            };
            match registry.transcript(&locator, params.limit) {
                Ok(result) => Ok(ok_response(id, serde_json::to_value(result)?)),
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
            handle_send(id, params, client_id, caller, registry).await
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
            handle_resize(id, params, caller, registry).await
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
            handle_signal(id, params, caller, registry).await
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
            handle_terminate(id, params, caller, registry).await
        }
        "terminal.rebind" => {
            let params: RebindParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("terminal.rebind 参数非法: {error}"),
                    ));
                }
            };
            handle_rebind(id, params, caller, registry).await
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
                registry.prune_for(&params, caller.scope())
            };
            match result {
                Ok(result) => Ok(ok_response(id, serde_json::to_value(result)?)),
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
            // 全局订阅会跨会话接收事件，仅管理员可用；普通连接只能显式订阅
            // 本会话终端（下面逐个鉴权）。
            if params.all == Some(true) && !caller.admin {
                return Ok(error_response(
                    id,
                    ErrorCode::PermissionDenied,
                    "terminal.subscribe(all=true) 仅管理员可用",
                ));
            }

            let mut registry = lock_registry(registry);
            match subscribe_terminals(&mut registry, params, client_id, caller, event_tx) {
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
            match unsubscribe_terminals(&mut registry, params, client_id, caller) {
                Ok(()) => Ok(ok_response(
                    id,
                    serde_json::to_value(SimpleResult { ok: true })?,
                )),
                Err(error) => Ok(response_from_core_error(id, error)),
            }
        }
        "heartbeat" => {
            // 新式心跳携带 root_session_id，用于终端保活；旧客户端发送 `{}`
            // 时只刷新连接级附着时间，不影响终端保活。
            let params: HeartbeatParams = serde_json::from_value(params).unwrap_or_default();
            if let Some(root_session_id) = params
                .root_session_id
                .as_deref()
                .map(str::trim)
                .filter(|root| !root.is_empty())
            {
                if !caller.can_access_root(root_session_id) {
                    return Ok(error_response(
                        id,
                        ErrorCode::PermissionDenied,
                        "heartbeat 的 root_session_id 与连接会话不一致",
                    ));
                }
                record_session_heartbeat(sessions, root_session_id, Instant::now());
            }
            {
                let mut registry = lock_registry(registry);
                registry.heartbeat(client_id, OffsetDateTime::now_utc());
            }
            Ok(ok_response(
                id,
                serde_json::to_value(SimpleResult { ok: true })?,
            ))
        }
        "session_closed" => {
            let params: SessionClosedParams = match serde_json::from_value(params) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(error_response(
                        id,
                        ErrorCode::BadRequest,
                        format!("session_closed 参数非法: {error}"),
                    ));
                }
            };
            let root_session_id = params.root_session_id.trim();
            if root_session_id.is_empty() {
                return Ok(error_response(
                    id,
                    ErrorCode::BadRequest,
                    "session_closed 需要 root_session_id",
                ));
            }
            if !caller.can_access_root(root_session_id) {
                return Ok(error_response(
                    id,
                    ErrorCode::PermissionDenied,
                    "session_closed 的 root_session_id 与连接会话不一致",
                ));
            }
            remove_session(sessions, root_session_id);
            Ok(ok_response(
                id,
                serde_json::to_value(SimpleResult { ok: true })?,
            ))
        }
        _ => Ok(error_response(
            id,
            ErrorCode::BadRequest,
            format!("方法尚未实现: {method}"),
        )),
    }
}

/// SSH 远端程序是否需要 helper（按 basename 匹配 `termx-pwsh`）。
///
/// Web 的 SSH 表单固定发送 `termx-pwsh`；普通 ssh 由本地自定义程序承载。
/// 旧客户端仍可直接发 `cmd`，因此按 basename 判断，兼容
/// `/usr/local/bin/termx-pwsh` 与 Windows 风格分隔符。
fn ssh_needs_helper(cmd: &str) -> bool {
    cmd.trim()
        .rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name == SHELL_EVENT_RUNTIME_ID)
}

/// 默认私钥候选（相对用户 home，按探测顺序）。
const DEFAULT_SSH_KEY_CANDIDATES: [&str; 2] = [".ssh/id_ed25519", ".ssh/id_rsa"];

/// 解析 SSH 私钥路径：显式路径优先且不改写；留空时探测本机默认私钥。
fn resolve_ssh_private_key(path: &str) -> Result<String, String> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from);
    resolve_ssh_private_key_in(path, home.as_deref())
}

/// [`resolve_ssh_private_key`] 的可测试实现；`home` 为 `None` 表示无法定位用户目录。
fn resolve_ssh_private_key_in(path: &str, home: Option<&Path>) -> Result<String, String> {
    let explicit = path.trim();
    if !explicit.is_empty() {
        return Ok(explicit.to_string());
    }
    let found = home.and_then(find_default_private_key);
    found
        .map(|path| path.to_string_lossy().into_owned())
        .ok_or_else(|| {
            "私钥认证路径为空，且未找到本机默认私钥 ~/.ssh/id_ed25519 或 ~/.ssh/id_rsa；请填写私钥路径"
                .to_string()
        })
}

/// 按固定顺序返回第一个存在的默认私钥。
fn find_default_private_key(home: &Path) -> Option<PathBuf> {
    DEFAULT_SSH_KEY_CANDIDATES
        .iter()
        .map(|relative| home.join(relative))
        .find(|candidate| candidate.is_file())
}

/// 创建终端：预检 → 锁外终止旧后端 → 锁外 spawn → 锁内注册 → 启动事件摄取。
///
/// 并发契约：registry 的 `std::sync::Mutex` 只在同步短操作中持有；
/// `spawn` / `terminate` / `foreground_pgrp` 全部放到 `spawn_blocking`，
/// 且不在持锁期间等待。
async fn handle_create(
    id: String,
    mut params: CreateParams,
    caller: &Caller,
    registry: &Arc<Mutex<Registry>>,
    runtimes: &RuntimeCatalog,
    remote_injection: &RemoteInjectionConfig,
    known_hosts_path: &Path,
) -> Result<Response, ServiceError> {
    // runtime_id 指向本机 runtime，ssh 指向远端主机，两者语义冲突，先拒绝再解析。
    if params.runtime_id.is_some() && params.ssh.is_some() {
        return Ok(error_response(
            id,
            ErrorCode::BadRequest,
            "runtime_id 与 ssh 不能同时使用".to_string(),
        ));
    }
    // runtime_id 与自定义 cmd/args 互斥；解析失败直接返回明确错误，不回退到
    // 请求里的 cmd，避免“选了内置 runtime 却跑了任意程序”。
    if let Some(runtime_id) = params.runtime_id.clone() {
        if !params.cmd.trim().is_empty() || !params.args.is_empty() {
            return Ok(error_response(
                id,
                ErrorCode::BadRequest,
                "runtime_id 与自定义 cmd/args 不能同时使用".to_string(),
            ));
        }
        match runtimes.resolve(&runtime_id) {
            Ok(runtime) => {
                params.cmd = runtime.bin_path.to_string_lossy().into_owned();
                params.args = runtime.args;
            }
            Err(error) => return Ok(runtime_error_response(id, &error)),
        }
    }
    // 远端注入计划：SSH + termx-pwsh（或旧请求显式要求）视为需要 helper，
    // 由 service 从生效的 remote injection 自动填充 helper 字段；只读预检固定
    // deny。授权只来自本次请求，不写入 registry 或任何状态。
    if params.ssh.is_some() {
        let allow_injection = params.allow_injection;
        let needs_helper = ssh_needs_helper(&params.cmd);
        if let Some(ssh) = params.ssh.as_mut() {
            // known_hosts 只来自服务端配置目录，客户端无法覆盖。
            ssh.known_hosts_path = Some(known_hosts_path.to_path_buf());
            if let SshAuth::Key { path, .. } = &mut ssh.auth {
                match resolve_ssh_private_key(path) {
                    Ok(resolved) => *path = resolved,
                    Err(message) => {
                        return Ok(error_response(id, ErrorCode::BadRequest, message));
                    }
                }
            }
            if let Err(error) = remote_injection.plan_for_ssh(ssh, allow_injection, needs_helper) {
                return Ok(error_response(
                    id,
                    ErrorCode::BadRequest,
                    format!("远端注入 helper 路径非法: {error}"),
                ));
            }
        }
    }
    // 只有本地 PTY 的 termx-pwsh 使用带外通道；SSH/其他 runtime 保持现状。
    let event_channel_eligible = params.mode == Mode::Pty
        && params.ssh.is_none()
        && params.runtime_id.as_deref() == Some(SHELL_EVENT_RUNTIME_ID);
    let event_channel = if event_channel_eligible {
        match ShellEventChannel::open() {
            Ok(channel) => {
                let env = params.env.get_or_insert_with(BTreeMap::new);
                env.insert(
                    EVENT_ENDPOINT_ENV.to_string(),
                    channel.endpoint().to_string(),
                );
                env.insert(EVENT_TOKEN_ENV.to_string(), channel.token().to_string());
                Some(channel)
            }
            Err(error) => {
                // 通道失败不阻断创建：transcript 退化为静默判定，不伪造退出码。
                tracing::warn!(%error, "shell 事件通道创建失败，降级为 best-effort");
                None
            }
        }
    } else {
        None
    };

    // 只做参数/冲突预检，不提前终止旧后端：否则旧进程退出的 `exited` 可能
    // 抢在 `register_create` 落 `interrupted` 之前。置换统一在锁内完成。
    let factory = {
        let registry = lock_registry(registry);
        if let Err(error) = caller.require_root(&params.root_session_id) {
            return Ok(response_from_core_error(id, error));
        }
        if let Err(error) = registry.prepare_create(&params) {
            return Ok(response_from_core_error(id, error));
        }
        registry.backend_factory()
    };

    let (backend, events) = match spawn_backend(factory, params.clone()).await {
        Ok(spawned) => spawned,
        Err(error) => return Ok(response_from_core_error(id, error)),
    };
    // 远端事件是否生效由后端快照决定：只有 ssh + pty + termx-pwsh + helper
    // `shell_events` 能力 + `started.events=true` 才会为 true。普通 SSH 保持
    // in-band OSC；本地通道逻辑不变。
    let remote_events = backend.shell_events();
    // 只有本地通道真正建立或远端 helper 通道生效时才关闭 in-band fallback；
    // 本地通道创建失败时仍保留 send/in-band 记录，避免 transcript 丢失。
    let event_channel_active = event_channel.is_some() || remote_events;
    let foreground_pgrp = read_foreground_pgrp(Arc::clone(&backend)).await;

    let registered = {
        let mut registry = lock_registry(registry);
        registry.register_create_with(
            params,
            Arc::clone(&backend),
            foreground_pgrp,
            !event_channel_active,
        )
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
    if let Some(channel) = event_channel {
        let (receiver, cancel) = channel.split();
        tokio::spawn(ingest_shell_events(
            Arc::clone(registry),
            TerminalId::new(result.terminal_id.clone()),
            receiver,
            cancel,
        ));
    }
    Ok(ok_response(id, serde_json::to_value(result)?))
}

/// 处理 `terminal.send`：锁内做 lease 门禁取后端，锁外阻塞写。
async fn handle_send(
    id: String,
    params: SendParams,
    client_id: &str,
    caller: &Caller,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    // 先在无锁阶段解码载荷（保留原有错误优先级），拿到终端模式后再追加换行。
    let mut payload = match send_payload(&params) {
        Ok(payload) => payload,
        Err(error) => return Ok(response_from_core_error(id, error)),
    };

    // 只有 `enter=true` 才视为一条命令；按键/密码等原始输入不进 transcript。
    let command_text = params
        .enter
        .then(|| String::from_utf8_lossy(&payload).into_owned());
    let (backend, mode, wrapped) = {
        let mut registry = lock_registry(registry);
        let locator = match authorize_locator(&registry, &params.locator, caller) {
            Ok(locator) => locator,
            Err(error) => return Ok(response_from_core_error(id, error)),
        };
        let backend = match registry.prepare_send(&locator, client_id) {
            Ok(backend) => backend,
            Err(error) => return Ok(response_from_core_error(id, error)),
        };
        let mode = match registry.mode(&locator) {
            Ok(mode) => mode,
            Err(error) => return Ok(response_from_core_error(id, error)),
        };
        // 与 prepare_send 同锁记录，保证新命令排在本次写入产生的输出之前；
        // 鉴权/lease 已通过，因此不会记录被拒绝的输入。事件通道终端改由宿主
        // `C` 建记录，这里跳过，避免 send 与宿主产生两条来源。
        if let (Some(text), Some(raw_id)) = (&command_text, &locator.terminal_id) {
            match registry.records_transcript_on_send(&locator) {
                Ok(true) => {
                    let terminal_id = TerminalId::new(raw_id.clone());
                    if let Err(error) = registry.record_transcript_command(
                        &terminal_id,
                        text,
                        OffsetDateTime::now_utc(),
                    ) {
                        tracing::debug!(%terminal_id, %error, "记录 transcript 命令失败");
                    }
                }
                Ok(false) => {}
                Err(error) => return Ok(response_from_core_error(id, error)),
            }
        }
        // 仅本地 PTY 的单行 text 命令尝试追加 marker；data/SSH/stdio/cmd 一律降级。
        let wrapped = TRANSCRIPT_MARKER_ENABLED
            .then(|| wrap_local_command(&registry, &locator, &params, mode))
            .flatten();
        (backend, mode, wrapped)
    };
    if let Some(wrapped) = wrapped {
        payload = wrapped.into_bytes();
    }
    if params.enter {
        // 协议 §6：pty 回车为 `\r`，stdio 为 `\n`。
        payload.push(enter_byte(mode));
    }

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
    caller: &Caller,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    let (cols, rows) = (params.cols, params.rows);
    let backend = {
        let mut registry = lock_registry(registry);
        let locator = match authorize_locator(&registry, &params.locator, caller) {
            Ok(locator) => locator,
            Err(error) => return Ok(response_from_core_error(id, error)),
        };
        match registry.prepare_resize(&locator, cols, rows) {
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
    caller: &Caller,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    let prepared = {
        let mut registry = lock_registry(registry);
        let locator = match authorize_locator(&registry, &params.locator, caller) {
            Ok(locator) => locator,
            Err(error) => return Ok(response_from_core_error(id, error)),
        };
        match registry.prepare_signal(&locator, &params.signal) {
            Ok(prepared) => prepared,
            Err(error) => return Ok(response_from_core_error(id, error)),
        }
    };

    let terminal_id = prepared.terminal_id.clone();
    let signal = prepared.signal.clone();
    // 信号发送在锁外阻塞执行；只有成功返回后才记录 pending_interrupt，
    // 失败（未知信号、进程已退出等）不得把随后的正常退出误报为 interrupted。
    match tokio::task::spawn_blocking(move || prepared.execute()).await {
        Ok(Ok(())) => {
            let mut registry = lock_registry(registry);
            if let Err(error) = registry.confirm_signal(&terminal_id, &signal) {
                tracing::debug!(terminal_id = %terminal_id, %error, "记录信号中断意图失败");
            }
            Ok(ok_response(
                id,
                serde_json::to_value(SimpleResult { ok: true })?,
            ))
        }
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
    caller: &Caller,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    let backend = {
        let mut registry = lock_registry(registry);
        let locator = match authorize_locator(&registry, &params.locator, caller) {
            Ok(locator) => locator,
            Err(error) => return Ok(response_from_core_error(id, error)),
        };
        match registry.prepare_terminate(&locator) {
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

/// 处理 `terminal.rebind`：锁内迁移绑定键，锁外终止被 `force` 置换的目标旧终端。
///
/// `force` 的目标旧终端在 registry 锁内落 `interrupted{source:"terminate"}` 并
/// 从索引移除；其真实进程终止必须等锁释放后执行。
async fn handle_rebind(
    id: String,
    params: RebindParams,
    caller: &Caller,
    registry: &Arc<Mutex<Registry>>,
) -> Result<Response, ServiceError> {
    let locator = TerminalLocator {
        terminal_id: Some(params.terminal_id),
        root_session_id: None,
        name: None,
    };
    let (result, displaced) = {
        let mut registry = lock_registry(registry);
        // 迁移授权是「目标会话拉取」：普通连接只能把终端迁到自己的会话，
        // 不要求当前已能访问源终端（跨会话搬移由目标会话发起或管理员执行）。
        if let Err(error) = caller.require_root(&params.root_session_id) {
            return Ok(response_from_core_error(id, error));
        }
        match registry.rebind(
            &locator,
            &params.root_session_id,
            params.name.as_deref(),
            params.force,
        ) {
            Ok(rebound) => rebound,
            Err(error) => return Ok(response_from_core_error(id, error)),
        }
    };
    if let Some(backend) = displaced {
        terminate_backend(backend).await;
    }

    Ok(ok_response(id, serde_json::to_value(result)?))
}

/// 解码 `terminal.send` 的输入字节（不含 `enter` 换行）。
///
/// `data` 优先于 `text`（对齐 PROTOCOL §6）；两者都缺省或 base64 非法时返回
/// `bad_request`。
fn send_payload(params: &SendParams) -> Result<Vec<u8>, CoreError> {
    if let Some(data) = &params.data {
        BASE64_STANDARD
            .decode(data)
            .map_err(|error| CoreError::BadRequest(format!("terminal.send.data 非法: {error}")))
    } else if let Some(text) = &params.text {
        Ok(text.as_bytes().to_vec())
    } else {
        Err(CoreError::BadRequest(
            "terminal.send 需要 data 或 text".to_string(),
        ))
    }
}

/// `enter=true` 时追加的换行字节：pty 用 `\r`，stdio 用 `\n`。
fn enter_byte(mode: Mode) -> u8 {
    match mode {
        Mode::Pty => b'\r',
        Mode::Stdio => b'\n',
    }
}

/// 为本地 PTY 的单行 `text` 命令构造带 shell marker 的发送文本。
///
/// 仅当 `enter=true && mode=pty && data 缺省 && text 存在` 时尝试；shell 为
/// cmd/未知（含 SSH 的 `shell`）或命令不适合单行执行时返回 `None`，调用方
/// 保持原文本发送（显式降级，不伪造完成）。
fn wrap_local_command(
    registry: &Registry,
    locator: &TerminalLocator,
    params: &SendParams,
    mode: Mode,
) -> Option<String> {
    if !params.enter || mode != Mode::Pty || params.data.is_some() {
        return None;
    }
    // SSH 远端 shell 不做本地包装（无论其启动程序是什么）。
    if registry.is_ssh(locator).unwrap_or(true) {
        return None;
    }
    let text = params.text.as_deref()?;
    let kind = marker::shell_kind(registry.shell_command(locator).ok()?);
    marker::wrap_command(kind, text)
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

/// 把 `termx-proc` 的输出流来源映射为协议 `Stream`。
fn protocol_stream(stream: OutputStream) -> Stream {
    match stream {
        OutputStream::Pty => Stream::Pty,
        OutputStream::Stdout => Stream::Stdout,
        OutputStream::Stderr => Stream::Stderr,
    }
}

/// 事件摄取任务：把 `termx-proc` 事件翻译为协议事件并写入 registry。
///
/// 每次只在同步块内取 registry 锁；收到终态事件后退出，避免后台任务泄漏。
async fn ingest_proc_events(
    registry: Arc<Mutex<Registry>>,
    terminal_id: TerminalId,
    mut events: Receiver<ProcEvent>,
) {
    // 远端 helper 与本地通道共用同一套 command-id + 代次判定；跟踪器跨事件保留。
    let mut tracker = ShellEventTracker::new();
    while let Some(event) = events.recv().await {
        let (result, terminal) = {
            let mut registry = registry
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            apply_proc_event(
                &mut registry,
                &terminal_id,
                &mut tracker,
                event,
                OffsetDateTime::now_utc(),
            )
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

/// 在已持有的 registry 锁内处理一条进程事件。
///
/// 返回值第二项表示是否为终态事件。所有 registry 操作保持同步短路径；
/// `ShellEvent` 复用本地通道的 tracker 语义，不是终态。
fn apply_proc_event(
    registry: &mut Registry,
    terminal_id: &TerminalId,
    tracker: &mut ShellEventTracker,
    event: ProcEvent,
    now: OffsetDateTime,
) -> (Result<(), CoreError>, bool) {
    match event {
        ProcEvent::Output { data, stream } => {
            let reported = registry.report_event(
                terminal_id,
                EventKind::Output {
                    data: BASE64_STANDARD.encode(&data),
                    stream: protocol_stream(stream),
                },
            );
            // 同一把锁内写入有界 transcript（短操作），再喂交互识别器。
            let result = match reported {
                Ok(_) => {
                    let text = String::from_utf8_lossy(&data);
                    if let Err(error) = registry.record_transcript_output(terminal_id, &text, now) {
                        tracing::debug!(%terminal_id, %error, "记录 transcript 输出失败");
                    }
                    registry.observe_output(terminal_id, &data, now).map(|_| ())
                }
                Err(error) => Err(error),
            };
            (result, false)
        }
        // 退出结果统一交给 report_exit：由 registry 结合 pending 决定
        // 发 interrupted（Ctrl-C / terminate / signal）还是 exited。
        ProcEvent::Exited(exit_code) => (
            registry
                .report_exit(terminal_id, ExitOutcome::Code(exit_code))
                .map(|_| ()),
            true,
        ),
        ProcEvent::Signaled(signal) => (
            registry
                .report_exit(terminal_id, ExitOutcome::Signal(signal))
                .map(|_| ()),
            true,
        ),
        // 远端 helper 的带外事件与本地通道同语义：C 绑定代次，只有
        // command-id + 代次都匹配的 D 才结束 transcript；不是终态。
        ProcEvent::ShellEvent(event) => (
            crate::shell_events::apply_event(registry, terminal_id, tracker, &event, now)
                .map(|_| ()),
            false,
        ),
    }
}

/// 带外 shell 事件摄取任务。
///
/// 只把 command-id 与代次都匹配的 `D` 交给
/// [`Registry::record_transcript_end_for_generation`]；迟到/不匹配的 `D` 被
/// 忽略，不会结束新命令。通道断开或终端被移除后退出，
/// `cancel` 随任务一起释放，关闭 listener 并清理 socket/pipe。
async fn ingest_shell_events(
    registry: Arc<Mutex<Registry>>,
    terminal_id: TerminalId,
    mut events: Receiver<crate::shell_events::ShellEvent>,
    _cancel: ShellEventCancel,
) {
    let mut tracker = ShellEventTracker::new();
    while let Some(event) = events.recv().await {
        let applied = {
            let mut registry = lock_registry(&registry);
            crate::shell_events::apply_event(
                &mut registry,
                &terminal_id,
                &mut tracker,
                &event,
                OffsetDateTime::now_utc(),
            )
        };
        match applied {
            Ok(_) => {}
            // 终端被 force 替换或删除：结束摄取。
            Err(CoreError::NotFound) => break,
            Err(error) => tracing::debug!(%error, terminal_id = %terminal_id, "shell 事件写入失败"),
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
    caller: &Caller,
    event_tx: &Sender<Event>,
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
            let id =
                registry.authorize(&locator, caller.admin, caller.root_session_id.as_deref())?;
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
    caller: &Caller,
) -> Result<(), CoreError> {
    match params.terminals {
        Some(locators) => {
            let mut ids: Vec<TerminalId> = Vec::new();
            let mut seen = BTreeSet::new();
            for locator in &locators {
                let id =
                    registry.authorize(locator, caller.admin, caller.root_session_id.as_deref())?;
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

/// runtime 解析错误 → 协议错误响应。
///
/// 未知 ID 返回 `not_found`；平台不兼容、路径越界、文件缺失/不可执行均返回
/// `bad_request`，不静默回退到请求里的 cmd。
fn runtime_error_response(id: String, error: &RuntimeError) -> Response {
    let code = match error {
        RuntimeError::NotFound(_) => ErrorCode::NotFound,
        RuntimeError::MissingManifest(_)
        | RuntimeError::Io { .. }
        | RuntimeError::InvalidManifest(_)
        | RuntimeError::Incompatible { .. }
        | RuntimeError::UnsafeBinary { .. }
        | RuntimeError::MissingBinary { .. }
        | RuntimeError::NotExecutable { .. } => ErrorCode::BadRequest,
    };
    error_response(id, code, error.to_string())
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
        CoreError::LimitExceeded(_) => ErrorCode::LimitExceeded,
        CoreError::BackendFailed(_) => ErrorCode::Internal,
        // 授权要求必须单独成码并带回写入计划，不能落到普通 bad_request；
        // 注入失败已由 core 映射为 BadRequest，最终返回 bad_request，不回退。
        CoreError::ConsentRequired { .. } => ErrorCode::ConsentRequired,
        CoreError::HostKeyUnknown { .. } => ErrorCode::HostKeyUnknown,
        CoreError::HostKeyChanged { .. } => ErrorCode::HostKeyChanged,
        CoreError::HostKeyRejected { .. } => ErrorCode::SshConnectFailed,
    };
    let terminal_id = match &error {
        CoreError::SessionExists { terminal_id } => Some(terminal_id.clone()),
        _ => None,
    };
    let injection = match &error {
        CoreError::ConsentRequired { details } => Some(Box::new(details.clone())),
        _ => None,
    };
    let host_key = match &error {
        CoreError::HostKeyUnknown { details } | CoreError::HostKeyChanged { details } => {
            Some(Box::new(details.clone()))
        }
        _ => None,
    };

    ProtocolError {
        code,
        message: error.to_string(),
        retryable: code.is_retryable(),
        terminal_id,
        injection,
        host_key,
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
            injection: None,
            host_key: None,
        }),
    }
}

/// 首帧鉴权结论。
enum Handshake {
    /// 通过；保留请求 ID、客户端标识与连接级调用者身份。
    Accepted {
        /// `hello` 请求 ID。
        id: String,
        /// 客户端标识。
        client: String,
        /// 连接级调用者身份。
        caller: Caller,
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

    let root_session_id = params
        .root_session_id
        .as_deref()
        .map(str::trim)
        .filter(|root| !root.is_empty())
        .map(str::to_string);
    if !params.admin && root_session_id.is_none() {
        return Handshake::rejected(
            request.id,
            ErrorCode::BadRequest,
            "hello 需要 root_session_id（或 admin=true）",
        );
    }
    // `@web` 是保留的未绑定会话：普通连接不得冒充，只有管理员可访问其终端。
    if !params.admin && root_session_id.as_deref() == Some(WEB_ROOT_SESSION_ID) {
        return Handshake::rejected(
            request.id,
            ErrorCode::BadRequest,
            "root_session_id 不能使用保留值 @web",
        );
    }

    Handshake::Accepted {
        id: request.id,
        client: params.client,
        caller: Caller {
            root_session_id: if params.admin { None } else { root_session_id },
            admin: params.admin,
        },
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
pub(crate) fn is_loopback(addr: SocketAddr) -> bool {
    addr.ip().is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use termx_core::MockFactory;
    use termx_protocol::TranscriptSource;
    use termx_protocol::shell_events::{ShellEvent, ShellEventKind};

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
    fn ssh_private_key_prefers_explicit_path() {
        let home = tempfile::tempdir().expect("创建临时 home");
        std::fs::create_dir_all(home.path().join(".ssh")).expect("创建 .ssh");
        std::fs::write(home.path().join(".ssh/id_ed25519"), "key").expect("写入默认私钥");

        assert_eq!(
            resolve_ssh_private_key_in("~/keys/custom", Some(home.path())).expect("显式路径应优先"),
            "~/keys/custom"
        );
    }

    #[test]
    fn ssh_private_key_defaults_prefer_ed25519_then_rsa() {
        let home = tempfile::tempdir().expect("创建临时 home");
        let ssh_dir = home.path().join(".ssh");
        std::fs::create_dir_all(&ssh_dir).expect("创建 .ssh");

        let ed25519 = ssh_dir.join("id_ed25519");
        let rsa = ssh_dir.join("id_rsa");
        std::fs::write(&ed25519, "key").expect("写入 id_ed25519");
        assert_eq!(
            PathBuf::from(
                resolve_ssh_private_key_in("  ", Some(home.path())).expect("应探测 id_ed25519")
            ),
            ed25519
        );

        std::fs::remove_file(&ed25519).expect("删除 id_ed25519");
        std::fs::write(&rsa, "key").expect("写入 id_rsa");
        assert_eq!(
            PathBuf::from(
                resolve_ssh_private_key_in("", Some(home.path())).expect("应回退 id_rsa")
            ),
            rsa
        );
    }

    #[test]
    fn ssh_private_key_reports_missing_defaults() {
        let home = tempfile::tempdir().expect("创建临时 home");
        let error =
            resolve_ssh_private_key_in("", Some(home.path())).expect_err("无默认私钥应报错");
        assert!(
            error.contains("id_ed25519") && error.contains("id_rsa"),
            "{error}"
        );

        let error = resolve_ssh_private_key_in("", None).expect_err("无 home 应报错");
        assert!(
            error.contains("id_ed25519") && error.contains("id_rsa"),
            "{error}"
        );
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
    fn send_payload_prefers_data_and_enter_byte_depends_on_mode() {
        let payload =
            send_payload(&send_params(Some("aGk="), Some("ignored"), true)).expect("data 应优先");
        assert_eq!(payload, b"hi");

        let payload = send_payload(&send_params(None, Some("echo"), false)).expect("text 应可用");
        assert_eq!(payload, b"echo");

        assert_eq!(enter_byte(Mode::Pty), b'\r');
        assert_eq!(enter_byte(Mode::Stdio), b'\n');
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

    /// 构造 mock 后端终端，返回 registry 与 terminal_id。
    fn proc_event_terminal() -> (Arc<Mutex<Registry>>, String) {
        let mut registry = Registry::with_factory(64, Arc::new(MockFactory));
        let created = registry
            .create(CreateParams {
                mode: Mode::Pty,
                cmd: "mock".to_string(),
                args: Vec::new(),
                cwd: None,
                env: None,
                cols: None,
                rows: None,
                root_session_id: "ses-1".to_string(),
                name: "build".to_string(),
                force: false,
                idle_timeout: None,
                ssh: None,
                allow_injection: false,
                runtime_id: None,
            })
            .expect("mock 终端应创建成功");
        (Arc::new(Mutex::new(registry)), created.terminal_id)
    }

    /// 把事件按顺序喂给摄取任务，等任务处理完再返回。
    async fn feed_proc_events(
        registry: &Arc<Mutex<Registry>>,
        terminal_id: &str,
        events: Vec<ProcEvent>,
    ) {
        let (tx, rx) = channel(8);
        let handle = tokio::spawn(ingest_proc_events(
            Arc::clone(registry),
            TerminalId::new(terminal_id.to_string()),
            rx,
        ));
        for event in events {
            tx.send(event).await.expect("摄取任务应仍在运行");
        }
        drop(tx);
        handle.await.expect("摄取任务不应 panic");
    }

    /// 读取 transcript 的第一条命令。
    fn first_transcript(
        registry: &Arc<Mutex<Registry>>,
        terminal_id: &str,
    ) -> termx_protocol::TranscriptCommand {
        let registry = lock_registry(registry);
        registry
            .transcript(
                &TerminalLocator {
                    terminal_id: Some(terminal_id.to_string()),
                    root_session_id: None,
                    name: None,
                },
                None,
            )
            .expect("读取 transcript")
            .commands
            .into_iter()
            .next()
            .expect("应有一条命令")
    }

    /// 构造 shell 事件。
    fn shell_proc_event(kind: ShellEventKind, code: Option<i32>, id: &str) -> ProcEvent {
        ProcEvent::ShellEvent(ShellEvent {
            kind,
            code,
            id: Some(id.to_string()),
            command: None,
            started_at_ms: None,
            ended_at_ms: None,
        })
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("合法时间戳")
    }

    #[tokio::test]
    async fn proc_shell_events_finish_transcript_with_remote_exit_code() {
        let (registry, terminal_id) = proc_event_terminal();
        {
            let mut guard = lock_registry(&registry);
            guard
                .record_transcript_command(&TerminalId::new(terminal_id.clone()), "first", now())
                .expect("记录命令");
        }
        feed_proc_events(
            &registry,
            &terminal_id,
            vec![
                shell_proc_event(ShellEventKind::CommandStart, None, "cmd-1"),
                shell_proc_event(ShellEventKind::CommandEnd, Some(7), "cmd-1"),
            ],
        )
        .await;

        let command = first_transcript(&registry, &terminal_id);
        assert_eq!(command.command, "first");
        assert_eq!(command.status, termx_protocol::TranscriptStatus::Done);
        assert_eq!(command.exit_code, Some(7));
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
        assert!(!command.best_effort);
    }

    #[test]
    fn late_proc_shell_event_does_not_finish_new_command() {
        let (registry, terminal_id) = proc_event_terminal();
        let id = TerminalId::new(terminal_id.clone());
        let mut tracker = ShellEventTracker::new();
        let mut guard = lock_registry(&registry);
        guard
            .record_transcript_command(&id, "first", now())
            .expect("记录第一条");
        // C1 绑定 g1。
        let (_, terminal) = apply_proc_event(
            &mut guard,
            &id,
            &mut tracker,
            shell_proc_event(ShellEventKind::CommandStart, None, "cmd-1"),
            now(),
        );
        assert!(!terminal);
        // service 已记录新命令 g2，旧 C 的迟到 D 必须被忽略。
        guard
            .record_transcript_command(&id, "second", now() + time::Duration::seconds(1))
            .expect("记录第二条");
        let (result, terminal) = apply_proc_event(
            &mut guard,
            &id,
            &mut tracker,
            shell_proc_event(ShellEventKind::CommandEnd, Some(0), "cmd-1"),
            now() + time::Duration::seconds(2),
        );
        assert!(result.is_ok());
        assert!(!terminal);

        let commands = guard
            .transcript(
                &TerminalLocator {
                    terminal_id: Some(terminal_id),
                    root_session_id: None,
                    name: None,
                },
                None,
            )
            .expect("读取 transcript")
            .commands;
        assert_eq!(commands[1].command, "second");
        assert_eq!(
            commands[1].status,
            termx_protocol::TranscriptStatus::Running
        );
        assert_eq!(commands[1].exit_code, None);
    }

    #[tokio::test]
    async fn output_without_shell_events_does_not_fake_exit() {
        let (registry, terminal_id) = proc_event_terminal();
        {
            let mut guard = lock_registry(&registry);
            guard
                .record_transcript_command(&TerminalId::new(terminal_id.clone()), "first", now())
                .expect("记录命令");
        }
        feed_proc_events(
            &registry,
            &terminal_id,
            vec![ProcEvent::Output {
                data: b"hi".to_vec(),
                stream: OutputStream::Pty,
            }],
        )
        .await;

        let command = first_transcript(&registry, &terminal_id);
        assert_eq!(command.status, termx_protocol::TranscriptStatus::Running);
        assert_eq!(command.exit_code, None);
        assert_eq!(command.ended_at, None);
    }
}
