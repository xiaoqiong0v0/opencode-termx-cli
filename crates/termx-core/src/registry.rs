//! 终端 registry：会话键 `(root_session_id, name)` 与 `terminal_id` 双索引。
//!
//! F2 维护状态、事件与命名冲突语义；F3 增加事件摄取、订阅与 attach/detach；
//! M2c1 通过可注入的 [`BackendFactory`] 创建真实 PTY 后端。
//!
//! 锁约定：service 在持有 registry 的 `std::sync::Mutex` 时只做同步短操作；
//! 可能阻塞的后端调用（`spawn` / `terminate` / `tree` / `foreground_pgrp`）
//! 一律在锁外进行。本模块的 `create_with_backend` 是测试便捷入口，其调用方
//! 自行保证不会跨 await 持锁。

use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use termx_protocol::{
    AttachResult, ClaimResult, Confidence, CreateParams, CreateResult, Event, EventKind,
    IdleTimeout, IdleTimeoutAction, IdleTimeoutUnit, InteractionState, LifecycleState, ListItem,
    ListParams, Mode, OutputParams, OutputResult, ProcNode, PruneBasis, PruneParams, PruneResult,
    ReadChunk, ReadResult, RebindResult, ResetReason, SubscriptionCursor, TerminalLocator,
    TerminalSnapshot, TimeSpan, TranscriptResult, TranscriptSource, TranscriptStatus,
    WEB_ROOT_SESSION_ID,
};
use time::OffsetDateTime;
use tokio::sync::{
    mpsc::{Sender, error::TrySendError},
    watch,
};

use crate::{
    BackendFactory, CoreError, DEFAULT_EVENT_BUFFER_CAPACITY, DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS,
    DEFAULT_TRANSCRIPT_CAPACITY, EventBuffer, EventRecord, InteractionAnalyzer, InteractionConfig,
    InteractionUpdate, ModeFactory, PendingInterrupt, TerminalBackend, TerminalEntry, TerminalId,
    TranscriptBuffer, now_rfc3339,
};

/// 会话内名称最大长度（按 Unicode 字符计数）。
const NAME_MAX_LEN: usize = 64;
/// force 终止旧终端时写入事件的信号名。
const TERMINATE_SIGNAL: &str = "SIGTERM";
/// force 终止旧终端时写入事件的来源。
const TERMINATE_SOURCE: &str = "terminate";
/// `terminal.signal(SIGINT)` 成功后记录的中断来源。
const CONTROL_C_SOURCE: &str = "control-c";
/// 其它信号成功送达后记录的中断来源。
const SIGNAL_SOURCE: &str = "signal";
/// 客户端主动 `terminal.detach` 时写入的断开原因。
const DETACH_REASON: &str = "detach";
/// 连接断开、服务端清理订阅时写入的断开原因。
pub const DETACH_REASON_DISCONNECT: &str = "disconnect";
/// `idle_timeout` 到期、仅断开订阅时写入的断开原因。
pub const IDLE_DETACH_REASON: &str = "idle_timeout";
/// `idle_timeout` 到期、终止后端时写入的事件来源。
pub const TIMEOUT_SOURCE: &str = "timeout";
/// 连续无客户端附着超时、终止后端时写入的事件来源。
pub const CLIENT_TIMEOUT_SOURCE: &str = "client_timeout";
/// `terminal.create` 返回的初始游标：`started`（seq=1）之前的位置。
pub const INITIAL_CURSOR: u64 = 0;
/// 订阅者事件队列的默认容量（条）。
pub const DEFAULT_SUBSCRIBER_QUEUE_CAPACITY: usize = 1024;
/// `terminal.read` 未指定 `max_bytes`（或给 0）时的默认字节上限。
pub const DEFAULT_READ_MAX_BYTES: u64 = 64 * 1024;

/// 进程退出结果：区分正常退出码与信号终止。
///
/// service 把 [`termx_proc::ProcEvent`] 映射为本类型后统一交给
/// [`Registry::report_exit`]，由 registry 结合 `pending_interrupt` 决定最终事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitOutcome {
    /// 正常退出码。
    Code(i32),
    /// 被信号终止（Unix `WTERMSIG` 原值）。
    Signal(i32),
}

/// `idle_timeout` 到期后由 registry 锁内判定、锁外执行的动作。
pub enum IdleAction {
    /// 仅断开订阅：`Detached` 已在 registry 锁内投递给订阅者，终端继续运行。
    Detach {
        /// 到期终端。
        terminal_id: TerminalId,
        /// 本次被断开的订阅客户端。
        clients: Vec<String>,
    },
    /// 终止终端：终态事件已在锁内写入，后端由调用方在锁外终止。
    Terminate(Arc<dyn TerminalBackend>),
}

impl fmt::Debug for IdleAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Detach {
                terminal_id,
                clients,
            } => formatter
                .debug_struct("Detach")
                .field("terminal_id", terminal_id)
                .field("clients", clients)
                .finish(),
            Self::Terminate(backend) => formatter
                .debug_tuple("Terminate")
                .field(&backend.pid())
                .finish(),
        }
    }
}

/// 超过「无存活会话」超时的候选终端。
///
/// 由 [`Registry::candidates_clientless`] 在锁内产生；携带后端 `Arc`，
/// service 可在锁外查询进程树（`no_children` 策略）或直接终止。
pub struct ClientlessCandidate {
    /// 到期终端。
    pub terminal_id: TerminalId,
    /// 所属 opencode 会话 ID。
    pub root_session_id: String,
    /// 终端根进程 PID；用于从进程树中排除根进程。
    pub pid: Option<u32>,
    /// 后端句柄，供锁外采集进程树或终止。
    pub backend: Arc<dyn TerminalBackend>,
}

impl fmt::Debug for ClientlessCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientlessCandidate")
            .field("terminal_id", &self.terminal_id)
            .field("root_session_id", &self.root_session_id)
            .field("pid", &self.pid)
            .finish()
    }
}

/// `terminal.signal` 的锁内准备结果：目标终端、后端句柄与信号名。
///
/// 锁内不记录 `pending_interrupt`：只有后端 `signal` 成功返回后，service 才在
/// 下一把 registry 锁内调用 [`Registry::confirm_signal`] 记录中断意图。
pub struct PreparedSignal {
    /// 目标终端。
    pub terminal_id: TerminalId,
    /// 目标后端。
    pub backend: Arc<dyn TerminalBackend>,
    /// 已校验格式的信号名，原样交给后端。
    pub signal: String,
}

impl PreparedSignal {
    /// 执行后端信号发送；调用方必须在 registry 锁外调用。
    pub fn execute(&self) -> Result<(), CoreError> {
        self.backend.signal(&self.signal)
    }
}

/// 每终端订阅发送端：有界通道，容量由 [`Registry::subscriber_queue_capacity`] 决定。
///
/// 投递一律 `try_send`，绝不阻塞事件写入；队满（任何事件）时移除订阅者并
/// 通知 service 断开，见 [`Registry::deliver_event`]。
type EventSender = Sender<Event>;
/// 同一终端下 `client_id -> 发送端`。
type TerminalSubscribers = BTreeMap<String, EventSender>;

/// 订阅簿：终端级订阅与全局订阅分离保存。
///
/// 全局订阅用于 `subscribe_all`，未来新建的终端无需回填订阅记录；投递时按
/// `client_id` 去重，避免同一客户端同时 attach 与 subscribe_all 时收到两份事件。
#[derive(Debug, Default)]
struct SubscriptionBook {
    /// 终端级订阅：`terminal_id -> client_id -> sender`。
    by_terminal: BTreeMap<TerminalId, TerminalSubscribers>,
    /// 全局订阅：`client_id -> sender`。
    all: BTreeMap<String, EventSender>,
    /// 全局订阅的终端排除集：`client_id -> {terminal_id}`。
    ///
    /// 客户端对某终端 `detach`/`unsubscribe` 后写入，`attach`/`subscribe`
    /// 该终端时移除；投递全局订阅事件时跳过被排除的终端。
    excluded: BTreeMap<String, BTreeSet<TerminalId>>,
    /// 连接级限流通知：任何事件无法投递时置 `true`，由 service 断开连接。
    throttle: BTreeMap<String, watch::Sender<bool>>,
}

/// 单次投递结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivery {
    /// 已投递。
    Kept,
    /// 接收端已关闭，订阅应清理。
    Closed,
    /// 队列已满：必须移除订阅者并通知 service 断开。
    Throttled,
}

/// 终端注册表。
///
/// 内部同时维护 `terminal_id -> entry` 与 `(root_session_id, name) -> terminal_id`
/// 两份索引；`BTreeMap` 保证 `list` 输出顺序稳定。
pub struct Registry {
    /// 新建终端使用的事件缓冲容量。
    event_buffer_capacity: usize,
    /// 新建终端使用的 transcript 容量（条）。
    transcript_capacity: usize,
    /// 每个订阅者的有界事件队列容量。
    subscriber_queue_capacity: usize,
    /// 运行中终端数量上限；`None` 表示不限制。
    max_terminals: Option<usize>,
    /// 新建终端的后端工厂；默认 [`ModeFactory`]（pty/stdio 分别路由），
    /// 测试可注入 [`crate::MockFactory`]。
    factory: Arc<dyn BackendFactory>,
    /// 新建终端使用的交互识别配置。
    interaction_config: InteractionConfig,
    /// 按 ID 索引。
    by_id: BTreeMap<TerminalId, TerminalEntry>,
    /// 按会话键索引。
    by_key: BTreeMap<(String, String), TerminalId>,
    /// 事件订阅簿。
    subscriptions: SubscriptionBook,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new(DEFAULT_EVENT_BUFFER_CAPACITY)
    }
}

impl Registry {
    /// 以给定事件缓冲容量创建空 registry；容量为 0 时按 1 处理。
    ///
    /// 默认工厂为 [`ModeFactory`]：`mode=pty` 走 ConPTY/PTY，`mode=stdio`
    /// 走独立管道，两者都会创建真实进程。
    pub fn new(event_buffer_capacity: usize) -> Self {
        Self::with_factory(event_buffer_capacity, Arc::new(ModeFactory::default()))
    }

    /// 使用指定后端工厂创建空 registry；订阅队列取默认容量。
    pub fn with_factory(event_buffer_capacity: usize, factory: Arc<dyn BackendFactory>) -> Self {
        Self::with_config(
            event_buffer_capacity,
            DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
            factory,
        )
    }

    /// 同时指定事件缓冲容量与订阅队列容量；测试用。
    pub fn with_config(
        event_buffer_capacity: usize,
        subscriber_queue_capacity: usize,
        factory: Arc<dyn BackendFactory>,
    ) -> Self {
        Self {
            event_buffer_capacity: event_buffer_capacity.max(1),
            transcript_capacity: DEFAULT_TRANSCRIPT_CAPACITY,
            subscriber_queue_capacity: subscriber_queue_capacity.max(1),
            max_terminals: None,
            factory,
            interaction_config: InteractionConfig::default(),
            by_id: BTreeMap::new(),
            by_key: BTreeMap::new(),
            subscriptions: SubscriptionBook::default(),
        }
    }

    /// 每个订阅者的有界队列容量。
    pub fn subscriber_queue_capacity(&self) -> usize {
        self.subscriber_queue_capacity
    }

    /// 设置运行中终端数量上限；`None` 表示不限制。
    pub fn set_max_terminals(&mut self, max_terminals: Option<usize>) {
        self.max_terminals = max_terminals;
    }

    /// 设置新建终端的 transcript 容量；只影响之后创建的终端。
    pub fn set_transcript_capacity(&mut self, capacity: usize) {
        self.transcript_capacity = capacity.max(1);
    }

    /// 设置新建终端使用的交互识别配置；只影响之后创建的终端。
    pub fn set_interaction_config(&mut self, interaction_config: InteractionConfig) {
        self.interaction_config = interaction_config;
    }

    /// 当前交互识别配置。
    pub fn interaction_config(&self) -> InteractionConfig {
        self.interaction_config
    }

    /// 当前运行中（非终态）终端数量。
    pub fn running_count(&self) -> usize {
        self.by_id
            .values()
            .filter(|entry| !entry.state.is_terminal())
            .count()
    }

    /// 是否已达运行中终端上限。
    fn at_terminal_limit(&self) -> bool {
        self.max_terminals
            .is_some_and(|max| self.running_count() >= max)
    }

    /// 登记连接级限流通知通道；状态事件无法投递时向该通道发送 `true`。
    ///
    /// service 在 `hello` 后登记、连接收尾时 `unregister_client`；registry
    /// 自身不创建该 watch，避免把连接生命周期耦合进 core。
    pub fn register_client(&mut self, client_id: &str, throttle: watch::Sender<bool>) {
        self.subscriptions
            .throttle
            .insert(client_id.to_string(), throttle);
    }

    /// 注销连接级限流通知通道。
    pub fn unregister_client(&mut self, client_id: &str) {
        self.subscriptions.throttle.remove(client_id);
    }

    /// 克隆后端工厂句柄；service 在锁外调用 `spawn` 时使用。
    pub fn backend_factory(&self) -> Arc<dyn BackendFactory> {
        Arc::clone(&self.factory)
    }

    /// 使用 registry 的工厂创建终端（同步便捷入口）。
    ///
    /// 该入口会丢弃事件接收端，仅供测试/单进程工具使用；service 使用
    /// [`Registry::prepare_create`] + [`Registry::register_create`] 保留事件流。
    pub fn create(&mut self, params: CreateParams) -> Result<CreateResult, CoreError> {
        // 仅做预检；同名旧终端由 register_create 在锁内落终态并返回待终止后端，
        // 避免“先终止旧进程、后写 interrupted”导致旧事件抢先落成 exited。
        self.prepare_create(&params)?;

        let (backend, events) = self.factory.spawn(&params)?;
        // 同步入口无人消费事件；丢弃接收端让 proc 读线程尽快结束。
        drop(events);
        let foreground_pgrp = backend.foreground_pgrp();
        let registered = self.register_create(params, Arc::clone(&backend), foreground_pgrp);
        let (result, displaced) = match registered {
            Ok(registered) => registered,
            Err(error) => {
                // 竞态/上限导致注册失败：回收刚启动的后端，避免孤儿。
                backend.terminate();
                return Err(error);
            }
        };
        if let Some(old) = displaced {
            old.terminate();
        }
        Ok(result)
    }

    /// 使用指定后端创建终端；供旧测试验证旧后端是否被终止。
    ///
    /// 不经过工厂；事件接收端为空。`force=true` 时旧终端在注册时先落终态，
    /// 返回的旧后端由本方法在注册后终止。
    pub fn create_with_backend(
        &mut self,
        params: CreateParams,
        backend: Arc<dyn TerminalBackend>,
    ) -> Result<CreateResult, CoreError> {
        // 仅做预检；旧后端统一由 register_create 置换后在锁外终止。
        self.prepare_create(&params)?;

        let foreground_pgrp = backend.foreground_pgrp();
        let (result, displaced) = self.register_create(params, backend, foreground_pgrp)?;
        if let Some(old) = displaced {
            old.terminate();
        }
        Ok(result)
    }

    /// 创建前预检：校验参数与同名冲突，不终止任何后端。
    ///
    /// `force=true` 时旧终端由 [`Registry::register_create`] 在锁内落终态、
    /// 从索引摘除，并把待终止后端交给调用方在锁外终止；这样旧摄取任务不会在
    /// `interrupted` 之前抢先写入 `exited`。
    pub fn prepare_create(&self, params: &CreateParams) -> Result<(), CoreError> {
        validate_create(params)?;

        let key = (params.root_session_id.clone(), params.name.clone());
        let Some(old_id) = self.by_key.get(&key) else {
            if self.at_terminal_limit() {
                return Err(CoreError::LimitExceeded(format!(
                    "运行中终端数量已达上限 {}",
                    self.max_terminals.unwrap_or_default()
                )));
            }
            return Ok(());
        };
        if !params.force {
            return Err(CoreError::SessionExists {
                terminal_id: old_id.0.clone(),
            });
        }
        Ok(())
    }

    /// 注册一个已启动的后端：写入 pid/pgid/foreground、入索引并 emit `started`。
    ///
    /// 若同名冲突且 `force=true`，旧终端会先落 `interrupted{source:"terminate"}`、
    /// 从索引移除，其后端通过返回值交给调用方在锁外终止。
    ///
    /// # 错误
    /// 参数非法、同名冲突（无 `force`）或终端不存在时返回 [`CoreError`]。
    pub fn register_create(
        &mut self,
        params: CreateParams,
        backend: Arc<dyn TerminalBackend>,
        foreground_pgrp: Option<u32>,
    ) -> Result<(CreateResult, Option<Arc<dyn TerminalBackend>>), CoreError> {
        self.register_create_with(params, backend, foreground_pgrp, true, false)
    }

    /// 注册一个已启动的后端，并指定事件来源与 transcript 输出来源。
    ///
    /// `in_band_osc=false` 用于自带带外事件通道的运行时（本地 `termx-pwsh`/
    /// `termx-bash` 或远端 helper 通道）：子程序输出的 `133/633` 不再结束命令，
    /// 退出码只来自事件通道；同时 `terminal.send` 不再预记录 transcript 命令。
    /// 其他语义与 [`Registry::register_create`] 完全一致。
    ///
    /// `transcript_output_from_events=true` 表示命令输出改由带外 `O` 事件提供，
    /// PTY 输出只用于显示、不再追加到 transcript；此时也不启用迟到的 PTY
    /// 尾段兜底（`allow_late_output`），静默结束不提前提升兜底（避免与迟到
    /// `O` 重复）。旧端/远端/raw 终端传 `false`。
    pub fn register_create_with(
        &mut self,
        params: CreateParams,
        backend: Arc<dyn TerminalBackend>,
        foreground_pgrp: Option<u32>,
        in_band_osc: bool,
        transcript_output_from_events: bool,
    ) -> Result<(CreateResult, Option<Arc<dyn TerminalBackend>>), CoreError> {
        validate_create(&params)?;

        let key = (params.root_session_id.clone(), params.name.clone());
        let mut displaced = None;
        if let Some(old_id) = self.by_key.get(&key).cloned() {
            if !params.force {
                return Err(CoreError::SessionExists {
                    terminal_id: old_id.0.clone(),
                });
            }

            // force：先落旧终端终态并从索引/订阅簿移除；后端由调用方在锁外终止。
            displaced = self.mark_interrupted(&old_id)?;
            self.by_id.remove(&old_id);
            self.by_key.remove(&key);
            self.subscriptions.by_terminal.remove(&old_id);
            for excluded in self.subscriptions.excluded.values_mut() {
                excluded.remove(&old_id);
            }
        }

        // 预检后仍可能被并发 create 抢先占用名额；锁内再校验一次。
        if self.at_terminal_limit() {
            return Err(CoreError::LimitExceeded(format!(
                "运行中终端数量已达上限 {}",
                self.max_terminals.unwrap_or_default()
            )));
        }

        let (cols, rows) = match params.mode {
            Mode::Pty => (
                params.cols.unwrap_or(DEFAULT_PTY_COLS),
                params.rows.unwrap_or(DEFAULT_PTY_ROWS),
            ),
            Mode::Stdio => (params.cols.unwrap_or(0), params.rows.unwrap_or(0)),
        };
        let id = TerminalId::generate();
        let created_at = now_rfc3339();
        let pid = backend.pid();
        let pgid = backend.pgid();
        let ssh = params.ssh.is_some();
        let runtime_id = params.runtime_id.clone();
        let allow_late_output = !transcript_output_from_events
            && (params.runtime_id.as_deref() == Some("termx-pwsh")
                || params
                    .cmd
                    .trim()
                    .rsplit(['/', '\\'])
                    .next()
                    .is_some_and(|name| name == "termx-pwsh"));

        let entry = TerminalEntry {
            id: id.clone(),
            root_session_id: params.root_session_id,
            name: params.name,
            mode: params.mode,
            ssh,
            shell_events: !in_band_osc,
            runtime_id,
            // 保存创建时的启动信息原值，供 status/list 展示（SSH 也保留原始 cmd/args）。
            command: params.cmd,
            args: params.args,
            cwd: params.cwd,
            state: LifecycleState::Running,
            interaction: InteractionState::None,
            confidence: Confidence::Low,
            pid: Some(pid),
            pgid: Some(pgid),
            ppid: None,
            foreground_pgrp,
            cols,
            rows,
            created_at: created_at.clone(),
            last_activity: None,
            seq: 0,
            events: EventBuffer::new(self.event_buffer_capacity),
            idle_timeout: params.idle_timeout,
            lease: None,
            clients: BTreeSet::new(),
            // 仅作连接级诊断；终端保活由 clientless_since 与会话心跳决定。
            last_client_seen: Some(created_at.clone()),
            // 创建时视为无会话：首个 ticker 周期开始计时。
            clientless_since: None,
            pending_interrupt: None,
            capabilities: backend.capabilities(),
            transcript: TranscriptBuffer::with_in_band_osc_and_late_output(
                self.transcript_capacity,
                in_band_osc,
                allow_late_output,
            )
            .with_output_from_events(transcript_output_from_events),
            analyzer: InteractionAnalyzer::with_start_time(
                self.interaction_config,
                OffsetDateTime::now_utc(),
            ),
            backend,
        };
        let mode = entry.mode;
        self.by_key.insert(key, id.clone());
        self.by_id.insert(id.clone(), entry);

        // 先入索引再发布，`subscribe_all` 的全局订阅者才能收到新终端的 started。
        self.publish(&id, EventKind::Started { pid, pgid, mode })?;

        Ok((
            CreateResult {
                terminal_id: id.0,
                // 返回 started 之前的位置，客户端 attach(cursor) 能回放 started(seq=1)。
                cursor: INITIAL_CURSOR,
            },
            displaced,
        ))
    }

    /// 按状态过滤并限制条数。
    pub fn list(&self, params: &ListParams) -> Vec<ListItem> {
        self.list_for(params, None)
    }

    /// 按状态过滤并限制条数；`root_session_id` 为 `Some` 时只返回该会话的终端。
    ///
    /// 会话隔离只应在服务端按连接身份传入；`None` 表示不限制（管理员）。
    pub fn list_for(&self, params: &ListParams, root_session_id: Option<&str>) -> Vec<ListItem> {
        let limit = params.limit.map_or(usize::MAX, |limit| limit as usize);

        let mut entries: Vec<_> = self
            .by_id
            .values()
            .filter(|entry| match root_session_id {
                Some(root) => entry.root_session_id == root,
                None => true,
            })
            .filter(|entry| match &params.states {
                Some(states) => states.contains(&entry.state),
                None => true,
            })
            .collect();
        // `limit` 取最近活动的 N 条；ID 作为稳定兜底，避免 HashMap 顺序导致结果不确定。
        entries.sort_by(|left, right| {
            right
                .activity_time()
                .cmp(&left.activity_time())
                .then_with(|| right.id.0.cmp(&left.id.0))
        });

        entries
            .into_iter()
            .take(limit)
            .map(|entry| ListItem {
                terminal_id: entry.id.0.clone(),
                root_session_id: entry.root_session_id.clone(),
                runtime_id: entry.runtime_id.clone(),
                name: entry.name.clone(),
                state: entry.state,
                structured_events: entry.shell_events,
                mode: entry.mode,
                command: entry.command.clone(),
                args: entry.args.clone(),
                cwd: entry.cwd.clone(),
                pid: entry.pid,
                idle_timeout: entry.idle_timeout,
                last_activity: entry.last_activity.clone(),
                created_at: entry.created_at.clone(),
            })
            .collect()
    }

    /// 按 `terminal_id` 或 `(root_session_id, name)` 返回快照。
    ///
    /// 运行中会读取后端的前台进程组与进程树（可能在锁外阻塞）；终态时返回
    /// 最后快照（后端可能已不再可用）。
    pub fn status(&self, locator: &TerminalLocator) -> Result<TerminalSnapshot, CoreError> {
        let (snapshot, backend) = self.status_parts(locator)?;
        Ok(fill_runtime_info(snapshot, backend.as_deref()))
    }

    /// 返回基础快照与可选的运行中后端句柄。
    ///
    /// service 用该接口把「读快照」与「查询后端运行信息」拆开：先在锁内取
    /// 快照与 `Arc`，释放锁后再调用后端的 `foreground_pgrp` / `tree`。
    pub fn status_parts(
        &self,
        locator: &TerminalLocator,
    ) -> Result<(TerminalSnapshot, Option<Arc<dyn TerminalBackend>>), CoreError> {
        let id = self.resolve(locator)?;
        let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
        if entry.state.is_terminal() {
            Ok((entry.snapshot(), None))
        } else {
            Ok((entry.snapshot(), Some(Arc::clone(&entry.backend))))
        }
    }

    /// 按游标只读回放输出：只返回 `Output` 事件，按 `seq` 升序。
    ///
    /// - 不创建/修改订阅，也不推进任何消费状态；可重复调用。
    /// - 返回的 `cursor` 是本次扫描到的最后 `seq`（含被跳过的非 Output 事件），
    ///   便于客户端继续增量读取。
    /// - `cursor` 早于 ring 下界，或所需区间的输出已被背压淘汰时，返回
    ///   `reset: too_old`、空 `chunks` 与基线游标（当前最后 `seq`）。
    /// - `max_bytes` 为本次累计 `data` 解码后字节上限；`None`/`0` 用
    ///   [`DEFAULT_READ_MAX_BYTES`]。单条输出本身超过上限时仍返回该条并前进
    ///   游标，避免空结果导致活锁。
    pub fn read(
        &self,
        locator: &TerminalLocator,
        cursor: u64,
        max_bytes: Option<u64>,
    ) -> Result<ReadResult, CoreError> {
        let id = self.resolve(locator)?;
        let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
        Ok(read_after(entry, cursor, max_bytes))
    }

    /// 记录一条新命令（status=running，输出清空）。
    ///
    /// 调用方只应在 `terminal.send` 通过鉴权/lease 且 `enter=true` 后调用；
    /// 按键/密码等 `enter=false` 的输入不得记录。
    pub fn record_transcript_command(
        &mut self,
        id: &TerminalId,
        text: &str,
        now: OffsetDateTime,
    ) -> Result<u64, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.record_command(text, now))
    }

    /// 取出并清除 `terminal.send` 记录留下的 pending 代次。
    ///
    /// 宿主 `C` 事件到达时先调用本方法：有 pending 表示该命令已由
    /// `terminal.send` 记录，应原地更新而不是重复插入。
    pub fn take_pending_transcript_generation(
        &mut self,
        id: &TerminalId,
    ) -> Result<Option<u64>, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.take_pending_generation())
    }

    /// 撤销 pending 的 transcript 命令（多行输入未执行时使用）。
    ///
    /// 仅当 pending 代次指向最后一条仍在运行、尚无结束时间的记录时删除它；
    /// 其余情况保持不变。返回是否删除了记录。
    pub fn cancel_pending_transcript(&mut self, id: &TerminalId) -> Result<bool, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.cancel_pending())
    }

    /// 按代次更新已记录命令的文本与开始时间（宿主 `C` 事件携带的值）。
    /// 字段为 `None` 时保持原值；返回是否发生变更。
    pub fn update_transcript_start(
        &mut self,
        id: &TerminalId,
        generation: u64,
        command: Option<&str>,
        started_at: Option<OffsetDateTime>,
    ) -> Result<bool, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry
            .transcript
            .update_command_for_generation(generation, command, started_at))
    }

    /// 记录宿主 `C` 事件新建的命令（没有 `terminal.send` 前置记录）。
    ///
    /// 返回新记录的命令代次，供后续 `D` 事件精确结束。
    pub fn record_transcript_command_at(
        &mut self,
        id: &TerminalId,
        text: &str,
        now: OffsetDateTime,
    ) -> Result<u64, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.record_command_at(text, now))
    }

    /// 追加一段原始输出文本（保留 ANSI）；空文本或尚无命令时跳过。
    pub fn record_transcript_output(
        &mut self,
        id: &TerminalId,
        text: &str,
        now: OffsetDateTime,
    ) -> Result<bool, CoreError> {
        if text.is_empty() {
            return Ok(false);
        }
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.record_output(text, now))
    }

    /// 按代次追加一段带外 `O` 输出。
    ///
    /// 只有当前最后一条命令的 generation 与 `generation` 匹配时才追加，避免
    /// 迟到/跨命令输出污染新命令。空文本或未知终端按无变更处理。
    pub fn record_transcript_output_for_generation(
        &mut self,
        id: &TerminalId,
        generation: u64,
        text: &str,
        now: OffsetDateTime,
    ) -> Result<bool, CoreError> {
        if text.is_empty() {
            return Ok(false);
        }
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry
            .transcript
            .record_output_for_generation(generation, text, now))
    }

    /// 收集 PTY 输出作为无 `O` 命令的兜底，不立即进入 transcript。
    pub fn record_pty_transcript_output(
        &mut self,
        id: &TerminalId,
        text: &str,
        now: OffsetDateTime,
    ) -> Result<bool, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.record_pty_output_for_current(text, now))
    }

    /// 以 `shell_integration` 来源结束当前命令。
    ///
    /// 用于不携带代次证据的调用方（如 in-band OSC）；带外事件通道应使用
    /// [`Registry::record_transcript_end_for_generation`] 以免迟到 `D` 结束
    /// 新命令。只结束当前（最后一条）处于 `running` 的命令，或可被真实证据
    /// 覆盖的 `best_effort` `done`；其他终态保持原样。
    /// 返回是否发生变更；未知终端返回 [`CoreError::NotFound`]。
    pub fn record_transcript_end(
        &mut self,
        id: &TerminalId,
        exit_code: Option<i32>,
        now: OffsetDateTime,
    ) -> Result<bool, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry
            .transcript
            .record_command_end(exit_code, TranscriptSource::ShellIntegration, now))
    }

    /// 读取当前最后一条 transcript 命令的代次；无命令时为 0。
    ///
    /// 供带外事件通道在 `C` 事件到来时绑定代次；未知终端返回
    /// [`CoreError::NotFound`]。
    pub fn current_transcript_generation(&self, id: &TerminalId) -> Result<u64, CoreError> {
        let entry = self.by_id.get(id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.current_generation())
    }

    /// 以 `shell_integration` 来源按代次结束当前命令。
    ///
    /// 只有当前最后一条命令的 generation 与 `generation` 一致时才结束，
    /// 其余语义与 [`Registry::record_transcript_end`] 相同。返回是否发生变更。
    pub fn record_transcript_end_for_generation(
        &mut self,
        id: &TerminalId,
        exit_code: Option<i32>,
        generation: u64,
        now: OffsetDateTime,
    ) -> Result<bool, CoreError> {
        self.record_transcript_end_for_generation_at(id, exit_code, generation, now)
    }

    /// 按代次结束当前命令，并使用宿主 `D` 提供的结束时间。
    ///
    /// 与 [`Registry::record_transcript_end_for_generation`] 相同，但
    /// `ended_at` 来自事件时间而非 service 处理时间。
    pub fn record_transcript_end_for_generation_at(
        &mut self,
        id: &TerminalId,
        exit_code: Option<i32>,
        generation: u64,
        ended_at: OffsetDateTime,
    ) -> Result<bool, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.record_command_end_for_generation_at(
            exit_code,
            TranscriptSource::ShellIntegration,
            generation,
            ended_at,
        ))
    }

    /// 对所有终端执行静默结束判定，返回被标记为 `done` 的命令数。
    ///
    /// `output_from_events=true` 的终端不由静默判定结束（`O` 来源命令只由
    /// 宿主 `D` 结束），见 [`TranscriptBuffer::finalize_quiet`]。
    pub fn finalize_quiet_transcripts(&mut self, now: OffsetDateTime, quiet: Duration) -> usize {
        let mut finalized = 0;
        for entry in self.by_id.values_mut() {
            if entry.transcript.finalize_quiet(now, quiet) {
                finalized += 1;
            }
        }
        finalized
    }

    /// 切换终端 transcript 文本是否权威地来自带外 `O` 事件。
    ///
    /// 事件通道结束（宿主退出/管道断开）后由 service 调回 `false`，让下一次
    /// idle tick 的静默判定可以提升 `C..D` 期间收集的 PTY 兜底；未知终端
    /// 返回 [`CoreError::NotFound`]。返回是否发生变更。
    pub fn set_transcript_output_from_events(
        &mut self,
        id: &TerminalId,
        output_from_events: bool,
    ) -> Result<bool, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.set_output_from_events(output_from_events))
    }

    /// 读取最近命令记录；`limit` 为 `None`/0 时返回全部保留记录。
    pub fn transcript(
        &self,
        locator: &TerminalLocator,
        limit: Option<u32>,
    ) -> Result<TranscriptResult, CoreError> {
        let id = self.resolve(locator)?;
        let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
        Ok(entry.transcript.read(limit))
    }

    /// 对**命令输出文本**做尾部截取或倒搜。
    ///
    /// 文本来源随终端能力切换，但行/字符/倒搜语义一致（都基于清洗后的
    /// 命令输出，不返回行号）：
    /// - 结构化终端（带外 shell 事件）：只用 transcript 中按时间顺序拼接的
    ///   命令输出，无 transcript 时为空；不退回 raw PTY，避免 prompt/控制串污染；
    /// - raw-only 终端：拼接事件环里的 `output`，剥离 ANSI/OSC 并处理 CR 重绘
    ///   与 CRLF；无 shell 事件可用，无法可靠剔除 prompt（已知限制）。
    ///
    /// 历史被背压裁剪后只返回当前仍保留的文本，不提供绝对行号。
    pub fn output_text(
        &self,
        locator: &TerminalLocator,
        params: &OutputParams,
    ) -> Result<OutputResult, CoreError> {
        let id = self.resolve(locator)?;
        let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
        let text = command_output_text(entry);
        let result = match (&params.pattern, params.tail_lines, params.tail_chars) {
            (Some(pattern), None, None) => {
                if pattern.is_empty() {
                    return Err(CoreError::BadRequest("搜索 pattern 不能为空".to_string()));
                }
                let nth = usize::try_from(params.nth.unwrap_or(1))
                    .map_err(|_| CoreError::BadRequest("nth 超出范围".to_string()))?;
                if nth == 0 {
                    return Err(CoreError::BadRequest("nth 必须从 1 开始".to_string()));
                }
                crate::search(&text, pattern, nth, params.only_matches)
            }
            (None, Some(lines), None) => crate::tail_lines(&text, lines as usize),
            (None, None, Some(chars)) => {
                let chars = usize::try_from(chars)
                    .map_err(|_| CoreError::BadRequest("tail-chars 超出范围".to_string()))?;
                crate::tail_chars(&text, chars)
            }
            (None, None, None) => {
                return Err(CoreError::BadRequest(
                    "需要 tail_lines、tail_chars 或 pattern 之一".to_string(),
                ));
            }
            _ => {
                return Err(CoreError::BadRequest(
                    "tail_lines/tail_chars/pattern 不能同时使用".to_string(),
                ));
            }
        };
        Ok(OutputResult { text: result })
    }

    /// 终止终端：落 `interrupted` 终态并调用后端；已终态时幂等返回 `Ok`。
    ///
    /// 同步便捷入口。service 应先调用 [`Registry::prepare_terminate`] 取后端
    /// `Arc`，释放 registry 锁后再终止，避免持锁执行阻塞操作。
    pub fn terminate(&mut self, locator: &TerminalLocator) -> Result<(), CoreError> {
        if let Some(backend) = self.prepare_terminate(locator)? {
            backend.terminate();
        }
        Ok(())
    }

    /// 终止前的锁内阶段：落 `interrupted{source:"terminate"}` 终态、清 lease，
    /// 并返回需要在锁外终止的后端。
    ///
    /// 已终态返回 `Ok(None)`（幂等，不重复发终态事件）；未知终端返回
    /// `not_found`。本方法不调用后端，可在持有 registry 锁时安全调用。
    pub fn prepare_terminate(
        &mut self,
        locator: &TerminalLocator,
    ) -> Result<Option<Arc<dyn TerminalBackend>>, CoreError> {
        let id = self.resolve(locator)?;
        self.mark_interrupted(&id)
    }

    /// 把终端绑定键迁移到新的 `(root_session_id, name)`。
    ///
    /// `terminal_id`、进程、lease、订阅与事件缓冲均保持不变，只改写
    /// `root_session_id` / `name` 与 `by_key`，并重置 `clientless_since`（新会话
    /// 从下一轮 ticker 重新计时）。终态终端返回 `already_terminal`。
    ///
    /// 目标键已存在时：
    /// - `force=false`：返回 `session_exists`，错误携带目标旧终端 ID；
    /// - `force=true`：目标旧终端先落 `interrupted{source:"terminate"}` 并从索引
    ///   移除，返回的后端 `Arc` 由调用方在 registry 锁外终止。
    ///
    /// `name=None` 表示保留原名；目标键与当前键相同时只重置计时，不产生事件。
    pub fn rebind(
        &mut self,
        locator: &TerminalLocator,
        root_session_id: &str,
        name: Option<&str>,
        force: bool,
    ) -> Result<(RebindResult, Option<Arc<dyn TerminalBackend>>), CoreError> {
        if root_session_id.is_empty() {
            return Err(CoreError::BadRequest(
                "root_session_id 不能为空".to_string(),
            ));
        }

        let id = self.resolve(locator)?;
        let (old_root_session_id, old_name, state) = {
            let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
            (
                entry.root_session_id.clone(),
                entry.name.clone(),
                entry.state,
            )
        };
        if state.is_terminal() {
            return Err(CoreError::AlreadyTerminal);
        }

        let new_name = match name {
            Some(name) => {
                validate_name(name)?;
                name.to_string()
            }
            None => old_name.clone(),
        };
        let old_key = (old_root_session_id.clone(), old_name.clone());
        let new_key = (root_session_id.to_string(), new_name.clone());

        // 目标键与当前键相同：无实际迁移，只重置无会话计时。
        if old_key == new_key {
            if let Some(entry) = self.by_id.get_mut(&id) {
                entry.clientless_since = None;
            }
            return Ok((
                RebindResult {
                    terminal_id: id.0,
                    root_session_id: root_session_id.to_string(),
                    name: new_name,
                },
                None,
            ));
        }

        let mut displaced = None;
        if let Some(target_id) = self.by_key.get(&new_key).cloned() {
            if target_id != id {
                if !force {
                    return Err(CoreError::SessionExists {
                        terminal_id: target_id.0.clone(),
                    });
                }
                // force：目标旧终端先落终态并从索引/订阅簿移除；
                // 后端由调用方在锁外终止（与 register_create 的置换语义一致）。
                displaced = self.mark_interrupted(&target_id)?;
                self.by_id.remove(&target_id);
                self.by_key.remove(&new_key);
                self.subscriptions.by_terminal.remove(&target_id);
                for excluded in self.subscriptions.excluded.values_mut() {
                    excluded.remove(&target_id);
                }
            }
        }

        self.by_key.remove(&old_key);
        {
            let entry = self.by_id.get_mut(&id).ok_or(CoreError::NotFound)?;
            entry.root_session_id = root_session_id.to_string();
            entry.name = new_name.clone();
            // 新会话的心跳/超时从下一轮 ticker 重新判断。
            entry.clientless_since = None;
        }
        self.by_key.insert(new_key, id.clone());
        self.publish(
            &id,
            EventKind::Rebound {
                old_root_session_id,
                new_root_session_id: root_session_id.to_string(),
                old_name,
                new_name: new_name.clone(),
            },
        )?;

        Ok((
            RebindResult {
                terminal_id: id.0,
                root_session_id: root_session_id.to_string(),
                name: new_name,
            },
            displaced,
        ))
    }

    /// 申请输入 lease。
    ///
    /// - 空闲：直接授予并返回 `owner = lease = client_id`。
    /// - 已由同一 client 持有：幂等返回。
    /// - 已被他人持有且 `force = false`：`permission_denied`。
    /// - 已被他人持有且 `force = true`：先向订阅者广播 `lease_revoked`，再授予。
    ///
    /// 终态终端拒绝 claim，对齐 `docs/specs/PROTOCOL.md` §13。
    pub fn claim(
        &mut self,
        locator: &TerminalLocator,
        client_id: &str,
        force: bool,
    ) -> Result<ClaimResult, CoreError> {
        let id = self.resolve(locator)?;
        let (state, current_owner) = {
            let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
            (entry.state, entry.lease.clone())
        };
        if state.is_terminal() {
            return Err(CoreError::AlreadyTerminal);
        }

        if let Some(owner) = current_owner {
            if owner == client_id {
                return Ok(ClaimResult {
                    owner,
                    lease: client_id.to_string(),
                });
            }
            if !force {
                return Err(CoreError::PermissionDenied);
            }

            // 先广播撤销事件，旧 owner 仍持有订阅时可即时收到。
            self.publish(&id, EventKind::LeaseRevoked { client_id: owner })?;
        }

        let entry = self.by_id.get_mut(&id).ok_or(CoreError::NotFound)?;
        entry.lease = Some(client_id.to_string());
        Ok(ClaimResult {
            owner: client_id.to_string(),
            lease: client_id.to_string(),
        })
    }

    /// 释放输入 lease；仅 owner 可释放，非 owner 或空闲时返回 `permission_denied`。
    ///
    /// 终态终端拒绝 release，对齐 `docs/specs/PROTOCOL.md` §13；连接断开时用
    /// [`Registry::release_all`] 绕过该限制做兜底清理。
    pub fn release(&mut self, locator: &TerminalLocator, client_id: &str) -> Result<(), CoreError> {
        let id = self.resolve(locator)?;
        let allowed = {
            let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
            if entry.state.is_terminal() {
                return Err(CoreError::AlreadyTerminal);
            }
            entry.lease.as_deref() == Some(client_id)
        };
        if !allowed {
            return Err(CoreError::PermissionDenied);
        }

        if let Some(entry) = self.by_id.get_mut(&id) {
            entry.lease = None;
        }
        // owner 主动释放同样视为 lease 被释放，广播给订阅者。
        self.publish(
            &id,
            EventKind::LeaseRevoked {
                client_id: client_id.to_string(),
            },
        )?;
        Ok(())
    }

    /// 返回终端创建时的启动程序（`CreateParams.cmd` 原值）。
    ///
    /// service 用它识别 shell 类型以决定是否追加 marker；只读借用，避免克隆。
    pub fn shell_command(&self, locator: &TerminalLocator) -> Result<&str, CoreError> {
        let id = self.resolve(locator)?;
        self.by_id
            .get(&id)
            .map(|entry| entry.command.as_str())
            .ok_or(CoreError::NotFound)
    }

    /// 是否为 SSH 终端。
    ///
    /// service 用它强制跳过本地 shell marker：远端 shell 无法保证本地追加的
    /// 语法成立，也不能把本机 `exit_code` 语义套到远端。
    pub fn is_ssh(&self, locator: &TerminalLocator) -> Result<bool, CoreError> {
        let id = self.resolve(locator)?;
        self.by_id
            .get(&id)
            .map(|entry| entry.ssh)
            .ok_or(CoreError::NotFound)
    }

    /// 该终端是否应在 `terminal.send` 时预记录 transcript 命令。
    ///
    /// 返回 `true` 表示命令在写后端前先落一条 pending 记录；宿主 `C` 到达后
    /// 通过 [`Registry::take_pending_transcript_generation`] 原地认领，避免
    /// 首段输出先于 `C` 处理时被上一命令的终态丢弃。
    ///
    /// `termx-bash`/`termx-pwsh` 的 `C` 与 PTY 输出都可能来自两条链路，
    /// 即使启用带外事件也必须 send-time 预记录，避免首段输出先于 `C`
    /// 被上一命令的终态丢弃。raw-only 终端继续依赖 in-band OSC/静默判定。
    /// 终端不存在时返回 [`CoreError::NotFound`]。
    pub fn records_transcript_on_send(&self, locator: &TerminalLocator) -> Result<bool, CoreError> {
        let id = self.resolve(locator)?;
        self.by_id
            .get(&id)
            .map(|entry| !entry.shell_events || is_structured_shell(entry))
            .ok_or(CoreError::NotFound)
    }

    /// 返回终端创建模式；`terminal.send` 据此决定 `enter` 追加 `\r` 还是 `\n`。
    pub fn mode(&self, locator: &TerminalLocator) -> Result<Mode, CoreError> {
        let id = self.resolve(locator)?;
        self.by_id
            .get(&id)
            .map(|entry| entry.mode)
            .ok_or(CoreError::NotFound)
    }

    /// 校验客户端是否可向该终端写入输入（供 M2 `terminal.send` 使用）。
    ///
    /// 终态终端返回 `already_terminal`；无 lease 或非 owner 返回
    /// `permission_denied`。
    pub fn ensure_input_allowed(
        &self,
        locator: &TerminalLocator,
        client_id: &str,
    ) -> Result<(), CoreError> {
        let id = self.resolve(locator)?;
        let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
        if entry.state.is_terminal() {
            return Err(CoreError::AlreadyTerminal);
        }
        match entry.lease.as_deref() {
            Some(owner) if owner == client_id => Ok(()),
            _ => Err(CoreError::PermissionDenied),
        }
    }

    /// 写入前的锁内阶段：校验 lease 归属、刷新 `last_activity` 并返回后端句柄。
    ///
    /// 终态终端返回 `already_terminal`；无 lease 或非 owner 返回
    /// `permission_denied`。调用方必须在释放 registry 锁后再调用
    /// [`TerminalBackend::write`]。
    pub fn prepare_send(
        &mut self,
        locator: &TerminalLocator,
        client_id: &str,
    ) -> Result<Arc<dyn TerminalBackend>, CoreError> {
        let id = self.resolve(locator)?;
        let entry = self.by_id.get_mut(&id).ok_or(CoreError::NotFound)?;
        if entry.state.is_terminal() {
            return Err(CoreError::AlreadyTerminal);
        }
        match entry.lease.as_deref() {
            Some(owner) if owner == client_id => {
                entry.touch(OffsetDateTime::now_utc());
                Ok(Arc::clone(&entry.backend))
            }
            _ => Err(CoreError::PermissionDenied),
        }
    }

    /// 向终端写入输入（同步便捷入口）。
    ///
    /// 持有 registry 锁的调用方应改用 [`Registry::prepare_send`] + 锁外写。
    pub fn send(
        &mut self,
        locator: &TerminalLocator,
        client_id: &str,
        bytes: &[u8],
    ) -> Result<(), CoreError> {
        self.prepare_send(locator, client_id)?.write(bytes)
    }

    /// 调整尺寸前的锁内阶段：校验非终态、记录尺寸与活动时间并返回后端句柄。
    ///
    /// 终态返回 `already_terminal`。尺寸先乐观写入条目快照，后端调整失败时
    /// 由调用方返回错误，客户端可再发一次 `resize` 纠正。
    pub fn prepare_resize(
        &mut self,
        locator: &TerminalLocator,
        cols: u16,
        rows: u16,
    ) -> Result<Arc<dyn TerminalBackend>, CoreError> {
        let id = self.resolve(locator)?;
        let entry = self.by_id.get_mut(&id).ok_or(CoreError::NotFound)?;
        if entry.state.is_terminal() {
            return Err(CoreError::AlreadyTerminal);
        }
        entry.cols = cols;
        entry.rows = rows;
        entry.touch(OffsetDateTime::now_utc());
        Ok(Arc::clone(&entry.backend))
    }

    /// 调整终端窗口大小（同步便捷入口）。
    pub fn resize(
        &mut self,
        locator: &TerminalLocator,
        cols: u16,
        rows: u16,
    ) -> Result<(), CoreError> {
        self.prepare_resize(locator, cols, rows)?.resize(cols, rows)
    }

    /// 发送信号前的锁内阶段：校验非终态并返回锁外执行所需信息。
    ///
    /// 本阶段**不**记录 `pending_interrupt`。后端 `signal` 成功返回后，调用方
    /// 必须再调用 [`Registry::confirm_signal`]；失败（未知信号、进程已退出等）
    /// 不落 pending，避免把随后的正常退出误报为 `interrupted`。
    ///
    /// 终态返回 `already_terminal`；不要求输入 lease（与 `claim` 一致，
    /// 信号属于进程控制而非输入）。
    pub fn prepare_signal(
        &mut self,
        locator: &TerminalLocator,
        name: &str,
    ) -> Result<PreparedSignal, CoreError> {
        let id = self.resolve(locator)?;
        let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
        if entry.state.is_terminal() {
            return Err(CoreError::AlreadyTerminal);
        }

        Ok(PreparedSignal {
            terminal_id: id,
            backend: Arc::clone(&entry.backend),
            signal: name.to_string(),
        })
    }

    /// 后端信号成功送达后记录中断意图；终端已终态时忽略。
    ///
    /// `SIGINT` 记为 `source:"control-c"`，其它受支持信号记为 `source:"signal"`；
    /// 未知信号不记录。进程随后退出时由 [`Registry::report_exit`] 映射为
    /// `interrupted`（Windows 无 `Signaled` 事件，也靠该 pending 统一语义）。
    pub fn confirm_signal(&mut self, id: &TerminalId, name: &str) -> Result<(), CoreError> {
        let Some(canonical) = canonical_signal(name) else {
            return Ok(());
        };
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        if entry.state.is_terminal() {
            return Ok(());
        }
        let source = if canonical == "SIGINT" {
            CONTROL_C_SOURCE
        } else {
            SIGNAL_SOURCE
        };
        entry.pending_interrupt = Some(PendingInterrupt {
            signal: canonical.to_string(),
            source: source.to_string(),
        });
        Ok(())
    }

    /// 向终端发送信号（同步便捷入口）。
    ///
    /// 持有 registry 锁的调用方应改用 [`Registry::prepare_signal`] +
    /// [`Registry::confirm_signal`]，并在两阶段之间释放锁调用后端。
    pub fn signal(&mut self, locator: &TerminalLocator, name: &str) -> Result<(), CoreError> {
        let prepared = self.prepare_signal(locator, name)?;
        prepared.execute()?;
        self.confirm_signal(&prepared.terminal_id, name)
    }

    /// 释放该客户端持有的全部 lease；供连接断开时兜底，避免终端永久锁死。
    ///
    /// 不校验终端状态；仍会向订阅者广播 `lease_revoked`，让其它客户端及时
    /// 知道 lease 已空闲（旧 owner 自身即将断开，是否收到不影响语义）。
    pub fn release_all(&mut self, client_id: &str) {
        let mut revoked = Vec::new();
        for (id, entry) in &mut self.by_id {
            if entry.lease.as_deref() == Some(client_id) {
                entry.lease = None;
                revoked.push(id.clone());
            }
        }

        for id in revoked {
            if let Err(error) = self.publish(
                &id,
                EventKind::LeaseRevoked {
                    client_id: client_id.to_string(),
                },
            ) {
                tracing::debug!(terminal_id = %id, %error, "广播 lease_revoked 失败");
            }
        }
    }

    /// 扫描并处理到期的 `idle_timeout`。
    ///
    /// 仅处理 `state=running`、`value > 0` 且 `now - last_activity >= timeout` 的
    /// 终端：
    /// - `detach`：向该终端的订阅者逐个投递 `detached{reason:"idle_timeout"}`，
    ///   移除终端级订阅并把最后活动时间刷新为 `now`，避免下一 tick 重复触发；
    ///   终端继续运行。
    /// - `terminate`：锁内落 `interrupted{signal:"SIGTERM", source:"timeout"}`
    ///   终态、清 lease 并取出后端 `Arc`；后端由调用方在锁外终止。
    ///
    /// `value = 0` 或未配置 `idle_timeout` 的终端永不触发。
    pub fn expire_idle(&mut self, now: OffsetDateTime) -> Vec<IdleAction> {
        let mut due = Vec::new();
        for (id, entry) in &self.by_id {
            if entry.state != LifecycleState::Running {
                continue;
            }
            let Some(idle_timeout) = entry.idle_timeout else {
                continue;
            };
            let Some(action) = idle_timeout_action(&idle_timeout) else {
                continue;
            };
            let Some(last_activity) = entry.activity_time() else {
                continue;
            };
            let elapsed_ms = (now - last_activity).whole_milliseconds();
            if elapsed_ms >= idle_timeout_millis(&idle_timeout) {
                due.push((id.clone(), action));
            }
        }

        let mut actions = Vec::new();
        for (id, action) in due {
            match action {
                IdleTimeoutAction::Detach => {
                    let clients = self.detach_idle_subscribers(&id, now);
                    actions.push(IdleAction::Detach {
                        terminal_id: id,
                        clients,
                    });
                }
                IdleTimeoutAction::Terminate => {
                    match self.mark_interrupted_with(&id, TERMINATE_SIGNAL, TIMEOUT_SOURCE) {
                        Ok(Some(backend)) => actions.push(IdleAction::Terminate(backend)),
                        // 同一锁内状态已被其它路径改写为终态：幂等跳过。
                        Ok(None) => {}
                        Err(error) => {
                            tracing::debug!(terminal_id = %id, %error, "空闲超时终止失败");
                        }
                    }
                }
            }
        }
        actions
    }

    /// 扫描「所属 opencode 会话已不存活」的运行中终端。
    ///
    /// `is_session_live` 由 service 根据会话心跳表提供（core 不维护会话 map）。
    /// 每个周期：
    /// - 会话存活：清空 `clientless_since`；
    /// - 会话不存活：首次记录 `now`，超过 `timeout` 时加入返回值。
    ///
    /// `timeout` 为 0 表示关闭该检查，直接返回空列表。返回的候选携带后端
    /// `Arc`，供调用方在锁外查询进程树或终止；registry 本身不调用后端。
    pub fn candidates_clientless<F>(
        &mut self,
        now: Instant,
        timeout: Duration,
        mut is_session_live: F,
    ) -> Vec<ClientlessCandidate>
    where
        F: FnMut(&str) -> bool,
    {
        if timeout.is_zero() {
            return Vec::new();
        }

        let mut due = Vec::new();
        for (id, entry) in &mut self.by_id {
            if entry.state != LifecycleState::Running {
                continue;
            }
            // Web 未绑定终端（保留 root）没有 opencode 会话，豁免无会话超时；
            // 只能由管理员手动 terminate。
            if entry.root_session_id == WEB_ROOT_SESSION_ID {
                entry.clientless_since = None;
                continue;
            }
            if is_session_live(&entry.root_session_id) {
                entry.clientless_since = None;
                continue;
            }
            match entry.clientless_since {
                None => entry.clientless_since = Some(now),
                Some(since) if now.saturating_duration_since(since) >= timeout => {
                    due.push(ClientlessCandidate {
                        terminal_id: id.clone(),
                        root_session_id: entry.root_session_id.clone(),
                        pid: entry.pid,
                        backend: Arc::clone(&entry.backend),
                    });
                }
                Some(_) => {}
            }
        }
        due
    }

    /// 重新开始无会话计时（`now` 为新的起点）。
    ///
    /// `no_children` 策略到期时若仍存在用户子进程，用该方法续期，避免
    /// 下一 tick 立即重复判定。终端不存在或已终态时返回 `false`。
    pub fn reset_clientless(&mut self, id: &TerminalId, now: Instant) -> bool {
        let Some(entry) = self.by_id.get_mut(id) else {
            return false;
        };
        if entry.state != LifecycleState::Running {
            return false;
        }
        entry.clientless_since = Some(now);
        true
    }

    /// 按指定来源终止终端：锁内落 `interrupted{signal:"SIGTERM", source}` 终态。
    ///
    /// 返回需要在锁外终止的后端；已终态返回 `None`（幂等），未知终端返回
    /// `not_found`。本方法不调用后端，可在持有 registry 锁时安全调用。
    pub fn terminate_with_source(
        &mut self,
        id: &TerminalId,
        source: &str,
    ) -> Result<Option<Arc<dyn TerminalBackend>>, CoreError> {
        if !self.by_id.contains_key(id) {
            return Err(CoreError::NotFound);
        }
        self.mark_interrupted_with(id, TERMINATE_SIGNAL, source)
    }

    /// 刷新该客户端已附着终端的 `last_client_seen`（仅诊断）。
    ///
    /// 兼容旧的无参 `heartbeat`：连接存活即刷新附着时间，但**不参与终端
    /// 保活**。终端保活只看 `root_session_id` 的会话心跳，见
    /// [`Registry::candidates_clientless`]。
    pub fn heartbeat(&mut self, client_id: &str, now: OffsetDateTime) {
        let at = crate::format_rfc3339(now);
        for entry in self.by_id.values_mut() {
            if entry.clients.contains(client_id) {
                entry.last_client_seen = Some(at.clone());
            }
        }
    }

    /// 清理终态终端。
    ///
    /// 仅删除 `state` 为终态的条目。**必须显式给出过滤或 `all=true`**：空过滤且
    /// `all=false` 返回 `BadRequest`，绝不隐式清理全部终态。`states` 作为附加过滤
    /// （即使包含运行态也不会删除运行中终端）；`older_than` 缺省时不做时间过滤；
    /// `basis` 缺省为 `last_activity`（缺失时回退创建时间）。`all=true` 时忽略
    /// 其余过滤，清理全部终态。删除时同步移除会话键、终端级订阅与全局订阅排除项，
    /// 返回被删除的 `terminal_id` 列表。
    pub fn prune<P>(&mut self, params: P) -> Result<PruneResult, CoreError>
    where
        P: Borrow<PruneParams>,
    {
        self.prune_for(params, None)
    }

    /// 清理终态终端；`root_session_id` 为 `Some` 时只清理该会话的终端。
    ///
    /// 过滤与 `prune` 相同；会话隔离由调用方按连接身份传入。
    pub fn prune_for<P>(
        &mut self,
        params: P,
        root_session_id: Option<&str>,
    ) -> Result<PruneResult, CoreError>
    where
        P: Borrow<PruneParams>,
    {
        let params = params.borrow();
        let has_states = params
            .states
            .as_ref()
            .is_some_and(|states| !states.is_empty());
        if !params.all && !has_states && params.older_than.is_none() {
            return Err(CoreError::BadRequest(
                "terminal.prune 需要 states/older_than 过滤，或显式 all=true".to_string(),
            ));
        }

        let now = OffsetDateTime::now_utc();
        let basis = params.basis.unwrap_or(PruneBasis::LastActivity);
        let threshold_ms = params.older_than.as_ref().map(time_span_millis);

        let mut removed = Vec::new();
        for (id, entry) in &self.by_id {
            if let Some(root) = root_session_id {
                if entry.root_session_id != root {
                    continue;
                }
            }
            if !entry.state.is_terminal() {
                continue;
            }
            if !params.all {
                if let Some(states) = &params.states {
                    if !states.contains(&entry.state) {
                        continue;
                    }
                }
                if let Some(threshold_ms) = threshold_ms {
                    let basis_time = match basis {
                        PruneBasis::LastActivity => entry.activity_time(),
                        PruneBasis::CreatedAt => crate::parse_rfc3339(&entry.created_at),
                    };
                    let Some(basis_time) = basis_time else {
                        continue;
                    };
                    if (now - basis_time).whole_milliseconds() < threshold_ms {
                        continue;
                    }
                }
            }
            removed.push(id.clone());
        }

        for id in &removed {
            if let Some(entry) = self.by_id.remove(id) {
                self.by_key
                    .remove(&(entry.root_session_id.clone(), entry.name.clone()));
            }
            self.subscriptions.by_terminal.remove(id);
            for excluded in self.subscriptions.excluded.values_mut() {
                excluded.remove(id);
            }
        }

        Ok(PruneResult {
            removed: removed.into_iter().map(|id| id.0).collect(),
        })
    }

    /// 解析定位器；`terminal_id` 优先于 `(root_session_id, name)`。
    pub fn resolve(&self, locator: &TerminalLocator) -> Result<TerminalId, CoreError> {
        if let Some(raw) = &locator.terminal_id {
            let id = TerminalId::new(raw.clone());
            return if self.by_id.contains_key(&id) {
                Ok(id)
            } else {
                Err(CoreError::NotFound)
            };
        }

        match (&locator.root_session_id, &locator.name) {
            (Some(root), Some(name)) => self
                .by_key
                .get(&(root.clone(), name.clone()))
                .cloned()
                .ok_or(CoreError::NotFound),
            _ => Err(CoreError::BadRequest(
                "定位参数需要 terminal_id 或 (root_session_id, name)".to_string(),
            )),
        }
    }

    /// 解析定位器并校验调用者是否有权访问该终端。
    ///
    /// `admin=true` 直接放行；否则要求终端 `root_session_id` 与调用者一致。
    /// 终端不存在返回 `not_found`，存在但归属不同返回 `permission_denied`
    /// （不携带终端内容）。调用方应在同一把 registry 锁内继续执行实际操作，
    /// 避免鉴权与操作之间的迁移竞态。
    pub fn authorize(
        &self,
        locator: &TerminalLocator,
        admin: bool,
        root_session_id: Option<&str>,
    ) -> Result<TerminalId, CoreError> {
        let id = self.resolve(locator)?;
        if admin {
            return Ok(id);
        }
        let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
        if root_session_id != Some(entry.root_session_id.as_str()) {
            return Err(CoreError::PermissionDenied);
        }
        Ok(id)
    }

    /// 返回指定终端的终端级订阅客户端列表（按 `client_id` 排序）。
    ///
    /// 只读诊断接口，用于测试与排障确认真实订阅状态；不改变投递语义。
    pub fn subscribed_clients(&self, id: &TerminalId) -> Vec<String> {
        self.subscriptions
            .by_terminal
            .get(id)
            .map(|clients| clients.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// 判断客户端是否在全局订阅中排除了指定终端。
    ///
    /// 只读诊断接口，用于确认 `prune` 是否清理了排除集；不改变投递语义。
    pub fn is_excluded_from_global(&self, client_id: &str, id: &TerminalId) -> bool {
        self.subscriptions
            .excluded
            .get(client_id)
            .is_some_and(|excluded| excluded.contains(id))
    }

    /// 收集全部终端后端句柄；service shutdown 时在锁外逐个 best-effort 终止。
    pub fn collect_backends(&self) -> Vec<Arc<dyn TerminalBackend>> {
        self.by_id
            .values()
            .map(|entry| Arc::clone(&entry.backend))
            .collect()
    }

    /// 按 ID 取只读条目。
    pub fn get(&self, id: &TerminalId) -> Option<&TerminalEntry> {
        self.by_id.get(id)
    }

    /// 按 ID 取可写条目（供后续事件注入）。
    pub fn get_mut(&mut self, id: &TerminalId) -> Option<&mut TerminalEntry> {
        self.by_id.get_mut(id)
    }

    /// 当前终端数量。
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// 写入一条事件：递增 `seq`、写 ring，并投递给该终端的订阅者。
    ///
    /// 终端已终态时丢弃**所有**后续事件（包括 `output`），返回当前 `seq`，
    /// 保证终态之后不再出现输出；终态事件（`exited` / `interrupted` /
    /// `error`）只发一次，避免事件摄取任务与 `terminate` 竞争时产生第二条终态。
    pub fn report_event(&mut self, id: &TerminalId, kind: EventKind) -> Result<u64, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        if entry.state.is_terminal() {
            return Ok(entry.seq);
        }
        if let Some(state) = terminal_state_for(&kind) {
            entry.state = state;
            // 终态不再接受输入，lease 与未结算的中断意图随之失效。
            entry.lease = None;
            entry.pending_interrupt = None;
            if let Some(status) = transcript_status_for(state) {
                entry
                    .transcript
                    .finalize_terminal(status, OffsetDateTime::now_utc());
            }
        }

        self.publish(id, kind)
    }

    /// 摄取一段终端输出到交互识别器，并在状态变化时广播事件。
    ///
    /// 锁内只做同步解析与 `report_event` 投递；不调用任何后端方法、不跨 `await`。
    /// 返回最后写入的 `seq`（无事件时为当前 `seq`）。
    pub fn observe_output(
        &mut self,
        id: &TerminalId,
        bytes: &[u8],
        now: OffsetDateTime,
    ) -> Result<u64, CoreError> {
        let (updates, state, confidence, current_seq) = {
            let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
            if entry.state.is_terminal() {
                return Ok(entry.seq);
            }
            let updates = entry.analyzer.observe_output(bytes, now);
            (
                updates,
                entry.analyzer.state(),
                entry.analyzer.confidence(),
                entry.seq,
            )
        };
        self.publish_interaction(id, updates, state, confidence, current_seq)
    }

    /// 用锁外采集的前台进程组与进程树采样一次交互状态。
    ///
    /// 调用方必须在 registry 锁内调用；`fg`/`tree` 由 service 在
    /// `spawn_blocking` 中读取。返回最后写入的 `seq`。
    pub fn sample_interaction(
        &mut self,
        id: &TerminalId,
        now: OffsetDateTime,
        fg: Option<u32>,
        tree: &[ProcNode],
    ) -> Result<u64, CoreError> {
        let (updates, state, confidence, current_seq) = {
            let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
            if entry.state.is_terminal() {
                return Ok(entry.seq);
            }
            let root_pgid = entry.pgid.unwrap_or(0);
            let updates = entry.analyzer.sample(now, fg, root_pgid, tree);
            (
                updates,
                entry.analyzer.state(),
                entry.analyzer.confidence(),
                entry.seq,
            )
        };
        self.publish_interaction(id, updates, state, confidence, current_seq)
    }

    /// 运行中终端的后端快照；service 交互 ticker 在锁内取句柄、锁外采集。
    pub fn running_backends(&self) -> Vec<(TerminalId, Arc<dyn TerminalBackend>)> {
        self.by_id
            .iter()
            .filter(|(_, entry)| entry.state == LifecycleState::Running)
            .map(|(id, entry)| (id.clone(), Arc::clone(&entry.backend)))
            .collect()
    }

    /// 同步快照字段并逐条广播交互更新。
    fn publish_interaction(
        &mut self,
        id: &TerminalId,
        updates: Vec<InteractionUpdate>,
        state: InteractionState,
        confidence: Confidence,
        mut seq: u64,
    ) -> Result<u64, CoreError> {
        if let Some(entry) = self.by_id.get_mut(id) {
            entry.interaction = state;
            entry.confidence = confidence;
        }
        for update in updates {
            seq = self.publish(id, update.into_event())?;
        }
        Ok(seq)
    }

    /// 结算进程退出：结合 `pending_interrupt` 决定发 `interrupted` 还是 `exited`。
    ///
    /// - 有 pending（Ctrl-C / terminate 已发出）：`interrupted{signal,source}`；
    /// - 无 pending 且 `outcome=Signal(sig)`：`interrupted{signal:<名>,source:"signal"}`；
    /// - 无 pending 且 `outcome=Code(code)`：`exited{exit_code}`。
    ///
    /// 终端已终态时由 [`Registry::report_event`] 统一忽略。
    pub fn report_exit(&mut self, id: &TerminalId, outcome: ExitOutcome) -> Result<u64, CoreError> {
        let kind = {
            let entry = self.by_id.get(id).ok_or(CoreError::NotFound)?;
            if entry.state.is_terminal() {
                return Ok(entry.seq);
            }
            match entry.pending_interrupt.clone() {
                Some(pending) => EventKind::Interrupted {
                    signal: pending.signal,
                    source: pending.source,
                },
                None => match outcome {
                    ExitOutcome::Code(exit_code) => EventKind::Exited { exit_code },
                    ExitOutcome::Signal(signal) => EventKind::Interrupted {
                        signal: signal_name(signal),
                        source: SIGNAL_SOURCE.to_string(),
                    },
                },
            }
        };
        self.report_event(id, kind)
    }

    /// 客户端附着：注册订阅、按游标回放，并 emit `attached`。
    ///
    /// 返回的 `cursor` 是**不含本次 `attached` 事件**的基线；`attached` 作为
    /// 首个实时事件紧随其后。`cursor` 早于 ring 下界时返回 `reset: too_old` +
    /// 基线游标，不回放历史；待回放事件超出订阅者队列剩余容量时返回
    /// `reset: buffer_overflow` + 基线游标，同样跳过历史但保持订阅；无
    /// `cursor` 时只回快照，不回放历史。
    pub fn attach(
        &mut self,
        locator: &TerminalLocator,
        client_id: &str,
        cursor: Option<u64>,
        tx: EventSender,
    ) -> Result<AttachResult, CoreError> {
        let id = self.resolve(locator)?;
        let (snapshot, baseline, reset, replay) = {
            let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
            // 为紧随回放之后的 `attached` 预留一个队列位置，避免回放刚好填满
            // 后 `attached` 投递失败触发 `limit_exceeded`。
            let available = tx.capacity().saturating_sub(1);
            let (reset, replay) = replay_after(entry, cursor, available);
            (entry.snapshot(), entry.seq, reset, replay)
        };

        // 先注册再回放：与 report_event 同受 registry 锁保护，不会出现空窗或重复。
        self.subscribe_terminal(&id, client_id, tx.clone());
        self.clear_exclusion(&id, client_id);
        // 终端级附着计入客户端集合，并刷新无客户端计时起点。
        self.mark_client_attached(&id, client_id, OffsetDateTime::now_utc());
        let delivery = self.deliver_replay(&tx, replay);
        if delivery == Delivery::Throttled {
            self.throttle_client(client_id);
        }
        self.publish(
            &id,
            EventKind::Attached {
                client_id: client_id.to_string(),
            },
        )?;

        Ok(AttachResult {
            cursor: baseline,
            snapshot,
            reset,
        })
    }

    /// 显式订阅单个终端；有 `cursor` 时按 ring 回放，但不 emit `attached`。
    ///
    /// 待回放事件超出订阅者队列剩余容量时返回 `reset: buffer_overflow` + 基线
    /// 游标并跳过历史回放；订阅仍然生效，后续实时事件正常投递。
    pub fn subscribe(
        &mut self,
        locator: &TerminalLocator,
        client_id: &str,
        cursor: Option<u64>,
        tx: EventSender,
    ) -> Result<SubscriptionCursor, CoreError> {
        let id = self.resolve(locator)?;
        let (baseline, reset, replay) = {
            let entry = self.by_id.get(&id).ok_or(CoreError::NotFound)?;
            let (reset, replay) = replay_after(entry, cursor, tx.capacity());
            (entry.seq, reset, replay)
        };

        self.subscribe_terminal(&id, client_id, tx.clone());
        self.clear_exclusion(&id, client_id);
        // 终端级订阅同样计入附着；`subscribe_all` 不计入。
        self.mark_client_attached(&id, client_id, OffsetDateTime::now_utc());
        let delivery = self.deliver_replay(&tx, replay);
        if delivery == Delivery::Throttled {
            self.throttle_client(client_id);
        }

        Ok(SubscriptionCursor {
            terminal_id: id.0.clone(),
            cursor: baseline,
            reset,
        })
    }

    /// 订阅所有现有终端以及未来新建终端的事件；返回现有终端的基线游标。
    pub fn subscribe_all(&mut self, client_id: &str, tx: EventSender) -> Vec<SubscriptionCursor> {
        self.subscriptions.all.insert(client_id.to_string(), tx);

        self.by_id
            .iter()
            .map(|(id, entry)| SubscriptionCursor {
                terminal_id: id.0.clone(),
                cursor: entry.seq,
                reset: None,
            })
            .collect()
    }

    /// 按定位器移除单个终端的订阅并 emit `detached`。
    pub fn detach(&mut self, locator: &TerminalLocator, client_id: &str) -> Result<(), CoreError> {
        let id = self.resolve(locator)?;
        self.detach_with_reason(&id, client_id, DETACH_REASON)
    }

    /// 带自定义原因的 detach；未订阅时幂等返回 `Ok`，且不重复 emit。
    pub fn detach_with_reason(
        &mut self,
        id: &TerminalId,
        client_id: &str,
        reason: &str,
    ) -> Result<(), CoreError> {
        if !self.by_id.contains_key(id) {
            return Err(CoreError::NotFound);
        }

        let removed = self
            .subscriptions
            .by_terminal
            .get_mut(id)
            .is_some_and(|clients| clients.remove(client_id).is_some());
        // 即使此前没有终端级订阅，detach 也把该终端从全局订阅中排除。
        self.exclude_terminal(id, client_id);
        // 最后一个客户端 detach 时记录当前时间，作为无客户端超时起点。
        self.mark_client_detached(id, client_id, OffsetDateTime::now_utc());
        if removed {
            self.publish(
                id,
                EventKind::Detached {
                    client_id: client_id.to_string(),
                    reason: reason.to_string(),
                },
            )?;
        }
        Ok(())
    }

    /// 移除该客户端的所有订阅并逐个 emit `detached`；供连接断开时清理。
    ///
    /// 仅对存在终端级订阅（`attach`/`subscribe`）的终端发 `detached`；
    /// 只通过 `subscribe_all` 关注该终端的客户端不发。
    pub fn detach_all(&mut self, client_id: &str, reason: &str) -> Result<(), CoreError> {
        let mut targets = BTreeSet::new();
        for (id, clients) in &self.subscriptions.by_terminal {
            if clients.contains_key(client_id) {
                targets.insert(id.clone());
            }
        }

        for clients in self.subscriptions.by_terminal.values_mut() {
            clients.remove(client_id);
        }
        self.subscriptions.all.remove(client_id);
        self.subscriptions.excluded.remove(client_id);

        // 同步清理条目的附着集合；最后一个客户端断开时刷新计时起点。
        let now = OffsetDateTime::now_utc();
        for entry in self.by_id.values_mut() {
            entry.remove_client(client_id, now);
        }

        for id in targets {
            if self.by_id.contains_key(&id) {
                self.publish(
                    &id,
                    EventKind::Detached {
                        client_id: client_id.to_string(),
                        reason: reason.to_string(),
                    },
                )?;
            }
        }
        Ok(())
    }

    /// 仅移除单个终端的订阅，不 emit `detached`；同时把该终端从全局订阅排除。
    pub fn unsubscribe(&mut self, id: &TerminalId, client_id: &str) -> Result<(), CoreError> {
        if !self.by_id.contains_key(id) {
            return Err(CoreError::NotFound);
        }

        if let Some(clients) = self.subscriptions.by_terminal.get_mut(id) {
            clients.remove(client_id);
        }
        self.exclude_terminal(id, client_id);
        self.mark_client_detached(id, client_id, OffsetDateTime::now_utc());
        Ok(())
    }

    /// 移除该客户端的全部订阅（含 `subscribe_all`），不 emit `detached`。
    pub fn unsubscribe_all(&mut self, client_id: &str) {
        for clients in self.subscriptions.by_terminal.values_mut() {
            clients.remove(client_id);
        }
        self.subscriptions.all.remove(client_id);
        self.subscriptions.excluded.remove(client_id);

        let now = OffsetDateTime::now_utc();
        for entry in self.by_id.values_mut() {
            entry.remove_client(client_id, now);
        }
    }

    /// 空闲超时的 detach 动作：向该终端订阅者投递 `detached` 并移除订阅。
    ///
    /// 每个终端级订阅客户端各收到一条带自身 `client_id` 的 `detached`；随后
    /// 移除终端级订阅，并把客户端加入该终端的全局订阅排除集（再次
    /// `attach`/`subscribe` 时恢复）。最后把 `last_activity` 刷成 `now`，
    /// 保证同一时间点不会重复触发。
    fn detach_idle_subscribers(&mut self, id: &TerminalId, now: OffsetDateTime) -> Vec<String> {
        let clients: Vec<String> = self
            .subscriptions
            .by_terminal
            .get(id)
            .map(|subscribers| subscribers.keys().cloned().collect())
            .unwrap_or_default();

        for client_id in &clients {
            if let Err(error) = self.publish(
                id,
                EventKind::Detached {
                    client_id: client_id.clone(),
                    reason: IDLE_DETACH_REASON.to_string(),
                },
            ) {
                tracing::debug!(terminal_id = %id, %error, "投递空闲 detached 失败");
            }
        }

        self.subscriptions.by_terminal.remove(id);
        for client_id in &clients {
            self.exclude_terminal(id, client_id);
            if let Some(entry) = self.by_id.get_mut(id) {
                entry.remove_client(client_id, now);
            }
        }
        if let Some(entry) = self.by_id.get_mut(id) {
            entry.touch(now);
        }
        clients
    }

    /// 注册或替换终端级订阅；同时清掉该订阅者在此终端上的陈旧滞后区间。
    fn subscribe_terminal(&mut self, id: &TerminalId, client_id: &str, tx: EventSender) {
        self.subscriptions
            .by_terminal
            .entry(id.clone())
            .or_default()
            .insert(client_id.to_string(), tx);
    }

    /// 把客户端计入终端附着集合并刷新无客户端计时起点。
    fn mark_client_attached(&mut self, id: &TerminalId, client_id: &str, at: OffsetDateTime) {
        if let Some(entry) = self.by_id.get_mut(id) {
            entry.add_client(client_id, at);
        }
    }

    /// 从终端附着集合移除客户端；集合清空时记录 `at`。
    fn mark_client_detached(&mut self, id: &TerminalId, client_id: &str, at: OffsetDateTime) {
        if let Some(entry) = self.by_id.get_mut(id) {
            entry.remove_client(client_id, at);
        }
    }

    /// 把终端加入该客户端的全局订阅排除集。
    fn exclude_terminal(&mut self, id: &TerminalId, client_id: &str) {
        self.subscriptions
            .excluded
            .entry(client_id.to_string())
            .or_default()
            .insert(id.clone());
    }

    /// 从该客户端的全局订阅排除集中移除终端。
    fn clear_exclusion(&mut self, id: &TerminalId, client_id: &str) {
        let becomes_empty =
            self.subscriptions
                .excluded
                .get_mut(client_id)
                .is_some_and(|excluded| {
                    excluded.remove(id);
                    excluded.is_empty()
                });
        if becomes_empty {
            self.subscriptions.excluded.remove(client_id);
        }
    }

    /// 写入事件并把完整事件投递给终端级与全局订阅者。
    ///
    /// 若写入过程淘汰了输出，`output_dropped` marker 已在 ring 内且各自占用
    /// 独立 `seq`；这里按 `seq` 升序依次投递（先本次事件，再各条 marker）。
    fn publish(&mut self, id: &TerminalId, kind: EventKind) -> Result<u64, CoreError> {
        let (seq, event, markers) = {
            let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
            let (record, markers) = entry.emit_with_drops(kind);
            let seq = record.seq;
            (seq, event_from_record(id, &record), markers)
        };

        self.deliver_event(id, &event);
        for marker in markers {
            self.deliver_event(id, &event_from_record(id, &marker));
        }
        Ok(seq)
    }

    /// 投递事件；同一客户端同时有终端级与全局订阅时只投递一次。
    ///
    /// 有界队列投递规则（任何事件一视同仁）：
    /// - 队满：移除该订阅者并触发 `throttle` 通知，由 service 发
    ///   `limit_exceeded` 后断开；不在订阅者侧复用 `seq` 补 marker。
    /// - 接收端已关闭：顺手移除失效订阅，避免长期泄漏。
    /// - `seq` 始终取事件自身的单调值，绝不回退。
    fn deliver_event(&mut self, id: &TerminalId, event: &Event) {
        let mut handled = BTreeSet::new();
        let mut remove_terminal = Vec::new();
        let mut remove_all = Vec::new();
        let mut throttled = Vec::new();

        let terminal_targets: Vec<(String, EventSender)> = self
            .subscriptions
            .by_terminal
            .get(id)
            .map(|clients| {
                clients
                    .iter()
                    .map(|(client_id, tx)| (client_id.clone(), tx.clone()))
                    .collect()
            })
            .unwrap_or_default();
        for (client_id, tx) in terminal_targets {
            handled.insert(client_id.clone());
            match try_deliver(&tx, event) {
                Delivery::Kept => {}
                Delivery::Closed => remove_terminal.push(client_id),
                Delivery::Throttled => {
                    remove_terminal.push(client_id.clone());
                    throttled.push(client_id);
                }
            }
        }

        let global_targets: Vec<(String, EventSender)> = self
            .subscriptions
            .all
            .iter()
            .filter(|(client_id, _)| !handled.contains(*client_id))
            .filter(|(client_id, _)| {
                !self
                    .subscriptions
                    .excluded
                    .get(*client_id)
                    .is_some_and(|excluded| excluded.contains(id))
            })
            .map(|(client_id, tx)| (client_id.clone(), tx.clone()))
            .collect();
        for (client_id, tx) in global_targets {
            match try_deliver(&tx, event) {
                Delivery::Kept => {}
                Delivery::Closed => remove_all.push(client_id),
                Delivery::Throttled => {
                    remove_all.push(client_id.clone());
                    throttled.push(client_id);
                }
            }
        }

        if let Some(clients) = self.subscriptions.by_terminal.get_mut(id) {
            for client_id in &remove_terminal {
                clients.remove(client_id);
            }
        }
        // 投递失败（接收端关闭/限流）同样清理条目的附着集合。
        let now = OffsetDateTime::now_utc();
        if let Some(entry) = self.by_id.get_mut(id) {
            for client_id in &remove_terminal {
                entry.remove_client(client_id, now);
            }
        }
        for client_id in remove_all {
            self.subscriptions.all.remove(&client_id);
        }
        for client_id in throttled {
            self.throttle_client(&client_id);
        }
    }

    /// 把历史回放投递给单个订阅者；规则与实时投递一致（含队满即断开）。
    fn deliver_replay(&mut self, tx: &EventSender, replay: Vec<Event>) -> Delivery {
        for event in replay {
            match try_deliver(tx, &event) {
                Delivery::Kept => {}
                other => return other,
            }
        }
        Delivery::Kept
    }

    /// 移除客户端全部订阅并通知 service 断开（队列无法继续投递时）。
    fn throttle_client(&mut self, client_id: &str) {
        if let Some(tx) = self.subscriptions.throttle.get(client_id) {
            if let Err(error) = tx.send(true) {
                tracing::debug!(%error, %client_id, "发送订阅限流通知失败");
            }
        }

        for clients in self.subscriptions.by_terminal.values_mut() {
            clients.remove(client_id);
        }
        self.subscriptions.all.remove(client_id);

        // 发送端关闭/限流时同步移除条目上的附着，避免残留客户端让无客户端超时失效。
        let now = OffsetDateTime::now_utc();
        for entry in self.by_id.values_mut() {
            entry.remove_client(client_id, now);
        }
    }

    /// 把终端置为 `interrupted` 终态、清 lease 并广播事件。
    ///
    /// 返回需要在锁外终止的后端；已终态返回 `None`（幂等）。本方法不调用任何
    /// 后端方法，避免在持有 registry 锁时阻塞。
    fn mark_interrupted(
        &mut self,
        id: &TerminalId,
    ) -> Result<Option<Arc<dyn TerminalBackend>>, CoreError> {
        self.mark_interrupted_with(id, TERMINATE_SIGNAL, TERMINATE_SOURCE)
    }

    /// 按指定信号与来源落 `interrupted` 终态。
    ///
    /// `detach`/`terminate` 与 `idle_timeout` 共用该路径，确保终态只发一次、
    /// lease 清理与后端 `Arc` 取出一致。
    fn mark_interrupted_with(
        &mut self,
        id: &TerminalId,
        signal: &str,
        source: &str,
    ) -> Result<Option<Arc<dyn TerminalBackend>>, CoreError> {
        let entry = self.by_id.get(id).ok_or(CoreError::NotFound)?;
        if entry.state.is_terminal() {
            return Ok(None);
        }
        let backend = Arc::clone(&entry.backend);

        if let Some(entry) = self.by_id.get_mut(id) {
            entry.state = LifecycleState::Interrupted;
            // 终态不再接受输入，lease 随之失效。
            entry.lease = None;
            // 记录中断意图；后续退出事件因终态被忽略，这里保留便于状态核对。
            entry.pending_interrupt = Some(PendingInterrupt {
                signal: signal.to_string(),
                source: source.to_string(),
            });
            entry
                .transcript
                .finalize_terminal(TranscriptStatus::Interrupted, OffsetDateTime::now_utc());
        }
        self.publish(
            id,
            EventKind::Interrupted {
                signal: signal.to_string(),
                source: source.to_string(),
            },
        )?;
        Ok(Some(backend))
    }
}

/// 取出 `idle_timeout` 的到期动作；`value = 0` 或缺少 action 时永不触发。
fn idle_timeout_action(idle_timeout: &IdleTimeout) -> Option<IdleTimeoutAction> {
    if idle_timeout.value == 0 {
        return None;
    }
    idle_timeout.action
}

/// 把 `idle_timeout` 换算成毫秒，用于与时间差比较。
fn idle_timeout_millis(idle_timeout: &IdleTimeout) -> i128 {
    let factor = match idle_timeout.unit {
        IdleTimeoutUnit::S => 1000,
        IdleTimeoutUnit::Ms => 1,
    };
    i128::from(idle_timeout.value).saturating_mul(factor)
}

/// 把 `prune.older_than` 换算成毫秒。
fn time_span_millis(span: &TimeSpan) -> i128 {
    let factor = match span.unit {
        IdleTimeoutUnit::S => 1000,
        IdleTimeoutUnit::Ms => 1,
    };
    i128::from(span.value).saturating_mul(factor)
}

/// 运行中终端补齐后端动态信息；终态或后端缺省时保持原快照。
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

/// 把外部信号名规范化为 `SIGINT` / `SIGTERM` / `SIGKILL`。
///
/// 接受大小写混写与省略 `SIG` 前缀的别名；未知信号返回 `None`，由后端决定
/// 是否报 `UnsupportedSignal`。与 `termx-proc` 的规范化保持一致。
fn canonical_signal(name: &str) -> Option<&'static str> {
    let upper = name.trim().to_ascii_uppercase();
    let bare = upper.strip_prefix("SIG").unwrap_or(upper.as_str());
    match bare {
        "INT" => Some("SIGINT"),
        "TERM" => Some("SIGTERM"),
        "KILL" => Some("SIGKILL"),
        _ => None,
    }
}

/// 信号编号 → 协议信号名；未知编号原样格式化为 `SIG<n>`。
///
/// `0` 是 SSH 层对“无法识别的信号名”的占位值，映射为 `"UNKNOWN"`，
/// 不伪造具体信号。v0 的 Unix 基线为 Linux 编号；Windows 不会产生信号终止。
fn signal_name(signal: i32) -> String {
    let name = match signal {
        0 => "UNKNOWN",
        1 => "SIGHUP",
        2 => "SIGINT",
        3 => "SIGQUIT",
        6 => "SIGABRT",
        9 => "SIGKILL",
        11 => "SIGSEGV",
        13 => "SIGPIPE",
        14 => "SIGALRM",
        15 => "SIGTERM",
        other => return format!("SIG{other}"),
    };
    name.to_string()
}

/// 是否为结构化 shell 终端（termx-bash / termx-pwsh）。
///
/// 两者的 `C` 事件与 PTY 输出都可能逆序到达；`terminal.send` 必须先落
/// pending 命令记录，等 `C` 到达后由代次认领，不能只靠 `C` 新建。
fn is_structured_shell(entry: &TerminalEntry) -> bool {
    matches!(
        entry.runtime_id.as_deref(),
        Some("termx-bash" | "termx-pwsh")
    ) || entry
        .command
        .trim()
        .rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| matches!(name, "termx-bash" | "termx-pwsh"))
}

/// 终态生命周期状态对应的命令记录状态；非终态返回 `None`。
fn transcript_status_for(state: LifecycleState) -> Option<TranscriptStatus> {
    match state {
        LifecycleState::Exited => Some(TranscriptStatus::Exited),
        LifecycleState::Interrupted => Some(TranscriptStatus::Interrupted),
        LifecycleState::Error => Some(TranscriptStatus::Error),
        _ => None,
    }
}

/// 终态事件对应的生命周期状态；非终态事件返回 `None`。
fn terminal_state_for(kind: &EventKind) -> Option<LifecycleState> {
    match kind {
        EventKind::Exited { .. } => Some(LifecycleState::Exited),
        EventKind::Interrupted { .. } => Some(LifecycleState::Interrupted),
        EventKind::Error { .. } => Some(LifecycleState::Error),
        _ => None,
    }
}

/// 按游标从 ring 取出可回放事件；游标所需历史不可用时返回 `too_old`。
///
/// `available` 是订阅者队列当前可容纳的事件数。待回放事件超过该值时返回
/// `buffer_overflow` 并**不构造回放**：逐条 `try_send` 会把队列填满并触发
/// `limit_exceeded` 断连，客户端应按基线游标改用 `terminal.read` 分段取历史。
fn replay_after(
    entry: &TerminalEntry,
    cursor: Option<u64>,
    available: usize,
) -> (Option<ResetReason>, Vec<Event>) {
    let Some(cursor) = cursor else {
        return (None, Vec::new());
    };

    if output_history_missing(entry, cursor) {
        return (Some(ResetReason::TooOld), Vec::new());
    }

    let pending = entry
        .events
        .records()
        .iter()
        .filter(|record| record.seq > cursor)
        .count();
    if pending > available {
        return (Some(ResetReason::BufferOverflow), Vec::new());
    }

    let events = entry
        .events
        .records()
        .iter()
        .filter(|record| record.seq > cursor)
        .map(|record| event_from_record(&entry.id, record))
        .collect();
    (None, events)
}

/// 拼接事件环中仍保留的全部 `output` 字节，再按 UTF-8 宽松解码。
///
/// 先拼字节再解码，避免单个 chunk 恰好切断多字节字符时产生乱码。
fn retained_output(entry: &TerminalEntry) -> String {
    let mut bytes = Vec::new();
    for record in entry.events.records() {
        if let EventKind::Output { data, .. } = &record.kind {
            match BASE64_STANDARD.decode(data) {
                Ok(chunk) => bytes.extend_from_slice(&chunk),
                Err(error) => {
                    tracing::warn!(terminal_id = %entry.id, %error, "输出 chunk 不是合法 base64，已跳过");
                }
            }
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// 取出 `read`/`search` 使用的命令输出文本。
///
/// 结构化终端（`shell_events=true`，即 `termx-pwsh`/`termx-bash` 或远端
/// 事件通道）只读 transcript 的清洗后命令输出；没有命令或没有输出时为空，
/// 绝不回退到 raw PTY。raw-only 终端没有 shell 事件，只能清洗保留的原始
/// 输出，prompt 无法可靠剔除。
fn command_output_text(entry: &TerminalEntry) -> String {
    if entry.shell_events {
        return entry.transcript.output_text();
    }
    crate::clean_raw_output(&retained_output(entry))
}

/// `terminal.read` 的只读扫描实现。
fn read_after(entry: &TerminalEntry, cursor: u64, max_bytes: Option<u64>) -> ReadResult {
    let limit = match max_bytes {
        Some(value) if value > 0 => value,
        _ => DEFAULT_READ_MAX_BYTES,
    };

    if output_history_missing(entry, cursor) {
        return ReadResult {
            chunks: Vec::new(),
            cursor: entry.seq,
            reset: Some(ResetReason::TooOld),
        };
    }

    let mut chunks = Vec::new();
    let mut scanned = cursor;
    let mut total = 0_u64;
    for record in entry.events.records() {
        if record.seq <= cursor {
            continue;
        }
        if let EventKind::Output { data, .. } = &record.kind {
            let bytes = base64_decoded_len(data);
            if total.saturating_add(bytes) > limit && !chunks.is_empty() {
                // 不跨过未返回的记录，保证下一次 read 能从这里继续。
                break;
            }
            // 单条输出本身超过 max_bytes 时仍返回该条并前进游标，避免客户端
            // 反复拿到空结果形成活锁。
            total = total.saturating_add(bytes);
            chunks.push(ReadChunk {
                seq: record.seq,
                ts: record.ts.clone(),
                data: data.clone(),
            });
        }
        // 非 Output 事件不返回，但游标要越过它们。
        scanned = record.seq;
    }

    ReadResult {
        chunks,
        cursor: scanned,
        reset: None,
    }
}

/// 游标所需的历史是否已不可用。
///
/// 两种情形：
/// - `cursor + 1` 早于 ring 中最小 `seq`（整段历史已回绕）；
/// - 游标落在输出背压已丢弃的区间内（`cursor < 曾丢弃的最大 to_seq`）。
///
/// 丢弃高水位由 [`EventBuffer::dropped_through`] 独立维护：`output_dropped`
/// marker 自身也可能被后续淘汰，只扫描 ring 内 marker 会在该窗口得到
/// `lost_to = 0`，把已丢区间当成连续可读而静默跳过输出。
fn output_history_missing(entry: &TerminalEntry, cursor: u64) -> bool {
    if entry
        .events
        .oldest_seq()
        .is_some_and(|oldest| cursor.saturating_add(1) < oldest)
    {
        return true;
    }

    let mut lost_to = entry.events.dropped_through();
    for record in entry.events.records() {
        if let EventKind::OutputDropped { to_seq, .. } = &record.kind {
            lost_to = lost_to.max(*to_seq);
        }
    }
    if let Some(to_seq) = entry.events.pending_drop_through() {
        lost_to = lost_to.max(to_seq);
    }
    cursor < lost_to
}

/// 记录 → 协议事件。
fn event_from_record(id: &TerminalId, record: &EventRecord) -> Event {
    Event {
        terminal_id: id.0.clone(),
        seq: record.seq,
        ts: record.ts.clone(),
        kind: record.kind.clone(),
    }
}

/// 向单个订阅者投递一条事件；队满或通道关闭立即返回对应结果。
///
/// `seq` 直接使用事件自身的单调值，绝不为了补 marker 而复用旧 `seq`。
fn try_deliver(tx: &EventSender, event: &Event) -> Delivery {
    match tx.try_send(event.clone()) {
        Ok(()) => Delivery::Kept,
        Err(TrySendError::Full(_)) => Delivery::Throttled,
        Err(TrySendError::Closed(_)) => Delivery::Closed,
    }
}

/// base64 文本解码后的字节数（不实际解码）。
///
/// 事件里的 `data` 均由标准 base64 编码生成；按 4 字符 3 字节换算并扣除
/// 末尾 `=` 填充即可，避免为统计字节数引入额外解码开销。
fn base64_decoded_len(data: &str) -> u64 {
    let padding = data.bytes().rev().take_while(|byte| *byte == b'=').count();
    let bytes = (data.len() / 4).saturating_mul(3);
    bytes.saturating_sub(padding) as u64
}

/// 校验创建参数：名称 → `idle_timeout`。
///
/// `mode` 由默认 [`ModeFactory`] 路由；pty/stdio 都是合法模式。
fn validate_create(params: &CreateParams) -> Result<(), CoreError> {
    validate_name(&params.name)?;
    if params.ssh.is_some() && params.mode != Mode::Pty {
        return Err(CoreError::BadRequest("SSH 终端仅支持 pty 模式".to_string()));
    }
    if let Some(idle_timeout) = &params.idle_timeout {
        idle_timeout
            .validate()
            .map_err(CoreError::InvalidIdleTimeout)?;
    }
    Ok(())
}

/// 校验会话内名称。
fn validate_name(name: &str) -> Result<(), CoreError> {
    let invalid = name.is_empty()
        || name.chars().count() > NAME_MAX_LEN
        || name.contains('/')
        || name.contains('\\');
    if invalid {
        Err(CoreError::InvalidName)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MockBackend, MockFactory};
    use termx_protocol::{IdleTimeout, IdleTimeoutUnit, Stream};

    /// 使用 mock 工厂的 registry；全部既有测试语义保持不变。
    fn mock_registry() -> Registry {
        Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory))
    }

    /// 构造创建参数。
    fn params(root_session_id: &str, name: &str, force: bool) -> CreateParams {
        CreateParams {
            mode: Mode::Pty,
            cmd: "mock".to_string(),
            args: Vec::new(),
            cwd: None,
            env: None,
            cols: None,
            rows: None,
            root_session_id: root_session_id.to_string(),
            name: name.to_string(),
            force,
            idle_timeout: None,
            ssh: None,
            allow_injection: false,
            runtime_id: None,
        }
    }

    /// 解包 rebind 失败结果；成功时按预期失败信息 panic。
    ///
    /// `rebind` 成功值含 `Arc<dyn TerminalBackend>`（未实现 `Debug`），不能直接用
    /// `expect_err`。
    fn expect_rebind_err(
        result: Result<(RebindResult, Option<Arc<dyn TerminalBackend>>), CoreError>,
    ) -> CoreError {
        match result {
            Ok(_) => panic!("rebind 本应失败"),
            Err(error) => error,
        }
    }

    /// 按 `terminal_id` 构造定位器。
    fn by_id(terminal_id: &str) -> TerminalLocator {
        TerminalLocator {
            terminal_id: Some(terminal_id.to_string()),
            root_session_id: None,
            name: None,
        }
    }

    #[test]
    fn create_returns_26_char_id_and_zero_cursor() {
        let mut registry = mock_registry();
        let result = registry
            .create(params("ses-1", "build", false))
            .expect("create 应成功");

        assert_eq!(result.terminal_id.len(), 26);
        // cursor 为 started(seq=1) 之前的位置，保证 attach 能回放 started。
        assert_eq!(result.cursor, INITIAL_CURSOR);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn pty_defaults_to_80x24_and_started_event_is_seq_one() {
        let mut registry = mock_registry();
        let result = registry
            .create(params("ses-1", "build", false))
            .expect("create 应成功");
        let id = TerminalId::new(result.terminal_id.clone());
        let entry = registry.get(&id).expect("终端应存在");

        assert_eq!((entry.cols, entry.rows), (80, 24));
        assert_eq!(entry.state, LifecycleState::Running);
        assert_eq!(entry.seq, 1);

        let record = entry.events.records().front().expect("应有 started 事件");
        assert_eq!(record.seq, 1);
        match &record.kind {
            EventKind::Started { pid, pgid, mode } => {
                assert_eq!(*pid, entry.pid.expect("pid 应存在"));
                assert_eq!(*pgid, entry.pgid.expect("pgid 应存在"));
                assert_eq!(*mode, Mode::Pty);
            }
            other => panic!("期望 started 事件，实际 {other:?}"),
        }
    }

    #[test]
    fn event_seq_increments_per_terminal() {
        let mut registry = mock_registry();
        let first = registry
            .create(params("ses-1", "first", false))
            .expect("create 应成功");
        let second = registry
            .create(params("ses-1", "second", false))
            .expect("create 应成功");

        let first_id = TerminalId::new(first.terminal_id);
        let second_id = TerminalId::new(second.terminal_id);

        let entry = registry.get_mut(&first_id).expect("终端应存在");
        let seq = entry.emit(EventKind::Output {
            data: "aGk=".to_string(),
            stream: Stream::Stdout,
        });
        assert_eq!(seq, 2);
        assert_eq!(entry.seq, 2);

        let seqs: Vec<u64> = entry
            .events
            .records()
            .iter()
            .map(|record| record.seq)
            .collect();
        assert_eq!(seqs, vec![1, 2]);

        // 每个终端独立计数。
        assert_eq!(registry.get(&second_id).expect("终端应存在").seq, 1);
    }

    #[test]
    fn duplicate_without_force_reports_old_terminal_id() {
        let mut registry = mock_registry();
        let first = registry
            .create(params("ses-1", "build", false))
            .expect("首次 create 应成功");

        let error = registry
            .create(params("ses-1", "build", false))
            .expect_err("同名且无 force 应失败");
        assert_eq!(
            error,
            CoreError::SessionExists {
                terminal_id: first.terminal_id.clone()
            }
        );
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn force_terminates_old_terminal_and_replaces_key() {
        let mut registry = mock_registry();
        let old_backend = Arc::new(MockBackend::new());
        let first = registry
            .create_with_backend(params("ses-1", "build", false), old_backend.clone())
            .expect("首次 create 应成功");
        let first_id = TerminalId::new(first.terminal_id.clone());

        assert_eq!(
            registry.get(&first_id).expect("旧终端应存在").state,
            LifecycleState::Running
        );

        let second = registry
            .create(params("ses-1", "build", true))
            .expect("force create 应成功");

        assert_ne!(first.terminal_id, second.terminal_id);
        assert!(old_backend.was_terminated(), "旧后端应被终止");
        assert_eq!(registry.len(), 1);
        assert_eq!(
            registry.status(&by_id(&first.terminal_id)),
            Err(CoreError::NotFound)
        );

        let items = registry.list(&ListParams::default());
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].terminal_id, second.terminal_id);
    }

    #[test]
    fn invalid_names_are_rejected() {
        let mut registry = mock_registry();
        let long_name = "x".repeat(NAME_MAX_LEN + 1);

        for name in ["", "a/b", "a\\b", long_name.as_str()] {
            let error = registry
                .create(params("ses-1", name, false))
                .expect_err("非法名称应失败");
            assert_eq!(error, CoreError::InvalidName, "名称 {name:?} 应非法");
        }
        assert!(registry.is_empty());
    }

    #[test]
    fn name_length_boundary_is_64() {
        let mut registry = mock_registry();
        let name_at_limit = "x".repeat(NAME_MAX_LEN);
        registry
            .create(params("ses-1", &name_at_limit, false))
            .expect("64 字符名称应通过");

        let too_long = "y".repeat(NAME_MAX_LEN + 1);
        let error = registry
            .create(params("ses-1", &too_long, false))
            .expect_err("65 字符名称应拒绝");
        assert_eq!(error, CoreError::InvalidName);
    }

    #[test]
    fn positive_idle_timeout_without_action_is_rejected() {
        let mut registry = mock_registry();
        let mut create = params("ses-1", "build", false);
        create.idle_timeout = Some(IdleTimeout {
            value: 1,
            unit: IdleTimeoutUnit::S,
            action: None,
        });

        let error = registry.create(create).expect_err("缺少 action 应失败");
        assert!(matches!(error, CoreError::InvalidIdleTimeout(_)));
    }

    #[test]
    fn list_filters_by_state_and_applies_limit() {
        let mut registry = mock_registry();
        let first = registry
            .create(params("ses-1", "a", false))
            .expect("create 应成功");
        registry
            .create(params("ses-1", "b", false))
            .expect("create 应成功");
        registry
            .create(params("ses-1", "c", false))
            .expect("create 应成功");

        registry
            .terminate(&by_id(&first.terminal_id))
            .expect("terminate 应成功");

        assert_eq!(registry.list(&ListParams::default()).len(), 3);
        assert_eq!(
            registry
                .list(&ListParams {
                    states: Some(vec![LifecycleState::Running]),
                    limit: None,
                })
                .len(),
            2
        );
        assert_eq!(
            registry
                .list(&ListParams {
                    states: Some(vec![LifecycleState::Interrupted]),
                    limit: None,
                })
                .len(),
            1
        );
        assert_eq!(
            registry
                .list(&ListParams {
                    states: None,
                    limit: Some(2),
                })
                .len(),
            2
        );
    }

    #[test]
    fn terminate_marks_terminal_and_is_idempotent() {
        let mut registry = mock_registry();
        let created = registry
            .create(params("ses-1", "build", false))
            .expect("create 应成功");
        let locator = by_id(&created.terminal_id);

        registry.terminate(&locator).expect("terminate 应成功");
        let status = registry.status(&locator).expect("终态仍可只读");
        assert_eq!(status.state, LifecycleState::Interrupted);
        assert_eq!(status.cursor, 2);

        // 幂等：不重复发终态事件。
        registry.terminate(&locator).expect("重复 terminate 应成功");
        assert_eq!(registry.status(&locator).expect("终态仍可只读").cursor, 2);
    }

    #[test]
    fn locator_supports_terminal_id_and_session_key() {
        let mut registry = mock_registry();
        let created = registry
            .create(params("ses-1", "build", false))
            .expect("create 应成功");

        let by_session = TerminalLocator {
            terminal_id: None,
            root_session_id: Some("ses-1".to_string()),
            name: Some("build".to_string()),
        };
        assert_eq!(
            registry
                .status(&by_session)
                .expect("按会话键应找到")
                .terminal_id,
            created.terminal_id
        );
        assert_eq!(
            registry
                .status(&TerminalLocator::default())
                .expect_err("空定位器应失败"),
            CoreError::BadRequest(
                "定位参数需要 terminal_id 或 (root_session_id, name)".to_string()
            )
        );
    }

    #[test]
    fn clientless_expiry_tracks_session_liveness_and_resets() {
        let mut registry = mock_registry();
        let created = registry
            .create(params("ses-1", "build", false))
            .expect("create 应成功");
        let id = TerminalId::new(created.terminal_id.clone());
        let timeout = Duration::from_millis(100);
        let start = Instant::now();

        // 会话存活时不计时。
        assert!(
            registry
                .candidates_clientless(start, timeout, |_| true)
                .is_empty()
        );
        assert!(
            registry
                .get(&id)
                .expect("终端应存在")
                .clientless_since
                .is_none()
        );

        // 首轮无会话只记录起点，未到超时不返回。
        assert!(
            registry
                .candidates_clientless(start, timeout, |_| false)
                .is_empty()
        );
        assert!(
            registry
                .get(&id)
                .expect("终端应存在")
                .clientless_since
                .is_some()
        );

        // 超过超时返回候选；即使仍有 attach 也不影响（附着不算客户端）。
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        registry
            .attach(&by_id(&created.terminal_id), "c1", None, tx)
            .expect("attach 应成功");
        let due =
            registry.candidates_clientless(start + Duration::from_millis(200), timeout, |_| false);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].terminal_id, id);

        // 会话恢复存活后清空计时。
        assert!(
            registry
                .candidates_clientless(start + Duration::from_millis(250), timeout, |_| true)
                .is_empty()
        );
        assert!(
            registry
                .get(&id)
                .expect("终端应存在")
                .clientless_since
                .is_none()
        );

        // no_children 续期后重新计时，未到超时不返回。
        let resumed = start + Duration::from_millis(300);
        assert!(
            registry
                .candidates_clientless(resumed, timeout, |_| false)
                .is_empty()
        );
        assert!(
            registry
                .candidates_clientless(resumed + Duration::from_millis(50), timeout, |_| false)
                .is_empty()
        );
        assert!(registry.reset_clientless(&id, resumed + Duration::from_millis(60)));
        assert!(
            registry
                .candidates_clientless(resumed + Duration::from_millis(100), timeout, |_| false)
                .is_empty()
        );
        let due =
            registry
                .candidates_clientless(resumed + Duration::from_millis(200), timeout, |_| false);
        assert_eq!(due.len(), 1);

        // 终止来源必须为 client_timeout。
        let backend = registry
            .terminate_with_source(&id, CLIENT_TIMEOUT_SOURCE)
            .expect("终止应成功")
            .expect("运行中终端应返回后端");
        drop(backend);
        let entry = registry.get(&id).expect("终态仍可只读");
        assert_eq!(entry.state, LifecycleState::Interrupted);
        let last = entry.events.records().back().expect("应有终态事件");
        match &last.kind {
            EventKind::Interrupted { signal, source } => {
                assert_eq!(signal, "SIGTERM");
                assert_eq!(source, CLIENT_TIMEOUT_SOURCE);
            }
            other => panic!("期望 interrupted 事件，实际 {other:?}"),
        }
    }

    #[test]
    fn clientless_scan_skips_web_root_terminal() {
        let mut registry = mock_registry();
        let created = registry
            .create(params(WEB_ROOT_SESSION_ID, "web", false))
            .expect("create 应成功");
        let id = TerminalId::new(created.terminal_id.clone());
        let timeout = Duration::from_millis(10);
        let start = Instant::now();

        // 即使没有任何存活会话，@web 终端也不参与无会话超时。
        assert!(
            registry
                .candidates_clientless(start, timeout, |_| false)
                .is_empty()
        );
        assert!(
            registry
                .candidates_clientless(start + Duration::from_millis(100), timeout, |_| false)
                .is_empty()
        );
        let entry = registry.get(&id).expect("终端应存在");
        assert_eq!(entry.state, LifecycleState::Running);
        assert!(
            entry.clientless_since.is_none(),
            "@web 终端不应记录无会话计时"
        );
    }

    #[test]
    fn rebind_moves_key_and_preserves_id_lease_and_process() {
        let mut registry = mock_registry();
        let backend = Arc::new(MockBackend::new());
        let created = registry
            .create_with_backend(params("ses-1", "build", false), backend.clone())
            .expect("create 应成功");
        let id = TerminalId::new(created.terminal_id.clone());
        registry
            .claim(&by_id(&created.terminal_id), "client-a", false)
            .expect("claim 应成功");
        let pid = registry.get(&id).expect("终端应存在").pid;

        let (result, displaced) = registry
            .rebind(&by_id(&created.terminal_id), "ses-2", Some("verify"), false)
            .expect("rebind 应成功");
        assert!(displaced.is_none());
        assert_eq!(result.terminal_id, created.terminal_id);
        assert_eq!(result.root_session_id, "ses-2");
        assert_eq!(result.name, "verify");

        let old_key = TerminalLocator {
            terminal_id: None,
            root_session_id: Some("ses-1".to_string()),
            name: Some("build".to_string()),
        };
        assert_eq!(registry.status(&old_key), Err(CoreError::NotFound));

        let new_key = TerminalLocator {
            terminal_id: None,
            root_session_id: Some("ses-2".to_string()),
            name: Some("verify".to_string()),
        };
        assert_eq!(
            registry.status(&new_key).expect("新键应可解析").terminal_id,
            created.terminal_id
        );
        let entry = registry.get(&id).expect("终端应存在");
        assert_eq!(entry.root_session_id, "ses-2");
        assert_eq!(entry.name, "verify");
        assert_eq!(entry.pid, pid, "进程保持不变");
        assert_eq!(entry.lease.as_deref(), Some("client-a"), "lease 保持不变");
        assert_eq!(entry.state, LifecycleState::Running);
        assert_eq!(backend.terminate_calls(), 0, "迁移不应终止进程");

        let rebound = entry.events.records().back().expect("应有 rebound 事件");
        match &rebound.kind {
            EventKind::Rebound {
                old_root_session_id,
                new_root_session_id,
                old_name,
                new_name,
            } => {
                assert_eq!(old_root_session_id, "ses-1");
                assert_eq!(new_root_session_id, "ses-2");
                assert_eq!(old_name, "build");
                assert_eq!(new_name, "verify");
            }
            other => panic!("期望 rebound 事件，实际 {other:?}"),
        }
    }

    #[test]
    fn rebind_conflict_without_force_reports_target_terminal_id() {
        let mut registry = mock_registry();
        let source = registry
            .create(params("ses-1", "build", false))
            .expect("create 应成功");
        let target = registry
            .create(params("ses-2", "build", false))
            .expect("create 应成功");

        let error =
            expect_rebind_err(registry.rebind(&by_id(&source.terminal_id), "ses-2", None, false));
        assert_eq!(
            error,
            CoreError::SessionExists {
                terminal_id: target.terminal_id.clone()
            }
        );
        // 失败时两个终端都保持原键。
        assert_eq!(
            registry
                .status(&TerminalLocator {
                    terminal_id: None,
                    root_session_id: Some("ses-1".to_string()),
                    name: Some("build".to_string()),
                })
                .expect("源键仍应存在")
                .terminal_id,
            source.terminal_id
        );
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn rebind_force_terminates_target_and_keeps_source() {
        let mut registry = mock_registry();
        let source = registry
            .create(params("ses-1", "build", false))
            .expect("create 应成功");
        let target_backend = Arc::new(MockBackend::new());
        let target = registry
            .create_with_backend(params("ses-2", "build", false), target_backend.clone())
            .expect("create 应成功");

        let (result, displaced) = registry
            .rebind(&by_id(&source.terminal_id), "ses-2", None, true)
            .expect("force rebind 应成功");
        // 目标旧终端在锁内落终态、锁外由调用方终止。
        displaced.expect("应返回目标旧后端").terminate();
        assert!(target_backend.was_terminated(), "目标旧后端应被终止");
        assert_eq!(result.terminal_id, source.terminal_id);
        assert_eq!(result.root_session_id, "ses-2");
        assert_eq!(result.name, "build");

        assert_eq!(
            registry.status(&by_id(&target.terminal_id)),
            Err(CoreError::NotFound)
        );
        assert_eq!(
            registry
                .status(&TerminalLocator {
                    terminal_id: None,
                    root_session_id: Some("ses-2".to_string()),
                    name: Some("build".to_string()),
                })
                .expect("新键应指向源终端")
                .terminal_id,
            source.terminal_id
        );
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn rebind_rejects_terminal_and_resets_clientless_timer() {
        let mut registry = mock_registry();
        let created = registry
            .create(params("ses-1", "build", false))
            .expect("create 应成功");
        let id = TerminalId::new(created.terminal_id.clone());
        let timeout = Duration::from_millis(100);
        let start = Instant::now();

        // 首轮无会话写入计时起点。
        assert!(
            registry
                .candidates_clientless(start, timeout, |_| false)
                .is_empty()
        );
        assert!(
            registry
                .get(&id)
                .expect("终端应存在")
                .clientless_since
                .is_some()
        );

        registry
            .rebind(&by_id(&created.terminal_id), "ses-2", None, false)
            .expect("rebind 应成功");
        assert!(
            registry
                .get(&id)
                .expect("终端应存在")
                .clientless_since
                .is_none(),
            "迁移后应重置无会话计时"
        );

        // 改名同样允许；非法名称被拒绝。
        registry
            .rebind(
                &by_id(&created.terminal_id),
                "ses-2",
                Some("renamed"),
                false,
            )
            .expect("改名应成功");
        assert_eq!(
            expect_rebind_err(registry.rebind(
                &by_id(&created.terminal_id),
                "ses-2",
                Some("a/b"),
                false
            )),
            CoreError::InvalidName
        );

        registry
            .terminate(&by_id(&created.terminal_id))
            .expect("terminate 应成功");
        assert_eq!(
            expect_rebind_err(registry.rebind(&by_id(&created.terminal_id), "ses-3", None, false)),
            CoreError::AlreadyTerminal
        );
    }

    #[test]
    fn subscribe_all_does_not_count_as_terminal_attachment() {
        let mut registry = mock_registry();
        let created = registry
            .create(params("ses-1", "build", false))
            .expect("create 应成功");
        let id = TerminalId::new(created.terminal_id);
        let (tx, _rx) = tokio::sync::mpsc::channel(8);

        registry.subscribe_all("global", tx);

        assert!(
            registry.get(&id).expect("终端应存在").clients.is_empty(),
            "subscribe_all 的全局订阅不计入单终端附着"
        );
    }
}
