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
};

use termx_protocol::{
    AttachResult, ClaimResult, Confidence, CreateParams, CreateResult, Event, EventKind,
    IdleTimeout, IdleTimeoutAction, IdleTimeoutUnit, InteractionState, LifecycleState, ListItem,
    ListParams, Mode, ProcNode, PruneBasis, PruneParams, PruneResult, ReadChunk, ReadResult,
    ResetReason, SubscriptionCursor, TerminalLocator, TerminalSnapshot, TimeSpan,
};
use time::OffsetDateTime;
use tokio::sync::{
    mpsc::{Sender, error::TrySendError},
    watch,
};

use crate::{
    BackendFactory, CoreError, DEFAULT_EVENT_BUFFER_CAPACITY, DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS,
    EventBuffer, EventRecord, InteractionAnalyzer, InteractionConfig, InteractionUpdate,
    ModeFactory, PendingInterrupt, TerminalBackend, TerminalEntry, TerminalId, now_rfc3339,
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

        let entry = TerminalEntry {
            id: id.clone(),
            root_session_id: params.root_session_id,
            name: params.name,
            mode: params.mode,
            state: LifecycleState::Running,
            interaction: InteractionState::None,
            confidence: Confidence::Low,
            pid: Some(pid),
            pgid: Some(pgid),
            ppid: None,
            foreground_pgrp,
            cols,
            rows,
            created_at,
            last_activity: None,
            seq: 0,
            events: EventBuffer::new(self.event_buffer_capacity),
            idle_timeout: params.idle_timeout,
            lease: None,
            pending_interrupt: None,
            capabilities: backend.capabilities(),
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
        let limit = params.limit.map_or(usize::MAX, |limit| limit as usize);

        self.by_id
            .values()
            .filter(|entry| match &params.states {
                Some(states) => states.contains(&entry.state),
                None => true,
            })
            .take(limit)
            .map(|entry| ListItem {
                terminal_id: entry.id.0.clone(),
                root_session_id: entry.root_session_id.clone(),
                name: entry.name.clone(),
                state: entry.state,
                mode: entry.mode,
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

    /// 申请输入 lease。
    ///
    /// - 空闲：直接授予并返回 `owner = lease = client_id`。
    /// - 已由同一 client 持有：幂等返回。
    /// - 已被他人持有且 `force = false`：`permission_denied`。
    /// - 已被他人持有且 `force = true`：先向订阅者广播 `lease_revoked`，再授予。
    ///
    /// 终态终端拒绝 claim，对齐 `docs/PROTOCOL.md` §13。
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
    /// 终态终端拒绝 release，对齐 `docs/PROTOCOL.md` §13；连接断开时用
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

    /// 清理终态终端。
    ///
    /// 仅删除 `state` 为终态的条目；`states` 作为附加过滤（即使包含运行态也不会
    /// 删除运行中终端）。`older_than` 缺省时不做时间过滤；`basis` 缺省为
    /// `last_activity`（缺失时回退创建时间）。删除时同步移除会话键、终端级订阅
    /// 与全局订阅排除项，返回被删除的 `terminal_id` 列表。
    pub fn prune<P>(&mut self, params: P) -> PruneResult
    where
        P: Borrow<PruneParams>,
    {
        let params = params.borrow();
        let now = OffsetDateTime::now_utc();
        let basis = params.basis.unwrap_or(PruneBasis::LastActivity);
        let threshold_ms = params.older_than.as_ref().map(time_span_millis);

        let mut removed = Vec::new();
        for (id, entry) in &self.by_id {
            if !entry.state.is_terminal() {
                continue;
            }
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

        PruneResult {
            removed: removed.into_iter().map(|id| id.0).collect(),
        }
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
    /// 基线游标，不回放历史；无 `cursor` 时只回快照，不回放历史。
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
            let (reset, replay) = replay_after(entry, cursor);
            (entry.snapshot(), entry.seq, reset, replay)
        };

        // 先注册再回放：与 report_event 同受 registry 锁保护，不会出现空窗或重复。
        self.subscribe_terminal(&id, client_id, tx.clone());
        self.clear_exclusion(&id, client_id);
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
            let (reset, replay) = replay_after(entry, cursor);
            (entry.seq, reset, replay)
        };

        self.subscribe_terminal(&id, client_id, tx.clone());
        self.clear_exclusion(&id, client_id);
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
        Ok(())
    }

    /// 移除该客户端的全部订阅（含 `subscribe_all`），不 emit `detached`。
    pub fn unsubscribe_all(&mut self, client_id: &str) {
        for clients in self.subscriptions.by_terminal.values_mut() {
            clients.remove(client_id);
        }
        self.subscriptions.all.remove(client_id);
        self.subscriptions.excluded.remove(client_id);
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
            for client_id in remove_terminal {
                clients.remove(&client_id);
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
fn replay_after(entry: &TerminalEntry, cursor: Option<u64>) -> (Option<ResetReason>, Vec<Event>) {
    let Some(cursor) = cursor else {
        return (None, Vec::new());
    };

    if output_history_missing(entry, cursor) {
        return (Some(ResetReason::TooOld), Vec::new());
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
}
