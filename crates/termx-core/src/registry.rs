//! 终端 registry：会话键 `(root_session_id, name)` 与 `terminal_id` 双索引。
//!
//! F2 维护状态、事件与命名冲突语义；F3 增加事件摄取、订阅与 attach/detach。
//! 当前使用 mock 后端，不创建真实进程。

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use termx_protocol::{
    AttachResult, ClaimResult, Confidence, CreateParams, CreateResult, Event, EventKind,
    InteractionState, LifecycleState, ListItem, ListParams, Mode, ResetReason, SubscriptionCursor,
    TerminalLocator, TerminalSnapshot,
};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    CoreError, DEFAULT_EVENT_BUFFER_CAPACITY, EventBuffer, MockBackend, TerminalBackend,
    TerminalEntry, TerminalId, now_rfc3339,
};

/// 会话内名称最大长度（按 Unicode 字符计数）。
const NAME_MAX_LEN: usize = 64;
/// pty 模式默认列数。
const DEFAULT_COLS: u16 = 80;
/// pty 模式默认行数。
const DEFAULT_ROWS: u16 = 24;
/// force 终止旧终端时写入事件的信号名。
const TERMINATE_SIGNAL: &str = "SIGTERM";
/// force 终止旧终端时写入事件的来源。
const TERMINATE_SOURCE: &str = "terminate";
/// 客户端主动 `terminal.detach` 时写入的断开原因。
const DETACH_REASON: &str = "detach";
/// 连接断开、服务端清理订阅时写入的断开原因。
pub const DETACH_REASON_DISCONNECT: &str = "disconnect";
/// `terminal.create` 返回的初始游标：`started`（seq=1）之前的位置。
pub const INITIAL_CURSOR: u64 = 0;

/// 每终端订阅发送端；F3 先使用无界通道。
///
/// M3 引入有界队列与 `output_dropped` 背压后，会替换为有界发送端并增加
/// 每客户端队列上限；当前 `UnboundedSender` 只保证不因慢读者阻塞事件写入。
type EventSender = UnboundedSender<Event>;
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
}

/// 终端注册表。
///
/// 内部同时维护 `terminal_id -> entry` 与 `(root_session_id, name) -> terminal_id`
/// 两份索引；`BTreeMap` 保证 `list` 输出顺序稳定。
pub struct Registry {
    /// 新建终端使用的事件缓冲容量。
    event_buffer_capacity: usize,
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
    pub fn new(event_buffer_capacity: usize) -> Self {
        Self {
            event_buffer_capacity: event_buffer_capacity.max(1),
            by_id: BTreeMap::new(),
            by_key: BTreeMap::new(),
            subscriptions: SubscriptionBook::default(),
        }
    }

    /// 使用 [`MockBackend`] 创建终端。
    pub fn create(&mut self, params: CreateParams) -> Result<CreateResult, CoreError> {
        self.create_with_backend(params, Arc::new(MockBackend::new()))
    }

    /// 使用指定后端创建终端；测试可借此验证旧后端是否被终止。
    ///
    /// 校验顺序：名称 → `idle_timeout` → 同名冲突；`force=true` 时先终止并移除旧终端，
    /// 再写入新终端。
    pub fn create_with_backend(
        &mut self,
        params: CreateParams,
        backend: Arc<dyn TerminalBackend>,
    ) -> Result<CreateResult, CoreError> {
        validate_name(&params.name)?;
        if let Some(idle_timeout) = &params.idle_timeout {
            idle_timeout
                .validate()
                .map_err(CoreError::InvalidIdleTimeout)?;
        }

        let key = (params.root_session_id.clone(), params.name.clone());
        if let Some(old_id) = self.by_key.get(&key).cloned() {
            if !params.force {
                return Err(CoreError::SessionExists {
                    terminal_id: old_id.0,
                });
            }

            // force：先落旧终端终态并终止后端，再从索引与订阅簿移除。
            self.terminate_by_id(&old_id)?;
            self.by_id.remove(&old_id);
            self.by_key.remove(&key);
            self.subscriptions.by_terminal.remove(&old_id);
            for excluded in self.subscriptions.excluded.values_mut() {
                excluded.remove(&old_id);
            }
        }

        let (cols, rows) = match params.mode {
            Mode::Pty => (
                params.cols.unwrap_or(DEFAULT_COLS),
                params.rows.unwrap_or(DEFAULT_ROWS),
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
            foreground_pgrp: None,
            cols,
            rows,
            created_at,
            last_activity: None,
            seq: 0,
            events: EventBuffer::new(self.event_buffer_capacity),
            idle_timeout: params.idle_timeout,
            lease: None,
            backend,
        };
        let mode = entry.mode;
        self.by_key.insert(key, id.clone());
        self.by_id.insert(id.clone(), entry);

        // 先入索引再发布，`subscribe_all` 的全局订阅者才能收到新终端的 started。
        self.publish(&id, EventKind::Started { pid, pgid, mode })?;

        Ok(CreateResult {
            terminal_id: id.0,
            // 返回 started 之前的位置，客户端 attach(cursor) 能回放 started(seq=1)。
            cursor: INITIAL_CURSOR,
        })
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
    pub fn status(&self, locator: &TerminalLocator) -> Result<TerminalSnapshot, CoreError> {
        let id = self.resolve(locator)?;
        self.by_id
            .get(&id)
            .map(TerminalEntry::snapshot)
            .ok_or(CoreError::NotFound)
    }

    /// 终止终端：落 `interrupted` 终态并调用后端；已终态时幂等返回 `Ok`。
    pub fn terminate(&mut self, locator: &TerminalLocator) -> Result<(), CoreError> {
        let id = self.resolve(locator)?;
        self.terminate_by_id(&id)
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

    /// 释放该客户端持有的全部 lease；供连接断开时兜底，避免终端永久锁死。
    ///
    /// 不校验终端状态，也不广播事件（owner 自身已离开）。
    pub fn release_all(&mut self, client_id: &str) {
        for entry in self.by_id.values_mut() {
            if entry.lease.as_deref() == Some(client_id) {
                entry.lease = None;
            }
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
    /// 终态事件（`exited` / `interrupted` / `error`）只允许一次；终端已终态时
    /// 再次写入终态事件返回 [`CoreError::AlreadyTerminal`]，不会二次发终态。
    pub fn report_event(&mut self, id: &TerminalId, kind: EventKind) -> Result<u64, CoreError> {
        let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
        if let Some(state) = terminal_state_for(&kind) {
            if entry.state.is_terminal() {
                return Err(CoreError::AlreadyTerminal);
            }
            entry.state = state;
            // 终态不再接受输入，lease 随之失效。
            entry.lease = None;
        }

        self.publish(id, kind)
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
        for event in replay {
            let _ = tx.send(event);
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
        for event in replay {
            let _ = tx.send(event);
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

    /// 注册或替换终端级订阅。
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
    fn publish(&mut self, id: &TerminalId, kind: EventKind) -> Result<u64, CoreError> {
        let (seq, event) = {
            let entry = self.by_id.get_mut(id).ok_or(CoreError::NotFound)?;
            let seq = entry.emit(kind);
            let record = entry
                .events
                .records()
                .back()
                .expect("刚写入的事件必然存在于 ring");
            (
                seq,
                Event {
                    terminal_id: id.0.clone(),
                    seq,
                    ts: record.ts.clone(),
                    kind: record.kind.clone(),
                },
            )
        };

        self.deliver_event(id, &event);
        Ok(seq)
    }

    /// 投递事件；同一客户端同时有终端级与全局订阅时只投递一次。
    ///
    /// 接收端已关闭时顺手移除失效订阅，避免长期泄漏。
    fn deliver_event(&mut self, id: &TerminalId, event: &Event) {
        let mut delivered = BTreeSet::new();
        let mut stale_terminal = Vec::new();

        if let Some(clients) = self.subscriptions.by_terminal.get(id) {
            for (client_id, tx) in clients {
                if tx.send(event.clone()).is_ok() {
                    delivered.insert(client_id.clone());
                } else {
                    stale_terminal.push(client_id.clone());
                }
            }
        }

        let mut stale_all = Vec::new();
        for (client_id, tx) in &self.subscriptions.all {
            if delivered.contains(client_id) {
                continue;
            }
            if self
                .subscriptions
                .excluded
                .get(client_id)
                .is_some_and(|excluded| excluded.contains(id))
            {
                continue;
            }
            if tx.send(event.clone()).is_err() {
                stale_all.push(client_id.clone());
            }
        }

        if let Some(clients) = self.subscriptions.by_terminal.get_mut(id) {
            for client_id in stale_terminal {
                clients.remove(&client_id);
            }
        }
        for client_id in stale_all {
            self.subscriptions.all.remove(&client_id);
        }
    }

    /// 终止指定 ID 的终端；已终态时不再重复发终态事件。
    fn terminate_by_id(&mut self, id: &TerminalId) -> Result<(), CoreError> {
        let already_terminal = self
            .by_id
            .get(id)
            .ok_or(CoreError::NotFound)?
            .state
            .is_terminal();
        if already_terminal {
            return Ok(());
        }

        if let Some(entry) = self.by_id.get_mut(id) {
            entry.state = LifecycleState::Interrupted;
            // 终态不再接受输入，lease 随之失效。
            entry.lease = None;
        }
        self.publish(
            id,
            EventKind::Interrupted {
                signal: TERMINATE_SIGNAL.to_string(),
                source: TERMINATE_SOURCE.to_string(),
            },
        )?;

        if let Some(entry) = self.by_id.get(id) {
            entry.backend.terminate();
        }
        Ok(())
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

/// 按游标从 ring 取出可回放事件；游标早于下界时返回 `too_old`。
fn replay_after(entry: &TerminalEntry, cursor: Option<u64>) -> (Option<ResetReason>, Vec<Event>) {
    let Some(cursor) = cursor else {
        return (None, Vec::new());
    };

    if let Some(oldest) = entry.events.records().front().map(|record| record.seq) {
        if cursor.saturating_add(1) < oldest {
            return (Some(ResetReason::TooOld), Vec::new());
        }
    }

    let events = entry
        .events
        .records()
        .iter()
        .filter(|record| record.seq > cursor)
        .map(|record| Event {
            terminal_id: entry.id.as_str().to_string(),
            seq: record.seq,
            ts: record.ts.clone(),
            kind: record.kind.clone(),
        })
        .collect();
    (None, events)
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
    use termx_protocol::{IdleTimeout, IdleTimeoutUnit, Stream};

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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
        let mut registry = Registry::default();
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
