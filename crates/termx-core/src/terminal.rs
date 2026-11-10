//! 终端标识与运行时条目。

use std::{collections::BTreeSet, fmt, sync::Arc, time::Instant};

use termx_protocol::{
    Capabilities, Confidence, EventKind, IdleTimeout, InteractionState, LifecycleState, Mode,
    TerminalSnapshot,
};
use time::OffsetDateTime;
use ulid::Ulid;

use crate::{EventBuffer, EventRecord, InteractionAnalyzer, TerminalBackend, TranscriptBuffer};

/// 终端内部全局唯一标识；使用 26 字符 ULID 表示。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TerminalId(pub(crate) String);

impl TerminalId {
    /// 用已有字符串构造 ID（用于按 `terminal_id` 查找）。
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    /// 生成新的 26 字符 ULID。
    pub fn generate() -> Self {
        Self(Ulid::generate().to_string())
    }

    /// 以字符串形式访问。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TerminalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for TerminalId {
    fn from(raw: String) -> Self {
        Self(raw)
    }
}

/// 已发出、尚未随进程退出结算的中断意图。
///
/// `signal` 是协议信号名（如 `SIGINT`），`source` 是协议来源
/// （`control-c` / `terminate`）；进程退出时由 registry 转成
/// [`termx_protocol::EventKind::Interrupted`]。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingInterrupt {
    /// 信号名。
    pub signal: String,
    /// 事件来源。
    pub source: String,
}

/// 单个终端的运行时条目。
///
/// F2 由 [`crate::Registry`] 持有；`seq` 与 `events.last_seq()` 保持同步。
/// `last_activity` 只随会重置空闲计时的活动事件刷新（见
/// [`TerminalEntry::emit_with_drops`] 与 [`TerminalEntry::touch`]）。
pub struct TerminalEntry {
    /// 全局唯一 ID。
    pub id: TerminalId,
    /// 所属 opencode 会话 ID。
    pub root_session_id: String,
    /// 会话内名称。
    pub name: String,
    /// 创建模式。
    pub mode: Mode,
    /// 是否为 SSH 终端（远端 shell，不做本地 marker 包装）。
    pub ssh: bool,
    /// 是否使用带外 shell 事件通道（本地通道或远端 helper 通道）。
    ///
    /// 为 `true` 时 transcript 的 `C`/`D` 由宿主事件建立；`terminal.send`
    /// 是否预记录命令由 [`crate::Registry::records_transcript_on_send`] 决定
    /// （`termx-bash`/`termx-pwsh` 需要预记录，避免首段输出先于 `C` 被丢弃）。
    pub shell_events: bool,
    /// 创建时指定的 runtime ID；旧 cmd/args 终端为 `None`。
    pub runtime_id: Option<String>,
    /// 启动程序（`CreateParams.cmd` 原值）。
    pub command: String,
    /// 启动参数（`CreateParams.args` 原值）。
    pub args: Vec<String>,
    /// 启动工作目录（`CreateParams.cwd` 原值）。
    pub cwd: Option<String>,
    /// 生命周期状态。
    pub state: LifecycleState,
    /// 交互状态。
    pub interaction: InteractionState,
    /// 交互推断置信度。
    pub confidence: Confidence,
    /// 主进程 ID；真实后端在创建时即有值。
    pub pid: Option<u32>,
    /// 进程组 ID。
    pub pgid: Option<u32>,
    /// 父进程 ID。
    pub ppid: Option<u32>,
    /// 前台进程组 ID。
    pub foreground_pgrp: Option<u32>,
    /// 列数。
    pub cols: u16,
    /// 行数。
    pub rows: u16,
    /// 创建时间（RFC3339 UTC）。
    pub created_at: String,
    /// 最后一次事件时间（RFC3339 UTC）。
    pub last_activity: Option<String>,
    /// 最新事件序号。
    pub seq: u64,
    /// 事件环形缓冲。
    pub events: EventBuffer,
    /// 空闲超时配置。
    pub idle_timeout: Option<IdleTimeout>,
    /// 输入 lease 的持有者（连接级 `client_id`）；`None` 表示空闲。
    pub lease: Option<String>,
    /// 当前附着该终端的连接级 `client_id` 集合。
    ///
    /// 只统计终端级 `attach`/`subscribe`；`subscribe_all` 的全局订阅不计入。
    /// 与订阅簿 `by_terminal` 同步维护，用于无客户端附着超时判定。
    pub clients: BTreeSet<String>,
    /// 最近一次客户端附着时间（RFC3339 UTC）。
    ///
    /// 仅保留为连接级诊断信息；终端保活改由所属 `root_session_id` 的
    /// 会话心跳决定（见 [`TerminalEntry::clientless_since`]）。
    pub last_client_seen: Option<String>,
    /// 无存活会话的计时起点；`None` 表示当前视为有会话存活或尚未开始计时。
    ///
    /// service 的 idle ticker 在每个周期根据会话心跳表更新该值：
    /// 会话存活时清空，不存活时首次写入当前时刻；超过
    /// `client_idle_timeout` 后由策略决定是否终止。
    pub clientless_since: Option<Instant>,
    /// 已发出但尚未在退出事件中结算的中断意图。
    pub pending_interrupt: Option<PendingInterrupt>,
    /// 每终端一份的交互识别状态机。
    pub(crate) analyzer: InteractionAnalyzer,
    /// 后端能力；SSH fallback 为 `Some(helper=false, bestEffort=true)`。
    pub capabilities: Option<Capabilities>,
    /// 信息页 transcript（命令 + 原始输出文本，有界）。
    pub transcript: TranscriptBuffer,
    /// 后端进程句柄。
    pub backend: Arc<dyn TerminalBackend>,
}

impl TerminalEntry {
    /// 写入一条事件并同步 `seq`；活动事件同时刷新 `last_activity`。
    ///
    /// 返回值是本次事件的 `seq`；若伴随输出淘汰，registry 还需调用
    /// [`TerminalEntry::emit_with_drops`] 拿到 `output_dropped` marker 并投递。
    pub fn emit(&mut self, kind: EventKind) -> u64 {
        self.emit_with_drops(kind).0.seq
    }

    /// 写入事件并立即落成待发的 `output_dropped` marker（可能多条）。
    ///
    /// 返回 `(本次事件记录, output_dropped 记录列表)`。`self.seq` 取最后一条
    /// 记录（通常是最新 marker）的 `seq`，保证快照游标始终是最后 seq。
    ///
    /// `last_activity` 只在**活动事件**（`started` / `output` / `attached`）时
    /// 刷新：交互、状态与终态事件不刷新，否则周期性 `waiting_input` 会让
    /// `idle_timeout` 永不触发（对齐 `docs/specs/PROTOCOL.md` §6）。
    pub fn emit_with_drops(&mut self, kind: EventKind) -> (EventRecord, Vec<EventRecord>) {
        let refreshes_activity = refreshes_activity(&kind);
        let record = self.events.emit(kind);
        let markers = self.events.take_drop_markers();
        self.seq = markers.last().map_or(record.seq, |marker| marker.seq);
        if refreshes_activity {
            self.last_activity = Some(record.ts.clone());
        }
        (record, markers)
    }

    /// 把最后活动时间刷新为 `at`（RFC3339 UTC）。
    ///
    /// `send` / `resize` 等不产生事件的输入路径用它显式续期，避免被
    /// `idle_timeout` 误判为空闲。
    pub fn touch(&mut self, at: OffsetDateTime) {
        self.last_activity = Some(crate::format_rfc3339(at));
    }

    /// 最后活动时间；缺失或格式非法时回退到创建时间。
    ///
    /// 创建时一定会写入 `started` 事件，因此正常路径总有 `last_activity`；
    /// 回退仅用于容错（例如测试手工构造的条目）。
    pub fn activity_time(&self) -> Option<OffsetDateTime> {
        self.last_activity
            .as_deref()
            .and_then(crate::parse_rfc3339)
            .or_else(|| crate::parse_rfc3339(&self.created_at))
    }

    /// 记录一个客户端附着：加入集合并刷新 `last_client_seen`。
    pub fn add_client(&mut self, client_id: &str, at: OffsetDateTime) {
        self.clients.insert(client_id.to_string());
        self.last_client_seen = Some(crate::format_rfc3339(at));
    }

    /// 移除一个客户端附着；集合变为空时记录 `at` 作为无客户端计时起点。
    pub fn remove_client(&mut self, client_id: &str, at: OffsetDateTime) {
        if self.clients.remove(client_id) && self.clients.is_empty() {
            self.last_client_seen = Some(crate::format_rfc3339(at));
        }
    }

    /// 无客户端计时起点；缺失或格式非法时回退到创建时间。
    pub fn client_seen_time(&self) -> Option<OffsetDateTime> {
        self.last_client_seen
            .as_deref()
            .and_then(crate::parse_rfc3339)
            .or_else(|| crate::parse_rfc3339(&self.created_at))
    }

    /// 生成协议快照。
    ///
    /// 基础快照不查询后端；`terminal.status` 由 registry 在锁外补入
    /// `foreground_pgrp` 与 `tree`，终态时保持最后快照。
    pub fn snapshot(&self) -> TerminalSnapshot {
        TerminalSnapshot {
            terminal_id: self.id.0.clone(),
            state: self.state,
            mode: self.mode,
            runtime_id: self.runtime_id.clone(),
            command: self.command.clone(),
            args: self.args.clone(),
            cwd: self.cwd.clone(),
            pid: self.pid,
            pgid: self.pgid,
            ppid: self.ppid,
            foreground_pgrp: self.foreground_pgrp,
            tree: Vec::new(),
            interactive: self.interaction,
            confidence: self.confidence,
            cols: self.cols,
            rows: self.rows,
            cursor: self.seq,
            structured_events: self.shell_events,
            capabilities: self.capabilities.clone(),
        }
    }
}

/// 判断事件是否刷新 `last_activity`。
///
/// 对齐 `docs/specs/PROTOCOL.md` §6：`started` 初始化，`output` 与 `attached` 刷新；
/// `send` / `resize` 由 [`TerminalEntry::touch`] 显式续期。交互、订阅状态与
/// 终态事件不刷新，避免周期性 `waiting_input` 让 `idle_timeout` 永不触发。
fn refreshes_activity(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Started { .. } | EventKind::Output { .. } | EventKind::Attached { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use termx_protocol::Stream;

    #[test]
    fn generated_terminal_ids_are_26_char_ulids_and_unique() {
        let first = TerminalId::generate();
        let second = TerminalId::generate();

        assert_eq!(first.as_str().len(), 26);
        assert_eq!(second.as_str().len(), 26);
        assert_ne!(first, second);
    }

    #[test]
    fn entry_emit_updates_seq_and_only_activity_events_refresh_last_activity() {
        let mut entry = TerminalEntry {
            id: TerminalId::generate(),
            root_session_id: "ses-1".to_string(),
            name: "build".to_string(),
            mode: Mode::Pty,
            ssh: false,
            shell_events: false,
            runtime_id: None,
            command: "mock".to_string(),
            args: Vec::new(),
            cwd: None,
            state: LifecycleState::Running,
            interaction: InteractionState::None,
            confidence: Confidence::Low,
            pid: Some(1000),
            pgid: Some(1000),
            ppid: None,
            foreground_pgrp: None,
            cols: 80,
            rows: 24,
            created_at: crate::now_rfc3339(),
            last_activity: None,
            seq: 0,
            events: EventBuffer::default(),
            idle_timeout: None,
            lease: None,
            clients: BTreeSet::new(),
            last_client_seen: None,
            clientless_since: None,
            pending_interrupt: None,
            capabilities: None,
            transcript: TranscriptBuffer::default(),
            analyzer: InteractionAnalyzer::with_start_time(
                crate::InteractionConfig::default(),
                OffsetDateTime::now_utc(),
            ),
            backend: Arc::new(crate::MockBackend::new()),
        };

        // 终态与交互事件不刷新 last_activity。
        assert_eq!(entry.emit(EventKind::Exited { exit_code: 0 }), 1);
        assert_eq!(entry.seq, 1);
        assert!(entry.last_activity.is_none(), "终态事件不应刷新活动时间");

        assert_eq!(
            entry.emit(EventKind::WaitingInput {
                confidence: Confidence::High,
                reason: "shell_integration".to_string(),
            }),
            2
        );
        assert!(
            entry.last_activity.is_none(),
            "交互事件不应刷新活动时间，否则 idle_timeout 永不触发"
        );

        // 输出事件刷新活动时间。
        assert_eq!(
            entry.emit(EventKind::Output {
                data: "aGk=".to_string(),
                stream: Stream::Pty,
            }),
            3
        );
        assert!(entry.last_activity.is_some(), "output 应刷新活动时间");

        let snapshot = entry.snapshot();
        assert_eq!(snapshot.cursor, 3);
        // 快照必须能直接序列化为协议 JSON（service 层据此回包）。
        let json = serde_json::to_value(&snapshot).expect("快照应可序列化");
        assert_eq!(json["terminal_id"], snapshot.terminal_id);
        assert_eq!(json["state"], "running");
    }
}
