//! 终端标识与运行时条目。

use std::{fmt, sync::Arc};

use termx_protocol::{
    Confidence, EventKind, IdleTimeout, InteractionState, LifecycleState, Mode, TerminalSnapshot,
};
use ulid::Ulid;

use crate::{EventBuffer, TerminalBackend};

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

/// 单个终端的运行时条目。
///
/// F2 由 [`crate::Registry`] 持有；`seq` 与 `events.last_seq()` 保持同步，
/// `last_activity` 随每次事件更新。
pub struct TerminalEntry {
    /// 全局唯一 ID。
    pub id: TerminalId,
    /// 所属 opencode 会话 ID。
    pub root_session_id: String,
    /// 会话内名称。
    pub name: String,
    /// 创建模式。
    pub mode: Mode,
    /// 生命周期状态。
    pub state: LifecycleState,
    /// 交互状态。
    pub interaction: InteractionState,
    /// 交互推断置信度。
    pub confidence: Confidence,
    /// 主进程 ID；mock 后端在创建时即有值。
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
    /// 后端进程句柄。
    pub backend: Arc<dyn TerminalBackend>,
}

impl TerminalEntry {
    /// 写入一条事件，并同步 `seq` / `last_activity`。
    pub fn emit(&mut self, kind: EventKind) -> u64 {
        let seq = self.events.emit(kind);
        self.seq = seq;
        self.last_activity = self.events.latest_ts().map(str::to_string);
        seq
    }

    /// 生成协议快照；F2 的进程树暂为空。
    pub fn snapshot(&self) -> TerminalSnapshot {
        TerminalSnapshot {
            terminal_id: self.id.0.clone(),
            state: self.state,
            mode: self.mode,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_terminal_ids_are_26_char_ulids_and_unique() {
        let first = TerminalId::generate();
        let second = TerminalId::generate();

        assert_eq!(first.as_str().len(), 26);
        assert_eq!(second.as_str().len(), 26);
        assert_ne!(first, second);
    }

    #[test]
    fn entry_emit_updates_seq_and_last_activity() {
        let mut entry = TerminalEntry {
            id: TerminalId::generate(),
            root_session_id: "ses-1".to_string(),
            name: "build".to_string(),
            mode: Mode::Pty,
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
            backend: Arc::new(crate::MockBackend::new()),
        };

        assert_eq!(entry.emit(EventKind::Exited { exit_code: 0 }), 1);
        assert_eq!(entry.seq, 1);
        assert!(entry.last_activity.is_some());

        let snapshot = entry.snapshot();
        assert_eq!(snapshot.cursor, 1);
        // 快照必须能直接序列化为协议 JSON（service 层据此回包）。
        let json = serde_json::to_value(&snapshot).expect("快照应可序列化");
        assert_eq!(json["terminal_id"], snapshot.terminal_id);
        assert_eq!(json["state"], "running");
    }
}
