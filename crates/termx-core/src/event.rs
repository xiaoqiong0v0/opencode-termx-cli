//! 每终端事件缓冲：单调 `seq` + 有界 ring buffer。
//!
//! `seq` 从 1 开始且只增不减；环形缓冲写满后丢弃最旧记录，但不影响 `seq`，
//! 便于客户端按游标检测缺口（对齐 `docs/PROTOCOL.md` §8）。

use std::collections::VecDeque;

use termx_protocol::EventKind;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// 默认事件缓冲容量。
pub const DEFAULT_EVENT_BUFFER_CAPACITY: usize = 4096;

/// 一条事件记录；`ts` 为 RFC3339 UTC 字符串。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    /// 每终端单调递增序号，从 1 开始。
    pub seq: u64,
    /// RFC3339 UTC 时间戳。
    pub ts: String,
    /// 事件类型与载荷。
    pub kind: EventKind,
}

/// 单终端事件环形缓冲。
#[derive(Debug, Clone)]
pub struct EventBuffer {
    /// 最大保留条数（至少为 1）。
    capacity: usize,
    /// 下一条事件的序号。
    next_seq: u64,
    /// 已保留的事件，按 `seq` 升序。
    records: VecDeque<EventRecord>,
}

impl EventBuffer {
    /// 以给定容量创建缓冲；容量为 0 时按 1 处理。
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_seq: 1,
            records: VecDeque::new(),
        }
    }

    /// 写入一条事件并返回其 `seq`；超出容量时丢弃最旧记录。
    pub fn emit(&mut self, kind: EventKind) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;

        if self.records.len() == self.capacity {
            self.records.pop_front();
        }
        self.records.push_back(EventRecord {
            seq,
            ts: now_rfc3339(),
            kind,
        });
        seq
    }

    /// 已保留的事件（按 `seq` 升序）。
    pub fn records(&self) -> &VecDeque<EventRecord> {
        &self.records
    }

    /// 已写入的最后一个 `seq`；从未写入时为 0。
    pub fn last_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    /// 缓冲容量。
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// 最新一条记录的时间戳。
    pub fn latest_ts(&self) -> Option<&str> {
        self.records.back().map(|record| record.ts.as_str())
    }
}

impl Default for EventBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_EVENT_BUFFER_CAPACITY)
    }
}

/// 把 UTC 时间格式化为 RFC3339 字符串。
pub fn format_rfc3339(at: OffsetDateTime) -> String {
    at.format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

/// 生成 RFC3339 UTC 时间戳。
pub fn now_rfc3339() -> String {
    format_rfc3339(OffsetDateTime::now_utc())
}

/// 解析 RFC3339 时间戳；格式非法时返回 `None`。
pub fn parse_rfc3339(ts: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(ts, &Rfc3339).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use termx_protocol::{EventKind, Stream};

    /// 构造一条无副作用的测试事件。
    fn output_event() -> EventKind {
        EventKind::Output {
            data: "aGk=".to_string(),
            stream: Stream::Stdout,
        }
    }

    #[test]
    fn seq_starts_at_one_and_increases_monotonically() {
        let mut buffer = EventBuffer::new(4);
        assert_eq!(buffer.last_seq(), 0);
        assert_eq!(buffer.emit(output_event()), 1);
        assert_eq!(buffer.emit(output_event()), 2);
        assert_eq!(buffer.emit(output_event()), 3);
        assert_eq!(buffer.last_seq(), 3);

        let seqs: Vec<u64> = buffer.records().iter().map(|record| record.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[test]
    fn ring_drops_oldest_but_seq_keeps_increasing() {
        let mut buffer = EventBuffer::new(2);
        assert_eq!(buffer.emit(output_event()), 1);
        assert_eq!(buffer.emit(output_event()), 2);
        assert_eq!(buffer.emit(output_event()), 3);

        let seqs: Vec<u64> = buffer.records().iter().map(|record| record.seq).collect();
        assert_eq!(seqs, vec![2, 3]);
        assert_eq!(buffer.capacity(), 2);
        assert_eq!(buffer.last_seq(), 3);
    }

    #[test]
    fn zero_capacity_is_clamped_to_one() {
        let buffer = EventBuffer::new(0);
        assert_eq!(buffer.capacity(), 1);
    }
}
