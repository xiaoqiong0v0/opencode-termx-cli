//! 每终端事件缓冲：单调 `seq` + 有界 ring buffer。
//!
//! `seq` 从 1 开始且只增不减。ring 满时只淘汰输出类记录；被淘汰的
//! `output` 序号按**连续区间**记账，随后各落成一条新的 `output_dropped`
//! 事件（各自占用新的单调 `seq`）。状态、终态与交互事件永不淘汰：容量只
//! 约束可淘汰的输出类记录，状态事件持续写入时 ring 会按 FIFO 持续增长
//! （不设额外上限），直到终端被删除。对齐 `docs/PROTOCOL.md` §7/§8/§9。

use std::collections::VecDeque;

use termx_protocol::EventKind;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// 默认事件缓冲容量（输出类记录的预算）。
pub const DEFAULT_EVENT_BUFFER_CAPACITY: usize = 4096;

/// 被淘汰输出序号的闭区间。
type DropRange = (u64, u64);

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
    /// 最大保留条数（输出类记录的预算，至少为 1）。
    capacity: usize,
    /// 下一条事件的序号。
    next_seq: u64,
    /// 已保留的事件，按 `seq` 升序。
    records: VecDeque<EventRecord>,
    /// 已从 ring 淘汰、尚未落成 `output_dropped` 的输出序号闭区间。
    ///
    /// 区间互不重叠、按 `from` 升序；被状态事件隔开的输出会拆成多个区间，
    /// 保证每条 marker 只覆盖实际被丢弃的输出 `seq`。
    pending_drops: Vec<DropRange>,
    /// 曾被丢弃的最大输出 `seq`（高水位），一旦推进不再回退。
    ///
    /// marker 自身也可能被后续淘汰，届时 ring 里已没有 `output_dropped`
    /// 记录；仅靠扫描 marker 会得到 `lost_to=0`，把已丢区间误判为连续可读。
    /// 高水位独立于 ring 保留，供 read/attach 判定游标是否需要 reset。
    dropped_through: u64,
}

impl EventBuffer {
    /// 以给定容量创建缓冲；容量为 0 时按 1 处理。
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_seq: 1,
            records: VecDeque::new(),
            pending_drops: Vec::new(),
            dropped_through: 0,
        }
    }

    /// 写入一条事件并返回其记录；超出容量时只淘汰最旧的输出类记录。
    ///
    /// 被淘汰的输出序号会累积在 [`EventBuffer::take_drop_markers`] 中，由调用方
    /// （registry）紧接着落成 `output_dropped` 事件，避免在 `emit` 内递归发布。
    pub fn emit(&mut self, kind: EventKind) -> EventRecord {
        let seq = self.next_seq;
        self.next_seq += 1;

        self.make_room();
        let record = EventRecord {
            seq,
            ts: now_rfc3339(),
            kind,
        };
        self.records.push_back(record.clone());
        record
    }

    /// 把所有待发丢弃区间各落成一条 `output_dropped` 记录并返回。
    ///
    /// - 每条 marker 分配新的单调 `seq`，不复用被丢输出的 `seq`。
    /// - **先腾位置再取区间**：为 marker 腾位置而新淘汰的输出会并入待发区间，
    ///   连续/重叠者由 [`add_drop`] 合并，因此一条 marker 覆盖的是实际被丢的
    ///   完整闭区间，不会因 marker 自身挤占容量而产生级联的碎片 marker。
    /// - 被状态事件隔开的区间保持独立，按 `from_seq` 升序各发一条 marker。
    /// - marker 自身被淘汰时不再重新记账：它已作为事件投递过，ring 只保留
    ///   最近的缺口说明，避免小容量下 marker 互相淘汰造成无限循环。
    pub fn take_drop_markers(&mut self) -> Vec<EventRecord> {
        let mut markers = Vec::new();
        while !self.pending_drops.is_empty() {
            // 先为 marker 腾出位置；此步骤可能淘汰更多输出并合并进待发区间。
            self.make_room();
            let (from_seq, to_seq) = self.pending_drops.remove(0);

            let seq = self.next_seq;
            self.next_seq += 1;
            let record = EventRecord {
                seq,
                ts: now_rfc3339(),
                kind: EventKind::OutputDropped { from_seq, to_seq },
            };
            self.records.push_back(record.clone());
            markers.push(record);
        }
        markers
    }

    /// 已保留的事件（按 `seq` 升序）。
    pub fn records(&self) -> &VecDeque<EventRecord> {
        &self.records
    }

    /// 保留记录的最小 `seq`；缓冲为空时返回 `None`。
    pub fn oldest_seq(&self) -> Option<u64> {
        self.records.front().map(|record| record.seq)
    }

    /// 尚未落成 `output_dropped` 的待发区间（闭区间，按 `from` 升序）。
    pub fn pending_drops(&self) -> &[DropRange] {
        &self.pending_drops
    }

    /// 已被淘汰、尚未落成 marker 的最大输出序号。
    pub fn pending_drop_through(&self) -> Option<u64> {
        self.pending_drops.last().map(|(_, to)| *to)
    }

    /// 曾被丢弃的最大输出序号（高水位）；从未丢弃输出时为 0。
    ///
    /// 与 `pending_drops`/ring 内的 `output_dropped` 不同，该值不会因 marker
    /// 被淘汰而回退，read/attach 用它识别「已丢区间后续被当成连续可读」。
    pub fn dropped_through(&self) -> u64 {
        self.dropped_through
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

    /// 为写入新记录腾位置：只淘汰最旧的输出类记录。
    fn make_room(&mut self) {
        while self.records.len() >= self.capacity {
            let Some(index) = self.records.iter().position(is_evictable) else {
                // 状态事件永不淘汰；溢出只可能来自状态事件本身。
                break;
            };
            let evicted = self.records.remove(index).expect("刚定位到的记录必然存在");
            if let EventKind::Output { .. } = evicted.kind {
                add_drop(&mut self.pending_drops, evicted.seq, evicted.seq);
                // 高水位只推进不回退：marker 被淘汰后仍能识别已丢区间。
                self.dropped_through = self.dropped_through.max(evicted.seq);
            }
            // `output_dropped` 被淘汰时不再重新记账，避免小容量下无限补 marker。
        }
    }
}

impl Default for EventBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_EVENT_BUFFER_CAPACITY)
    }
}

/// 输出类记录可被背压淘汰；状态/终态/交互事件不可淘汰。
fn is_evictable(record: &EventRecord) -> bool {
    matches!(
        &record.kind,
        EventKind::Output { .. } | EventKind::OutputDropped { .. }
    )
}

/// 把一个闭区间插入待发列表；仅合并重叠或相邻（连续 `seq`）的区间。
fn add_drop(ranges: &mut Vec<DropRange>, from: u64, to: u64) {
    ranges.push((from, to));
    ranges.sort_unstable();
    let mut merged: Vec<DropRange> = Vec::with_capacity(ranges.len());
    for (current_from, current_to) in ranges.drain(..) {
        match merged.last_mut() {
            Some((_, last_to)) if current_from <= last_to.saturating_add(1) => {
                *last_to = (*last_to).max(current_to);
            }
            _ => merged.push((current_from, current_to)),
        }
    }
    *ranges = merged;
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

    /// 构造一条状态类测试事件。
    fn waiting_event() -> EventKind {
        EventKind::WaitingInput {
            confidence: termx_protocol::Confidence::Low,
            reason: "no_output".to_string(),
        }
    }

    #[test]
    fn seq_starts_at_one_and_increases_monotonically() {
        let mut buffer = EventBuffer::new(4);
        assert_eq!(buffer.last_seq(), 0);
        assert_eq!(buffer.emit(output_event()).seq, 1);
        assert_eq!(buffer.emit(output_event()).seq, 2);
        assert_eq!(buffer.emit(output_event()).seq, 3);
        assert_eq!(buffer.last_seq(), 3);

        let seqs: Vec<u64> = buffer.records().iter().map(|record| record.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[test]
    fn ring_drops_oldest_output_but_seq_keeps_increasing() {
        let mut buffer = EventBuffer::new(2);
        assert_eq!(buffer.emit(output_event()).seq, 1);
        assert_eq!(buffer.emit(output_event()).seq, 2);
        assert_eq!(buffer.emit(output_event()).seq, 3);

        // 淘汰 seq=1 后立刻落 marker（占用 seq=4）；为给 marker 腾位置，
        // seq=2 也并入同一闭区间，最终只保留最新输出 seq=3。
        let markers = buffer.take_drop_markers();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].seq, 4);
        assert!(matches!(
            markers[0].kind,
            EventKind::OutputDropped {
                from_seq: 1,
                to_seq: 2
            }
        ));
        let seqs: Vec<u64> = buffer.records().iter().map(|record| record.seq).collect();
        assert_eq!(seqs, vec![3, 4]);
        assert_eq!(buffer.capacity(), 2);
        assert_eq!(buffer.last_seq(), 4);
    }

    #[test]
    fn interleaved_state_events_split_dropped_ranges() {
        let mut buffer = EventBuffer::new(3);
        assert_eq!(buffer.emit(output_event()).seq, 1);
        assert_eq!(buffer.emit(waiting_event()).seq, 2);
        assert_eq!(buffer.emit(output_event()).seq, 3);
        assert_eq!(buffer.emit(output_event()).seq, 4);

        // 被淘汰的是 output 1 与 output 3/4，状态事件 2 保留，因此必须拆成
        // 两条 marker，且各自使用新的单调 seq。
        let markers = buffer.take_drop_markers();
        let ranges: Vec<(u64, u64, u64)> = markers
            .iter()
            .map(|record| match record.kind {
                EventKind::OutputDropped { from_seq, to_seq } => (record.seq, from_seq, to_seq),
                _ => panic!("marker 类型错误"),
            })
            .collect();
        assert_eq!(ranges.len(), 2);
        assert!(ranges.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert_eq!(ranges[0].1, 1);
        assert_eq!(ranges[1].1, 3);
        assert!(buffer.records().iter().any(|record| matches!(
            record.kind,
            EventKind::WaitingInput { .. } | EventKind::OutputDropped { .. }
        )));
    }

    #[test]
    fn state_events_are_never_evicted() {
        let mut buffer = EventBuffer::new(2);
        assert_eq!(buffer.emit(waiting_event()).seq, 1);
        assert_eq!(buffer.emit(waiting_event()).seq, 2);
        // 满且全是状态事件：状态事件永不淘汰，ring 继续增长（无额外上限）。
        assert_eq!(buffer.emit(waiting_event()).seq, 3);
        assert_eq!(buffer.records().len(), 3);
        assert!(
            buffer
                .records()
                .iter()
                .all(|record| matches!(record.kind, EventKind::WaitingInput { .. }))
        );
    }

    #[test]
    fn zero_capacity_is_clamped_to_one() {
        let buffer = EventBuffer::new(0);
        assert_eq!(buffer.capacity(), 1);
    }
}
