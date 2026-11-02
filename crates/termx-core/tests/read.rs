//! M3b `terminal.read` 单测：chunks/cursor、增量读取、ring 回绕 reset、max_bytes。

use std::sync::Arc;

use termx_core::{DEFAULT_EVENT_BUFFER_CAPACITY, MockFactory, Registry, TerminalId};
use termx_protocol::{CreateParams, EventKind, Mode, ResetReason, Stream, TerminalLocator};

/// 构造 pty 创建参数。
fn params(name: &str) -> CreateParams {
    CreateParams {
        mode: Mode::Pty,
        cmd: "mock".to_string(),
        args: Vec::new(),
        cwd: None,
        env: None,
        cols: None,
        rows: None,
        root_session_id: "ses-read".to_string(),
        name: name.to_string(),
        force: false,
        idle_timeout: None,
    }
}

/// 按 `terminal_id` 构造定位器。
fn locator(terminal_id: &str) -> TerminalLocator {
    TerminalLocator {
        terminal_id: Some(terminal_id.to_string()),
        root_session_id: None,
        name: None,
    }
}

/// 创建终端并返回 ID。
fn create(registry: &mut Registry) -> String {
    registry
        .create(params("build"))
        .expect("create 应成功")
        .terminal_id
}

/// 写入一条输出并按 base64 原文断言。
fn emit_output(registry: &mut Registry, id: &TerminalId, data: &str) -> u64 {
    registry
        .report_event(
            id,
            EventKind::Output {
                data: data.to_string(),
                stream: Stream::Stdout,
            },
        )
        .expect("report_event 应成功")
}

#[test]
fn read_returns_output_chunks_and_scans_state_events_in_cursor() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());

    // started=1，三个输出 seq=2/3/4。
    assert_eq!(emit_output(&mut registry, &id, "YQ=="), 2);
    assert_eq!(emit_output(&mut registry, &id, "YmI="), 3);
    assert_eq!(emit_output(&mut registry, &id, "Y2Nj"), 4);

    let result = registry
        .read(&locator(&terminal_id), 0, None)
        .expect("read 应成功");
    assert!(result.reset.is_none());
    let seqs: Vec<u64> = result.chunks.iter().map(|chunk| chunk.seq).collect();
    assert_eq!(seqs, vec![2, 3, 4]);
    assert_eq!(result.chunks[0].data, "YQ==");
    // 游标越过非 Output 的 started(seq=1) 与所有输出。
    assert_eq!(result.cursor, 4);

    // 再次 read 是增量的：从上次 cursor 继续，无重复。
    let next = registry
        .read(&locator(&terminal_id), result.cursor, None)
        .expect("增量 read 应成功");
    assert!(next.chunks.is_empty());
    assert_eq!(next.cursor, result.cursor);
    assert!(next.reset.is_none());
}

#[test]
fn read_after_cursor_returns_only_newer_chunks() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());
    emit_output(&mut registry, &id, "YQ==");
    emit_output(&mut registry, &id, "YmI=");
    emit_output(&mut registry, &id, "Y2Nj");

    let result = registry
        .read(&locator(&terminal_id), 2, None)
        .expect("read 应成功");
    let seqs: Vec<u64> = result.chunks.iter().map(|chunk| chunk.seq).collect();
    assert_eq!(seqs, vec![3, 4]);
    assert_eq!(result.cursor, 4);
}

#[test]
fn read_before_ring_lower_bound_returns_too_old_and_baseline() {
    // 缓冲很小：started 保留，输出被淘汰并落 output_dropped marker。
    let mut registry = Registry::with_factory(2, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());
    emit_output(&mut registry, &id, "YQ==");
    let last = emit_output(&mut registry, &id, "YmI=");

    let result = registry
        .read(&locator(&terminal_id), 0, None)
        .expect("read 应成功");
    assert_eq!(result.reset, Some(ResetReason::TooOld));
    assert!(result.chunks.is_empty());
    // 基线是当前最后 seq（包含 output_dropped marker），不是被淘汰的输出 seq。
    assert!(result.cursor > last);
    assert_eq!(
        result.cursor,
        registry.get(&id).expect("终端应存在").seq,
        "reset 时返回基线游标"
    );
    assert_eq!(
        registry.read(&locator(&terminal_id), result.cursor, None),
        Ok(termx_protocol::ReadResult {
            chunks: Vec::new(),
            cursor: result.cursor,
            reset: None,
        })
    );
}

#[test]
fn read_and_attach_reset_after_dropped_markers_are_evicted() {
    // 容量 2：连续写入会让 output_dropped marker 自身也被淘汰，
    // 高水位必须继续生效，不能把已丢区间当成连续可读。
    let mut registry = Registry::with_factory(2, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());
    for _ in 0..5 {
        emit_output(&mut registry, &id, "YQ==");
    }

    let entry = registry.get(&id).expect("终端应存在");
    assert!(
        !entry
            .events
            .records()
            .iter()
            .any(|record| matches!(record.kind, EventKind::OutputDropped { .. })),
        "该窗口内 marker 应已被淘汰: {:?}",
        entry
            .events
            .records()
            .iter()
            .map(|record| record.seq)
            .collect::<Vec<_>>()
    );
    let dropped_through = entry.events.dropped_through();
    assert!(dropped_through >= 6, "高水位应记住已丢弃输出");

    // 游标 0 落在已丢区间：read 必须 reset，而不是静默跳过输出。
    let result = registry
        .read(&locator(&terminal_id), 0, None)
        .expect("read 应成功");
    assert_eq!(result.reset, Some(ResetReason::TooOld));
    assert!(result.chunks.is_empty());
    assert_eq!(result.cursor, registry.get(&id).expect("终端应存在").seq);

    // 高水位之后的游标仍可增量读取。
    let fresh = registry
        .read(&locator(&terminal_id), dropped_through, None)
        .expect("read 应成功");
    assert!(fresh.reset.is_none());
    assert!(!fresh.chunks.is_empty(), "高水位之后仍有可读输出");

    // attach 走同一判定：cursor=0 必须返回 reset: too_old。
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let attached = registry
        .attach(&locator(&terminal_id), "client-a", Some(0), tx)
        .expect("attach 应成功");
    assert_eq!(attached.reset, Some(ResetReason::TooOld));
}

#[test]
fn read_max_bytes_truncates_at_chunk_boundary_and_resumes() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());
    // 解码后分别为 1 / 2 / 3 字节。
    emit_output(&mut registry, &id, "YQ==");
    emit_output(&mut registry, &id, "YmI=");
    emit_output(&mut registry, &id, "Y2Nj");

    let first = registry
        .read(&locator(&terminal_id), 0, Some(3))
        .expect("read 应成功");
    let seqs: Vec<u64> = first.chunks.iter().map(|chunk| chunk.seq).collect();
    assert_eq!(seqs, vec![2, 3], "1+2 字节，3 字节的 seq=4 不应返回");
    assert_eq!(first.cursor, 3);

    let second = registry
        .read(&locator(&terminal_id), first.cursor, Some(3))
        .expect("续读应成功");
    let seqs: Vec<u64> = second.chunks.iter().map(|chunk| chunk.seq).collect();
    assert_eq!(seqs, vec![4]);
    assert_eq!(second.cursor, 4);

    // 0 与 None 都回退默认上限（此处单条 3 字节，可全量返回）。
    let all = registry
        .read(&locator(&terminal_id), 0, Some(0))
        .expect("read 应成功");
    assert_eq!(all.chunks.len(), 3);
}

#[test]
fn read_returns_single_oversized_output_and_advances_cursor() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());
    // "YWFhYQ==" 解码后为 4 字节；max_bytes=1 时仍应返回并前进游标。
    let seq = emit_output(&mut registry, &id, "YWFhYQ==");

    let result = registry
        .read(&locator(&terminal_id), 0, Some(1))
        .expect("read 应成功");
    assert_eq!(result.chunks.len(), 1, "超长单条不能被跳过");
    assert_eq!(result.chunks[0].seq, seq);
    assert_eq!(result.cursor, seq, "游标必须前进，避免活锁");
    assert!(result.reset.is_none());
}
