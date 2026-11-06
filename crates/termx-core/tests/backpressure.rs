//! M3b 背压单测：输出淘汰 `output_dropped`、状态事件不淘汰、慢订阅者限流。

use std::sync::Arc;

use termx_core::{MockFactory, Registry, TerminalId};
use termx_protocol::{Confidence, CreateParams, Event, EventKind, Mode, Stream, TerminalLocator};
use tokio::sync::{
    mpsc::{Receiver, channel},
    watch,
};

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
        root_session_id: "ses-bp".to_string(),
        name: name.to_string(),
        force: false,
        idle_timeout: None,
        ssh: None,
        allow_injection: false,
        runtime_id: None,
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

/// 写入输出并返回 seq。
fn output(registry: &mut Registry, id: &TerminalId, data: &str) -> u64 {
    registry
        .report_event(
            id,
            EventKind::Output {
                data: data.to_string(),
                stream: Stream::Stdout,
            },
        )
        .expect("输出应写入成功")
}

/// 写入状态事件并返回 seq。
fn waiting(registry: &mut Registry, id: &TerminalId) -> u64 {
    registry
        .report_event(
            id,
            EventKind::WaitingInput {
                confidence: Confidence::Low,
                reason: "no_output".to_string(),
            },
        )
        .expect("状态事件应写入成功")
}

/// 取出当前已缓冲的全部事件。
fn drain(rx: &mut Receiver<Event>) -> Vec<Event> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

#[test]
fn ring_eviction_emits_output_dropped_with_closed_interval() {
    // 容量 4：started 永不淘汰，输出按最旧优先淘汰。
    let mut registry = Registry::with_factory(4, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());
    let (tx, mut rx) = channel(16);
    registry
        .subscribe(&locator(&terminal_id), "client-a", None, tx)
        .expect("subscribe 应成功");

    output(&mut registry, &id, "YQ=="); // seq=2
    output(&mut registry, &id, "YmI="); // seq=3
    output(&mut registry, &id, "Y2Nj"); // seq=4
    output(&mut registry, &id, "ZA=="); // seq=5，淘汰 2/3 并落 marker seq=6

    let events = drain(&mut rx);
    let dropped: Vec<(u64, u64, u64)> = events
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::OutputDropped { from_seq, to_seq } => Some((event.seq, *from_seq, *to_seq)),
            _ => None,
        })
        .collect();
    assert_eq!(dropped, vec![(6, 2, 3)], "闭区间应覆盖被淘汰的全部输出");

    let entry = registry.get(&id).expect("终端应存在");
    let seqs: Vec<u64> = entry.events.records().iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![1, 4, 5, 6], "只保留 started、最新输出与 marker");
    assert!(
        entry
            .events
            .records()
            .iter()
            .any(|record| matches!(record.kind, EventKind::Started { .. })),
        "started 是状态事件，必须保留"
    );
    assert_eq!(entry.seq, 6, "marker 占用 seq，快照游标随之前进");
}

#[test]
fn state_events_are_retained_when_ring_is_full() {
    let mut registry = Registry::with_factory(2, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());

    waiting(&mut registry, &id); // seq=2，ring 满
    output(&mut registry, &id, "YQ=="); // seq=3，无可淘汰输出，允许超额
    output(&mut registry, &id, "YmI="); // seq=4，淘汰 seq=3 并落 marker

    let entry = registry.get(&id).expect("终端应存在");
    let kinds: Vec<&EventKind> = entry
        .events
        .records()
        .iter()
        .map(|record| &record.kind)
        .collect();
    assert!(
        matches!(kinds[0], EventKind::Started { .. }),
        "started 必须在: {kinds:?}"
    );
    assert!(
        matches!(kinds[1], EventKind::WaitingInput { .. }),
        "waiting_input 必须在: {kinds:?}"
    );
    assert!(
        kinds.iter().any(|kind| matches!(
            kind,
            EventKind::OutputDropped {
                from_seq: 3,
                to_seq: 4
            }
        )),
        "应保留淘汰区间 marker: {kinds:?}"
    );
}

#[test]
fn slow_subscriber_full_on_state_event_is_throttled_and_closed() {
    // 订阅队列容量 1：attached 占满，随后状态事件必然投递失败。
    let mut registry = Registry::with_config(64, 1, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());
    let (tx, mut rx) = channel(1);
    let (throttle_tx, throttle_rx) = watch::channel(false);
    registry.register_client("client-a", throttle_tx);

    registry
        .attach(&locator(&terminal_id), "client-a", None, tx)
        .expect("attach 应成功");

    // 状态事件无法投递：移除订阅者并置位限流通知，绝不静默丢弃。
    waiting(&mut registry, &id);

    assert!(*throttle_rx.borrow(), "应触发 limit 通知");
    assert!(
        registry.subscribed_clients(&id).is_empty(),
        "订阅者应被移除"
    );

    // 唯一 sender 已被 registry 释放：接收端先拿到 attached，再收到断开。
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0].kind, EventKind::Attached { .. }));
    assert!(rx.try_recv().is_err(), "通道应已关闭");
}

#[test]
fn slow_subscriber_output_overflow_also_throttles_and_closes() {
    // 队列容量 2：attached + 一条 output 占满；后续输出同样触发限流，不再
    // 在订阅者侧复用 seq 补 marker。
    let mut registry = Registry::with_config(64, 2, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());
    let (tx, mut rx) = channel(2);
    let (throttle_tx, throttle_rx) = watch::channel(false);
    registry.register_client("client-a", throttle_tx);
    registry
        .attach(&locator(&terminal_id), "client-a", None, tx)
        .expect("attach 应成功");

    output(&mut registry, &id, "YQ=="); // seq=3，投递成功
    output(&mut registry, &id, "YmI="); // seq=4，队满 → 移除订阅者并限流

    assert!(*throttle_rx.borrow(), "输出队满也应触发 limit 通知");
    assert!(
        registry.subscribed_clients(&id).is_empty(),
        "队满后订阅者应被移除"
    );
    let drained = drain(&mut rx);
    assert_eq!(drained.len(), 2, "只能收到 attached 与 seq=3");
    assert_eq!(drained[1].seq, 3, "可见 seq 单调且不回退");
    assert!(rx.try_recv().is_err(), "通道应已关闭");
}

#[test]
fn interleaved_state_event_splits_dropped_ranges_into_multiple_markers() {
    // 容量 3：started / output / waiting_input / output / output。
    let mut registry = Registry::with_factory(3, Arc::new(MockFactory));
    let terminal_id = create(&mut registry);
    let id = TerminalId::new(terminal_id.clone());
    let (tx, mut rx) = channel(16);
    registry
        .subscribe(&locator(&terminal_id), "client-a", None, tx)
        .expect("subscribe 应成功");

    output(&mut registry, &id, "YQ=="); // seq=2
    waiting(&mut registry, &id); // seq=3
    // seq=4；其 emit 内补 marker 时要腾位置，ring 里只有它可淘汰，于是 4 被丢。
    output(&mut registry, &id, "YmI=");
    // marker 占用 seq=5/6（分别覆盖 2 与 4），本条输出最终落到 seq=7 并存活。
    output(&mut registry, &id, "Y2Nj");

    let events = drain(&mut rx);
    let dropped: Vec<(u64, u64, u64)> = events
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::OutputDropped { from_seq, to_seq } => Some((event.seq, *from_seq, *to_seq)),
            _ => None,
        })
        .collect();
    assert_eq!(dropped.len(), 2, "被状态事件隔开的输出应拆成两条 marker");
    assert!(dropped[0].0 < dropped[1].0, "marker 自身 seq 必须递增");
    // 区间只含实际被丢的 output：2 与 4 各一条；存活的最新输出不在区间内。
    assert_eq!((dropped[0].1, dropped[0].2), (2, 2));
    assert_eq!((dropped[1].1, dropped[1].2), (4, 4));
    let entry = registry.get(&id).expect("终端应存在");
    assert!(
        entry.events.records().iter().any(|record| matches!(
            record.kind,
            EventKind::Output { ref data, .. } if data == "Y2Nj"
        )),
        "最新输出应仍在 ring 中"
    );

    let entry = registry.get(&id).expect("终端应存在");
    assert!(
        entry
            .events
            .records()
            .iter()
            .any(|record| matches!(record.kind, EventKind::WaitingInput { .. })),
        "状态事件必须保留"
    );
}
