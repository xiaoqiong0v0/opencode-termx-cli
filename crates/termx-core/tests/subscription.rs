//! F3 订阅与回放测试：seq 单调、attach 回放、too_old、终态唯一、detach 事件。

use std::sync::Arc;

use termx_core::{DEFAULT_EVENT_BUFFER_CAPACITY, MockFactory, Registry, TerminalId};
use termx_protocol::{
    CreateParams, Event, EventKind, LifecycleState, Mode, ResetReason, Stream, TerminalLocator,
};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

/// 构造 pty 创建参数。
fn params(root_session_id: &str, name: &str) -> CreateParams {
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
fn create_terminal(registry: &mut Registry, name: &str) -> String {
    registry
        .create(params("ses-1", name))
        .expect("create 应成功")
        .terminal_id
}

/// 构造 output 事件。
fn output(data: &str) -> EventKind {
    EventKind::Output {
        data: data.to_string(),
        stream: Stream::Stdout,
    }
}

/// 取出当前已缓冲的全部事件。
fn drain(rx: &mut UnboundedReceiver<Event>) -> Vec<Event> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

#[test]
fn report_event_increments_seq_and_delivers_to_subscriber() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let id = TerminalId::new(terminal_id.clone());
    let (tx, mut rx) = unbounded_channel();
    registry
        .attach(&locator(&terminal_id), "client-a", Some(0), tx)
        .expect("attach 应成功");
    drain(&mut rx);

    let seq = registry
        .report_event(&id, output("aGk="))
        .expect("report_event 应成功");
    assert_eq!(seq, 3, "started=1, attached=2, output=3");

    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].seq, 3);
    assert!(matches!(events[0].kind, EventKind::Output { .. }));
    assert_eq!(registry.status(&locator(&terminal_id)).unwrap().cursor, 3);
}

#[test]
fn attach_with_cursor_zero_replays_started() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let (tx, mut rx) = unbounded_channel();

    let result = registry
        .attach(&locator(&terminal_id), "client-a", Some(0), tx)
        .expect("attach 应成功");

    assert_eq!(result.cursor, 1, "返回不含 attached 的基线");
    assert_eq!(result.snapshot.cursor, 1);
    assert!(result.reset.is_none());

    let events = drain(&mut rx);
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].kind, EventKind::Started { .. }));
    assert_eq!(events[0].seq, 1);
    assert!(matches!(
        events[1].kind,
        EventKind::Attached { ref client_id } if client_id == "client-a"
    ));
    assert_eq!(events[1].seq, 2);
}

#[test]
fn attach_without_cursor_returns_snapshot_only() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let (tx, mut rx) = unbounded_channel();

    let result = registry
        .attach(&locator(&terminal_id), "client-a", None, tx)
        .expect("attach 应成功");

    assert_eq!(result.cursor, 1);
    assert_eq!(result.snapshot.state, LifecycleState::Running);
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1, "不应回放历史，只应收到 attached");
    assert!(matches!(events[0].kind, EventKind::Attached { .. }));
}

#[test]
fn attach_with_cursor_older_than_ring_returns_too_old() {
    let mut registry = Registry::with_factory(2, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let id = TerminalId::new(terminal_id.clone());
    registry.report_event(&id, output("YQ==")).unwrap();
    registry.report_event(&id, output("Yg==")).unwrap();

    let (tx, mut rx) = unbounded_channel();
    let result = registry
        .attach(&locator(&terminal_id), "client-a", Some(0), tx)
        .expect("attach 应成功");

    assert_eq!(result.reset, Some(ResetReason::TooOld));
    assert_eq!(result.cursor, 3, "基线为当前 last seq，不含 attached");
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1, "too_old 不回放历史");
    assert!(matches!(events[0].kind, EventKind::Attached { .. }));
    assert_eq!(events[0].seq, 4);
}

#[test]
fn terminal_event_is_emitted_only_once() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let id = TerminalId::new(terminal_id.clone());

    assert_eq!(
        registry.report_event(&id, EventKind::Exited { exit_code: 0 }),
        Ok(2)
    );
    // 重复终态被忽略：不报错、不写 ring，返回当前 seq。
    assert_eq!(
        registry.report_event(
            &id,
            EventKind::Interrupted {
                signal: "SIGTERM".to_string(),
                source: "signal".to_string(),
            }
        ),
        Ok(2)
    );

    let snapshot = registry.status(&locator(&terminal_id)).expect("终态可只读");
    assert_eq!(snapshot.state, LifecycleState::Exited);
    assert_eq!(snapshot.cursor, 2);

    // Interrupted 先到达时同样锁定终态，后续 Exited 不再覆盖。
    let second = create_terminal(&mut registry, "second");
    let second_id = TerminalId::new(second.clone());
    assert_eq!(
        registry.report_event(
            &second_id,
            EventKind::Interrupted {
                signal: "SIGINT".to_string(),
                source: "signal".to_string(),
            }
        ),
        Ok(2)
    );
    assert_eq!(
        registry.report_event(&second_id, EventKind::Exited { exit_code: 0 }),
        Ok(2)
    );
    let snapshot = registry.status(&locator(&second)).expect("终态可只读");
    assert_eq!(snapshot.state, LifecycleState::Interrupted);
    assert_eq!(snapshot.cursor, 2);
}

#[test]
fn output_after_terminal_is_dropped() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let id = TerminalId::new(terminal_id.clone());
    let (tx, mut rx) = unbounded_channel();
    registry
        .attach(&locator(&terminal_id), "client-a", None, tx)
        .expect("attach 应成功");
    drain(&mut rx);

    registry
        .report_event(&id, EventKind::Exited { exit_code: 0 })
        .expect("终态事件应成功");
    let terminal_seq = registry.status(&locator(&terminal_id)).unwrap().cursor;
    assert!(matches!(
        drain(&mut rx).as_slice(),
        [Event {
            kind: EventKind::Exited { .. },
            ..
        }]
    ));

    // 终态之后到达的 output 必须丢弃：不递增 seq、不写 ring、不投递。
    assert_eq!(registry.report_event(&id, output("YQ==")), Ok(terminal_seq));
    assert!(drain(&mut rx).is_empty(), "终态后不应再收到 output");
    assert_eq!(
        registry.status(&locator(&terminal_id)).unwrap().cursor,
        terminal_seq,
        "终态 seq 不应被 output 推进"
    );
}

#[test]
fn detach_removes_subscription_and_emits_detached_to_others() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let id = TerminalId::new(terminal_id.clone());

    let (tx_a, mut rx_a) = unbounded_channel();
    registry
        .attach(&locator(&terminal_id), "client-a", None, tx_a)
        .expect("attach 应成功");
    let (tx_b, mut rx_b) = unbounded_channel();
    registry
        .subscribe(&locator(&terminal_id), "client-b", None, tx_b)
        .expect("subscribe 应成功");
    drain(&mut rx_a);
    drain(&mut rx_b);

    registry
        .detach(&locator(&terminal_id), "client-a")
        .expect("detach 应成功");

    let events_b = drain(&mut rx_b);
    assert_eq!(events_b.len(), 1);
    assert!(matches!(
        events_b[0].kind,
        EventKind::Detached { ref client_id, ref reason }
            if client_id == "client-a" && reason == "detach"
    ));
    assert!(drain(&mut rx_a).is_empty(), "detach 后不应再收到自己的事件");

    registry.report_event(&id, output("YQ==")).unwrap();
    assert!(drain(&mut rx_a).is_empty(), "detach 后不应收到后续 output");
    assert_eq!(drain(&mut rx_b).len(), 1);
}

#[test]
fn subscribe_all_receives_events_from_future_terminals() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let (tx, mut rx) = unbounded_channel();
    let cursors = registry.subscribe_all("client-a", tx);
    assert!(cursors.is_empty(), "尚无终端");

    let first = create_terminal(&mut registry, "first");
    let second = create_terminal(&mut registry, "second");

    let events = drain(&mut rx);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].terminal_id, first);
    assert_eq!(events[0].seq, 1);
    assert!(matches!(events[0].kind, EventKind::Started { .. }));
    assert_eq!(events[1].terminal_id, second);
    assert_eq!(events[1].seq, 1);
}

#[test]
fn unsubscribe_does_not_emit_detached() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let id = TerminalId::new(terminal_id.clone());
    let (tx, mut rx) = unbounded_channel();
    registry
        .attach(&locator(&terminal_id), "client-a", None, tx)
        .expect("attach 应成功");
    drain(&mut rx);

    registry
        .unsubscribe(&id, "client-a")
        .expect("unsubscribe 应成功");
    assert!(drain(&mut rx).is_empty(), "unsubscribe 不 emit detached");

    registry.report_event(&id, output("YQ==")).unwrap();
    assert!(drain(&mut rx).is_empty());
}

#[test]
fn attach_with_nonzero_cursor_replays_without_gap_or_duplicate() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let id = TerminalId::new(terminal_id.clone());
    registry.report_event(&id, output("YQ==")).unwrap();
    registry.report_event(&id, output("Yg==")).unwrap();

    let (tx, mut rx) = unbounded_channel();
    let result = registry
        .attach(&locator(&terminal_id), "client-a", Some(2), tx)
        .expect("attach 应成功");

    // 基线不含本次 attached；回放只含 seq=3，不漏 seq=2、不重复。
    assert_eq!(result.cursor, 3);
    let events = drain(&mut rx);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].seq, 3);
    assert!(matches!(
        events[0].kind,
        EventKind::Output { ref data, .. } if data == "Yg=="
    ));
    assert_eq!(events[1].seq, 4);
    assert!(matches!(events[1].kind, EventKind::Attached { .. }));
}

#[test]
fn global_subscription_excludes_detached_terminal_until_resubscribe() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let first = create_terminal(&mut registry, "first");
    let second = create_terminal(&mut registry, "second");

    let (global_tx, mut global_rx) = unbounded_channel();
    let cursors = registry.subscribe_all("client-a", global_tx);
    assert_eq!(cursors.len(), 2);
    assert!(drain(&mut global_rx).is_empty());

    // detach 后该终端的全局事件被排除。
    registry
        .detach(&locator(&first), "client-a")
        .expect("detach 应成功");
    registry
        .report_event(&TerminalId::new(first.clone()), output("YQ=="))
        .unwrap();
    assert!(drain(&mut global_rx).is_empty(), "detach 的终端不应再投递");

    // 其他终端不受影响。
    registry
        .report_event(&TerminalId::new(second.clone()), output("Yg=="))
        .unwrap();
    assert_eq!(drain(&mut global_rx).len(), 1);

    // 重新 attach 后恢复接收（终端级投递优先，全局按 client_id 去重）。
    let (terminal_tx, mut terminal_rx) = unbounded_channel();
    registry
        .attach(&locator(&first), "client-a", None, terminal_tx)
        .expect("attach 应成功");
    drain(&mut terminal_rx);
    registry
        .report_event(&TerminalId::new(first), output("Yw=="))
        .unwrap();
    let events = drain(&mut terminal_rx);
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0].kind, EventKind::Output { .. }));
    assert!(drain(&mut global_rx).is_empty());
}

#[test]
fn detach_all_emits_detached_only_for_terminal_subscriptions() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let first = create_terminal(&mut registry, "first");
    let _second = create_terminal(&mut registry, "second");

    let (observer_tx, mut observer_rx) = unbounded_channel();
    registry.subscribe_all("observer", observer_tx);
    drain(&mut observer_rx);

    let (global_tx, mut global_rx) = unbounded_channel();
    registry.subscribe_all("client-a", global_tx);
    let (terminal_tx, mut terminal_rx) = unbounded_channel();
    registry
        .attach(&locator(&first), "client-a", None, terminal_tx)
        .expect("attach 应成功");
    drain(&mut observer_rx);
    drain(&mut terminal_rx);
    drain(&mut global_rx);

    registry
        .detach_all("client-a", "disconnect")
        .expect("detach_all 应成功");

    let events = drain(&mut observer_rx);
    assert_eq!(events.len(), 1, "仅应对 attach 过的终端发 detached");
    assert_eq!(events[0].terminal_id, first);
    assert!(matches!(
        events[0].kind,
        EventKind::Detached { ref client_id, ref reason }
            if client_id == "client-a" && reason == "disconnect"
    ));
    assert!(drain(&mut terminal_rx).is_empty());
    assert!(drain(&mut global_rx).is_empty());
}
