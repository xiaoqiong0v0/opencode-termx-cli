//! M2c2 输入与终止路径单测：send 的 lease 门禁、resize/signal 终态拒绝、
//! terminate 幂等与终态事件唯一。

use std::sync::Arc;

use termx_core::{
    CoreError, DEFAULT_EVENT_BUFFER_CAPACITY, ExitOutcome, MockBackend, MockFactory, Registry,
    TerminalId,
};
use termx_protocol::{CreateParams, Event, EventKind, LifecycleState, Mode, TerminalLocator};
use tokio::sync::mpsc::{Receiver, channel};

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
        ssh: None,
        allow_injection: false,
        runtime_id: None,
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

/// 取出当前已缓冲的全部事件。
fn drain(rx: &mut Receiver<Event>) -> Vec<Event> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

#[test]
fn send_requires_lease_owner_and_rejects_terminal() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let backend = Arc::new(MockBackend::new());
    let created = registry
        .create_with_backend(
            params("ses-1", "build"),
            Arc::clone(&backend) as Arc<dyn termx_core::TerminalBackend>,
        )
        .expect("create 应成功");
    let locator = by_id(&created.terminal_id);

    // 未 claim → permission_denied，后端未收到写入。
    assert_eq!(
        registry.send(&locator, "client-a", b"x"),
        Err(CoreError::PermissionDenied)
    );
    assert!(backend.writes().is_empty());

    registry
        .claim(&locator, "client-a", false)
        .expect("claim 应成功");

    // 非 owner → permission_denied。
    assert_eq!(
        registry.send(&locator, "client-b", b"y"),
        Err(CoreError::PermissionDenied)
    );
    // owner → 写入后端。
    registry
        .send(&locator, "client-a", b"hi")
        .expect("owner 应可写入");
    assert_eq!(backend.writes(), vec![b"hi".to_vec()]);

    // 终态后写操作一律 already_terminal。
    registry.terminate(&locator).expect("terminate 应成功");
    for result in [
        registry.send(&locator, "client-a", b"z"),
        registry.resize(&locator, 100, 40),
        registry.signal(&locator, "SIGINT"),
    ] {
        assert_eq!(result, Err(CoreError::AlreadyTerminal));
    }
    assert_eq!(
        registry.status(&locator).expect("终态可读").state,
        LifecycleState::Interrupted
    );
}

#[test]
fn terminate_is_idempotent_and_emits_single_terminal_event() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let backend = Arc::new(MockBackend::new());
    let created = registry
        .create_with_backend(
            params("ses-1", "build"),
            Arc::clone(&backend) as Arc<dyn termx_core::TerminalBackend>,
        )
        .expect("create 应成功");
    let locator = by_id(&created.terminal_id);

    let (tx, mut rx) = channel(64);
    registry
        .attach(&locator, "client-a", Some(0), tx)
        .expect("attach 应成功");
    drain(&mut rx);

    registry.terminate(&locator).expect("首次 terminate 应成功");
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1, "首次 terminate 只发一条终态事件");
    assert!(matches!(
        events[0].kind,
        EventKind::Interrupted { ref signal, ref source }
            if signal == "SIGTERM" && source == "terminate"
    ));
    assert_eq!(backend.terminate_calls(), 1, "后端只应终止一次");
    let cursor = registry.status(&locator).expect("终态可读").cursor;

    registry.terminate(&locator).expect("重复 terminate 应幂等");
    assert!(drain(&mut rx).is_empty(), "重复 terminate 不应再发事件");
    assert_eq!(backend.terminate_calls(), 1, "后端不应被重复终止");
    assert_eq!(
        registry.status(&locator).expect("终态可读").cursor,
        cursor,
        "终态 seq 不应变化"
    );
}

#[test]
fn resize_and_signal_reach_backend_and_update_snapshot() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let backend = Arc::new(MockBackend::new());
    let created = registry
        .create_with_backend(
            params("ses-1", "build"),
            Arc::clone(&backend) as Arc<dyn termx_core::TerminalBackend>,
        )
        .expect("create 应成功");
    let locator = by_id(&created.terminal_id);

    registry.resize(&locator, 120, 40).expect("resize 应成功");
    assert_eq!(backend.resizes(), vec![(120, 40)]);
    let snapshot = registry.status(&locator).expect("运行中可读");
    assert_eq!((snapshot.cols, snapshot.rows), (120, 40));

    // 所有受支持信号都交给后端 decision（由后端决定 killpg / Job / ConPTY 写法），
    // registry 不再把 SIGINT 改写成 Ctrl-C 字节。
    registry.signal(&locator, "SIGINT").expect("signal 应成功");
    registry.signal(&locator, "SIGTERM").expect("signal 应成功");
    assert_eq!(
        backend.signals(),
        vec!["SIGINT".to_string(), "SIGTERM".to_string()]
    );
    assert!(backend.writes().is_empty(), "信号不应被改写成输入字节");
}

#[test]
fn sigint_sets_pending_and_next_exit_becomes_interrupted() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let backend = Arc::new(MockBackend::new());
    let created = registry
        .create_with_backend(
            params("ses-1", "build"),
            Arc::clone(&backend) as Arc<dyn termx_core::TerminalBackend>,
        )
        .expect("create 应成功");
    let locator = by_id(&created.terminal_id);
    let id = TerminalId::new(created.terminal_id.clone());

    let (tx, mut rx) = channel(64);
    registry
        .attach(&locator, "client-a", None, tx)
        .expect("attach 应成功");
    drain(&mut rx);

    registry.signal(&locator, "SIGINT").expect("Ctrl-C 应成功");
    assert_eq!(backend.signals(), vec!["SIGINT".to_string()]);
    assert_eq!(
        registry
            .get(&id)
            .expect("终端应存在")
            .pending_interrupt
            .as_ref()
            .map(|pending| (pending.signal.as_str(), pending.source.as_str())),
        Some(("SIGINT", "control-c"))
    );

    // 进程随后正常退出，但已有 pending → 发 interrupted 而非 exited。
    registry
        .report_exit(&id, ExitOutcome::Code(0))
        .expect("退出结算应成功");
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0].kind,
        EventKind::Interrupted { ref signal, ref source }
            if signal == "SIGINT" && source == "control-c"
    ));
    assert_eq!(
        registry.status(&locator).expect("终态可读").state,
        LifecycleState::Interrupted
    );
}

#[test]
fn signal_exit_without_pending_maps_to_interrupted_signal_source() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let created = registry
        .create(params("ses-1", "build"))
        .expect("create 应成功");
    let id = TerminalId::new(created.terminal_id.clone());

    registry
        .report_exit(&id, ExitOutcome::Signal(15))
        .expect("退出结算应成功");
    let snapshot = registry
        .status(&by_id(&created.terminal_id))
        .expect("终态可读");
    assert_eq!(snapshot.state, LifecycleState::Interrupted);
    let record = registry
        .get(&id)
        .expect("终端应存在")
        .events
        .records()
        .back()
        .expect("应有终态事件");
    assert!(matches!(
        record.kind,
        EventKind::Interrupted { ref signal, ref source }
            if signal == "SIGTERM" && source == "signal"
    ));
}

/// 复核项：后端 `signal` 失败时不得记录 `pending_interrupt`，避免把随后的
/// 正常退出误报为 `interrupted`。
#[test]
fn failed_signal_does_not_record_pending_interrupt() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let backend = Arc::new(MockBackend::new());
    let created = registry
        .create_with_backend(
            params("ses-1", "build"),
            Arc::clone(&backend) as Arc<dyn termx_core::TerminalBackend>,
        )
        .expect("create 应成功");
    let locator = by_id(&created.terminal_id);
    let id = TerminalId::new(created.terminal_id.clone());

    let error = registry
        .signal(&locator, "SIGHUP")
        .expect_err("未知信号应失败");
    assert_eq!(error, CoreError::UnsupportedSignal("SIGHUP".to_string()));
    assert!(backend.signals().is_empty(), "失败时后端不应记录信号");
    assert!(
        registry
            .get(&id)
            .expect("终端应存在")
            .pending_interrupt
            .is_none(),
        "失败不得记录 pending_interrupt"
    );

    // 随后正常退出仍落 exited，而不是 interrupted。
    registry
        .report_exit(&id, ExitOutcome::Code(0))
        .expect("退出结算应成功");
    let record = registry
        .get(&id)
        .expect("终端应存在")
        .events
        .records()
        .back()
        .expect("应有终态事件");
    assert!(matches!(record.kind, EventKind::Exited { exit_code: 0 }));
}
