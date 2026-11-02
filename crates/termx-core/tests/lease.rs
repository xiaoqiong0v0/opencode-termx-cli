//! F4 定位与输入 lease 测试：resolve、claim/release、ensure_input_allowed 与断连清理。

use std::sync::Arc;

use termx_core::{CoreError, DEFAULT_EVENT_BUFFER_CAPACITY, MockFactory, Registry, TerminalId};
use termx_protocol::{CreateParams, Event, EventKind, Mode, TerminalLocator};
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

/// 创建终端并返回其 ID。
fn create_terminal(registry: &mut Registry, name: &str) -> String {
    registry
        .create(params("ses-1", name))
        .expect("create 应成功")
        .terminal_id
}

/// 按 `terminal_id` 构造定位器。
fn by_id(terminal_id: &str) -> TerminalLocator {
    TerminalLocator {
        terminal_id: Some(terminal_id.to_string()),
        root_session_id: None,
        name: None,
    }
}

/// 按 `(root_session_id, name)` 构造定位器。
fn by_key(root_session_id: &str, name: &str) -> TerminalLocator {
    TerminalLocator {
        terminal_id: None,
        root_session_id: Some(root_session_id.to_string()),
        name: Some(name.to_string()),
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
fn resolve_prefers_terminal_id_then_session_key() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let expected = TerminalId::new(terminal_id.clone());

    // 1) 只给 terminal_id。
    assert_eq!(registry.resolve(&by_id(&terminal_id)), Ok(expected.clone()));

    // 2) 只给 (root_session_id, name)。
    assert_eq!(
        registry.resolve(&by_key("ses-1", "build")),
        Ok(expected.clone())
    );

    // 3) 都缺 → bad_request。
    assert_eq!(
        registry.resolve(&TerminalLocator::default()),
        Err(CoreError::BadRequest(
            "定位参数需要 terminal_id 或 (root_session_id, name)".to_string()
        ))
    );

    // 会话键存在但对应终端不在 → not_found。
    assert_eq!(
        registry.resolve(&by_key("ses-1", "missing")),
        Err(CoreError::NotFound)
    );

    // 同时给出时以 terminal_id 为准，不回退到会话键。
    let both_invalid_id = TerminalLocator {
        terminal_id: Some("01JNOTEXIST".to_string()),
        root_session_id: Some("ses-1".to_string()),
        name: Some("build".to_string()),
    };
    assert_eq!(registry.resolve(&both_invalid_id), Err(CoreError::NotFound));

    let both_valid_id = TerminalLocator {
        terminal_id: Some(terminal_id),
        root_session_id: Some("ses-1".to_string()),
        name: Some("other".to_string()),
    };
    assert_eq!(registry.resolve(&both_valid_id), Ok(expected));
}

#[test]
fn claim_grants_idle_lease_and_is_idempotent_for_same_owner() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let locator = by_id(&terminal_id);

    let granted = registry
        .claim(&locator, "client-a", false)
        .expect("空闲 lease 应授予");
    assert_eq!(granted.owner, "client-a");
    assert_eq!(granted.lease, "client-a");

    // 同一 owner 重复 claim 不报错，也不改变归属。
    let again = registry
        .claim(&locator, "client-a", false)
        .expect("同 owner 重复 claim 应幂等");
    assert_eq!(again.owner, "client-a");
}

#[test]
fn claim_conflict_requires_force_and_emits_revoke_to_old_owner() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let locator = by_id(&terminal_id);

    let (tx, mut rx) = unbounded_channel();
    registry
        .subscribe(&locator, "client-old", None, tx)
        .expect("旧 owner 订阅应成功");
    drain(&mut rx);

    registry
        .claim(&locator, "client-old", false)
        .expect("首次 claim 应成功");

    // 被他人占用且无 force → permission_denied。
    assert_eq!(
        registry.claim(&locator, "client-new", false),
        Err(CoreError::PermissionDenied)
    );
    assert!(
        registry
            .ensure_input_allowed(&locator, "client-old")
            .is_ok()
    );

    // force 抢占：旧 owner 收到 lease_revoked，lease 转给新 owner。
    let granted = registry
        .claim(&locator, "client-new", true)
        .expect("force claim 应成功");
    assert_eq!(granted.owner, "client-new");

    let events = drain(&mut rx);
    assert_eq!(events.len(), 1, "旧 owner 应收到一条 lease_revoked");
    assert!(matches!(
        events[0].kind,
        EventKind::LeaseRevoked { ref client_id } if client_id == "client-old"
    ));
    assert!(
        registry
            .ensure_input_allowed(&locator, "client-old")
            .is_err()
    );
    assert!(
        registry
            .ensure_input_allowed(&locator, "client-new")
            .is_ok()
    );
}

#[test]
fn release_only_succeeds_for_current_owner() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let locator = by_id(&terminal_id);

    // 空闲时释放 → permission_denied。
    assert_eq!(
        registry.release(&locator, "client-a"),
        Err(CoreError::PermissionDenied)
    );

    registry
        .claim(&locator, "client-a", false)
        .expect("claim 应成功");

    let (tx, mut rx) = unbounded_channel();
    registry
        .subscribe(&locator, "client-a", None, tx)
        .expect("owner 订阅应成功");
    drain(&mut rx);

    // 非 owner → permission_denied，lease 不变且不广播。
    assert_eq!(
        registry.release(&locator, "client-b"),
        Err(CoreError::PermissionDenied)
    );
    assert!(drain(&mut rx).is_empty());
    assert!(registry.ensure_input_allowed(&locator, "client-a").is_ok());

    // owner 释放成功，随后 lease 为空，并广播 lease_revoked。
    assert_eq!(registry.release(&locator, "client-a"), Ok(()));
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0].kind,
        EventKind::LeaseRevoked { ref client_id } if client_id == "client-a"
    ));
    assert_eq!(
        registry.ensure_input_allowed(&locator, "client-a"),
        Err(CoreError::PermissionDenied)
    );
    assert_eq!(
        registry.release(&locator, "client-a"),
        Err(CoreError::PermissionDenied)
    );
}

#[test]
fn terminal_state_clears_lease_and_rejects_input() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let locator = by_id(&terminal_id);
    let id = TerminalId::new(terminal_id);

    registry
        .claim(&locator, "client-a", false)
        .expect("claim 应成功");
    registry
        .report_event(&id, EventKind::Exited { exit_code: 0 })
        .expect("终态事件应成功");

    assert!(
        registry.get(&id).expect("终端应存在").lease.is_none(),
        "终态应清空 lease"
    );
    assert_eq!(
        registry.ensure_input_allowed(&locator, "client-a"),
        Err(CoreError::AlreadyTerminal)
    );
    assert_eq!(
        registry.claim(&locator, "client-b", false),
        Err(CoreError::AlreadyTerminal)
    );
    assert_eq!(
        registry.release(&locator, "client-a"),
        Err(CoreError::AlreadyTerminal)
    );

    // terminate 路径同样清空 lease。
    let second = create_terminal(&mut registry, "second");
    let second_locator = by_id(&second);
    let second_id = TerminalId::new(second);
    registry
        .claim(&second_locator, "client-b", false)
        .expect("claim 应成功");
    registry
        .terminate(&second_locator)
        .expect("terminate 应成功");
    assert!(
        registry
            .get(&second_id)
            .expect("终端应存在")
            .lease
            .is_none()
    );
    assert_eq!(
        registry.ensure_input_allowed(&second_locator, "client-b"),
        Err(CoreError::AlreadyTerminal)
    );
}

#[test]
fn ensure_input_allowed_requires_active_lease_owner() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = create_terminal(&mut registry, "build");
    let locator = by_key("ses-1", "build");
    assert_eq!(
        registry.resolve(&locator),
        Ok(TerminalId::new(terminal_id.clone()))
    );

    // 无 lease → 拒绝。
    assert_eq!(
        registry.ensure_input_allowed(&locator, "client-a"),
        Err(CoreError::PermissionDenied)
    );

    registry
        .claim(&locator, "client-a", false)
        .expect("claim 应成功");
    assert_eq!(registry.ensure_input_allowed(&locator, "client-a"), Ok(()));
    assert_eq!(
        registry.ensure_input_allowed(&locator, "client-b"),
        Err(CoreError::PermissionDenied)
    );

    // 定位不到终端 → not_found。
    assert_eq!(
        registry.ensure_input_allowed(&by_id("01JNOTEXIST"), "client-a"),
        Err(CoreError::NotFound)
    );
}

#[test]
fn release_all_clears_only_leases_owned_by_the_client_and_emits_revoked() {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let first = create_terminal(&mut registry, "first");
    let second = create_terminal(&mut registry, "second");

    // 观察者订阅两个终端，用于确认断连清理也会广播 lease_revoked。
    let (observer_tx, mut observer_rx) = unbounded_channel();
    registry
        .subscribe(&by_id(&first), "observer", None, observer_tx.clone())
        .expect("observer 订阅 first 应成功");
    registry
        .subscribe(&by_id(&second), "observer", None, observer_tx)
        .expect("observer 订阅 second 应成功");
    drain(&mut observer_rx);

    registry
        .claim(&by_id(&first), "client-a", false)
        .expect("first claim 应成功");
    registry
        .claim(&by_id(&second), "client-b", false)
        .expect("second claim 应成功");

    registry.release_all("client-a");

    let events = drain(&mut observer_rx);
    assert_eq!(events.len(), 1, "断连清理应只对 client-a 的 lease 广播一次");
    assert_eq!(events[0].terminal_id, first);
    assert!(matches!(
        events[0].kind,
        EventKind::LeaseRevoked { ref client_id } if client_id == "client-a"
    ));

    assert_eq!(
        registry.ensure_input_allowed(&by_id(&first), "client-a"),
        Err(CoreError::PermissionDenied),
        "断连应释放该 client 持有的 lease"
    );
    assert_eq!(
        registry.ensure_input_allowed(&by_id(&second), "client-b"),
        Ok(()),
        "其他 client 的 lease 不应受影响"
    );

    // 释放后可被其他连接重新 claim。
    let granted = registry
        .claim(&by_id(&first), "client-b", false)
        .expect("释放后应可重新 claim");
    assert_eq!(granted.owner, "client-b");
}
