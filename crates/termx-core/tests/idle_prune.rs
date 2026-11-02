//! M2 收尾测试：`idle_timeout` 的 detach/terminate 动作与 `terminal.prune`。

use std::sync::Arc;

use termx_core::{
    CoreError, DEFAULT_EVENT_BUFFER_CAPACITY, IDLE_DETACH_REASON, IdleAction, MockBackend,
    MockFactory, Registry, TIMEOUT_SOURCE, TerminalBackend, TerminalId,
};
use termx_protocol::{
    CreateParams, Event, EventKind, IdleTimeout, IdleTimeoutAction, IdleTimeoutUnit,
    LifecycleState, ListParams, Mode, PruneBasis, PruneParams, TerminalLocator, TimeSpan,
};
use time::{Duration, OffsetDateTime};
use tokio::sync::mpsc::{Receiver, channel, error::TryRecvError};

/// 使用 mock 工厂的 registry。
fn mock_registry() -> Registry {
    Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory))
}

/// 构造创建参数。
fn params(root_session_id: &str, name: &str, idle_timeout: Option<IdleTimeout>) -> CreateParams {
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
        idle_timeout,
    }
}

/// 构造指定数值/单位/动作的空闲超时。
fn idle(value: u64, unit: IdleTimeoutUnit, action: IdleTimeoutAction) -> IdleTimeout {
    IdleTimeout {
        value,
        unit,
        action: Some(action),
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

/// 在最后活动时间之后推进指定毫秒，作为 `expire_idle` 的“当前时间”。
fn after_last_activity(registry: &Registry, id: &TerminalId, advance_ms: i64) -> OffsetDateTime {
    registry
        .get(id)
        .expect("终端应存在")
        .activity_time()
        .expect("创建后应有活动时间")
        + Duration::milliseconds(advance_ms)
}

#[test]
fn zero_idle_timeout_never_triggers() {
    let mut registry = mock_registry();
    let created = registry
        .create_with_backend(
            params(
                "ses-idle-zero",
                "zero",
                Some(idle(0, IdleTimeoutUnit::S, IdleTimeoutAction::Detach)),
            ),
            Arc::new(MockBackend::new()),
        )
        .expect("create 应成功");
    let id = TerminalId::new(created.terminal_id.clone());

    let now = after_last_activity(&registry, &id, 0) + Duration::days(1);
    assert!(
        registry.expire_idle(now).is_empty(),
        "value=0 不应触发任何动作"
    );
    assert_eq!(
        registry
            .status(&by_id(&created.terminal_id))
            .expect("终端应存在")
            .state,
        LifecycleState::Running
    );

    // 未配置 idle_timeout 同样永不触发。
    let created_without = registry
        .create_with_backend(
            params("ses-idle-zero", "none", None),
            Arc::new(MockBackend::new()),
        )
        .expect("create 应成功");
    assert!(
        registry.expire_idle(now + Duration::days(1)).is_empty(),
        "None 不应触发任何动作"
    );
    assert_eq!(
        registry
            .status(&by_id(&created_without.terminal_id))
            .expect("终端应存在")
            .state,
        LifecycleState::Running
    );
}

#[test]
fn detach_action_emits_detached_and_terminal_stays_running() {
    let mut registry = mock_registry();
    let created = registry
        .create_with_backend(
            params(
                "ses-idle-detach",
                "detach",
                Some(idle(100, IdleTimeoutUnit::Ms, IdleTimeoutAction::Detach)),
            ),
            Arc::new(MockBackend::new()),
        )
        .expect("create 应成功");
    let id = TerminalId::new(created.terminal_id.clone());
    let locator = by_id(&created.terminal_id);

    let (tx, mut rx) = channel(64);
    registry
        .attach(&locator, "client-a", Some(0), tx)
        .expect("attach 应成功");
    drain(&mut rx);

    let now = after_last_activity(&registry, &id, 101);
    let actions = registry.expire_idle(now);
    assert_eq!(actions.len(), 1, "应产出一个 detach 动作");
    match &actions[0] {
        IdleAction::Detach {
            terminal_id,
            clients,
        } => {
            assert_eq!(terminal_id, &id);
            assert_eq!(clients, &vec!["client-a".to_string()]);
        }
        other => panic!("期望 Detach，实际 {other:?}"),
    }

    let events = drain(&mut rx);
    assert!(
        events.iter().any(|event| matches!(
            &event.kind,
            EventKind::Detached { client_id, reason }
                if client_id == "client-a" && reason == IDLE_DETACH_REASON
        )),
        "应收到 idle_timeout detached: {events:?}"
    );
    assert_eq!(
        registry.status(&locator).expect("终端应存在").state,
        LifecycleState::Running,
        "detach 不改变终端运行状态"
    );

    // last_activity 已刷新，同一时间点不应重复触发。
    assert!(
        registry.expire_idle(now).is_empty(),
        "同一时间点不应重复 detach"
    );
}

#[test]
fn terminate_action_emits_timeout_interrupted_and_keeps_backend_for_caller() {
    let mut registry = mock_registry();
    let backend = Arc::new(MockBackend::new());
    let created = registry
        .create_with_backend(
            params(
                "ses-idle-terminate",
                "terminate",
                Some(idle(100, IdleTimeoutUnit::Ms, IdleTimeoutAction::Terminate)),
            ),
            Arc::clone(&backend) as Arc<dyn termx_core::TerminalBackend>,
        )
        .expect("create 应成功");
    let id = TerminalId::new(created.terminal_id.clone());
    let locator = by_id(&created.terminal_id);

    let (tx, mut rx) = channel(64);
    registry
        .attach(&locator, "client-a", Some(0), tx)
        .expect("attach 应成功");
    drain(&mut rx);

    let now = after_last_activity(&registry, &id, 101);
    let actions = registry.expire_idle(now);
    assert_eq!(actions.len(), 1, "应产出一个 terminate 动作");
    assert!(
        matches!(actions[0], IdleAction::Terminate(_)),
        "期望 Terminate 动作: {actions:?}"
    );
    assert!(
        !backend.was_terminated(),
        "registry 锁内不直接终止后端，应由调用方在锁外执行"
    );

    let events = drain(&mut rx);
    assert!(
        events.iter().any(|event| matches!(
            &event.kind,
            EventKind::Interrupted { signal, source }
                if signal == "SIGTERM" && source == TIMEOUT_SOURCE
        )),
        "应收到 timeout interrupted: {events:?}"
    );
    let snapshot = registry.status(&locator).expect("终态应可读");
    assert_eq!(snapshot.state, LifecycleState::Interrupted);
    assert_eq!(
        registry.get(&id).expect("终端应存在").lease,
        None,
        "终态应清理 lease"
    );

    // 调用方在锁外终止后端；再次扫描不应重复产出终态。
    backend.terminate();
    assert!(backend.was_terminated());
    assert!(
        registry.expire_idle(now + Duration::seconds(1)).is_empty(),
        "终态终端不应再次触发"
    );
}

#[test]
fn prune_removes_only_terminal_terminals_and_reports_ids() {
    let mut registry = mock_registry();
    let terminated = registry
        .create_with_backend(
            params("ses-prune", "terminated", None),
            Arc::new(MockBackend::new()),
        )
        .expect("create 应成功");
    let terminated_other = registry
        .create_with_backend(
            params("ses-prune", "terminated-other", None),
            Arc::new(MockBackend::new()),
        )
        .expect("create 应成功");
    let running = registry
        .create_with_backend(
            params("ses-prune", "running", None),
            Arc::new(MockBackend::new()),
        )
        .expect("create 应成功");

    let terminated_id = TerminalId::new(terminated.terminal_id.clone());
    let running_id = TerminalId::new(running.terminal_id.clone());

    // 预置两类可清理状态：终端级订阅与全局订阅排除项。
    let (terminal_tx, mut terminal_rx) = channel(64);
    registry
        .attach(
            &by_id(&terminated.terminal_id),
            "client-sub",
            Some(0),
            terminal_tx,
        )
        .expect("attach 应成功");
    drain(&mut terminal_rx);

    let (global_tx, _global_rx) = channel(64);
    registry.subscribe_all("client-excl", global_tx);
    registry
        .unsubscribe(&terminated_id, "client-excl")
        .expect("unsubscribe 应成功");
    assert!(registry.is_excluded_from_global("client-excl", &terminated_id));
    assert_eq!(
        registry.subscribed_clients(&terminated_id),
        vec!["client-sub".to_string()]
    );

    registry
        .terminate(&by_id(&terminated.terminal_id))
        .expect("terminate 应成功");
    registry
        .terminate(&by_id(&terminated_other.terminal_id))
        .expect("terminate 应成功");
    // 终态事件会投递给仍存在的终端级订阅，先清空再断言订阅被移除。
    drain(&mut terminal_rx);

    let result = registry.prune(PruneParams::default());
    let mut removed = result.removed.clone();
    removed.sort();
    let mut expected = vec![
        terminated.terminal_id.clone(),
        terminated_other.terminal_id.clone(),
    ];
    expected.sort();
    assert_eq!(removed, expected, "removed 应恰好是全部终态终端 ID");
    assert_eq!(result.removed.len(), 2);

    // by_id 与 by_key 双索引都必须删除。
    assert_eq!(
        registry.status(&by_id(&terminated.terminal_id)),
        Err(CoreError::NotFound),
        "已清理终端应解析不到"
    );
    assert_eq!(
        registry.status(&TerminalLocator {
            terminal_id: None,
            root_session_id: Some("ses-prune".to_string()),
            name: Some("terminated".to_string()),
        }),
        Err(CoreError::NotFound),
        "by_key 会话键索引应同步删除"
    );
    assert!(registry.get(&terminated_id).is_none());
    assert!(registry.get(&running_id).is_some(), "运行中终端应保留");

    // 终端级订阅被移除：registry 侧发送端 drop 后接收端断开。
    assert!(registry.subscribed_clients(&terminated_id).is_empty());
    assert!(
        matches!(terminal_rx.try_recv(), Err(TryRecvError::Disconnected)),
        "prune 后终端级订阅的发送端应被释放"
    );

    // 全局订阅排除集同步清理，避免无界内存增长。
    assert!(
        !registry.is_excluded_from_global("client-excl", &terminated_id),
        "prune 应把已删除终端移出全局排除集"
    );

    assert_eq!(
        registry
            .status(&by_id(&running.terminal_id))
            .expect("运行中应保留")
            .state,
        LifecycleState::Running
    );
    let items = registry.list(&ListParams::default());
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].terminal_id, running.terminal_id);

    // states 过滤也绝不会删除运行中终端。
    let result = registry.prune(PruneParams {
        states: Some(vec![LifecycleState::Running]),
        older_than: None,
        basis: None,
    });
    assert!(result.removed.is_empty());
    assert_eq!(registry.len(), 1);
}

#[test]
fn prune_basis_selects_created_at_or_last_activity() {
    let mut registry = mock_registry();
    let old_created = registry
        .create_with_backend(
            params("ses-prune-basis", "old-created", None),
            Arc::new(MockBackend::new()),
        )
        .expect("create 应成功");
    let old_created_id = TerminalId::new(old_created.terminal_id.clone());
    // 创建时间改成很久以前；终态事件会把 last_activity 刷成当前时间。
    registry
        .get_mut(&old_created_id)
        .expect("终端应存在")
        .created_at = "2000-01-01T00:00:00Z".to_string();
    registry
        .terminate(&by_id(&old_created.terminal_id))
        .expect("terminate 应成功");

    let recent = registry
        .create_with_backend(
            params("ses-prune-basis", "recent", None),
            Arc::new(MockBackend::new()),
        )
        .expect("create 应成功");
    let recent_id = TerminalId::new(recent.terminal_id.clone());

    // 以 created_at 为基准：只删除“创建时间很久以前”的终态终端。
    let result = registry.prune(PruneParams {
        states: None,
        older_than: Some(TimeSpan {
            value: 1,
            unit: IdleTimeoutUnit::S,
        }),
        basis: Some(PruneBasis::CreatedAt),
    });
    assert_eq!(result.removed, vec![old_created.terminal_id]);
    assert!(registry.get(&old_created_id).is_none());
    assert!(registry.get(&recent_id).is_some());

    // 以 last_activity 为基准：把 recent 的活动时间改旧后才能被清理。
    registry
        .get_mut(&recent_id)
        .expect("终端应存在")
        .last_activity = Some("2000-01-01T00:00:00Z".to_string());
    registry
        .terminate(&by_id(&recent.terminal_id))
        .expect("terminate 应成功");

    let result = registry.prune(PruneParams {
        states: None,
        older_than: Some(TimeSpan {
            value: 1,
            unit: IdleTimeoutUnit::Ms,
        }),
        basis: Some(PruneBasis::LastActivity),
    });
    assert!(
        result.removed.is_empty(),
        "终态事件的 last_activity 是当前时间"
    );

    // 把终态记录的 last_activity 改旧，验证 last_activity 基准确实生效。
    registry
        .get_mut(&recent_id)
        .expect("终端应存在")
        .last_activity = Some("2000-01-01T00:00:00Z".to_string());
    let result = registry.prune(PruneParams {
        states: None,
        older_than: Some(TimeSpan {
            value: 1,
            unit: IdleTimeoutUnit::Ms,
        }),
        basis: Some(PruneBasis::LastActivity),
    });
    assert_eq!(result.removed, vec![recent.terminal_id]);
}
