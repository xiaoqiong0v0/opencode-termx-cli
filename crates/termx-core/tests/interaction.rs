//! M4b registry 接线测试：输出摄取与交互采样会写入条目状态并广播事件。

use std::sync::Arc;

use termx_core::{
    DEFAULT_EVENT_BUFFER_CAPACITY, InteractionConfig, MockFactory, Registry, TerminalId,
};
use termx_protocol::{
    Confidence, CreateParams, EventKind, InteractionState, LifecycleState, Mode, ProcNode,
};
use time::OffsetDateTime;

/// 使用 mock 后端与固定交互配置的 registry。
fn registry() -> Registry {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    registry.set_interaction_config(InteractionConfig {
        enabled: true,
        no_output_threshold: std::time::Duration::from_secs(1),
        repeat_interval: std::time::Duration::from_secs(5),
    });
    registry
}

/// 创建 mock 终端并返回 ID 与根进程组。
fn create(registry: &mut Registry, name: &str) -> (TerminalId, u32) {
    let params = CreateParams {
        mode: Mode::Pty,
        cmd: "mock".to_string(),
        args: Vec::new(),
        cwd: None,
        env: None,
        cols: None,
        rows: None,
        root_session_id: "ses-i".to_string(),
        name: name.to_string(),
        force: false,
        idle_timeout: None,
    };
    let created = registry.create(params).expect("create 应成功");
    let id = TerminalId::new(created.terminal_id);
    let root_pgid = registry.get(&id).expect("终端应存在").pgid.expect("pgid");
    (id, root_pgid)
}

/// 固定时间基准。
fn at(offset_ms: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("合法时间戳")
        + time::Duration::milliseconds(offset_ms)
}

#[test]
fn observe_output_emits_waiting_input_and_updates_entry() {
    let mut registry = registry();
    let (id, _) = create(&mut registry, "osc");

    let before = registry.get(&id).expect("终端应存在").seq;
    let seq = registry
        .observe_output(&id, b"\x1b]133;A\x07", at(0))
        .expect("摄取应成功");
    assert_eq!(seq, before + 1, "等待事件应紧接上一条事件");

    let entry = registry.get(&id).expect("终端应存在");
    assert_eq!(entry.interaction, InteractionState::WaitingInput);
    assert_eq!(entry.confidence, Confidence::High);
    assert_eq!(entry.state, LifecycleState::Running);

    let record = entry.events.records().back().expect("应有最新事件");
    match &record.kind {
        EventKind::WaitingInput { confidence, reason } => {
            assert_eq!(*confidence, Confidence::High);
            assert_eq!(reason, "shell_integration");
        }
        other => panic!("期望 waiting_input，实际 {other:?}"),
    }
}

#[test]
fn sample_interaction_emits_foreground_change_and_return_to_root() {
    let mut registry = registry();
    let (id, root_pgid) = create(&mut registry, "job");
    let tree = vec![
        ProcNode {
            pid: root_pgid,
            ppid: 1,
            pgid: root_pgid,
            cmd: "sh".to_string(),
        },
        ProcNode {
            pid: root_pgid + 1,
            ppid: root_pgid,
            pgid: root_pgid + 1,
            cmd: "python3".to_string(),
        },
    ];

    registry
        .sample_interaction(&id, at(0), Some(root_pgid + 1), &tree)
        .expect("采样应成功");
    let entry = registry.get(&id).expect("终端应存在");
    assert_eq!(entry.interaction, InteractionState::Interactive);
    assert_eq!(entry.confidence, Confidence::Medium);
    assert!(matches!(
        entry.events.records().back().map(|record| &record.kind),
        Some(EventKind::InteractiveChanged { active: true, .. })
    ));

    registry
        .sample_interaction(&id, at(10), Some(root_pgid), &tree)
        .expect("采样应成功");
    let entry = registry.get(&id).expect("终端应存在");
    assert_eq!(entry.interaction, InteractionState::None);
    assert!(matches!(
        entry.events.records().back().map(|record| &record.kind),
        Some(EventKind::InteractiveChanged { active: false, .. })
    ));
    assert_eq!(entry.state, LifecycleState::Running, "交互变化不是终态");
}

#[test]
fn silent_terminal_emits_low_confidence_no_output_once_per_interval() {
    let mut registry = registry();
    let (id, root_pgid) = create(&mut registry, "silent");
    // 注册时间即静默起点（registry 用真实时钟），因此采样时间也取真实时钟。
    let base = OffsetDateTime::now_utc();

    // 1s 后达到阈值。
    registry
        .sample_interaction(&id, base + time::Duration::seconds(1), Some(root_pgid), &[])
        .expect("采样应成功");
    let entry = registry.get(&id).expect("终端应存在");
    assert_eq!(entry.interaction, InteractionState::WaitingInput);
    assert_eq!(entry.confidence, Confidence::Low);
    match entry.events.records().back().map(|record| &record.kind) {
        Some(EventKind::WaitingInput { confidence, reason }) => {
            assert_eq!(*confidence, Confidence::Low);
            assert_eq!(reason, "no_output");
        }
        other => panic!("期望 waiting_input，实际 {other:?}"),
    }
    let after_first = entry.seq;

    // 未到重复间隔：不产生新事件。
    registry
        .sample_interaction(
            &id,
            base + time::Duration::milliseconds(1500),
            Some(root_pgid),
            &[],
        )
        .expect("采样应成功");
    assert_eq!(registry.get(&id).expect("终端应存在").seq, after_first);

    // 到达重复间隔：允许再次投递。
    registry
        .sample_interaction(&id, base + time::Duration::seconds(6), Some(root_pgid), &[])
        .expect("采样应成功");
    assert!(registry.get(&id).expect("终端应存在").seq > after_first);
}
