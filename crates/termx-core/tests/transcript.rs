//! 命令记录语义（core）：开始/输出/结束、容量与清洗。
#![cfg(any(windows, unix))]

use std::sync::Arc;

use termx_core::{MockFactory, Registry, TerminalId, format_rfc3339};
use termx_protocol::{CreateParams, EventKind, Mode, TerminalLocator, TranscriptStatus};
use time::OffsetDateTime;

/// 构造使用 mock 后端的 registry（不启动真实进程）。
fn registry() -> Registry {
    Registry::with_factory(64, Arc::new(MockFactory))
}

/// 创建参数。
fn params(root: &str, name: &str) -> CreateParams {
    CreateParams {
        mode: Mode::Pty,
        cmd: "mock".to_string(),
        args: Vec::new(),
        cwd: None,
        env: None,
        cols: None,
        rows: None,
        root_session_id: root.to_string(),
        name: name.to_string(),
        force: false,
        idle_timeout: None,
        ssh: None,
        allow_injection: false,
        runtime_id: None,
    }
}

/// 以 terminal_id 定位。
fn by_id(id: &str) -> TerminalLocator {
    TerminalLocator {
        terminal_id: Some(id.to_string()),
        root_session_id: None,
        name: None,
    }
}

/// 固定基准时间。
fn base() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("合法时间戳")
}

#[test]
fn command_and_output_are_recorded_and_next_command_finalizes_previous() {
    let mut registry = registry();
    let created = registry.create(params("ses-1", "build")).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();

    registry
        .record_transcript_command(&id, "echo hi\r\n", t0)
        .expect("记录命令");
    registry
        .record_transcript_output(&id, "echo hi\r\nhi\r\nD:\\proj>", t0)
        .expect("记录输出");

    let running = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(running.commands.len(), 1);
    assert_eq!(running.commands[0].command, "echo hi", "命令去掉末尾 CR/LF");
    assert_eq!(running.commands[0].status, TranscriptStatus::Running);
    assert!(running.commands[0].ended_at.is_none());
    assert_eq!(running.commands[0].output, "hi", "清洗回显与提示符");

    let t1 = t0 + time::Duration::seconds(3);
    registry
        .record_transcript_command(&id, "cd ..", t1)
        .expect("记录下一条命令");
    let result = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(result.commands.len(), 2);
    assert_eq!(result.commands[0].status, TranscriptStatus::Done);
    assert_eq!(
        result.commands[0].ended_at.as_deref(),
        Some(termx_core::format_rfc3339(t1).as_str())
    );
    assert_eq!(result.commands[1].status, TranscriptStatus::Running);
}

#[test]
fn capacity_evicts_oldest_and_read_limit_returns_latest() {
    let mut registry = registry();
    registry.set_transcript_capacity(3);
    let created = registry.create(params("ses-1", "build")).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();

    for index in 0..5 {
        registry
            .record_transcript_command(
                &id,
                &format!("cmd-{index}"),
                t0 + time::Duration::seconds(index),
            )
            .expect("记录命令");
    }

    let all = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    let names: Vec<&str> = all
        .commands
        .iter()
        .map(|cmd| cmd.command.as_str())
        .collect();
    assert_eq!(names, vec!["cmd-2", "cmd-3", "cmd-4"], "只保留最近 3 条");
    assert_eq!(all.commands[0].status, TranscriptStatus::Done);
    assert_eq!(all.commands[2].status, TranscriptStatus::Running);

    let latest = registry
        .transcript(&by_id(&created.terminal_id), Some(1))
        .expect("读取最近一条");
    assert_eq!(latest.commands.len(), 1);
    assert_eq!(latest.commands[0].command, "cmd-4");
}

#[test]
fn terminal_event_finalizes_running_command() {
    let mut registry = registry();
    let created = registry.create(params("ses-1", "build")).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    registry
        .record_transcript_command(&id, "sleep 10", base())
        .expect("记录命令");

    registry
        .report_event(
            &id,
            EventKind::Interrupted {
                signal: "SIGINT".to_string(),
                source: "control-c".to_string(),
            },
        )
        .expect("落终态");

    let result = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(result.commands[0].status, TranscriptStatus::Interrupted);
    assert!(result.commands[0].ended_at.is_some());
}

#[test]
fn shell_event_end_records_exit_code_and_upgrades_best_effort() {
    let mut registry = registry();
    let created = registry.create(params("ses-1", "build")).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();

    registry
        .record_transcript_command(&id, "cmd /c exit 7", t0)
        .expect("记录命令");
    assert!(
        registry
            .record_transcript_end(&id, Some(7), t0 + time::Duration::seconds(1))
            .expect("结束命令")
    );

    let result = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(result.commands[0].status, TranscriptStatus::Done);
    assert_eq!(result.commands[0].exit_code, Some(7));
    assert_eq!(
        result.commands[0].source,
        Some(termx_protocol::TranscriptSource::ShellIntegration)
    );
    assert!(!result.commands[0].best_effort);

    // 已由真实证据结束的命令不再被重复结束。
    assert!(
        !registry
            .record_transcript_end(&id, Some(9), t0 + time::Duration::seconds(2))
            .expect("重复结束应被忽略")
    );
    let result = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(result.commands[0].exit_code, Some(7));
}

#[test]
fn register_create_with_disables_in_band_osc_for_shell_events() {
    use termx_core::MockBackend;

    let mut registry = registry();
    let params = params("ses-1", "build");
    registry.prepare_create(&params).expect("预检");
    let (created, displaced) = registry
        .register_create_with(params, Arc::new(MockBackend::new()), None, false)
        .expect("注册");
    assert!(displaced.is_none());
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();

    registry
        .record_transcript_command(&id, "echo hi", t0)
        .expect("记录命令");
    registry
        .record_transcript_output(&id, "\u{1b}]133;D;0\u{7}", t0)
        .expect("记录输出");

    let result = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(result.commands[0].status, TranscriptStatus::Running);
    assert_eq!(result.commands[0].exit_code, None);
}

#[test]
fn records_transcript_on_send_follows_event_channel() {
    use termx_core::MockBackend;

    let mut registry = registry();
    let in_band = registry.create(params("ses-1", "in-band")).expect("create");
    assert!(
        registry
            .records_transcript_on_send(&by_id(&in_band.terminal_id))
            .expect("查询 in-band 终端"),
        "普通终端应保留 send-time 记录"
    );

    let params = params("ses-1", "events");
    registry.prepare_create(&params).expect("预检");
    let (created, displaced) = registry
        .register_create_with(params, Arc::new(MockBackend::new()), None, false)
        .expect("注册事件通道终端");
    assert!(displaced.is_none());
    assert!(
        !registry
            .records_transcript_on_send(&by_id(&created.terminal_id))
            .expect("查询事件通道终端"),
        "事件通道终端不应在 send 时记录"
    );

    let missing = TerminalLocator {
        terminal_id: Some("missing".to_string()),
        root_session_id: None,
        name: None,
    };
    assert!(matches!(
        registry.records_transcript_on_send(&missing),
        Err(termx_core::CoreError::NotFound)
    ));
}

#[test]
fn pending_generation_updates_command_and_start_without_new_record() {
    let mut registry = registry();
    let created = registry.create(params("ses-1", "build")).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let send_time = base();
    let host_start = send_time + time::Duration::seconds(2);

    registry
        .record_transcript_command(&id, "run", send_time)
        .expect("记录 send fallback");
    let generation = registry
        .take_pending_transcript_generation(&id)
        .expect("读取 pending")
        .expect("send 记录应留下 pending");
    assert!(
        registry
            .update_transcript_start(&id, generation, Some("run --flag"), Some(host_start))
            .expect("更新开始")
    );
    assert!(
        registry
            .take_pending_transcript_generation(&id)
            .expect("再次读取 pending")
            .is_none(),
        "pending 只能消费一次"
    );

    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands.len(), 1, "更新不应插入新记录");
    assert_eq!(commands[0].command, "run --flag");
    assert_eq!(commands[0].started_at, format_rfc3339(host_start));
}

#[test]
fn host_command_without_pending_inserts_record() {
    let mut registry = registry();
    let created = registry.create(params("ses-1", "build")).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let start = base() + time::Duration::seconds(4);

    let generation = registry
        .record_transcript_command_at(&id, "run", start)
        .expect("新建宿主命令");
    assert_eq!(generation, 1);
    assert!(
        registry
            .take_pending_transcript_generation(&id)
            .expect("读取 pending")
            .is_none(),
        "宿主新建命令不应留下 pending"
    );

    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].command, "run");
    assert_eq!(commands[0].started_at, format_rfc3339(start));
}

#[test]
fn generation_end_accepts_host_end_time() {
    let mut registry = registry();
    let created = registry.create(params("ses-1", "build")).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let start = base();
    let ended = start + time::Duration::milliseconds(750);

    let generation = registry
        .record_transcript_command_at(&id, "run", start)
        .expect("新建宿主命令");
    assert!(
        registry
            .record_transcript_end_for_generation_at(&id, Some(7), generation, ended)
            .expect("按宿主时间结束")
    );

    let command = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands
        .remove(0);
    assert_eq!(command.status, TranscriptStatus::Done);
    assert_eq!(
        command.ended_at.as_deref(),
        Some(format_rfc3339(ended).as_str())
    );
}
