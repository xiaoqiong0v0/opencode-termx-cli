//! 命令记录语义（core）：开始/输出/结束、容量与清洗。
#![cfg(any(windows, unix))]

use std::sync::Arc;

use termx_core::{MockFactory, Registry, TerminalId};
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
