//! 远端 `O` data 进 service 的 transcript 语义（不依赖真实 SSH）。
//!
//! 远端 helper 把 `O` 帧透传为 `ProcEvent::ShellEvent`，与本地事件通道共用
//! [`termx_service::shell_events::apply_event`]；PTY 输出在远端也走同一条
//! [`Registry::record_pty_transcript_output`] 兜底路径。本文件直接驱动这两条
//! 路径，验证：`O` 为主、PTY 回显不双写、混合 host+native 才标 `incomplete`、
//! 旧 helper 的 `O` 不带 `data` 时退化为 PTY 兜底。`handle_create` 的布尔
//! 连线（`event_channel_active` / `transcript_output_from_events`）由
//! `remote_o_wiring.rs` 经真实 service 路径锁定。

#![cfg(any(windows, unix))]

use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use termx_core::{MockFactory, Registry, TerminalId};
use termx_protocol::{CreateParams, Mode, TerminalLocator, TranscriptSource, TranscriptStatus};
use termx_service::shell_events::{ShellEvent, ShellEventKind, ShellEventTracker, apply_event};
use time::OffsetDateTime;

/// 构造使用 mock 后端的 registry。
fn registry() -> Registry {
    Registry::with_factory(64, Arc::new(MockFactory))
}

/// 创建参数：远端 `termx-pwsh`（`ssh` 有值、`runtime_id` 为结构化 runtime）。
fn params() -> CreateParams {
    CreateParams {
        mode: Mode::Pty,
        cmd: "termx-pwsh".to_string(),
        args: Vec::new(),
        cwd: None,
        env: None,
        cols: None,
        rows: None,
        root_session_id: "ses-remote-o".to_string(),
        name: "remote-o".to_string(),
        force: false,
        idle_timeout: None,
        // 本测试直接驱动 service 层共享路径（`apply_event` + PTY 兜底），
        // 不建真实 SSH 会话；远端 helper 的 `shell_event` 最终映射到同一条
        // `ProcEvent::ShellEvent → apply_event` 路径，因此 `ssh` 保持 `None`。
        ssh: None,
        allow_injection: true,
        runtime_id: Some("termx-pwsh".to_string()),
    }
}

/// 按 terminal_id 定位。
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

/// 构造 `O` 事件：`data` 为 base64 编码的 UTF-8 文本。
fn output_event(id: &str, text: &str) -> ShellEvent {
    ShellEvent {
        kind: ShellEventKind::CommandOutput,
        code: None,
        id: Some(id.to_string()),
        command: None,
        started_at_ms: None,
        ended_at_ms: None,
        data: Some(STANDARD.encode(text.as_bytes())),
        dropped: false,
    }
}

/// 构造不带 `data` 的 `O` 事件（旧 helper 兼容路径：远端只转发事件帧、
/// 不透传输出文本）。
fn output_event_without_data(id: &str) -> ShellEvent {
    ShellEvent {
        kind: ShellEventKind::CommandOutput,
        code: None,
        id: Some(id.to_string()),
        command: None,
        started_at_ms: None,
        ended_at_ms: None,
        data: None,
        dropped: false,
    }
}

/// 构造 `C` 事件（带命令文本）。
fn start_event(id: &str, command: &str) -> ShellEvent {
    ShellEvent {
        kind: ShellEventKind::CommandStart,
        code: None,
        id: Some(id.to_string()),
        command: Some(command.to_string()),
        started_at_ms: None,
        ended_at_ms: None,
        data: None,
        dropped: false,
    }
}

/// 构造 `D` 事件。
fn end_event(id: &str, code: i32) -> ShellEvent {
    ShellEvent {
        kind: ShellEventKind::CommandEnd,
        code: Some(code),
        id: Some(id.to_string()),
        command: None,
        started_at_ms: None,
        ended_at_ms: None,
        data: None,
        dropped: false,
    }
}

#[test]
fn remote_o_data_authoritative_pty_echo_not_duplicated() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();
    let mut tracker = ShellEventTracker::new();

    // `C` 建记录并绑定代次。
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &start_event("cmd-1", "Write-Output hi"),
            t0
        )
        .expect("C")
    );

    // 远端 PTY 回显先到（`O` 的回显副本），走兜底路径，不立即进 transcript。
    registry
        .record_pty_transcript_output(&id, "PS /tmp> Write-Output hi\r\nhi\r\nPS /tmp>", t0)
        .expect("记录 PTY 兜底");

    // 透传来的 `O` data 追加到同一命令。
    assert!(
        apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &output_event("cmd-1", "hi\n"),
            t0
        )
        .expect("O")
    );

    // `D` 结束命令：结束时会丢弃 PTY 兜底（`O` 为主）。
    assert!(apply_event(&mut registry, &id, &mut tracker, &end_event("cmd-1", 0), t0).expect("D"));

    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].status, TranscriptStatus::Done);
    assert_eq!(commands[0].exit_code, Some(0));
    assert_eq!(commands[0].source, Some(TranscriptSource::ShellIntegration));
    // 以 O 为准：输出只含 O 文本，PTY 回显不得双写进 output。
    assert_eq!(commands[0].output, "hi", "PTY 回显不得与 O 双写");
    assert!(
        !commands[0].incomplete,
        "PTY 只是 O 的回显副本时不得标 incomplete"
    );
    assert!(!commands[0].best_effort);
}

#[test]
fn remote_o_data_marks_incomplete_when_pty_has_uncovered_native_lines() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();
    let mut tracker = ShellEventTracker::new();

    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &start_event("cmd-2", "Write-Output host; cmd /c echo native"),
            t0,
        )
        .expect("C")
    );

    // 混合命令：PTY 兜底含 native 段，O 只覆盖 host 段。
    registry
        .record_pty_transcript_output(&id, "host\r\nnative\r\n", t0)
        .expect("记录 PTY 兜底");
    assert!(
        apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &output_event("cmd-2", "host\n"),
            t0
        )
        .expect("O")
    );
    assert!(apply_event(&mut registry, &id, &mut tracker, &end_event("cmd-2", 0), t0).expect("D"));

    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands[0].output, "host", "PTY 兜底不得追加到 O 文本");
    assert!(
        commands[0].incomplete,
        "PTY 存在 O 未覆盖的 native 行时必须标 incomplete"
    );
    assert_eq!(commands[0].exit_code, Some(0), "incomplete 不得伪造退出码");
}

#[test]
fn remote_o_data_without_pty_is_pure_and_not_incomplete() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();
    let mut tracker = ShellEventTracker::new();

    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &start_event("cmd-3", "Write-Output hi"),
            t0
        )
        .expect("C")
    );
    assert!(
        apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &output_event("cmd-3", "hi\n"),
            t0
        )
        .expect("O")
    );
    assert!(apply_event(&mut registry, &id, &mut tracker, &end_event("cmd-3", 0), t0).expect("D"));

    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands[0].output, "hi");
    assert!(!commands[0].incomplete, "纯 O 命令不得标 incomplete");
}

/// 旧 helper 兼容：`O` 帧不带 `data` 时不崩、不追加文本，整条命令退化为
/// PTY 兜底路径——`apply_command_output` 对 `None` 早退（shell_events.rs:160），
/// `event_output_seen` 不置位，`D` 结束时由 `settle_pty_output` 把 PTY 文本
/// 并入 transcript。
#[test]
fn remote_o_without_data_degrades_to_pty_fallback() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();
    let mut tracker = ShellEventTracker::new();

    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &start_event("cmd-4", "Write-Output hi"),
            t0
        )
        .expect("C")
    );

    // PTY 兜底先承载真实输出（远端 PTY 合并流）。
    registry
        .record_pty_transcript_output(&id, "PS /tmp> Write-Output hi\r\nhi\r\n", t0)
        .expect("记录 PTY 兜底");

    // 旧 helper 的 `O` 不携带 data：早退、无变更、不崩溃。
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &output_event_without_data("cmd-4"),
            t0
        )
        .expect("O")
    );

    // 无 `O` 覆盖：`D` 结束时把 PTY 兜底并入 transcript。
    assert!(apply_event(&mut registry, &id, &mut tracker, &end_event("cmd-4", 0), t0).expect("D"));

    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].status, TranscriptStatus::Done);
    assert_eq!(commands[0].exit_code, Some(0));
    assert_eq!(commands[0].source, Some(TranscriptSource::ShellIntegration));
    // PTY 兜底并入：输出等于清理后的 PTY 文本，而不是空/只有回显。
    assert_eq!(
        commands[0].output, "hi",
        "O 无 data 时命令输出应来自 PTY 兜底"
    );
    assert!(
        !commands[0].incomplete,
        "PTY 兜底完整并入时不得标 incomplete"
    );
}
