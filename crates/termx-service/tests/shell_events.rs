//! 本地带外 shell 事件通道：传输、token 校验、command-id 匹配与降级。

#![cfg(any(windows, unix))]

use std::{sync::Arc, time::Duration};

use termx_core::{MockFactory, Registry, TerminalId, format_rfc3339};
use termx_protocol::{CreateParams, Mode, TerminalLocator, TranscriptSource, TranscriptStatus};
use termx_service::shell_events::{
    ShellEvent, ShellEventChannel, ShellEventKind, ShellEventTracker, apply_event,
};
use time::OffsetDateTime;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    time::timeout,
};

/// 构造使用 mock 后端的 registry。
fn registry() -> Registry {
    Registry::with_factory(64, Arc::new(MockFactory))
}

/// 创建参数。
fn params() -> CreateParams {
    CreateParams {
        mode: Mode::Pty,
        cmd: "mock".to_string(),
        args: Vec::new(),
        cwd: None,
        env: None,
        cols: None,
        rows: None,
        root_session_id: "ses-1".to_string(),
        name: "build".to_string(),
        force: false,
        idle_timeout: None,
        ssh: None,
        allow_injection: false,
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

/// 构造事件（旧探针：不带命令元数据）。
fn event(kind: ShellEventKind, code: Option<i32>, id: &str) -> ShellEvent {
    event_with_meta(kind, code, id, None, None, None)
}

/// 构造带命令元数据的事件。
fn event_with_meta(
    kind: ShellEventKind,
    code: Option<i32>,
    id: &str,
    command: Option<&str>,
    started_at_ms: Option<i64>,
    ended_at_ms: Option<i64>,
) -> ShellEvent {
    ShellEvent {
        kind,
        code,
        id: Some(id.to_string()),
        command: command.map(str::to_string),
        started_at_ms,
        ended_at_ms,
        data: None,
        dropped: false,
    }
}

/// Unix 毫秒时间戳。
fn millis(at: OffsetDateTime) -> i64 {
    i64::try_from(at.unix_timestamp_nanos() / 1_000_000).expect("毫秒时间戳应在范围内")
}

/// 连接 channel 暴露的端点。
#[cfg(windows)]
async fn connect(endpoint: &str) -> Box<dyn AsyncWrite + Unpin + Send> {
    use tokio::net::windows::named_pipe::ClientOptions;

    let name = endpoint.strip_prefix("pipe:").expect("pipe 端点");
    let client = ClientOptions::new()
        .open(format!(r"\\.\pipe\{name}"))
        .expect("连接 named pipe");
    Box::new(client)
}

/// 连接 channel 暴露的端点。
#[cfg(unix)]
async fn connect(endpoint: &str) -> Box<dyn AsyncWrite + Unpin + Send> {
    let path = endpoint.strip_prefix("unix:").expect("unix 端点");
    let stream = tokio::net::UnixStream::connect(path)
        .await
        .expect("连接 Unix socket");
    Box::new(stream)
}

/// 写一行 JSON。
async fn send_line(stream: &mut Box<dyn AsyncWrite + Unpin + Send>, line: &str) {
    stream.write_all(line.as_bytes()).await.expect("写帧");
    stream.write_all(b"\n").await.expect("写换行");
    stream.flush().await.expect("flush");
}

#[tokio::test]
async fn channel_delivers_events_after_valid_token() {
    let channel = ShellEventChannel::open().expect("打开通道");
    let endpoint = channel.endpoint().to_string();
    let token = channel.token().to_string();
    let (mut receiver, _cancel) = channel.split();

    let mut stream = connect(&endpoint).await;
    send_line(&mut stream, &format!(r#"{{"v":1,"token":"{token}"}}"#)).await;
    send_line(
        &mut stream,
        r#"{"v":1,"event":"C","id":"cmd-1","command":"echo hi","started_at_ms":1700000000000}"#,
    )
    .await;
    send_line(
        &mut stream,
        r#"{"v":1,"event":"D","code":7,"id":"cmd-1","ended_at_ms":1700000001000}"#,
    )
    .await;

    let start = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("等待 C 超时")
        .expect("应收到 C");
    assert_eq!(start.kind, ShellEventKind::CommandStart);
    assert_eq!(start.id.as_deref(), Some("cmd-1"));
    assert_eq!(start.command.as_deref(), Some("echo hi"));
    assert_eq!(start.started_at_ms, Some(1_700_000_000_000));

    let end = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("等待 D 超时")
        .expect("应收到 D");
    assert_eq!(end.kind, ShellEventKind::CommandEnd);
    assert_eq!(end.code, Some(7));
    assert_eq!(end.id.as_deref(), Some("cmd-1"));
    assert_eq!(end.ended_at_ms, Some(1_700_000_001_000));
}

#[tokio::test]
async fn wrong_token_is_rejected() {
    let channel = ShellEventChannel::open().expect("打开通道");
    let endpoint = channel.endpoint().to_string();
    let (mut receiver, _cancel) = channel.split();

    let mut stream = connect(&endpoint).await;
    send_line(&mut stream, r#"{"v":1,"token":"not-the-token"}"#).await;

    let result = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("等待拒绝超时");
    assert!(result.is_none(), "错误 token 不应投递任何事件");
}

#[tokio::test]
async fn disconnect_degrades_without_events() {
    let channel = ShellEventChannel::open().expect("打开通道");
    let endpoint = channel.endpoint().to_string();
    let token = channel.token().to_string();
    let (mut receiver, _cancel) = channel.split();

    let mut stream = connect(&endpoint).await;
    send_line(&mut stream, &format!(r#"{{"v":1,"token":"{token}"}}"#)).await;
    drop(stream);

    let result = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("等待断开超时");
    assert!(result.is_none(), "断开后通道应结束，且不产生事件");
}

#[test]
fn late_command_end_with_stale_id_does_not_finish_new_command() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();
    let mut tracker = ShellEventTracker::new();

    registry
        .record_transcript_command(&id, "first", t0)
        .expect("记录第一条");
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandStart, None, "cmd-1"),
            t0,
        )
        .expect("C1")
    );
    assert!(
        apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandEnd, Some(0), "cmd-1"),
            t0,
        )
        .expect("D1")
    );

    // 新命令开始后，旧命令的迟到 D 必须被忽略。
    registry
        .record_transcript_command(&id, "second", t0 + time::Duration::seconds(1))
        .expect("记录第二条");
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandStart, None, "cmd-2"),
            t0,
        )
        .expect("C2")
    );
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandEnd, Some(0), "cmd-1"),
            t0,
        )
        .expect("迟到 D1 应被忽略")
    );

    let result = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(result.commands[1].command, "second");
    assert_eq!(result.commands[1].status, TranscriptStatus::Running);
    assert_eq!(result.commands[1].exit_code, None);

    // 与当前 C 匹配的 D 正常结束新命令。
    assert!(
        apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandEnd, Some(7), "cmd-2"),
            t0 + time::Duration::seconds(2),
        )
        .expect("D2")
    );
    let result = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(result.commands[1].status, TranscriptStatus::Done);
    assert_eq!(result.commands[1].exit_code, Some(7));
    assert_eq!(
        result.commands[1].source,
        Some(TranscriptSource::ShellIntegration)
    );
    assert!(!result.commands[1].best_effort);
}

#[test]
fn late_command_end_with_stale_generation_does_not_finish_new_command() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();
    let mut tracker = ShellEventTracker::new();

    registry
        .record_transcript_command(&id, "first", t0)
        .expect("记录第一条");
    // C1 绑定当时最后一条命令（代次 g1）。
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandStart, None, "cmd-1"),
            t0,
        )
        .expect("C1")
    );

    // 旧命令仍在运行时，service 已记录新命令（代次 g2）。
    registry
        .record_transcript_command(&id, "second", t0 + time::Duration::seconds(1))
        .expect("记录第二条");

    // 旧 C 的迟到 D：id 仍匹配 active C，但代次已过期，不得结束新命令。
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandEnd, Some(0), "cmd-1"),
            t0 + time::Duration::seconds(2),
        )
        .expect("迟到 D1 应被忽略")
    );
    let result = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(result.commands[1].command, "second");
    assert_eq!(result.commands[1].status, TranscriptStatus::Running);
    assert_eq!(result.commands[1].exit_code, None);

    // 新 C 绑定 g2，随后的 D 正常结束新命令。
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandStart, None, "cmd-2"),
            t0 + time::Duration::seconds(2),
        )
        .expect("C2")
    );
    assert!(
        apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandEnd, Some(7), "cmd-2"),
            t0 + time::Duration::seconds(3),
        )
        .expect("D2")
    );
    let result = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript");
    assert_eq!(result.commands[1].status, TranscriptStatus::Done);
    assert_eq!(result.commands[1].exit_code, Some(7));
    assert_eq!(
        result.commands[1].source,
        Some(TranscriptSource::ShellIntegration)
    );
}

#[test]
fn host_command_metadata_updates_send_record_without_duplicate() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let send_time = base();
    let host_start = send_time + time::Duration::seconds(2);
    let host_end = host_start + time::Duration::milliseconds(500);
    let mut tracker = ShellEventTracker::new();

    // `terminal.send` 先按发送时间记录，随后宿主 C 带精确命令与开始时间。
    registry
        .record_transcript_command(&id, "run", send_time)
        .expect("记录 send fallback");

    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event_with_meta(
                ShellEventKind::CommandStart,
                None,
                "cmd-1",
                Some("run --flag"),
                Some(millis(host_start)),
                None,
            ),
            send_time,
        )
        .expect("C")
    );
    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands.len(), 1, "C 应更新 send 记录而不是重复插入");
    assert_eq!(commands[0].command, "run --flag");
    assert_eq!(
        commands[0].started_at,
        format_rfc3339(host_start),
        "开始时间应来自宿主而不是 send"
    );

    assert!(
        apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event_with_meta(
                ShellEventKind::CommandEnd,
                Some(7),
                "cmd-1",
                None,
                None,
                Some(millis(host_end)),
            ),
            host_end,
        )
        .expect("D")
    );
    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].status, TranscriptStatus::Done);
    assert_eq!(commands[0].exit_code, Some(7));
    assert_eq!(
        commands[0].ended_at.as_deref(),
        Some(format_rfc3339(host_end).as_str()),
        "结束时间应来自宿主 ended_at_ms"
    );
}

#[test]
fn host_command_without_send_inserts_transcript_record() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let host_start = base() + time::Duration::seconds(3);
    let host_end = host_start + time::Duration::milliseconds(250);
    let mut tracker = ShellEventTracker::new();

    // raw 控制台输入：没有 terminal.send 记录，C 直接新建。
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event_with_meta(
                ShellEventKind::CommandStart,
                None,
                "cmd-raw",
                Some("run"),
                Some(millis(host_start)),
                None,
            ),
            base(),
        )
        .expect("C")
    );
    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].command, "run");
    assert_eq!(commands[0].started_at, format_rfc3339(host_start));

    assert!(
        apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event_with_meta(
                ShellEventKind::CommandEnd,
                Some(0),
                "cmd-raw",
                None,
                None,
                Some(millis(host_end)),
            ),
            host_end,
        )
        .expect("D")
    );
    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands[0].status, TranscriptStatus::Done);
    assert_eq!(
        commands[0].ended_at.as_deref(),
        Some(format_rfc3339(host_end).as_str())
    );
}

#[test]
fn legacy_command_start_without_command_still_binds_generation() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();
    let mut tracker = ShellEventTracker::new();

    registry
        .record_transcript_command(&id, "first", t0)
        .expect("记录命令");
    // 旧探针 C 没有 command/start：仍应认领 send 记录并允许 D 结束。
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandStart, None, "cmd-1"),
            t0,
        )
        .expect("C")
    );
    assert!(
        apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandEnd, Some(3), "cmd-1"),
            t0 + time::Duration::seconds(1),
        )
        .expect("D")
    );
    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].command, "first");
    assert_eq!(commands[0].status, TranscriptStatus::Done);
    assert_eq!(commands[0].exit_code, Some(3));
}

#[test]
fn command_start_without_command_or_pending_does_not_insert() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();
    let mut tracker = ShellEventTracker::new();

    // 没有 send 记录、C 也不带命令文本（旧探针/不完整 C）：不伪造命令记录。
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandStart, None, "cmd-1"),
            t0,
        )
        .expect("C")
    );
    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert!(commands.is_empty(), "不完整 C 不应插入记录");

    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandEnd, Some(0), "cmd-1"),
            t0,
        )
        .expect("D")
    );
}

#[test]
fn command_abort_cancels_pending_send_record() {
    let mut registry = registry();
    let created = registry.create(params()).expect("create");
    let id = TerminalId::new(created.terminal_id.clone());
    let t0 = base();
    let mut tracker = ShellEventTracker::new();

    registry
        .record_transcript_command(&id, "if ($true) {", t0)
        .expect("记录 send 命令");
    let abort = ShellEvent {
        kind: ShellEventKind::CommandAbort,
        code: None,
        id: None,
        command: None,
        started_at_ms: None,
        ended_at_ms: None,
        data: None,
        dropped: false,
    };
    assert!(
        !apply_event(&mut registry, &id, &mut tracker, &abort, t0).expect("X"),
        "X 只清理 pending，不算结束命令"
    );

    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert!(commands.is_empty(), "X 应删除未执行的 pending 记录");
}

#[test]
fn command_abort_clears_active_tracker() {
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
            &event_with_meta(
                ShellEventKind::CommandStart,
                None,
                "cmd-1",
                Some("Get-ChildItem"),
                None,
                None,
            ),
            t0,
        )
        .expect("C")
    );
    assert_eq!(tracker.active_command_id(), Some("cmd-1"));

    let abort = ShellEvent {
        kind: ShellEventKind::CommandAbort,
        code: None,
        id: None,
        command: None,
        started_at_ms: None,
        ended_at_ms: None,
        data: None,
        dropped: false,
    };
    assert!(!apply_event(&mut registry, &id, &mut tracker, &abort, t0).expect("X"));
    assert_eq!(tracker.active_command_id(), None, "X 应清空 active");

    // active 已清空：迟到的 D 不再结束命令。
    assert!(
        !apply_event(
            &mut registry,
            &id,
            &mut tracker,
            &event(ShellEventKind::CommandEnd, Some(0), "cmd-1"),
            t0,
        )
        .expect("D")
    );
    let commands = registry
        .transcript(&by_id(&created.terminal_id), None)
        .expect("读取 transcript")
        .commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].status, TranscriptStatus::Running);
}
