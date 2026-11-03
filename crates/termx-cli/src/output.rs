//! 事件与结果的输出格式化。
//!
//! JSON 模式把事件/结果压成单行 JSON；人类模式把 `output` 按原始字节写到
//! stdout/stderr（不二次编码），其余事件作为元信息写到 stderr。

use std::io::{self, Write};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::Serialize;
use termx_protocol::{Event, EventKind, ListItem, Stream, TerminalSnapshot};

use crate::error::CliError;

/// 终端终态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalEnd {
    /// 自然退出，携带子进程退出码。
    Exited(i32),
    /// 被信号/控制中断。
    Interrupted,
    /// 失败终态。
    Error,
}

impl TerminalEnd {
    /// 映射为 CLI 退出码：子进程退出码原样保留，中断用 130（128+SIGINT）。
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Exited(code) if (0..=255).contains(code) => *code,
            Self::Exited(_) => 1,
            Self::Interrupted => 130,
            Self::Error => 1,
        }
    }
}

/// 事件输出器。
pub struct Printer {
    /// 是否输出单行 JSON。
    json: bool,
    /// 人类模式下是否把非 output 事件写到 stderr。
    meta_to_stderr: bool,
}

impl Printer {
    /// 构造输出器。
    pub fn new(json: bool, meta_to_stderr: bool) -> Self {
        Self {
            json,
            meta_to_stderr,
        }
    }

    /// 输出一个事件；遇到终态时返回对应 [`TerminalEnd`]。
    pub fn render_event(&self, event: &Event) -> Result<Option<TerminalEnd>, CliError> {
        if self.json {
            print_json(event)?;
            return Ok(terminal_end(&event.kind));
        }

        if let EventKind::Output { data, stream } = &event.kind {
            let bytes = BASE64_STANDARD
                .decode(data)
                .map_err(|error| CliError::Usage(format!("output 不是合法 base64: {error}")))?;
            write_stream(*stream, &bytes)?;
            return Ok(None);
        }

        if self.meta_to_stderr {
            self.render_meta(event)?;
        }
        Ok(terminal_end(&event.kind))
    }

    /// 人类模式元信息输出。
    fn render_meta(&self, event: &Event) -> Result<(), CliError> {
        let mut err = io::stderr().lock();
        let seq = event.seq;
        match &event.kind {
            EventKind::Started { pid, pgid, mode } => {
                writeln!(
                    err,
                    "[{seq}] started pid={pid} pgid={pgid} mode={}",
                    scalar(mode)
                )?;
            }
            EventKind::Output { .. } => {}
            EventKind::OutputDropped { from_seq, to_seq } => {
                writeln!(err, "[{seq}] output_dropped {from_seq}..={to_seq}")?;
            }
            EventKind::WaitingInput { confidence, reason } => {
                writeln!(
                    err,
                    "[{seq}] waiting_input confidence={} reason={reason}",
                    scalar(confidence)
                )?;
            }
            EventKind::InteractiveChanged {
                active,
                pid,
                command,
                confidence,
                ..
            } => {
                writeln!(
                    err,
                    "[{seq}] interactive_changed active={active} pid={} command={} confidence={}",
                    option_u32(*pid),
                    command.as_deref().unwrap_or("-"),
                    scalar(confidence)
                )?;
            }
            EventKind::Attached { client_id } => {
                writeln!(err, "[{seq}] attached client_id={client_id}")?;
            }
            EventKind::Detached { client_id, reason } => {
                writeln!(
                    err,
                    "[{seq}] detached client_id={client_id} reason={reason}"
                )?;
            }
            EventKind::LeaseRevoked { client_id } => {
                writeln!(err, "[{seq}] lease_revoked client_id={client_id}")?;
            }
            EventKind::Exited { exit_code } => {
                writeln!(err, "[{seq}] exited exit_code={exit_code}")?;
            }
            EventKind::Interrupted { signal, source } => {
                writeln!(err, "[{seq}] interrupted signal={signal} source={source}")?;
            }
            EventKind::Error {
                code,
                message,
                retryable,
            } => {
                writeln!(
                    err,
                    "[{seq}] error code={} message={message} retryable={retryable}",
                    scalar(code)
                )?;
            }
            EventKind::Capabilities {
                helper,
                version,
                best_effort,
            } => {
                writeln!(
                    err,
                    "[{seq}] capabilities helper={helper} version={version} best_effort={best_effort}"
                )?;
            }
        }
        err.flush()?;
        Ok(())
    }
}

/// 打印单行 JSON 结果。
pub fn print_json<T: Serialize>(value: &T) -> Result<(), CliError> {
    let line = serde_json::to_string(value)
        .map_err(|error| CliError::Usage(format!("JSON 序列化失败: {error}")))?;
    let mut out = io::stdout().lock();
    writeln!(out, "{line}")?;
    out.flush()?;
    Ok(())
}

/// 把原始字节写到 stdout（`read` 等人读模式使用）。
pub fn write_stdout(bytes: &[u8]) -> Result<(), CliError> {
    let mut out = io::stdout().lock();
    out.write_all(bytes)?;
    out.flush()?;
    Ok(())
}

/// 人类模式打印状态快照。
pub fn print_snapshot(snapshot: &TerminalSnapshot) -> Result<(), CliError> {
    let mut out = io::stdout().lock();
    writeln!(out, "terminal_id: {}", snapshot.terminal_id)?;
    writeln!(out, "state: {}", scalar(&snapshot.state))?;
    writeln!(out, "mode: {}", scalar(&snapshot.mode))?;
    writeln!(out, "pid: {}", option_u32(snapshot.pid))?;
    writeln!(out, "pgid: {}", option_u32(snapshot.pgid))?;
    writeln!(out, "ppid: {}", option_u32(snapshot.ppid))?;
    writeln!(
        out,
        "foreground_pgrp: {}",
        option_u32(snapshot.foreground_pgrp)
    )?;
    writeln!(out, "interactive: {}", scalar(&snapshot.interactive))?;
    writeln!(out, "confidence: {}", scalar(&snapshot.confidence))?;
    writeln!(out, "cols: {} rows: {}", snapshot.cols, snapshot.rows)?;
    writeln!(out, "cursor: {}", snapshot.cursor)?;
    for node in &snapshot.tree {
        writeln!(
            out,
            "  tree pid={} ppid={} pgid={} cmd={}",
            node.pid, node.ppid, node.pgid, node.cmd
        )?;
    }
    out.flush()?;
    Ok(())
}

/// 人类模式打印终端列表。
pub fn print_list(items: &[ListItem]) -> Result<(), CliError> {
    let mut out = io::stdout().lock();
    if items.is_empty() {
        writeln!(out, "（无终端）")?;
        out.flush()?;
        return Ok(());
    }

    writeln!(
        out,
        "{:<26} {:<16} {:<14} {:<6} {:<8} {:<10}",
        "TERMINAL_ID", "ROOT", "NAME", "STATE", "MODE", "PID"
    )?;
    for item in items {
        writeln!(
            out,
            "{:<26} {:<16} {:<14} {:<6} {:<8} {:<10}",
            item.terminal_id,
            item.root_session_id,
            item.name,
            scalar(&item.state),
            scalar(&item.mode),
            option_u32(item.pid),
        )?;
    }
    out.flush()?;
    Ok(())
}

/// 把事件类型映射为终态。
fn terminal_end(kind: &EventKind) -> Option<TerminalEnd> {
    match kind {
        EventKind::Exited { exit_code } => Some(TerminalEnd::Exited(*exit_code)),
        EventKind::Interrupted { .. } => Some(TerminalEnd::Interrupted),
        EventKind::Error { .. } => Some(TerminalEnd::Error),
        _ => None,
    }
}

/// 按输出流写到 stdout/stderr。
fn write_stream(stream: Stream, bytes: &[u8]) -> Result<(), CliError> {
    match stream {
        Stream::Stderr => {
            let mut err = io::stderr().lock();
            err.write_all(bytes)?;
            err.flush()?;
        }
        Stream::Pty | Stream::Stdout => {
            let mut out = io::stdout().lock();
            out.write_all(bytes)?;
            out.flush()?;
        }
    }
    Ok(())
}

/// 取枚举的 snake_case 文本；不可序列化时退回 Debug。
fn scalar<T: Serialize + std::fmt::Debug>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(text)) => text,
        Ok(other) => other.to_string(),
        Err(_) => format!("{value:?}"),
    }
}

/// 可选 PID 的人类可读形式。
fn option_u32(value: Option<u32>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}
