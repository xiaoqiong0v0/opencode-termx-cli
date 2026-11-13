//! service 侧的 shell 带外事件薄层。
//!
//! 传输、token 校验与 JSON Lines 帧解析在 [`termx_shell_events`] 中；本模块
//! 只保留与 registry 相关的判定逻辑：
//! - `C` 绑定当时最后一条 transcript 命令的代次；
//! - `O` 只有 command-id 与当前 active 一致时解码 base64/UTF-8 并追加到该
//!   代次命令，不生成 xterm 输出；
//! - `D` 只有 command-id 与代次都匹配时才结束 transcript，避免旧命令的迟到
//!   `D` 关掉新命令；
//! - 通道失败/断开只让 transcript 退化为静默判定，不伪造退出码。

use base64::{Engine as _, engine::general_purpose::STANDARD};
use termx_core::{CoreError, Registry, TerminalId};
use time::OffsetDateTime;

pub use termx_shell_events::{
    CONNECT_TIMEOUT, EVENT_ENDPOINT_ENV, EVENT_QUEUE_CAPACITY, EVENT_TOKEN_ENV, HANDSHAKE_TIMEOUT,
    MAX_FRAME_BYTES, ShellEvent, ShellEventCancel, ShellEventChannel, ShellEventKind,
};

/// 当前 `C` 绑定的命令身份：command-id 与当时的 transcript 代次。
#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveCommand {
    id: String,
    generation: u64,
}

/// 记录当前 `C` 的 command-id 与代次，并用它们校验后续 `D`。
#[derive(Debug, Default)]
pub struct ShellEventTracker {
    active: Option<ActiveCommand>,
}

impl ShellEventTracker {
    /// 创建空跟踪器。
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前 active command-id。
    pub fn active_command_id(&self) -> Option<&str> {
        self.active.as_ref().map(|active| active.id.as_str())
    }

    /// 当前 active command 绑定的 transcript 代次。
    pub fn active_generation(&self) -> Option<u64> {
        self.active.as_ref().map(|active| active.generation)
    }

    /// 观察事件；只有与当前 `C` 匹配的 `D` 返回 `(代次, 退出码)`。
    ///
    /// `C` 用 `current_generation` 绑定“当时最后一条”transcript 命令；
    /// `D` 只有 command-id 匹配才生效，返回绑定的代次供 registry 二次校验，
    /// 避免旧命令的迟到 `D` 结束后来记录的新命令。没有 active、缺少 id
    /// 或不匹配的 `D` 都返回 `None`。
    pub fn observe(
        &mut self,
        event: &ShellEvent,
        current_generation: u64,
    ) -> Option<(u64, Option<i32>)> {
        match event.kind {
            ShellEventKind::CommandStart => {
                self.active = event.id.clone().map(|id| ActiveCommand {
                    id,
                    generation: current_generation,
                });
                None
            }
            ShellEventKind::CommandEnd => {
                let active = self.active.as_ref()?;
                if event.id.as_deref() != Some(active.id.as_str()) {
                    return None;
                }
                let generation = active.generation;
                self.active = None;
                Some((generation, event.code))
            }
            ShellEventKind::PromptStart | ShellEventKind::PromptEnd => None,
            // `O` 不改变 active 命令；匹配与追加由 `apply_event` 处理。
            ShellEventKind::CommandOutput => None,
            // `X`：多行输入未执行，丢弃当前 active，避免迟到的 `D` 误结束。
            ShellEventKind::CommandAbort => {
                self.active = None;
                None
            }
        }
    }
}

/// 把一条事件应用到 registry；返回是否真的结束了 transcript 命令。
///
/// 供 service 摄取任务与测试共用：
/// - `C` 优先认领 `terminal.send` 留下的 pending 代次并原地更新
///   command/started_at；没有 pending 时用宿主命令文本新建记录；两者都没有
///   （旧探针且无命令文本）时只绑定当前代次，不伪造记录；
/// - `D` 按 command-id 与代次双重匹配，只有都命中才结束命令，并使用宿主
///   `ended_at_ms`（缺省用 `now`）。
pub fn apply_event(
    registry: &mut Registry,
    terminal_id: &TerminalId,
    tracker: &mut ShellEventTracker,
    event: &ShellEvent,
    now: OffsetDateTime,
) -> Result<bool, CoreError> {
    match event.kind {
        ShellEventKind::CommandStart => {
            apply_command_start(registry, terminal_id, tracker, event, now)
        }
        ShellEventKind::CommandEnd => {
            let Some((generation, exit_code)) = tracker.observe(event, 0) else {
                return Ok(false);
            };
            let ended_at = event.ended_at_ms.and_then(from_unix_millis).unwrap_or(now);
            let ended = registry.record_transcript_end_for_generation_at(
                terminal_id,
                exit_code,
                generation,
                ended_at,
            )?;
            if event.dropped {
                // 宿主队列丢弃过 O 帧：文本可能缺失，但退出码/状态保持真实。
                // 已由静默判定结束的记录也要补上标记。
                registry.mark_transcript_incomplete_for_generation(terminal_id, generation)?;
            }
            Ok(ended)
        }
        ShellEventKind::PromptStart | ShellEventKind::PromptEnd => Ok(false),
        ShellEventKind::CommandOutput => {
            apply_command_output(registry, terminal_id, tracker, event, now)
        }
        // `X`：撤销 `terminal.send` 留下但未执行的 pending 记录。
        ShellEventKind::CommandAbort => {
            let _ = tracker.observe(event, 0);
            registry.cancel_pending_transcript(terminal_id)?;
            Ok(false)
        }
    }
}

/// 处理 `O`：只有 command-id 与当前 active 一致时把输出追加到该命令。
///
/// `O` 与 `C`/`D` 走同一条 FIFO 通道；这里再校验 command-id 与代次，避免
/// 旧命令或伪造事件污染新命令。base64/UTF-8 解码失败按无变更处理。
fn apply_command_output(
    registry: &mut Registry,
    terminal_id: &TerminalId,
    tracker: &ShellEventTracker,
    event: &ShellEvent,
    now: OffsetDateTime,
) -> Result<bool, CoreError> {
    let Some(active_id) = tracker.active_command_id() else {
        return Ok(false);
    };
    if event.id.as_deref() != Some(active_id) {
        return Ok(false);
    }
    let Some(generation) = tracker.active_generation() else {
        return Ok(false);
    };
    let Some(data) = event.data.as_deref() else {
        return Ok(false);
    };
    let Ok(bytes) = STANDARD.decode(data) else {
        return Ok(false);
    };
    let Ok(text) = String::from_utf8(bytes) else {
        return Ok(false);
    };
    registry.record_transcript_output_for_generation(terminal_id, generation, &text, now)
}

/// 处理 `C`：认领 pending 或新建记录，并绑定代次供后续 `D` 使用。
fn apply_command_start(
    registry: &mut Registry,
    terminal_id: &TerminalId,
    tracker: &mut ShellEventTracker,
    event: &ShellEvent,
    now: OffsetDateTime,
) -> Result<bool, CoreError> {
    let started_at = event.started_at_ms.and_then(from_unix_millis);
    let generation = match registry.take_pending_transcript_generation(terminal_id)? {
        // `terminal.send` 已记录：原地更新命令与开始时间，不重复插入。
        Some(generation) => {
            registry.update_transcript_start(
                terminal_id,
                generation,
                event.command.as_deref(),
                started_at,
            )?;
            generation
        }
        None => match event
            .command
            .as_deref()
            .filter(|command| !command.trim().is_empty())
        {
            // raw 控制台输入：没有 send 记录，用宿主数据新建命令。
            Some(command) => registry.record_transcript_command_at(
                terminal_id,
                command,
                started_at.unwrap_or(now),
            )?,
            // 旧探针 / 不完整 `C`：没有命令文本时不伪造记录。
            None => registry.current_transcript_generation(terminal_id)?,
        },
    };
    tracker.observe(event, generation);
    Ok(false)
}

/// 把 Unix 毫秒时间戳转换为 UTC 时间；越界值返回 `None`。
fn from_unix_millis(millis: i64) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis).checked_mul(1_000_000)?).ok()
}
