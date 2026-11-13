//! 每终端有界命令记录：命令、起止时间、状态与清洗后的输出。
//!
//! 与事件缓冲分开：本模块只服务信息页与模型读取入口，不参与事件投递、
//! 背压与订阅回放。容量按命令条数淘汰最旧记录；输出在读取时统一清洗
//! （去 ANSI、命令回显与末尾提示符）。

use std::collections::VecDeque;
use std::time::Duration;

use termx_protocol::{TranscriptCommand, TranscriptResult, TranscriptSource, TranscriptStatus};
use termx_terminal::{OscEvent, OscScanner};
use time::OffsetDateTime;

use crate::output::{normalize_controls, strip_ansi};

/// 默认 transcript 容量（命令条数）。
pub const DEFAULT_TRANSCRIPT_CAPACITY: usize = 200;

/// 默认静默判定阈值：超过该时间没有新输出即认为当前命令结束。
pub const DEFAULT_QUIET_FINALIZE: Duration = Duration::from_millis(1000);

/// 单条命令保留的最大输出字节数（64 KiB，含截断标记）。
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;

/// 输出超过上限时追加一次的可读标记。
pub const OUTPUT_TRUNCATED_MARKER: &str = "\n[output truncated]\n";

/// 单条命令的内部记录（时间保留为可计算类型）。
#[derive(Debug, Clone)]
struct CommandRecord {
    /// 命令文本（已去掉末尾 CR/LF）。
    command: String,
    /// 开始时间。
    started_at: OffsetDateTime,
    /// 结束时间；运行中为 `None`。
    ended_at: Option<OffsetDateTime>,
    /// 当前状态。
    status: TranscriptStatus,
    /// 原始输出（保留 ANSI，读取时清洗）。
    output: String,
    /// PTY 兜底输出：仅在命令没有任何 `O` 事件时于结束时并入 `output`。
    /// 用于 native/直接 console 命令；有 `O` 时丢弃，避免双写。
    pty_output: String,
    /// 当前命令是否已收到 `O` 输出事件。
    event_output_seen: bool,
    /// 输出是否已因超过上限被截断（只追加一次标记）。
    truncated: bool,
    /// 最后一次收到输出的时间；用于静默判定。
    last_output_at: OffsetDateTime,
    /// 命令退出码；未结束或无法确定时为 `None`。
    exit_code: Option<i32>,
    /// 结束证据来源；未结束时为 `None`。
    source: Option<TranscriptSource>,
    /// 结束判定是否为 best-effort（静默推断等）。
    best_effort: bool,
    /// 输出文本是否已知不完整（混合 host/native、队列丢弃或屏障失败）。
    ///
    /// 只描述“可能缺少文本”，不改变状态、退出码或 `best_effort`。
    output_incomplete: bool,
    /// PTY 兜底因超过上限被截断；结束时据此标记可能缺文本。
    pty_output_overflowed: bool,
    /// 单调递增的命令代次；用于带外事件按代次结束命令。
    generation: u64,
}

impl CommandRecord {
    /// 转换为协议结构，并在读取时清洗输出。
    fn to_protocol(&self) -> TranscriptCommand {
        TranscriptCommand {
            command: self.command.clone(),
            started_at: crate::format_rfc3339(self.started_at),
            ended_at: self.ended_at.map(crate::format_rfc3339),
            status: self.status,
            output: clean_output(&self.command, &self.output),
            exit_code: self.exit_code,
            source: self.source,
            best_effort: self.best_effort,
            incomplete: self.output_incomplete,
        }
    }
}

/// 单终端有界命令记录缓冲。
#[derive(Debug, Clone)]
pub struct TranscriptBuffer {
    /// 最大保留命令条数（至少为 1）。
    capacity: usize,
    /// 每次变更递增的修订号。
    revision: u64,
    /// 最近一次命令分配到的代次；无命令时为 0。
    last_generation: u64,
    /// `terminal.send` 记录但尚未被宿主的 `C` 事件认领的代次。
    pending_generation: Option<u64>,
    /// 已保留命令，按时间升序。
    commands: VecDeque<CommandRecord>,
    /// 当前命令输出的 OSC 133/633 增量扫描器（跨输出分块保持状态）。
    scanner: OscScanner,
    /// 是否解析 stdout 上的 in-band OSC 133/633。
    ///
    /// 使用带外事件通道的运行时（如 `termx-pwsh`/`termx-bash`）必须关闭该
    /// 开关，否则子程序仍可通过 stdout 伪造命令结束。
    in_band_osc: bool,
    /// 真实 D 之后仍允许把输出追加到最后一条命令。
    ///
    /// 仅用于宿主无法保证 D 在最后一段输出之后的平台（当前 Windows
    /// ConPTY 的 `termx-pwsh`）；命令状态仍保持 `done`，只追加迟到输出。
    allow_late_output: bool,
    /// transcript 文本是否权威地来自带外 `O` 事件。
    ///
    /// `true` 时命令只由宿主 `D` 结束：静默判定完全不生效，避免 1s 无输出
    /// 就把仍在运行的长命令标成 `done`（影长命令需等到 `D` 才返回输出）；
    /// 兜底只是“没有 O 时”的补充，也不提前提升。事件通道死亡后由 service
    /// 调回 `false`，让后续 idle tick 能提升 PTY 兜底。默认 `false`。
    output_from_events: bool,
}

impl TranscriptBuffer {
    /// 以给定容量创建；容量为 0 时按 1 处理。
    ///
    /// 默认解析 stdout 上的 in-band OSC 133/633，保持既有 shell 行为。
    pub fn new(capacity: usize) -> Self {
        Self::with_in_band_osc(capacity, true)
    }

    /// 以给定容量与 in-band OSC 开关创建；容量为 0 时按 1 处理。
    ///
    /// `in_band_osc=false` 时 `record_output` 不再把输出喂给 OSC 扫描器，
    /// 事件只能来自带外通道。
    pub fn with_in_band_osc(capacity: usize, in_band_osc: bool) -> Self {
        Self::with_in_band_osc_and_late_output(capacity, in_band_osc, false)
    }

    /// 以容量、in-band OSC 开关与迟到输出策略创建。
    ///
    /// `allow_late_output=true` 时，真实 D 之后的输出仍追加到最后一条
    /// 命令；命令状态不回到 running，只刷新输出内容。
    pub fn with_in_band_osc_and_late_output(
        capacity: usize,
        in_band_osc: bool,
        allow_late_output: bool,
    ) -> Self {
        Self {
            capacity: capacity.max(1),
            revision: 0,
            last_generation: 0,
            pending_generation: None,
            commands: VecDeque::new(),
            scanner: OscScanner::new(),
            in_band_osc,
            allow_late_output,
            output_from_events: false,
        }
    }

    /// 设置 transcript 文本是否权威地来自带外 `O` 事件；返回自身便于链式构造。
    ///
    /// 默认 `false`（PTY/OSC 路径）；`termx-pwsh` 本地带外事件终端传 `true`。
    pub fn with_output_from_events(mut self, output_from_events: bool) -> Self {
        self.output_from_events = output_from_events;
        self
    }

    /// 当前 transcript 文本是否权威地来自带外 `O` 事件。
    pub fn output_from_events(&self) -> bool {
        self.output_from_events
    }

    /// 切换 transcript 文本是否权威地来自带外 `O` 事件。
    ///
    /// 事件通道断开/失败后由 service 切回 `false`，让静默判定重新可以提升
    /// `C..D` 期间收集的 PTY 兜底。不改变已有记录，也不递增修订号；返回是否
    /// 发生变更。
    pub fn set_output_from_events(&mut self, output_from_events: bool) -> bool {
        if self.output_from_events == output_from_events {
            return false;
        }
        self.output_from_events = output_from_events;
        true
    }

    /// 当前是否解析 in-band OSC。
    pub fn in_band_osc(&self) -> bool {
        self.in_band_osc
    }

    /// 开始一条新命令：上一条仍在运行的命令先标记为 `done`。
    ///
    /// 每次分配一个新的单调代次，供 [`Self::record_command_end_for_generation`]
    /// 精确结束“当时最后一条”命令。返回变更后的修订号。
    ///
    /// 该记录同时登记为 pending：紧随其后的宿主 `C` 事件应通过
    /// [`Self::take_pending_generation`] 认领它并更新命令/开始时间，而不是
    /// 再插入一条重复记录。
    pub fn record_command(&mut self, command: &str, now: OffsetDateTime) -> u64 {
        let generation = self.push_command(command, now);
        self.pending_generation = Some(generation);
        self.revision
    }

    /// 记录宿主 `C` 事件新建的命令（没有 `terminal.send` 前置记录）。
    ///
    /// 返回新记录的命令代次，供 `D` 事件精确结束该命令。
    pub fn record_command_at(&mut self, command: &str, now: OffsetDateTime) -> u64 {
        self.push_command(command, now)
    }

    /// 取出并清除 `terminal.send` 留下的 pending 代次；无 pending 时返回 `None`。
    pub fn take_pending_generation(&mut self) -> Option<u64> {
        self.pending_generation.take()
    }

    /// 撤销 pending 命令记录（多行输入未执行时使用）。
    ///
    /// 仅当 `pending_generation` 指向最后一条记录、该记录仍为 `running` 且
    /// 尚无 `ended_at` 时删除它并清除 pending，同时递增修订号；其余情况
    /// 保持不变。返回是否删除了记录。
    pub fn cancel_pending(&mut self) -> bool {
        let Some(pending) = self.pending_generation else {
            return false;
        };
        let Some(last) = self.commands.back() else {
            return false;
        };
        if last.generation != pending
            || last.status != TranscriptStatus::Running
            || last.ended_at.is_some()
        {
            return false;
        }
        self.commands.pop_back();
        self.pending_generation = None;
        self.bump();
        true
    }

    /// 按代次更新已记录命令的文本与开始时间（宿主 `C` 事件携带的值）。
    ///
    /// `command`/`started_at` 为 `None` 时保持原值（旧探针不填这些字段）。
    /// 仅当记录仍存在且字段确实变化时才递增修订号；返回是否发生变更。
    pub fn update_command_for_generation(
        &mut self,
        generation: u64,
        command: Option<&str>,
        started_at: Option<OffsetDateTime>,
    ) -> bool {
        let Some(record) = self
            .commands
            .iter_mut()
            .find(|record| record.generation == generation)
        else {
            return false;
        };
        let mut changed = false;
        if let Some(command) = command {
            let command = command.trim_end_matches(['\r', '\n']);
            if record.command != command {
                record.command = command.to_string();
                changed = true;
            }
        }
        if let Some(started_at) = started_at {
            if record.started_at != started_at {
                record.started_at = started_at;
                changed = true;
            }
        }
        if changed {
            self.bump();
        }
        changed
    }

    /// 插入一条新命令并分配代次；返回代次。
    fn push_command(&mut self, command: &str, now: OffsetDateTime) -> u64 {
        // 上一条命令可能留下半截 OSC；新命令从干净状态开始扫描。
        self.scanner.reset();
        if let Some(last) = self.commands.back_mut() {
            if last.status == TranscriptStatus::Running {
                last.status = TranscriptStatus::Done;
                last.ended_at = Some(now);
            }
        }
        self.last_generation = self.last_generation.saturating_add(1);
        self.commands.push_back(CommandRecord {
            command: command.trim_end_matches(['\r', '\n']).to_string(),
            started_at: now,
            ended_at: None,
            status: TranscriptStatus::Running,
            output: String::new(),
            pty_output: String::new(),
            event_output_seen: false,
            truncated: false,
            last_output_at: now,
            exit_code: None,
            source: None,
            best_effort: false,
            output_incomplete: false,
            pty_output_overflowed: false,
            generation: self.last_generation,
        });
        while self.commands.len() > self.capacity {
            self.commands.pop_front();
        }
        self.bump();
        self.last_generation
    }

    /// 追加一段输出到当前（最后一条）命令。
    ///
    /// 终态命令不再接收输出；静默判定为 `done` 的命令在被追加输出时
    /// 重新回到 `running`。空文本或尚无命令时忽略。单条输出最多保留
    /// [`MAX_OUTPUT_BYTES`] 字节：超限时按 UTF-8 字符边界截断并追加一次
    /// [`OUTPUT_TRUNCATED_MARKER`]，后续输出不再增长。
    ///
    /// 若启用 in-band OSC（默认），输出同时喂给 OSC 扫描器：识别到
    /// `133;D;<exit_code>` / `633;D;<exit_code>` 时立即以 `shell_integration`
    /// 结束当前命令。marker 本身仍进入原始输出，读取时由 [`clean_output`]
    /// 的 ANSI 清洗去掉。使用带外事件通道的终端应关闭 in-band OSC。
    pub fn record_output(&mut self, text: &str, now: OffsetDateTime) -> bool {
        if text.is_empty() {
            return false;
        }
        let command_end = self
            .in_band_osc
            .then(|| {
                self.scanner
                    .push(text.as_bytes())
                    .into_iter()
                    .rev()
                    .find_map(|event| match event {
                        OscEvent::CommandEnd { exit_code, .. } => Some(exit_code),
                        _ => None,
                    })
            })
            .flatten();

        let output_changed = self.append_output(text, now);
        let end_applied = command_end.is_some_and(|exit_code| {
            self.record_command_end(exit_code, TranscriptSource::ShellIntegration, now)
        });
        if end_applied {
            return true;
        }
        if output_changed {
            self.bump();
            return true;
        }
        false
    }

    /// 按代次追加一段输出（带外 `O` 事件使用）。
    ///
    /// 只有当前最后一条命令的 generation 匹配时才追加，避免迟到/跨命令的
    /// 输出污染新命令。不解析 in-band OSC；`O` 的文本已是命令输出本身。
    /// 空文本、代次不匹配或命令已真实结束时忽略。返回是否发生变更。
    ///
    /// 若此前已收集过 PTY 兜底文本，先保留到命令结束再做内容比对：
    /// 纯 host 命令的 PTY 文本只是 `O` 的回显副本，不应误报；只有兜底里
    /// 存在 `O` 未覆盖的可见行（native 段）才在结束时标记 `incomplete`。
    pub fn record_output_for_generation(
        &mut self,
        generation: u64,
        text: &str,
        now: OffsetDateTime,
    ) -> bool {
        if text.is_empty() {
            return false;
        }
        let Some(last) = self.commands.back() else {
            return false;
        };
        if last.generation != generation {
            return false;
        }
        if let Some(last) = self.commands.back_mut() {
            // 不再清空 pty_output：结束时用它和 O 文本比对，区分纯 host
            // 回显与真正的 native 段。
            last.event_output_seen = true;
        }
        if self.append_output(text, now) {
            self.bump();
            true
        } else {
            false
        }
    }

    /// 收集 PTY 输出作为兜底：仅当命令最终没有任何 `O` 时，在内结束
    /// 时并入 transcript。用于 native/直接 console 文本；有 O 时会在结束
    /// 时与 O 文本比对，只有 O 未覆盖的可见行才标记 `incomplete`。
    ///
    /// 不递增 revision、不改变状态；prompt/D 后的输出不会命中 running 命令。
    /// 兜底同样受 [`MAX_OUTPUT_BYTES`] 限制；溢出会记住，结束时视为可能缺文本。
    pub fn record_pty_output_for_current(&mut self, text: &str, now: OffsetDateTime) -> bool {
        if text.is_empty() {
            return false;
        }
        let Some(last) = self.commands.back_mut() else {
            return false;
        };
        if last.status != TranscriptStatus::Running {
            return false;
        }
        last.last_output_at = now;
        if last.pty_output.len() >= MAX_OUTPUT_BYTES {
            last.pty_output_overflowed = true;
            return false;
        }
        let remaining = MAX_OUTPUT_BYTES.saturating_sub(last.pty_output.len());
        let take = floor_char_boundary(text, remaining.min(text.len()));
        if take < text.len() {
            last.pty_output_overflowed = true;
        }
        last.pty_output.push_str(&text[..take]);
        false
    }

    /// 把当前（最后一条）命令标记为输出不完整；返回是否发生变更。
    ///
    /// 只翻转 `incomplete` 标记并递增修订号，不改变状态、退出码或
    /// `best_effort`；重复标记不再计费。
    pub fn mark_output_incomplete(&mut self) -> bool {
        let Some(last) = self.commands.back_mut() else {
            return false;
        };
        if last.output_incomplete {
            return false;
        }
        last.output_incomplete = true;
        self.bump();
        true
    }

    /// 按代次把命令标记为输出不完整；代次不匹配时无变更。
    ///
    /// 供 `D` 携带的 `dropped` 与屏障失败使用：命令可能已经结束，标记仍要
    /// 落到同一条记录上；不伪造退出码、不改变 `best_effort`。
    pub fn mark_output_incomplete_for_generation(&mut self, generation: u64) -> bool {
        let Some(last) = self.commands.back_mut() else {
            return false;
        };
        if last.generation != generation || last.output_incomplete {
            return false;
        }
        last.output_incomplete = true;
        self.bump();
        true
    }

    /// 把输出追加到当前命令并刷新活动时间；不改变修订号。
    fn append_output(&mut self, text: &str, now: OffsetDateTime) -> bool {
        let Some(last) = self.commands.back_mut() else {
            return false;
        };
        if matches!(
            last.status,
            TranscriptStatus::Exited | TranscriptStatus::Interrupted | TranscriptStatus::Error
        ) || (last.status == TranscriptStatus::Done
            && !last.best_effort
            && !self.allow_late_output)
        {
            // 真实结束（shell-integration D）之后到达的输出属于
            // 下一个 prompt/续行，不应复活上一条命令；只有静默
            // best-effort 结束或显式允许迟到输出时例外。
            return false;
        }
        if last.truncated {
            // 已截断：只刷新活动时间，内容不再增长。
            last.last_output_at = now;
            return revive_done(last);
        }

        append_output_limited(last, text);
        last.last_output_at = now;
        revive_done(last);
        true
    }

    /// 以结构化命令结束事件结束当前命令。
    ///
    /// 当前（最后一条）命令处于 `running` 时生效：写入 `done`、
    /// `ended_at=now`、`exit_code` 与结束证据来源，返回是否发生变更。
    /// 已被静默判定结束（`best_effort=true`）的命令允许被真实 marker 覆盖；
    /// 其他终态不重复结束，也不覆盖既有记录。
    pub fn record_command_end(
        &mut self,
        exit_code: Option<i32>,
        source: TranscriptSource,
        now: OffsetDateTime,
    ) -> bool {
        let Some(last) = self.commands.back_mut() else {
            return false;
        };
        let changed = finish_command(last, exit_code, source, now);
        if changed {
            self.bump();
        }
        changed
    }

    /// 按代次结束命令：仅当当前最后一条命令的 generation 匹配时生效。
    ///
    /// 供带外事件通道使用：`C` 事件绑定“当时最后一条命令”的代次，`D` 只
    /// 结束该代次，避免旧命令的迟到 `D` 结束后来记录的新命令。结束语义与
    /// [`Self::record_command_end`] 完全一致。
    pub fn record_command_end_for_generation(
        &mut self,
        exit_code: Option<i32>,
        source: TranscriptSource,
        generation: u64,
        now: OffsetDateTime,
    ) -> bool {
        self.record_command_end_for_generation_at(exit_code, source, generation, now)
    }

    /// 按代次结束命令，并使用宿主 `D` 提供的结束时间。
    ///
    /// 代次不匹配时不生效；其余语义与
    /// [`Self::record_command_end_for_generation`] 相同。
    pub fn record_command_end_for_generation_at(
        &mut self,
        exit_code: Option<i32>,
        source: TranscriptSource,
        generation: u64,
        ended_at: OffsetDateTime,
    ) -> bool {
        let Some(last) = self.commands.back_mut() else {
            return false;
        };
        if last.generation != generation {
            return false;
        }
        let changed = finish_command(last, exit_code, source, ended_at);
        if changed {
            self.bump();
        }
        changed
    }

    /// 当前最后一条命令的代次；没有命令时为 0。
    pub fn current_generation(&self) -> u64 {
        self.last_generation
    }

    /// 静默结束判定：当前命令超过 `quiet` 没有新输出时标记 `done`。
    ///
    /// `ended_at` 取最后一次输出时间（无输出时即开始时间）。静默推断
    /// 来源标为 `process_group` 且 `best_effort=true`；不伪造退出码。
    ///
    /// `output_from_events=true` 时**完全不生效**（返回 `false`，状态/输出
    /// 不变）：`O` 来源命令只由宿主 `D` 结束，1s 静默不能提前把一个仍在
    /// 运行的长命令标成完成。事件通道死亡后 service 会先切回
    /// `output_from_events=false`，下一次 tick 才允许提升 PTY 兜底。
    ///
    /// 返回是否发生变更。
    pub fn finalize_quiet(&mut self, now: OffsetDateTime, quiet: Duration) -> bool {
        if self.output_from_events {
            return false;
        }
        let Some(last) = self.commands.back_mut() else {
            return false;
        };
        if last.status != TranscriptStatus::Running {
            return false;
        }
        let idle = now - last.last_output_at;
        if idle < time::Duration::try_from(quiet).unwrap_or(time::Duration::ZERO) {
            return false;
        }
        settle_pty_output(last);
        last.status = TranscriptStatus::Done;
        last.ended_at = Some(last.last_output_at);
        last.source = Some(TranscriptSource::ProcessGroup);
        last.best_effort = true;
        self.bump();
        true
    }

    /// 终端进入终态：把仍在运行的当前命令标记为对应状态。
    ///
    /// 已结束的命令保持不变；返回是否发生变更。
    pub fn finalize_terminal(&mut self, status: TranscriptStatus, now: OffsetDateTime) -> bool {
        let Some(last) = self.commands.back_mut() else {
            return false;
        };
        if last.status != TranscriptStatus::Running {
            return false;
        }
        settle_pty_output(last);
        last.status = status;
        last.ended_at = Some(now);
        self.bump();
        true
    }

    /// 读取最近命令记录。
    ///
    /// `limit` 为 `None` 或 0 时返回全部保留记录；否则只返回最近的 N 条
    /// （仍按时间升序）。输出在返回前完成清洗。
    pub fn read(&self, limit: Option<u32>) -> TranscriptResult {
        let take = match limit {
            Some(value) if value > 0 => value as usize,
            _ => self.commands.len(),
        };
        let skip = self.commands.len().saturating_sub(take);
        let commands = self
            .commands
            .iter()
            .skip(skip)
            .map(CommandRecord::to_protocol)
            .collect();
        TranscriptResult {
            commands,
            revision: self.revision,
        }
    }

    /// 按时间顺序拼接各命令**清洗后**的输出文本（命令块之间以换行分隔）。
    ///
    /// 供 `read`/`search` 在结构化终端上以“命令输出行”为语义读取：不包含
    /// raw PTY 的 ANSI/OSC、命令回显与提示符。空输出的命令被跳过；没有任何
    /// 输出时返回空串。命令块之间显式插入 `\n`，避免相邻命令的输出粘成
    /// 同一行（否则 `--only-matches` 会跨命令夹带内容）。
    pub fn output_text(&self) -> String {
        let mut text = String::new();
        for record in &self.commands {
            let cleaned = clean_output(&record.command, &record.output);
            if cleaned.is_empty() {
                continue;
            }
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&cleaned);
        }
        text
    }

    /// 当前保留条数。
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// 是否没有任何命令记录。
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// 容量。
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// 当前修订号。
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// 递增修订号并返回新值。
    fn bump(&mut self) -> u64 {
        self.revision = self.revision.saturating_add(1);
        self.revision
    }
}

impl Default for TranscriptBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_TRANSCRIPT_CAPACITY)
    }
}

/// 在命令结束时收敛 PTY 兜底：
/// - 没有 `O`：把兜底并入正式输出；兜底溢出则标记不完整。
/// - 有 `O`：丢弃兜底；若兜底里存在 `O` 未覆盖的可见行（native 段），
///   或兜底溢出，则只标记 `incomplete`，不追加、不伪造退出码。
fn settle_pty_output(record: &mut CommandRecord) {
    if record.pty_output.is_empty() {
        if record.pty_output_overflowed {
            record.output_incomplete = true;
        }
        return;
    }
    if !record.event_output_seen {
        let fallback = std::mem::take(&mut record.pty_output);
        append_output_limited(record, &fallback);
        if record.pty_output_overflowed {
            record.output_incomplete = true;
        }
        return;
    }

    if record.pty_output_overflowed {
        record.output_incomplete = true;
    }
    let fallback = clean_output(&record.command, &record.pty_output);
    let events = clean_output(&record.command, &record.output);
    if has_uncovered_visible_lines(&fallback, &events) {
        record.output_incomplete = true;
    }
    record.pty_output.clear();
}

/// PTY 兜底里是否有 `O` 文本未覆盖的可见行。
fn has_uncovered_visible_lines(fallback: &str, events: &str) -> bool {
    fallback
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .any(|line| !events.contains(line))
}

/// 结束一条命令记录：`running` 或可升级的 `best_effort done` 才生效。
///
/// 返回是否发生变更；不触碰修订号，由调用方统一 bump。
fn finish_command(
    record: &mut CommandRecord,
    exit_code: Option<i32>,
    source: TranscriptSource,
    now: OffsetDateTime,
) -> bool {
    let upgradeable = record.status == TranscriptStatus::Done && record.best_effort;
    if record.status != TranscriptStatus::Running && !upgradeable {
        return false;
    }
    settle_pty_output(record);
    record.status = TranscriptStatus::Done;
    record.ended_at = Some(now);
    record.exit_code = exit_code;
    record.source = Some(source);
    record.best_effort = false;
    true
}

/// 已结束命令收到新输出时回到 `running`，并清除上一次结束留下的证据。
///
/// `shell_integration` 结束的命令不复活：marker 之后的提示符/尾部输出属于
/// shell，不应把已确定的状态与退出码改回 `running`/`None`。
fn revive_done(record: &mut CommandRecord) -> bool {
    if record.status != TranscriptStatus::Done {
        return false;
    }
    if record.source == Some(TranscriptSource::ShellIntegration) {
        return false;
    }
    record.status = TranscriptStatus::Running;
    record.ended_at = None;
    record.exit_code = None;
    record.source = None;
    record.best_effort = false;
    true
}

/// 按上限追加输出；超限时保留不超过 [`MAX_OUTPUT_BYTES`] 的内容并追加一次标记。
///
/// 截断点始终落在 UTF-8 字符边界上；必要时回退已有内容末尾为标记腾出空间。
fn append_output_limited(record: &mut CommandRecord, text: &str) {
    let remaining = MAX_OUTPUT_BYTES.saturating_sub(record.output.len());
    if text.len() <= remaining {
        record.output.push_str(text);
        return;
    }

    let overflow = OUTPUT_TRUNCATED_MARKER.len().saturating_sub(remaining);
    if overflow > 0 {
        let keep =
            floor_char_boundary(&record.output, record.output.len().saturating_sub(overflow));
        record.output.truncate(keep);
    }
    let remaining = MAX_OUTPUT_BYTES.saturating_sub(record.output.len());
    let content_budget = remaining.saturating_sub(OUTPUT_TRUNCATED_MARKER.len());
    let take = floor_char_boundary(text, content_budget);
    record.output.push_str(&text[..take]);
    record.output.push_str(OUTPUT_TRUNCATED_MARKER);
    record.truncated = true;
}

/// 返回不超过 `max` 且落在 UTF-8 字符边界上的字节下标。
fn floor_char_boundary(text: &str, max: usize) -> usize {
    if max >= text.len() {
        return text.len();
    }
    let mut index = max;
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// 清洗命令输出（信息页/模型读取使用）。
///
/// 规则：去 ANSI 与控制字符；CR/CRLF 统一为 LF；若首行包含命令文本则视为
/// 终端回显并删除；去掉末尾空行与提示符行（cmd `...>`、PowerShell `PS ...>`、
/// bash/zsh `user@host:...$/#`）；去掉首尾空行，保留其余原文。
pub fn clean_output(command: &str, raw: &str) -> String {
    let stripped = strip_ansi(raw);
    let normalized = normalize_controls(&stripped);
    let mut lines: Vec<String> = normalized
        .split('\n')
        .filter_map(strip_marker_echo)
        .collect();

    let command = command.trim();
    // 多行命令在 PTY 回显里可能被折叠成一行；去掉空白后再比对，避免
    // 预记录造成的命令回显被当成输出（例如 `for ...; do ...; done`）。
    let compact_command = compact_for_echo(command);
    // 终端回显可能带提示符/启动横幅（ConPTY 常见），在前几行里找到命令并删到它为止。
    if !command.is_empty() {
        if let Some(index) = lines.iter().take(10).position(|line| {
            line.contains(command)
                || (!compact_command.is_empty()
                    && compact_for_echo(line).contains(&compact_command))
        }) {
            lines.drain(0..=index);
        } else {
            while lines
                .first()
                .is_some_and(|line| line.trim().is_empty() || is_prompt_line(line))
            {
                lines.remove(0);
            }
        }
    }

    // 启动横幅不属于命令输出。
    lines.retain(|line| !is_shell_noise(line));

    // 提示符可能粘在输出末尾，也可能在行中间（PowerShell 启动慢时尤其常见）；
    // 命中提示符后，其后的横幅/下一命令都属于 shell，不属于本命令。
    let mut cleaned = Vec::with_capacity(lines.len());
    for line in lines {
        let Some(line) = strip_marker_echo(&line) else {
            continue;
        };
        let stripped = strip_prompt_anywhere(&line);
        if stripped.trim().is_empty() && !line.trim().is_empty() {
            continue;
        }
        // PTY 可能在命令回显尚未写完时被截断（ConPTY 光标/分块），
        // 留下命令前缀。它仍是回显，不是程序输出。
        let compact_line = compact_for_echo(&stripped);
        if !compact_command.is_empty()
            && compact_line.len() >= 8
            && compact_command.starts_with(&compact_line)
        {
            continue;
        }
        cleaned.push(stripped);
    }
    let mut lines = cleaned;

    while lines
        .last()
        .is_some_and(|line| line.trim().is_empty() || is_prompt_line(line))
    {
        lines.pop();
    }
    while lines
        .first()
        .is_some_and(|line| line.trim().is_empty() || is_prompt_line(line))
    {
        lines.remove(0);
    }

    lines.join("\n")
}

/// 去掉全部空白，用于比对多行命令与折叠成单行的 PTY 回显。
fn compact_for_echo(text: &str) -> String {
    text.chars().filter(|ch| !ch.is_whitespace()).collect()
}

/// 判断 shell 启动横幅/版权行（这些不是命令输出）。
fn is_shell_noise(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with("PowerShell ")
        || trimmed.starts_with("Windows PowerShell")
        || trimmed.starts_with("加载个人和系统配置文件耗时")
        || trimmed.starts_with("Loading personal and system profiles")
        || trimmed.starts_with("版权所有")
        || trimmed.starts_with("Copyright (C) Microsoft")
}

/// 去掉 termx 注入的 marker 命令回显（shell 会把整条包装命令回显出来）。
///
/// marker 本体是 OSC，`strip_ansi` 会删掉；这里只处理回显里的**命令文本**。
/// 命中 marker 后缀时保留命令前的输出；纯 marker 行返回 `None`。
fn strip_marker_echo(line: &str) -> Option<String> {
    let trimmed = line.trim_end();
    // marker 回显可能被 PSReadLine 重绘成多行；整行丢弃，避免命令文本
    // 在输出里重复出现。正常输出不会包含这些 termx 私有标记。
    if trimmed.contains("__termx_ec")
        || trimmed.contains("]133;D;")
        || trimmed.contains("]133;D\u{7}")
    {
        return None;
    }
    Some(line.to_string())
}

/// 剥离行内任意位置的 shell 提示符，保留提示符之前的输出。
fn strip_prompt_anywhere(line: &str) -> String {
    let trimmed = line.trim_end();
    if trimmed.is_empty() {
        return String::new();
    }

    // PowerShell：`PS C:\path>`；提示符后可能紧跟横幅或下一条命令。
    if let Some(pos) = trimmed.rfind("PS ") {
        let rest = &trimmed[pos + 3..];
        if let Some(end) = rest.find('>') {
            let path = &rest[..end];
            if path.contains('\\') || path.contains('/') {
                return trimmed[..pos].trim_end().to_string();
            }
        }
    }

    // cmd：`C:\path>` / `\\server\share>`。
    if let Some(pos) = trimmed.rfind(":\\") {
        let start = pos.saturating_sub(1);
        if trimmed[start..].contains('>') {
            return trimmed[..start].trim_end().to_string();
        }
    }
    if let Some(pos) = trimmed.rfind("\\\\") {
        if trimmed[pos..].contains('>') {
            return trimmed[..pos].trim_end().to_string();
        }
    }

    // Unix：`user@host:path$` / `#`（通常粘在行尾）。
    strip_trailing_prompt(trimmed)
}

/// 剥离行尾粘着的 shell 提示符（cmd/PowerShell/Unix），保留前面的输出。
fn strip_trailing_prompt(line: &str) -> String {
    let trimmed = line.trim_end();
    if trimmed.is_empty() {
        return String::new();
    }

    // PowerShell 优先：`PS C:\path>`（直接保留 PS 之前的内容）。
    if let Some(before) = trimmed.strip_suffix('>') {
        if let Some(pos) = before.rfind("PS ") {
            if before[pos..].contains('\\') {
                return trimmed[..pos].trim_end().to_string();
            }
        }
        // cmd：`C:\path>` / `\\server\share>`（可能粘在输出后面）。
        if let Some(start) = before
            .rfind(":\\")
            .map(|index| index.saturating_sub(1))
            .or_else(|| before.rfind("\\\\"))
        {
            if before[start..].contains('\\') {
                return trimmed[..start].trim_end().to_string();
            }
        }
    }

    // Unix：`user@host:path$` / `#`。
    if let Some(last) = trimmed.chars().last() {
        if last == '$' || last == '#' {
            if let Some(at) = trimmed.rfind('@') {
                let start = trimmed[..at]
                    .rfind(char::is_whitespace)
                    .map(|index| index + 1)
                    .unwrap_or(0);
                if trimmed[start..].contains(':') {
                    return trimmed[..start].trim_end().to_string();
                }
            }
        }
    }

    trimmed.to_string()
}

/// 判断一行是否是常见 shell 提示符。
fn is_prompt_line(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.starts_with("PS ") && trimmed.ends_with('>') {
        return true;
    }
    if is_windows_prompt(trimmed) {
        return true;
    }
    is_unix_prompt(trimmed)
}

/// cmd.exe 提示符：`C:\path>`、`\\server\share>` 或单独的 `>`。
fn is_windows_prompt(line: &str) -> bool {
    let Some(prefix) = line.strip_suffix('>') else {
        return false;
    };
    let prefix = prefix.trim_end();
    if prefix.is_empty() {
        return true;
    }
    let bytes = prefix.as_bytes();
    if bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic() {
        return true;
    }
    prefix.starts_with("\\\\")
}

/// bash/zsh 提示符：`user@host:path$` 或 `user@host:path#`。
fn is_unix_prompt(line: &str) -> bool {
    let Some(prefix) = line.strip_suffix(['$', '#']) else {
        return false;
    };
    prefix.contains('@') && prefix.contains(':')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 固定基准时间，避免测试依赖系统时钟。
    fn base() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("合法时间戳")
    }

    #[test]
    fn next_command_finalizes_previous_as_done() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        let t1 = t0 + time::Duration::seconds(5);

        buffer.record_command("echo one", t0);
        assert!(buffer.record_output("one\r\n", t0));
        buffer.record_command("echo two", t1);

        let result = buffer.read(None);
        assert_eq!(result.commands.len(), 2);
        assert_eq!(result.commands[0].command, "echo one");
        assert_eq!(result.commands[0].status, TranscriptStatus::Done);
        assert_eq!(
            result.commands[0].ended_at.as_deref(),
            Some(crate::format_rfc3339(t1).as_str())
        );
        assert_eq!(result.commands[1].status, TranscriptStatus::Running);
        assert!(result.commands[1].ended_at.is_none());
    }

    #[test]
    fn output_after_real_command_end_is_not_appended() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        let t1 = t0 + time::Duration::seconds(1);

        buffer.record_command("ls", t0);
        assert!(buffer.record_command_end(Some(0), TranscriptSource::ShellIntegration, t0));

        // 续行提示 `>>` 属于下一个 prompt，不能挂到已结束的 ls。
        assert!(!buffer.record_output(">>\r\n", t1));

        let result = buffer.read(None);
        assert_eq!(result.commands.len(), 1);
        assert_eq!(result.commands[0].status, TranscriptStatus::Done);
        assert!(
            result.commands[0].output.is_empty(),
            "已真实结束的命令不应接收后续输出"
        );
    }

    #[test]
    fn late_output_can_be_appended_when_enabled() {
        let mut buffer = TranscriptBuffer::with_in_band_osc_and_late_output(8, false, true);
        let t0 = base();
        buffer.record_command("Write-Output one; Write-Output two", t0);
        assert!(buffer.record_command_end(Some(0), TranscriptSource::ShellIntegration, t0));
        assert!(buffer.record_output("one\r\n", t0));
        assert!(buffer.record_output("two\r\n", t0 + time::Duration::milliseconds(10)));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(command.output, "one\ntwo");
    }

    #[test]
    fn cancel_pending_removes_only_pending_last_record() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();

        // 无 pending：no-op。
        assert!(!buffer.cancel_pending());
        assert_eq!(buffer.len(), 0);

        buffer.record_command("multiline", t0);
        let revision = buffer.revision();
        assert!(buffer.cancel_pending());
        assert!(buffer.is_empty(), "pending 记录应被删除");
        assert!(buffer.revision() > revision, "删除记录应递增修订号");
        // 再次取消没有 pending，不再变更。
        assert!(!buffer.cancel_pending());
    }

    #[test]
    fn cancel_pending_ignores_non_pending_or_finished_records() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();

        // 宿主 C 新建的记录不是 pending：不得删除。
        buffer.record_command_at("from host", t0);
        assert!(!buffer.cancel_pending());
        assert_eq!(buffer.len(), 1);

        // send 记录但已被 D 结束：不得删除。
        buffer.record_command("sent", t0);
        assert!(buffer.record_command_end(Some(0), TranscriptSource::ShellIntegration, t0));
        assert!(!buffer.cancel_pending());
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn quiet_finalize_uses_last_output_time() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        buffer.record_command("ping", t0);
        buffer.record_output("tick\n", t0 + time::Duration::milliseconds(200));

        assert!(!buffer.finalize_quiet(
            t0 + time::Duration::milliseconds(900),
            DEFAULT_QUIET_FINALIZE
        ));
        assert!(buffer.finalize_quiet(
            t0 + time::Duration::milliseconds(1200),
            DEFAULT_QUIET_FINALIZE
        ));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(
            command.ended_at.as_deref(),
            Some(crate::format_rfc3339(t0 + time::Duration::milliseconds(200)).as_str())
        );
    }

    #[test]
    fn generation_scoped_end_ignores_stale_generation() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        let t1 = t0 + time::Duration::seconds(1);

        assert_eq!(buffer.current_generation(), 0, "无命令时代次为 0");
        buffer.record_command("first", t0);
        let first_generation = buffer.current_generation();
        assert_eq!(first_generation, 1);

        buffer.record_command("second", t1);
        assert_eq!(buffer.current_generation(), 2);

        // 旧命令代次的迟到结束不得影响新命令。
        assert!(!buffer.record_command_end_for_generation(
            Some(0),
            TranscriptSource::ShellIntegration,
            first_generation,
            t1,
        ));
        let commands = buffer.read(None).commands;
        assert_eq!(commands[1].status, TranscriptStatus::Running);
        assert_eq!(commands[1].exit_code, None);

        // 当前代次的结束照常生效。
        assert!(buffer.record_command_end_for_generation(
            Some(7),
            TranscriptSource::ShellIntegration,
            2,
            t1,
        ));
        let commands = buffer.read(None).commands;
        assert_eq!(commands[1].status, TranscriptStatus::Done);
        assert_eq!(commands[1].exit_code, Some(7));
        assert_eq!(commands[1].source, Some(TranscriptSource::ShellIntegration));
    }

    #[test]
    fn command_end_records_exit_code_and_source() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        let t1 = t0 + time::Duration::seconds(2);
        buffer.record_command("false", t0);

        assert!(buffer.record_command_end(Some(1), TranscriptSource::ShellIntegration, t1));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(
            command.ended_at.as_deref(),
            Some(crate::format_rfc3339(t1).as_str())
        );
        assert_eq!(command.exit_code, Some(1));
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
        assert!(!command.best_effort);
    }

    #[test]
    fn command_end_without_exit_code_keeps_none() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        buffer.record_command("interactive", t0);

        assert!(buffer.record_command_end(None, TranscriptSource::Unknown, t0));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(command.exit_code, None);
        assert_eq!(command.source, Some(TranscriptSource::Unknown));
        assert!(!command.best_effort);
    }

    #[test]
    fn command_end_does_not_repeat_on_terminal_state() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        let t1 = t0 + time::Duration::seconds(1);
        buffer.record_command("false", t0);
        assert!(buffer.record_command_end(Some(2), TranscriptSource::ShellIntegration, t1));

        let revision = buffer.revision();
        assert!(!buffer.record_command_end(Some(9), TranscriptSource::ProcessGroup, t0));
        assert_eq!(buffer.revision(), revision, "终态不应再次计费");

        // 终端终态也不覆盖已记录的退出码。
        assert!(!buffer.finalize_terminal(TranscriptStatus::Exited, t0));
        let command = &buffer.read(None).commands[0];
        assert_eq!(command.exit_code, Some(2));
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
        assert_eq!(
            command.ended_at.as_deref(),
            Some(crate::format_rfc3339(t1).as_str())
        );
    }

    #[test]
    fn osc_command_end_marks_done_with_exit_code_and_source() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        let t1 = t0 + time::Duration::seconds(2);
        buffer.record_command("false", t0);

        assert!(buffer.record_output("false\r\n\u{1b}]133;D;7\u{7}\r\n", t1));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(command.exit_code, Some(7));
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
        assert!(!command.best_effort);
        assert_eq!(
            command.ended_at.as_deref(),
            Some(crate::format_rfc3339(t1).as_str())
        );
        assert!(
            !command.output.contains("133;D"),
            "marker 应被清洗: {:?}",
            command.output
        );
    }

    #[test]
    fn marker_split_across_output_chunks_is_parsed() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        buffer.record_command("false", t0);

        assert!(buffer.record_output("out\r\n\u{1b}]133;D", t0));
        assert_eq!(
            buffer.read(None).commands[0].status,
            TranscriptStatus::Running,
            "半个 marker 不应结束命令"
        );

        assert!(buffer.record_output(";5\u{7}", t0 + time::Duration::seconds(1)));
        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(command.exit_code, Some(5));
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
    }

    #[test]
    fn in_band_osc_disabled_ignores_fake_command_end() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        buffer.record_command("echo hi", t0);

        assert!(buffer.record_output("\u{1b}]133;D;0\u{7}\r\n", t0));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Running);
        assert_eq!(
            command.exit_code, None,
            "带外模式下 stdout OSC 不得结束命令"
        );
        assert_eq!(command.source, None);
        assert!(!buffer.in_band_osc());
    }

    #[test]
    fn in_band_osc_enabled_still_parses_marker() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, true);
        let t0 = base();
        buffer.record_command("false", t0);

        assert!(buffer.record_output("\u{1b}]133;D;7\u{7}", t0));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(command.exit_code, Some(7));
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
        assert!(buffer.in_band_osc());
    }

    #[test]
    fn quiet_finalize_does_not_override_marker_result() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        buffer.record_command("false", t0);
        buffer.record_output("\u{1b}]133;D;3\u{7}", t0);

        assert!(
            !buffer.finalize_quiet(t0 + time::Duration::seconds(10), DEFAULT_QUIET_FINALIZE),
            "已由 marker 结束的命令不应再被静默判定改写"
        );

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(command.exit_code, Some(3));
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
    }

    #[test]
    fn marker_upgrades_previous_quiet_finalize() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        buffer.record_command("sleep 5", t0);
        buffer.record_output("tick\n", t0);
        assert!(buffer.finalize_quiet(t0 + time::Duration::seconds(2), DEFAULT_QUIET_FINALIZE));

        buffer.record_output("\u{1b}]133;D;9\u{7}", t0 + time::Duration::seconds(3));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(command.exit_code, Some(9));
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
        assert!(!command.best_effort);
    }

    #[test]
    fn output_after_marker_does_not_revive_command() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        buffer.record_command("false", t0);
        buffer.record_output("\u{1b}]133;D;4\u{7}", t0);

        buffer.record_output("prompt$ ", t0 + time::Duration::seconds(1));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(command.exit_code, Some(4));
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
    }

    #[test]
    fn pty_fallback_is_promoted_when_command_has_no_output_events() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        let generation = buffer.record_command("cmd /c echo hi", t0);
        buffer.record_pty_output_for_current("hi\r\n", t0);
        assert!(buffer.record_command_end_for_generation_at(
            Some(0),
            TranscriptSource::ShellIntegration,
            generation,
            t0,
        ));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "hi");
    }

    #[test]
    fn pty_fallback_is_discarded_when_output_event_exists() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        let generation = buffer.record_command("Write-Output hi", t0);
        buffer.record_pty_output_for_current("PTY duplicate\r\n", t0);
        assert!(buffer.record_output_for_generation(generation, "hi\n", t0));
        assert!(buffer.record_command_end_for_generation_at(
            Some(0),
            TranscriptSource::ShellIntegration,
            generation,
            t0,
        ));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "hi");
        assert!(command.incomplete, "混合 host+native 必须标记不完整");
    }

    #[test]
    fn pure_output_event_is_not_incomplete() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        let generation = buffer.record_command("Write-Output hi", t0);
        assert!(buffer.record_output_for_generation(generation, "hi\n", t0));
        assert!(buffer.record_command_end_for_generation_at(
            Some(0),
            TranscriptSource::ShellIntegration,
            generation,
            t0,
        ));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "hi");
        assert!(!command.incomplete, "纯 O 命令不得被标记不完整");
    }

    #[test]
    fn pure_output_event_with_pty_echo_is_not_incomplete() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        let generation = buffer.record_command("Write-Output token", t0);
        buffer.record_pty_output_for_current(
            "PS C:\\proj> Write-Output token\r\ntoken\r\nPS C:\\proj>",
            t0,
        );
        assert!(buffer.record_output_for_generation(generation, "token\n", t0));
        assert!(buffer.record_command_end_for_generation_at(
            Some(0),
            TranscriptSource::ShellIntegration,
            generation,
            t0,
        ));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "token");
        assert!(!command.incomplete, "PTY 只是 O 的回显副本时不得标记不完整");
    }

    #[test]
    fn pure_pty_fallback_is_not_incomplete() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        let generation = buffer.record_command("cmd /c echo hi", t0);
        buffer.record_pty_output_for_current("hi\r\n", t0);
        assert!(buffer.record_command_end_for_generation_at(
            Some(0),
            TranscriptSource::ShellIntegration,
            generation,
            t0,
        ));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "hi");
        assert!(!command.incomplete, "纯 PTY 兜底命令不得被标记不完整");
    }

    #[test]
    fn pty_after_output_event_marks_incomplete_without_duplicating() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        let generation = buffer.record_command("Write-Output host; cmd /c echo native", t0);
        assert!(buffer.record_output_for_generation(generation, "host\n", t0));
        assert!(!buffer.record_pty_output_for_current("native\r\n", t0));
        assert!(!buffer.record_pty_output_for_current("native\r\n", t0));
        assert!(buffer.record_command_end_for_generation_at(
            Some(0),
            TranscriptSource::ShellIntegration,
            generation,
            t0,
        ));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "host", "PTY 兜底不得追加到 O 文本");
        assert!(command.incomplete, "混合命令必须标记 incomplete");
        assert!(!command.best_effort, "incomplete 不得改变 best_effort");
    }

    #[test]
    fn output_event_after_pty_buffer_marks_incomplete_without_duplicating() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        let generation = buffer.record_command("cmd /c echo native; Write-Output host", t0);
        buffer.record_pty_output_for_current("native\r\n", t0);
        assert!(buffer.record_output_for_generation(generation, "host\n", t0));
        assert!(buffer.record_command_end_for_generation_at(
            Some(0),
            TranscriptSource::ShellIntegration,
            generation,
            t0,
        ));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "host", "已缓存的 PTY 兜底必须丢弃");
        assert!(command.incomplete, "O 到达前已有兜底必须标记 incomplete");
    }

    #[test]
    fn mark_incomplete_after_end_keeps_exit_code_and_best_effort() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        let generation = buffer.record_command("false", t0);
        assert!(buffer.record_command_end_for_generation_at(
            Some(1),
            TranscriptSource::ShellIntegration,
            generation,
            t0,
        ));
        assert!(buffer.mark_output_incomplete_for_generation(generation));
        assert!(
            !buffer.mark_output_incomplete_for_generation(generation),
            "重复标记不再变更"
        );

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.exit_code, Some(1), "incomplete 不得伪造/清除退出码");
        assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
        assert!(!command.best_effort);
        assert!(command.incomplete);
    }

    #[test]
    fn quiet_finalize_ignored_for_event_output() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false).with_output_from_events(true);
        let t0 = base();
        buffer.record_command("Start-Sleep 3; Write-Output late", t0);
        // PTY 兜底先到（混合 host/native 命令），静默判定必须完全不生效。
        buffer.record_pty_output_for_current("PTY fallback\r\n", t0);
        assert!(!buffer.finalize_quiet(t0 + time::Duration::seconds(3), DEFAULT_QUIET_FINALIZE));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Running);
        assert!(command.ended_at.is_none(), "O 来源命令只能由 D 结束");
        assert!(!command.best_effort);
        assert_eq!(command.output, "");
    }

    #[test]
    fn quiet_finalize_after_event_channel_death_promotes_fallback() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false).with_output_from_events(true);
        let t0 = base();
        buffer.record_command("cmd /c echo hi", t0);
        buffer.record_pty_output_for_current("hi\r\n", t0);

        assert!(
            buffer.set_output_from_events(false),
            "通道死亡后应切回兜底模式"
        );
        assert!(!buffer.set_output_from_events(false), "重复切换不再变更");
        assert!(buffer.finalize_quiet(t0 + time::Duration::seconds(2), DEFAULT_QUIET_FINALIZE));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert!(command.best_effort);
        assert_eq!(command.output, "hi");
    }

    #[test]
    fn late_event_output_not_duplicated_with_fallback() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false).with_output_from_events(true);
        let t0 = base();
        let generation = buffer.record_command("Write-Output late", t0);
        buffer.record_pty_output_for_current("PTY fallback\r\n", t0);
        assert!(!buffer.finalize_quiet(t0 + time::Duration::seconds(2), DEFAULT_QUIET_FINALIZE));

        // 迟到的 O 到达：只应留下 O 文本，不得与兜底重复。
        assert!(buffer.record_output_for_generation(
            generation,
            "event text\n",
            t0 + time::Duration::seconds(3)
        ));
        assert!(buffer.record_command_end_for_generation_at(
            Some(0),
            TranscriptSource::ShellIntegration,
            generation,
            t0 + time::Duration::seconds(3),
        ));
        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "event text");
        assert!(
            !command.output.contains("PTY fallback"),
            "静默提升兜底会与迟到 O 重复: {:?}",
            command.output
        );
        assert!(command.incomplete, "迟到 O 与已有兜底混合必须标记不完整");
    }

    #[test]
    fn quiet_finalize_promotes_fallback_without_event_output() {
        let mut buffer = TranscriptBuffer::with_in_band_osc(8, false);
        let t0 = base();
        buffer.record_command("cmd /c echo hi", t0);
        buffer.record_pty_output_for_current("hi\r\n", t0);
        assert!(buffer.finalize_quiet(t0 + time::Duration::seconds(2), DEFAULT_QUIET_FINALIZE));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "hi");
        assert!(command.best_effort);
    }

    #[test]
    fn command_start_defaults_clear_completion_fields() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        buffer.record_command("first", t0);
        assert!(buffer.record_command_end(Some(0), TranscriptSource::ShellIntegration, t0));

        buffer.record_command("second", t0 + time::Duration::seconds(1));
        let commands = buffer.read(None).commands;
        assert_eq!(commands[0].exit_code, Some(0), "上一条已记录的证据应保留");
        assert_eq!(commands[1].exit_code, None);
        assert_eq!(commands[1].source, None);
        assert!(!commands[1].best_effort);
    }

    #[test]
    fn quiet_finalize_is_best_effort_without_exit_code() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        buffer.record_command("ping", t0);
        buffer.record_output("tick\n", t0 + time::Duration::milliseconds(100));

        assert!(buffer.finalize_quiet(
            t0 + time::Duration::milliseconds(1200),
            DEFAULT_QUIET_FINALIZE
        ));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Done);
        assert_eq!(command.exit_code, None, "静默结束不得伪造退出码");
        assert_eq!(command.source, Some(TranscriptSource::ProcessGroup));
        assert!(command.best_effort);
    }

    #[test]
    fn terminal_status_finalizes_running_command_only() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        buffer.record_command("sleep 10", t0);
        assert!(buffer.finalize_terminal(TranscriptStatus::Interrupted, t0));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.status, TranscriptStatus::Interrupted);
        assert!(command.ended_at.is_some());
        assert!(!buffer.record_output("late\n", t0 + time::Duration::seconds(1)));
    }

    #[test]
    fn capacity_evicts_oldest_commands() {
        let mut buffer = TranscriptBuffer::new(2);
        let t0 = base();
        for index in 1..=4 {
            buffer.record_command(&format!("cmd-{index}"), t0 + time::Duration::seconds(index));
        }
        assert_eq!(buffer.len(), 2);
        let result = buffer.read(None);
        assert_eq!(result.commands.len(), 2);
        assert_eq!(result.commands[0].command, "cmd-3");
        assert_eq!(result.commands[1].command, "cmd-4");
        assert!(result.commands[0].status == TranscriptStatus::Done);
    }

    #[test]
    fn read_limit_returns_latest_in_ascending_order() {
        let mut buffer = TranscriptBuffer::new(8);
        let t0 = base();
        for index in 1..=3 {
            buffer.record_command(&format!("cmd-{index}"), t0 + time::Duration::seconds(index));
        }
        let result = buffer.read(Some(2));
        assert_eq!(result.commands.len(), 2);
        assert_eq!(result.commands[0].command, "cmd-2");
        assert_eq!(result.commands[1].command, "cmd-3");
    }

    #[test]
    fn clean_output_strips_ansi_echo_and_trailing_prompts() {
        let raw = "\u{1b}[32mecho hi\u{1b}[0m\r\nhi\r\nD:\\WorkSpace\\proj>";
        assert_eq!(clean_output("echo hi", raw), "hi");

        let raw = "PS C:\\proj> echo hi\r\nhi\r\nPS C:\\proj>";
        assert_eq!(clean_output("echo hi", raw), "hi");

        let raw = "echo hi\r\nhi\r\nuser@host:~/proj$";
        assert_eq!(clean_output("echo hi", raw), "hi");
    }

    #[test]
    fn clean_output_handles_powershell_cursor_positioning_and_banner() {
        let raw = "PS C:\\proj> pwd\r\nPath\r\n----\r\nD:\\proj\u{1b}[12;1HMode                 LastWriteTime\r\n----                 -------------\r\nPowerShell 7.6.9\r\n加载个人和系统配置文件耗时 728 毫秒。\r\nPS C:\\proj> ls\r\n";
        let cleaned = clean_output("pwd", raw);
        assert!(cleaned.contains("D:\\proj"));
        assert!(cleaned.contains("Mode"));
        assert!(!cleaned.contains("PowerShell"));
        assert!(!cleaned.contains("加载个人"));
        assert!(!cleaned.contains("PS C:"));
        assert!(!cleaned.contains("ls"));
    }

    #[test]
    fn clean_output_strips_inline_powershell_prompt_and_next_command() {
        let raw = "pwd\r\nD:\\projPS C:\\proj> PowerShell 7.6.9\r\n加载个人和系统配置文件耗时 728 毫秒。\r\nPS C:\\proj> ls\r\n";
        assert_eq!(clean_output("pwd", raw), "D:\\proj");
    }

    #[test]
    fn clean_output_strips_marker_command_echo() {
        let raw = "Write-Output hi; $__termx_ec = if ($?) {0} elseif ($LASTEXITCODE -is [int]) {$LASTEXITCODE} else {1}; [Console]::Write(\"$([char]27)]133;D;$__termx_ec$([char]7)\")\r\nhi\r\n";
        assert_eq!(clean_output("Write-Output hi", raw), "hi");

        let raw = "echo hi; __termx_ec=$?; printf '\\033]133;D;%s\\007' \"$__termx_ec\"\r\nhi\r\n";
        assert_eq!(clean_output("echo hi", raw), "hi");

        // 多行命令回显被折叠成一行时也必须识别为回显，不能混进输出。
        let raw = "for i in 1 2 3; do echo line$i; done\r\nline1\r\nline2\r\nline3\r\n";
        assert_eq!(
            clean_output("for i in 1 2 3;\ndo\n  echo line$i;\ndone", raw),
            "line1\nline2\nline3"
        );
    }

    #[test]
    fn clean_output_strips_partial_command_echo_fragment() {
        let raw = "PS C:\\proj> Write-Output 'pure-host-abcdef\r\npure-host-abcdef\r\nPS C:\\proj>";
        assert_eq!(
            clean_output("Write-Output 'pure-host-abcdef'", raw),
            "pure-host-abcdef"
        );
    }

    #[test]
    fn clean_output_keeps_blank_lines_inside_output() {
        let raw = "printf 'a\\n\\nb'\r\na\r\n\r\nb\r\nC:\\>";
        assert_eq!(clean_output("printf 'a\\n\\nb'", raw), "a\n\nb");
    }

    #[test]
    fn clean_output_keeps_non_prompt_angle_brackets() {
        let raw = "echo '<tag>'\r\n<tag>\r\nD:\\>";
        assert_eq!(clean_output("echo '<tag>'", raw), "<tag>");
    }

    #[test]
    fn oversized_output_is_truncated_once_and_stops_growing() {
        let mut buffer = TranscriptBuffer::new(4);
        let t0 = base();
        buffer.record_command("yes", t0);

        // 恰好填满上限：不截断、无标记。
        assert!(buffer.record_output(&"a".repeat(MAX_OUTPUT_BYTES), t0));
        let full = buffer.commands.back().expect("应有当前命令").output.clone();
        assert_eq!(full.len(), MAX_OUTPUT_BYTES);
        assert!(!full.contains(OUTPUT_TRUNCATED_MARKER));

        // 再有任何输出都会截断并只追加一次标记。
        assert!(buffer.record_output("tail", t0 + time::Duration::seconds(1)));
        let truncated = buffer.commands.back().expect("应有当前命令").output.clone();
        assert!(truncated.contains(OUTPUT_TRUNCATED_MARKER));
        assert_eq!(
            truncated.matches(OUTPUT_TRUNCATED_MARKER).count(),
            1,
            "截断标记只应出现一次"
        );
        assert!(
            truncated.len() <= MAX_OUTPUT_BYTES,
            "截断后总长度不应超过上限: {}",
            truncated.len()
        );

        // 后续输出不再增长。
        assert!(!buffer.record_output("more", t0 + time::Duration::seconds(2)));
        assert_eq!(
            buffer.commands.back().expect("应有当前命令").output,
            truncated
        );
    }

    #[test]
    fn truncation_does_not_split_utf8() {
        let mut buffer = TranscriptBuffer::new(4);
        let t0 = base();
        buffer.record_command("cat utf8.txt", t0);
        // 每字符 3 字节，保证截断点落在字符内部以验证回退。
        let text = "中".repeat(MAX_OUTPUT_BYTES / 3 + 10);
        assert!(buffer.record_output(&text, t0));

        let output = &buffer.commands.back().expect("应有当前命令").output;
        let marker = output.find(OUTPUT_TRUNCATED_MARKER).expect("应有截断标记");
        assert!(output.is_char_boundary(marker));
        assert!(output[..marker].chars().all(|ch| ch == '中'));
        assert!(output.len() <= MAX_OUTPUT_BYTES);
    }

    #[test]
    fn normal_output_is_not_truncated() {
        let mut buffer = TranscriptBuffer::new(4);
        let t0 = base();
        buffer.record_command("echo hi", t0);
        assert!(buffer.record_output("echo hi\r\nhi\r\nD:\\>", t0));

        let command = &buffer.read(None).commands[0];
        assert_eq!(command.output, "hi");
        assert!(!command.output.contains(OUTPUT_TRUNCATED_MARKER));
    }

    #[test]
    fn clean_output_keeps_truncation_marker() {
        let raw = format!(
            "echo big\r\n{}\r\n[output truncated]\r\nD:\\>",
            "x".repeat(32)
        );
        let cleaned = clean_output("echo big", &raw);
        assert!(cleaned.ends_with("[output truncated]"), "实际: {cleaned:?}");
    }
}
