//! 每终端有界命令记录：命令、起止时间、状态与清洗后的输出。
//!
//! 与事件缓冲分开：本模块只服务信息页与模型读取入口，不参与事件投递、
//! 背压与订阅回放。容量按命令条数淘汰最旧记录；输出在读取时统一清洗
//! （去 ANSI、命令回显与末尾提示符）。

use std::collections::VecDeque;
use std::time::Duration;

use termx_protocol::{TranscriptCommand, TranscriptResult, TranscriptStatus};
use time::OffsetDateTime;

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
    /// 输出是否已因超过上限被截断（只追加一次标记）。
    truncated: bool,
    /// 最后一次收到输出的时间；用于静默判定。
    last_output_at: OffsetDateTime,
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
    /// 已保留命令，按时间升序。
    commands: VecDeque<CommandRecord>,
}

impl TranscriptBuffer {
    /// 以给定容量创建；容量为 0 时按 1 处理。
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            revision: 0,
            commands: VecDeque::new(),
        }
    }

    /// 开始一条新命令：上一条仍在运行的命令先标记为 `done`。
    ///
    /// 返回变更后的修订号。
    pub fn record_command(&mut self, command: &str, now: OffsetDateTime) -> u64 {
        if let Some(last) = self.commands.back_mut() {
            if last.status == TranscriptStatus::Running {
                last.status = TranscriptStatus::Done;
                last.ended_at = Some(now);
            }
        }
        self.commands.push_back(CommandRecord {
            command: command.trim_end_matches(['\r', '\n']).to_string(),
            started_at: now,
            ended_at: None,
            status: TranscriptStatus::Running,
            output: String::new(),
            truncated: false,
            last_output_at: now,
        });
        while self.commands.len() > self.capacity {
            self.commands.pop_front();
        }
        self.bump()
    }

    /// 追加一段输出到当前（最后一条）命令。
    ///
    /// 终态命令不再接收输出；静默判定为 `done` 的命令在被追加输出时
    /// 重新回到 `running`。空文本或尚无命令时忽略。单条输出最多保留
    /// [`MAX_OUTPUT_BYTES`] 字节：超限时按 UTF-8 字符边界截断并追加一次
    /// [`OUTPUT_TRUNCATED_MARKER`]，后续输出不再增长。
    pub fn record_output(&mut self, text: &str, now: OffsetDateTime) -> bool {
        if text.is_empty() {
            return false;
        }
        let Some(last) = self.commands.back_mut() else {
            return false;
        };
        if matches!(
            last.status,
            TranscriptStatus::Exited | TranscriptStatus::Interrupted | TranscriptStatus::Error
        ) {
            return false;
        }
        if last.truncated {
            // 已截断：只刷新活动时间，内容不再增长。
            last.last_output_at = now;
            if last.status == TranscriptStatus::Done {
                last.status = TranscriptStatus::Running;
                last.ended_at = None;
                self.bump();
                return true;
            }
            return false;
        }

        append_output_limited(last, text);
        last.last_output_at = now;
        if last.status == TranscriptStatus::Done {
            last.status = TranscriptStatus::Running;
            last.ended_at = None;
        }
        self.bump();
        true
    }

    /// 静默结束判定：当前命令超过 `quiet` 没有新输出时标记 `done`。
    ///
    /// `ended_at` 取最后一次输出时间（无输出时即开始时间）。返回是否发生变更。
    pub fn finalize_quiet(&mut self, now: OffsetDateTime, quiet: Duration) -> bool {
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
        last.status = TranscriptStatus::Done;
        last.ended_at = Some(last.last_output_at);
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
    let mut lines: Vec<String> = normalized.split('\n').map(str::to_string).collect();

    let command = command.trim();
    // 终端回显可能带提示符/启动横幅（ConPTY 常见），在前几行里找到命令并删到它为止。
    if !command.is_empty() {
        if let Some(index) = lines
            .iter()
            .take(10)
            .position(|line| line.contains(command))
        {
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

    // 提示符可能粘在输出末尾（ConPTY），先剥离最后几行。
    for line in lines.iter_mut().rev().take(3) {
        *line = strip_trailing_prompt(line);
    }

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

/// 去掉 ANSI 转义序列（CSI / OSC / 其它单字符 ESC 序列）。
fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            out.push(ch);
            continue;
        }
        match chars.peek() {
            // CSI：ESC [ ... 终止字节 0x40..=0x7e
            Some('[') => {
                chars.next();
                for next in chars.by_ref() {
                    if ('@'..='~').contains(&next) {
                        break;
                    }
                }
            }
            // OSC：ESC ] ... BEL 或 ST(ESC \)
            Some(']') => {
                chars.next();
                while let Some(next) = chars.next() {
                    if next == '\u{7}' {
                        break;
                    }
                    if next == '\u{1b}' {
                        if chars.peek() == Some(&'\\') {
                            chars.next();
                        }
                        break;
                    }
                }
            }
            // 其它两字符 ESC 序列。
            Some(_) => {
                chars.next();
            }
            None => {}
        }
    }
    out
}

/// 统一换行并丢弃不可打印控制字符（保留换行与制表）。
fn normalize_controls(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                out.push('\n');
            }
            '\u{8}' => {
                if !out.is_empty() && !out.ends_with('\n') {
                    out.pop();
                }
            }
            ch if ch.is_control() && ch != '\n' && ch != '\t' => {}
            ch => out.push(ch),
        }
    }
    out
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
