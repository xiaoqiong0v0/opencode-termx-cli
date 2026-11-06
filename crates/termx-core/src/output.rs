//! 命令输出文本的清洗、尾部截取与倒搜。
//!
//! 这里的「行」指**命令输出行**，不是终端显示行：
//! - [`clean_raw_output`]：把 raw PTY 文本剥成命令输出文本（去 ANSI/OSC、
//!   处理 `\r` 重绘、CRLF 与退格），供没有 shell 事件的 raw-only 终端使用；
//! - [`tail_lines`]：最后 N 行；
//! - [`tail_chars`]：最后 N 个 Unicode 字符；
//! - [`search`]：从末尾倒搜第 N 个字面量匹配。
//!
//! 结构化终端应先用 transcript 的命令输出文本（已清洗、无 prompt）再调用
//! 上述函数。历史被背压裁剪后，只能基于当前保留的文本计算；调用方不得
//! 假设旧位置可用。
//!
//! raw-only 终端没有 shell 事件，无法可靠剔除 prompt；这里只保证不返回
//! ANSI/OSC 等控制串。

/// 取最后 `limit` 行（保留原始换行符，历史不足时返回全部文本）。
///
/// 末尾换行不额外算作一行，因此 `"a\nb\n"` 的最后一行是 `"b\n"`。
/// `limit == 0` 返回空串。
pub fn tail_lines(text: &str, limit: usize) -> String {
    if limit == 0 || text.is_empty() {
        return String::new();
    }

    let bytes = text.as_bytes();
    let mut seen = 0;
    let mut start = 0;
    let mut index = bytes.len();
    while index > 0 {
        index -= 1;
        if bytes[index] != b'\n' {
            continue;
        }
        // 文本末尾的换行是上一行的结束符，不开启新行。
        if index + 1 == bytes.len() {
            continue;
        }
        seen += 1;
        if seen == limit {
            start = index + 1;
            break;
        }
    }
    text[start..].to_string()
}

/// 取最后最多 `limit` 个 Unicode 字符（按 UTF-8 字符边界，历史不足时返回全部）。
pub fn tail_chars(text: &str, limit: usize) -> String {
    if limit == 0 {
        return String::new();
    }
    let total = text.chars().count();
    if total <= limit {
        return text.to_string();
    }
    text.chars().skip(total - limit).collect()
}

/// 从末尾倒搜第 `nth` 个字面量匹配。
///
/// - `nth == 1` 表示最后一个匹配；
/// - 默认返回从该匹配起点到文本末尾；
/// - `only_matches=true` 时只返回匹配所在行（不含行结束符）；
/// - 无匹配或 `nth` 超出范围时返回空串。
pub fn search(text: &str, pattern: &str, nth: usize, only_matches: bool) -> String {
    if pattern.is_empty() || nth == 0 {
        return String::new();
    }

    let starts: Vec<usize> = text
        .match_indices(pattern)
        .map(|(start, _)| start)
        .collect();
    if starts.len() < nth {
        return String::new();
    }
    let start = starts[starts.len() - nth];
    if only_matches {
        line_at(text, start)
    } else {
        text[start..].to_string()
    }
}

/// 取 `index` 所在行（去掉行结束符；CRLF 的 `\r` 也去掉）。
fn line_at(text: &str, index: usize) -> String {
    let line_start = text[..index].rfind('\n').map_or(0, |pos| pos + 1);
    let line_end = text[index..]
        .find('\n')
        .map_or(text.len(), |pos| index + pos);
    let line = &text[line_start..line_end];
    line.strip_suffix('\r').unwrap_or(line).to_string()
}

/// 把 raw PTY 文本清洗成命令输出文本（raw-only 终端使用）。
///
/// 规则：去 ANSI/OSC 等转义序列；CRLF 统一为 LF；行内单独的 `\r` 按终端
/// 重绘语义回到行首覆盖（进度条只保留最后一帧），`\b` 退格；丢弃其余不可
/// 打印控制字符，保留换行与制表。
///
/// 与 transcript 的 [`crate::clean_output`] 不同：这里**不感知 prompt**，
/// raw-only 终端没有 shell 事件，无法可靠区分输出与提示符。
pub fn clean_raw_output(raw: &str) -> String {
    let stripped = strip_ansi(raw);
    normalize_redraw(&stripped)
}

/// 行内重绘：`\r` 回到行首覆盖、`\b` 退格、CRLF 与 LF 换行。
fn normalize_redraw(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut line: Vec<char> = Vec::new();
    let mut cursor = 0usize;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                    flush_line(&mut out, &mut line, &mut cursor);
                } else {
                    cursor = 0;
                }
            }
            '\n' => flush_line(&mut out, &mut line, &mut cursor),
            '\u{8}' => cursor = cursor.saturating_sub(1),
            ch if ch.is_control() && ch != '\t' => {}
            ch => {
                if cursor < line.len() {
                    line[cursor] = ch;
                } else {
                    line.extend(std::iter::repeat_n(' ', cursor - line.len()));
                    line.push(ch);
                }
                cursor += 1;
            }
        }
    }
    out.extend(line.iter());
    out
}

/// 结束当前行：写入缓冲并保留换行，重置游标。
fn flush_line(out: &mut String, line: &mut Vec<char>, cursor: &mut usize) {
    out.extend(line.iter());
    out.push('\n');
    line.clear();
    *cursor = 0;
}

/// 去除 ANSI 转义序列（CSI / OSC / 其它单字符 ESC 序列）。
///
/// `crate::clean_output` 也复用它；PowerShell 表格常用 `ESC[row;colH`
/// 绝对定位代替换行，这里转成换行，避免删除转义后把两行粘在一起。
pub(crate) fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            out.push(ch);
            continue;
        }
        match chars.peek() {
            // CSI：ESC [ ... 终止字节 0x40..=0x7e。
            Some('[') => {
                chars.next();
                let mut final_byte = None;
                for next in chars.by_ref() {
                    if ('@'..='~').contains(&next) {
                        final_byte = Some(next);
                        break;
                    }
                }
                if matches!(final_byte, Some('H' | 'f')) && !out.ends_with('\n') {
                    out.push('\n');
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
///
/// 行内单独的 `\r` 视为换行（transcript 清洗沿用的历史语义）；`\b` 退格。
pub(crate) fn normalize_controls(input: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_lines_handles_trailing_newline_and_short_history() {
        assert_eq!(tail_lines("a\nb\nc", 2), "b\nc");
        assert_eq!(tail_lines("a\nb\nc\n", 2), "b\nc\n");
        assert_eq!(tail_lines("a\nb\nc\n", 1), "c\n");
        assert_eq!(tail_lines("only", 5), "only");
        assert_eq!(tail_lines("", 2), "");
        assert_eq!(tail_lines("a\nb\n", 0), "");
    }

    #[test]
    fn tail_lines_counts_empty_final_line_correctly() {
        assert_eq!(tail_lines("a\nb\n", 1), "b\n");
        assert_eq!(tail_lines("a\n\n", 1), "\n");
        assert_eq!(tail_lines("\n", 1), "\n");
    }

    #[test]
    fn tail_chars_uses_unicode_boundaries() {
        assert_eq!(tail_chars("你好abc", 3), "abc");
        assert_eq!(tail_chars("你好abc", 4), "好abc");
        assert_eq!(tail_chars("你好", 10), "你好");
        assert_eq!(tail_chars("你好", 0), "");
        assert_eq!(tail_chars("😀x", 1), "x");
    }

    #[test]
    fn search_counts_matches_from_end() {
        let text = "alpha one\nbeta alpha two\ngamma alpha\n";
        // 最后一个匹配到末尾。
        assert_eq!(search(text, "alpha", 1, false), "alpha\n");
        // 倒数第二个匹配到末尾。
        assert_eq!(search(text, "alpha", 2, false), "alpha two\ngamma alpha\n");
        // 超出匹配数量。
        assert_eq!(search(text, "alpha", 9, false), "");
        assert_eq!(search(text, "", 1, false), "");
    }

    #[test]
    fn search_only_matches_returns_anchor_line() {
        let text = "alpha one\nbeta alpha two\ngamma alpha\n";
        assert_eq!(search(text, "alpha", 1, true), "gamma alpha");
        assert_eq!(search(text, "alpha", 2, true), "beta alpha two");
        // CRLF 行结束符不进入结果。
        assert_eq!(search("x\r\nalpha\r\n", "alpha", 1, true), "alpha");
    }

    #[test]
    fn clean_raw_output_strips_ansi_and_osc() {
        let raw = "\u{1b}[?25l\u{1b}]0;window title\u{7}\u{1b}[32mgreen\u{1b}[0m\r\n";
        let cleaned = clean_raw_output(raw);
        assert_eq!(cleaned, "green\n");
        assert!(!cleaned.contains("[?25l"));
        assert!(!cleaned.contains('\u{1b}'));
    }

    #[test]
    fn clean_raw_output_applies_cr_redraw_and_crlf() {
        // 进度条只保留最后一帧。
        assert_eq!(clean_raw_output("10%\r20%\r100%\r\n"), "100%\n");
        // 短的重绘不会清掉旧行剩余部分（真实终端行为）。
        assert_eq!(clean_raw_output("abcdef\rXY\n"), "XYcdef\n");
        // 退格。
        assert_eq!(clean_raw_output("abc\u{8}d\r\n"), "abd\n");
        // CRLF 是换行，不是重绘。
        assert_eq!(clean_raw_output("a\r\nb\n"), "a\nb\n");
    }

    #[test]
    fn clean_raw_output_keeps_utf8_and_tabs() {
        assert_eq!(clean_raw_output("\t你好\r\n"), "\t你好\n");
        assert_eq!(clean_raw_output("😀\r\n"), "😀\n");
        assert_eq!(clean_raw_output("你\u{1b}[0m好"), "你好");
    }

    #[test]
    fn cleaned_text_drives_tail_lines_and_search_on_command_lines() {
        // prompt 仍在（raw-only 无 shell 事件，已知限制），但控制串必须消失，
        // 且行边界按清洗后的文本计算。
        let raw = "\u{1b}[?25lPS C:\\proj> alpha one\r\nbeta alpha two\r\n\u{1b}[0m";
        let text = clean_raw_output(raw);
        assert_eq!(tail_lines(&text, 1), "beta alpha two\n");
        assert_eq!(search(&text, "alpha", 1, true), "beta alpha two");
        assert_eq!(search(&text, "beta", 1, true), "beta alpha two");
    }
}
