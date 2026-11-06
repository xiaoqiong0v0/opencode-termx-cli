//! OSC 133/633（shell integration）增量扫描器。
//!
//! 只识别 shell integration 所需的命令子集：`A`（prompt 开始）、
//! `B`（prompt 结束 / 命令输入开始）、`C`（命令执行 / 输出开始）、
//! `D`（命令结束）。其他 OSC、CSI 与普通文本一律忽略。

/// OSC 序列的起始字节 `ESC`。
const ESC: u8 = 0x1b;
/// OSC 序列的 introducer `]`，紧跟在 `ESC` 之后。
const OSC_INTRODUCER: u8 = b']';
/// BEL 终止符。
const BEL: u8 = 0x07;
/// 字符串终止符 `ST` 的第二个字节（`ESC \`）。
const ST_FINAL: u8 = b'\\';
/// 单个 OSC 载荷的最大字节数；超过后丢弃该 OSC 并恢复普通解析。
const MAX_OSC_PAYLOAD: usize = 4096;
/// 命令参数分隔符。
const PARAM_SEPARATOR: u8 = b';';

/// OSC 133/633 序列的来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OscSource {
    /// `OSC 133`（shell integration 标准）。
    Osc133,
    /// `OSC 633`（VS Code shell integration 扩展）。
    Osc633,
}

/// 扫描器识别出的 shell integration 事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OscEvent {
    /// `A`：prompt 开始。
    PromptStart(OscSource),
    /// `B`：prompt 结束，命令输入开始。
    PromptEnd(OscSource),
    /// `C`：命令已执行，输出开始。
    CommandStart(OscSource),
    /// `D`：命令结束；`exit_code` 来自 `D;<code>`，缺省或非法时为 `None`。
    CommandEnd {
        /// 序列来源（`OSC 133` / `OSC 633`）。
        source: OscSource,
        /// 可选退出码；无法解析时不报错，填 `None`。
        exit_code: Option<i32>,
    },
}

/// 增量解析状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ParseState {
    /// 普通文本，等待 `ESC`。
    #[default]
    Ground,
    /// 已读到 `ESC`，等待后续字节。
    Escape,
    /// 正在收集 OSC 载荷（`ESC ]` 之后）。
    Osc,
    /// OSC 载荷内读到 `ESC`，等待可能的 `ST`（`ESC \`）。
    OscEscape,
}

/// OSC 133/633 增量扫描器。
///
/// 调用方把终端输出按任意分块交给 [`OscScanner::push`]，扫描器跨分块
/// 保持状态，返回本批字节中新识别的事件。缓冲区有界，未终止的 OSC
/// 超过 [`MAX_OSC_PAYLOAD`] 会被丢弃并恢复普通解析。
#[derive(Debug, Default, Clone)]
pub struct OscScanner {
    /// 当前解析状态。
    state: ParseState,
    /// 当前 OSC 载荷（不含 `ESC ]` 与终止符）。
    payload: Vec<u8>,
}

impl OscScanner {
    /// 创建新的扫描器。
    pub fn new() -> Self {
        Self::default()
    }

    /// 清空解析状态与缓冲，回到初始状态。
    pub fn reset(&mut self) {
        self.state = ParseState::Ground;
        self.payload.clear();
    }

    /// 增量输入一段终端输出，返回其中识别到的事件。
    ///
    /// 分块边界可以落在任意字节（包括 `ESC`、`BEL`、`ST` 中间）。
    #[must_use]
    pub fn push(&mut self, bytes: &[u8]) -> Vec<OscEvent> {
        let mut events = Vec::new();
        for &byte in bytes {
            self.advance(byte, &mut events);
        }
        events
    }

    /// 按单字节推进状态机。
    fn advance(&mut self, byte: u8, events: &mut Vec<OscEvent>) {
        match self.state {
            ParseState::Ground => {
                if byte == ESC {
                    self.state = ParseState::Escape;
                }
            }
            ParseState::Escape => match byte {
                OSC_INTRODUCER => {
                    self.payload.clear();
                    self.state = ParseState::Osc;
                }
                // 连续 `ESC`：仍等待序列引导字符。
                ESC => {}
                _ => self.state = ParseState::Ground,
            },
            ParseState::Osc => match byte {
                BEL => self.finish(events),
                ESC => self.state = ParseState::OscEscape,
                _ => {
                    if self.payload.len() >= MAX_OSC_PAYLOAD {
                        // 超长且未终止：丢弃载荷，恢复普通解析。
                        self.payload.clear();
                        self.state = ParseState::Ground;
                    } else {
                        self.payload.push(byte);
                    }
                }
            },
            ParseState::OscEscape => match byte {
                ST_FINAL => self.finish(events),
                // `ESC ESC`：当前 OSC 作废，新的 `ESC` 继续作为引导符。
                ESC => {
                    self.payload.clear();
                    self.state = ParseState::Escape;
                }
                // `ESC ]`：当前 OSC 作废，直接开始新的 OSC。
                OSC_INTRODUCER => {
                    self.payload.clear();
                    self.state = ParseState::Osc;
                }
                // 非法 `ESC x`：放弃当前 OSC，`x` 按普通文本忽略。
                _ => {
                    self.payload.clear();
                    self.state = ParseState::Ground;
                }
            },
        }
    }

    /// 处理 OSC 终止符：解析载荷、清空状态并记录事件。
    fn finish(&mut self, events: &mut Vec<OscEvent>) {
        if let Some(event) = parse_payload(&self.payload) {
            events.push(event);
        }
        self.payload.clear();
        self.state = ParseState::Ground;
    }
}

/// 解析 OSC 载荷（`133;A[;...]` / `633;D[;exit_code]`）为事件。
fn parse_payload(payload: &[u8]) -> Option<OscEvent> {
    let mut params = payload.split(|&byte| byte == PARAM_SEPARATOR);
    let code = params.next()?;
    let command = params.next()?;

    let source = match code {
        b"133" => OscSource::Osc133,
        b"633" => OscSource::Osc633,
        _ => return None,
    };

    let event = match command {
        b"A" => OscEvent::PromptStart(source),
        b"B" => OscEvent::PromptEnd(source),
        b"C" => OscEvent::CommandStart(source),
        b"D" => OscEvent::CommandEnd {
            source,
            exit_code: params.next().and_then(parse_exit_code),
        },
        _ => return None,
    };
    Some(event)
}

/// 解析可选的退出码；非 UTF-8、空串或非整数一律返回 `None`。
fn parse_exit_code(raw: &[u8]) -> Option<i32> {
    std::str::from_utf8(raw).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 单条 `133;A` 以 BEL 终止。
    #[test]
    fn parses_single_osc_133_bel() {
        let mut scanner = OscScanner::new();
        let events = scanner.push(b"\x1b]133;A\x07");
        assert_eq!(events, vec![OscEvent::PromptStart(OscSource::Osc133)]);
    }

    /// 单条 `633;D` 以 ST 终止。
    #[test]
    fn parses_single_osc_633_st() {
        let mut scanner = OscScanner::new();
        let events = scanner.push(b"\x1b]633;D\x1b\\");
        assert_eq!(
            events,
            vec![OscEvent::CommandEnd {
                source: OscSource::Osc633,
                exit_code: None
            }]
        );
    }

    /// 序列被拆分到多次 push（逐字节）仍能识别。
    #[test]
    fn parses_across_chunk_boundaries() {
        let mut scanner = OscScanner::new();
        let mut events = Vec::new();
        for &byte in b"\x1b]133;B\x1b\\" {
            events.extend(scanner.push(&[byte]));
        }
        assert_eq!(events, vec![OscEvent::PromptEnd(OscSource::Osc133)]);
    }

    /// 一条序列跨两次 push，且终止符 `ESC \` 被拆开。
    #[test]
    fn keeps_state_when_st_is_split() {
        let mut scanner = OscScanner::new();
        assert!(scanner.push(b"\x1b]633;C").is_empty());
        assert!(scanner.push(b"\x1b").is_empty());
        let events = scanner.push(b"\\");
        assert_eq!(events, vec![OscEvent::CommandStart(OscSource::Osc633)]);
    }

    /// BEL 与 ST 两种终止符都能识别。
    #[test]
    fn parses_both_terminators() {
        let mut scanner = OscScanner::new();
        let events = scanner.push(b"\x1b]133;A\x07\x1b]633;D\x1b\\");
        assert_eq!(
            events,
            vec![
                OscEvent::PromptStart(OscSource::Osc133),
                OscEvent::CommandEnd {
                    source: OscSource::Osc633,
                    exit_code: None
                },
            ]
        );
    }

    /// 四个命令字符映射到对应事件，并保留来源。
    #[test]
    fn maps_all_command_letters() {
        let mut scanner = OscScanner::new();
        let events = scanner.push(b"\x1b]133;A\x07\x1b]133;B\x07\x1b]633;C\x07\x1b]633;D\x07");
        assert_eq!(
            events,
            vec![
                OscEvent::PromptStart(OscSource::Osc133),
                OscEvent::PromptEnd(OscSource::Osc133),
                OscEvent::CommandStart(OscSource::Osc633),
                OscEvent::CommandEnd {
                    source: OscSource::Osc633,
                    exit_code: None
                },
            ]
        );
    }

    /// `133;D` / `633;D` 携带的退出码被正确解析。
    #[test]
    fn parses_exit_codes_from_133_and_633() {
        let mut scanner = OscScanner::new();
        let events = scanner.push(b"\x1b]133;D;0\x07\x1b]633;D;1\x07");
        assert_eq!(
            events,
            vec![
                OscEvent::CommandEnd {
                    source: OscSource::Osc133,
                    exit_code: Some(0),
                },
                OscEvent::CommandEnd {
                    source: OscSource::Osc633,
                    exit_code: Some(1),
                },
            ]
        );
    }

    /// `D` 后的退出码可以跨 chunk 拆分。
    #[test]
    fn parses_exit_code_across_chunk_boundaries() {
        let mut scanner = OscScanner::new();
        let mut events = Vec::new();
        for &byte in b"\x1b]133;D;25\x07" {
            events.extend(scanner.push(&[byte]));
        }
        assert_eq!(
            events,
            vec![OscEvent::CommandEnd {
                source: OscSource::Osc133,
                exit_code: Some(25),
            }]
        );
    }

    /// 负数是合法 `i32`，按原值保留。
    #[test]
    fn keeps_negative_exit_code() {
        let mut scanner = OscScanner::new();
        let events = scanner.push(b"\x1b]633;D;-1\x07");
        assert_eq!(
            events,
            vec![OscEvent::CommandEnd {
                source: OscSource::Osc633,
                exit_code: Some(-1),
            }]
        );
    }

    /// 空串、非数字与溢出值都填 `None`，且不影响后续解析。
    #[test]
    fn invalid_exit_codes_become_none() {
        let mut scanner = OscScanner::new();
        let events =
            scanner.push(b"\x1b]133;D;\x07\x1b]633;D;abc\x07\x1b]133;D;99999999999999999999\x07");
        assert_eq!(
            events,
            vec![
                OscEvent::CommandEnd {
                    source: OscSource::Osc133,
                    exit_code: None,
                },
                OscEvent::CommandEnd {
                    source: OscSource::Osc633,
                    exit_code: None,
                },
                OscEvent::CommandEnd {
                    source: OscSource::Osc133,
                    exit_code: None,
                },
            ]
        );
    }

    /// `D` 的额外参数只取第一个作为退出码；`A` 的附加信息仍被忽略。
    #[test]
    fn ignores_trailing_parameters() {
        let mut scanner = OscScanner::new();
        let events = scanner.push(b"\x1b]133;D;0;extra\x07\x1b]633;A;cl=m\x07");
        assert_eq!(
            events,
            vec![
                OscEvent::CommandEnd {
                    source: OscSource::Osc133,
                    exit_code: Some(0),
                },
                OscEvent::PromptStart(OscSource::Osc633),
            ]
        );
    }

    /// 未知 OSC（窗口标题）与普通文本不产生事件。
    #[test]
    fn ignores_unrelated_osc_and_text() {
        let mut scanner = OscScanner::new();
        let events =
            scanner.push(b"hello\x1b]0;title\x07 world 133;A \x1b]633;E;cmd\x07\x1b]1330;A\x07");
        assert!(events.is_empty());
    }

    /// 超长且未终止的 OSC 会被丢弃，后续序列仍可解析。
    #[test]
    fn recovers_after_overlong_unterminated_osc() {
        let mut scanner = OscScanner::new();
        let mut overlong = b"\x1b]133;A".to_vec();
        overlong.extend(std::iter::repeat_n(b'x', MAX_OSC_PAYLOAD + 32));
        assert!(scanner.push(&overlong).is_empty());
        assert!(scanner.payload.len() <= MAX_OSC_PAYLOAD);

        let events = scanner.push(b"\x1b]633;D\x07");
        assert_eq!(
            events,
            vec![OscEvent::CommandEnd {
                source: OscSource::Osc633,
                exit_code: None
            }]
        );
    }

    /// `reset` 丢弃半截状态，之后的输入从干净状态开始。
    #[test]
    fn reset_discards_partial_state() {
        let mut scanner = OscScanner::new();
        assert!(scanner.push(b"\x1b]133;A").is_empty());
        scanner.reset();
        let events = scanner.push(b"plain 133;A\x07\x1b]133;A\x07");
        assert_eq!(events, vec![OscEvent::PromptStart(OscSource::Osc133)]);
    }
}
