//! `Registry::output_text`：尾部行/字符与倒搜，覆盖 raw 输出与 UTF-8 边界。

use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use termx_core::{
    CoreError, DEFAULT_EVENT_BUFFER_CAPACITY, MockBackend, MockFactory, Registry, TerminalId,
};
use termx_protocol::{CreateParams, EventKind, Mode, OutputParams, Stream, TerminalLocator};

/// 构造 pty 创建参数。
fn params(name: &str) -> CreateParams {
    CreateParams {
        mode: Mode::Pty,
        cmd: "mock".to_string(),
        args: Vec::new(),
        cwd: None,
        env: None,
        cols: None,
        rows: None,
        root_session_id: "ses-output".to_string(),
        name: name.to_string(),
        force: false,
        idle_timeout: None,
        ssh: None,
        allow_injection: false,
        runtime_id: None,
    }
}

/// 按 `terminal_id` 构造定位器。
fn locator(terminal_id: &str) -> TerminalLocator {
    TerminalLocator {
        terminal_id: Some(terminal_id.to_string()),
        root_session_id: None,
        name: None,
    }
}

/// 创建终端并写入原始输出字节。
fn terminal_with_output(chunks: &[&[u8]]) -> (Registry, String) {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let terminal_id = registry
        .create(params("output"))
        .expect("create 应成功")
        .terminal_id;
    let id = TerminalId::new(terminal_id.clone());
    for chunk in chunks {
        registry
            .report_event(
                &id,
                EventKind::Output {
                    data: BASE64_STANDARD.encode(chunk),
                    stream: Stream::Stdout,
                },
            )
            .expect("report_event 应成功");
    }
    (registry, terminal_id)
}

/// 尾部行读取。
#[test]
fn output_tail_lines_returns_last_lines() {
    let (registry, terminal_id) = terminal_with_output(&[b"one\ntwo\n", b"three\n"]);
    let result = registry
        .output_text(
            &locator(&terminal_id),
            &OutputParams {
                locator: TerminalLocator::default(),
                tail_lines: Some(2),
                tail_chars: None,
                pattern: None,
                nth: None,
                only_matches: false,
            },
        )
        .expect("output 应成功");
    assert_eq!(result.text, "two\nthree\n");
}

/// 尾部字符按 Unicode 字符边界，且能跨 chunk 拼接多字节字符。
#[test]
fn output_tail_chars_handles_utf8_split_across_chunks() {
    let text = "你好x";
    let bytes = text.as_bytes();
    // 在“你”的 3 字节中间切开，验证先拼字节再解码。
    let (registry, terminal_id) = terminal_with_output(&[&bytes[..2], &bytes[2..]]);
    let result = registry
        .output_text(
            &locator(&terminal_id),
            &OutputParams {
                locator: TerminalLocator::default(),
                tail_lines: None,
                tail_chars: Some(2),
                pattern: None,
                nth: None,
                only_matches: false,
            },
        )
        .expect("output 应成功");
    assert_eq!(result.text, "好x");
}

/// 倒搜 nth 与 only-matches，均基于原始输出。
#[test]
fn output_search_backwards_and_only_matches() {
    let (registry, terminal_id) =
        terminal_with_output(&[b"alpha one\nbeta alpha two\n", b"gamma alpha\n"]);

    let from_match = registry
        .output_text(
            &locator(&terminal_id),
            &OutputParams {
                locator: TerminalLocator::default(),
                tail_lines: None,
                tail_chars: None,
                pattern: Some("alpha".to_string()),
                nth: Some(2),
                only_matches: false,
            },
        )
        .expect("output 应成功");
    assert_eq!(from_match.text, "alpha two\ngamma alpha\n");

    let only_line = registry
        .output_text(
            &locator(&terminal_id),
            &OutputParams {
                locator: TerminalLocator::default(),
                tail_lines: None,
                tail_chars: None,
                pattern: Some("alpha".to_string()),
                nth: Some(1),
                only_matches: true,
            },
        )
        .expect("output 应成功");
    assert_eq!(only_line.text, "gamma alpha");
}

/// 无匹配返回空串；互斥参数返回 bad_request。
#[test]
fn output_rejects_ambiguous_params_and_returns_empty_when_missing() {
    let (registry, terminal_id) = terminal_with_output(&[b"no hit\n"]);
    let missing = registry
        .output_text(
            &locator(&terminal_id),
            &OutputParams {
                locator: TerminalLocator::default(),
                tail_lines: None,
                tail_chars: None,
                pattern: Some("absent".to_string()),
                nth: Some(1),
                only_matches: false,
            },
        )
        .expect("output 应成功");
    assert_eq!(missing.text, "");

    let ambiguous = registry.output_text(
        &locator(&terminal_id),
        &OutputParams {
            locator: TerminalLocator::default(),
            tail_lines: Some(1),
            tail_chars: Some(1),
            pattern: None,
            nth: None,
            only_matches: false,
        },
    );
    assert!(matches!(ambiguous, Err(CoreError::BadRequest(_))));

    let nobody = registry.output_text(
        &locator(&terminal_id),
        &OutputParams {
            locator: TerminalLocator::default(),
            tail_lines: None,
            tail_chars: None,
            pattern: None,
            nth: None,
            only_matches: false,
        },
    );
    assert!(matches!(nobody, Err(CoreError::BadRequest(_))));
}

/// 构造 `terminal.output` 参数（locator 由调用方另给）。
fn output_params(
    tail_lines: Option<u32>,
    tail_chars: Option<u64>,
    pattern: Option<&str>,
    nth: Option<u32>,
    only_matches: bool,
) -> OutputParams {
    OutputParams {
        locator: TerminalLocator::default(),
        tail_lines,
        tail_chars,
        pattern: pattern.map(str::to_string),
        nth,
        only_matches,
    }
}

/// 固定基准时间。
fn base_time() -> time::OffsetDateTime {
    time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("合法时间戳")
}

/// 创建带外 shell 事件（结构化）终端；`in_band_osc=false` 即 `shell_events=true`。
fn structured_terminal() -> (Registry, String) {
    let mut registry = Registry::with_factory(DEFAULT_EVENT_BUFFER_CAPACITY, Arc::new(MockFactory));
    let create_params = params("structured");
    registry.prepare_create(&create_params).expect("预检应成功");
    let (created, displaced) = registry
        .register_create_with(
            create_params,
            Arc::new(MockBackend::new()),
            None,
            false,
            false,
        )
        .expect("注册应成功");
    assert!(displaced.is_none());
    (registry, created.terminal_id)
}

/// 从 `terminal_id` 构造终端 ID。
fn terminal(id: &str) -> TerminalId {
    TerminalId::new(id.to_string())
}

/// raw-only：剥离 ANSI/OSC，并按 CR 重绘折叠进度行。
#[test]
fn raw_only_output_strips_ansi_osc_and_redraw() {
    let (registry, terminal_id) = terminal_with_output(&[
        b"\x1b[?25l\x1b[32mone\x1b[0m\r\n",
        b"progress 10%\rprogress 20%\r\n",
        b"\x1b]0;window title\x07two\r\n",
    ]);
    let text = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(Some(3), None, None, None, false),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(text, "one\nprogress 20%\ntwo\n");
    assert!(!text.contains("[?25l"));
    assert!(!text.contains('\u{1b}'));
}

/// raw-only：倒搜与 only-matches 基于清洗后的命令输出行。
#[test]
fn raw_only_search_only_matches_uses_cleaned_line() {
    let (registry, terminal_id) =
        terminal_with_output(&[b"\x1b[?25lalpha one\r\n", b"beta alpha two\r\n\x1b[0m"]);
    let line = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(None, None, Some("alpha"), Some(1), true),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(line, "beta alpha two");
    assert!(!line.contains('\u{1b}'));
    assert!(!line.contains("[?25l"));
}

/// structured：只用 transcript 的清洗输出，忽略 raw PTY（含控制串）。
#[test]
fn structured_output_uses_transcript_not_raw_pty() {
    let (mut registry, terminal_id) = structured_terminal();
    let id = terminal(&terminal_id);
    let t0 = base_time();

    // raw PTY 里塞满控制串与一条不应出现的噪声行。
    registry
        .report_event(
            &id,
            EventKind::Output {
                data: BASE64_STANDARD.encode(b"\x1b[?25lraw-only-noise\r\n\x1b]0;t\x07"),
                stream: Stream::Stdout,
            },
        )
        .expect("注入 raw 输出");

    registry
        .record_transcript_command(&id, "cmd one", t0)
        .expect("记录命令");
    registry
        .record_transcript_output(&id, "cmd one\r\nalpha one\r\nbeta alpha two\r\n", t0)
        .expect("记录输出");
    registry
        .record_transcript_end(&id, Some(0), t0)
        .expect("结束命令");
    registry
        .record_transcript_command(&id, "cmd two", t0 + time::Duration::seconds(1))
        .expect("记录命令");
    registry
        .record_transcript_output(
            &id,
            "cmd two\r\ngamma alpha\r\n",
            t0 + time::Duration::seconds(1),
        )
        .expect("记录输出");

    let tail = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(Some(3), None, None, None, false),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(tail, "alpha one\nbeta alpha two\ngamma alpha");

    // raw PTY 内容不得泄漏。
    let noisy = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(None, None, Some("raw-only-noise"), Some(1), false),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(noisy, "");

    // only-matches 不跨命令、不带 beta。
    let line = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(None, None, Some("alpha"), Some(1), true),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(line, "gamma alpha");
}

/// structured：没有 transcript 时返回空，不退回 raw PTY。
#[test]
fn structured_output_is_empty_without_transcript() {
    let (mut registry, terminal_id) = structured_terminal();
    let id = terminal(&terminal_id);
    registry
        .report_event(
            &id,
            EventKind::Output {
                data: BASE64_STANDARD.encode(b"raw text alpha\r\n"),
                stream: Stream::Stdout,
            },
        )
        .expect("注入 raw 输出");

    let tail = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(Some(5), None, None, None, false),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(tail, "");

    let found = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(None, None, Some("alpha"), Some(1), false),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(found, "");
}

/// structured：`--only-matches` 不跨越命令块边界（不夹带前一条命令）。
#[test]
fn only_matches_does_not_cross_command_boundaries() {
    let (mut registry, terminal_id) = structured_terminal();
    let id = terminal(&terminal_id);
    let t0 = base_time();

    registry
        .record_transcript_command(&id, "one", t0)
        .expect("记录命令");
    registry
        .record_transcript_output(&id, "one\r\nbeta\r\n", t0)
        .expect("记录输出");
    registry
        .record_transcript_command(&id, "two", t0 + time::Duration::seconds(1))
        .expect("记录命令");
    registry
        .record_transcript_output(
            &id,
            "two\r\ngamma alpha\r\n",
            t0 + time::Duration::seconds(1),
        )
        .expect("记录输出");

    let line = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(None, None, Some("alpha"), Some(1), true),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(line, "gamma alpha", "不得跨命令夹带 beta");
}

/// structured：UTF-8 尾部字符与历史不足。
#[test]
fn structured_tail_chars_and_short_history() {
    let (mut registry, terminal_id) = structured_terminal();
    let id = terminal(&terminal_id);
    let t0 = base_time();

    registry
        .record_transcript_command(&id, "echo", t0)
        .expect("记录命令");
    registry
        .record_transcript_output(&id, "echo\r\n你好世界\r\n", t0)
        .expect("记录输出");

    let chars = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(None, Some(2), None, None, false),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(chars, "世界");

    let all = registry
        .output_text(
            &locator(&terminal_id),
            &output_params(Some(100), None, None, None, false),
        )
        .expect("output 应成功")
        .text;
    assert_eq!(all, "你好世界");
}
