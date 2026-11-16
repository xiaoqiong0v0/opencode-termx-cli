//! 远端 fake child 探针：读取 helper 注入的 endpoint/token，连接 UDS 并发送事件。
//!
//! 由 `tests/shell_events.rs` 通过重新执行测试二进制触发：
//! `--exact shell_event_probe_child --nocapture` + 环境变量。

#![cfg(unix)]

use std::{env, io::Write, os::unix::net::UnixStream, thread, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};

/// 触发探针的测试函数名；父测试用它重新执行自身。
pub const PROBE_TEST_NAME: &str = "shell_event_probe_child";
/// 未指定时发送的事件序列。
const DEFAULT_SCRIPT: &str = "A,B,C,D";
/// 未指定环境变量时 `O` 帧携带的输出文本。
pub const DEFAULT_OUTPUT: &str = "TERMX_O_DATA_MARKER";
/// 发送事件后默认保持连接的时间，便于父测试在 terminate 前观察 socket。
const DEFAULT_HOLD_MS: u64 = 30_000;

/// 探针入口；缺少 endpoint 时安静返回（不是本进程想跑探针）。
pub fn run() {
    let Ok(endpoint) = env::var("TERMX_EVENT_ENDPOINT") else {
        return;
    };
    let token = env::var("TERMX_EVENT_TOKEN").unwrap_or_default();
    if let Ok(record) = env::var("TERMX_EVENT_PROBE_RECORD") {
        let _ = std::fs::write(record, endpoint.as_bytes());
    }

    // 先向 PTY 写一段稳定标记，供父测试验证 Output 先于 D 转发。
    println!("TERMX_BARRIER_OUTPUT");
    let _ = std::io::stdout().flush();

    let path = endpoint.strip_prefix("unix:").unwrap_or(&endpoint);
    let Ok(mut stream) = UnixStream::connect(path) else {
        return;
    };

    let sent_token = if env::var("TERMX_EVENT_PROBE_BAD_TOKEN").is_ok() {
        "wrong-token"
    } else {
        token.as_str()
    };
    if writeln!(stream, r#"{{"v":1,"token":"{sent_token}"}}"#).is_err() {
        return;
    }
    let _ = stream.flush();

    let script =
        env::var("TERMX_EVENT_PROBE_SCRIPT").unwrap_or_else(|_| DEFAULT_SCRIPT.to_string());
    for item in script
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let mut frame = serde_json::json!({ "v": 1, "event": item });
        if item == "C" || item == "D" || item == "O" {
            frame["id"] = serde_json::json!("cmd-1");
        }
        if item == "D" {
            frame["code"] = serde_json::json!(7);
        }
        if item == "O" {
            let output =
                env::var("TERMX_EVENT_PROBE_OUTPUT").unwrap_or_else(|_| DEFAULT_OUTPUT.to_string());
            // 与 termx-shell-events 解析口径一致：data 为 base64 编码的 UTF-8 文本。
            frame["data"] = serde_json::json!(STANDARD.encode(output.as_bytes()));
        }
        if writeln!(stream, "{frame}").is_err() {
            break;
        }
        let _ = stream.flush();
        thread::sleep(Duration::from_millis(50));
    }

    let hold_ms = env::var("TERMX_EVENT_PROBE_HOLD_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_HOLD_MS);
    thread::sleep(Duration::from_millis(hold_ms));
}
