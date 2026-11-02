//! 交互识别集成测试探针（仅测试构建，不随 publish 带出 bin）。
//!
//! 父测试用 `--exact probe_child` 重新执行当前测试二进制；本模块在检测到
//! [`MODE_ENV`] 时写出指定 OSC 133 序列并保持运行，供 service 集成测试验证
//! `waiting_input{shell_integration}`，且不会自然退出而误报 `exited`。
//!
//! 用法（由父测试设置环境变量）：`--prompt`（默认，`133;A`）/ `--command`
//! （`133;C`）/ `--end`（`133;D`）/ `--echo`（写出 prompt 后逐行回显 stdin）。

use std::{
    io::{BufRead as _, Write},
    thread,
    time::Duration,
};

/// 探针模式环境变量；未设置时 [`run_if_child`] 直接返回，不影响普通测试。
pub const MODE_ENV: &str = "TERMX_INTERACTION_PROBE_MODE";
/// 子进程执行的测试名；父测试用 `--exact` 只跑它。
pub const CHILD_TEST: &str = "probe_child";

/// 探针保持运行的时长；需长于测试等待上限。
const RUN_DURATION: Duration = Duration::from_secs(30);

/// OSC 133 命令字符。
const PROMPT_START: &[u8] = b"\x1b]133;A\x07";
/// `133;C`：命令执行开始。
const COMMAND_START: &[u8] = b"\x1b]133;C\x07";
/// `133;D`：命令结束。
const COMMAND_END: &[u8] = b"\x1b]133;D\x07";

/// 以探针模式运行；未设置 [`MODE_ENV`] 时立即返回（父测试正常执行）。
pub fn run_if_child() {
    let Ok(mode) = std::env::var(MODE_ENV) else {
        return;
    };
    run(&mode);
}

/// 写出 OSC 序列后按模式保持运行。
fn run(mode: &str) {
    let sequence = match mode {
        "--command" => COMMAND_START,
        "--end" => COMMAND_END,
        _ => PROMPT_START,
    };

    let mut stdout = std::io::stdout();
    stdout.write_all(sequence).expect("写入 OSC 序列失败");
    stdout.flush().expect("刷新 stdout 失败");

    if mode == "--echo" {
        echo_stdin();
        return;
    }

    thread::sleep(RUN_DURATION);
}

/// 逐行读取 stdin 并回显到 stdout，保持进程存活。
fn echo_stdin() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let Ok(line) = line else {
            break;
        };
        if writeln!(stdout, "{line}").is_err() {
            break;
        }
        if stdout.flush().is_err() {
            break;
        }
    }
    // stdin EOF 后仍保持一段存活，避免测试窗口内自然退出。
    thread::sleep(RUN_DURATION);
}
