//! stdio 后端测试探针（仅测试构建，不随库发布）。
//!
//! 提供三种模式，供集成测试验证 cwd/env 传递、参数不经 shell、以及终止整棵
//! 进程树：
//! - `--probe <text>`：输出 `marker=yes|no`（当前目录是否存在 `marker.txt`）、
//!   `env=<TERMX_PROBE_ENV>` 与 `arg=<text>`，随后正常退出；
//! - `--child`：启动自身 `--sleep` 子进程，输出 `child=<pid>` 并等待其结束；
//! - `--sleep`：睡眠 30 秒，供父进程或测试终止。

use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

/// `--sleep` 的睡眠时长；需长于测试超时。
const SLEEP_DURATION: Duration = Duration::from_secs(30);

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--probe") => print_probe(args.get(1).map(String::as_str).unwrap_or("")),
        Some("--child") => spawn_child(),
        Some("--sleep") => thread::sleep(SLEEP_DURATION),
        _ => {
            eprintln!("用法: --probe <text> | --child | --sleep");
            std::process::exit(2);
        }
    }
}

/// 打印 cwd/env/参数观测结果；`println!` 在测试重定向下会刷新。
fn print_probe(text: &str) {
    let marker = std::path::Path::new("marker.txt").exists();
    let env = std::env::var("TERMX_PROBE_ENV").unwrap_or_default();
    println!("marker={}", if marker { "yes" } else { "no" });
    println!("env={env}");
    println!("arg={text}");
}

/// 启动一个长驻子进程并把它暴露给测试，随后等待其结束。
///
/// 先短暂睡眠，确保父进程（测试侧）已完成 Windows Job Object 关联，后代才会
/// 继承 Job；否则后代可能逃逸到 Job 之外，使“终止整棵树”断言不稳定。
fn spawn_child() {
    thread::sleep(Duration::from_millis(300));
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(error) => {
            eprintln!("无法定位自身可执行文件: {error}");
            std::process::exit(3);
        }
    };
    let mut child = match Command::new(exe)
        .arg("--sleep")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            eprintln!("启动子进程失败: {error}");
            std::process::exit(3);
        }
    };
    println!("child={}", child.id());
    let _ = child.wait();
}
