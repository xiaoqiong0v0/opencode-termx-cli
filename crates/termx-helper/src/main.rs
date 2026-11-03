//! `termx-helper` 可执行入口。
//!
//! - `termx-helper --version`：打印 `termx-helper/<主版本>` 后退出；
//! - `termx-helper` / `termx-helper serve`：从 stdin 读取 JSON Lines 协议。
//!
//! stdout 只用于协议帧（`serve`）或版本号（`--version`）；用法说明写 stderr。

use std::process::ExitCode;

use termx_protocol::HELPER_PROTOCOL_VERSION;

/// 命令行用法。
const USAGE: &str = "用法: termx-helper [serve] | termx-helper --version";

/// 未知参数退出码。
const EXIT_USAGE: u8 = 2;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => run_serve(),
        [arg] if arg == "serve" => run_serve(),
        [arg] if arg == "--version" || arg == "-V" => {
            println!("{HELPER_PROTOCOL_VERSION}");
            ExitCode::SUCCESS
        }
        [arg] if arg == "--help" || arg == "-h" => {
            eprintln!("{USAGE}");
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(EXIT_USAGE)
        }
    }
}

/// 构建多线程运行时并执行 `serve`。
fn run_serve() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("启动 tokio 运行时失败: {err}");
            return ExitCode::from(termx_helper::EXIT_INTERNAL);
        }
    };
    runtime.block_on(termx_helper::serve())
}
