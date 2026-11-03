//! termx-cli 可执行入口：解析子命令、建立 tokio runtime、映射退出码。
//!
//! 退出码约定：
//! - `exec` 原样返回子进程退出码（0..=255），`interrupted` 返回 130；
//! - 用法/未实现错误返回 2；
//! - 连接、协议与本地 I/O 错误返回 1。

mod cli;
mod commands;
mod config;
mod error;
mod output;

use clap::Parser;

use crate::{cli::Cli, error::CliError};

fn main() {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("termx-cli: 创建 tokio runtime 失败: {error}");
            std::process::exit(1);
        }
    };

    let code = match runtime.block_on(commands::run(&cli)) {
        Ok(code) => code,
        Err(error) => {
            report_error(&error);
            error.exit_code()
        }
    };
    // 显式退出以便及时结束后台任务；连接已在各子命令内 close。
    std::process::exit(code);
}

/// 把错误写到 stderr；不回显 token 或请求原文。
fn report_error(error: &CliError) {
    eprintln!("termx-cli: {error}");
}
