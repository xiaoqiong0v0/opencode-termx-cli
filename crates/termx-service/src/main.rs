//! termx-service 可执行入口：解析显式参数、初始化 tracing、启动服务循环。

use termx_service::{Command, Service, USAGE, cli::parse_args, logging::init_logging};

/// 参数错误的退出码（与运行期错误区分）。
const EXIT_USAGE: i32 = 2;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = match parse_args(std::env::args().skip(1), |key| std::env::var_os(key)) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("termx-service: {error}\n\n{USAGE}");
            std::process::exit(EXIT_USAGE);
        }
    };

    let args = match command {
        Command::Help => {
            println!("{USAGE}");
            return Ok(());
        }
        Command::Run(args) => args,
    };

    let config = args.into_config();
    // guard 必须活到进程退出，确保非阻塞日志 worker 的尾部日志被 flush。
    let _log_guard = init_logging(&config);

    let service = Service::bind(config).await?;
    tracing::info!(addr = %service.local_addr(), "termx-service 启动");
    // 走 run_until：收到终止信号后停止 accept/ticker，并在退出前终止所有
    // 后端，避免 stdio 子进程随 service 退出而残留。
    service.run_until(shutdown_signal()).await?;
    Ok(())
}

/// 等待进程终止信号。
///
/// 所有平台监听 Ctrl-C；Unix 额外监听 `SIGTERM`（容器/系统服务常用）。
/// 监听失败时保持等待，避免把「无法捕获信号」误当成「收到信号」而立即退出。
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "监听 Ctrl-C 失败");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let terminate = async {
            match signal(SignalKind::terminate()) {
                Ok(mut stream) => {
                    stream.recv().await;
                }
                Err(error) => {
                    tracing::warn!(%error, "监听 SIGTERM 失败");
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::select! {
            () = ctrl_c => {}
            () = terminate => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;

    tracing::info!("收到终止信号，开始停止服务并清理后端");
}
