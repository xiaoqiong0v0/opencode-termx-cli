//! termx-service 可执行入口：加载配置、初始化 tracing、启动服务循环。

use std::path::PathBuf;

use termx_service::{Service, ServiceConfig};

/// 未设置 `RUST_LOG` 时的默认日志级别。
const DEFAULT_LOG_FILTER: &str = "info";
/// token 文件命令行参数。
const TOKEN_FILE_ARG: &str = "--token-file";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let token_file = parse_token_file_arg(std::env::args().skip(1))?;
    let mut config = ServiceConfig::from_env()?;
    if let Some(path) = token_file {
        config = config.with_token_file(path);
    }
    let service = Service::bind(config).await?;

    tracing::info!(addr = %service.local_addr(), "termx-service 启动");
    service.run().await?;
    Ok(())
}

/// 解析 `--token-file <path>` / `--token-file=<path>`；缺失路径时报错。
fn parse_token_file_arg<I>(args: I) -> std::io::Result<Option<PathBuf>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut token_file = None;
    while let Some(arg) = args.next() {
        if arg == TOKEN_FILE_ARG {
            let value = args.next().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "--token-file 缺少路径")
            })?;
            token_file = Some(PathBuf::from(value));
        } else if let Some(value) = arg.strip_prefix(&format!("{TOKEN_FILE_ARG}=")) {
            if value.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--token-file 缺少路径",
                ));
            }
            token_file = Some(PathBuf::from(value));
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("未知参数: {arg}"),
            ));
        }
    }
    Ok(token_file)
}

/// 初始化 tracing，支持 `RUST_LOG` 覆盖默认级别。
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(DEFAULT_LOG_FILTER));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_file_arg_supports_both_forms_and_rejects_missing_value() {
        let path = parse_token_file_arg(["--token-file".to_string(), "a/token".to_string()])
            .expect("应解析成功")
            .expect("应给出路径");
        assert_eq!(path, PathBuf::from("a/token"));

        let path = parse_token_file_arg(["--token-file=b/token".to_string()])
            .expect("应解析成功")
            .expect("应给出路径");
        assert_eq!(path, PathBuf::from("b/token"));

        assert!(parse_token_file_arg(["--token-file".to_string()]).is_err());
        assert!(parse_token_file_arg(["--unknown".to_string()]).is_err());
    }
}
