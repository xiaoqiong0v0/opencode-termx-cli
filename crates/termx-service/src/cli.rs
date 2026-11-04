//! termx-service 命令行参数解析。
//!
//! 契约：
//!
//! ```text
//! termx-service --config-dir <DIR> [--port <u16>] [--token-file <FILE>]
//!               [--log-dir <DIR>] [--log-enabled true|false] [--log-max-days <N>]
//!               [--web-port <u16>] [--web-allow-remote true|false]
//! ```
//!
//! `--config-dir` 必填；缺失时接受非空 `TERMX_CONFIG_DIR` 作为显式来源，
//! 两者都没有则报错。解析支持 `--key value` 与 `--key=value` 两种形式，
//! 未知参数报错。Web 登录密码只走环境变量 `TERMX_WEB_PASSWORD`，不进命令行。

use std::{ffi::OsString, net::SocketAddr, path::PathBuf};

use crate::config::{DEFAULT_BIND_ADDR, DEFAULT_LOG_MAX_DAYS, ServiceConfig};

/// 配置目录环境变量：仅作为显式来源，不与平台默认目录组合。
const CONFIG_DIR_ENV: &str = "TERMX_CONFIG_DIR";
/// `--config-dir` 参数名。
const CONFIG_DIR_ARG: &str = "--config-dir";
/// `--port` 参数名。
const PORT_ARG: &str = "--port";
/// `--token-file` 参数名。
const TOKEN_FILE_ARG: &str = "--token-file";
/// `--log-dir` 参数名。
const LOG_DIR_ARG: &str = "--log-dir";
/// `--log-enabled` 参数名。
const LOG_ENABLED_ARG: &str = "--log-enabled";
/// `--log-max-days` 参数名。
const LOG_MAX_DAYS_ARG: &str = "--log-max-days";
/// `--web-port` 参数名。
const WEB_PORT_ARG: &str = "--web-port";
/// `--web-allow-remote` 参数名。
const WEB_ALLOW_REMOTE_ARG: &str = "--web-allow-remote";

/// 命令行用法说明。
pub const USAGE: &str = "\
用法: termx-service --config-dir <DIR> [--port <u16>] [--token-file <FILE>]
                    [--log-dir <DIR>] [--log-enabled true|false] [--log-max-days <N>]
                    [--web-port <u16>] [--web-allow-remote true|false]

选项:
  --config-dir <DIR>    必填；token/port 文件所在目录（也可用非空 TERMX_CONFIG_DIR）
  --port <u16>          监听 127.0.0.1 的端口，缺省 0（由系统分配）
  --token-file <FILE>   token 文件路径，缺省 <DIR>/token
  --log-dir <DIR>       文件日志目录；--log-enabled true 时必填
  --log-enabled <bool>  是否写文件日志（true/false），缺省 false
  --log-max-days <N>    日志保留天数（含当天），缺省 7
  --web-port <u16>      Web 服务端口，缺省 0（不启动 Web 服务）
  --web-allow-remote <bool>
                        是否允许远程访问 Web，缺省 false（仅 127.0.0.1）
  -h, --help            打印本帮助

环境变量:
  TERMX_WEB_PASSWORD    Web 登录密码；远程访问时必填（不进命令行/日志）";

/// 解析后的命令行结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// 正常运行，携带服务参数。
    Run(ServiceArgs),
    /// 请求打印用法。
    Help,
}

/// 服务命令行参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceArgs {
    /// token/port 文件所在目录（必填）。
    pub config_dir: PathBuf,
    /// 监听端口；`0` 表示由 OS 分配。
    pub port: u16,
    /// 自定义 token 文件；`None` 表示使用 `<config_dir>/token`。
    pub token_file: Option<PathBuf>,
    /// 文件日志目录；`log_enabled` 为 `true` 时必须有值。
    pub log_dir: Option<PathBuf>,
    /// 是否写文件日志。
    pub log_enabled: bool,
    /// 文件日志保留天数（含当天）。
    pub log_max_days: u32,
    /// Web 服务端口；`0` 表示不启动 Web 服务。
    pub web_port: u16,
    /// 是否允许 Web 远程访问；缺省 `false`。
    pub web_allow_remote: bool,
}

impl ServiceArgs {
    /// 转换为服务配置；参数解析阶段已完成全部校验，因此不会失败。
    #[must_use]
    pub fn into_config(self) -> ServiceConfig {
        let mut config = ServiceConfig::from_env(self.config_dir);
        config.bind_addr = SocketAddr::new(DEFAULT_BIND_ADDR.ip(), self.port);
        config.token_file = self.token_file;
        config.log_enabled = self.log_enabled;
        config.log_dir = self.log_dir;
        config.log_max_days = self.log_max_days;
        config.web_port = self.web_port;
        config.web_allow_remote = self.web_allow_remote;
        config
    }
}

/// 参数解析错误。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CliError {
    /// 既没有 `--config-dir`，也没有非空 `TERMX_CONFIG_DIR`。
    #[error("缺少必需参数 --config-dir（或设置非空环境变量 TERMX_CONFIG_DIR）")]
    MissingConfigDir,
    /// 参数缺少取值。
    #[error("参数 {arg} 缺少取值")]
    MissingValue {
        /// 参数名。
        arg: &'static str,
    },
    /// 参数取值为空。
    #[error("参数 {arg} 的取值不能为空")]
    EmptyValue {
        /// 参数名。
        arg: &'static str,
    },
    /// 参数取值无法解析。
    #[error("参数 {arg} 的取值非法: {value}")]
    InvalidValue {
        /// 参数名。
        arg: &'static str,
        /// 原始取值。
        value: String,
    },
    /// 无法识别的参数。
    #[error("未知参数: {0}")]
    UnknownArg(String),
    /// 启用文件日志却没有提供日志目录。
    #[error("--log-enabled true 时必须提供 --log-dir")]
    LogDirRequired,
}

/// 解析过程中的原始参数值。
struct RawArgs {
    /// 配置目录；可能来自参数或环境变量。
    config_dir: Option<PathBuf>,
    /// 监听端口，缺省 0。
    port: u16,
    /// token 文件覆盖。
    token_file: Option<PathBuf>,
    /// 文件日志目录。
    log_dir: Option<PathBuf>,
    /// 是否写文件日志。
    log_enabled: bool,
    /// 日志保留天数。
    log_max_days: u32,
    /// Web 服务端口。
    web_port: u16,
    /// 是否允许 Web 远程访问。
    web_allow_remote: bool,
}

impl RawArgs {
    /// 全部取缺省值。
    fn new() -> Self {
        Self {
            config_dir: None,
            port: 0,
            token_file: None,
            log_dir: None,
            log_enabled: false,
            log_max_days: DEFAULT_LOG_MAX_DAYS,
            web_port: 0,
            web_allow_remote: false,
        }
    }

    /// 写入一个已识别参数的取值。
    fn apply(&mut self, name: &'static str, value: String) -> Result<(), CliError> {
        match name {
            CONFIG_DIR_ARG => self.config_dir = Some(PathBuf::from(value)),
            PORT_ARG => self.port = parse_u16(PORT_ARG, value)?,
            TOKEN_FILE_ARG => self.token_file = Some(PathBuf::from(value)),
            LOG_DIR_ARG => self.log_dir = Some(PathBuf::from(value)),
            LOG_ENABLED_ARG => self.log_enabled = parse_bool(LOG_ENABLED_ARG, value)?,
            LOG_MAX_DAYS_ARG => self.log_max_days = parse_max_days(value)?,
            WEB_PORT_ARG => self.web_port = parse_u16(WEB_PORT_ARG, value)?,
            WEB_ALLOW_REMOTE_ARG => {
                self.web_allow_remote = parse_bool(WEB_ALLOW_REMOTE_ARG, value)?;
            }
            _ => unreachable!("调用方已过滤未知参数"),
        }
        Ok(())
    }
}

/// 从迭代器与可注入环境解析参数。
///
/// `env` 只用于读取 `TERMX_CONFIG_DIR`；显式 `--config-dir` 优先。
pub fn parse_args<I, F>(args: I, env: F) -> Result<Command, CliError>
where
    I: IntoIterator<Item = String>,
    F: Fn(&str) -> Option<OsString>,
{
    let mut iter = args.into_iter();
    let mut raw = RawArgs::new();

    while let Some(arg) = iter.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(Command::Help);
        }

        let (name, inline) = split_arg(&arg);
        let Some(arg_name) = canonical_arg(name) else {
            return Err(CliError::UnknownArg(name.to_string()));
        };
        let value = take_value(arg_name, inline, &mut iter)?;
        raw.apply(arg_name, value)?;
    }

    let config_dir = match raw.config_dir {
        Some(dir) => dir,
        None => env(CONFIG_DIR_ENV)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .ok_or(CliError::MissingConfigDir)?,
    };

    if raw.log_enabled && raw.log_dir.is_none() {
        return Err(CliError::LogDirRequired);
    }

    Ok(Command::Run(ServiceArgs {
        config_dir,
        port: raw.port,
        token_file: raw.token_file,
        log_dir: raw.log_dir,
        log_enabled: raw.log_enabled,
        log_max_days: raw.log_max_days,
        web_port: raw.web_port,
        web_allow_remote: raw.web_allow_remote,
    }))
}

/// 把参数名归一为静态常量；未知参数返回 `None`。
fn canonical_arg(name: &str) -> Option<&'static str> {
    match name {
        CONFIG_DIR_ARG => Some(CONFIG_DIR_ARG),
        PORT_ARG => Some(PORT_ARG),
        TOKEN_FILE_ARG => Some(TOKEN_FILE_ARG),
        LOG_DIR_ARG => Some(LOG_DIR_ARG),
        LOG_ENABLED_ARG => Some(LOG_ENABLED_ARG),
        LOG_MAX_DAYS_ARG => Some(LOG_MAX_DAYS_ARG),
        WEB_PORT_ARG => Some(WEB_PORT_ARG),
        WEB_ALLOW_REMOTE_ARG => Some(WEB_ALLOW_REMOTE_ARG),
        _ => None,
    }
}

/// 把参数拆成「名字」与可选的 `=取值`。
fn split_arg(arg: &str) -> (&str, Option<String>) {
    match arg.split_once('=') {
        Some((name, value)) => (name, Some(value.to_string())),
        None => (arg, None),
    }
}

/// 取参数值：优先 `=` 后的内联值，否则消费下一个参数。
fn take_value<I>(
    arg: &'static str,
    inline: Option<String>,
    iter: &mut I,
) -> Result<String, CliError>
where
    I: Iterator<Item = String>,
{
    let value = match inline {
        Some(value) => value,
        None => iter.next().ok_or(CliError::MissingValue { arg })?,
    };
    if value.is_empty() {
        return Err(CliError::EmptyValue { arg });
    }
    Ok(value)
}

/// 解析端口（`u16`，允许 0）。
fn parse_u16(arg: &'static str, value: String) -> Result<u16, CliError> {
    value
        .parse::<u16>()
        .map_err(|_| CliError::InvalidValue { arg, value })
}

/// 解析布尔开关；接受 true/false（大小写不敏感）与 1/0、on/off、yes/no。
fn parse_bool(arg: &'static str, value: String) -> Result<bool, CliError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "on" | "yes" => Ok(true),
        "false" | "0" | "off" | "no" => Ok(false),
        _ => Err(CliError::InvalidValue { arg, value }),
    }
}

/// 解析日志保留天数；必须为正整数。
fn parse_max_days(value: String) -> Result<u32, CliError> {
    value
        .parse::<u32>()
        .ok()
        .filter(|days| *days > 0)
        .ok_or_else(|| CliError::InvalidValue {
            arg: LOG_MAX_DAYS_ARG,
            value: value.clone(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    /// 用字符串参数与内存环境表调用解析。
    fn parse(args: &[&str], env: &[(&str, &str)]) -> Result<Command, CliError> {
        let args: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        parse_args(args, |key| {
            env.iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| OsString::from(*value))
        })
    }

    /// 断言解析为正常运行并返回参数。
    fn run(args: &[&str], env: &[(&str, &str)]) -> ServiceArgs {
        match parse(args, env).expect("应解析成功") {
            Command::Run(args) => args,
            Command::Help => panic!("本应解析为运行参数"),
        }
    }

    #[test]
    fn missing_config_dir_with_empty_env_is_error() {
        assert_eq!(parse(&[], &[]), Err(CliError::MissingConfigDir));
        // 空环境变量不算显式来源。
        assert_eq!(
            parse(&[], &[(CONFIG_DIR_ENV, "")]),
            Err(CliError::MissingConfigDir)
        );
    }

    #[test]
    fn config_dir_comes_from_arg_or_non_empty_env() {
        let from_arg = run(&["--config-dir", "/arg"], &[]);
        assert_eq!(from_arg.config_dir, PathBuf::from("/arg"));

        let from_env = run(&[], &[(CONFIG_DIR_ENV, "/env")]);
        assert_eq!(from_env.config_dir, PathBuf::from("/env"));

        // 命令行参数优先于环境变量。
        let arg_wins = run(&["--config-dir=/arg"], &[(CONFIG_DIR_ENV, "/env")]);
        assert_eq!(arg_wins.config_dir, PathBuf::from("/arg"));
    }

    #[test]
    fn supports_space_and_equals_forms_for_every_option() {
        let spaced = run(
            &[
                "--config-dir",
                "/cfg",
                "--port",
                "8080",
                "--token-file",
                "/t/token",
                "--log-dir",
                "/logs",
                "--log-enabled",
                "true",
                "--log-max-days",
                "3",
                "--web-port",
                "8138",
                "--web-allow-remote",
                "true",
            ],
            &[],
        );
        let equals = run(
            &[
                "--config-dir=/cfg",
                "--port=8080",
                "--token-file=/t/token",
                "--log-dir=/logs",
                "--log-enabled=true",
                "--log-max-days=3",
                "--web-port=8138",
                "--web-allow-remote=true",
            ],
            &[],
        );
        assert_eq!(spaced, equals);
        assert_eq!(spaced.port, 8080);
        assert_eq!(spaced.token_file, Some(PathBuf::from("/t/token")));
        assert_eq!(spaced.log_dir, Some(PathBuf::from("/logs")));
        assert!(spaced.log_enabled);
        assert_eq!(spaced.log_max_days, 3);
        assert_eq!(spaced.web_port, 8138);
        assert!(spaced.web_allow_remote);
    }

    #[test]
    fn defaults_are_port_zero_logs_disabled_and_seven_days() {
        let args = run(&["--config-dir", "/cfg"], &[]);
        assert_eq!(args.port, 0);
        assert_eq!(args.token_file, None);
        assert_eq!(args.log_dir, None);
        assert!(!args.log_enabled);
        assert_eq!(args.log_max_days, DEFAULT_LOG_MAX_DAYS);
        assert_eq!(args.web_port, 0, "Web 默认不启动");
        assert!(!args.web_allow_remote, "Web 默认仅回环");
    }

    #[test]
    fn rejects_invalid_port_and_log_max_days() {
        for value in ["abc", "70000", "-1"] {
            assert!(
                matches!(
                    parse(&["--config-dir", "/cfg", "--port", value], &[]),
                    Err(CliError::InvalidValue { arg: PORT_ARG, .. })
                ),
                "非法端口 {value:?} 应被拒绝"
            );
            assert!(
                matches!(
                    parse(&["--config-dir", "/cfg", "--web-port", value], &[]),
                    Err(CliError::InvalidValue {
                        arg: WEB_PORT_ARG,
                        ..
                    })
                ),
                "非法 Web 端口 {value:?} 应被拒绝"
            );
        }
        for value in ["0", "abc", "-1"] {
            assert!(
                matches!(
                    parse(&["--config-dir", "/cfg", "--log-max-days", value], &[]),
                    Err(CliError::InvalidValue {
                        arg: LOG_MAX_DAYS_ARG,
                        ..
                    })
                ),
                "非法保留天数 {value:?} 应被拒绝"
            );
        }
        assert!(matches!(
            parse(
                &["--config-dir", "/cfg", "--web-allow-remote", "maybe"],
                &[]
            ),
            Err(CliError::InvalidValue {
                arg: WEB_ALLOW_REMOTE_ARG,
                ..
            })
        ));
    }

    #[test]
    fn log_enabled_requires_log_dir() {
        assert_eq!(
            parse(&["--config-dir", "/cfg", "--log-enabled", "true"], &[]),
            Err(CliError::LogDirRequired)
        );
        // 关闭文件日志时允许不带日志目录。
        let disabled = run(&["--config-dir", "/cfg", "--log-enabled=false"], &[]);
        assert!(!disabled.log_enabled);
        assert!(disabled.log_dir.is_none());
    }

    #[test]
    fn rejects_unknown_args_and_missing_values() {
        assert!(matches!(
            parse(&["--config-dir", "/cfg", "--nope"], &[]),
            Err(CliError::UnknownArg(arg)) if arg == "--nope"
        ));
        for arg in [
            CONFIG_DIR_ARG,
            PORT_ARG,
            TOKEN_FILE_ARG,
            LOG_DIR_ARG,
            LOG_ENABLED_ARG,
            LOG_MAX_DAYS_ARG,
            WEB_PORT_ARG,
            WEB_ALLOW_REMOTE_ARG,
        ] {
            assert_eq!(
                parse(&[arg], &[]),
                Err(CliError::MissingValue { arg }),
                "参数 {arg} 缺值应报错"
            );
            assert_eq!(
                parse(&[&format!("{arg}=")], &[]),
                Err(CliError::EmptyValue { arg }),
                "参数 {arg} 空值应报错"
            );
        }
    }

    #[test]
    fn help_is_recognized_without_config_dir() {
        assert_eq!(parse(&["--help"], &[]), Ok(Command::Help));
        assert_eq!(parse(&["-h"], &[]), Ok(Command::Help));
    }

    #[test]
    fn into_config_sets_bind_addr_token_port_and_log_fields() {
        let args = run(
            &[
                "--config-dir",
                "/cfg",
                "--port",
                "4321",
                "--log-dir",
                "/logs",
                "--log-enabled",
                "true",
                "--log-max-days",
                "2",
                "--web-port",
                "8138",
                "--web-allow-remote",
                "true",
            ],
            &[],
        );
        let config = args.into_config();
        assert_eq!(config.config_dir, PathBuf::from("/cfg"));
        assert_eq!(
            config.bind_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4321)
        );
        assert_eq!(config.token_path(), PathBuf::from("/cfg/token"));
        assert_eq!(config.port_path(), PathBuf::from("/cfg/port"));
        assert!(config.log_enabled);
        assert_eq!(config.log_dir, Some(PathBuf::from("/logs")));
        assert_eq!(config.log_max_days, 2);
        assert_eq!(config.web_port, 8138);
        assert!(config.web_allow_remote);
    }
}
