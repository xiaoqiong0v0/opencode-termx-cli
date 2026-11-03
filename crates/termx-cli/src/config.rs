//! 连接配置：全局参数覆盖 + 本机 ws/token 自动发现。

use std::{ffi::OsString, path::Path};

use termx_client::{ClientConfig, DEFAULT_CLIENT_NAME, TERMX_CONFIG_DIR_ENV, TERMX_TOKEN_ENV};

use crate::error::CliError;

/// root session id 环境变量。
pub const ROOT_SESSION_ID_ENV: &str = "TERMX_ROOT_SESSION_ID";
/// root session id 缺省值。
pub const DEFAULT_ROOT_SESSION_ID: &str = "default";

/// 连接发现覆盖项，来自全局参数。
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectionOverrides<'a> {
    /// 配置目录覆盖。
    pub config_dir: Option<&'a Path>,
    /// token 覆盖。
    pub token: Option<&'a str>,
    /// WebSocket 地址覆盖。
    pub endpoint: Option<&'a str>,
}

/// 解析 root session id：显式值 > 环境变量 > `default`。
pub fn resolve_root_session_id(explicit: Option<&str>) -> String {
    explicit
        .map(str::to_string)
        .or_else(|| non_empty_env(ROOT_SESSION_ID_ENV))
        .unwrap_or_else(|| DEFAULT_ROOT_SESSION_ID.to_string())
}

/// 按覆盖项发现客户端配置。
///
/// `--endpoint` 只替换地址；token 仍按 `--token` > `TERMX_TOKEN` > token 文件的
/// 优先级解析，不因缺少 `port` 文件而跳过 token 文件回退。
pub fn resolve_client_config(overrides: ConnectionOverrides<'_>) -> Result<ClientConfig, CliError> {
    let config_dir = overrides.config_dir;
    let token = overrides.token;
    let env_source = |key: &str| -> Option<OsString> {
        if key == TERMX_CONFIG_DIR_ENV {
            if let Some(dir) = config_dir {
                return Some(dir.as_os_str().to_owned());
            }
        }
        if key == TERMX_TOKEN_ENV {
            if let Some(value) = token {
                return Some(OsString::from(value));
            }
        }
        std::env::var_os(key)
    };

    match overrides.endpoint {
        Some(endpoint) => {
            let token = match token {
                Some(token) => token.to_string(),
                None => ClientConfig::discover_token_with(env_source)?,
            };
            Ok(ClientConfig::new(endpoint, token, DEFAULT_CLIENT_NAME))
        }
        None => Ok(ClientConfig::discover_with(env_source)?),
    }
}

/// 读取非空环境变量。
fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
