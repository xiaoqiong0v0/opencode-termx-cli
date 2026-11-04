//! 连接配置：全局参数覆盖 + 显式配置目录/endpoint。
//!
//! 不再推导平台默认配置目录：调用方必须给出 `--config-dir` / `--endpoint`
//! 之一（或非空 `TERMX_CONFIG_DIR`）。token 优先级为
//! `--token` > `TERMX_TOKEN` > `<config-dir>/token`。

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

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
    /// 调用者 opencode 会话 ID；缺省取 `TERMX_ROOT_SESSION_ID`，再缺省 `default`。
    pub root_session_id: Option<&'a str>,
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
/// - 只给 `--endpoint`：token 按 `--token` > `TERMX_TOKEN` > token 文件解析；
///   既没有配置目录也没有 token 时返回用法错误。
/// - 只给 `--config-dir`（或非空 `TERMX_CONFIG_DIR`）：读取 `port` 与 token。
/// - 两者都没有：返回用法错误，不再回退平台默认目录。
pub fn resolve_client_config(overrides: ConnectionOverrides<'_>) -> Result<ClientConfig, CliError> {
    let ConnectionOverrides {
        config_dir,
        token,
        endpoint,
        root_session_id,
    } = overrides;
    let root_session_id = resolve_root_session_id(root_session_id);
    let config_dir = effective_config_dir(config_dir);
    let explicit_token = token.map(str::trim).filter(|value| !value.is_empty());
    let env_source = |key: &str| -> Option<OsString> {
        if key == TERMX_CONFIG_DIR_ENV {
            if let Some(dir) = config_dir.as_deref() {
                return Some(dir.as_os_str().to_owned());
            }
        }
        if key == TERMX_TOKEN_ENV {
            if let Some(value) = explicit_token {
                return Some(OsString::from(value));
            }
        }
        std::env::var_os(key)
    };

    match endpoint {
        Some(endpoint) => {
            let token = match explicit_token {
                Some(token) => token.to_string(),
                None => match config_dir.as_deref() {
                    Some(dir) => ClientConfig::discover_token_from_dir(dir, env_source)?,
                    None => non_empty_env(TERMX_TOKEN_ENV).ok_or_else(|| {
                        CliError::Usage(format!(
                            "只给出 --endpoint 时必须提供 --token 或非空 {TERMX_TOKEN_ENV}，或改用 --config-dir 读取 token"
                        ))
                    })?,
                },
            };
            Ok(ClientConfig::new(endpoint, token, DEFAULT_CLIENT_NAME)
                .with_root_session_id(root_session_id))
        }
        None => match config_dir.as_deref() {
            Some(dir) => Ok(ClientConfig::discover_from_dir(dir, env_source)?
                .with_root_session_id(root_session_id)),
            None => Err(CliError::Usage(
                "需要 --config-dir <DIR> 或 --endpoint <URL> 之一（也可用非空 TERMX_CONFIG_DIR）；不再推导平台默认配置目录"
                    .to_string(),
            )),
        },
    }
}

/// 有效配置目录：显式 `--config-dir` > 非空 `TERMX_CONFIG_DIR`。
fn effective_config_dir(explicit: Option<&Path>) -> Option<PathBuf> {
    explicit
        .map(PathBuf::from)
        .or_else(|| non_empty_env(TERMX_CONFIG_DIR_ENV).map(PathBuf::from))
}

/// 读取非空环境变量。
fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
