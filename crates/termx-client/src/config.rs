//! 客户端配置与发现规则。
//!
//! 发现优先级与服务端保持一致（`docs/PROTOCOL.md` §3）：配置目录取
//! `TERMX_CONFIG_DIR`，否则按平台默认；token 取 `TERMX_TOKEN`（非空覆盖），
//! 否则读配置目录下的 `token` 文件；端口读配置目录下的 `port` 文件后组装
//! `ws://127.0.0.1:<port>`。

use std::{
    ffi::OsString,
    fmt, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::ClientError;

/// 配置目录覆盖变量。
pub const TERMX_CONFIG_DIR_ENV: &str = "TERMX_CONFIG_DIR";
/// token 覆盖变量；非空时优先于 token 文件。
pub const TERMX_TOKEN_ENV: &str = "TERMX_TOKEN";
/// 端口文件名。
pub const PORT_FILE_NAME: &str = "port";
/// token 文件名。
pub const TOKEN_FILE_NAME: &str = "token";
/// `hello.client` 的默认标签。
pub const DEFAULT_CLIENT_NAME: &str = "termx-client";
/// 单次请求默认超时；可通过 [`ClientConfig::with_request_timeout`] 覆盖。
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// 平台配置目录名。
const TERMX_DIR_NAME: &str = "termx";
/// Unix 缺省配置子目录。
#[cfg(not(windows))]
const UNIX_CONFIG_SUBDIR: &str = ".config";
/// Windows 用户配置根变量。
#[cfg(windows)]
const APPDATA_ENV: &str = "APPDATA";
/// XDG 配置根变量。
#[cfg(not(windows))]
const XDG_CONFIG_HOME_ENV: &str = "XDG_CONFIG_HOME";
/// 用户主目录变量。
#[cfg(not(windows))]
const HOME_ENV: &str = "HOME";

/// 客户端连接配置。
///
/// `Debug` 为手写实现：token 只显示是否已设置，绝不写入日志。
#[derive(Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// WebSocket 地址，形如 `ws://127.0.0.1:45678`；仅允许环回字面量。
    pub endpoint: String,
    /// `hello.token` 使用的鉴权 token。
    pub token: String,
    /// `hello.client` 标签；仅用于服务端日志，不参与归属判断。
    pub client: String,
    /// 单次请求等待响应的超时；超时返回 [`ClientError::Timeout`]。
    pub request_timeout: Duration,
}

impl fmt::Debug for ClientConfig {
    /// 遮蔽 token，只暴露是否为空。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let token = if self.token.is_empty() {
            "<empty>"
        } else {
            "<redacted>"
        };
        formatter
            .debug_struct("ClientConfig")
            .field("endpoint", &self.endpoint)
            .field("token", &token)
            .field("client", &self.client)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl ClientConfig {
    /// 直接构造配置；请求超时使用 [`DEFAULT_REQUEST_TIMEOUT`]。
    pub fn new(
        endpoint: impl Into<String>,
        token: impl Into<String>,
        client: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: token.into(),
            client: client.into(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// 覆盖单次请求超时。
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// 按当前进程环境发现配置。
    ///
    /// 端口/token 文件缺失或格式非法时按 [`ClientError::Connect`] 返回，
    /// 因为发现阶段属于「建立连接」的一部分。
    pub fn discover() -> Result<Self, ClientError> {
        Self::discover_with(|key| std::env::var_os(key))
    }

    /// 使用可注入的环境源发现配置（便于测试，也可供上层复用自定义环境源）。
    pub fn discover_with<F>(env: F) -> Result<Self, ClientError>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        let config_dir = resolve_config_dir(&env)?;
        let port = read_port(&config_dir)?;
        let token = read_token(&config_dir, &env)?;
        Ok(Self {
            endpoint: format!("ws://127.0.0.1:{port}"),
            token,
            client: DEFAULT_CLIENT_NAME.to_string(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        })
    }

    /// 只按优先级发现 token：非空 `TERMX_TOKEN` > token 文件。
    ///
    /// 供显式 `--endpoint` 场景使用：地址由调用方给出时不需要 `port` 文件，
    /// 但 token 仍遵循同一优先级，避免与文档不一致的回退行为。
    pub fn discover_token_with<F>(env: F) -> Result<String, ClientError>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        let config_dir = resolve_config_dir(&env)?;
        read_token(&config_dir, &env)
    }
}

/// 解析配置目录：`TERMX_CONFIG_DIR` 优先，否则按平台默认。
fn resolve_config_dir<F>(env: &F) -> Result<PathBuf, ClientError>
where
    F: Fn(&str) -> Option<OsString>,
{
    if let Some(dir) = env(TERMX_CONFIG_DIR_ENV).filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(dir));
    }

    #[cfg(windows)]
    {
        env(APPDATA_ENV)
            .filter(|value| !value.is_empty())
            .map(|appdata| PathBuf::from(appdata).join(TERMX_DIR_NAME))
            .ok_or_else(|| {
                ClientError::Connect(format!("无法确定配置目录：环境变量 {APPDATA_ENV} 缺失"))
            })
    }

    #[cfg(not(windows))]
    {
        let base = env(XDG_CONFIG_HOME_ENV)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                env(HOME_ENV)
                    .filter(|value| !value.is_empty())
                    .map(|home| PathBuf::from(home).join(UNIX_CONFIG_SUBDIR))
            })
            .ok_or_else(|| {
                ClientError::Connect(format!(
                    "无法确定配置目录：{XDG_CONFIG_HOME_ENV} 与 {HOME_ENV} 均缺失"
                ))
            })?;
        Ok(base.join(TERMX_DIR_NAME))
    }
}

/// 读取端口文件并校验为有效非零端口。
fn read_port(config_dir: &Path) -> Result<u16, ClientError> {
    let path = config_dir.join(PORT_FILE_NAME);
    let text = read_trimmed(&path, "端口")?;
    let port: u16 = text
        .parse()
        .map_err(|_| ClientError::Connect(format!("端口文件内容非法: {}", path.display())))?;
    if port == 0 {
        return Err(ClientError::Connect(format!(
            "端口文件内容非法（0 不是可连接端口）: {}",
            path.display()
        )));
    }
    Ok(port)
}

/// 读取 token：非空 `TERMX_TOKEN` 覆盖 token 文件。
fn read_token<F>(config_dir: &Path, env: &F) -> Result<String, ClientError>
where
    F: Fn(&str) -> Option<OsString>,
{
    if let Some(token) = env(TERMX_TOKEN_ENV)
        .and_then(|value| value.into_string().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Ok(token);
    }
    read_trimmed(&config_dir.join(TOKEN_FILE_NAME), "token")
}

/// 读取文本文件并去除首尾空白；空内容视为非法。
fn read_trimmed(path: &Path, label: &str) -> Result<String, ClientError> {
    let text = fs::read_to_string(path).map_err(|error| {
        ClientError::Connect(format!("读取{label}文件失败 {}: {error}", path.display()))
    })?;
    let text = text.trim();
    if text.is_empty() {
        return Err(ClientError::Connect(format!(
            "{label}文件为空: {}",
            path.display()
        )));
    }
    Ok(text.to_string())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, ffi::OsString};

    use super::*;

    /// 把 map 适配成 `discover_with` 需要闭包；缺省值返回 `None`。
    fn env_from<'a>(map: &'a HashMap<&'a str, OsString>) -> impl Fn(&str) -> Option<OsString> + 'a {
        move |key| map.get(key).cloned()
    }

    #[test]
    fn discover_reads_port_and_token_file() {
        let dir = tempfile::tempdir().expect("临时目录");
        fs::write(dir.path().join(PORT_FILE_NAME), "45678\n").expect("写端口文件");
        fs::write(dir.path().join(TOKEN_FILE_NAME), " file-token \n").expect("写 token 文件");

        let mut env = HashMap::new();
        env.insert(TERMX_CONFIG_DIR_ENV, dir.path().as_os_str().to_owned());

        let config = ClientConfig::discover_with(env_from(&env)).expect("发现配置");
        assert_eq!(config.endpoint, "ws://127.0.0.1:45678");
        assert_eq!(config.token, "file-token");
        assert_eq!(config.client, DEFAULT_CLIENT_NAME);
    }

    #[test]
    fn discover_prefers_non_empty_token_env() {
        let dir = tempfile::tempdir().expect("临时目录");
        fs::write(dir.path().join(PORT_FILE_NAME), "1234").expect("写端口文件");
        fs::write(dir.path().join(TOKEN_FILE_NAME), "file-token").expect("写 token 文件");

        let mut env = HashMap::new();
        env.insert(TERMX_CONFIG_DIR_ENV, dir.path().as_os_str().to_owned());
        env.insert(TERMX_TOKEN_ENV, OsString::from(" env-token "));

        let config = ClientConfig::discover_with(env_from(&env)).expect("发现配置");
        assert_eq!(config.token, "env-token");
    }

    #[test]
    fn discover_rejects_missing_or_invalid_port() {
        let dir = tempfile::tempdir().expect("临时目录");
        fs::write(dir.path().join(TOKEN_FILE_NAME), "file-token").expect("写 token 文件");

        let mut env = HashMap::new();
        env.insert(TERMX_CONFIG_DIR_ENV, dir.path().as_os_str().to_owned());
        let error = ClientConfig::discover_with(env_from(&env)).expect_err("缺少端口应失败");
        assert!(matches!(error, ClientError::Connect(_)));

        fs::write(dir.path().join(PORT_FILE_NAME), "0").expect("写端口文件");
        let error = ClientConfig::discover_with(env_from(&env)).expect_err("端口 0 应失败");
        assert!(matches!(error, ClientError::Connect(_)));
    }

    #[test]
    fn discover_rejects_missing_token() {
        let dir = tempfile::tempdir().expect("临时目录");
        fs::write(dir.path().join(PORT_FILE_NAME), "4321").expect("写端口文件");

        let mut env = HashMap::new();
        env.insert(TERMX_CONFIG_DIR_ENV, dir.path().as_os_str().to_owned());
        let error = ClientConfig::discover_with(env_from(&env)).expect_err("缺少 token 应失败");
        assert!(matches!(error, ClientError::Connect(_)));
    }

    #[test]
    fn debug_masks_token() {
        let config = ClientConfig::new("ws://127.0.0.1:1", "super-secret", "test");
        let text = format!("{config:?}");
        assert!(
            !text.contains("super-secret"),
            "Debug 不应泄露 token: {text}"
        );
        assert!(text.contains("<redacted>"), "应标记 token 已遮蔽: {text}");
    }

    #[test]
    fn request_timeout_defaults_and_is_configurable() {
        let config = ClientConfig::new("ws://127.0.0.1:1", "t", "test");
        assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);

        let config = config.with_request_timeout(Duration::from_millis(250));
        assert_eq!(config.request_timeout, Duration::from_millis(250));
    }

    #[test]
    fn discover_token_ignores_missing_port_file() {
        let dir = tempfile::tempdir().expect("临时目录");
        fs::write(dir.path().join(TOKEN_FILE_NAME), "file-token").expect("写 token 文件");

        let mut env = HashMap::new();
        env.insert(TERMX_CONFIG_DIR_ENV, dir.path().as_os_str().to_owned());
        let token =
            ClientConfig::discover_token_with(env_from(&env)).expect("无端口文件也应能发现 token");
        assert_eq!(token, "file-token");
    }
}
