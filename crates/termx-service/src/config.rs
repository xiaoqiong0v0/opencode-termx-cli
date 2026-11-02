//! 服务配置、配置目录解析与 token/port 文件读写。
//!
//! 配置目录不写死具体路径：优先 `TERMX_CONFIG_DIR`，否则按平台落到用户配置目录。
//! token 只写到 `config_dir/token`，端口写到 `config_dir/port`。

use std::{
    ffi::OsString,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

/// 覆盖配置目录的环境变量。
const TERMX_CONFIG_DIR_ENV: &str = "TERMX_CONFIG_DIR";
/// 直接提供 token 的环境变量；优先于默认 token 文件。
const TERMX_TOKEN_ENV: &str = "TERMX_TOKEN";
/// Linux/macOS 的 XDG 配置根目录环境变量。
const XDG_CONFIG_HOME_ENV: &str = "XDG_CONFIG_HOME";
/// Unix 用户主目录环境变量。
const HOME_ENV: &str = "HOME";
/// Windows 用户配置根目录环境变量。
const APPDATA_ENV: &str = "APPDATA";
/// 配置目录内的 termx 子目录名。
const TERMX_DIR_NAME: &str = "termx";
/// Unix 缺省配置子路径。
const UNIX_CONFIG_SUBDIR: &str = ".config";

/// token 文件名。
pub const TOKEN_FILE_NAME: &str = "token";
/// 监听端口文件名。
pub const PORT_FILE_NAME: &str = "port";

/// 默认监听地址：仅环回，端口由 OS 分配。
pub const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

/// 每个终端的事件缓冲默认容量。
pub const DEFAULT_EVENT_BUFFER_CAPACITY: usize = termx_core::DEFAULT_EVENT_BUFFER_CAPACITY;
/// 入站单帧默认上限（1 MiB）。
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;

/// 配置解析失败原因。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// 平台默认配置目录所需的用户环境变量缺失。
    #[error("环境变量 {name} 未设置，无法解析 termx 配置目录")]
    MissingEnv {
        /// 缺失的环境变量名。
        name: &'static str,
    },
}

/// 服务配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceConfig {
    /// token/port 文件所在目录。
    pub config_dir: PathBuf,
    /// 监听地址；缺省为 `127.0.0.1:0`。
    pub bind_addr: SocketAddr,
    /// 每个终端的事件缓冲容量；缺省 4096。
    pub event_buffer_capacity: usize,
    /// 自定义 token 文件路径；`Some` 时优先于 `TERMX_TOKEN` 与默认文件。
    pub token_file: Option<PathBuf>,
    /// 入站单帧字节上限；缺省 [`DEFAULT_MAX_FRAME_BYTES`]。
    pub max_frame_bytes: usize,
}

impl ServiceConfig {
    /// 用给定配置目录构造配置，监听地址取 [`DEFAULT_BIND_ADDR`]。
    pub fn new(config_dir: PathBuf) -> Self {
        Self {
            config_dir,
            bind_addr: DEFAULT_BIND_ADDR,
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            token_file: None,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }

    /// 覆盖监听地址（便于测试或后续配置项扩展）。
    #[must_use]
    pub fn with_bind_addr(mut self, bind_addr: SocketAddr) -> Self {
        self.bind_addr = bind_addr;
        self
    }

    /// 覆盖每个终端的事件缓冲容量。
    #[must_use]
    pub fn with_event_buffer_capacity(mut self, event_buffer_capacity: usize) -> Self {
        self.event_buffer_capacity = event_buffer_capacity;
        self
    }

    /// 覆盖 token 文件路径（对应 `--token-file`）。
    #[must_use]
    pub fn with_token_file(mut self, token_file: PathBuf) -> Self {
        self.token_file = Some(token_file);
        self
    }

    /// 覆盖入站单帧字节上限。
    #[must_use]
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes.max(1);
        self
    }

    /// 从进程环境解析配置。
    ///
    /// 配置目录优先 `TERMX_CONFIG_DIR`，否则按平台默认：
    /// - Linux/macOS：`$XDG_CONFIG_HOME/termx` 或 `~/.config/termx`
    /// - Windows：`%APPDATA%\termx`
    pub fn from_env() -> Result<Self, ConfigError> {
        let config_dir = resolve_config_dir(Platform::CURRENT, |key: &str| std::env::var_os(key))?;
        Ok(Self::new(config_dir))
    }

    /// token 文件路径；存在 `--token-file` 覆盖时使用覆盖值。
    pub fn token_path(&self) -> PathBuf {
        self.token_file
            .clone()
            .unwrap_or_else(|| self.config_dir.join(TOKEN_FILE_NAME))
    }

    /// 端口文件路径。
    pub fn port_path(&self) -> PathBuf {
        self.config_dir.join(PORT_FILE_NAME)
    }
}

/// 配置目录解析规则所属平台。
///
/// 独立成枚举是为了在任一平台上都能单测 Unix/Windows 两套规则。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Platform {
    /// Linux / macOS。
    Unix,
    /// Windows。
    Windows,
}

impl Platform {
    /// 编译目标平台。
    const CURRENT: Self = if cfg!(windows) {
        Self::Windows
    } else {
        Self::Unix
    };
}

/// 按平台规则解析配置目录。
fn resolve_config_dir<F>(platform: Platform, env: F) -> Result<PathBuf, ConfigError>
where
    F: Fn(&str) -> Option<OsString>,
{
    if let Some(dir) = env(TERMX_CONFIG_DIR_ENV).filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(dir));
    }

    match platform {
        Platform::Windows => env(APPDATA_ENV)
            .filter(|value| !value.is_empty())
            .map(|appdata| PathBuf::from(appdata).join(TERMX_DIR_NAME))
            .ok_or(ConfigError::MissingEnv { name: APPDATA_ENV }),
        Platform::Unix => {
            let base = env(XDG_CONFIG_HOME_ENV)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
                .or_else(|| {
                    env(HOME_ENV)
                        .filter(|value| !value.is_empty())
                        .map(|home| PathBuf::from(home).join(UNIX_CONFIG_SUBDIR))
                })
                .ok_or(ConfigError::MissingEnv { name: HOME_ENV })?;
            Ok(base.join(TERMX_DIR_NAME))
        }
    }
}

/// 读取已有 token；不存在或为空时生成新 token 并写盘。
///
/// 发现优先级：`--token-file` 覆盖 > `TERMX_TOKEN` 环境变量 > 默认 token 文件。
/// Unix 上同时把 token 文件权限收紧到 `0600`；Windows 的 ACL 收紧留待后续。
pub(crate) fn load_or_create_token(config: &ServiceConfig) -> std::io::Result<String> {
    load_token_with_env(config, |key| std::env::var_os(key))
}

/// 带可注入环境的 token 解析实现，便于单测优先级。
fn load_token_with_env<F>(config: &ServiceConfig, env: F) -> std::io::Result<String>
where
    F: Fn(&str) -> Option<OsString>,
{
    // 1) --token-file 显式指定文件时，文件内容优先于环境变量。
    if config.token_file.is_some() {
        return load_or_create_token_at(&config.token_path());
    }

    // 2) TERMX_TOKEN 直接提供 token，不落盘。
    if let Some(token) = env(TERMX_TOKEN_ENV)
        .and_then(|value| value.into_string().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        tracing::debug!("使用 TERMX_TOKEN 提供的 token");
        return Ok(token);
    }

    // 3) 默认 token 文件。
    load_or_create_token_at(&config.token_path())
}

/// 从指定路径读取 token；不存在或为空时生成并原子写入。
fn load_or_create_token_at(path: &Path) -> std::io::Result<String> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }

    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let token = contents.trim();
            if !token.is_empty() {
                #[cfg(unix)]
                set_token_permissions(path)?;
                tracing::debug!(path = %path.display(), "复用已有 token");
                return Ok(token.to_string());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let token = generate_token();
    write_token_file(path, &token)?;
    tracing::info!(path = %path.display(), "已生成新的鉴权 token");
    Ok(token)
}

/// 写出实际监听端口。
pub(crate) fn write_port_file(config: &ServiceConfig, port: u16) -> std::io::Result<()> {
    std::fs::write(config.port_path(), port.to_string())
}

/// 生成 32 字节随机 token 的十六进制表示（64 个字符）。
fn generate_token() -> String {
    use rand::Rng as _;
    use std::fmt::Write as _;

    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);

    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut token, "{byte:02x}").expect("写入 String 不会失败");
    }
    token
}

/// 原子化写入 token 文件：同目录临时文件写完 `rename` 覆盖。
fn write_token_file(path: &Path, token: &str) -> std::io::Result<()> {
    let temp_path = temp_token_path(path);
    let write_result = write_temp_token_file(&temp_path, token);
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&temp_path);
        return Err(error);
    }

    if let Err(error) = std::fs::rename(&temp_path, path) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(error);
    }
    Ok(())
}

/// 同目录临时文件名；避免跨设备 rename 失败。
fn temp_token_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(TOKEN_FILE_NAME);
    path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()))
}

/// 写入临时 token 文件。
#[cfg(unix)]
fn write_temp_token_file(path: &Path, token: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(token.as_bytes())?;
    file.sync_all()
}

/// 写入临时 token 文件（Windows 继承目录 ACL，显式 ACL 收紧留待后续）。
#[cfg(not(unix))]
fn write_temp_token_file(path: &Path, token: &str) -> std::io::Result<()> {
    use std::io::Write as _;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(token.as_bytes())?;
    file.sync_all()
}

/// 收紧 token 文件权限到 `0600`。
#[cfg(unix)]
fn set_token_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// 用给定环境变量表调用配置目录解析。
    fn resolve_with(platform: Platform, pairs: &[(&str, &str)]) -> Result<PathBuf, ConfigError> {
        let env: BTreeMap<String, String> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        resolve_config_dir(platform, |key| env.get(key).map(OsString::from))
    }

    #[test]
    fn termx_config_dir_has_highest_priority() {
        let unix = resolve_with(
            Platform::Unix,
            &[
                (TERMX_CONFIG_DIR_ENV, "/tmp/termx"),
                (XDG_CONFIG_HOME_ENV, "/xdg"),
                (HOME_ENV, "/home/user"),
            ],
        )
        .expect("应解析成功");
        assert_eq!(unix, PathBuf::from("/tmp/termx"));

        let windows = resolve_with(
            Platform::Windows,
            &[
                (TERMX_CONFIG_DIR_ENV, "C:\\termx"),
                (APPDATA_ENV, "C:\\Users\\user\\AppData\\Roaming"),
            ],
        )
        .expect("应解析成功");
        assert_eq!(windows, PathBuf::from("C:\\termx"));
    }

    #[test]
    fn empty_termx_config_dir_falls_back_to_platform_default() {
        let unix = resolve_with(
            Platform::Unix,
            &[
                (TERMX_CONFIG_DIR_ENV, ""),
                (XDG_CONFIG_HOME_ENV, "/xdg"),
                (HOME_ENV, "/home/user"),
            ],
        )
        .expect("应解析成功");
        assert_eq!(unix, PathBuf::from("/xdg").join(TERMX_DIR_NAME));
    }

    #[test]
    fn unix_uses_xdg_then_home_config() {
        let xdg = resolve_with(
            Platform::Unix,
            &[(XDG_CONFIG_HOME_ENV, "/xdg"), (HOME_ENV, "/home/user")],
        )
        .expect("应解析成功");
        assert_eq!(xdg, PathBuf::from("/xdg").join(TERMX_DIR_NAME));

        let home = resolve_with(Platform::Unix, &[(HOME_ENV, "/home/user")]).expect("应解析成功");
        assert_eq!(
            home,
            PathBuf::from("/home/user")
                .join(UNIX_CONFIG_SUBDIR)
                .join(TERMX_DIR_NAME)
        );
    }

    #[test]
    fn unix_reports_missing_home() {
        let error = resolve_with(Platform::Unix, &[]).expect_err("应缺少 HOME");
        assert_eq!(error, ConfigError::MissingEnv { name: HOME_ENV });
    }

    #[test]
    fn windows_uses_appdata_and_reports_missing() {
        let appdata = resolve_with(
            Platform::Windows,
            &[(APPDATA_ENV, "C:\\Users\\user\\AppData\\Roaming")],
        )
        .expect("应解析成功");
        assert_eq!(
            appdata,
            PathBuf::from("C:\\Users\\user\\AppData\\Roaming").join(TERMX_DIR_NAME)
        );

        let error = resolve_with(Platform::Windows, &[]).expect_err("应缺少 APPDATA");
        assert_eq!(error, ConfigError::MissingEnv { name: APPDATA_ENV });
    }

    #[test]
    fn default_config_uses_ephemeral_loopback_address() {
        let config = ServiceConfig::new(PathBuf::from("/tmp/termx"));
        assert_eq!(config.bind_addr, DEFAULT_BIND_ADDR);
        assert!(config.bind_addr.ip().is_loopback());
        assert_eq!(config.bind_addr.port(), 0);
        assert_eq!(
            config.token_path(),
            PathBuf::from("/tmp/termx").join(TOKEN_FILE_NAME)
        );
        assert_eq!(
            config.port_path(),
            PathBuf::from("/tmp/termx").join(PORT_FILE_NAME)
        );
        assert_eq!(config.event_buffer_capacity, DEFAULT_EVENT_BUFFER_CAPACITY);
        assert_eq!(
            config
                .clone()
                .with_event_buffer_capacity(8)
                .event_buffer_capacity,
            8
        );
        assert!(config.token_file.is_none());
        assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
        assert_eq!(
            config.clone().with_max_frame_bytes(0).max_frame_bytes,
            1,
            "帧上限至少为 1"
        );

        let custom = PathBuf::from("/tmp/custom-token");
        let with_file = config.with_token_file(custom.clone());
        assert_eq!(with_file.token_path(), custom);
    }

    #[test]
    fn token_resolution_priority_is_file_then_env_then_default() {
        let dir = tempfile::tempdir().expect("创建临时目录失败");

        // TERMX_TOKEN 优先于默认 token 文件，且不落盘。
        let env_config = ServiceConfig::new(dir.path().join("env"));
        let token = load_token_with_env(&env_config, |key| {
            (key == TERMX_TOKEN_ENV).then(|| OsString::from("env-token"))
        })
        .expect("应使用环境变量 token");
        assert_eq!(token, "env-token");
        assert!(!env_config.token_path().exists(), "环境变量 token 不应落盘");

        // --token-file 覆盖优先于 TERMX_TOKEN。
        let file_path = dir.path().join("custom-token");
        std::fs::write(&file_path, "file-token").expect("写入测试 token 失败");
        let file_config = ServiceConfig::new(dir.path().join("file")).with_token_file(file_path);
        let token = load_token_with_env(&file_config, |key| {
            (key == TERMX_TOKEN_ENV).then(|| OsString::from("env-token"))
        })
        .expect("应使用文件 token");
        assert_eq!(token, "file-token");

        // 都没有时生成默认文件并复用，临时文件不残留。
        let default_config = ServiceConfig::new(dir.path().join("default"));
        let first = load_token_with_env(&default_config, |_| None).expect("应生成 token");
        let second = load_token_with_env(&default_config, |_| None).expect("应复用 token");
        assert_eq!(first, second);
        assert!(default_config.token_path().exists());
        let leftovers: Vec<String> = std::fs::read_dir(&default_config.config_dir)
            .expect("读取配置目录失败")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "不应残留临时文件: {leftovers:?}");
    }

    #[test]
    fn generated_token_is_hex_and_long_enough() {
        let token = generate_token();
        assert_eq!(token.len(), 64);
        assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}
