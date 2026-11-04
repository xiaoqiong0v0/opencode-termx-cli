//! 服务配置、环境调参与 token/port 文件读写。
//!
//! 配置目录必须由调用方显式提供（服务入口来自 `--config-dir` 或非空
//! `TERMX_CONFIG_DIR`），本模块**不做任何平台默认目录推导**。
//! token 写到 `config_dir/token`（可由 `--token-file` 覆盖），端口写到
//! `config_dir/port`；文件日志目录同样由调用方显式提供。

use std::{
    ffi::OsString,
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

/// 直接提供 token 的环境变量；优先于默认 token 文件。
const TERMX_TOKEN_ENV: &str = "TERMX_TOKEN";
/// 覆盖空闲检查 ticker 周期的环境变量，单位为毫秒。
const TERMX_IDLE_TICK_MS_ENV: &str = "TERMX_IDLE_TICK_MS";
/// 覆盖交互识别 ticker 周期的环境变量，单位为毫秒。
const TERMX_INTERACTION_TICK_MS_ENV: &str = "TERMX_INTERACTION_TICK_MS";
/// 覆盖交互识别无输出阈值的环境变量，单位为毫秒。
const TERMX_INTERACTION_NO_OUTPUT_MS_ENV: &str = "TERMX_INTERACTION_NO_OUTPUT_MS";
/// 覆盖交互识别同一等待信号重复投递间隔的环境变量，单位为毫秒。
const TERMX_INTERACTION_REPEAT_MS_ENV: &str = "TERMX_INTERACTION_REPEAT_MS";
/// 关闭/开启交互识别的环境变量；`0`/`false`/`off`/`no` 视为关闭。
const TERMX_INTERACTION_ENABLED_ENV: &str = "TERMX_INTERACTION_ENABLED";
/// 覆盖运行中终端数量上限的环境变量；缺失或 0 表示不限制。
const TERMX_MAX_TERMINALS_ENV: &str = "TERMX_MAX_TERMINALS";
/// 覆盖 stdio 写队列容量的环境变量。
const TERMX_STDIO_WRITE_QUEUE_ENV: &str = "TERMX_STDIO_WRITE_QUEUE";
/// 覆盖每终端命令记录容量的环境变量。
const TERMX_TRANSCRIPT_CAPACITY_ENV: &str = "TERMX_TRANSCRIPT_CAPACITY";
/// Web 登录密码；只从环境变量读取，不进入命令行与日志输出。
const TERMX_WEB_PASSWORD_ENV: &str = "TERMX_WEB_PASSWORD";
/// 覆盖「无存活会话」超时的环境变量，单位为毫秒；`0` 表示关闭。
const TERMX_CLIENT_IDLE_TIMEOUT_MS_ENV: &str = "TERMX_CLIENT_IDLE_TIMEOUT_MS";
/// 无存活会话到期后的处理策略：`never` / `no_children` / `any`。
const TERMX_CLIENT_IDLE_POLICY_ENV: &str = "TERMX_CLIENT_IDLE_POLICY";
/// 覆盖会话心跳 TTL 的环境变量，单位为毫秒。
const TERMX_SESSION_TTL_MS_ENV: &str = "TERMX_SESSION_TTL_MS";

/// token 文件名。
pub const TOKEN_FILE_NAME: &str = "token";
/// 监听端口文件名。
pub const PORT_FILE_NAME: &str = "port";
/// 单实例锁文件名；同一 `config_dir` 只允许一个 service 持有该文件的排他锁。
pub const SERVICE_LOCK_FILE_NAME: &str = "service.lock";

/// 默认监听地址：仅环回，端口由 OS 分配。
pub const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

/// 每个终端的事件缓冲默认容量。
pub const DEFAULT_EVENT_BUFFER_CAPACITY: usize = termx_core::DEFAULT_EVENT_BUFFER_CAPACITY;
/// 每个终端的命令记录默认容量（条）。
pub const DEFAULT_TRANSCRIPT_CAPACITY: usize = termx_core::DEFAULT_TRANSCRIPT_CAPACITY;
/// 每个订阅者的有界事件队列默认容量。
pub const DEFAULT_SUBSCRIBER_QUEUE_CAPACITY: usize = termx_core::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY;
/// 入站单帧默认上限（1 MiB）。
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;
/// 空闲检查后台 ticker 的默认周期。
pub const DEFAULT_IDLE_TICK_INTERVAL: Duration = Duration::from_secs(1);
/// 无存活会话的默认超时（5 分钟）；`0` 表示关闭。
pub const DEFAULT_CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// 会话心跳 TTL：超过该时间未收到心跳视为会话已结束。
pub const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(90);
/// 交互识别后台 ticker 的默认周期。
pub const DEFAULT_INTERACTION_TICK_INTERVAL: Duration = Duration::from_millis(250);
/// 交互识别默认无输出阈值（与 core 分析器共用同一来源）。
pub const DEFAULT_INTERACTION_NO_OUTPUT_THRESHOLD: Duration =
    termx_core::DEFAULT_NO_OUTPUT_THRESHOLD;
/// 同一等待信号默认的最小重复投递间隔（与 core 分析器共用同一来源）。
pub const DEFAULT_INTERACTION_REPEAT_INTERVAL: Duration = termx_core::DEFAULT_REPEAT_INTERVAL;
/// stdio stdin 有界写队列默认容量。
pub const DEFAULT_STDIO_WRITE_QUEUE_CAPACITY: usize =
    termx_proc::DEFAULT_STDIO_WRITE_QUEUE_CAPACITY;
/// 文件日志默认保留天数（含当天）。
pub const DEFAULT_LOG_MAX_DAYS: u32 = 7;

/// 无存活会话超过超时后的处理策略。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClientIdlePolicy {
    /// 永不因无会话终止。
    Never,
    /// 到期时若仍有用户子进程则续期，否则终止（默认）。
    #[default]
    NoChildren,
    /// 到期即终止，不检查子进程。
    Any,
}

impl ClientIdlePolicy {
    /// 解析环境变量/配置字符串；无法识别时返回 `None`。
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "never" => Some(Self::Never),
            "no_children" | "no-children" | "nochildren" => Some(Self::NoChildren),
            "any" => Some(Self::Any),
            _ => None,
        }
    }

    /// 配置/日志使用的规范名。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::NoChildren => "no_children",
            Self::Any => "any",
        }
    }
}

/// 服务配置。
///
/// 自定义 `Debug`：`web_password` 只在输出中标记是否已设置，绝不打印明文。
#[derive(Clone, PartialEq, Eq)]
pub struct ServiceConfig {
    /// token/port 文件所在目录。
    pub config_dir: PathBuf,
    /// 监听地址；缺省为 `127.0.0.1:0`。
    pub bind_addr: SocketAddr,
    /// 每个终端的事件缓冲容量；缺省 4096。
    pub event_buffer_capacity: usize,
    /// 每个终端的命令记录容量（条）；缺省 200。
    pub transcript_capacity: usize,
    /// 每个订阅者的有界事件队列容量；缺省 1024。
    pub subscriber_queue_capacity: usize,
    /// 自定义 token 文件路径；`Some` 时优先于 `TERMX_TOKEN` 与默认文件。
    pub token_file: Option<PathBuf>,
    /// 入站单帧字节上限；缺省 [`DEFAULT_MAX_FRAME_BYTES`]。
    pub max_frame_bytes: usize,
    /// 空闲检查 ticker 周期；缺省 [`DEFAULT_IDLE_TICK_INTERVAL`]。
    pub idle_tick_interval: Duration,
    /// 无存活会话的超时；`0` 表示关闭。
    ///
    /// 「存活」由 `root_session_id` 的会话心跳决定（见 `session_ttl`），
    /// Web 附着与终端级 `attach`/`subscribe` 均不参与保活。
    /// 缺省 [`DEFAULT_CLIENT_IDLE_TIMEOUT`]（5 分钟）。
    pub client_idle_timeout: Duration,
    /// 无存活会话到期后的处理策略；缺省 [`ClientIdlePolicy::NoChildren`]。
    pub client_idle_policy: ClientIdlePolicy,
    /// 会话心跳 TTL；超过该时间未收到 `heartbeat` 视为会话结束。
    /// 缺省 [`DEFAULT_SESSION_TTL`]（90 秒）。
    pub session_ttl: Duration,
    /// 交互识别 ticker 周期；缺省 [`DEFAULT_INTERACTION_TICK_INTERVAL`]。
    pub interaction_tick_interval: Duration,
    /// 交互识别无输出阈值；缺省 [`DEFAULT_INTERACTION_NO_OUTPUT_THRESHOLD`]。
    pub interaction_no_output_threshold: Duration,
    /// 同一等待信号的最小重复投递间隔；缺省 [`DEFAULT_INTERACTION_REPEAT_INTERVAL`]。
    pub interaction_repeat_interval: Duration,
    /// 是否启用交互识别。
    pub interaction_enabled: bool,
    /// 运行中终端数量上限；`None` 表示不限制。
    pub max_terminals: Option<usize>,
    /// stdio stdin 有界写队列容量；缺省 [`DEFAULT_STDIO_WRITE_QUEUE_CAPACITY`]。
    pub stdio_write_queue_capacity: usize,
    /// 是否写文件日志；缺省 `false`（仅 stderr）。
    pub log_enabled: bool,
    /// 文件日志目录；`log_enabled` 为 `true` 时必须有值。
    pub log_dir: Option<PathBuf>,
    /// 文件日志保留天数（含当天）；缺省 [`DEFAULT_LOG_MAX_DAYS`]。
    pub log_max_days: u32,
    /// Web 服务端口；`0` 表示不启动 Web 服务。
    pub web_port: u16,
    /// 是否允许远程访问；`false` 时 Web 只绑定 `127.0.0.1`。
    pub web_allow_remote: bool,
    /// Web 登录密码；来自 `TERMX_WEB_PASSWORD`，远程访问时必填。
    pub web_password: Option<String>,
}

impl fmt::Debug for ServiceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceConfig")
            .field("config_dir", &self.config_dir)
            .field("bind_addr", &self.bind_addr)
            .field("event_buffer_capacity", &self.event_buffer_capacity)
            .field("transcript_capacity", &self.transcript_capacity)
            .field("subscriber_queue_capacity", &self.subscriber_queue_capacity)
            .field("token_file", &self.token_file)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("idle_tick_interval", &self.idle_tick_interval)
            .field("client_idle_timeout", &self.client_idle_timeout)
            .field("client_idle_policy", &self.client_idle_policy)
            .field("session_ttl", &self.session_ttl)
            .field("interaction_tick_interval", &self.interaction_tick_interval)
            .field(
                "interaction_no_output_threshold",
                &self.interaction_no_output_threshold,
            )
            .field(
                "interaction_repeat_interval",
                &self.interaction_repeat_interval,
            )
            .field("interaction_enabled", &self.interaction_enabled)
            .field("max_terminals", &self.max_terminals)
            .field(
                "stdio_write_queue_capacity",
                &self.stdio_write_queue_capacity,
            )
            .field("log_enabled", &self.log_enabled)
            .field("log_dir", &self.log_dir)
            .field("log_max_days", &self.log_max_days)
            .field("web_port", &self.web_port)
            .field("web_allow_remote", &self.web_allow_remote)
            .field(
                "web_password",
                &self.web_password.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl ServiceConfig {
    /// 用给定配置目录构造配置，监听地址取 [`DEFAULT_BIND_ADDR`]。
    pub fn new(config_dir: PathBuf) -> Self {
        Self {
            config_dir,
            bind_addr: DEFAULT_BIND_ADDR,
            event_buffer_capacity: DEFAULT_EVENT_BUFFER_CAPACITY,
            transcript_capacity: DEFAULT_TRANSCRIPT_CAPACITY,
            subscriber_queue_capacity: DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
            token_file: None,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            idle_tick_interval: DEFAULT_IDLE_TICK_INTERVAL,
            client_idle_timeout: DEFAULT_CLIENT_IDLE_TIMEOUT,
            client_idle_policy: ClientIdlePolicy::NoChildren,
            session_ttl: DEFAULT_SESSION_TTL,
            interaction_tick_interval: DEFAULT_INTERACTION_TICK_INTERVAL,
            interaction_no_output_threshold: DEFAULT_INTERACTION_NO_OUTPUT_THRESHOLD,
            interaction_repeat_interval: DEFAULT_INTERACTION_REPEAT_INTERVAL,
            interaction_enabled: true,
            max_terminals: None,
            stdio_write_queue_capacity: DEFAULT_STDIO_WRITE_QUEUE_CAPACITY,
            log_enabled: false,
            log_dir: None,
            log_max_days: DEFAULT_LOG_MAX_DAYS,
            web_port: 0,
            web_allow_remote: false,
            web_password: None,
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

    /// 覆盖每个终端的命令记录容量；0 会抬到 1。
    #[must_use]
    pub fn with_transcript_capacity(mut self, transcript_capacity: usize) -> Self {
        self.transcript_capacity = transcript_capacity.max(1);
        self
    }

    /// 覆盖每个订阅者的有界事件队列容量；0 会抬到 1。
    #[must_use]
    pub fn with_subscriber_queue_capacity(mut self, subscriber_queue_capacity: usize) -> Self {
        self.subscriber_queue_capacity = subscriber_queue_capacity.max(1);
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

    /// 覆盖空闲检查 ticker 周期；0 会抬到 1ms，避免 `tokio::time::interval` panic。
    #[must_use]
    pub fn with_idle_tick_interval(mut self, idle_tick_interval: Duration) -> Self {
        self.idle_tick_interval = clamp_non_zero(idle_tick_interval);
        self
    }

    /// 覆盖无存活会话超时；`0` 表示关闭（不会抬升为非零）。
    #[must_use]
    pub fn with_client_idle_timeout(mut self, timeout: Duration) -> Self {
        self.client_idle_timeout = timeout;
        self
    }

    /// 覆盖无存活会话到期策略。
    #[must_use]
    pub fn with_client_idle_policy(mut self, policy: ClientIdlePolicy) -> Self {
        self.client_idle_policy = policy;
        self
    }

    /// 覆盖会话心跳 TTL；0 会抬到 1ms，避免「立即过期」意外终止终端。
    #[must_use]
    pub fn with_session_ttl(mut self, ttl: Duration) -> Self {
        self.session_ttl = clamp_non_zero(ttl);
        self
    }

    /// 覆盖交互识别 ticker 周期；0 会抬到 1ms。
    #[must_use]
    pub fn with_interaction_tick_interval(mut self, interval: Duration) -> Self {
        self.interaction_tick_interval = clamp_non_zero(interval);
        self
    }

    /// 覆盖交互识别无输出阈值；0 会抬到 1ms。
    #[must_use]
    pub fn with_interaction_no_output_threshold(mut self, threshold: Duration) -> Self {
        self.interaction_no_output_threshold = clamp_non_zero(threshold);
        self
    }

    /// 覆盖同一等待信号的重复投递间隔；0 会抬到 1ms。
    #[must_use]
    pub fn with_interaction_repeat_interval(mut self, interval: Duration) -> Self {
        self.interaction_repeat_interval = clamp_non_zero(interval);
        self
    }

    /// 开启/关闭交互识别。
    #[must_use]
    pub fn with_interaction_enabled(mut self, enabled: bool) -> Self {
        self.interaction_enabled = enabled;
        self
    }

    /// 覆盖运行中终端数量上限；`None` 表示不限制。
    #[must_use]
    pub fn with_max_terminals(mut self, max_terminals: Option<usize>) -> Self {
        self.max_terminals = max_terminals;
        self
    }

    /// 覆盖 stdio stdin 写队列容量；0 会抬到 1。
    #[must_use]
    pub fn with_stdio_write_queue_capacity(mut self, capacity: usize) -> Self {
        self.stdio_write_queue_capacity = capacity.max(1);
        self
    }

    /// 覆盖 Web 服务端口；`0` 表示不启动 Web 服务。
    #[must_use]
    pub fn with_web_port(mut self, web_port: u16) -> Self {
        self.web_port = web_port;
        self
    }

    /// 是否允许 Web 远程访问；默认 `false`（只绑定回环）。
    #[must_use]
    pub fn with_web_allow_remote(mut self, allow_remote: bool) -> Self {
        self.web_allow_remote = allow_remote;
        self
    }

    /// 覆盖 Web 登录密码；传 `None` 表示未配置。
    #[must_use]
    pub fn with_web_password(mut self, password: Option<String>) -> Self {
        self.web_password = password;
        self
    }

    /// 用显式配置目录构造配置，并读取环境调参。
    /// 配置目录**只**来自参数，不会从 `TERMX_CONFIG_DIR`、`XDG_CONFIG_HOME`、
    /// `APPDATA` 或 `HOME` 推导；调用方（命令行入口）负责解析与校验。
    ///
    /// 空闲检查周期可由 `TERMX_IDLE_TICK_MS`（毫秒）覆盖；缺省为
    /// [`DEFAULT_IDLE_TICK_INTERVAL`]，非法或 0 值回退默认。
    /// 无存活会话超时可由 `TERMX_CLIENT_IDLE_TIMEOUT_MS`（毫秒）覆盖；
    /// 缺省 [`DEFAULT_CLIENT_IDLE_TIMEOUT`]，`0` 表示关闭，非法值回退默认。
    /// 策略由 `TERMX_CLIENT_IDLE_POLICY`（`never`/`no_children`/`any`）覆盖；
    /// 会话 TTL 由 `TERMX_SESSION_TTL_MS`（毫秒）覆盖。
    /// `TERMX_MAX_TERMINALS`（0 或缺失 = 不限制）与
    /// `TERMX_STDIO_WRITE_QUEUE`（0 或缺失 = 默认容量）同理。
    #[must_use]
    pub fn from_env(config_dir: PathBuf) -> Self {
        Self::from_env_with(config_dir, |key: &str| std::env::var_os(key))
    }

    /// 带可注入环境的 [`ServiceConfig::from_env`]，便于单测。
    fn from_env_with<F>(config_dir: PathBuf, env: F) -> Self
    where
        F: Fn(&str) -> Option<OsString>,
    {
        let mut config = Self::new(config_dir);
        config.idle_tick_interval = idle_tick_interval_from_env(&env);
        config.client_idle_timeout = duration_allow_zero_from_ms_env(
            TERMX_CLIENT_IDLE_TIMEOUT_MS_ENV,
            DEFAULT_CLIENT_IDLE_TIMEOUT,
            &env,
        );
        config.client_idle_policy = env(TERMX_CLIENT_IDLE_POLICY_ENV)
            .and_then(|value| value.into_string().ok())
            .and_then(|value| ClientIdlePolicy::parse(&value))
            .unwrap_or_default();
        config.session_ttl =
            duration_from_ms_env(TERMX_SESSION_TTL_MS_ENV, DEFAULT_SESSION_TTL, &env);
        config.interaction_tick_interval = duration_from_ms_env(
            TERMX_INTERACTION_TICK_MS_ENV,
            DEFAULT_INTERACTION_TICK_INTERVAL,
            &env,
        );
        config.interaction_no_output_threshold = duration_from_ms_env(
            TERMX_INTERACTION_NO_OUTPUT_MS_ENV,
            DEFAULT_INTERACTION_NO_OUTPUT_THRESHOLD,
            &env,
        );
        config.interaction_repeat_interval = duration_from_ms_env(
            TERMX_INTERACTION_REPEAT_MS_ENV,
            DEFAULT_INTERACTION_REPEAT_INTERVAL,
            &env,
        );
        config.interaction_enabled = bool_from_env(TERMX_INTERACTION_ENABLED_ENV, true, &env);
        config.max_terminals = positive_usize_from_env(TERMX_MAX_TERMINALS_ENV, &env);
        config.transcript_capacity = positive_usize_from_env(TERMX_TRANSCRIPT_CAPACITY_ENV, &env)
            .unwrap_or(DEFAULT_TRANSCRIPT_CAPACITY);
        config.stdio_write_queue_capacity =
            positive_usize_from_env(TERMX_STDIO_WRITE_QUEUE_ENV, &env)
                .unwrap_or(DEFAULT_STDIO_WRITE_QUEUE_CAPACITY);
        config.web_password = web_password_from_env(&env);
        config
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

    /// 单实例锁文件路径。
    pub fn service_lock_path(&self) -> PathBuf {
        self.config_dir.join(SERVICE_LOCK_FILE_NAME)
    }
}

/// 从环境读取空闲检查 ticker 周期。
///
/// `TERMX_IDLE_TICK_MS` 为毫秒整数；缺失、非法或 0 时回退
/// [`DEFAULT_IDLE_TICK_INTERVAL`]，避免 `tokio::time::interval` 收到 0 周期。
fn idle_tick_interval_from_env<F>(env: F) -> Duration
where
    F: Fn(&str) -> Option<OsString>,
{
    duration_from_ms_env(TERMX_IDLE_TICK_MS_ENV, DEFAULT_IDLE_TICK_INTERVAL, env)
}

/// 从环境读取毫秒周期；缺失、非法或 0 时回退默认值。
fn duration_from_ms_env<F>(name: &str, default: Duration, env: F) -> Duration
where
    F: Fn(&str) -> Option<OsString>,
{
    let parsed = env(name)
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|millis| *millis > 0);
    parsed.map_or(default, Duration::from_millis)
}

/// 从环境读取毫秒时长；允许 `0`（表示关闭），缺失或非法时回退默认值。
fn duration_allow_zero_from_ms_env<F>(name: &str, default: Duration, env: F) -> Duration
where
    F: Fn(&str) -> Option<OsString>,
{
    env(name)
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map_or(default, Duration::from_millis)
}

/// 从环境读取布尔开关；无法识别时回退默认值。
fn bool_from_env<F>(name: &str, default: bool, env: F) -> bool
where
    F: Fn(&str) -> Option<OsString>,
{
    let Some(value) = env(name).and_then(|value| value.into_string().ok()) else {
        return default;
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "0" | "false" | "off" | "no" => false,
        "1" | "true" | "on" | "yes" => true,
        _ => default,
    }
}

/// 把 0 周期抬到 1ms，避免 `tokio::time::interval` panic。
fn clamp_non_zero(interval: Duration) -> Duration {
    if interval.is_zero() {
        Duration::from_millis(1)
    } else {
        interval
    }
}

/// 读取正整数值的环境变量；缺失、非法或 0 返回 `None`。
fn positive_usize_from_env<F>(name: &str, env: F) -> Option<usize>
where
    F: Fn(&str) -> Option<OsString>,
{
    env(name)
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

/// 从环境读取 Web 登录密码；缺失、空串或无法解码时返回 `None`。
fn web_password_from_env<F>(env: F) -> Option<String>
where
    F: Fn(&str) -> Option<OsString>,
{
    env(TERMX_WEB_PASSWORD_ENV)
        .and_then(|value| value.into_string().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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

/// 原子化写出实际监听端口。
///
/// 先写同目录临时文件再 `rename` 覆盖：任一步失败只清理临时文件，不会留下
/// 半写的 port 文件，也不会破坏上一次成功写入的有效内容。
pub(crate) fn write_port_file(config: &ServiceConfig, port: u16) -> std::io::Result<()> {
    let path = config.port_path();
    let temp_path = temp_file_path(&path);
    let result = std::fs::write(&temp_path, port.to_string())
        .and_then(|()| std::fs::rename(&temp_path, &path));
    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

/// 生成 32 字节随机 token 的十六进制表示（64 个字符）。
pub(crate) fn generate_token() -> String {
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
    let temp_path = temp_file_path(path);
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
fn temp_file_path(path: &Path) -> PathBuf {
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

    #[test]
    fn from_env_keeps_explicit_config_dir_and_never_derives_one() {
        // 即使环境里存在 TERMX_CONFIG_DIR / HOME / APPDATA 等变量，
        // 显式传入的目录也不会被覆盖，更不会回退到平台默认目录。
        let config = ServiceConfig::from_env_with(PathBuf::from("/explicit/termx"), |key| {
            matches!(
                key,
                "TERMX_CONFIG_DIR" | "HOME" | "APPDATA" | "XDG_CONFIG_HOME"
            )
            .then(|| OsString::from("/should/not/be/used"))
        });
        assert_eq!(config.config_dir, PathBuf::from("/explicit/termx"));

        // 空环境同样不会改变显式目录。
        let empty_env = ServiceConfig::from_env_with(PathBuf::from("/only/source"), |_| None);
        assert_eq!(empty_env.config_dir, PathBuf::from("/only/source"));
    }

    #[test]
    fn idle_tick_interval_reads_env_with_default_fallback() {
        let default = idle_tick_interval_from_env(|_| None);
        assert_eq!(default, DEFAULT_IDLE_TICK_INTERVAL);

        let custom = idle_tick_interval_from_env(|key| {
            (key == TERMX_IDLE_TICK_MS_ENV).then(|| OsString::from("25"))
        });
        assert_eq!(custom, Duration::from_millis(25));

        // 0 / 非法值 / 空白一律回退默认，避免 0 周期让 interval panic。
        for invalid in ["0", "abc", "  "] {
            let fallback = idle_tick_interval_from_env(|key| {
                (key == TERMX_IDLE_TICK_MS_ENV).then(|| OsString::from(invalid))
            });
            assert_eq!(fallback, DEFAULT_IDLE_TICK_INTERVAL, "非法值 {invalid:?}");
        }
    }

    #[test]
    fn client_idle_timeout_reads_env_and_allows_zero() {
        let default = ServiceConfig::new(PathBuf::from("/tmp/termx"));
        assert_eq!(default.client_idle_timeout, DEFAULT_CLIENT_IDLE_TIMEOUT);
        assert_eq!(default.client_idle_timeout, Duration::from_secs(5 * 60));
        assert_eq!(default.client_idle_policy, ClientIdlePolicy::NoChildren);
        assert_eq!(default.session_ttl, DEFAULT_SESSION_TTL);

        let custom = ServiceConfig::from_env_with(PathBuf::from("/tmp/termx"), |key| {
            (key == TERMX_CLIENT_IDLE_TIMEOUT_MS_ENV).then(|| OsString::from("1200"))
        });
        assert_eq!(custom.client_idle_timeout, Duration::from_millis(1200));

        // 0 表示关闭，必须保留为 0 而不是回退默认。
        let disabled = ServiceConfig::from_env_with(PathBuf::from("/tmp/termx"), |key| {
            (key == TERMX_CLIENT_IDLE_TIMEOUT_MS_ENV).then(|| OsString::from("0"))
        });
        assert!(disabled.client_idle_timeout.is_zero());

        // 非法值回退默认。
        let invalid = ServiceConfig::from_env_with(PathBuf::from("/tmp/termx"), |key| {
            (key == TERMX_CLIENT_IDLE_TIMEOUT_MS_ENV).then(|| OsString::from("abc"))
        });
        assert_eq!(invalid.client_idle_timeout, DEFAULT_CLIENT_IDLE_TIMEOUT);

        // builder 对 0 不做非零抬升。
        assert!(
            ServiceConfig::new(PathBuf::from("/tmp/termx"))
                .with_client_idle_timeout(Duration::ZERO)
                .client_idle_timeout
                .is_zero()
        );

        // 策略与 TTL 可分别覆盖；非法策略回退默认。
        let tuned = ServiceConfig::from_env_with(PathBuf::from("/tmp/termx"), |key| match key {
            TERMX_CLIENT_IDLE_POLICY_ENV => Some(OsString::from("any")),
            TERMX_SESSION_TTL_MS_ENV => Some(OsString::from("250")),
            _ => None,
        });
        assert_eq!(tuned.client_idle_policy, ClientIdlePolicy::Any);
        assert_eq!(tuned.session_ttl, Duration::from_millis(250));

        let never = ServiceConfig::from_env_with(PathBuf::from("/tmp/termx"), |key| {
            (key == TERMX_CLIENT_IDLE_POLICY_ENV).then(|| OsString::from("never"))
        });
        assert_eq!(never.client_idle_policy, ClientIdlePolicy::Never);

        let invalid_policy = ServiceConfig::from_env_with(PathBuf::from("/tmp/termx"), |key| {
            (key == TERMX_CLIENT_IDLE_POLICY_ENV).then(|| OsString::from("bogus"))
        });
        assert_eq!(
            invalid_policy.client_idle_policy,
            ClientIdlePolicy::NoChildren
        );
    }

    #[test]
    fn interaction_settings_read_env_with_defaults() {
        let defaults = ServiceConfig::new(PathBuf::from("/tmp/termx"));
        assert_eq!(
            defaults.interaction_tick_interval,
            DEFAULT_INTERACTION_TICK_INTERVAL
        );
        assert_eq!(
            defaults.interaction_no_output_threshold,
            DEFAULT_INTERACTION_NO_OUTPUT_THRESHOLD
        );
        assert_eq!(
            defaults.interaction_repeat_interval,
            DEFAULT_INTERACTION_REPEAT_INTERVAL
        );
        assert!(defaults.interaction_enabled);

        let table = [
            (TERMX_INTERACTION_TICK_MS_ENV, "40"),
            (TERMX_INTERACTION_NO_OUTPUT_MS_ENV, "1500"),
            (TERMX_INTERACTION_REPEAT_MS_ENV, "9000"),
            (TERMX_INTERACTION_ENABLED_ENV, "off"),
        ];
        let env = |key: &str| {
            table
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| OsString::from(*value))
        };
        assert_eq!(
            duration_from_ms_env(
                TERMX_INTERACTION_TICK_MS_ENV,
                DEFAULT_INTERACTION_TICK_INTERVAL,
                env,
            ),
            Duration::from_millis(40)
        );
        assert_eq!(
            duration_from_ms_env(
                TERMX_INTERACTION_NO_OUTPUT_MS_ENV,
                DEFAULT_INTERACTION_NO_OUTPUT_THRESHOLD,
                env,
            ),
            Duration::from_millis(1500)
        );
        assert_eq!(
            duration_from_ms_env(
                TERMX_INTERACTION_REPEAT_MS_ENV,
                DEFAULT_INTERACTION_REPEAT_INTERVAL,
                env,
            ),
            Duration::from_millis(9000)
        );
        assert!(!bool_from_env(TERMX_INTERACTION_ENABLED_ENV, true, env));
        assert!(bool_from_env("TERMX_UNKNOWN", true, env));
        for invalid in ["0", "abc", "  "] {
            assert_eq!(
                duration_from_ms_env(
                    TERMX_INTERACTION_TICK_MS_ENV,
                    DEFAULT_INTERACTION_TICK_INTERVAL,
                    |key| (key == TERMX_INTERACTION_TICK_MS_ENV).then(|| OsString::from(invalid)),
                ),
                DEFAULT_INTERACTION_TICK_INTERVAL,
                "非法值 {invalid:?}"
            );
        }

        let configured = defaults
            .clone()
            .with_interaction_tick_interval(Duration::from_millis(10))
            .with_interaction_no_output_threshold(Duration::from_millis(20))
            .with_interaction_repeat_interval(Duration::from_millis(30))
            .with_interaction_enabled(false);
        assert_eq!(
            configured.interaction_tick_interval,
            Duration::from_millis(10)
        );
        assert_eq!(
            configured.interaction_no_output_threshold,
            Duration::from_millis(20)
        );
        assert_eq!(
            configured.interaction_repeat_interval,
            Duration::from_millis(30)
        );
        assert!(!configured.interaction_enabled);
        assert_eq!(
            defaults
                .clone()
                .with_interaction_tick_interval(Duration::ZERO)
                .interaction_tick_interval,
            Duration::from_millis(1)
        );
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
        assert_eq!(
            config.subscriber_queue_capacity,
            DEFAULT_SUBSCRIBER_QUEUE_CAPACITY
        );
        assert_eq!(
            config
                .clone()
                .with_subscriber_queue_capacity(2)
                .subscriber_queue_capacity,
            2
        );
        assert_eq!(
            config
                .clone()
                .with_subscriber_queue_capacity(0)
                .subscriber_queue_capacity,
            1,
            "订阅队列容量至少为 1"
        );
        assert!(config.token_file.is_none());
        assert!(!config.log_enabled);
        assert!(config.log_dir.is_none());
        assert_eq!(config.log_max_days, DEFAULT_LOG_MAX_DAYS);
        assert_eq!(config.web_port, 0, "默认不启动 Web 服务");
        assert!(!config.web_allow_remote, "默认仅回环");
        assert!(config.web_password.is_none());
        assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
        assert_eq!(
            config.clone().with_max_frame_bytes(0).max_frame_bytes,
            1,
            "帧上限至少为 1"
        );
        assert_eq!(config.idle_tick_interval, DEFAULT_IDLE_TICK_INTERVAL);
        assert_eq!(
            config
                .clone()
                .with_idle_tick_interval(Duration::from_millis(50))
                .idle_tick_interval,
            Duration::from_millis(50)
        );
        assert_eq!(
            config
                .clone()
                .with_idle_tick_interval(Duration::ZERO)
                .idle_tick_interval,
            Duration::from_millis(1),
            "0 周期应抬到 1ms"
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

    #[test]
    fn web_password_comes_from_env_and_is_masked_in_debug() {
        let secret = "s3cret-value";
        let config = ServiceConfig::from_env_with(PathBuf::from("/tmp/termx"), |key| {
            (key == TERMX_WEB_PASSWORD_ENV).then(|| OsString::from(secret))
        });
        assert_eq!(config.web_password.as_deref(), Some(secret));

        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"), "Debug 应标记已遮蔽");
        assert!(!debug.contains(secret), "Debug 不得输出密码明文");

        // 空串与空白视为未配置。
        for empty in ["", "   "] {
            let config = ServiceConfig::from_env_with(PathBuf::from("/tmp/termx"), |key| {
                (key == TERMX_WEB_PASSWORD_ENV).then(|| OsString::from(empty))
            });
            assert!(config.web_password.is_none(), "空值 {empty:?} 不应视为密码");
        }

        let configured = ServiceConfig::new(PathBuf::from("/tmp/termx"))
            .with_web_port(8138)
            .with_web_allow_remote(true)
            .with_web_password(Some("pw".to_string()));
        assert_eq!(configured.web_port, 8138);
        assert!(configured.web_allow_remote);
        assert_eq!(configured.web_password.as_deref(), Some("pw"));
    }
}
