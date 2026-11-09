//! SSH 会话（M6b-2）：russh 连接、helper 分发/握手与 fallback。
//!
//! - 连接/认证（密码；私钥已接线）；
//! - 配置了 helper 时：上传/校验远端 sidecar → `exec <helper> serve` → `hello`
//!   握手 → `spawn`，随后用 helper 协议做输入/缩放/信号/终止/进程树；
//! - helper 不可用/版本不符/摘要不符：关闭 helper 通道并回退无 helper 的
//!   fallback（`exec` 或 `request_pty + request_shell`），能力标记 best-effort；
//! - 读输出、`window_change`、信号、`eof + close` 终止；channel EOF/Close 或
//!   连接断开即结束，并在 `exit-status` 可得时给出真实退出码。
//!
//! 刻意不复用系统 `ssh`/`scp`：凭据只在本进程内使用，不经命令行或日志。
//! 事件统一使用 [`termx_proc::ProcEvent`]，与本地 PTY/stdio 后端共用 service
//! 的摄取路径。

mod helper;
mod known_hosts;

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, OnceLock, mpsc::Sender},
    time::Duration,
};

use known_hosts::{HostKeyInfo, HostKeyTarget, KnownHostVerdict, KnownHosts, KnownHostsError};
use russh::{
    Channel, ChannelMsg, Disconnect, Sig,
    client::{self, Msg},
    keys::{HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate, load_secret_key},
};
use termx_proc::{OutputStream, ProcEvent};
use termx_protocol::{
    Capabilities, InjectionConsent, ProcNode, SshAuth, SshConfig, SshHostKeyPolicy,
    validate_path_spec,
};
use thiserror::Error;
use tokio::{
    runtime::Runtime,
    sync::mpsc::{self, Receiver, UnboundedSender, channel, unbounded_channel},
};

/// fallback 会话默认终端类型。
pub const DEFAULT_TERM: &str = "xterm-256color";

/// 需要带外 shell 事件通道的远端程序名（按 basename 匹配）。
///
/// `termx-pwsh` 为首个受管 shell；`termx-bash` 使用同一协议，因此远端 helper
/// 只需按程序名下发 `events=true` 并注入 `TERMX_EVENT_ENDPOINT/TOKEN`。
pub const SHELL_EVENT_PROGRAMS: &[&str] = &["termx-pwsh", "termx-bash"];
/// 兼容旧调用方：首个结构化程序名。
pub const SHELL_EVENT_PROGRAM: &str = SHELL_EVENT_PROGRAMS[0];

/// TCP 建连 + 认证的总超时。
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// `SshSession::connect` 等待初始化结果的上限。
///
/// 必须大于建连超时与 helper 分发/握手超时之和，让内层先超时并完成远端清理，
/// 避免两层超时同时到期导致清理任务被一并取消。
const INIT_TIMEOUT: Duration =
    Duration::from_secs(CONNECT_TIMEOUT.as_secs() + helper::HELPER_BOOTSTRAP_TIMEOUT.as_secs() + 5);

/// helper 是否被配置为启用（不代表远端实际可用）。
///
/// `upload_helper=true` 或显式 `helper_path` 都视为启用；该判定同时供 core
/// 工厂在建立连接前拒绝 fallback 无法支持的 `cwd`/`env`。
pub fn helper_enabled(config: &SshConfig) -> bool {
    config.upload_helper
        || config
            .helper_path
            .as_deref()
            .is_some_and(|path| !path.trim().is_empty())
}

/// 无 `exit-status` 可用的 best-effort 退出码。
///
/// 对齐 `docs/reference/REFERENCE_AND_PITFALLS.md` §5：无 helper 时只能得到 channel 级结果，
/// 不得伪造真实退出码。
pub const BEST_EFFORT_EXIT_CODE: i32 = -1;

/// 事件队列容量；service 摄取任务通常立即消费，容量只用于吸收建连后的突发输出。
const EVENT_QUEUE_CAPACITY: usize = 1024;

/// SSH 专用 tokio 运行时工作线程数。
const RUNTIME_WORKERS: usize = 2;

/// SSH 会话错误。
#[derive(Debug, Error)]
pub enum SshError {
    /// 专用 tokio 运行时初始化失败。
    #[error("SSH 运行时初始化失败: {0}")]
    Runtime(String),
    /// 参数缺失或非法。
    #[error("SSH 参数非法: {0}")]
    BadRequest(String),
    /// TCP 连接、密钥交换、打开 channel 等失败。
    #[error("SSH 连接失败: {message}")]
    Connect {
        /// 可读原因（不含凭据）。
        message: String,
    },
    /// 用户名/密码（或私钥）未被服务器接受。
    #[error("SSH 认证失败")]
    AuthFailed,
    /// 主机密钥未被信任；需 known_hosts 记录或显式确认。
    #[error("SSH 主机密钥未被信任: {message}")]
    HostKeyRejected {
        /// 可读原因（不含凭据）。
        message: String,
    },
    /// 未知主机密钥；`ask` 策略下客户端确认指纹后可重试。
    #[error("SSH 主机密钥未知（{host}:{port} {algorithm} {fingerprint}）")]
    HostKeyUnknown {
        /// 目标主机。
        host: String,
        /// 目标端口。
        port: u16,
        /// 密钥算法。
        algorithm: String,
        /// 当前密钥 SHA256 指纹。
        fingerprint: String,
    },
    /// 主机密钥与 known_hosts 不一致；可能被中间人替换。
    #[error(
        "SSH 主机密钥已变化（{host}:{port} {algorithm} {fingerprint}，原记录 {recorded_fingerprint}），可能遭遇中间人攻击"
    )]
    HostKeyChanged {
        /// 目标主机。
        host: String,
        /// 目标端口。
        port: u16,
        /// 当前密钥算法。
        algorithm: String,
        /// 当前密钥 SHA256 指纹。
        fingerprint: String,
        /// known_hosts 中记录的旧指纹。
        recorded_fingerprint: String,
    },
    /// 会话已结束，无法再写/缩放/发信号。
    #[error("SSH 会话已关闭")]
    Closed,
    /// 不支持的信号名。
    #[error("不支持的信号: {0}")]
    UnsupportedSignal(String),
    /// 远端注入需要用户授权；调用方应把载荷上报，不得 fallback。
    #[error("远端注入需要授权（host={host}, user={user}）", host = .0.host, user = .0.user)]
    ConsentRequired(InjectionConsent),
    /// 远端 helper 注入失败（预检/路径/权限/安装/写入等）；必须直接失败，
    /// 不得 fallback。消息只包含目标路径与原因，不含任何凭据。
    #[error("远端 helper 注入失败（{path}）: {message}")]
    InjectionFailed {
        /// 注入目标路径（`~/...` 未展开时保留原始 spec）。
        path: String,
        /// 失败原因（不含 token/密码）。
        message: String,
    },
}

/// 该错误是否必须直接上报、不能回退 fallback。
///
/// 仅 helper 协议/版本/握手等普通错误保留 fallback；授权与注入失败直接失败。
fn must_not_fallback(error: &SshError) -> bool {
    matches!(
        error,
        SshError::ConsentRequired(_) | SshError::InjectionFailed { .. }
    )
}

/// 远端 channel 的请求类型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SshChannelMode {
    /// 申请交互式 shell。
    Shell,
    /// 执行单条远端命令（已按 POSIX 规则拼接）。
    Exec(String),
}

/// helper 模式下直接启动的程序与参数（不经 shell 二次分词）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelperProgram {
    /// 可执行程序。
    pub cmd: String,
    /// 程序参数。
    pub args: Vec<String>,
}

/// 建立 SSH channel 的请求参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshConnectRequest {
    /// shell 或 exec。
    pub mode: SshChannelMode,
    /// 是否申请伪终端；shell 固定为 `true`。
    pub pty: bool,
    /// 初始终端列数。
    pub cols: u16,
    /// 初始终端行数。
    pub rows: u16,
    /// `TERM` 值。
    pub term: String,
    /// helper 模式的工作目录；fallback 若有值则视为不支持。
    pub cwd: Option<String>,
    /// helper 模式的环境变量覆盖；fallback 若有值则视为不支持。
    pub env: Option<BTreeMap<String, String>>,
    /// helper 模式直接启动的程序；`None` 时按 `mode` 推导。
    pub program: Option<HelperProgram>,
    /// 是否请求 helper 带外 shell 事件通道；仅 `from_create` 在 pty + 远端
    /// 结构化程序（`termx-pwsh`/`termx-bash`）时置 `true`。
    pub events: bool,
}

impl SshConnectRequest {
    /// 申请交互式 shell（带 PTY）。
    pub fn shell(cols: u16, rows: u16) -> Self {
        Self {
            mode: SshChannelMode::Shell,
            pty: true,
            cols,
            rows,
            term: DEFAULT_TERM.to_string(),
            cwd: None,
            env: None,
            program: None,
            events: false,
        }
    }

    /// 执行远端命令并申请 PTY（输出为合并流）。
    pub fn exec(command: impl Into<String>, cols: u16, rows: u16) -> Self {
        Self {
            mode: SshChannelMode::Exec(command.into()),
            pty: true,
            cols,
            rows,
            term: DEFAULT_TERM.to_string(),
            cwd: None,
            env: None,
            program: None,
            events: false,
        }
    }

    /// 执行远端命令且不申请 PTY（stdout/stderr 分流）。
    pub fn exec_no_pty(command: impl Into<String>) -> Self {
        Self {
            mode: SshChannelMode::Exec(command.into()),
            pty: false,
            cols: 0,
            rows: 0,
            term: DEFAULT_TERM.to_string(),
            cwd: None,
            env: None,
            program: None,
            events: false,
        }
    }

    /// 按 `terminal.create` 的 `cmd`/`args` 组装请求。
    ///
    /// `cmd` 为空或等于 `shell` 时申请交互式 shell；否则把 `cmd + args`
    /// 按 POSIX 单引号规则拼成远端命令，并保留原始程序/参数供 helper 使用。
    ///
    /// 只有 pty + basename 属于结构化程序（`termx-pwsh`/`termx-bash`）才请求
    /// 带外 shell 事件通道；其他 SSH 命令保持原有 in-band 行为，避免误关闭
    /// 普通远端会话的 OSC 判定。
    pub fn from_create(cmd: &str, args: &[String], cols: u16, rows: u16) -> Self {
        let trimmed = cmd.trim();
        if trimmed.is_empty() || trimmed == "shell" {
            return Self::shell(cols, rows);
        }
        let line = build_command_line(cmd, args);
        let mut request = Self::exec(line, cols, rows);
        request.events = request.pty && is_shell_event_program(cmd);
        request.program = Some(HelperProgram {
            cmd: cmd.to_string(),
            args: args.to_vec(),
        });
        request
    }

    /// 设置 helper 模式的工作目录与环境变量覆盖。
    pub fn with_working_dir(
        mut self,
        cwd: Option<String>,
        env: Option<BTreeMap<String, String>>,
    ) -> Self {
        self.cwd = cwd;
        self.env = env;
        self
    }
}

/// 判断 `cmd` 的 basename 是否需要带外 shell 事件通道。
///
/// 同时接受 `/usr/local/bin/termx-pwsh`、`/usr/local/bin/termx-bash` 与
/// Windows 风格分隔符；只比较文件名，不依赖远端路径存在。命令来自协议字符串，
/// 不按本机路径语义解析。
fn is_shell_event_program(cmd: &str) -> bool {
    cmd.trim()
        .rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| SHELL_EVENT_PROGRAMS.contains(&name))
}

/// 把程序与参数拼成 POSIX shell 命令行。
///
/// 每个片段用单引号包裹，并把内部 `'` 转义为 `'\''`；这样远端登录 shell
/// 不会对参数做二次分词。
pub fn build_command_line(program: &str, args: &[String]) -> String {
    let mut parts = Vec::with_capacity(args.len() + 1);
    parts.push(shell_quote(program));
    parts.extend(args.iter().map(|arg| shell_quote(arg)));
    parts.join(" ")
}

/// POSIX 单引号转义。
pub(crate) fn shell_quote(raw: &str) -> String {
    let escaped = raw.replace('\'', "'\\''");
    format!("'{escaped}'")
}

/// 连接建立后、驱动启动前的会话形态。
enum Prepared {
    /// helper 已握手并 `spawn`。
    Helper {
        /// 已认证连接。
        handle: client::Handle<HostKeyPolicy>,
        /// helper 会话。
        session: helper::HelperSession,
    },
    /// 无 helper 的 fallback。
    Fallback {
        /// 已认证连接。
        handle: client::Handle<HostKeyPolicy>,
        /// 已打开并申请 shell/exec 的 channel。
        channel: Channel<Msg>,
    },
}

impl Prepared {
    /// 供 `SshSession` 保存的只读信息。
    fn info(&self) -> SessionInfo {
        match self {
            Self::Helper { session, .. } => SessionInfo {
                pid: session.pid,
                pgid: session.pgid,
                capabilities: session.capabilities(),
                helper: true,
                events_active: session.shell_events_active(),
            },
            Self::Fallback { .. } => SessionInfo {
                pid: 0,
                pgid: 0,
                capabilities: fallback_capabilities(),
                helper: false,
                events_active: false,
            },
        }
    }

    /// 调用方已放弃时释放连接与临时文件。
    async fn abort(self) {
        match self {
            Self::Helper { handle, session } => {
                helper::abort(&handle, session).await;
            }
            Self::Fallback { handle, .. } => {
                let _ = handle.disconnect(Disconnect::ByApplication, "", "").await;
            }
        }
    }
}

/// `SshSession` 保存的只读会话信息。
struct SessionInfo {
    /// 根进程 ID；fallback 为 0。
    pid: u32,
    /// 进程组 ID；fallback 为 0。
    pgid: u32,
    /// 能力描述。
    capabilities: Capabilities,
    /// 是否为 helper 模式。
    helper: bool,
    /// 远端带外 shell 事件通道是否实际生效（helper + `started.events=true`）。
    events_active: bool,
}

/// 已建立的 SSH 会话句柄。
///
/// 所有方法都是非阻塞的：真正的 SSH 读写由专用运行时中的驱动任务完成，失败会
/// 以 [`SshError::Closed`] 或驱动任务结束体现。`Debug` 只输出对端标签，不含凭据。
pub struct SshSession {
    /// 控制通道；发送端 drop 后驱动任务会停止。
    control: UnboundedSender<SshControl>,
    /// 对端标签（user@host:port），可安全写入日志。
    peer: String,
    /// 只读会话信息。
    info: SessionInfo,
}

impl fmt::Debug for SshSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SshSession")
            .field("peer", &self.peer)
            .field("helper", &self.info.helper)
            .field("events_active", &self.info.events_active)
            .field("pid", &self.info.pid)
            .field("pgid", &self.info.pgid)
            .finish()
    }
}

impl SshSession {
    /// 建立连接、认证，优先尝试 helper；失败时回退 fallback。
    ///
    /// 返回会话句柄与事件接收端；事件为 [`ProcEvent`]，终态只发一次。
    ///
    /// # 错误
    /// 参数非法、建连/认证失败、主机密钥未被信任时返回 [`SshError`]；建连/认证
    /// 受 [`CONNECT_TIMEOUT`] 限制，helper 分发/握手另有独立的总超时与清理。
    pub fn connect(
        config: &SshConfig,
        request: SshConnectRequest,
    ) -> Result<(Self, Receiver<ProcEvent>), SshError> {
        validate(config, &request)?;
        let runtime = runtime()?;
        let peer = format!("{}@{}:{}", config.user, config.host, config.port);

        let (event_tx, event_rx) = channel(EVENT_QUEUE_CAPACITY);
        let (control_tx, control_rx) = unbounded_channel();
        let (init_tx, init_rx) = std::sync::mpsc::sync_channel(1);

        let config = config.clone();
        runtime.spawn(async move {
            match open_prepared(config, &request).await {
                Ok(prepared) => {
                    let info = prepared.info();
                    if init_tx.send(Ok(info)).is_err() {
                        // 调用方已超时放弃：主动断开并清理，避免后台连接/文件泄漏。
                        prepared.abort().await;
                        return;
                    }
                    match prepared {
                        Prepared::Helper { handle, session } => {
                            helper::drive(handle, session, event_tx, control_rx).await;
                        }
                        Prepared::Fallback { handle, channel } => {
                            drive_fallback(handle, channel, &request, event_tx, control_rx).await;
                        }
                    }
                }
                Err(error) => {
                    let _ = init_tx.send(Err(error));
                }
            }
        });

        // 该等待只用于把初始化错误同步给调用方；超时后后台任务仍会按内层超时
        // 收敛并清理（见 `open_prepared_inner`），不会留下远端临时目录。
        let info = match init_rx.recv_timeout(INIT_TIMEOUT) {
            Ok(Ok(info)) => info,
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(SshError::Connect {
                    message: format!("连接/认证/helper 初始化超过 {} 秒", INIT_TIMEOUT.as_secs()),
                });
            }
        };
        Ok((
            Self {
                control: control_tx,
                peer,
                info,
            },
            event_rx,
        ))
    }

    /// 向远端写入输入字节。
    pub fn write(&self, data: &[u8]) -> Result<(), SshError> {
        self.control
            .send(SshControl::Write(data.to_vec()))
            .map_err(|_| SshError::Closed)
    }

    /// 请求调整远端 PTY 尺寸。
    pub fn resize(&self, cols: u16, rows: u16) -> Result<(), SshError> {
        self.control
            .send(SshControl::Resize { cols, rows })
            .map_err(|_| SshError::Closed)
    }

    /// 发送信号（`SIGINT` / `SIGTERM` / `SIGKILL` 及无前缀别名）。
    pub fn signal(&self, name: &str) -> Result<(), SshError> {
        let sig =
            parse_signal(name).ok_or_else(|| SshError::UnsupportedSignal(name.to_string()))?;
        self.control
            .send(SshControl::Signal(sig))
            .map_err(|_| SshError::Closed)
    }

    /// 终止会话（幂等）：helper 模式下发 `terminate`，fallback 下发 `eof + close`。
    pub fn terminate(&self) {
        // 会话已结束时发送失败属正常路径，不向调用方报错。
        let _ = self.control.send(SshControl::Terminate);
    }

    /// 对端标签（`user@host:port`）。
    pub fn peer(&self) -> &str {
        &self.peer
    }

    /// 远端根进程 ID；fallback 模式为 0。
    pub fn pid(&self) -> u32 {
        self.info.pid
    }

    /// 远端进程组 ID；fallback 模式为 0。
    pub fn pgid(&self) -> u32 {
        self.info.pgid
    }

    /// 是否运行在 helper 模式。
    pub fn helper(&self) -> bool {
        self.info.helper
    }

    /// 后端能力。
    pub fn capabilities(&self) -> Capabilities {
        self.info.capabilities.clone()
    }

    /// 远端是否已启用带外 shell 事件通道。
    ///
    /// 只有 helper 模式、helper 声明 `shell_events` 能力且 `started.events=true`
    /// 时才为 `true`；service 据此关闭该终端的 in-band OSC 解析。
    pub fn shell_events(&self) -> bool {
        self.info.helper && self.info.events_active
    }

    /// 前台进程组；helper 模式返回根进程组，fallback 无值。
    pub fn foreground_pgrp(&self) -> Option<u32> {
        if self.info.helper && self.info.pgid > 0 {
            Some(self.info.pgid)
        } else {
            None
        }
    }

    /// 远端进程树；fallback 或查询超时返回空。
    pub fn tree(&self) -> Vec<ProcNode> {
        if !self.info.helper {
            return Vec::new();
        }
        let (reply, receiver) = helper::tree_channel();
        if self.control.send(SshControl::Tree(reply)).is_err() {
            return Vec::new();
        }
        receiver.recv_timeout(helper::TREE_WAIT).unwrap_or_default()
    }
}

/// 驱动任务控制消息。
pub(crate) enum SshControl {
    /// 写入标准输入。
    Write(Vec<u8>),
    /// 调整窗口尺寸。
    Resize { cols: u16, rows: u16 },
    /// 发送信号。
    Signal(Sig),
    /// 终止会话。
    Terminate,
    /// 查询远端进程树。
    Tree(Sender<Vec<ProcNode>>),
}

/// fallback 能力描述：无 helper，退出码/进程信息均为 best-effort。
fn fallback_capabilities() -> Capabilities {
    Capabilities {
        helper: false,
        version: "russh-fallback".to_string(),
        best_effort: true,
        runtime_catalog: false,
    }
}

/// 校验失败时记录的细节；连接失败后由 [`map_connect_error`] 转成明确错误。
#[derive(Debug, Clone)]
enum HostKeyFailure {
    /// 未知主机。
    Unknown {
        algorithm: String,
        fingerprint: String,
    },
    /// 与 known_hosts 记录不一致。
    Changed {
        algorithm: String,
        fingerprint: String,
        recorded_fingerprint: String,
    },
    /// 证书或存储错误等明确拒绝原因。
    Rejected(String),
}

/// 单次校验结论。
#[derive(Debug, Clone, PartialEq, Eq)]
enum HostKeyDecision {
    /// 接受连接。
    Accept,
    /// 未知主机；`ask` 策略需要客户端确认。
    Unknown,
    /// 密钥变化，必须硬失败。
    Changed { recorded: HostKeyInfo },
}

impl HostKeyDecision {
    /// 便于在 `match` 中区分；仅测试使用。
    #[cfg(test)]
    fn is_accept(&self) -> bool {
        matches!(self, Self::Accept)
    }
}

/// 主机密钥校验策略。
pub(crate) struct HostKeyPolicy {
    /// 策略。
    mode: SshHostKeyPolicy,
    /// 目标主机。
    host: String,
    /// 目标端口。
    port: u16,
    /// known_hosts 存储；`accept_any` 时可为空。
    known_hosts: Option<KnownHosts>,
    /// 本次请求允许首次信任的指纹。
    trusted_fingerprint: Option<String>,
    /// 校验失败细节，供连接错误分类。
    failure: Arc<Mutex<Option<HostKeyFailure>>>,
}

impl HostKeyPolicy {
    /// 构造策略；`failure` 与调用方共享。
    fn new(
        config: &SshConfig,
        mode: SshHostKeyPolicy,
        failure: Arc<Mutex<Option<HostKeyFailure>>>,
    ) -> Self {
        Self {
            mode,
            host: config.host.clone(),
            port: config.port,
            known_hosts: config
                .known_hosts_path
                .as_ref()
                .map(|path| KnownHosts::new(path.clone())),
            trusted_fingerprint: config
                .trusted_host_key
                .as_deref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            failure,
        }
    }

    /// 纯策略判定；不依赖 russh 类型，便于单测。
    fn evaluate(&self, info: &HostKeyInfo) -> Result<HostKeyDecision, KnownHostsError> {
        if self.mode == SshHostKeyPolicy::AcceptAny {
            return Ok(HostKeyDecision::Accept);
        }
        let store = self.known_hosts.as_ref().ok_or_else(|| {
            KnownHostsError::Io("未配置 known_hosts 路径，无法校验主机密钥".to_string())
        })?;
        let target = HostKeyTarget::new(self.host.clone(), self.port);
        match store.check(&target, info)? {
            KnownHostVerdict::Match => Ok(HostKeyDecision::Accept),
            KnownHostVerdict::Unknown => {
                if self.mode == SshHostKeyPolicy::Ask
                    && self.trusted_fingerprint.as_deref() == Some(info.fingerprint.as_str())
                {
                    store.trust(&target, info)?;
                    Ok(HostKeyDecision::Accept)
                } else {
                    Ok(HostKeyDecision::Unknown)
                }
            }
            KnownHostVerdict::Changed { recorded } => Ok(HostKeyDecision::Changed { recorded }),
        }
    }

    /// 记录失败原因；覆盖更具体的拒绝原因，保证错误分类稳定。
    fn record_failure(&self, failure: HostKeyFailure) {
        let mut slot = self
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.is_none() {
            *slot = Some(failure);
        }
    }
}

impl client::Handler for HostKeyPolicy {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let key = match server_public_key {
            PublicKeyOrCertificate::PublicKey { key, .. } => key,
            // 证书链校验尚未实现；明确拒绝，避免静默放行。
            PublicKeyOrCertificate::Certificate(_) => {
                self.record_failure(HostKeyFailure::Rejected(
                    "暂不支持 SSH 主机证书".to_string(),
                ));
                return Ok(false);
            }
        };
        let info = HostKeyInfo::new(
            key.algorithm().to_string(),
            key.fingerprint(HashAlg::Sha256).to_string(),
        );
        match self.evaluate(&info) {
            Ok(HostKeyDecision::Accept) => Ok(true),
            Ok(HostKeyDecision::Unknown) => {
                self.record_failure(HostKeyFailure::Unknown {
                    algorithm: info.algorithm,
                    fingerprint: info.fingerprint,
                });
                Ok(false)
            }
            Ok(HostKeyDecision::Changed { recorded }) => {
                self.record_failure(HostKeyFailure::Changed {
                    algorithm: info.algorithm,
                    fingerprint: info.fingerprint,
                    recorded_fingerprint: recorded.fingerprint,
                });
                Ok(false)
            }
            Err(KnownHostsError::KeyChanged { recorded }) => {
                self.record_failure(HostKeyFailure::Changed {
                    algorithm: info.algorithm,
                    fingerprint: info.fingerprint,
                    recorded_fingerprint: recorded.fingerprint,
                });
                Ok(false)
            }
            Err(error) => {
                self.record_failure(HostKeyFailure::Rejected(error.to_string()));
                Ok(false)
            }
        }
    }
}

/// 把策略解析后的连接错误转成明确的主机密钥错误。
fn map_connect_error(
    error: russh::Error,
    config: &SshConfig,
    failure: &Mutex<Option<HostKeyFailure>>,
) -> SshError {
    let failure = failure
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    match failure {
        Some(HostKeyFailure::Unknown {
            algorithm,
            fingerprint,
        }) => SshError::HostKeyUnknown {
            host: config.host.clone(),
            port: config.port,
            algorithm,
            fingerprint,
        },
        Some(HostKeyFailure::Changed {
            algorithm,
            fingerprint,
            recorded_fingerprint,
        }) => SshError::HostKeyChanged {
            host: config.host.clone(),
            port: config.port,
            algorithm,
            fingerprint,
            recorded_fingerprint,
        },
        Some(HostKeyFailure::Rejected(message)) => SshError::HostKeyRejected { message },
        None => map_russh_error(error),
    }
}

/// 解析生效策略：显式字段优先，否则兼容旧 `accept_any_host_key`。
fn host_key_policy(config: &SshConfig) -> Result<SshHostKeyPolicy, SshError> {
    match (config.host_key_policy, config.accept_any_host_key) {
        (Some(policy), true) if policy != SshHostKeyPolicy::AcceptAny => Err(SshError::BadRequest(
            "accept_any_host_key 与 host_key_policy 冲突".to_string(),
        )),
        (Some(policy), _) => Ok(policy),
        (None, true) => Ok(SshHostKeyPolicy::AcceptAny),
        (None, false) => Ok(SshHostKeyPolicy::Ask),
    }
}

/// 校验配置与请求，避免把明显非法参数带到网络阶段。
fn validate(config: &SshConfig, request: &SshConnectRequest) -> Result<(), SshError> {
    if config.host.trim().is_empty() {
        return Err(SshError::BadRequest("host 不能为空".to_string()));
    }
    if config.user.trim().is_empty() {
        return Err(SshError::BadRequest("user 不能为空".to_string()));
    }
    if config.port == 0 {
        return Err(SshError::BadRequest("port 不能为 0".to_string()));
    }
    if let SshAuth::Key { path, .. } = &config.auth {
        if path.trim().is_empty() {
            return Err(SshError::BadRequest("私钥路径不能为空".to_string()));
        }
    }
    // 非 AcceptAny 必须有服务端 known_hosts 路径；否则无法校验也不应静默接受。
    let policy = host_key_policy(config)?;
    if policy != SshHostKeyPolicy::AcceptAny && config.known_hosts_path.is_none() {
        return Err(SshError::BadRequest(
            "未配置 known_hosts 路径，无法校验 SSH 主机密钥".to_string(),
        ));
    }
    if policy == SshHostKeyPolicy::Ask {
        if let Some(fingerprint) = config.trusted_host_key.as_deref() {
            if !fingerprint.trim().starts_with("SHA256:") {
                return Err(SshError::BadRequest(
                    "trusted_host_key 必须是 SHA256 指纹".to_string(),
                ));
            }
        }
    }
    validate_helper(config)?;
    // fallback 无法应用远端 cwd/env；在 SSH 层同样拒绝，避免绕过 core 工厂
    // 直接调用时被静默忽略。
    if !helper_enabled(config) {
        if request.cwd.is_some() {
            return Err(SshError::BadRequest(
                "未启用 helper 时 SSH 终端不支持 cwd".to_string(),
            ));
        }
        if request.env.as_ref().is_some_and(|env| !env.is_empty()) {
            return Err(SshError::BadRequest(
                "未启用 helper 时 SSH 终端不支持 env".to_string(),
            ));
        }
    }
    if request.pty && (request.cols == 0 || request.rows == 0) {
        return Err(SshError::BadRequest(
            "申请 PTY 时 cols/rows 必须大于 0".to_string(),
        ));
    }
    Ok(())
}

/// 校验 helper 相关配置的一致性与路径安全。
///
/// - `upload_helper=true` 必须提供 `helper_local_path`；`helper_path` 可选，
///   给出时表示**持久安装目标**（可以是绝对路径或 `~/...`）；
/// - 不上传时 `helper_path` 表示远端已存在文件，`helper_local_path` 无意义；
/// - 任何显式 helper 路径都必须满足注入路径规则（绝对路径或 `~/`，拒绝
///   空白/控制字符/`..`/相对路径）；
/// - 只读模式允许配置 `helper_sha256` 做远端摘要校验，但必须有 `helper_path`。
fn validate_helper(config: &SshConfig) -> Result<(), SshError> {
    let path = config
        .helper_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty());

    if config.upload_helper {
        if config
            .helper_local_path
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .is_none()
        {
            return Err(SshError::BadRequest(
                "upload_helper=true 但缺少 helper_local_path".to_string(),
            ));
        }
        if let Some(path) = path {
            validate_injection_path(path, "helper_path")?;
        }
        return Ok(());
    }

    if config
        .helper_local_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .is_some()
    {
        return Err(SshError::BadRequest(
            "未启用 upload_helper 时不应设置 helper_local_path".to_string(),
        ));
    }
    match path {
        Some(path) => validate_injection_path(path, "helper_path"),
        None => {
            if configured_sha(config).is_some() {
                return Err(SshError::BadRequest(
                    "设置 helper_sha256 时必须同时设置 helper_path".to_string(),
                ));
            }
            Ok(())
        }
    }
}

/// 校验注入路径；错误里保留字段名便于定位。
fn validate_injection_path(path: &str, field: &str) -> Result<(), SshError> {
    validate_path_spec(path).map_err(|error| SshError::BadRequest(format!("{field} 非法: {error}")))
}

/// 归一化期望摘要，供校验与 bootstrap 共用。
fn configured_sha(config: &SshConfig) -> Option<String> {
    config
        .helper_sha256
        .as_deref()
        .map(|sha| sha.trim().to_ascii_lowercase())
        .filter(|sha| !sha.is_empty())
}

/// 专用 tokio 运行时；进程内只初始化一次，避免每个会话起新线程。
fn runtime() -> Result<&'static Runtime, SshError> {
    static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
    match RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(RUNTIME_WORKERS)
            .thread_name("termx-ssh")
            .enable_all()
            .build()
            .map_err(|error| error.to_string())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(message) => Err(SshError::Runtime(message.clone())),
    }
}

/// 连接、认证，并优先尝试 helper。
///
/// 各阶段使用独立超时：建连/认证受 [`CONNECT_TIMEOUT`] 限制，helper 分发与
/// 握手受 [`helper::HELPER_BOOTSTRAP_TIMEOUT`] 限制；外层等待更宽，避免内外层
/// 同时到期。内层超时会正常返回错误；已安装文件不会被删除。
async fn open_prepared(
    config: SshConfig,
    request: &SshConnectRequest,
) -> Result<Prepared, SshError> {
    let handle = match tokio::time::timeout(CONNECT_TIMEOUT, open_connection(&config)).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(SshError::Connect {
                message: format!("连接/认证超过 {} 秒", CONNECT_TIMEOUT.as_secs()),
            });
        }
    };

    if helper_enabled(&config) {
        let bootstrap = helper::bootstrap(handle, &config, request);
        let bootstrapped =
            match tokio::time::timeout(helper::HELPER_BOOTSTRAP_TIMEOUT, bootstrap).await {
                Ok(result) => result,
                Err(_) => {
                    return Err(SshError::Connect {
                        message: format!(
                            "helper 分发/握手超过 {} 秒",
                            helper::HELPER_BOOTSTRAP_TIMEOUT.as_secs()
                        ),
                    });
                }
            };
        match bootstrapped {
            Ok((handle, session)) => return Ok(Prepared::Helper { handle, session }),
            Err((handle, error)) => {
                // 需要授权或注入失败必须原样上报，绝不能用 fallback 掩盖；
                // 只有 helper 协议/版本/握手等普通错误才继续走 fallback。
                if must_not_fallback(&error) {
                    let _ = handle.disconnect(Disconnect::ByApplication, "", "").await;
                    return Err(error);
                }
                // helper 协议不可用不阻塞：仅在请求依赖 helper 能力时才明确报错，
                // 否则复用同一连接回退 fallback。
                tracing::info!(reason = %error, "helper 协议不可用，回退 SSH fallback");
                if request.cwd.is_some() || request.env.as_ref().is_some_and(|env| !env.is_empty())
                {
                    let _ = handle.disconnect(Disconnect::ByApplication, "", "").await;
                    return Err(SshError::BadRequest(
                        "helper 不可用，无法在 fallback 模式应用 cwd/env".to_string(),
                    ));
                }
                let channel = open_fallback_channel(&handle, request).await?;
                return Ok(Prepared::Fallback { handle, channel });
            }
        }
    }

    let channel = open_fallback_channel(&handle, request).await?;
    Ok(Prepared::Fallback { handle, channel })
}

/// 建立 TCP/SSH 连接并完成认证。
async fn open_connection(config: &SshConfig) -> Result<client::Handle<HostKeyPolicy>, SshError> {
    let mode = host_key_policy(config)?;
    let failure = Arc::new(Mutex::new(None));
    let policy = HostKeyPolicy::new(config, mode, Arc::clone(&failure));
    let ssh_config = Arc::new(client::Config::default());
    let mut handle = client::connect(ssh_config, (config.host.as_str(), config.port), policy)
        .await
        .map_err(|error| map_connect_error(error, config, &failure))?;
    authenticate(&mut handle, config).await?;
    Ok(handle)
}

/// 打开 fallback channel：按需申请 PTY，再申请 shell 或执行命令。
async fn open_fallback_channel(
    handle: &client::Handle<HostKeyPolicy>,
    request: &SshConnectRequest,
) -> Result<Channel<Msg>, SshError> {
    let ssh_channel = handle
        .channel_open_session()
        .await
        .map_err(map_russh_error)?;
    if request.pty {
        ssh_channel
            .request_pty(
                true,
                &request.term,
                u32::from(request.cols),
                u32::from(request.rows),
                0,
                0,
                &[],
            )
            .await
            .map_err(map_russh_error)?;
    }
    match &request.mode {
        SshChannelMode::Shell => {
            ssh_channel
                .request_shell(true)
                .await
                .map_err(map_russh_error)?;
        }
        SshChannelMode::Exec(command) => {
            ssh_channel
                .exec(true, command.as_bytes())
                .await
                .map_err(map_russh_error)?;
        }
    }
    Ok(ssh_channel)
}

/// 按配置执行密码或私钥认证。
async fn authenticate(
    handle: &mut client::Handle<HostKeyPolicy>,
    config: &SshConfig,
) -> Result<(), SshError> {
    let accepted = match &config.auth {
        SshAuth::Password { password } => handle
            .authenticate_password(config.user.as_str(), password.as_str())
            .await
            .map_err(map_russh_error)?,
        SshAuth::Key { path, passphrase } => {
            let key = load_secret_key(path, passphrase.as_deref()).map_err(|error| {
                SshError::Connect {
                    message: format!("读取私钥失败: {error}"),
                }
            })?;
            let hash = handle
                .best_supported_rsa_hash()
                .await
                .map_err(map_russh_error)?
                .flatten();
            handle
                .authenticate_publickey(
                    config.user.as_str(),
                    PrivateKeyWithHashAlg::new(Arc::new(key), hash),
                )
                .await
                .map_err(map_russh_error)?
        }
    };
    if accepted.success() {
        Ok(())
    } else {
        Err(SshError::AuthFailed)
    }
}

/// 驱动任务：转发输出、执行控制消息，并在 channel 结束时发终态事件。
async fn drive_fallback(
    handle: client::Handle<HostKeyPolicy>,
    ssh_channel: Channel<Msg>,
    request: &SshConnectRequest,
    events: mpsc::Sender<ProcEvent>,
    mut control: mpsc::UnboundedReceiver<SshControl>,
) {
    let merged_stream = if request.pty {
        OutputStream::Pty
    } else {
        OutputStream::Stdout
    };
    let (mut read_half, write_half) = ssh_channel.split();
    let mut exit_code = None;
    let mut exit_signal = None;

    loop {
        tokio::select! {
            message = read_half.wait() => match message {
                None => break,
                Some(ChannelMsg::Data { data }) => {
                    let event = ProcEvent::Output { data: data.to_vec(), stream: merged_stream };
                    if events.send(event).await.is_err() {
                        break;
                    }
                }
                Some(ChannelMsg::ExtendedData { data, ext }) => {
                    // ext=1 为 stderr（RFC 4254 §5.2）；其余扩展流按 stdout 处理。
                    let stream = if ext == 1 { OutputStream::Stderr } else { merged_stream };
                    let event = ProcEvent::Output { data: data.to_vec(), stream };
                    if events.send(event).await.is_err() {
                        break;
                    }
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    exit_code = Some(exit_status as i32);
                }
                Some(ChannelMsg::ExitSignal { signal_name, .. }) => {
                    exit_signal = Some(signal_name);
                }
                Some(ChannelMsg::Close) => break,
                Some(_) => {}
            },
            command = control.recv() => match command {
                None => break,
                Some(SshControl::Write(data)) => {
                    if write_half.data_bytes(data).await.is_err() {
                        break;
                    }
                }
                Some(SshControl::Resize { cols, rows }) => {
                    let _ = write_half
                        .window_change(u32::from(cols), u32::from(rows), 0, 0)
                        .await;
                }
                Some(SshControl::Signal(signal)) => {
                    let _ = write_half.signal(signal).await;
                }
                Some(SshControl::Terminate) => {
                    let _ = write_half.eof().await;
                    let _ = write_half.close().await;
                    break;
                }
                Some(SshControl::Tree(reply)) => {
                    // fallback 没有远端进程树；同步返回空，避免调用方等待。
                    let _ = reply.send(Vec::new());
                }
            },
        }
    }

    let terminal = match (exit_signal, exit_code) {
        (Some(signal), _) => ProcEvent::Signaled(signal_number(&signal)),
        (None, Some(code)) => ProcEvent::Exited(code),
        // 只有 channel 关闭时没有可靠退出码，按 best-effort 语义收尾。
        (None, None) => ProcEvent::Exited(BEST_EFFORT_EXIT_CODE),
    };
    let _ = events.send(terminal).await;
    let _ = handle.disconnect(Disconnect::ByApplication, "", "").await;
}

/// 把 russh 错误映射为带安全提示的分类错误。
pub(crate) fn map_russh_error(error: russh::Error) -> SshError {
    let message = error.to_string();
    match &error {
        russh::Error::UnknownKey | russh::Error::KeyChanged { .. } | russh::Error::Keys(_) => {
            SshError::HostKeyRejected {
                message: format!(
                    "{message}（需 known_hosts 记录，或显式设置 accept_any_host_key=true）"
                ),
            }
        }
        _ => SshError::Connect { message },
    }
}

/// 把协议信号名映射为 russh 信号；与本地后端一致，只接受 INT/TERM/KILL 及别名。
fn parse_signal(name: &str) -> Option<Sig> {
    let upper = name.trim().to_ascii_uppercase();
    let bare = upper.strip_prefix("SIG").unwrap_or(upper.as_str());
    match bare {
        "INT" => Some(Sig::INT),
        "TERM" => Some(Sig::TERM),
        "KILL" => Some(Sig::KILL),
        _ => None,
    }
}

/// 把 SSH 信号名映射为 Unix 信号编号，供 `ProcEvent::Signaled` 复用。
///
/// 未知/自定义信号不猜测为 `SIGTERM`：能解析名称/编号时原样映射，
/// 否则返回 `0`（由 core 映射为协议里的 `"UNKNOWN"`）。
fn signal_number(signal: &Sig) -> i32 {
    match signal {
        Sig::HUP => 1,
        Sig::INT => 2,
        Sig::QUIT => 3,
        Sig::ILL => 4,
        Sig::ABRT => 6,
        Sig::FPE => 8,
        Sig::KILL => 9,
        Sig::USR1 => 10,
        Sig::SEGV => 11,
        Sig::PIPE => 13,
        Sig::ALRM => 14,
        Sig::TERM => 15,
        Sig::Custom(name) => signal_number_by_name(name).unwrap_or(0),
    }
}

/// 解析信号名（大小写与 `SIG` 前缀不敏感）或纯编号。
fn signal_number_by_name(name: &str) -> Option<i32> {
    let upper = name.trim().to_ascii_uppercase();
    let bare = upper.strip_prefix("SIG").unwrap_or(upper.as_str());
    match bare {
        "HUP" => Some(1),
        "INT" => Some(2),
        "QUIT" => Some(3),
        "ILL" => Some(4),
        "ABRT" => Some(6),
        "FPE" => Some(8),
        "KILL" => Some(9),
        "USR1" => Some(10),
        "SEGV" => Some(11),
        "PIPE" => Some(13),
        "ALRM" => Some(14),
        "TERM" => Some(15),
        other => other.parse::<i32>().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_line_quotes_every_argument() {
        let line = build_command_line(
            "sh",
            &["-c".to_string(), "echo hello_ssh; exit 3".to_string()],
        );
        assert_eq!(line, "'sh' '-c' 'echo hello_ssh; exit 3'");

        let quoted = build_command_line("echo", &["it's".to_string()]);
        assert_eq!(quoted, "'echo' 'it'\\''s'");
    }

    #[test]
    fn from_create_routes_shell_and_exec() {
        assert_eq!(
            SshConnectRequest::from_create("", &[], 80, 24),
            SshConnectRequest::shell(80, 24)
        );
        assert_eq!(
            SshConnectRequest::from_create("shell", &[], 80, 24),
            SshConnectRequest::shell(80, 24)
        );
        assert_eq!(
            SshConnectRequest::from_create(" uptime ", &[], 80, 24).mode,
            SshChannelMode::Exec("' uptime '".to_string())
        );

        // 只有 pty + basename 属于结构化程序才请求带外事件。
        for cmd in [
            SHELL_EVENT_PROGRAM,
            "/usr/local/bin/termx-pwsh",
            "C:\\termx\\termx-pwsh",
            "termx-bash",
            "/usr/local/bin/termx-bash",
            "C:\\termx\\termx-bash",
        ] {
            assert!(
                SshConnectRequest::from_create(cmd, &[], 80, 24).events,
                "{cmd:?} 应请求 shell 事件"
            );
        }
        for cmd in [
            "pwsh",
            "bash",
            "termx-pwsh-old",
            "termx-bash-old",
            "shell",
            "",
        ] {
            assert!(
                !SshConnectRequest::from_create(cmd, &[], 80, 24).events,
                "{cmd:?} 不应请求 shell 事件"
            );
        }
    }

    /// 测试用配置：默认不启用 helper。
    fn base_config() -> SshConfig {
        SshConfig {
            host: "127.0.0.1".to_string(),
            port: 22,
            user: "tester".to_string(),
            auth: SshAuth::Password {
                password: "secret".to_string(),
            },
            helper_path: None,
            helper_local_path: None,
            helper_sha256: None,
            upload_helper: false,
            accept_any_host_key: true,
            host_key_policy: None,
            trusted_host_key: None,
            known_hosts_path: None,
            injection: termx_protocol::InjectionPlan::default(),
        }
    }

    /// 校验在联网前完成，配置非法时直接返回 `BadRequest`。
    fn connect_error(config: SshConfig, request: SshConnectRequest) -> SshError {
        SshSession::connect(&config, request).expect_err("非法配置应被拒绝")
    }

    /// 注入失败与授权要求都必须直接上报；只有协议/连接错误才允许 fallback。
    #[test]
    fn injection_failures_never_fall_back() {
        let failed = SshError::InjectionFailed {
            path: "~/.termx/bin/termx-helper".to_string(),
            message: "目标不可写".to_string(),
        };
        assert!(must_not_fallback(&failed));
        assert!(must_not_fallback(&SshError::ConsentRequired(
            termx_protocol::InjectionConsent {
                host: "h".to_string(),
                user: "u".to_string(),
                writes: Vec::new(),
            }
        )));
        assert!(!must_not_fallback(&SshError::Connect {
            message: "握手失败".to_string(),
        }));
    }

    /// 注入失败消息带目标路径，且不泄露配置里的凭据。
    #[test]
    fn injection_failure_message_has_path_without_credentials() {
        let config = base_config();
        let password = match &config.auth {
            SshAuth::Password { password } => password.clone(),
            _ => unreachable!("测试配置固定使用密码认证"),
        };
        let text = SshError::InjectionFailed {
            path: "/opt/termx-helper".to_string(),
            message: "预检失败".to_string(),
        }
        .to_string();
        assert!(
            text.contains("/opt/termx-helper"),
            "消息应包含目标路径: {text}"
        );
        assert!(!text.contains(&password), "消息不得包含凭据: {text}");
    }

    #[test]
    fn upload_helper_requires_local_path() {
        let mut config = base_config();
        config.upload_helper = true;
        let error = connect_error(config, SshConnectRequest::shell(80, 24));
        assert!(
            matches!(error, SshError::BadRequest(message) if message.contains("helper_local_path"))
        );
    }

    #[test]
    fn upload_mode_accepts_helper_path_as_install_target() {
        let mut config = base_config();
        config.upload_helper = true;
        config.helper_local_path = Some("./termx-helper".to_string());
        config.helper_path = Some("~/.termx/bin/termx-helper".to_string());
        assert!(validate_helper(&config).is_ok(), "helper_path 是安装目标");

        config.helper_path = Some("relative/helper".to_string());
        assert!(matches!(
            validate_helper(&config),
            Err(SshError::BadRequest(message)) if message.contains("helper_path")
        ));
    }

    #[test]
    fn helper_path_must_follow_injection_rules() {
        for path in [
            "/usr/local/bin/termx helper",
            "relative/helper",
            "/usr/local/../termx-helper",
        ] {
            let mut config = base_config();
            config.helper_path = Some(path.to_string());
            let error = connect_error(config, SshConnectRequest::shell(80, 24));
            assert!(
                matches!(error, SshError::BadRequest(message) if message.contains("helper_path")),
                "路径 {path:?} 应被拒绝"
            );
        }
    }

    #[test]
    fn local_helper_path_requires_upload_flag() {
        let mut config = base_config();
        config.helper_local_path = Some("./termx-helper".to_string());
        let error = connect_error(config, SshConnectRequest::shell(80, 24));
        assert!(
            matches!(error, SshError::BadRequest(message) if message.contains("upload_helper"))
        );
    }

    #[test]
    fn helper_sha_requires_helper_path() {
        let mut config = base_config();
        config.helper_sha256 = Some("f".repeat(64));
        let error = connect_error(config, SshConnectRequest::shell(80, 24));
        assert!(
            matches!(error, SshError::BadRequest(message) if message.contains("helper_sha256"))
        );
    }

    #[test]
    fn fallback_rejects_cwd_and_env() {
        let request =
            SshConnectRequest::shell(80, 24).with_working_dir(Some("/tmp".to_string()), None);
        assert!(matches!(
            connect_error(base_config(), request),
            SshError::BadRequest(message) if message.contains("cwd")
        ));

        let mut env = BTreeMap::new();
        env.insert("TERM".to_string(), "xterm".to_string());
        let request = SshConnectRequest::shell(80, 24).with_working_dir(None, Some(env));
        assert!(matches!(
            connect_error(base_config(), request),
            SshError::BadRequest(message) if message.contains("env")
        ));
    }

    #[test]
    fn helper_enabled_matches_validation_rules() {
        assert!(!helper_enabled(&base_config()));

        let mut config = base_config();
        config.helper_path = Some("/usr/local/bin/termx-helper".to_string());
        assert!(helper_enabled(&config));

        let mut config = base_config();
        config.upload_helper = true;
        assert!(helper_enabled(&config));
    }

    #[test]
    fn unknown_signals_are_not_mapped_to_sigterm() {
        assert_eq!(signal_number(&Sig::Custom("BOGUS".to_string())), 0);
        assert_eq!(signal_number(&Sig::Custom("USR1".to_string())), 10);
        assert_eq!(signal_number(&Sig::Custom("SIGTERM".to_string())), 15);
        assert_eq!(signal_number_by_name("BOGUS"), None);
    }

    #[test]
    fn signal_parsing_matches_local_backend() {
        assert!(matches!(parse_signal("int"), Some(Sig::INT)));
        assert!(matches!(parse_signal(" SIGTERM "), Some(Sig::TERM)));
        assert!(matches!(parse_signal("KILL"), Some(Sig::KILL)));
        assert!(parse_signal("SIGHUP").is_none());
    }

    /// 构造策略测试配置。
    fn host_key_config(
        mode: SshHostKeyPolicy,
        path: Option<std::path::PathBuf>,
        trusted: Option<&str>,
    ) -> SshConfig {
        let mut config = base_config();
        config.accept_any_host_key = false;
        config.host_key_policy = Some(mode);
        config.known_hosts_path = path;
        config.trusted_host_key = trusted.map(str::to_string);
        config
    }

    /// 测试用主机密钥信息。
    fn host_key_info(fingerprint: &str) -> HostKeyInfo {
        HostKeyInfo::new("ssh-ed25519", format!("SHA256:{fingerprint}"))
    }

    #[test]
    fn policy_resolves_legacy_accept_any_and_rejects_conflicts() {
        let mut config = base_config();
        assert_eq!(
            host_key_policy(&config).expect("旧开关"),
            SshHostKeyPolicy::AcceptAny
        );

        config.accept_any_host_key = false;
        assert_eq!(
            host_key_policy(&config).expect("默认 Ask"),
            SshHostKeyPolicy::Ask
        );

        config.accept_any_host_key = true;
        config.host_key_policy = Some(SshHostKeyPolicy::Strict);
        assert!(matches!(
            host_key_policy(&config),
            Err(SshError::BadRequest(message)) if message.contains("冲突")
        ));
    }

    #[test]
    fn ask_policy_requires_explicit_fingerprint_and_writes_once() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("known_hosts");
        let info = host_key_info("abc");

        let policy = HostKeyPolicy::new(
            &host_key_config(SshHostKeyPolicy::Ask, Some(path.clone()), None),
            SshHostKeyPolicy::Ask,
            Arc::new(Mutex::new(None)),
        );
        assert_eq!(
            policy.evaluate(&info).expect("未知"),
            HostKeyDecision::Unknown
        );
        assert!(!path.exists(), "未经确认不得写 known_hosts");

        let policy = HostKeyPolicy::new(
            &host_key_config(
                SshHostKeyPolicy::Ask,
                Some(path.clone()),
                Some("SHA256:abc"),
            ),
            SshHostKeyPolicy::Ask,
            Arc::new(Mutex::new(None)),
        );
        assert!(policy.evaluate(&info).expect("首次信任").is_accept());
        assert_eq!(
            policy.evaluate(&info).expect("二次匹配"),
            HostKeyDecision::Accept
        );
        assert!(path.exists(), "确认后应落盘");
    }

    #[test]
    fn strict_policy_never_trusts_unknown_even_with_fingerprint() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("known_hosts");
        let info = host_key_info("abc");
        let policy = HostKeyPolicy::new(
            &host_key_config(
                SshHostKeyPolicy::Strict,
                Some(path.clone()),
                Some("SHA256:abc"),
            ),
            SshHostKeyPolicy::Strict,
            Arc::new(Mutex::new(None)),
        );
        assert_eq!(
            policy.evaluate(&info).expect("严格拒绝"),
            HostKeyDecision::Unknown
        );
        assert!(!path.exists());
    }

    #[test]
    fn changed_key_is_hard_failure() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("known_hosts");
        let old = host_key_info("old");
        let config = host_key_config(
            SshHostKeyPolicy::Ask,
            Some(path.clone()),
            Some("SHA256:old"),
        );
        let policy = HostKeyPolicy::new(&config, SshHostKeyPolicy::Ask, Arc::new(Mutex::new(None)));
        assert!(policy.evaluate(&old).expect("首次信任").is_accept());

        let new = host_key_info("new");
        let decision = policy.evaluate(&new).expect("查询");
        assert!(
            matches!(decision, HostKeyDecision::Changed { ref recorded } if recorded.fingerprint == "SHA256:old"),
            "指纹变化必须拒绝: {decision:?}"
        );
    }

    #[test]
    fn accept_any_never_writes_known_hosts() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("known_hosts");
        let config = host_key_config(SshHostKeyPolicy::AcceptAny, Some(path.clone()), None);
        let policy = HostKeyPolicy::new(
            &config,
            SshHostKeyPolicy::AcceptAny,
            Arc::new(Mutex::new(None)),
        );
        assert!(
            policy
                .evaluate(&host_key_info("any"))
                .expect("接受")
                .is_accept()
        );
        assert!(!path.exists(), "AcceptAny 不得落盘");
    }

    #[test]
    fn validate_requires_known_hosts_path_for_non_accept_any() {
        let mut config = base_config();
        config.accept_any_host_key = false;
        let error = connect_error(config, SshConnectRequest::shell(80, 24));
        assert!(
            matches!(&error, SshError::BadRequest(message) if message.contains("known_hosts")),
            "{error:?}"
        );
    }

    #[test]
    fn validate_rejects_malformed_trusted_fingerprint() {
        let mut config = base_config();
        config.accept_any_host_key = false;
        config.known_hosts_path = Some(std::path::PathBuf::from("known_hosts"));
        config.trusted_host_key = Some("not-a-fingerprint".to_string());
        let error = connect_error(config, SshConnectRequest::shell(80, 24));
        assert!(
            matches!(&error, SshError::BadRequest(message) if message.contains("trusted_host_key")),
            "{error:?}"
        );
    }
}
