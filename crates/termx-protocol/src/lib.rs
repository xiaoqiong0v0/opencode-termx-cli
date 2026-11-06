//! termx 协议 v1 的类型定义。
//!
//! 本 crate 是 `docs/specs/PROTOCOL.md` 的代码映射，不含任何 OS 依赖，可独立单测。
//! 约定：每个 WebSocket text frame 承载一个完整 JSON 对象；事件无 `id`，
//! 以 `terminal_id` + 每终端单调 `seq` 定位；`ts` 为 RFC3339 UTC 字符串。

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

pub mod helper;
pub mod injection;
pub mod shell_events;

pub use injection::{
    InjectionConsent, InjectionConsentMode, InjectionPathError, InjectionPlan, InjectionWrite,
    InjectionWriteAction, InjectionWriteKind, InjectionWriteReason, expand_home,
    validate_path_spec,
};

/// 主协议版本标识，对应 `hello.proto`。
pub const PROTOCOL_VERSION: &str = "termx/1";

/// helper 协议版本标识，对应 `helper.hello.proto`。
pub const HELPER_PROTOCOL_VERSION: &str = "termx-helper/1";

/// helper 能力：支持带外 shell 事件通道（`spawn.events` / `started.events`）。
pub const SHELL_EVENTS_CAPABILITY: &str = "shell_events";

/// Web 未绑定终端的保留 `root_session_id`。
///
/// 该值的终端不参与「无存活会话」超时扫描，只能手动终止；非管理员连接
/// 不能以它作为自身会话，也不能访问该会话下的终端。见 `docs/specs/PROTOCOL.md` §6。
pub const WEB_ROOT_SESSION_ID: &str = "@web";

/// 协议 JSON 编解码错误。
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// JSON 序列化或反序列化失败。
    #[error("协议 JSON 编解码失败: {0}")]
    Json(#[from] serde_json::Error),
}

/// 将协议消息序列化为单个 text frame 的 JSON 文本。
pub fn to_json<T: Serialize>(value: &T) -> Result<String, CodecError> {
    Ok(serde_json::to_string(value)?)
}

/// 从单个 text frame 解析协议消息。
pub fn from_json<T: for<'de> Deserialize<'de>>(text: &str) -> Result<T, CodecError> {
    Ok(serde_json::from_str(text)?)
}

/// 生命周期状态（正交维度之一）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    /// 已创建，尚未开始运行。
    Created,
    /// 运行中。
    Running,
    /// 正在退出。
    Exiting,
    /// 自然退出（终态）。
    Exited,
    /// 被信号/控制中断（终态）。
    Interrupted,
    /// 失败（终态）。
    Error,
}

impl LifecycleState {
    /// 是否为终态（`exited` / `interrupted` / `error`）。
    ///
    /// 终态之后拒绝写操作，事件只发一次。
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Exited | Self::Interrupted | Self::Error)
    }
}

/// 交互状态（运行期子状态，不参与生命周期）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionState {
    /// 无交互对象。
    None,
    /// 前台处于交互式程序。
    Interactive,
    /// 疑似等待输入。
    WaitingInput,
}

/// 交互/推断结论的置信度。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// 低：仅启发式。
    Low,
    /// 中：多信号一致。
    Medium,
    /// 高：shell integration 等直接证据。
    High,
}

/// 终端输出流来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stream {
    /// PTY 合并流。
    Pty,
    /// 独立 stdout。
    Stdout,
    /// 独立 stderr。
    Stderr,
}

/// 终端创建模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// 伪终端模式（合并 stdout/stderr）。
    Pty,
    /// 管道 stdio 模式。
    Stdio,
}

/// SSH 认证方式。
///
/// `Debug` 实现会遮蔽密码/口令；`Serialize` 保留原值以便经本机 WebSocket 传输，
/// 调用方不得把本类型或 [`SshConfig`] 以 Debug/日志形式输出。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SshAuth {
    /// 密码认证。
    Password {
        /// 登录密码（Debug 时遮蔽）。
        password: String,
    },
    /// 私钥认证（M6b-1 预留，russh 侧已接线）。
    Key {
        /// 私钥文件路径。
        path: String,
        /// 私钥口令（Debug 时遮蔽）。
        #[serde(skip_serializing_if = "Option::is_none")]
        passphrase: Option<String>,
    },
}

impl fmt::Debug for SshAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Password { .. } => formatter
                .debug_struct("Password")
                .field("password", &"<redacted>")
                .finish(),
            Self::Key { path, passphrase } => formatter
                .debug_struct("Key")
                .field("path", path)
                .field("passphrase", &passphrase.as_ref().map(|_| "<redacted>"))
                .finish(),
        }
    }
}

/// SSH 主机密钥信任策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SshHostKeyPolicy {
    /// 默认：已知主机必须匹配；未知主机要求用户确认（首次信任）。
    #[default]
    Ask,
    /// 只接受 known_hosts 中已记录的密钥，未知主机直接拒绝。
    Strict,
    /// 高级/测试：接受任意主机密钥，且不写入 known_hosts。
    AcceptAny,
}

/// 未知/变化的主机密钥信息，供客户端展示并确认首次信任。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshHostKeyInfo {
    /// 目标主机。
    pub host: String,
    /// 目标端口。
    pub port: u16,
    /// 密钥算法（如 `ssh-ed25519`）。
    pub algorithm: String,
    /// 当前密钥的 SHA256 指纹（`SHA256:...`）。
    pub fingerprint: String,
    /// 密钥变化时已记录的旧指纹；未知主机为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_fingerprint: Option<String>,
}

/// `terminal.create` 的 SSH 连接参数。
///
/// 存在该字段时 `mode` 必须为 [`Mode::Pty`]，由 termx-ssh 建立远端会话。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshConfig {
    /// 主机名或 IP。
    pub host: String,
    /// 端口。
    pub port: u16,
    /// 登录用户。
    pub user: String,
    /// 认证方式。
    pub auth: SshAuth,
    /// 远端已存在的 helper 路径；仅在 `upload_helper=false` 时使用。
    ///
    /// 必须是单个路径（拒绝空白/控制字符）；与 `upload_helper=true` 互斥。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub helper_path: Option<String>,
    /// 本地 helper 二进制路径；`upload_helper=true` 时上传到远端专用临时目录。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub helper_local_path: Option<String>,
    /// 期望的 helper 二进制 sha256（小写十六进制）；不匹配时不使用 helper。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub helper_sha256: Option<String>,
    /// 是否把 `helper_local_path` 上传到远端 `mktemp -d` 新建的私有目录（默认 false）。
    ///
    /// 为 `true` 时必须提供 `helper_local_path`，且不得同时设置 `helper_path`。
    #[serde(default)]
    pub upload_helper: bool,
    /// 是否接受任意主机密钥。
    ///
    /// 兼容旧客户端的布尔开关：`true` 等价于 [`SshHostKeyPolicy::AcceptAny`]，
    /// 仅测试或用户显式开启时使用；不写入 known_hosts。
    #[serde(default)]
    pub accept_any_host_key: bool,
    /// 主机密钥策略；缺省时由 `accept_any_host_key` 推导（`true` → `accept_any`，
    /// 否则 `ask`）。显式设置且与 `accept_any_host_key` 冲突时连接前拒绝。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_key_policy: Option<SshHostKeyPolicy>,
    /// 用户确认后的主机密钥 SHA256 指纹（仅对本次请求生效）。
    ///
    /// `ask` 策略下与本次服务端密钥一致时才写入 known_hosts 并继续；不匹配
    /// 按密钥变化硬失败。`strict` 忽略该字段；`accept_any` 不需要该字段。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_host_key: Option<String>,
    /// 服务端内部 known_hosts 路径；由 service 注入，**不进入 JSON**。
    #[serde(skip)]
    pub known_hosts_path: Option<std::path::PathBuf>,
    /// 内部注入计划；由 service 按配置与本次请求组装，**不进入 JSON**。
    ///
    /// 直接反序列化 `terminal.create` 时取 [`InjectionPlan::default`]
    /// （`consent=ask`、`allow=false`），保证需要写入时默认要求授权。
    #[serde(skip)]
    pub injection: InjectionPlan,
}

impl fmt::Debug for SshConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SshConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("auth", &self.auth)
            .field("helper_path", &self.helper_path)
            .field("helper_local_path", &self.helper_local_path)
            .field("helper_sha256", &self.helper_sha256)
            .field("upload_helper", &self.upload_helper)
            .field("accept_any_host_key", &self.accept_any_host_key)
            .field("host_key_policy", &self.host_key_policy)
            .field(
                "trusted_host_key",
                &self.trusted_host_key.as_ref().map(|_| "<set>"),
            )
            .field("known_hosts_path", &self.known_hosts_path)
            .field("injection", &self.injection)
            .finish()
    }
}

/// 协议错误码，序列化为 snake_case。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// token 错误或握手失败。
    AuthFailed,
    /// 主协议/helper 版本不匹配。
    VersionMismatch,
    /// 参数或帧非法。
    BadRequest,
    /// 终端不存在或已清理。
    NotFound,
    /// 对终态终端做写操作。
    AlreadyTerminal,
    /// 同名 `(root_session_id, name)` 已存在且未带 `force`。
    SessionExists,
    /// 非 lease owner 或权限不足。
    PermissionDenied,
    /// 超过会话/缓冲/队列上限。
    LimitExceeded,
    /// 进程创建失败。
    SpawnFailed,
    /// PTY/ConPTY 创建失败。
    PtyFailed,
    /// SSH 连接失败。
    SshConnectFailed,
    /// helper 握手/启动失败。
    HelperFailed,
    /// 帧或状态机违例。
    ProtocolError,
    /// 远端注入需要用户授权；`error.injection` 描述将写入的文件。
    ConsentRequired,
    /// SSH 主机密钥未知；`error.host_key` 带指纹，客户端确认后可重试。
    HostKeyUnknown,
    /// SSH 主机密钥与 known_hosts 记录不一致；可能被中间人劫持，硬失败。
    HostKeyChanged,
    /// 服务崩溃/重启（客户端合成）。
    ServiceLost,
    /// 未分类错误。
    Internal,
}

impl ErrorCode {
    /// 该错误码是否建议客户端重试（对齐 `docs/specs/PROTOCOL.md` §10）。
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::LimitExceeded
                | Self::SshConnectFailed
                | Self::HelperFailed
                | Self::ServiceLost
                | Self::Internal
        )
    }
}

/// 协议错误载荷。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    /// 错误码。
    pub code: ErrorCode,
    /// 人类可读说明（不含敏感数据）。
    pub message: String,
    /// 是否可重试。
    pub retryable: bool,
    /// 相关终端 ID；`session_exists` 时携带旧终端 ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<String>,
    /// `consent_required` 时的写入计划；其他错误码缺省。
    ///
    /// 用 `Box` 装以缩小 `Result` 的错误分支（clippy `result_large_err`）；
    /// serde 对 `Box<T>` 透明，JSON 形状不变。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub injection: Option<Box<InjectionConsent>>,
    /// `host_key_unknown` / `host_key_changed` 时的主机密钥信息。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_key: Option<Box<SshHostKeyInfo>>,
}

/// 请求消息。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    /// 客户端生成、同一连接内唯一的请求 ID。
    pub id: String,
    /// RPC 方法名，如 `terminal.create`。
    pub method: String,
    /// 方法参数，形状由方法决定。
    pub params: serde_json::Value,
}

/// 响应消息；成功时带 `result`，失败时带 `error`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    /// 与请求配对的 ID。
    pub id: String,
    /// 是否成功。
    pub ok: bool,
    /// 成功结果；失败时缺省。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// 失败错误；成功时缺省。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,
}

/// 事件信封：共用 `terminal_id` / `seq` / `ts`，具体类型由 `kind` 决定。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// 所属终端 ID。
    pub terminal_id: String,
    /// 每终端单调递增序号，从 1 开始。
    pub seq: u64,
    /// RFC3339 UTC 时间戳。
    pub ts: String,
    /// 事件类型与载荷（以 `type` 标签平铺）。
    #[serde(flatten)]
    pub kind: EventKind,
}

/// 事件类型与载荷。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    /// 进程已创建。
    Started {
        /// 进程 ID。
        pid: u32,
        /// 进程组 ID。
        pgid: u32,
        /// 创建模式。
        mode: Mode,
    },
    /// 输出增量。
    Output {
        /// base64 编码的输出字节。
        data: String,
        /// 输出流来源。
        stream: Stream,
    },
    /// 背压丢弃的最旧 output 区间。
    OutputDropped {
        /// 区间起始 seq（含）。
        from_seq: u64,
        /// 区间结束 seq（含）。
        to_seq: u64,
    },
    /// 疑似等待输入（非终态）。
    WaitingInput {
        /// 置信度。
        confidence: Confidence,
        /// 推断依据，如 `prompt` / `stdin_blocked`。
        reason: String,
    },
    /// 交互对象进入/切换/退出。
    InteractiveChanged {
        /// 是否处于交互态。
        active: bool,
        /// 交互进程 PID；退出交互时可能缺省。
        #[serde(skip_serializing_if = "Option::is_none")]
        pid: Option<u32>,
        /// 交互进程组 ID；退出交互时可能缺省。
        #[serde(skip_serializing_if = "Option::is_none")]
        pgid: Option<u32>,
        /// 交互进程命令；退出交互时可能缺省。
        #[serde(skip_serializing_if = "Option::is_none")]
        command: Option<String>,
        /// 置信度。
        confidence: Confidence,
    },
    /// 某客户端附着。
    Attached {
        /// 客户端 ID。
        client_id: String,
    },
    /// 某客户端断开（含 idle_timeout）。
    Detached {
        /// 客户端 ID。
        client_id: String,
        /// 断开原因。
        reason: String,
    },
    /// 输入 lease 被抢占/释放。
    LeaseRevoked {
        /// 客户端 ID。
        client_id: String,
    },
    /// 自然退出（终态）。
    Exited {
        /// 进程退出码。
        exit_code: i32,
    },
    /// 信号/控制中断（终态）。
    Interrupted {
        /// 信号名，如 `SIGINT`。
        signal: String,
        /// 来源：`control-c` / `signal` / `timeout` / `terminate`。
        source: String,
    },
    /// 失败（终态，含 `service_lost`）。
    Error {
        /// 错误码。
        code: ErrorCode,
        /// 错误说明。
        message: String,
        /// 是否可重试。
        retryable: bool,
    },
    /// 终端绑定键已迁移到新的 `(root_session_id, name)`。
    ///
    /// `terminal_id`、进程与输入 lease 均保持不变；`seq` 继续在同一终端内递增。
    Rebound {
        /// 迁移前的 opencode 会话 ID。
        old_root_session_id: String,
        /// 迁移后的 opencode 会话 ID。
        new_root_session_id: String,
        /// 迁移前的会话内名称。
        old_name: String,
        /// 迁移后的会话内名称。
        new_name: String,
    },
    /// SSH/helper 能力与降级标识。
    Capabilities {
        /// helper 是否可用。
        helper: bool,
        /// helper/协议版本。
        version: String,
        /// 是否降级为 best-effort。
        #[serde(rename = "bestEffort")]
        best_effort: bool,
    },
}

/// 进程树节点。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcNode {
    /// 进程 ID。
    pub pid: u32,
    /// 父进程 ID。
    pub ppid: u32,
    /// 进程组 ID。
    pub pgid: u32,
    /// 命令行（已截断）。
    pub cmd: String,
}

/// 终端状态快照。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalSnapshot {
    /// 终端 ID。
    pub terminal_id: String,
    /// 生命周期状态。
    pub state: LifecycleState,
    /// 创建时指定的 runtime ID；旧 cmd/args 终端缺省。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_id: Option<String>,
    /// 创建模式。
    pub mode: Mode,
    /// 启动程序（`terminal.create` 的原始 `cmd`）。
    pub command: String,
    /// 启动参数（`terminal.create` 的原始 `args`）。
    #[serde(default)]
    pub args: Vec<String>,
    /// 启动工作目录（`terminal.create` 的原始 `cwd`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// 主进程 ID（未创建时缺省）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// 进程组 ID（未创建时缺省）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pgid: Option<u32>,
    /// 父进程 ID（未创建时缺省）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ppid: Option<u32>,
    /// 前台进程组 ID（无 TTY 时缺省）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreground_pgrp: Option<u32>,
    /// 进程树。
    pub tree: Vec<ProcNode>,
    /// 交互状态。
    pub interactive: InteractionState,
    /// 交互推断置信度。
    pub confidence: Confidence,
    /// 列数。
    pub cols: u16,
    /// 行数。
    pub rows: u16,
    /// 当前事件缓冲基线游标。
    pub cursor: u64,
    /// 是否具备结构化 shell 事件（prompt/命令生命周期）能力。
    ///
    /// 仅当终端真正建立了带外 shell 事件通道（本地 `termx-pwsh` 或远端
    /// helper）时为 `true`；普通程序、普通 shell 与降级终端均为 `false`。
    /// 旧服务端缺省为 `false`，客户端应隐藏信息页。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub structured_events: bool,
    /// 后端能力；SSH fallback 时 `helper=false`、`bestEffort=true`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Capabilities>,
}

/// `terminal.read` 返回的一段输出。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadChunk {
    /// 事件序号。
    pub seq: u64,
    /// RFC3339 UTC 时间戳。
    pub ts: String,
    /// base64 编码的输出字节。
    pub data: String,
}

/// `idle_timeout` 的时间单位。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdleTimeoutUnit {
    /// 秒。
    S,
    /// 毫秒。
    Ms,
}

/// `idle_timeout` 到期动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdleTimeoutAction {
    /// 仅断开客户端，终端继续运行。
    Detach,
    /// 终止终端。
    Terminate,
}

/// 空闲超时配置；省略或 `value = 0` 表示永不超时。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdleTimeout {
    /// 时长数值。
    pub value: u64,
    /// 时间单位。
    pub unit: IdleTimeoutUnit,
    /// 到期动作；`value > 0` 时必填，`value = 0` 时可缺省。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<IdleTimeoutAction>,
}

impl IdleTimeout {
    /// 校验 `value > 0` 时 `action` 必须存在。
    ///
    /// 返回 `Err` 时给出可读原因，供 `bad_request` 使用。
    pub fn validate(&self) -> Result<(), String> {
        if self.value > 0 && self.action.is_none() {
            return Err("idle_timeout.value > 0 时 action 必填".to_string());
        }
        Ok(())
    }
}

/// `terminal.create` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateParams {
    /// 创建模式。
    pub mode: Mode,
    /// 可执行程序。
    ///
    /// 使用 `runtime_id` 时可缺省；服务端会忽略该值并用 runtime manifest 的
    /// `bin` 覆盖。
    #[serde(default)]
    pub cmd: String,
    /// 程序参数。
    #[serde(default)]
    pub args: Vec<String>,
    /// 工作目录。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// 环境变量增量（JSON 对象 map）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
    /// 列数（pty 可默认）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cols: Option<u16>,
    /// 行数（pty 可默认）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<u16>,
    /// 所属 opencode 会话 ID。
    pub root_session_id: String,
    /// 会话内名称，禁止 `/`、`\` 与空串。
    pub name: String,
    /// 同名已存在时是否终止旧终端后重建。
    #[serde(default)]
    pub force: bool,
    /// 空闲超时；缺省表示永不超时。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_timeout: Option<IdleTimeout>,
    /// SSH 连接参数；存在时走远端 SSH 后端（fallback 模式）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssh: Option<SshConfig>,
    /// 内置 runtime ID；存在时服务端从 runtime catalog 解析程序与参数。
    ///
    /// 与自定义 `cmd`/`args` 互斥（空 `cmd` 与空 `args` 不算冲突）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_id: Option<String>,
    /// 本次创建是否已获用户授权，允许向远端写入 helper/runtime。
    ///
    /// 缺省 `false`；仅在配置为 `ask` 时需要显式置 `true`。
    #[serde(default)]
    pub allow_injection: bool,
}

/// `runtime.list` 的单个条目；不包含本机绝对路径。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInfo {
    /// 稳定 runtime ID。
    pub id: String,
    /// 展示名。
    pub name: String,
    /// 版本。
    pub version: String,
    /// 平台。
    pub platform: String,
    /// 架构。
    pub arch: String,
    /// bin 是否存在且可执行。
    pub available: bool,
}

/// `runtime.list` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeListResult {
    /// 当前平台兼容的 runtime。
    pub runtimes: Vec<RuntimeInfo>,
}

/// `terminal.create` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateResult {
    /// 新建终端的 ID。
    pub terminal_id: String,
    /// 创建时的事件序列位置，供 `attach` 回放。
    pub cursor: u64,
}

/// `terminal.attach` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
    /// 客户端已处理的最后 seq；缺省表示只要快照。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<u64>,
}

/// `terminal.detach` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DetachParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
}

/// `terminal.status` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StatusParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
}

/// `subscribe` 的单个终端订阅项。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeTarget {
    /// 终端 ID。
    pub terminal_id: String,
    /// 起始游标；缺省表示从当前基线开始。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<u64>,
}

/// `subscribe` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SubscribeParams {
    /// 指定终端订阅列表。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminals: Option<Vec<SubscribeTarget>>,
    /// 是否订阅全部终端。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all: Option<bool>,
}

/// 单个终端的订阅游标。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionCursor {
    /// 终端 ID。
    pub terminal_id: String,
    /// 当前基线游标。
    pub cursor: u64,
    /// 需要重置本地视图的原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset: Option<ResetReason>,
}

/// `subscribe` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeResult {
    /// 每个订阅终端的游标。
    pub cursors: Vec<SubscriptionCursor>,
}

/// 终端定位：`terminal_id` 或 `(root_session_id, name)` 二选一。
///
/// 两者都给时以 `terminal_id` 为准（见 `docs/specs/PROTOCOL.md` §6）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TerminalLocator {
    /// 终端内部 ID；存在时优先。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<String>,
    /// 所属 opencode 会话 ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_session_id: Option<String>,
    /// 会话内名称。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl TerminalLocator {
    /// 三个定位字段是否都缺省。
    pub fn is_empty(&self) -> bool {
        self.terminal_id.is_none() && self.root_session_id.is_none() && self.name.is_none()
    }
}

/// `hello` 参数。
///
/// 连接级访问身份由 `root_session_id` 或 `admin` 二选一确定：
/// - 普通客户端必须携带 `root_session_id`，之后只能访问该会话的终端；
/// - `admin: true` 用于 Web 代理等管理连接，可访问/迁移全部终端。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloParams {
    /// 主协议版本。
    pub proto: String,
    /// 鉴权 token。
    pub token: String,
    /// 客户端标识。
    pub client: String,
    /// 调用者 opencode 会话 ID；非 admin 连接必填。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_session_id: Option<String>,
    /// 是否为管理员连接（Web 代理使用）。
    #[serde(default)]
    pub admin: bool,
}

/// helper / SSH 能力描述。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// helper 是否可用。
    pub helper: bool,
    /// helper/协议版本。
    pub version: String,
    /// 是否降级为 best-effort。
    #[serde(rename = "bestEffort")]
    pub best_effort: bool,
    /// 服务端是否支持 `runtime.list` / `terminal.create.runtime_id`。
    ///
    /// 旧客户端反序列化时缺省为 `false`。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub runtime_catalog: bool,
}

/// `hello` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloResult {
    /// 主协议版本。
    pub proto: String,
    /// 服务端标识。
    pub server: String,
    /// 能力描述。
    pub capabilities: Capabilities,
}

/// `terminal.claim` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
    /// 是否抢占现有 lease。
    #[serde(default)]
    pub force: bool,
}

/// `terminal.claim` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimResult {
    /// 当前 lease owner（客户端 ID）。
    pub owner: String,
    /// lease 标识；v1 直接使用 owner 的连接级 `client_id`。
    pub lease: String,
}

/// `terminal.release` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReleaseParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
}

/// 通用成功结果；用于 `detach` / `release` / `resize` / `signal` / `terminate` / `unsubscribe`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimpleResult {
    /// 操作是否成功。
    pub ok: bool,
}

/// `terminal.list` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListParams {
    /// 过滤的生命周期状态。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub states: Option<Vec<LifecycleState>>,
    /// 返回条数上限。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// `terminal.list` 的单条记录。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListItem {
    /// 终端 ID。
    pub terminal_id: String,
    /// 所属 opencode 会话 ID。
    pub root_session_id: String,
    /// 创建时指定的 runtime ID；旧 cmd/args 终端缺省。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_id: Option<String>,
    /// 会话内名称。
    pub name: String,
    /// 生命周期状态。
    pub state: LifecycleState,
    /// 是否具备结构化 shell 事件能力；普通终端为 `false`。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub structured_events: bool,
    /// 创建模式。
    pub mode: Mode,
    /// 启动程序（`terminal.create` 的原始 `cmd`）。
    pub command: String,
    /// 启动参数（`terminal.create` 的原始 `args`）。
    #[serde(default)]
    pub args: Vec<String>,
    /// 启动工作目录（`terminal.create` 的原始 `cwd`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// 主进程 ID（未创建时缺省）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// 空闲超时配置。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_timeout: Option<IdleTimeout>,
    /// 最后一次活动时间（RFC3339 UTC）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<String>,
    /// 创建时间（RFC3339 UTC）。
    pub created_at: String,
}

/// `session.list` 参数；当前不需要任何字段。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionListParams {}

/// 已知 opencode 会话的摘要（不含 token 等敏感信息）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInfo {
    /// opencode 会话 ID。
    pub root_session_id: String,
    /// 是否在会话 TTL 内收到过心跳。
    pub live: bool,
    /// 距最近一次心跳的毫秒数；仅存活会话提供。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_ms: Option<u64>,
    /// 该会话当前关联的终端数。
    pub terminals: u32,
}

/// `session.list` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionListResult {
    /// 按 `root_session_id` 升序排列的会话摘要。
    pub sessions: Vec<SessionInfo>,
}

/// `terminal.attach` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachResult {
    /// 当前基线游标。
    pub cursor: u64,
    /// 终端快照。
    pub snapshot: TerminalSnapshot,
    /// 需要重置本地视图的原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset: Option<ResetReason>,
}

/// `terminal.status` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusResult {
    /// 终端状态快照。
    pub snapshot: TerminalSnapshot,
}

/// `terminal.read` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
    /// 起始游标（必填，对齐 PROTOCOL §6）。
    pub cursor: u64,
    /// 单次返回的字节上限。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

/// `terminal.read` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadResult {
    /// 输出片段，按 `seq` 升序。
    pub chunks: Vec<ReadChunk>,
    /// 读取后的游标。
    pub cursor: u64,
    /// 需要重置本地视图的原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset: Option<ResetReason>,
}

/// `terminal.output` 参数：对当前保留的原始输出做尾部截取或倒搜。
///
/// 三组条件互斥：
/// - `tail_lines` / `tail_chars`：返回最后 N 行 / N 个 Unicode 字符；
/// - `pattern`（配合 `nth` / `only_matches`）：从末尾倒搜。
///
/// 不提供绝对行号或 `from_line` 语义；历史被背压裁剪后旧位置不保证可用。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
    /// 最后 N 行。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tail_lines: Option<u32>,
    /// 最后最多 N 个 Unicode 字符（按 UTF-8 字符边界）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tail_chars: Option<u64>,
    /// 倒搜的字面量 pattern。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    /// 从末尾数第 N 个匹配（1 起）；缺省 1。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nth: Option<u32>,
    /// 只返回匹配所在行。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub only_matches: bool,
}

/// `terminal.output` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputResult {
    /// 截取/搜索结果原文；无匹配时为空串。
    pub text: String,
}

/// 游标不可回放时的重置原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetReason {
    /// 缓冲已因背压回绕。
    BufferOverflow,
    /// 游标早于缓冲下界。
    TooOld,
    /// 使用当前基线重新开始。
    Baseline,
}

/// `terminal.send` 参数；`data` 与 `text` 二选一（同传取 `data`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
    /// base64 编码的原始字节。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    /// UTF-8 文本。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// 是否追加换行。
    #[serde(default)]
    pub enter: bool,
}

/// `terminal.resize` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResizeParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
    /// 列数。
    pub cols: u16,
    /// 行数。
    pub rows: u16,
}

/// `terminal.signal` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
    /// 信号名，如 `SIGINT`。
    pub signal: String,
}

/// `terminal.terminate` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminateParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
    /// 是否强制终止（跳过优雅退出）。
    #[serde(default)]
    pub force: bool,
}

/// `terminal.rebind` 参数。
///
/// 定位器只接受 `terminal_id`：`root_session_id` 与 `name` 在本方法中表示**目标**
/// 绑定键，无法再用作旧键定位（旧键可由 `terminal_id` 唯一确定）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebindParams {
    /// 终端内部 ID（必填）。
    pub terminal_id: String,
    /// 目标 opencode 会话 ID。
    pub root_session_id: String,
    /// 目标会话内名称；缺省表示保留原名。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// 目标键已存在时是否终止目标旧终端后迁移。
    #[serde(default)]
    pub force: bool,
}

/// `terminal.rebind` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebindResult {
    /// 终端 ID（迁移前后不变）。
    pub terminal_id: String,
    /// 迁移后的 opencode 会话 ID。
    pub root_session_id: String,
    /// 迁移后的会话内名称。
    pub name: String,
}

/// `{ value, unit }` 形式的时间跨度，用于 `prune.older_than`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeSpan {
    /// 时长数值。
    pub value: u64,
    /// 时间单位。
    pub unit: IdleTimeoutUnit,
}

/// `terminal.prune` 的清理基准。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PruneBasis {
    /// 按最后活动时间。
    LastActivity,
    /// 按创建时间。
    CreatedAt,
}

/// `terminal.prune` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PruneParams {
    /// 过滤的生命周期状态。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub states: Option<Vec<LifecycleState>>,
    /// 早于该时长的会话才清理。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub older_than: Option<TimeSpan>,
    /// 清理基准。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basis: Option<PruneBasis>,
    /// 是否清理全部终态终端；缺省 `false`，且为空过滤时必须显式置 `true`。
    #[serde(default)]
    pub all: bool,
}

/// `terminal.prune` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneResult {
    /// 被清理的终端 ID 列表。
    pub removed: Vec<String>,
}

/// `unsubscribe` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UnsubscribeParams {
    /// 要取消订阅的终端；缺省表示全部。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminals: Option<Vec<TerminalLocator>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;

    /// 构造带统一信封的示例事件。
    fn sample_event(kind: EventKind) -> Event {
        Event {
            terminal_id: "01JABCDEF".to_string(),
            seq: 7,
            ts: "2026-11-02T09:00:00.000Z".to_string(),
            kind,
        }
    }

    #[test]
    fn event_round_trips_all_kinds() {
        let kinds = vec![
            EventKind::Started {
                pid: 100,
                pgid: 100,
                mode: Mode::Pty,
            },
            EventKind::Output {
                data: "aGVsbG8=".to_string(),
                stream: Stream::Stdout,
            },
            EventKind::OutputDropped {
                from_seq: 3,
                to_seq: 5,
            },
            EventKind::WaitingInput {
                confidence: Confidence::Medium,
                reason: "prompt".to_string(),
            },
            EventKind::InteractiveChanged {
                active: false,
                pid: None,
                pgid: None,
                command: None,
                confidence: Confidence::Low,
            },
            EventKind::Attached {
                client_id: "c-1".to_string(),
            },
            EventKind::Detached {
                client_id: "c-1".to_string(),
                reason: "idle_timeout".to_string(),
            },
            EventKind::LeaseRevoked {
                client_id: "c-1".to_string(),
            },
            EventKind::Exited { exit_code: 7 },
            EventKind::Interrupted {
                signal: "SIGTERM".to_string(),
                source: "terminate".to_string(),
            },
            EventKind::Error {
                code: ErrorCode::ServiceLost,
                message: "daemon 重启".to_string(),
                retryable: true,
            },
            EventKind::Rebound {
                old_root_session_id: "ses-1".to_string(),
                new_root_session_id: "ses-2".to_string(),
                old_name: "build".to_string(),
                new_name: "verify".to_string(),
            },
            EventKind::Capabilities {
                helper: true,
                version: HELPER_PROTOCOL_VERSION.to_string(),
                best_effort: false,
            },
        ];
        assert_eq!(kinds.len(), 13, "EventKind 变体数量应为 13");

        for kind in kinds {
            let event = sample_event(kind);
            let json = to_json(&event).expect("事件序列化失败");
            let back: Event = from_json(&json).expect("事件反序列化失败");
            assert_eq!(event, back);
        }
    }

    #[test]
    fn event_json_shares_envelope_and_uses_type_tag() {
        let event = sample_event(EventKind::InteractiveChanged {
            active: true,
            pid: Some(4321),
            pgid: Some(4321),
            command: Some("python".to_string()),
            confidence: Confidence::High,
        });
        let json: JsonValue =
            serde_json::from_str(&to_json(&event).expect("序列化失败")).expect("JSON 解析失败");

        assert_eq!(json["type"], "interactive_changed");
        assert_eq!(json["terminal_id"], "01JABCDEF");
        assert_eq!(json["seq"], 7);
        assert_eq!(json["ts"], "2026-11-02T09:00:00.000Z");
        assert_eq!(json["active"], true);
        assert_eq!(json["confidence"], "high");
    }

    #[test]
    fn error_codes_serialize_as_snake_case() {
        let cases = [
            (ErrorCode::AuthFailed, "auth_failed"),
            (ErrorCode::VersionMismatch, "version_mismatch"),
            (ErrorCode::BadRequest, "bad_request"),
            (ErrorCode::NotFound, "not_found"),
            (ErrorCode::AlreadyTerminal, "already_terminal"),
            (ErrorCode::SessionExists, "session_exists"),
            (ErrorCode::PermissionDenied, "permission_denied"),
            (ErrorCode::LimitExceeded, "limit_exceeded"),
            (ErrorCode::SpawnFailed, "spawn_failed"),
            (ErrorCode::PtyFailed, "pty_failed"),
            (ErrorCode::SshConnectFailed, "ssh_connect_failed"),
            (ErrorCode::HelperFailed, "helper_failed"),
            (ErrorCode::ProtocolError, "protocol_error"),
            (ErrorCode::ConsentRequired, "consent_required"),
            (ErrorCode::HostKeyUnknown, "host_key_unknown"),
            (ErrorCode::HostKeyChanged, "host_key_changed"),
            (ErrorCode::ServiceLost, "service_lost"),
            (ErrorCode::Internal, "internal"),
        ];
        assert_eq!(cases.len(), 18, "ErrorCode 变体数量应为 18");

        for (code, expected) in cases {
            let json = serde_json::to_value(code).expect("错误码序列化失败");
            assert_eq!(json, expected);
        }
    }

    #[test]
    fn lifecycle_terminal_states_are_recognized() {
        let terminal = [
            LifecycleState::Exited,
            LifecycleState::Interrupted,
            LifecycleState::Error,
        ];
        let non_terminal = [
            LifecycleState::Created,
            LifecycleState::Running,
            LifecycleState::Exiting,
        ];

        for state in terminal {
            assert!(state.is_terminal(), "{state:?} 应为终态");
        }
        for state in non_terminal {
            assert!(!state.is_terminal(), "{state:?} 不应为终态");
        }
    }

    #[test]
    fn response_shapes_distinguish_ok_and_error() {
        let ok = Response {
            id: "c-001".to_string(),
            ok: true,
            result: Some(serde_json::json!({ "terminal_id": "01JABCDEF" })),
            error: None,
        };
        let ok_json: JsonValue =
            serde_json::from_str(&to_json(&ok).expect("序列化失败")).expect("JSON 解析失败");
        assert_eq!(ok_json["ok"], true);
        assert_eq!(ok_json["result"]["terminal_id"], "01JABCDEF");
        assert!(ok_json.get("error").is_none());

        let err = Response {
            id: "c-002".to_string(),
            ok: false,
            result: None,
            error: Some(ProtocolError {
                code: ErrorCode::NotFound,
                message: "终端不存在".to_string(),
                retryable: false,
                terminal_id: None,
                injection: None,
                host_key: None,
            }),
        };
        let err_json: JsonValue =
            serde_json::from_str(&to_json(&err).expect("序列化失败")).expect("JSON 解析失败");
        assert_eq!(err_json["ok"], false);
        assert!(err_json.get("result").is_none());
        assert_eq!(err_json["error"]["code"], "not_found");
        assert_eq!(err_json["error"]["retryable"], false);
    }

    #[test]
    fn capabilities_uses_best_effort_camel_case() {
        let event = sample_event(EventKind::Capabilities {
            helper: false,
            version: "termx-helper/1".to_string(),
            best_effort: true,
        });
        let json: JsonValue =
            serde_json::from_str(&to_json(&event).expect("序列化失败")).expect("JSON 解析失败");

        assert_eq!(json["type"], "capabilities");
        assert_eq!(json["bestEffort"], true);
        assert_eq!(json["helper"], false);
    }

    #[test]
    fn retryable_codes_match_protocol_table() {
        let retryable = [
            ErrorCode::LimitExceeded,
            ErrorCode::SshConnectFailed,
            ErrorCode::HelperFailed,
            ErrorCode::ServiceLost,
            ErrorCode::Internal,
        ];
        let non_retryable = [
            ErrorCode::AuthFailed,
            ErrorCode::VersionMismatch,
            ErrorCode::BadRequest,
            ErrorCode::NotFound,
            ErrorCode::AlreadyTerminal,
            ErrorCode::SessionExists,
            ErrorCode::PermissionDenied,
            ErrorCode::SpawnFailed,
            ErrorCode::PtyFailed,
            ErrorCode::ProtocolError,
            ErrorCode::ConsentRequired,
            ErrorCode::HostKeyUnknown,
            ErrorCode::HostKeyChanged,
        ];

        for code in retryable {
            assert!(code.is_retryable(), "{code:?} 应可重试");
        }
        for code in non_retryable {
            assert!(!code.is_retryable(), "{code:?} 不应可重试");
        }
    }

    /// 构造覆盖全部可选字段的创建参数。
    fn sample_create_params(idle_timeout: Option<IdleTimeout>) -> CreateParams {
        let mut env = BTreeMap::new();
        env.insert("TERM".to_string(), "xterm-256color".to_string());
        env.insert("LANG".to_string(), "C.UTF-8".to_string());

        CreateParams {
            mode: Mode::Pty,
            cmd: "pwsh".to_string(),
            args: vec!["-NoLogo".to_string()],
            cwd: Some("D:\\work".to_string()),
            env: Some(env),
            cols: Some(120),
            rows: Some(30),
            root_session_id: "ses-1".to_string(),
            name: "build".to_string(),
            force: true,
            idle_timeout,
            ssh: None,
            allow_injection: false,
            runtime_id: None,
        }
    }

    #[test]
    fn create_params_round_trip() {
        let never = sample_create_params(Some(IdleTimeout {
            value: 0,
            unit: IdleTimeoutUnit::S,
            action: None,
        }));
        let terminate = sample_create_params(Some(IdleTimeout {
            value: 500,
            unit: IdleTimeoutUnit::Ms,
            action: Some(IdleTimeoutAction::Terminate),
        }));

        // env 必须序列化为 JSON 对象；value=0 时 action 不出现。
        let never_json: JsonValue =
            serde_json::from_str(&to_json(&never).expect("序列化失败")).expect("JSON 解析失败");
        assert_eq!(never_json["env"]["TERM"], "xterm-256color");
        assert_eq!(never_json["force"], true);
        assert!(never_json["idle_timeout"].get("action").is_none());

        for params in [never, terminate] {
            let json = to_json(&params).expect("序列化失败");
            let back: CreateParams = from_json(&json).expect("反序列化失败");
            assert_eq!(params, back);
        }
    }

    #[test]
    fn runtime_id_is_optional_and_old_create_params_still_parse() {
        let old = serde_json::json!({
            "mode": "pty",
            "cmd": "pwsh",
            "root_session_id": "ses-1",
            "name": "old"
        });
        let params: CreateParams = serde_json::from_value(old).expect("旧参数应兼容");
        assert_eq!(params.runtime_id, None);
        assert_eq!(params.cmd, "pwsh");

        let runtime_only = serde_json::json!({
            "mode": "pty",
            "root_session_id": "ses-1",
            "name": "rt",
            "runtime_id": "termx-pwsh"
        });
        let params: CreateParams =
            serde_json::from_value(runtime_only).expect("runtime 参数应解析");
        assert_eq!(params.runtime_id.as_deref(), Some("termx-pwsh"));
        assert!(params.cmd.is_empty(), "cmd 可缺省为空");
        let json: JsonValue =
            serde_json::from_str(&to_json(&params).expect("序列化失败")).expect("JSON 解析失败");
        assert_eq!(json["runtime_id"], "termx-pwsh");

        let info = RuntimeInfo {
            id: "termx-pwsh".to_string(),
            name: "termx-pwsh".to_string(),
            version: "0.1.0".to_string(),
            platform: "windows".to_string(),
            arch: "x86_64".to_string(),
            available: true,
        };
        let list = RuntimeListResult {
            runtimes: vec![info],
        };
        let back: RuntimeListResult =
            from_json(&to_json(&list).expect("序列化失败")).expect("反序列化失败");
        assert_eq!(list, back);
        assert!(!to_json(&list).expect("序列化失败").contains("bin"));
    }

    #[test]
    fn ssh_config_round_trips_and_debug_redacts_secrets() {
        let config = SshConfig {
            host: "127.0.0.1".to_string(),
            port: 2222,
            user: "afubek".to_string(),
            auth: SshAuth::Password {
                password: "s3cret-pass".to_string(),
            },
            helper_path: Some("/usr/local/bin/termx-helper".to_string()),
            helper_local_path: Some("./target/termx-helper".to_string()),
            helper_sha256: Some("0123456789abcdef".to_string()),
            upload_helper: true,
            accept_any_host_key: true,
            host_key_policy: None,
            trusted_host_key: None,
            known_hosts_path: None,
            injection: InjectionPlan::default(),
        };

        let json = to_json(&config).expect("序列化失败");
        assert!(!json.contains("injection"), "注入计划不得进入 JSON: {json}");
        let back: SshConfig = from_json(&json).expect("反序列化失败");
        assert_eq!(config, back, "serde 需保留原值供本机传输");

        let debug = format!("{config:?}");
        assert!(
            !debug.contains("s3cret-pass"),
            "Debug 不得泄露密码: {debug}"
        );
        assert!(debug.contains("<redacted>"), "Debug 应标注遮蔽: {debug}");

        let key = SshAuth::Key {
            path: "/home/afubek/.ssh/id_ed25519".to_string(),
            passphrase: Some("key-pass".to_string()),
        };
        let key_debug = format!("{key:?}");
        assert!(
            !key_debug.contains("key-pass"),
            "Debug 不得泄露口令: {key_debug}"
        );
    }

    #[test]
    fn idle_timeout_validate_requires_action_only_when_positive() {
        let zero_without_action = IdleTimeout {
            value: 0,
            unit: IdleTimeoutUnit::S,
            action: None,
        };
        assert!(zero_without_action.validate().is_ok());

        let zero_with_action = IdleTimeout {
            value: 0,
            unit: IdleTimeoutUnit::Ms,
            action: Some(IdleTimeoutAction::Terminate),
        };
        assert!(zero_with_action.validate().is_ok());

        let positive_with_action = IdleTimeout {
            value: 1,
            unit: IdleTimeoutUnit::S,
            action: Some(IdleTimeoutAction::Detach),
        };
        assert!(positive_with_action.validate().is_ok());

        let positive_without_action = IdleTimeout {
            value: 1,
            unit: IdleTimeoutUnit::S,
            action: None,
        };
        assert!(positive_without_action.validate().is_err());
    }

    /// 构造覆盖进程树与可选字段的快照。
    fn sample_snapshot() -> TerminalSnapshot {
        TerminalSnapshot {
            terminal_id: "01JABCDEF".to_string(),
            state: LifecycleState::Running,
            mode: Mode::Pty,
            command: "pwsh".to_string(),
            args: vec!["-NoLogo".to_string()],
            cwd: Some("C:\\work".to_string()),
            pid: Some(100),
            pgid: Some(100),
            ppid: Some(1),
            foreground_pgrp: Some(100),
            tree: vec![ProcNode {
                pid: 100,
                ppid: 1,
                pgid: 100,
                cmd: "pwsh -NoLogo".to_string(),
            }],
            interactive: InteractionState::Interactive,
            confidence: Confidence::High,
            cols: 120,
            rows: 30,
            cursor: 42,
            structured_events: false,
            capabilities: None,
            runtime_id: None,
        }
    }

    #[test]
    fn snapshot_and_rpc_types_round_trip() {
        let snapshot = sample_snapshot();
        let snapshot_back: TerminalSnapshot =
            from_json(&to_json(&snapshot).expect("序列化失败")).expect("反序列化失败");
        assert_eq!(snapshot, snapshot_back);

        let chunk = ReadChunk {
            seq: 42,
            ts: "2026-11-02T09:00:00.000Z".to_string(),
            data: "aGVsbG8=".to_string(),
        };
        let chunk_back: ReadChunk =
            from_json(&to_json(&chunk).expect("序列化失败")).expect("反序列化失败");
        assert_eq!(chunk, chunk_back);

        let attach = AttachParams {
            locator: TerminalLocator {
                terminal_id: Some("01JABCDEF".to_string()),
                root_session_id: None,
                name: None,
            },
            cursor: Some(41),
        };
        let attach_back: AttachParams =
            from_json(&to_json(&attach).expect("序列化失败")).expect("反序列化失败");
        assert_eq!(attach, attach_back);

        let hello = HelloParams {
            proto: PROTOCOL_VERSION.to_string(),
            token: "secret".to_string(),
            client: "termx-cli".to_string(),
            root_session_id: Some("ses-1".to_string()),
            admin: false,
        };
        let hello_json: JsonValue =
            serde_json::from_str(&to_json(&hello).expect("序列化失败")).expect("JSON 解析失败");
        assert_eq!(hello_json["proto"], PROTOCOL_VERSION);
        assert_eq!(hello_json["root_session_id"], "ses-1");
        assert_eq!(hello_json["admin"], false);
        let hello_back: HelloParams =
            from_json(&to_json(&hello).expect("序列化失败")).expect("反序列化失败");
        assert_eq!(hello, hello_back);
    }

    #[test]
    fn none_options_are_omitted_from_json() {
        let params = CreateParams {
            mode: Mode::Stdio,
            cmd: "cat".to_string(),
            args: Vec::new(),
            cwd: None,
            env: None,
            cols: None,
            rows: None,
            root_session_id: "ses-1".to_string(),
            name: "io".to_string(),
            force: false,
            idle_timeout: None,
            ssh: None,
            allow_injection: false,
            runtime_id: None,
        };
        let params_json: JsonValue =
            serde_json::from_str(&to_json(&params).expect("序列化失败")).expect("JSON 解析失败");
        for key in [
            "cwd",
            "env",
            "cols",
            "rows",
            "idle_timeout",
            "ssh",
            "runtime_id",
        ] {
            assert!(params_json.get(key).is_none(), "{key} 不应出现在 JSON 中");
        }

        let attach = AttachParams {
            locator: TerminalLocator {
                terminal_id: Some("01JABCDEF".to_string()),
                root_session_id: None,
                name: None,
            },
            cursor: None,
        };
        let attach_json: JsonValue =
            serde_json::from_str(&to_json(&attach).expect("序列化失败")).expect("JSON 解析失败");
        assert!(attach_json.get("cursor").is_none());

        let error = ProtocolError {
            code: ErrorCode::SessionExists,
            message: "同名会话已存在".to_string(),
            retryable: false,
            terminal_id: None,
            injection: None,
            host_key: None,
        };
        let error_json: JsonValue =
            serde_json::from_str(&to_json(&error).expect("序列化失败")).expect("JSON 解析失败");
        assert!(error_json.get("terminal_id").is_none());
        assert!(error_json.get("injection").is_none());

        let mut snapshot = sample_snapshot();
        snapshot.pid = None;
        let snapshot_json: JsonValue =
            serde_json::from_str(&to_json(&snapshot).expect("序列化失败")).expect("JSON 解析失败");
        assert!(snapshot_json.get("pid").is_none());
    }

    #[test]
    fn session_exists_error_carries_terminal_id() {
        let error = ProtocolError {
            code: ErrorCode::SessionExists,
            message: "同名会话已存在".to_string(),
            retryable: false,
            terminal_id: Some("01JOLD".to_string()),
            injection: None,
            host_key: None,
        };
        let json: JsonValue =
            serde_json::from_str(&to_json(&error).expect("序列化失败")).expect("JSON 解析失败");

        assert_eq!(json["code"], "session_exists");
        assert_eq!(json["terminal_id"], "01JOLD");
    }

    #[test]
    fn consent_required_error_carries_injection_payload() {
        let error = ProtocolError {
            code: ErrorCode::ConsentRequired,
            message: "需要授权写入远端 helper".to_string(),
            retryable: false,
            terminal_id: None,
            injection: Some(Box::new(InjectionConsent {
                host: "example.com".to_string(),
                user: "alice".to_string(),
                writes: vec![InjectionWrite {
                    kind: InjectionWriteKind::Helper,
                    action: InjectionWriteAction::Install,
                    reason: InjectionWriteReason::Missing,
                    path: "/home/alice/.termx/bin/termx-helper".to_string(),
                    size: Some(1024),
                    sha256: Some("f".repeat(64)),
                }],
            })),
            host_key: None,
        };
        let json: JsonValue =
            serde_json::from_str(&to_json(&error).expect("序列化失败")).expect("JSON 解析失败");
        assert_eq!(json["code"], "consent_required");
        assert_eq!(json["retryable"], false);
        assert_eq!(json["injection"]["host"], "example.com");
        assert_eq!(json["injection"]["writes"][0]["kind"], "helper");
        assert_eq!(json["injection"]["writes"][0]["action"], "install");
        assert_eq!(json["injection"]["writes"][0]["reason"], "missing");

        let legacy: ProtocolError =
            from_json(r#"{"code":"consent_required","message":"x","retryable":false}"#)
                .expect("旧形状应可解析");
        assert!(legacy.injection.is_none());
    }

    #[test]
    fn allow_injection_defaults_false_and_round_trips_true() {
        let legacy: CreateParams =
            from_json(r#"{"mode":"pty","root_session_id":"ses-1","name":"x"}"#)
                .expect("旧参数应兼容");
        assert!(!legacy.allow_injection);

        let json = to_json(&legacy).expect("序列化失败");
        let value: JsonValue = serde_json::from_str(&json).expect("JSON 解析失败");
        assert_eq!(value["allow_injection"], false);

        let mut allowed = legacy;
        allowed.allow_injection = true;
        let back: CreateParams =
            from_json(&to_json(&allowed).expect("序列化失败")).expect("反序列化失败");
        assert!(back.allow_injection);
        assert_eq!(allowed, back);
    }

    #[test]
    fn terminal_locator_detects_empty_and_omits_none() {
        assert!(TerminalLocator::default().is_empty());
        assert!(
            !TerminalLocator {
                terminal_id: Some("01JABCDEF".to_string()),
                ..TerminalLocator::default()
            }
            .is_empty()
        );
        assert!(
            !TerminalLocator {
                root_session_id: Some("ses-1".to_string()),
                name: Some("build".to_string()),
                terminal_id: None,
            }
            .is_empty()
        );

        let locator = TerminalLocator {
            terminal_id: Some("01JABCDEF".to_string()),
            root_session_id: None,
            name: None,
        };
        let json: JsonValue =
            serde_json::from_str(&to_json(&locator).expect("序列化失败")).expect("JSON 解析失败");
        assert!(json.get("root_session_id").is_none());
        assert!(json.get("name").is_none());
    }

    #[test]
    fn rebind_params_round_trip_and_omit_optional_name() {
        let params: RebindParams =
            from_json(r#"{"terminal_id":"01JABCDEF","root_session_id":"ses-2","force":true}"#)
                .expect("rebind 反序列化失败");
        assert_eq!(params.terminal_id, "01JABCDEF");
        assert_eq!(params.root_session_id, "ses-2");
        assert!(params.name.is_none());
        assert!(params.force);

        let value: JsonValue =
            serde_json::from_str(&to_json(&params).expect("序列化失败")).expect("JSON 解析失败");
        assert!(value.get("name").is_none(), "缺省 name 不应出现在 JSON 中");
        let back: RebindParams =
            from_json(&to_json(&params).expect("序列化失败")).expect("反序列化失败");
        assert_eq!(params, back);

        let named: RebindParams =
            from_json(r#"{"terminal_id":"01JABCDEF","root_session_id":"ses-2","name":"verify"}"#)
                .expect("带 name 的 rebind 反序列化失败");
        assert_eq!(named.name.as_deref(), Some("verify"));
        assert!(!named.force);
    }

    #[test]
    fn locator_params_flatten_and_round_trip() {
        // 只给 terminal_id：定位字段与外层参数平铺在同一层。
        let attach: AttachParams =
            from_json(r#"{"terminal_id":"01JABCDEF","cursor":3}"#).expect("attach 反序列化失败");
        assert_eq!(attach.locator.terminal_id.as_deref(), Some("01JABCDEF"));
        assert!(attach.locator.root_session_id.is_none());
        assert_eq!(attach.cursor, Some(3));

        let attach_json: JsonValue =
            serde_json::from_str(&to_json(&attach).expect("序列化失败")).expect("JSON 解析失败");
        assert_eq!(attach_json["terminal_id"], "01JABCDEF");
        assert!(attach_json.get("root_session_id").is_none());
        assert!(attach_json.get("name").is_none());

        // 只给 (root_session_id, name)。
        let claim: ClaimParams =
            from_json(r#"{"root_session_id":"ses-1","name":"build"}"#).expect("claim 反序列化失败");
        assert!(claim.locator.terminal_id.is_none());
        assert_eq!(claim.locator.root_session_id.as_deref(), Some("ses-1"));
        assert_eq!(claim.locator.name.as_deref(), Some("build"));
        assert!(!claim.force);

        let claim_back: ClaimParams =
            from_json(&to_json(&claim).expect("序列化失败")).expect("反序列化失败");
        assert_eq!(claim, claim_back);

        // 三项都缺省时定位器为空，交由 core 判定 bad_request/not_found。
        let empty: StatusParams = from_json("{}").expect("status 反序列化失败");
        assert!(empty.locator.is_empty());

        let detach: DetachParams =
            from_json(r#"{"terminal_id":"01JABCDEF"}"#).expect("detach 反序列化失败");
        assert_eq!(detach.locator.terminal_id.as_deref(), Some("01JABCDEF"));

        let release: ReleaseParams = from_json(r#"{"root_session_id":"ses-1","name":"build"}"#)
            .expect("release 反序列化失败");
        assert_eq!(release.locator.root_session_id.as_deref(), Some("ses-1"));
        assert_eq!(release.locator.name.as_deref(), Some("build"));

        // 其余带定位器的方法参数同样支持平铺。
        let read: ReadParams =
            from_json(r#"{"root_session_id":"ses-1","name":"build","cursor":7,"max_bytes":4096}"#)
                .expect("read 反序列化失败");
        assert_eq!(read.locator.name.as_deref(), Some("build"));
        assert_eq!(read.cursor, 7);
        assert_eq!(read.max_bytes, Some(4096));

        let send: SendParams = from_json(r#"{"terminal_id":"01JABCDEF","text":"ls","enter":true}"#)
            .expect("send 反序列化失败");
        assert_eq!(send.locator.terminal_id.as_deref(), Some("01JABCDEF"));
        assert_eq!(send.text.as_deref(), Some("ls"));
        assert!(send.enter);

        let resize: ResizeParams =
            from_json(r#"{"name":"build","root_session_id":"ses-1","cols":120,"rows":30}"#)
                .expect("resize 反序列化失败");
        assert_eq!((resize.cols, resize.rows), (120, 30));

        let signal: SignalParams = from_json(r#"{"terminal_id":"01JABCDEF","signal":"SIGINT"}"#)
            .expect("signal 反序列化失败");
        assert_eq!(signal.signal, "SIGINT");

        let terminate: TerminateParams = from_json(r#"{"terminal_id":"01JABCDEF","force":true}"#)
            .expect("terminate 反序列化失败");
        assert!(terminate.force);
    }

    #[test]
    fn event_kind_type_tags_are_exact() {
        let cases = vec![
            (
                EventKind::Started {
                    pid: 1,
                    pgid: 1,
                    mode: Mode::Pty,
                },
                "started",
            ),
            (
                EventKind::Output {
                    data: "aGk=".to_string(),
                    stream: Stream::Pty,
                },
                "output",
            ),
            (
                EventKind::OutputDropped {
                    from_seq: 1,
                    to_seq: 2,
                },
                "output_dropped",
            ),
            (
                EventKind::WaitingInput {
                    confidence: Confidence::Low,
                    reason: "no_output".to_string(),
                },
                "waiting_input",
            ),
            (
                EventKind::InteractiveChanged {
                    active: true,
                    pid: Some(1),
                    pgid: Some(1),
                    command: Some("sh".to_string()),
                    confidence: Confidence::High,
                },
                "interactive_changed",
            ),
            (
                EventKind::Attached {
                    client_id: "c-1".to_string(),
                },
                "attached",
            ),
            (
                EventKind::Detached {
                    client_id: "c-1".to_string(),
                    reason: "idle_timeout".to_string(),
                },
                "detached",
            ),
            (
                EventKind::LeaseRevoked {
                    client_id: "c-1".to_string(),
                },
                "lease_revoked",
            ),
            (EventKind::Exited { exit_code: 0 }, "exited"),
            (
                EventKind::Interrupted {
                    signal: "SIGINT".to_string(),
                    source: "control-c".to_string(),
                },
                "interrupted",
            ),
            (
                EventKind::Error {
                    code: ErrorCode::Internal,
                    message: "x".to_string(),
                    retryable: true,
                },
                "error",
            ),
            (
                EventKind::Capabilities {
                    helper: false,
                    version: HELPER_PROTOCOL_VERSION.to_string(),
                    best_effort: true,
                },
                "capabilities",
            ),
        ];
        assert_eq!(cases.len(), 12, "EventKind 变体数量应为 12");

        for (kind, expected) in cases {
            let json: JsonValue =
                serde_json::from_str(&to_json(&sample_event(kind)).expect("序列化失败"))
                    .expect("JSON 解析失败");
            assert_eq!(json["type"].as_str(), Some(expected));
        }
    }

    #[test]
    fn missing_optional_fields_deserialize_to_none() {
        let error: ProtocolError =
            from_json(r#"{"code":"session_exists","message":"x","retryable":false}"#)
                .expect("ProtocolError 反序列化失败");
        assert_eq!(error.terminal_id, None);

        let event: Event = from_json(
            r#"{"type":"interactive_changed","terminal_id":"t","seq":1,
                "ts":"2026-11-02T09:00:00.000Z","active":false,"confidence":"low"}"#,
        )
        .expect("Event 反序列化失败");
        match event.kind {
            EventKind::InteractiveChanged {
                pid, pgid, command, ..
            } => {
                assert_eq!(pid, None);
                assert_eq!(pgid, None);
                assert_eq!(command, None);
            }
            other => panic!("期望 interactive_changed，实际 {other:?}"),
        }
    }

    #[test]
    fn prune_params_all_defaults_to_false() {
        let params: PruneParams = from_json("{}").expect("空 PruneParams 反序列化失败");
        assert!(!params.all, "缺省 all 必须为 false");

        let params: PruneParams = from_json(r#"{"all":true}"#).expect("显式 all 反序列化失败");
        assert!(params.all);

        let value: JsonValue =
            serde_json::from_str(&to_json(&PruneParams::default()).expect("序列化失败"))
                .expect("JSON 解析失败");
        assert_eq!(value["all"], JsonValue::Bool(false));
    }

    #[test]
    fn transcript_command_keeps_legacy_json_compatible() {
        // 旧 JSON 没有新字段：反序列化应使用默认值。
        let legacy: TranscriptCommand = from_json(
            r#"{"command":"ls","started_at":"2026-11-02T09:00:00Z",
                "ended_at":null,"status":"running","output":"a.txt"}"#,
        )
        .expect("旧 TranscriptCommand 反序列化失败");
        assert_eq!(legacy.exit_code, None);
        assert_eq!(legacy.source, None);
        assert!(!legacy.best_effort);

        // 默认值不应出现在序列化结果中。
        let legacy_json: JsonValue =
            serde_json::from_str(&to_json(&legacy).expect("序列化失败")).expect("JSON 解析失败");
        for key in ["exit_code", "source", "best_effort"] {
            assert!(
                legacy_json.get(key).is_none(),
                "{key} 不应出现在旧形状 JSON 中"
            );
        }

        // 完整字段可往返，且枚举使用 snake_case。
        let full = TranscriptCommand {
            command: "false".to_string(),
            started_at: "2026-11-02T09:00:00Z".to_string(),
            ended_at: Some("2026-11-02T09:00:01Z".to_string()),
            status: TranscriptStatus::Done,
            output: String::new(),
            exit_code: Some(1),
            source: Some(TranscriptSource::ProcessGroup),
            best_effort: true,
        };
        let full_json: JsonValue =
            serde_json::from_str(&to_json(&full).expect("序列化失败")).expect("JSON 解析失败");
        assert_eq!(full_json["exit_code"], 1);
        assert_eq!(full_json["source"], "process_group");
        assert_eq!(full_json["best_effort"], true);

        let back: TranscriptCommand =
            from_json(&to_json(&full).expect("序列化失败")).expect("反序列化失败");
        assert_eq!(back, full);

        for (source, expected) in [
            (TranscriptSource::ShellIntegration, "shell_integration"),
            (TranscriptSource::ProcessGroup, "process_group"),
            (TranscriptSource::Unknown, "unknown"),
        ] {
            let json = serde_json::to_value(source).expect("来源序列化失败");
            assert_eq!(json, expected);
        }
    }
}

/// 命令记录状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptStatus {
    /// 已开始、仍在等待输出/结束。
    Running,
    /// 命令正常结束（下一条命令到来或静默达到阈值）。
    Done,
    /// 终端进程已退出。
    Exited,
    /// 终端被信号/终止操作中断。
    Interrupted,
    /// 终端进入错误终态。
    Error,
}

/// 命令结束证据来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptSource {
    /// shell integration（OSC 133/633）报告的命令结束。
    ShellIntegration,
    /// 依据进程组/静默等进程级证据推断。
    ProcessGroup,
    /// 来源未知。
    Unknown,
}

/// `bool` 字段为 `false` 时省略，保持旧 JSON 形状。
fn is_false(value: &bool) -> bool {
    !*value
}

/// 单条命令记录（信息页使用）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptCommand {
    /// 用户发送且 `enter=true` 的命令文本（已去掉末尾 CR/LF）。
    pub command: String,
    /// 命令开始时间（RFC3339 UTC）。
    pub started_at: String,
    /// 命令结束时间（RFC3339 UTC）；`running` 时为 `None`。
    pub ended_at: Option<String>,
    /// 命令状态。
    pub status: TranscriptStatus,
    /// 已清洗的输出（去 ANSI、命令回显与末尾提示符）。
    pub output: String,
    /// 命令退出码；无法确定时为 `None`（不得伪造）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// 命令结束证据来源；未结束时为 `None`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<TranscriptSource>,
    /// 结束判定是否为 best-effort（例如静默推断）；默认 `false`。
    #[serde(default, skip_serializing_if = "is_false")]
    pub best_effort: bool,
}

/// `terminal.transcript` 参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TranscriptParams {
    /// 终端定位器。
    #[serde(flatten)]
    pub locator: TerminalLocator,
    /// 返回最近多少条命令；缺省/0 表示返回全部保留记录。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// `terminal.transcript` 结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptResult {
    /// 按时间升序排列的命令记录。
    pub commands: Vec<TranscriptCommand>,
    /// 每次记录/追加/结束时递增的修订号，供客户端判断是否需要重渲染。
    pub revision: u64,
}
