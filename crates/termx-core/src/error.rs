//! core 层错误类型；由 service 层映射为协议错误码。

use thiserror::Error;

/// registry 操作错误。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CoreError {
    /// 同名 `(root_session_id, name)` 已存在且未带 `force`。
    #[error("同名会话已存在: {terminal_id}")]
    SessionExists {
        /// 已存在终端的 ID；供 `session_exists.terminal_id` 回传。
        terminal_id: String,
    },
    /// 名称非法：空串、包含 `/` 或 `\`、或超过长度上限。
    #[error("终端名称非法：不能为空、不能包含 `/` 或 `\\`，且长度不超过 64")]
    InvalidName,
    /// `idle_timeout` 校验失败。
    #[error("idle_timeout 非法: {0}")]
    InvalidIdleTimeout(String),
    /// 终端不存在（或已被 force 替换并移除）。
    #[error("终端不存在")]
    NotFound,
    /// 终端已处于终态，拒绝写操作。
    #[error("终端已终止，拒绝写操作")]
    AlreadyTerminal,
    /// 非输入 lease owner 或 lease 已被他人占用。
    #[error("非输入 lease owner 或权限不足")]
    PermissionDenied,
    /// 参数缺失或不完整。
    #[error("请求参数非法: {0}")]
    BadRequest(String),
    /// 后端进程创建失败（含 PTY/ConPTY 初始化失败）。
    #[error("进程创建失败: {message}")]
    SpawnFailed {
        /// 可读的失败原因。
        message: String,
    },
    /// 后端操作（写/缩放/信号/终止）失败。
    #[error("后端操作失败: {0}")]
    BackendFailed(String),
    /// 不支持的信号名。
    #[error("不支持的信号: {0}")]
    UnsupportedSignal(String),
    /// 超过资源上限（终端数量、stdio 写队列等），对应 `limit_exceeded`。
    #[error("超过资源上限: {0}")]
    LimitExceeded(String),
}
