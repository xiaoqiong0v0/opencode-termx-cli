//! 客户端错误类型。
//!
//! 错误分层对齐 `docs/PROTOCOL.md` §10：握手阶段的 `auth_failed` /
//! `version_mismatch` 单独成变体，其余协议错误原样保留 [`ProtocolError`]，
//! 便于调用方按 `code` 做业务分支。

use termx_protocol::ProtocolError;

/// termx 客户端操作错误。
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// 建立 WebSocket 连接失败（地址非法、服务未启动、握手被拒等）。
    #[error("连接失败: {0}")]
    Connect(String),
    /// 鉴权失败：token 无效。
    #[error("鉴权失败：token 无效")]
    Auth,
    /// 主协议版本不匹配。
    #[error("协议版本不匹配")]
    VersionMismatch,
    /// 服务端返回的协议错误（含 `not_found` / `permission_denied` 等）。
    #[error("协议错误: {0:?}")]
    Protocol(ProtocolError),
    /// 连接建立后收发失败。
    #[error("传输失败: {0}")]
    Transport(String),
    /// 连接已关闭，或请求等待期间连接断开。
    #[error("连接已关闭")]
    Closed,
    /// 连接/握手超时。
    #[error("操作超时")]
    Timeout,
}
