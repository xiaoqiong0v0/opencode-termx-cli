//! SSH 连接与远端 helper 协议（M0 占位）。
//!
//! M6 起接入 `russh` 与 helper 握手/降级；本阶段只保留目标描述类型，
//! 不引入任何 SSH 实现依赖。

/// SSH 连接目标。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshTarget {
    /// 主机名或地址。
    pub host: String,
    /// 登录用户名。
    pub user: String,
    /// 端口。
    pub port: u16,
}
