//! CLI 错误类型与退出码映射。
//!
//! 错误信息只包含方法与错误码，不回显 token 或请求原文。

use std::fmt;

use termx_client::ClientError;
use termx_protocol::ProtocolError;

/// CLI 顶层错误。
#[derive(Debug)]
pub enum CliError {
    /// 参数组合非法（例如 locator 缺失、`--env` 不是 `K=V`）。
    Usage(String),
    /// 客户端连接/协议错误。
    Client(ClientError),
    /// 本地 I/O 错误（stdout/stderr 写入等）。
    Io(std::io::Error),
    /// 尚未实现的子命令（M6 的 `ssh`）。
    Unsupported(String),
}

impl CliError {
    /// 进程退出码：用法错误 2，连接/协议/IO 错误 1。
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) | Self::Unsupported(_) => 2,
            Self::Client(_) | Self::Io(_) => 1,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) => write!(formatter, "用法错误: {message}"),
            Self::Client(error) => write!(formatter, "{}", describe_client_error(error)),
            Self::Io(error) => write!(formatter, "I/O 错误: {error}"),
            Self::Unsupported(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for CliError {}

impl From<ClientError> for CliError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<std::io::Error> for CliError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// 把客户端错误转成稳定可读文本；协议错误保留 snake_case 错误码。
fn describe_client_error(error: &ClientError) -> String {
    match error {
        ClientError::Protocol(protocol) => {
            format!(
                "协议错误[{}]: {}",
                protocol_code_name(protocol),
                protocol.message
            )
        }
        other => other.to_string(),
    }
}

/// 取协议错误码的 snake_case 名称；序列化失败时退回 Debug 形式。
fn protocol_code_name(error: &ProtocolError) -> String {
    match serde_json::to_value(error.code) {
        Ok(serde_json::Value::String(name)) => name,
        _ => format!("{:?}", error.code),
    }
}
