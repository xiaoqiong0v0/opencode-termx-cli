//! 本地 PTY / ConPTY 与进程管理。
//!
//! Windows 直接绑定 Win32 `CreatePseudoConsole` 系列接口，Unix/Linux 直接使用
//! 操作系统 PTY（`openpty` / `fork` / `setsid` / `execve`），均不依赖 node-pty
//! / portable-pty / wezterm 等封装；其它平台统一返回 [`ProcError::Unsupported`]，
//! 保证工作区可跨平台编译。

use std::collections::BTreeMap;

use thiserror::Error;

#[cfg(not(any(windows, unix)))]
mod unsupported;
#[cfg(not(any(windows, unix)))]
pub use unsupported::{PtySession, StdioSession};
#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::PtySession;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::PtySession;

/// 跨平台管道 stdio 后端；非 Windows/Unix 平台由 `unsupported` 提供同名占位。
#[cfg(any(windows, unix))]
mod stdio;
#[cfg(any(windows, unix))]
pub use stdio::StdioSession;

/// `termx-proc` 的统一错误类型。
#[derive(Debug, Error)]
pub enum ProcError {
    /// 创建子进程失败（含参数、环境、工作目录等错误）。
    #[error("创建进程失败：{message}")]
    SpawnFailed {
        /// 可读的失败原因（含 Win32 错误信息）。
        message: String,
    },
    /// 伪终端（ConPTY/PTY）操作失败。
    #[error("伪终端操作失败：{message}")]
    PtyFailed {
        /// 可读的失败原因（含 Win32 错误信息）。
        message: String,
    },
    /// 底层 I/O 失败。
    #[error("I/O 失败：{0}")]
    Io(#[from] std::io::Error),
    /// 当前平台尚未实现 PTY 后端。
    #[error("当前平台不支持 PTY")]
    Unsupported,
    /// 会话已退出、未启动或写端已关闭。
    #[error("进程未运行")]
    NotRunning,
    /// 内部不变量或系统调用结果异常。
    #[error("内部错误：{message}")]
    Internal {
        /// 可读的错误原因。
        message: String,
    },
    /// 不支持的信号名。
    #[error("不支持的信号: {0}")]
    UnsupportedSignal(String),
    /// stdio 写入队列已满；调用方应映射为 `limit_exceeded`。
    #[error("stdio 写入队列已满")]
    WriteQueueFull,
}

/// stdio stdin 有界写队列的默认容量（条）。
pub const DEFAULT_STDIO_WRITE_QUEUE_CAPACITY: usize = 64;

/// 把外部信号名规范化为 `SIGINT` / `SIGTERM` / `SIGKILL`。
///
/// 接受大小写混写与省略 `SIG` 前缀的别名（如 `int`、`TERM`）；未知信号
/// 返回 `None`，由调用方转成 [`ProcError::UnsupportedSignal`]。
pub(crate) fn normalize_signal(name: &str) -> Option<&'static str> {
    let upper = name.trim().to_ascii_uppercase();
    let bare = upper.strip_prefix("SIG").unwrap_or(upper.as_str());
    match bare {
        "INT" => Some("SIGINT"),
        "TERM" => Some("SIGTERM"),
        "KILL" => Some("SIGKILL"),
        _ => None,
    }
}

/// 创建 PTY 会话所需的配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PtyConfig {
    /// 可执行程序（可带完整路径；裸文件名按 Windows 搜索顺序解析）。
    pub program: String,
    /// 传给程序的参数（由本库按 Windows 命令行规则转义，不经过 shell）。
    pub args: Vec<String>,
    /// 子进程工作目录；`None` 表示继承父进程当前目录。
    pub cwd: Option<String>,
    /// 环境变量覆盖表；`None` 表示完整继承父进程环境。
    ///
    /// 为 `Some` 时以父环境为基线，按变量名（不区分大小写）覆盖后再排序。
    pub env: Option<BTreeMap<String, String>>,
    /// 初始终端列数。
    pub cols: u16,
    /// 初始终端行数。
    pub rows: u16,
}

impl Default for PtyConfig {
    fn default() -> Self {
        Self {
            program: String::new(),
            args: Vec::new(),
            cwd: None,
            env: None,
            cols: 80,
            rows: 24,
        }
    }
}

/// 输出流来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputStream {
    /// PTY 合并流（stdout/stderr 已由终端合并）。
    Pty,
    /// 独立 stdout 管道。
    Stdout,
    /// 独立 stderr 管道。
    Stderr,
}

/// 本地进程会话（PTY / stdio）产生的事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcEvent {
    /// 一段输出字节（已按读取缓冲切分，不保证 UTF-8 边界）。
    Output {
        /// 输出字节。
        data: Vec<u8>,
        /// 输出来源流。
        stream: OutputStream,
    },
    /// 子进程退出；承载进程退出码（原样按 `i32` 解释低 32 位）。
    ///
    /// Windows 上被强制终止也走该事件，不会产生 [`ProcEvent::Signaled`]。
    Exited(i32),
    /// 子进程被信号终止；承载信号编号（Unix `WTERMSIG` 原值）。
    ///
    /// 仅 Unix 后端会发送；Windows 永不发送。
    Signaled(i32),
    /// shell 带外事件（prompt / command 生命周期）。
    ///
    /// 本地 PTY/stdio 后端不产生该事件；远端 helper 通道会把子进程经 UDS
    /// 上报的 `A/B/C/D` 原样转成该变体，交给 service 复用 transcript 判定。
    ShellEvent(termx_protocol::shell_events::ShellEvent),
}

#[cfg(test)]
mod tests {
    use super::normalize_signal;

    #[test]
    fn signal_names_accept_prefix_and_aliases() {
        for name in ["SIGINT", "sigint", "INT", "int", " SIGINT "] {
            assert_eq!(normalize_signal(name), Some("SIGINT"), "{name:?}");
        }
        assert_eq!(normalize_signal("TERM"), Some("SIGTERM"));
        assert_eq!(normalize_signal("sigterm"), Some("SIGTERM"));
        assert_eq!(normalize_signal("KILL"), Some("SIGKILL"));
        assert_eq!(normalize_signal("SIGKILL"), Some("SIGKILL"));

        for name in ["", "SIGHUP", "STOP", "SIG"] {
            assert_eq!(normalize_signal(name), None, "{name:?}");
        }
    }
}
