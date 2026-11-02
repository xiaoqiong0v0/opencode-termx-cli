//! 既非 Windows 也非 Unix 的平台（如 wasm）的占位后端。
//!
//! Unix/Linux 已由 `unix.rs` 提供真实 PTY 后端；本模块所有操作都返回
//! [`ProcError::Unsupported`]，保证工作区在其它目标上仍可编译。

use termx_protocol::ProcNode;
use tokio::sync::mpsc::Receiver;

use crate::{ProcError, ProcEvent, PtyConfig};

/// 非 Windows 平台上的空 PTY 会话。
#[derive(Debug)]
pub struct PtySession {
    /// 占位字段，确保结构体不能在该平台被直接构造。
    _private: (),
}

/// 非 Windows/Unix 平台上的空 stdio 会话。
#[derive(Debug)]
pub struct StdioSession {
    /// 占位字段，确保结构体不能在该平台被直接构造。
    _private: (),
}

impl StdioSession {
    /// 当前平台没有管道 stdio 后端。
    pub fn spawn(_cfg: PtyConfig) -> Result<(Self, Receiver<ProcEvent>), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 当前平台没有管道 stdio 后端。
    pub fn spawn_with_write_queue(
        _cfg: PtyConfig,
        _write_queue_capacity: usize,
    ) -> Result<(Self, Receiver<ProcEvent>), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时没有 pid。
    pub fn pid(&self) -> u32 {
        0
    }

    /// 未启动时没有进程组。
    pub fn pgid(&self) -> u32 {
        0
    }

    /// stdio 没有前台进程组。
    pub fn foreground_pgrp(&self) -> Result<Option<u32>, ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时不可写入。
    pub fn write(&mut self, _data: &[u8]) -> Result<(), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时不可调整窗口大小。
    pub fn resize(&mut self, _cols: u16, _rows: u16) -> Result<(), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 当前平台不支持信号。
    pub fn signal(&mut self, _name: &str) -> Result<(), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时没有可终止的进程。
    pub fn terminate(&mut self) -> Result<(), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时没有进程树。
    pub fn tree(&self) -> Result<Vec<ProcNode>, ProcError> {
        Err(ProcError::Unsupported)
    }
}

impl PtySession {
    /// 非 Windows 平台尚无 PTY 后端。
    pub fn spawn(_cfg: PtyConfig) -> Result<(Self, Receiver<ProcEvent>), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时没有 pid。
    pub fn pid(&self) -> u32 {
        0
    }

    /// 未启动时没有进程组。
    pub fn pgid(&self) -> u32 {
        0
    }

    /// 当前平台没有前台进程组。
    pub fn foreground_pgrp(&self) -> Result<u32, ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时不可写入。
    pub fn write(&mut self, _data: &[u8]) -> Result<(), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时不可调整窗口大小。
    pub fn resize(&mut self, _cols: u16, _rows: u16) -> Result<(), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 当前平台不支持信号。
    pub fn signal(&mut self, _name: &str) -> Result<(), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时没有可终止的进程。
    pub fn terminate(&mut self) -> Result<(), ProcError> {
        Err(ProcError::Unsupported)
    }

    /// 未启动时没有进程树。
    pub fn tree(&self) -> Result<Vec<ProcNode>, ProcError> {
        Err(ProcError::Unsupported)
    }
}
