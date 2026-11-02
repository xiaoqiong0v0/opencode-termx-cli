//! 终端后端抽象与工厂。
//!
//! 依赖方向：本模块位于 `core`，可依赖 `termx-proc`（`proc` 不反向依赖 `core`）。
//! - [`PtyFactory`] 调用 [`termx_proc::PtySession::spawn`] 创建真实 PTY/ConPTY；
//! - [`MockFactory`] / [`MockBackend`] 不创建真实进程，供 registry / service 的
//!   既有 mock 测试复用；
//! - 所有可能阻塞的后端方法（`write` / `resize` / `signal` / `terminate` / `tree`
//!   等）由调用方在 registry `std::sync::Mutex` 之外调用；本模块自身不做跨
//!   await 操作。

use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError,
    atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
};

use termx_proc::{ProcError, ProcEvent, PtyConfig, PtySession};
use termx_protocol::{CreateParams, Mode, ProcNode};
use tokio::sync::mpsc::{Receiver, channel};

use crate::CoreError;

/// mock 后端的 pid/pgid 起始值。
pub const MOCK_PID_START: u32 = 1000;

/// pty 模式默认列数。
pub const DEFAULT_PTY_COLS: u16 = 80;

/// pty 模式默认行数。
pub const DEFAULT_PTY_ROWS: u16 = 24;

/// 进程内 mock pid 分配器；从 [`MOCK_PID_START`] 起递增。
static NEXT_MOCK_PID: AtomicU32 = AtomicU32::new(MOCK_PID_START);

/// 后端工厂产出：已启动的后端句柄与事件接收端。
pub type SpawnedBackend = (Arc<dyn TerminalBackend>, Receiver<ProcEvent>);

/// 终端后端能力。
///
/// `pid` / `pgid` 是创建后不变的快照；`foreground_pgrp` / `tree` 反映运行期
/// 状态，可能失败或为空（例如终端已退出、平台无进程组语义）。
pub trait TerminalBackend: Send + Sync {
    /// 主进程 ID。
    fn pid(&self) -> u32;
    /// 进程组 ID。
    fn pgid(&self) -> u32;
    /// 前台进程组 ID；无 TTY/进程组语义或查询失败时返回 `None`。
    fn foreground_pgrp(&self) -> Option<u32>;
    /// 以根进程为起点的进程树；查询失败时返回空树。
    fn tree(&self) -> Vec<ProcNode>;
    /// 向终端写入输入字节。
    ///
    /// 可能阻塞（PTY 写缓冲满），调用方必须在 registry 锁之外执行。
    fn write(&self, data: &[u8]) -> Result<(), CoreError>;
    /// 调整终端窗口大小。
    fn resize(&self, cols: u16, rows: u16) -> Result<(), CoreError>;
    /// 发送信号；接受 `SIGINT` / `SIGTERM` / `SIGKILL` 及其无前缀别名。
    ///
    /// 未知信号返回 [`CoreError::UnsupportedSignal`]。
    fn signal(&self, name: &str) -> Result<(), CoreError>;
    /// 终止后端进程树（幂等）。
    fn terminate(&self);
}

/// 后端工厂：把 `terminal.create` 参数变成真实进程或 mock 进程。
///
/// 实现方必须自身不阻塞 async 运行时：service 会把 `spawn` 放到
/// `tokio::task::spawn_blocking` 中执行。
pub trait BackendFactory: Send + Sync {
    /// 按创建参数启动后端并返回事件接收端。
    ///
    /// # 错误
    /// 进程/PTY 创建失败返回 [`CoreError::SpawnFailed`]；参数不支持时返回
    /// [`CoreError::BadRequest`]。
    fn spawn(&self, params: &CreateParams) -> Result<SpawnedBackend, CoreError>;
}

/// 真实 PTY 后端：包装 [`termx_proc::PtySession`]。
///
/// `PtySession` 的可变操作需要 `&mut self`，这里用 [`Mutex`] 串行化；因此
/// `PtyBackend` 可安全地以 `Arc` 在 registry 与后台任务间共享。
pub struct PtyBackend {
    /// 底层 PTY 会话；所有访问都在锁内完成。
    session: Mutex<PtySession>,
    /// 创建后不变的根进程 ID。
    pid: u32,
    /// 创建后不变的进程组 ID。
    pgid: u32,
}

impl PtyBackend {
    /// 包装一个已启动的 PTY 会话。
    pub fn new(session: PtySession) -> Self {
        let pid = session.pid();
        let pgid = session.pgid();
        Self {
            session: Mutex::new(session),
            pid,
            pgid,
        }
    }

    /// 获取会话锁；锁中毒时取回内部值继续（会话本身无跨调用不变量）。
    fn lock_session(&self) -> MutexGuard<'_, PtySession> {
        self.session.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl TerminalBackend for PtyBackend {
    fn pid(&self) -> u32 {
        self.pid
    }

    fn pgid(&self) -> u32 {
        self.pgid
    }

    fn foreground_pgrp(&self) -> Option<u32> {
        self.lock_session().foreground_pgrp().ok()
    }

    fn tree(&self) -> Vec<ProcNode> {
        self.lock_session().tree().unwrap_or_default()
    }

    fn write(&self, data: &[u8]) -> Result<(), CoreError> {
        self.lock_session().write(data).map_err(map_proc_error)
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<(), CoreError> {
        self.lock_session()
            .resize(cols, rows)
            .map_err(map_proc_error)
    }

    fn signal(&self, name: &str) -> Result<(), CoreError> {
        self.lock_session().signal(name).map_err(map_proc_error)
    }

    fn terminate(&self) {
        // terminate 自身幂等；错误只能记录，调用方不应因清理失败而中断。
        if let Err(error) = self.lock_session().terminate() {
            tracing::warn!(%error, "终止 PTY 会话失败");
        }
    }
}

/// 把 `termx-proc` 错误映射为 core 错误；未知信号保持可区分。
fn map_proc_error(error: ProcError) -> CoreError {
    match error {
        ProcError::UnsupportedSignal(name) => CoreError::UnsupportedSignal(name),
        other => CoreError::BackendFailed(other.to_string()),
    }
}

/// 真实 PTY 工厂：按 `CreateParams` 组装 [`PtyConfig`] 后启动 ConPTY/PTY。
#[derive(Debug, Default, Clone, Copy)]
pub struct PtyFactory;

impl BackendFactory for PtyFactory {
    fn spawn(&self, params: &CreateParams) -> Result<SpawnedBackend, CoreError> {
        if params.mode != Mode::Pty {
            return Err(CoreError::BadRequest(
                "stdio 模式将在 M3 支持，当前请使用 pty".to_string(),
            ));
        }

        let config = PtyConfig {
            program: params.cmd.clone(),
            args: params.args.clone(),
            cwd: params.cwd.clone(),
            env: params.env.clone(),
            cols: params.cols.unwrap_or(DEFAULT_PTY_COLS),
            rows: params.rows.unwrap_or(DEFAULT_PTY_ROWS),
        };

        let (session, events) =
            PtySession::spawn(config).map_err(|error| CoreError::SpawnFailed {
                message: error.to_string(),
            })?;
        Ok((Arc::new(PtyBackend::new(session)), events))
    }
}

/// 不创建真实进程的 mock 后端，供 registry / service 测试使用。
#[derive(Debug)]
pub struct MockBackend {
    /// 分配到的 pid；mock 中 pgid 与 pid 相同。
    pid: u32,
    /// 是否已调用过 [`TerminalBackend::terminate`]（保留布尔语义）。
    terminated: AtomicBool,
    /// `terminate` 调用次数，用于验证终态只终止一次。
    terminate_calls: AtomicUsize,
    /// 收到的写入数据。
    writes: Mutex<Vec<Vec<u8>>>,
    /// 收到的窗口尺寸调整。
    resizes: Mutex<Vec<(u16, u16)>>,
    /// 收到的信号名。
    signals: Mutex<Vec<String>>,
}

impl MockBackend {
    /// 从全局原子计数器分配新的 pid/pgid。
    pub fn new() -> Self {
        Self {
            pid: NEXT_MOCK_PID.fetch_add(1, Ordering::Relaxed),
            terminated: AtomicBool::new(false),
            terminate_calls: AtomicUsize::new(0),
            writes: Mutex::new(Vec::new()),
            resizes: Mutex::new(Vec::new()),
            signals: Mutex::new(Vec::new()),
        }
    }

    /// 是否已被终止；测试用于验证 `force` 先终止旧终端。
    pub fn was_terminated(&self) -> bool {
        self.terminated.load(Ordering::Relaxed)
    }

    /// `terminate` 被调用的次数。
    pub fn terminate_calls(&self) -> usize {
        self.terminate_calls.load(Ordering::Relaxed)
    }

    /// 已收到的写入数据（按调用顺序）。
    pub fn writes(&self) -> Vec<Vec<u8>> {
        self.writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// 已收到的尺寸调整（按调用顺序）。
    pub fn resizes(&self) -> Vec<(u16, u16)> {
        self.resizes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// 已收到的信号名（按调用顺序）。
    pub fn signals(&self) -> Vec<String> {
        self.signals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Default for MockBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl TerminalBackend for MockBackend {
    fn pid(&self) -> u32 {
        self.pid
    }

    fn pgid(&self) -> u32 {
        self.pid
    }

    fn foreground_pgrp(&self) -> Option<u32> {
        Some(self.pid)
    }

    fn tree(&self) -> Vec<ProcNode> {
        Vec::new()
    }

    fn write(&self, data: &[u8]) -> Result<(), CoreError> {
        self.writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(data.to_vec());
        Ok(())
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<(), CoreError> {
        self.resizes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((cols, rows));
        Ok(())
    }

    fn signal(&self, name: &str) -> Result<(), CoreError> {
        self.signals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(name.to_string());
        Ok(())
    }

    fn terminate(&self) {
        self.terminated.store(true, Ordering::Relaxed);
        self.terminate_calls.fetch_add(1, Ordering::Relaxed);
    }
}

/// mock 工厂：每次调用产生一个新的 [`MockBackend`]，事件通道立即关闭。
#[derive(Debug, Default, Clone, Copy)]
pub struct MockFactory;

impl BackendFactory for MockFactory {
    fn spawn(&self, _params: &CreateParams) -> Result<SpawnedBackend, CoreError> {
        let (sender, receiver) = channel(1);
        // mock 不产生事件；显式丢弃发送端，让摄取任务立即结束。
        drop(sender);
        Ok((Arc::new(MockBackend::new()), receiver))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_backend_allocates_increasing_pids_and_tracks_termination() {
        let first = MockBackend::new();
        let second = MockBackend::new();

        assert!(first.pid() >= MOCK_PID_START);
        assert!(second.pid() > first.pid());
        assert_eq!(first.pid(), first.pgid());
        assert_eq!(first.foreground_pgrp(), Some(first.pid()));
        assert!(first.tree().is_empty());
        assert!(!first.was_terminated());

        first.terminate();
        assert!(first.was_terminated());
        assert!(!second.was_terminated());
    }
}
