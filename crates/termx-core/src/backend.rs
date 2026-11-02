//! 终端后端抽象与工厂。
//!
//! 依赖方向：本模块位于 `core`，可依赖 `termx-proc`（`proc` 不反向依赖 `core`）。
//! - [`PtyFactory`] 调用 [`termx_proc::PtySession::spawn`] 创建真实 PTY/ConPTY；
//! - [`StdioFactory`] 调用 [`termx_proc::StdioSession::spawn`] 创建独立管道；
//! - [`ModeFactory`] 按 `mode` 路由到上述两种真实后端，是 registry 的默认工厂；
//! - [`MockFactory`] / [`MockBackend`] 不创建真实进程，供 registry / service 的
//!   既有 mock 测试复用；
//! - 所有可能阻塞的后端方法（`write` / `resize` / `signal` / `terminate` / `tree`
//!   等）由调用方在 registry `std::sync::Mutex` 之外调用；本模块自身不做跨
//!   await 操作。

use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError,
    atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
};

use termx_proc::{ProcError, ProcEvent, PtyConfig, PtySession, StdioSession};
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
        ProcError::WriteQueueFull => CoreError::LimitExceeded("stdio 写入队列已满".to_string()),
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
                "PtyFactory 仅支持 pty 模式".to_string(),
            ));
        }

        let config = build_config(params);
        let (session, events) =
            PtySession::spawn(config).map_err(|error| CoreError::SpawnFailed {
                message: error.to_string(),
            })?;
        Ok((Arc::new(PtyBackend::new(session)), events))
    }
}

/// 真实管道 stdio 后端：包装 [`termx_proc::StdioSession`]。
pub struct StdioBackend {
    /// 底层 stdio 会话；所有可变操作都在锁内完成。
    session: Mutex<StdioSession>,
    /// 创建后不变的根进程 ID。
    pid: u32,
    /// 创建后不变的进程组 ID。
    pgid: u32,
}

impl StdioBackend {
    /// 包装一个已启动的 stdio 会话。
    pub fn new(session: StdioSession) -> Self {
        let pid = session.pid();
        let pgid = session.pgid();
        Self {
            session: Mutex::new(session),
            pid,
            pgid,
        }
    }

    /// 获取会话锁；锁中毒时取回内部值继续。
    fn lock_session(&self) -> MutexGuard<'_, StdioSession> {
        self.session.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl TerminalBackend for StdioBackend {
    fn pid(&self) -> u32 {
        self.pid
    }

    fn pgid(&self) -> u32 {
        self.pgid
    }

    fn foreground_pgrp(&self) -> Option<u32> {
        // stdio 没有 TTY/前台进程组。
        self.lock_session().foreground_pgrp().ok().flatten()
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
        if let Err(error) = self.lock_session().terminate() {
            tracing::warn!(%error, "终止 stdio 会话失败");
        }
    }
}

/// 真实 stdio 工厂：启动独立管道的子进程。
#[derive(Debug, Clone, Copy)]
pub struct StdioFactory {
    /// stdin 有界写队列容量（条）。
    write_queue_capacity: usize,
}

impl StdioFactory {
    /// 使用指定写队列容量创建工厂。
    pub fn with_write_queue_capacity(write_queue_capacity: usize) -> Self {
        Self {
            write_queue_capacity,
        }
    }
}

impl Default for StdioFactory {
    fn default() -> Self {
        Self::with_write_queue_capacity(termx_proc::DEFAULT_STDIO_WRITE_QUEUE_CAPACITY)
    }
}

impl BackendFactory for StdioFactory {
    fn spawn(&self, params: &CreateParams) -> Result<SpawnedBackend, CoreError> {
        if params.mode != Mode::Stdio {
            return Err(CoreError::BadRequest(
                "StdioFactory 仅支持 stdio 模式".to_string(),
            ));
        }

        let (session, events) =
            StdioSession::spawn_with_write_queue(build_config(params), self.write_queue_capacity)
                .map_err(|error| CoreError::SpawnFailed {
                message: error.to_string(),
            })?;
        Ok((Arc::new(StdioBackend::new(session)), events))
    }
}

/// 按 `mode` 路由到真实 PTY / stdio 工厂；registry 的默认工厂。
///
/// 测试仍可用 [`crate::MockFactory`] 注入单一 mock 工厂，此时所有模式都走 mock。
pub struct ModeFactory {
    /// pty 模式工厂。
    pty: Arc<dyn BackendFactory>,
    /// stdio 模式工厂。
    stdio: Arc<dyn BackendFactory>,
}

impl ModeFactory {
    /// 用自定义子工厂构造路由器（便于测试或替换平台实现）。
    pub fn new(pty: Arc<dyn BackendFactory>, stdio: Arc<dyn BackendFactory>) -> Self {
        Self { pty, stdio }
    }

    /// 用默认 PTY/stdio 子工厂构造路由器，并指定 stdio 写队列容量。
    pub fn with_stdio_write_queue_capacity(write_queue_capacity: usize) -> Self {
        Self {
            pty: Arc::new(PtyFactory),
            stdio: Arc::new(StdioFactory::with_write_queue_capacity(
                write_queue_capacity,
            )),
        }
    }
}

impl Default for ModeFactory {
    fn default() -> Self {
        Self {
            pty: Arc::new(PtyFactory),
            stdio: Arc::new(StdioFactory::default()),
        }
    }
}

impl BackendFactory for ModeFactory {
    fn spawn(&self, params: &CreateParams) -> Result<SpawnedBackend, CoreError> {
        match params.mode {
            Mode::Pty => self.pty.spawn(params),
            Mode::Stdio => self.stdio.spawn(params),
        }
    }
}

/// 把创建参数转换为 [`PtyConfig`]；stdio 路径忽略尺寸。
fn build_config(params: &CreateParams) -> PtyConfig {
    PtyConfig {
        program: params.cmd.clone(),
        args: params.args.clone(),
        cwd: params.cwd.clone(),
        env: params.env.clone(),
        cols: params.cols.unwrap_or(DEFAULT_PTY_COLS),
        rows: params.rows.unwrap_or(DEFAULT_PTY_ROWS),
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
        // 与真实后端保持一致：只接受 SIGINT/SIGTERM/SIGKILL（含无前缀别名），
        // 未知信号返回错误，便于测试「失败不落 pending_interrupt」。
        let upper = name.trim().to_ascii_uppercase();
        let bare = upper.strip_prefix("SIG").unwrap_or(upper.as_str());
        if !matches!(bare, "INT" | "TERM" | "KILL") {
            return Err(CoreError::UnsupportedSignal(name.to_string()));
        }
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
