//! Windows ConPTY 后端：直接调用 Win32 `CreatePseudoConsole` / Job Object。
//!
//! 关键不变量：
//! - 管道句柄不可继承，`CreateProcessW` 使用 `bInheritHandles = FALSE`；子进程
//!   的控制台句柄由 `PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE` 属性注入；
//! - 伪控制台会 dup 控制台侧管道句柄，因此 `CreatePseudoConsole` 成功后立即
//!   关闭本进程副本；
//! - 子进程先以 `CREATE_SUSPENDED` 创建，加入 Job Object 后再恢复，避免
//!   “进程已退出导致无法加入 Job”的竞态，并保证 `terminate` 能清理整棵进程树；
//! - ConPTY 输出管道不会在客户端退出时自动 EOF，因此由退出监视线程在进程
//!   结束后关闭伪控制台，迫使读线程读到 EOF 后再发送唯一的 `Exited`；
//! - 输出读端由读线程独占，进程句柄与 HPCON 通过 `Arc` 共享，避免跨线程关闭
//!   同一对象；
//! - 输出屏障由请求方用 `CancelSynchronousIo` 打断读线程阻塞中的 `ReadFile`：
//!   读线程被取消后不当作 EOF，而是排空当前已可读输出、确认后继续读取，因此
//!   屏障返回时对应 `Output` 已入队（与 Unix 后端语义一致，不依赖时间窗口）。

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::c_void;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use termx_protocol::ProcNode;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, warn};
use windows::Win32::Foundation::{
    CloseHandle, ERROR_NOT_FOUND, ERROR_OPERATION_ABORTED, HANDLE, INVALID_HANDLE_VALUE,
};
use windows::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows::Win32::System::Console::{
    COORD, ClosePseudoConsole, CreatePseudoConsole, HPCON, ResizePseudoConsole,
};
use windows::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW, TH32CS_SNAPPROCESS,
};
use windows::Win32::System::Environment::{FreeEnvironmentStringsW, GetEnvironmentStringsW};
use windows::Win32::System::IO::CancelSynchronousIo;
use windows::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject,
};
use windows::Win32::System::Pipes::{CreatePipe, PeekNamedPipe};
use windows::Win32::System::Threading::{
    CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW, DeleteProcThreadAttributeList,
    EXTENDED_STARTUPINFO_PRESENT, GetCurrentThreadId, GetExitCodeProcess, INFINITE,
    InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST, OpenProcess, OpenThread,
    PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, PROCESS_INFORMATION, PROCESS_SET_QUOTA, PROCESS_TERMINATE,
    ResumeThread, STARTF_USESTDHANDLES, STARTUPINFOEXW, THREAD_TERMINATE, TerminateProcess,
    UpdateProcThreadAttribute, WaitForSingleObject,
};
use windows::core::{Error as WinError, PCWSTR, PWSTR};

use crate::{BarrierReport, OutputStream, ProcError, ProcEvent, PtyConfig, normalize_signal};

/// 事件通道容量；读线程满时通过 `blocking_send` 施加背压。
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// ConPTY 中表示 Ctrl-C 的输入字节。
const CTRL_C_BYTE: u8 = 0x03;

/// 读线程单次 `ReadFile` 的最大字节数。
const READ_BUFFER_SIZE: usize = 8 * 1024;

/// 强制终止进程树时写入的退出码。
const TERMINATE_EXIT_CODE: u32 = 1;

/// 进程退出后等待 conhost 把尾部输出刷入管道的宽限时长。
const EXIT_FLUSH_GRACE: std::time::Duration = std::time::Duration::from_millis(200);

/// `CancelSynchronousIo` 未命中（读线程尚未真正进入 `ReadFile`）后的重试间隔。
///
/// 只用于异常路径收敛：正常路径在读线程阻塞时一次取消即命中；该重试不参与
/// 事件排序，也不用于等待输出产生。
const CANCEL_RETRY_INTERVAL: Duration = Duration::from_millis(5);

/// 与退出监视线程共享的伪控制台；`None` 表示已关闭。
type SharedConsole = Arc<Mutex<Option<PseudoConsole>>>;

/// ConPTY 会话。
///
/// 句柄所有权：`input`（父进程写端）、`job` 由本结构持有；HPCON 由本结构与
/// 退出监视线程共享；输出读端由读线程独占；进程句柄由读线程与监视线程共享。
pub struct PtySession {
    /// 根进程 ID（Windows 无 Unix 进程组，`pgid()` 返回该值）。
    pid: u32,
    /// 写入终端输入的管道写端。
    input: Option<OwnedHandle>,
    /// 承载整棵进程树的 Job Object。
    job: Option<OwnedHandle>,
    /// 伪控制台句柄（共享、幂等关闭）。
    hpc: SharedConsole,
    /// 输出屏障控制端；与读线程共享，`barrier_handle` 克隆它。
    barrier: Arc<BarrierControl>,
    /// `terminate` 是否已执行（幂等标记）。
    terminated: bool,
}

/// ConPTY 输出屏障句柄：请求读线程先排空当前可读的 PTY 输出。
///
/// 句柄是 `Arc` 克隆，只共享屏障状态，不借用 `PtySession`（因此等待期间不会
/// 持有会话互斥锁），可在 service 的阻塞线程池或 helper 的事件转发任务里安全
/// 使用。
#[derive(Debug, Clone)]
pub struct BarrierHandle {
    /// 请求/确认计数、输出序号与读线程登记信息。
    inner: Arc<BarrierControl>,
}

impl BarrierHandle {
    /// 请求读线程排空当前可读输出并等待确认。
    ///
    /// 语义与 Unix 后端一致：返回 [`BarrierReport::Drained`] 时，编号不超过
    /// `output_seq` 的 `Output` 事件已全部入队；读线程已结束时直接返回当前
    /// 序号；只有等待超过 `timeout` 才返回 [`ProcError::BarrierTimeout`]。
    pub fn barrier(&self, timeout: Duration) -> Result<BarrierReport, ProcError> {
        self.inner.barrier(timeout)
    }
}

/// 屏障共享状态：请求/确认计数、读线程登记与关闭标志。
#[derive(Debug, Default)]
struct BarrierState {
    /// 已投递的屏障请求数。
    requested: u64,
    /// 读线程已处理的请求数。
    acked: u64,
    /// 最近一次确认时的输出事件序号。
    acked_seq: u64,
    /// 读线程是否已结束（终端退出或输出管道关闭）。
    closed: bool,
    /// 读线程 ID；`None` 表示尚未登记。
    reader_tid: Option<u32>,
    /// 读线程是否已发布“即将阻塞在 `ReadFile`”。
    reader_blocking: bool,
}

/// 与读线程共享的屏障控制块。
#[derive(Debug, Default)]
struct BarrierControl {
    /// 请求/确认状态与读线程登记。
    state: Mutex<BarrierState>,
    /// 状态变化通知。
    cv: Condvar,
    /// 读线程累计入队的 `Output` 事件数。
    output_seq: AtomicU64,
}

impl BarrierControl {
    /// 当前已入队的输出事件数。
    fn output_seq(&self) -> u64 {
        self.output_seq.load(Ordering::SeqCst)
    }

    /// 入队一段输出并递增屏障序号；空数据不产生事件。
    ///
    /// 返回 `false` 表示接收端已丢弃，读线程应停止。
    fn send_output(&self, tx: &Sender<ProcEvent>, data: &[u8]) -> bool {
        if data.is_empty() {
            return true;
        }
        self.output_seq.fetch_add(1, Ordering::SeqCst);
        tx.blocking_send(ProcEvent::Output {
            data: data.to_vec(),
            stream: OutputStream::Pty,
        })
        .is_ok()
    }

    /// 登记读线程 ID，供屏障请求用 `CancelSynchronousIo` 唤醒阻塞的读取。
    fn register_reader(&self, tid: u32) {
        self.lock_state().reader_tid = Some(tid);
    }

    /// 读线程是否已结束。
    fn is_closed(&self) -> bool {
        self.lock_state().closed
    }

    /// 是否有尚未确认的屏障请求。
    fn has_pending(&self) -> bool {
        let state = self.lock_state();
        !state.closed && state.requested > state.acked
    }

    /// 发布“即将阻塞在 `ReadFile`”，与请求方共用一把锁消除丢失唤醒。
    ///
    /// 返回 `false` 表示已有待处理请求或读线程已关闭，调用方应回到循环顶部
    /// 重新判断，而不是进入阻塞读取。
    fn enter_blocking_read(&self) -> bool {
        let mut state = self.lock_state();
        if state.closed || state.requested > state.acked {
            return false;
        }
        state.reader_blocking = true;
        true
    }

    /// 结束“即将阻塞读”的发布；`ReadFile` 返回后调用。
    fn leave_blocking_read(&self) {
        self.lock_state().reader_blocking = false;
    }

    /// 投递一次屏障请求并等待读线程确认。
    ///
    /// 读线程已结束时直接返回当前输出序号（不会再有新输出，顺序已确定）；
    /// 只有等待超时才返回 [`ProcError::BarrierTimeout`]。
    fn barrier(&self, timeout: Duration) -> Result<BarrierReport, ProcError> {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock_state();
        if state.closed {
            return Ok(BarrierReport::Drained {
                output_seq: self.output_seq(),
            });
        }

        state.requested = state.requested.saturating_add(1);
        let request = state.requested;
        let mut retry_cancel = true;
        loop {
            if state.acked >= request {
                return Ok(BarrierReport::Drained {
                    output_seq: state.acked_seq,
                });
            }
            if state.closed {
                return Ok(BarrierReport::Drained {
                    output_seq: self.output_seq(),
                });
            }

            if retry_cancel {
                retry_cancel = self.cancel_blocked_reader(&state);
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(ProcError::BarrierTimeout);
            }
            let wait = if retry_cancel {
                (deadline - now).min(CANCEL_RETRY_INTERVAL)
            } else {
                deadline - now
            };
            let (guard, _) = self
                .cv
                .wait_timeout(state, wait)
                .unwrap_or_else(PoisonError::into_inner);
            state = guard;
        }
    }

    /// 尝试取消读线程当前的同步 `ReadFile`。
    ///
    /// 返回 `true` 表示需要稍后重试：读线程已发布阻塞意图，但取消调用此刻未
    /// 命中（`ERROR_NOT_FOUND`，读线程刚好还没进入 `ReadFile`）。其它失败
    /// （线程已退出、无权打开）不再重试，由 `closed` 或超时收敛。
    fn cancel_blocked_reader(&self, state: &BarrierState) -> bool {
        let Some(tid) = state.reader_tid else {
            return false;
        };
        if !state.reader_blocking {
            // 未阻塞：读线程会在进入 `ReadFile` 前看到本次请求。
            return false;
        }
        match open_thread_and_cancel(tid) {
            Ok(()) => false,
            Err(err) if win32_code(&err) == ERROR_NOT_FOUND.0 => {
                debug!(thread_id = tid, "取消 ConPTY 读取未命中，稍后重试");
                true
            }
            Err(err) => {
                // 线程已退出或无权取消：等读线程自己收敛（closed 或超时）。
                debug!(thread_id = tid, error = %err, "取消 ConPTY 读取失败");
                false
            }
        }
    }

    /// 处理一批屏障请求：排空当前可读输出后确认。
    ///
    /// 只确认进入本函数时已投递的请求；期间新到的请求留到下一轮，保证每个
    /// 请求都对应一次“排空已可读输出”，不借用别人的排空结果。
    fn handle_requests(&self, output: &OwnedHandle, tx: &Sender<ProcEvent>) -> ReaderStep {
        let target = {
            let state = self.lock_state();
            if state.closed {
                return ReaderStep::Detached;
            }
            if state.requested <= state.acked {
                return ReaderStep::Continue;
            }
            state.requested
        };

        let outcome = drain_readable(output, tx, self);

        let mut state = self.lock_state();
        state.acked = state.acked.max(target);
        state.acked_seq = self.output_seq();
        if !matches!(outcome, DrainOutcome::Done) {
            state.closed = true;
        }
        self.cv.notify_all();
        outcome.into()
    }

    /// 标记读线程结束并唤醒所有等待者（幂等）。
    fn mark_closed(&self) {
        let mut state = self.lock_state();
        state.closed = true;
        state.reader_blocking = false;
        self.cv.notify_all();
    }

    /// 获取屏障状态锁；中毒时取回内部值（状态只有计数与标志，可继续使用）。
    fn lock_state(&self) -> MutexGuard<'_, BarrierState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl PtySession {
    /// 输出屏障句柄：与读线程共享同一控制块，不借用会话锁。
    pub fn barrier_handle(&self) -> BarrierHandle {
        BarrierHandle {
            inner: Arc::clone(&self.barrier),
        }
    }
}

impl PtySession {
    /// 在 ConPTY 中启动子进程并返回会话与事件接收端。
    ///
    /// # 参数
    /// - `cfg`：可执行程序、参数、工作目录、环境覆盖与初始窗口尺寸。
    ///
    /// # 错误
    /// 参数为空、管道/伪终端创建失败、`CreateProcessW` 失败或 Job 关联失败时
    /// 返回 [`ProcError`]；失败路径不会留下运行中的子进程。
    pub fn spawn(cfg: PtyConfig) -> Result<(Self, Receiver<ProcEvent>), ProcError> {
        if cfg.program.trim().is_empty() {
            return Err(ProcError::SpawnFailed {
                message: "program 不能为空".to_string(),
            });
        }

        let (pty_in, parent_in, parent_out, pty_out) = create_pipes()?;
        let console = create_pseudo_console(&cfg, &pty_in, &pty_out)?;
        // 伪控制台已 dup 控制台侧句柄；立即关闭本进程副本，
        // 否则输出管道永远等不到 EOF。
        drop(pty_in);
        drop(pty_out);

        let mut launch = ProcessLaunch {
            command_line: build_command_line(&cfg.program, &cfg.args),
            environment: cfg.env.as_ref().map(build_environment_block),
            current_dir: cfg.cwd.as_ref().map(|cwd| wide_null(cwd)),
        };
        let attributes = ProcThreadAttributes::new(console.raw())?;
        let (process, thread, pid) = create_process(&mut launch, &attributes)?;

        // 先加入 Job（KILL_ON_JOB_CLOSE），再恢复进程，避免退出竞态。
        let job = assign_job(process.get())?;
        if unsafe { ResumeThread(thread.get()) } == u32::MAX {
            let err = WinError::from_win32();
            return Err(ProcError::SpawnFailed {
                message: format!("恢复子进程失败: {err}"),
            });
        }
        drop(thread);
        drop(attributes);

        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let console: SharedConsole = Arc::new(Mutex::new(Some(console)));
        let process = Arc::new(process);
        let barrier = Arc::new(BarrierControl::default());

        let reader = std::thread::Builder::new()
            .name("termx-conpty-read".to_string())
            .spawn({
                let process = Arc::clone(&process);
                let barrier = Arc::clone(&barrier);
                move || read_loop(parent_out, process, tx, barrier)
            });
        if let Err(err) = reader {
            // job / console 随局部变量析构，KILL_ON_JOB_CLOSE 保证子进程被清理。
            return Err(ProcError::Io(err));
        }

        let watcher = std::thread::Builder::new()
            .name("termx-conpty-wait".to_string())
            .spawn({
                let process = Arc::clone(&process);
                let console = Arc::clone(&console);
                move || watch_exit(process, console)
            });
        if let Err(err) = watcher {
            // 读线程会因伪控制台关闭而退出；Job 析构会清理子进程。
            return Err(ProcError::Io(err));
        }

        let session = Self {
            pid,
            input: Some(parent_in),
            job: Some(job),
            hpc: console,
            barrier,
            terminated: false,
        };
        Ok((session, rx))
    }

    /// 根进程 ID。
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// 进程组 ID；Windows 无进程组语义，返回根进程 ID。
    pub fn pgid(&self) -> u32 {
        self.pid
    }

    /// 前台进程组 ID；Windows 无进程组语义，返回根进程 ID。
    pub fn foreground_pgrp(&self) -> Result<u32, ProcError> {
        Ok(self.pid)
    }

    /// 向终端输入写入字节。
    pub fn write(&mut self, data: &[u8]) -> Result<(), ProcError> {
        let input = self.input.as_ref().ok_or(ProcError::NotRunning)?;
        if data.is_empty() {
            return Ok(());
        }

        let mut written = 0u32;
        unsafe { WriteFile(input.get(), Some(data), Some(&mut written), None) }
            .map_err(|err| ProcError::Io(std::io::Error::other(format!("写入 PTY 失败: {err}"))))?;
        if written as usize != data.len() {
            return Err(ProcError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "写入 PTY 未写完",
            )));
        }
        Ok(())
    }

    /// 调整伪控制台窗口大小。
    pub fn resize(&mut self, cols: u16, rows: u16) -> Result<(), ProcError> {
        let console = lock_console(&self.hpc);
        let console = console.as_ref().ok_or(ProcError::NotRunning)?;
        unsafe { ResizePseudoConsole(console.raw(), coord(cols, rows)) }.map_err(|err| {
            ProcError::PtyFailed {
                message: format!("ResizePseudoConsole 失败: {err}"),
            }
        })
    }

    /// 向终端发送指定信号。
    ///
    /// Windows 无 Unix 信号语义：`SIGINT` 等价于向终端写入 Ctrl-C（`0x03`），
    /// `SIGTERM` / `SIGKILL` 等价于终止整个 Job Object；未知信号返回
    /// [`ProcError::UnsupportedSignal`]。
    pub fn signal(&mut self, name: &str) -> Result<(), ProcError> {
        let canonical =
            normalize_signal(name).ok_or_else(|| ProcError::UnsupportedSignal(name.to_string()))?;
        match canonical {
            "SIGINT" => self.write(&[CTRL_C_BYTE]),
            _ => self.terminate(),
        }
    }

    /// 终止整棵进程树并关闭伪控制台（幂等）。
    pub fn terminate(&mut self) -> Result<(), ProcError> {
        if self.terminated {
            return Ok(());
        }

        let result = match self.job.as_ref() {
            Some(job) => {
                unsafe { TerminateJobObject(job.get(), TERMINATE_EXIT_CODE) }.map_err(|err| {
                    ProcError::Io(std::io::Error::other(format!(
                        "TerminateJobObject 失败: {err}"
                    )))
                })
            }
            None => Ok(()),
        };

        // 即使杀 Job 失败也要关闭伪控制台，避免 conhost 残留。
        close_console(&self.hpc);
        self.input.take();
        self.terminated = true;
        result
    }

    /// 采集从根进程出发的进程树（ToolHelp 快照）。
    ///
    /// Windows 的父进程 ID 在父进程退出后保持不变，因此即使根进程已退出，
    /// 仍可按 `th32ParentProcessID` 链找回后代；`cmd` 使用 exe 名，
    /// `pgid` 固定为 0。
    pub fn tree(&self) -> Result<Vec<ProcNode>, ProcError> {
        tree_for(self.pid)
    }
}

/// 采集指定根进程的进程树；PTY 与 stdio 后端共用。
///
/// 根进程已退出时快照里没有它，但仍会展开其子节点：Windows 的
/// `th32ParentProcessID` 在父进程退出后保持不变。
pub(crate) fn tree_for(root_pid: u32) -> Result<Vec<ProcNode>, ProcError> {
    let processes = snapshot_processes()?;
    let by_pid: HashMap<u32, usize> = processes
        .iter()
        .enumerate()
        .map(|(index, proc_entry)| (proc_entry.pid, index))
        .collect();
    let mut children: HashMap<u32, Vec<usize>> = HashMap::new();
    for (index, proc_entry) in processes.iter().enumerate() {
        children.entry(proc_entry.ppid).or_default().push(index);
    }

    let mut nodes = Vec::new();
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(root_pid);
    while let Some(pid) = queue.pop_front() {
        if !visited.insert(pid) {
            continue;
        }
        if let Some(&index) = by_pid.get(&pid) {
            let proc_entry = &processes[index];
            nodes.push(ProcNode {
                pid: proc_entry.pid,
                ppid: proc_entry.ppid,
                pgid: 0,
                cmd: proc_entry.cmd.clone(),
            });
        }
        if let Some(kids) = children.get(&pid) {
            queue.extend(kids.iter().map(|&kid| processes[kid].pid));
        }
    }
    Ok(nodes)
}

/// 按 pid 终止单个进程（stdio 后端的**回退**路径）。
///
/// stdio 启动时已把子进程加入 `KILL_ON_JOB_CLOSE` 的 Job Object，正常终止走
/// `TerminateJobObject`（可清理整棵进程树）。本函数只用于 Job 创建/关联失败
/// 或终止失败时的降级：Windows 无 Unix 进程组语义，此时仅能终止根进程，
/// 后代可能残留。
pub(crate) fn terminate_process(pid: u32) -> Result<(), ProcError> {
    use windows::Win32::System::Threading::{OpenProcess, PROCESS_TERMINATE, TerminateProcess};

    let handle = unsafe { OpenProcess(PROCESS_TERMINATE, false, pid) }.map_err(|err| {
        ProcError::Io(std::io::Error::other(format!(
            "OpenProcess({pid}) 失败: {err}"
        )))
    })?;
    let handle = OwnedHandle::from_raw(handle);
    unsafe { TerminateProcess(handle.get(), TERMINATE_EXIT_CODE) }.map_err(|err| {
        ProcError::Io(std::io::Error::other(format!(
            "TerminateProcess({pid}) 失败: {err}"
        )))
    })
}

impl Drop for PtySession {
    fn drop(&mut self) {
        if !self.terminated {
            let _ = self.terminate();
        }
        // 关闭 Job 句柄触发 KILL_ON_JOB_CLOSE，兜底清理残余子进程。
        self.job.take();
    }
}

/// 拥有所有权的 Win32 句柄；析构时关闭，关闭失败只记录不 panic。
struct OwnedHandle(HANDLE);

// SAFETY: Win32 句柄可在任意线程使用；`Arc<OwnedHandle>` 保证引用计数归零前
// 不会析构，因此不存在“关闭后仍被其它线程使用”的竞争。
unsafe impl Send for OwnedHandle {}
// SAFETY: 共享的用法只有 `WaitForSingleObject` / `GetExitCodeProcess`，二者对
// 同一句柄的并发只读调用是线程安全的。
unsafe impl Sync for OwnedHandle {}

impl OwnedHandle {
    /// 接管一个有效句柄。
    fn from_raw(handle: HANDLE) -> Self {
        Self(handle)
    }

    /// 取得裸句柄值（不转移所有权）。
    fn get(&self) -> HANDLE {
        self.0
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if !self.0.is_invalid() {
            if let Err(err) = unsafe { CloseHandle(self.0) } {
                warn!(error = %err, "关闭 Win32 句柄失败");
            }
        }
    }
}

/// stdio 后端的 Job Object 句柄（`KILL_ON_JOB_CLOSE`）。
///
/// 与 PTY 共用 `assign_job` 的配置；stdio 只能按 pid 打开进程句柄后加入，
/// 若子进程已极快退出，加入失败由调用方降级为只终止根进程。
pub(crate) struct JobHandle {
    /// Job 句柄；析构即关闭并触发 `KILL_ON_JOB_CLOSE`。
    handle: OwnedHandle,
}

impl JobHandle {
    /// 创建带 `KILL_ON_JOB_CLOSE` 的 Job。
    pub(crate) fn create() -> Result<Self, ProcError> {
        let job = unsafe { CreateJobObjectW(None, PCWSTR::null()) }.map_err(|err| {
            ProcError::Internal {
                message: format!("CreateJobObjectW 失败: {err}"),
            }
        })?;
        let handle = OwnedHandle::from_raw(job);
        let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        unsafe {
            SetInformationJobObject(
                handle.get(),
                JobObjectExtendedLimitInformation,
                (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast::<c_void>(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        }
        .map_err(|err| ProcError::Internal {
            message: format!("SetInformationJobObject 失败: {err}"),
        })?;
        Ok(Self { handle })
    }

    /// 按 pid 打开进程并加入 Job。
    pub(crate) fn assign_pid(&self, pid: u32) -> Result<(), ProcError> {
        let process = unsafe { OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, false, pid) }
            .map_err(|err| ProcError::Internal {
                message: format!("OpenProcess({pid}) 失败: {err}"),
            })?;
        let process = OwnedHandle::from_raw(process);
        unsafe { AssignProcessToJobObject(self.handle.get(), process.get()) }.map_err(|err| {
            ProcError::Internal {
                message: format!("AssignProcessToJobObject({pid}) 失败: {err}"),
            }
        })
    }

    /// 终止 Job 内全部进程。
    pub(crate) fn terminate(&self) -> Result<(), ProcError> {
        unsafe { TerminateJobObject(self.handle.get(), TERMINATE_EXIT_CODE) }.map_err(|err| {
            ProcError::Io(std::io::Error::other(format!(
                "TerminateJobObject 失败: {err}"
            )))
        })
    }
}

/// 伪控制台句柄；析构时调用 `ClosePseudoConsole`。
struct PseudoConsole(HPCON);

impl PseudoConsole {
    /// 裸 HPCON。
    fn raw(&self) -> HPCON {
        self.0
    }
}

impl Drop for PseudoConsole {
    fn drop(&mut self) {
        unsafe { ClosePseudoConsole(self.0) };
    }
}

/// 获取共享伪控制台；互斥锁中毒时复用内部值，不 panic。
fn lock_console(console: &SharedConsole) -> MutexGuard<'_, Option<PseudoConsole>> {
    console.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 幂等关闭伪控制台：`take()` 后立即释放锁，`ClosePseudoConsole` 在锁外执行。
fn close_console(console: &SharedConsole) {
    let taken = lock_console(console).take();
    drop(taken);
}

/// `PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE` 的属性列表；析构时释放。
struct ProcThreadAttributes {
    /// 保持缓冲区存活（Windows 只保存指针）。
    _buffer: Vec<usize>,
    /// 属性列表首地址。
    list: LPPROC_THREAD_ATTRIBUTE_LIST,
}

impl ProcThreadAttributes {
    /// 查询所需大小、初始化并写入伪控制台属性。
    fn new(hpc: HPCON) -> Result<Self, ProcError> {
        let mut size = 0usize;
        // 第一次调用按文档必然返回 ERROR_INSUFFICIENT_BUFFER，仅用于取大小。
        let _ = unsafe { InitializeProcThreadAttributeList(None, 1, None, &mut size) };
        if size == 0 {
            return Err(ProcError::PtyFailed {
                message: "InitializeProcThreadAttributeList 未返回所需大小".to_string(),
            });
        }

        // 用 usize 分配以保证 PVOID 对齐。
        let words = size.div_ceil(size_of::<usize>());
        let mut buffer = vec![0usize; words];
        let list = LPPROC_THREAD_ATTRIBUTE_LIST(buffer.as_mut_ptr().cast::<c_void>());
        unsafe { InitializeProcThreadAttributeList(Some(list), 1, None, &mut size) }.map_err(
            |err| ProcError::PtyFailed {
                message: format!("InitializeProcThreadAttributeList 失败: {err}"),
            },
        )?;

        let attributes = Self {
            _buffer: buffer,
            list,
        };
        unsafe {
            UpdateProcThreadAttribute(
                list,
                0,
                PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
                // 注意：该属性的 lpValue 是 HPCON 值本身，不是指向 HPCON 的指针
                // （官方 ConPTY 示例与 Windows Terminal 均如此传递）。
                Some(hpc.0 as *const c_void),
                size_of::<HPCON>(),
                None,
                None,
            )
        }
        .map_err(|err| ProcError::PtyFailed {
            message: format!("UpdateProcThreadAttribute 失败: {err}"),
        })?;
        Ok(attributes)
    }
}

impl Drop for ProcThreadAttributes {
    fn drop(&mut self) {
        unsafe { DeleteProcThreadAttributeList(self.list) };
    }
}

/// 一次子进程启动所需的可变缓冲区。
struct ProcessLaunch {
    /// `CreateProcessW` 会改写命令行缓冲区，因此必须可写且以 NUL 结尾。
    command_line: Vec<u16>,
    /// UTF-16 环境块（双 NUL 结尾）；`None` 表示继承父环境。
    environment: Option<Vec<u16>>,
    /// UTF-16 工作目录；`None` 表示继承父进程当前目录。
    current_dir: Option<Vec<u16>>,
}

/// ToolHelp 快照中的一条进程记录。
struct ProcessEntry {
    /// 进程 ID。
    pid: u32,
    /// 父进程 ID。
    ppid: u32,
    /// 可执行文件名。
    cmd: String,
}

/// 创建两组不可继承管道。
///
/// 返回 `(pty_in, parent_in, parent_out, pty_out)`：
/// `pty_in` / `pty_out` 交给伪控制台，`parent_in` 用于写入，
/// `parent_out` 用于读取。
fn create_pipes() -> Result<(OwnedHandle, OwnedHandle, OwnedHandle, OwnedHandle), ProcError> {
    // 与 ConPTY 官方示例一致：不传 SECURITY_ATTRIBUTES（不可继承），
    // 子进程的控制台由 PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE 注入。
    let mut pty_in = HANDLE::default();
    let mut parent_in = HANDLE::default();
    unsafe { CreatePipe(&mut pty_in, &mut parent_in, None, 0) }.map_err(|err| {
        ProcError::PtyFailed {
            message: format!("CreatePipe（输入）失败: {err}"),
        }
    })?;
    let pty_in = OwnedHandle::from_raw(pty_in);
    let parent_in = OwnedHandle::from_raw(parent_in);

    let mut parent_out = HANDLE::default();
    let mut pty_out = HANDLE::default();
    unsafe { CreatePipe(&mut parent_out, &mut pty_out, None, 0) }.map_err(|err| {
        ProcError::PtyFailed {
            message: format!("CreatePipe（输出）失败: {err}"),
        }
    })?;
    let parent_out = OwnedHandle::from_raw(parent_out);
    let pty_out = OwnedHandle::from_raw(pty_out);

    Ok((pty_in, parent_in, parent_out, pty_out))
}

/// 用已创建的控制台侧管道创建伪控制台。
fn create_pseudo_console(
    cfg: &PtyConfig,
    pty_in: &OwnedHandle,
    pty_out: &OwnedHandle,
) -> Result<PseudoConsole, ProcError> {
    let hpc =
        unsafe { CreatePseudoConsole(coord(cfg.cols, cfg.rows), pty_in.get(), pty_out.get(), 0) }
            .map_err(|err| ProcError::PtyFailed {
            message: format!("CreatePseudoConsole 失败: {err}"),
        })?;
    Ok(PseudoConsole(hpc))
}

/// 以挂起方式创建子进程，返回 `(进程, 线程, pid)`。
fn create_process(
    launch: &mut ProcessLaunch,
    attributes: &ProcThreadAttributes,
) -> Result<(OwnedHandle, OwnedHandle, u32), ProcError> {
    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    // 显式清空标准句柄，避免子进程继承父进程被重定向的 stdout/stderr；
    // ConPTY 会为子进程提供控制台句柄（与 wezterm 的处理一致）。
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = INVALID_HANDLE_VALUE;
    startup.StartupInfo.hStdOutput = INVALID_HANDLE_VALUE;
    startup.StartupInfo.hStdError = INVALID_HANDLE_VALUE;
    startup.lpAttributeList = attributes.list;

    let mut info = PROCESS_INFORMATION::default();
    let environment = launch
        .environment
        .as_ref()
        .map(|block| block.as_ptr().cast::<c_void>());
    let current_dir = launch
        .current_dir
        .as_ref()
        .map(|cwd| PCWSTR::from_raw(cwd.as_ptr()))
        .unwrap_or(PCWSTR::null());
    let command_line = PWSTR::from_raw(launch.command_line.as_mut_ptr());

    unsafe {
        CreateProcessW(
            PCWSTR::null(),
            Some(command_line),
            None,
            None,
            // ConPTY 属性自行注入控制台句柄，不能继承父进程的其它句柄。
            false,
            EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT | CREATE_SUSPENDED,
            environment,
            current_dir,
            &startup.StartupInfo,
            &mut info,
        )
    }
    .map_err(|err| ProcError::SpawnFailed {
        message: format!("CreateProcessW 失败: {err}"),
    })?;

    Ok((
        OwnedHandle::from_raw(info.hProcess),
        OwnedHandle::from_raw(info.hThread),
        info.dwProcessId,
    ))
}

/// 创建 Job Object、设置 `KILL_ON_JOB_CLOSE` 并把子进程加入其中。
///
/// 失败时终止（仍挂起的）子进程，避免留下孤儿。
fn assign_job(process: HANDLE) -> Result<OwnedHandle, ProcError> {
    let job = match unsafe { CreateJobObjectW(None, PCWSTR::null()) } {
        Ok(job) => OwnedHandle::from_raw(job),
        Err(err) => {
            return Err(job_failure(
                process,
                format!("CreateJobObjectW 失败: {err}"),
            ));
        }
    };

    let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    if let Err(err) = unsafe {
        SetInformationJobObject(
            job.get(),
            JobObjectExtendedLimitInformation,
            (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast::<c_void>(),
            size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    } {
        return Err(job_failure(
            process,
            format!("SetInformationJobObject 失败: {err}"),
        ));
    }

    if let Err(err) = unsafe { AssignProcessToJobObject(job.get(), process) } {
        return Err(job_failure(
            process,
            format!("AssignProcessToJobObject 失败: {err}"),
        ));
    }
    Ok(job)
}

/// 构造 Job 相关失败：尽力终止挂起的子进程后再返回错误。
fn job_failure(process: HANDLE, message: String) -> ProcError {
    unsafe {
        let _ = TerminateProcess(process, TERMINATE_EXIT_CODE);
    }
    ProcError::SpawnFailed { message }
}

/// 退出监视线程：等子进程结束后留出 flush 宽限，再关闭伪控制台迫使读线程 EOF。
///
/// ConPTY 的输出管道不会在客户端退出时自动关闭；若不主动关闭 HPCON，读线程会
/// 永远阻塞在 `ReadFile` 上，`Exited` 也就永远发不出去。
fn watch_exit(process: Arc<OwnedHandle>, console: SharedConsole) {
    let _ = unsafe { WaitForSingleObject(process.get(), INFINITE) };
    // 给 conhost 一点时间把最后一段输出刷入管道（快速命令必需）。
    std::thread::sleep(EXIT_FLUSH_GRACE);
    close_console(&console);
}

/// 后台读线程：阻塞读取输出直到 EOF，并响应输出屏障请求。
///
/// 屏障请求到来时用 `CancelSynchronousIo` 打断阻塞中的 `ReadFile`；读线程看到
/// `ERROR_OPERATION_ABORTED` 后回到循环顶部，先排空已可读输出并确认，再继续
/// 正常读取。所有退出路径（含提前返回与 panic）都会标记屏障关闭，等待方不会
/// 永久卡住。
fn read_loop(
    output: OwnedHandle,
    process: Arc<OwnedHandle>,
    tx: Sender<ProcEvent>,
    barrier: Arc<BarrierControl>,
) {
    // 任何退出路径都唤醒屏障等待者。
    let _closed_guard = ReaderGuard {
        barrier: Arc::clone(&barrier),
    };
    // SAFETY: GetCurrentThreadId 无参数、无副作用，返回本线程 ID。
    barrier.register_reader(unsafe { GetCurrentThreadId() });

    let mut buffer = vec![0u8; READ_BUFFER_SIZE];
    'read: loop {
        // 读线程可能被 terminate 之外的路径标记关闭：直接走退出流程。
        if barrier.is_closed() {
            break;
        }

        // 有待处理屏障请求：先排空当前可读输出，保证请求返回前 Output 已入队。
        while barrier.has_pending() {
            match barrier.handle_requests(&output, &tx) {
                ReaderStep::Continue => {}
                // 管道已结束：走 EOF 流程（等进程退出并发送 `Exited`）。
                ReaderStep::Eof => break 'read,
                // 接收端已丢弃：停止读取并让句柄随作用域关闭。
                ReaderStep::Detached => return,
            }
        }

        // 与请求方在同一把锁下发布阻塞意图：请求若已到达则不进入阻塞读，
        // 从而消除“取消早于 ReadFile 开始”的丢失唤醒。
        if !barrier.enter_blocking_read() {
            continue;
        }
        let mut read = 0u32;
        // SAFETY: `output` 是读线程独占的有效管道读端；`buffer` 是有效缓冲区，
        // `read` 指向栈上 u32。
        let result = unsafe { ReadFile(output.get(), Some(&mut buffer), Some(&mut read), None) };
        barrier.leave_blocking_read();

        match result {
            Ok(()) if read == 0 => break,
            Ok(()) => {
                if !barrier.send_output(&tx, &buffer[..read as usize]) {
                    // 接收端已丢弃：停止读取并让句柄随作用域关闭。
                    return;
                }
            }
            Err(err) => {
                // 屏障取消会打断阻塞读，这不是 EOF：回循环顶部处理请求。
                if win32_code(&err) == ERROR_OPERATION_ABORTED.0 {
                    continue;
                }
                // ERROR_BROKEN_PIPE / 管道关闭都按 EOF 处理。
                debug!(error = %err, "ConPTY 输出读取结束");
                break;
            }
        }
    }

    // 读端已 EOF：先唤醒屏障等待者，再等进程退出并发送唯一的 `Exited`。
    barrier.mark_closed();

    let _ = unsafe { WaitForSingleObject(process.get(), INFINITE) };
    let mut code = 0u32;
    if let Err(err) = unsafe { GetExitCodeProcess(process.get(), &mut code) } {
        debug!(error = %err, "GetExitCodeProcess 失败，按 0 处理");
    }
    let _ = tx.blocking_send(ProcEvent::Exited(code as i32));
}

/// 读线程退出守卫：正常结束、提前返回或 panic 都唤醒屏障等待者。
struct ReaderGuard {
    /// 与读线程共享的屏障控制块。
    barrier: Arc<BarrierControl>,
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        self.barrier.mark_closed();
    }
}

/// 读线程处理屏障请求后的动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReaderStep {
    /// 请求已确认，继续正常读取。
    Continue,
    /// 输出管道已结束，走 EOF 流程（等进程退出并发送 `Exited`）。
    Eof,
    /// 事件接收端已丢弃，直接结束读线程。
    Detached,
}

/// 排空当前可读输出的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainOutcome {
    /// 已排空（可能读不到任何字节）。
    Done,
    /// 输出管道已结束。
    Eof,
    /// 事件接收端已丢弃。
    Detached,
}

impl From<DrainOutcome> for ReaderStep {
    fn from(outcome: DrainOutcome) -> Self {
        match outcome {
            DrainOutcome::Done => Self::Continue,
            DrainOutcome::Eof => Self::Eof,
            DrainOutcome::Detached => Self::Detached,
        }
    }
}

/// 排空“当前已可读”的 ConPTY 输出并全部入队。
///
/// `PeekNamedPipe` 只查询可读字节数、不阻塞，也不引入时间窗口：读到 0 即返回，
/// 屏障之后才产生的输出仍由正常读取路径处理。
fn drain_readable(
    output: &OwnedHandle,
    tx: &Sender<ProcEvent>,
    barrier: &BarrierControl,
) -> DrainOutcome {
    let mut buffer = vec![0u8; READ_BUFFER_SIZE];
    loop {
        let Some(available) = available_bytes(output) else {
            return DrainOutcome::Eof;
        };
        if available == 0 {
            return DrainOutcome::Done;
        }
        let want = (available as usize).min(buffer.len());
        let mut read = 0u32;
        // SAFETY: `output` 是读线程独占的有效管道读端；`PeekNamedPipe` 已确认
        // 至少有 `available` 字节可读，因此本次 `ReadFile` 不会阻塞。
        match unsafe {
            ReadFile(
                output.get(),
                Some(&mut buffer[..want]),
                Some(&mut read),
                None,
            )
        } {
            Ok(()) if read == 0 => return DrainOutcome::Eof,
            Ok(()) => {
                if !barrier.send_output(tx, &buffer[..read as usize]) {
                    return DrainOutcome::Detached;
                }
            }
            Err(err) => {
                debug!(error = %err, "排空 ConPTY 输出失败");
                return DrainOutcome::Eof;
            }
        }
    }
}

/// `PeekNamedPipe`：返回当前可读字节数；查询失败返回 `None`。
fn available_bytes(output: &OwnedHandle) -> Option<u32> {
    let mut available = 0u32;
    // SAFETY: `output` 是有效管道读端；不提供缓冲，仅查询可读字节数。
    match unsafe { PeekNamedPipe(output.get(), None, 0, None, Some(&mut available), None) } {
        Ok(()) => Some(available),
        Err(err) => {
            debug!(error = %err, "PeekNamedPipe 失败");
            None
        }
    }
}

/// 打开读线程句柄并取消其当前同步 I/O。
///
/// 返回 `Err` 时调用方按“线程已退出/尚未进入系统调用”处理，不在此重试。
fn open_thread_and_cancel(tid: u32) -> windows::core::Result<()> {
    // SAFETY: `THREAD_TERMINATE` 是 CancelSynchronousIo 要求的访问权限；句柄只
    // 在本函数内使用，随 `OwnedHandle` 析构关闭。
    let thread = unsafe { OpenThread(THREAD_TERMINATE, false, tid) }?;
    let thread = OwnedHandle::from_raw(thread);
    unsafe { CancelSynchronousIo(thread.get()) }
}

/// 取 Win32 错误码。
///
/// `windows` 把 `GetLastError` 结果编码为 `HRESULT_FROM_WIN32`（`0x8007_xxxx`），
/// 低 16 位即原始错误码。
fn win32_code(error: &WinError) -> u32 {
    error.code().0 as u32 & 0xFFFF
}

/// 采集当前系统全部进程（ToolHelp）。
fn snapshot_processes() -> Result<Vec<ProcessEntry>, ProcError> {
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) }.map_err(|err| {
        ProcError::Internal {
            message: format!("CreateToolhelp32Snapshot 失败: {err}"),
        }
    })?;
    let snapshot = OwnedHandle::from_raw(snapshot);

    let mut entry = PROCESSENTRY32W {
        dwSize: size_of::<PROCESSENTRY32W>() as u32,
        ..unsafe { std::mem::zeroed() }
    };
    let mut processes = Vec::new();
    if unsafe { Process32FirstW(snapshot.get(), &mut entry) }.is_ok() {
        loop {
            processes.push(ProcessEntry {
                pid: entry.th32ProcessID,
                ppid: entry.th32ParentProcessID,
                cmd: decode_utf16_buffer(&entry.szExeFile),
            });
            if unsafe { Process32NextW(snapshot.get(), &mut entry) }.is_err() {
                break;
            }
        }
    }
    Ok(processes)
}

/// 把以 NUL 结尾的 UTF-16 定长缓冲区转换为 `String`。
fn decode_utf16_buffer(buffer: &[u16]) -> String {
    let len = buffer
        .iter()
        .position(|&unit| unit == 0)
        .unwrap_or(buffer.len());
    String::from_utf16_lossy(&buffer[..len])
}

/// 依据父环境构造 UTF-16 环境块：父环境为基线，`overrides` 覆盖后按名排序。
fn build_environment_block(overrides: &BTreeMap<String, String>) -> Vec<u16> {
    // key 为变量名的大写形式（Windows 环境变量名不区分大小写）。
    let mut entries: BTreeMap<String, String> = BTreeMap::new();
    for entry in parent_environment_entries() {
        if let Some((name, _)) = split_env_entry(&entry) {
            entries.insert(name.to_uppercase(), entry);
        }
    }
    for (name, value) in overrides {
        entries.insert(name.to_uppercase(), format!("{name}={value}"));
    }

    let mut block = Vec::new();
    for entry in entries.values() {
        block.extend(entry.encode_utf16());
        block.push(0);
    }
    // 环境块以额外的 NUL 结束（双 NUL）。
    block.push(0);
    block
}

/// 读取当前进程的父环境（不含结尾双 NUL）。
fn parent_environment_entries() -> Vec<String> {
    let raw = unsafe { GetEnvironmentStringsW() };
    if raw.is_null() {
        return Vec::new();
    }

    let mut len = 0usize;
    unsafe {
        while !(*raw.0.add(len) == 0 && *raw.0.add(len + 1) == 0) {
            len += 1;
        }
    }
    let wide = unsafe { std::slice::from_raw_parts(raw.0, len) };
    let entries: Vec<String> = wide
        .split(|&unit| unit == 0)
        .filter(|segment| !segment.is_empty())
        .map(String::from_utf16_lossy)
        .collect();
    unsafe {
        let _ = FreeEnvironmentStringsW(PCWSTR::from_raw(raw.0));
    }
    entries
}

/// 拆分 `NAME=VALUE`；兼容 `=C:=C:\path` 这类以 `=` 开头的特殊变量。
fn split_env_entry(entry: &str) -> Option<(&str, &str)> {
    if let Some(rest) = entry.strip_prefix('=') {
        let index = rest.find('=')?;
        // `entry` 前多一个 '='，因此名字结束位置是 `index + 1`。
        Some((&entry[..index + 1], &entry[index + 2..]))
    } else {
        let index = entry.find('=')?;
        Some((&entry[..index], &entry[index + 1..]))
    }
}

/// 把 `program` 与 `args` 拼成 CreateProcessW 命令行（UTF-16，NUL 结尾）。
fn build_command_line(program: &str, args: &[String]) -> Vec<u16> {
    let mut line = String::new();
    append_quoted(&mut line, program);
    for arg in args {
        line.push(' ');
        append_quoted(&mut line, arg);
    }
    wide_null(&line)
}

/// 按 Windows CRT / CommandLineToArgvW 规则追加一个参数。
///
/// 不经过 shell，且对空格、制表符、双引号与尾部反斜杠做正确转义，
/// 避免参数注入或拆分错误。
fn append_quoted(out: &mut String, arg: &str) {
    let needs_quotes = arg.is_empty()
        || arg
            .chars()
            .any(|ch| matches!(ch, ' ' | '\t' | '\n' | '\u{0B}' | '"'));
    if !needs_quotes {
        out.push_str(arg);
        return;
    }

    out.push('"');
    let mut backslashes = 0usize;
    for ch in arg.chars() {
        match ch {
            '\\' => backslashes += 1,
            '"' => {
                // 引号前的反斜杠需要双写，再补一个转义引号。
                out.extend(std::iter::repeat_n('\\', backslashes * 2 + 1));
                out.push('"');
                backslashes = 0;
            }
            _ => {
                out.extend(std::iter::repeat_n('\\', backslashes));
                out.push(ch);
                backslashes = 0;
            }
        }
    }
    // 结尾反斜杠位于闭合引号前，需要双写。
    out.extend(std::iter::repeat_n('\\', backslashes * 2));
    out.push('"');
}

/// 转成 UTF-16 并追加 NUL。
fn wide_null(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

/// 把列/行转换为 `COORD`；限制在 `i16` 可表示范围内。
fn coord(cols: u16, rows: u16) -> COORD {
    let max = i16::MAX as u16;
    COORD {
        X: cols.clamp(1, max) as i16,
        Y: rows.clamp(1, max) as i16,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_line_quotes_spaces_and_ampersand() {
        let args = vec!["/C".to_string(), "echo hi & exit 7".to_string()];
        let line = build_command_line("cmd.exe", &args);
        let line = String::from_utf16_lossy(&line[..line.len() - 1]);
        assert_eq!(line, r#"cmd.exe /C "echo hi & exit 7""#);
    }

    #[test]
    fn command_line_escapes_quotes_and_trailing_backslashes() {
        let mut line = String::new();
        append_quoted(&mut line, r#"a"b"#);
        assert_eq!(line, r#""a\"b""#);

        let mut line = String::new();
        append_quoted(&mut line, "a b\\");
        assert_eq!(line, r#""a b\\""#);
    }

    #[test]
    fn env_entry_splits_special_and_regular_names() {
        assert_eq!(split_env_entry("PATH=C:\\x"), Some(("PATH", "C:\\x")));
        assert_eq!(split_env_entry("=C:=C:\\work"), Some(("=C:", "C:\\work")));
        assert_eq!(split_env_entry("NO_EQUALS"), None);
    }

    /// 向管道写端写入全部字节（测试数据远小于管道缓冲，一次即可写完）。
    fn write_pipe(handle: &OwnedHandle, data: &[u8]) {
        let mut written = 0u32;
        unsafe { WriteFile(handle.get(), Some(data), Some(&mut written), None) }.expect("写入管道");
        assert_eq!(written as usize, data.len(), "管道写入不完整");
    }

    /// 创建一对独立的测试管道，返回 `(写端, 读端)`。
    fn test_pipe() -> (OwnedHandle, OwnedHandle) {
        let mut read = HANDLE::default();
        let mut write = HANDLE::default();
        unsafe { CreatePipe(&mut read, &mut write, None, 0) }.expect("创建测试管道失败");
        (OwnedHandle::from_raw(write), OwnedHandle::from_raw(read))
    }

    /// 读出事件通道中的全部输出字节（不等待）。
    fn collect_output(rx: &mut Receiver<ProcEvent>) -> Vec<u8> {
        let mut bytes = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let ProcEvent::Output { data, .. } = event {
                bytes.extend_from_slice(&data);
            }
        }
        bytes
    }

    #[test]
    fn drain_readable_queues_all_pending_pipe_bytes() {
        let (writer, reader) = test_pipe();

        let payload = b"BARRIER_MARK";
        write_pipe(&writer, payload);

        let (tx, mut rx) = mpsc::channel(8);
        let barrier = BarrierControl::default();
        assert_eq!(
            drain_readable(&reader, &tx, &barrier),
            DrainOutcome::Done,
            "排空应成功"
        );
        drop(tx);

        assert_eq!(barrier.output_seq(), 1, "每段入队输出对应一个序号");
        assert_eq!(collect_output(&mut rx), payload);
    }

    #[test]
    fn barrier_times_out_without_reader() {
        let handle = BarrierHandle {
            inner: Arc::new(BarrierControl::default()),
        };
        match handle.barrier(Duration::from_millis(50)) {
            Err(ProcError::BarrierTimeout) => {}
            other => panic!("无读线程时应返回 BarrierTimeout，实际 {other:?}"),
        }
    }

    #[test]
    fn barrier_returns_current_seq_after_reader_closed() {
        let inner = Arc::new(BarrierControl::default());
        inner.output_seq.store(3, Ordering::SeqCst);
        inner.mark_closed();

        let handle = BarrierHandle { inner };
        match handle.barrier(Duration::from_millis(50)) {
            Ok(BarrierReport::Drained { output_seq }) => assert_eq!(output_seq, 3),
            other => panic!("读线程结束后应直接返回当前序号，实际 {other:?}"),
        }
    }

    #[test]
    fn pending_request_is_visible_before_blocking_read() {
        let barrier = BarrierControl::default();
        barrier.register_reader(42);

        // 读线程先发布阻塞意图，随后请求方投递请求：取消目标可被找到。
        assert!(barrier.enter_blocking_read(), "无请求时应允许进入阻塞读");
        {
            let mut state = barrier.lock_state();
            state.requested = 1;
        }
        assert!(barrier.has_pending(), "请求应可见");
        {
            let state = barrier.lock_state();
            assert_eq!(state.reader_tid, Some(42));
            assert!(state.reader_blocking, "阻塞意图应已发布");
        }

        // 读线程返回后请求优先于再次阻塞。
        barrier.leave_blocking_read();
        assert!(
            !barrier.enter_blocking_read(),
            "有待处理请求时不应进入阻塞读"
        );
    }
}
