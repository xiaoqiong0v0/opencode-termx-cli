//! 跨平台管道 stdio 后端：`std::process::Command` + 独立 stdout/stderr 管道。
//!
//! 关键不变量：
//! - 不分配 TTY：`stdin/stdout/stderr` 全部为 `piped`，子进程看不到控制终端；
//! - 不经过 shell：`program` / `args` 原样交给 `Command`，由系统直接启动；
//! - stdout / stderr 各由一个读线程独占，输出事件带 [`OutputStream`] 区分来源；
//! - wait 线程负责回收子进程并发送**唯一**的 `Exited` / `Signaled`，发送前
//!   等待两个读线程排空（带超时），尽量保证输出事件先于终态事件；
//! - Unix 上子进程通过 `process_group(0)` 进入独立进程组，`terminate` 可
//!   `killpg` 清理整组；Windows 只能终止根子进程，后代清理限制见
//!   [`crate::windows::terminate_process`] 注释。

use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio},
    sync::{
        Arc, Condvar, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver as StdReceiver, SyncSender, TrySendError, sync_channel},
    },
    time::Duration,
};

#[cfg(windows)]
use std::ffi::OsString;
#[cfg(unix)]
use std::time::Instant;

use termx_protocol::ProcNode;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, warn};

use crate::{OutputStream, ProcError, ProcEvent, PtyConfig, normalize_signal};

/// 事件通道容量；读线程满时通过 `blocking_send` 施加背压。
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// 读线程单次 `read` 的最大字节数。
const READ_BUFFER_SIZE: usize = 8 * 1024;

/// stdout / stderr 读线程数量。
const READER_COUNT: usize = 2;

/// wait 线程等待读线程排空输出的上限。
const READER_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// `terminate` 发出 SIGTERM 后的宽限时长；超时仍存活则升级为 SIGKILL。
#[cfg(unix)]
const TERMINATE_GRACE: Duration = Duration::from_millis(300);

/// `terminate` 轮询进程组是否存活的间隔。
#[cfg(unix)]
const TERMINATE_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// 管道 stdio 会话。
///
/// stdin 由**有界写队列 + 专用 writer 线程**承载：write 只做非阻塞
/// `try_send`，绝不在调用方线程里持锁 `write_all`，避免慢子进程拖死 service。
/// 子进程句柄由 wait 线程独占，`terminate` 通过 pid / 进程组 / Job 杀进程。
pub struct StdioSession {
    /// 根子进程 ID。
    pid: u32,
    /// 子进程组 ID；Unix 上 process_group(0) 后等于 pid，Windows 无进程组
    /// 语义，同样返回 pid。
    pgid: u32,
    /// 写入队列发送端；`terminate` / `Drop` 时丢弃以结束 writer 线程并关闭 stdin。
    write_tx: Option<SyncSender<Vec<u8>>>,
    /// wait 线程是否已完成回收；用于避免向已退出进程发送信号。
    exited: Arc<AtomicBool>,
    /// Windows 上承载整棵进程树的 Job Object（KILL_ON_JOB_CLOSE）。
    #[cfg(windows)]
    job: Option<crate::windows::JobHandle>,
    /// `terminate` 是否已执行（幂等标记）。
    terminated: bool,
}

impl StdioSession {
    /// 启动管道 stdio 子进程并返回会话与事件接收端。
    ///
    /// 使用默认写入队列容量 [`crate::DEFAULT_STDIO_WRITE_QUEUE_CAPACITY`]。
    ///
    /// # 参数
    /// - `cfg`：可执行程序、参数、工作目录与环境覆盖；`cols` / `rows` 被忽略。
    ///
    /// # 错误
    /// `program` 为空、`Command::spawn` 失败或读/等待/writer 线程创建失败时
    /// 返回 [`ProcError`]；失败路径会终止并回收已启动的子进程。
    pub fn spawn(cfg: PtyConfig) -> Result<(Self, Receiver<ProcEvent>), ProcError> {
        Self::spawn_with_write_queue(cfg, crate::DEFAULT_STDIO_WRITE_QUEUE_CAPACITY)
    }

    /// 以指定 stdin 写队列容量启动管道 stdio 子进程。
    ///
    /// 队列容量至少为 1；`write` 队满时返回 [`ProcError::WriteQueueFull`]，
    /// 由上层映射为 `limit_exceeded`。
    pub fn spawn_with_write_queue(
        cfg: PtyConfig,
        write_queue_capacity: usize,
    ) -> Result<(Self, Receiver<ProcEvent>), ProcError> {
        if cfg.program.trim().is_empty() {
            return Err(ProcError::SpawnFailed {
                message: "program 不能为空".to_string(),
            });
        }

        let mut command = Command::new(&cfg.program);
        command
            .args(&cfg.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(cwd) = &cfg.cwd {
            command.current_dir(cwd);
        }
        apply_environment(&mut command, cfg.env.as_ref());
        // Unix 上让子进程进入独立进程组，terminate 可 killpg 清理整组；
        // 不调用 setsid，保持与会话/控制终端的原有关系。
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }

        let mut child = command.spawn().map_err(|error| ProcError::SpawnFailed {
            message: format!("启动 {} 失败: {error}", cfg.program),
        })?;
        let pid = child.id();
        let pipes = take_pipes(&mut child);
        let (stdin, stdout, stderr) = match pipes {
            Ok(pipes) => pipes,
            Err(error) => {
                // 管道配置异常时回收已启动的子进程，避免孤儿。
                abort_child(child);
                return Err(error);
            }
        };

        // Windows：把子进程放进 Job（KILL_ON_JOB_CLOSE），terminate 可清理后代。
        // 子进程可能极快退出导致 Assign 失败，这里只记录，不阻断启动。
        #[cfg(windows)]
        let job = match crate::windows::JobHandle::create() {
            Ok(job) => {
                if let Err(error) = job.assign_pid(pid) {
                    debug!(pid, %error, "stdio 子进程加入 Job 失败，将只终止根进程");
                }
                Some(job)
            }
            Err(error) => {
                debug!(pid, %error, "创建 stdio Job 失败，将只终止根进程");
                None
            }
        };

        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let gate = ReaderGate::new(READER_COUNT);
        let exited = Arc::new(AtomicBool::new(false));

        if let Err(error) = spawn_reader(stdout, OutputStream::Stdout, tx.clone(), &gate) {
            abort_child(child);
            return Err(error);
        }
        if let Err(error) = spawn_reader(stderr, OutputStream::Stderr, tx.clone(), &gate) {
            abort_child(child);
            return Err(error);
        }

        let waiter = std::thread::Builder::new()
            .name("termx-stdio-wait".to_string())
            .spawn({
                let gate = Arc::clone(&gate);
                let exited = Arc::clone(&exited);
                move || wait_child(child, tx, gate, exited)
            });
        if let Err(error) = waiter {
            abort_child_by_pid(pid);
            return Err(ProcError::Io(error));
        }

        let (write_tx, write_rx) = sync_channel::<Vec<u8>>(write_queue_capacity.max(1));
        if let Err(error) = spawn_writer(stdin, write_rx) {
            // 队列发送端随作用域丢弃；按 pid 终止已启动的子进程。
            drop(write_tx);
            abort_child_by_pid(pid);
            return Err(error);
        }

        let session = Self {
            pid,
            pgid: pid,
            write_tx: Some(write_tx),
            exited,
            #[cfg(windows)]
            job,
            terminated: false,
        };
        Ok((session, rx))
    }

    /// 根子进程 ID。
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// 进程组 ID；Unix 独立进程组与 Windows 无进程组语义时都等于 `pid`。
    pub fn pgid(&self) -> u32 {
        self.pgid
    }

    /// 前台进程组；stdio 没有 TTY，永远返回 `None`。
    pub fn foreground_pgrp(&self) -> Result<Option<u32>, ProcError> {
        Ok(None)
    }

    /// 向 stdin 有界队列投递一段字节；队列满时立即返回错误，不阻塞调用方。
    ///
    /// 实际 `write_all` + flush 由专用 writer 线程完成。
    pub fn write(&mut self, data: &[u8]) -> Result<(), ProcError> {
        if data.is_empty() {
            return Ok(());
        }
        let tx = self.write_tx.as_ref().ok_or(ProcError::NotRunning)?;
        match tx.try_send(data.to_vec()) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(ProcError::WriteQueueFull),
            Err(TrySendError::Disconnected(_)) => Err(ProcError::NotRunning),
        }
    }

    /// 调整窗口大小；stdio 没有 TTY，空操作成功。
    pub fn resize(&mut self, _cols: u16, _rows: u16) -> Result<(), ProcError> {
        Ok(())
    }

    /// 向子进程（Unix 为整个进程组）发送信号。
    ///
    /// Windows 无 Unix 信号语义，`SIGINT` / `SIGTERM` / `SIGKILL` 统一按
    /// [`StdioSession::terminate`] 处理；未知信号返回
    /// [`ProcError::UnsupportedSignal`]。
    pub fn signal(&mut self, name: &str) -> Result<(), ProcError> {
        let canonical =
            normalize_signal(name).ok_or_else(|| ProcError::UnsupportedSignal(name.to_string()))?;

        #[cfg(unix)]
        {
            if self.exited.load(Ordering::SeqCst) {
                return Err(ProcError::NotRunning);
            }
            let signal = match canonical {
                "SIGINT" => nix::sys::signal::Signal::SIGINT,
                "SIGTERM" => nix::sys::signal::Signal::SIGTERM,
                _ => nix::sys::signal::Signal::SIGKILL,
            };
            match nix::sys::signal::killpg(nix::unistd::Pid::from_raw(self.pgid as i32), signal) {
                Ok(()) => Ok(()),
                // 进程组已不存在：等价于信号已送达（幂等）。
                Err(nix::errno::Errno::ESRCH) => Ok(()),
                Err(error) => Err(ProcError::Io(io::Error::from(error))),
            }
        }

        #[cfg(windows)]
        {
            let _ = canonical;
            self.terminate()
        }
    }

    /// 终止子进程并关闭写入队列（幂等）。
    ///
    /// Unix 上先向独立进程组发 `SIGTERM`，宽限后升级为 `SIGKILL`；Windows 上
    /// 优先 `TerminateJobObject` 清理整棵进程树，无 Job 时回退到根进程。
    /// 关闭队列会让 writer 线程在写完当前块后退出并关闭 stdin。
    pub fn terminate(&mut self) -> Result<(), ProcError> {
        if self.terminated {
            return Ok(());
        }
        self.terminated = true;
        self.close_writer();

        #[cfg(unix)]
        {
            if self.exited.load(Ordering::SeqCst) {
                return Ok(());
            }
            let pgrp = nix::unistd::Pid::from_raw(self.pgid as i32);
            if let Err(error) = nix::sys::signal::killpg(pgrp, nix::sys::signal::Signal::SIGTERM) {
                if error != nix::errno::Errno::ESRCH {
                    debug!(pgid = self.pgid, %error, "stdio 发送 SIGTERM 失败");
                }
            }

            let deadline = Instant::now() + TERMINATE_GRACE;
            while !self.exited.load(Ordering::SeqCst)
                && group_alive(pgrp)
                && Instant::now() < deadline
            {
                std::thread::sleep(TERMINATE_POLL_INTERVAL);
            }
            if !self.exited.load(Ordering::SeqCst) && group_alive(pgrp) {
                if let Err(error) =
                    nix::sys::signal::killpg(pgrp, nix::sys::signal::Signal::SIGKILL)
                {
                    if error != nix::errno::Errno::ESRCH {
                        debug!(pgid = self.pgid, %error, "stdio 发送 SIGKILL 失败");
                    }
                }
            }
        }

        #[cfg(windows)]
        {
            let mut terminated_tree = false;
            let mut failure = None;
            if let Some(job) = self.job.take() {
                match job.terminate() {
                    Ok(()) => terminated_tree = true,
                    Err(error) => {
                        debug!(pid = self.pid, %error, "TerminateJobObject 失败");
                        failure = Some(error);
                    }
                }
                // 关闭 Job 句柄：KILL_ON_JOB_CLOSE 兜底清理残余后代。
                drop(job);
            }
            // Job 不可用或终止失败时退回只杀根进程；成功则清掉先前错误。
            if !terminated_tree && !self.exited.load(Ordering::SeqCst) {
                match crate::windows::terminate_process(self.pid) {
                    Ok(()) => failure = None,
                    Err(error) => {
                        debug!(pid = self.pid, %error, "stdio 终止子进程失败");
                        failure = failure.or(Some(error));
                    }
                }
            }
            // 进程已自然退出时不再上报错误：信号语义等价于已送达。
            if let Some(error) = failure {
                if !self.exited.load(Ordering::SeqCst) {
                    return Err(error);
                }
            }
        }

        Ok(())
    }

    /// 采集从根进程出发的进程树；复用平台实现。
    pub fn tree(&self) -> Result<Vec<ProcNode>, ProcError> {
        #[cfg(windows)]
        {
            crate::windows::tree_for(self.pid)
        }
        #[cfg(unix)]
        {
            // stdio 子进程没有独立会话，传 0 只按父子链遍历。
            crate::unix::tree_for(self.pid, 0)
        }
    }

    /// 关闭写入队列；writer 线程排空当前项后退出并关闭 stdin。
    fn close_writer(&mut self) {
        let taken = self.write_tx.take();
        drop(taken);
    }
}

impl Drop for StdioSession {
    fn drop(&mut self) {
        // 先终止子进程（Unix killpg / Windows Job），再关闭队列，确保 blocked
        // writer 不会因对端不读而无限等待，也不留下残留进程。
        if !self.terminated {
            let _ = self.terminate();
        } else {
            self.close_writer();
        }
    }
}

/// 取出 stdin/stdout/stderr 三个必需管道。
fn take_pipes(child: &mut Child) -> Result<(ChildStdin, ChildStdout, ChildStderr), ProcError> {
    let stdin = take_pipe(child.stdin.take(), "stdin")?;
    let stdout = take_pipe(child.stdout.take(), "stdout")?;
    let stderr = take_pipe(child.stderr.take(), "stderr")?;
    Ok((stdin, stdout, stderr))
}

/// 取出单个必需管道；缺失表示 `Stdio::piped()` 配置失效。
fn take_pipe<T>(pipe: Option<T>, name: &str) -> Result<T, ProcError> {
    pipe.ok_or_else(|| ProcError::Internal {
        message: format!("stdio {name} 管道未创建"),
    })
}

/// 启动一个读线程；失败时返回 I/O 错误。
fn spawn_reader<R>(
    reader: R,
    stream: OutputStream,
    tx: Sender<ProcEvent>,
    gate: &Arc<ReaderGate>,
) -> Result<(), ProcError>
where
    R: Read + Send + 'static,
{
    let gate = Arc::clone(gate);
    std::thread::Builder::new()
        .name(format!("termx-stdio-read-{stream:?}"))
        .spawn(move || read_stream(reader, stream, tx, gate))
        .map(|_| ())
        .map_err(ProcError::Io)
}

/// 启动 stdin writer 线程；失败时返回 I/O 错误。
fn spawn_writer(stdin: ChildStdin, write_rx: StdReceiver<Vec<u8>>) -> Result<(), ProcError> {
    std::thread::Builder::new()
        .name("termx-stdio-writer".to_string())
        .spawn(move || write_stream(stdin, write_rx))
        .map(|_| ())
        .map_err(ProcError::Io)
}

/// writer 线程：顺序消费有界队列并写 stdin；队列关闭或写入失败即退出。
fn write_stream(mut stdin: ChildStdin, write_rx: StdReceiver<Vec<u8>>) {
    while let Ok(data) = write_rx.recv() {
        if stdin.write_all(&data).and_then(|()| stdin.flush()).is_err() {
            debug!("stdio stdin 写入结束");
            break;
        }
    }
    // stdin 随函数返回关闭，触发子进程 EOF。
}

/// 读线程：阻塞读取直到 EOF，把每个分片转换为带流来源的 `Output` 事件。
fn read_stream<R>(mut reader: R, stream: OutputStream, tx: Sender<ProcEvent>, gate: Arc<ReaderGate>)
where
    R: Read,
{
    let mut buffer = vec![0u8; READ_BUFFER_SIZE];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                let event = ProcEvent::Output {
                    data: buffer[..read].to_vec(),
                    stream,
                };
                if tx.blocking_send(event).is_err() {
                    // 接收端已丢弃：停止读取并让管道随作用域关闭。
                    break;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                debug!(?stream, %error, "stdio 输出读取结束");
                break;
            }
        }
    }
    gate.done();
}

/// wait 线程：回收子进程，等待读线程排空后发送唯一的终态事件。
fn wait_child(
    mut child: Child,
    tx: Sender<ProcEvent>,
    gate: Arc<ReaderGate>,
    exited: Arc<AtomicBool>,
) {
    let status = child.wait();
    exited.store(true, Ordering::SeqCst);
    gate.wait_idle(READER_DRAIN_TIMEOUT);

    let event = match status {
        Ok(status) => terminal_event(&status),
        Err(error) => {
            warn!(%error, "stdio wait 失败，按退出码 -1 处理");
            ProcEvent::Exited(-1)
        }
    };
    let _ = tx.blocking_send(event);
}

/// 把 `ExitStatus` 转为终态事件：Unix 优先报告信号，Windows 报告退出码。
fn terminal_event(status: &ExitStatus) -> ProcEvent {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return ProcEvent::Signaled(signal);
        }
    }

    ProcEvent::Exited(status.code().unwrap_or(-1))
}

/// 启动失败时终止并回收子进程，避免遗留孤儿。
fn abort_child(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// 线程创建失败、`Child` 已移动时按 pid 尽力终止。
fn abort_child_by_pid(pid: u32) {
    #[cfg(windows)]
    {
        let _ = crate::windows::terminate_process(pid);
    }
    #[cfg(unix)]
    {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}

/// 进程组内是否仍有进程（`killpg(pgrp, 0)`）。
#[cfg(unix)]
fn group_alive(pgrp: nix::unistd::Pid) -> bool {
    match nix::sys::signal::killpg(pgrp, None::<nix::sys::signal::Signal>) {
        Ok(()) => true,
        Err(nix::errno::Errno::ESRCH) => false,
        // EPERM 表示进程组存在但无权限，按存活处理。
        Err(_) => true,
    }
}

/// 应用环境变量覆盖：父环境为基线，`overrides` 覆盖后启动子进程。
fn apply_environment(command: &mut Command, overrides: Option<&BTreeMap<String, String>>) {
    let Some(overrides) = overrides else {
        return;
    };

    #[cfg(windows)]
    {
        // Windows 环境变量名不区分大小写：先按大写名合并父环境，再写入覆盖项，
        // 避免出现仅大小写不同的重复变量。
        let mut entries: BTreeMap<String, (OsString, OsString)> = BTreeMap::new();
        for (name, value) in std::env::vars_os() {
            entries.insert(name.to_string_lossy().to_uppercase(), (name, value));
        }
        for (name, value) in overrides {
            entries.insert(
                name.to_uppercase(),
                (OsString::from(name), OsString::from(value)),
            );
        }
        command.env_clear();
        command.envs(entries.into_values());
    }

    #[cfg(not(windows))]
    {
        // Unix 变量名区分大小写，直接以父环境为基线追加覆盖项。
        command.envs(overrides);
    }
}

/// 一次性读线程闸门：两个读线程都结束后唤醒 wait 线程。
struct ReaderGate {
    /// 尚未结束的读线程数。
    remaining: Mutex<usize>,
    /// 归零通知。
    cv: Condvar,
}

impl ReaderGate {
    /// 创建带计数的闸门。
    fn new(remaining: usize) -> Arc<Self> {
        Arc::new(Self {
            remaining: Mutex::new(remaining),
            cv: Condvar::new(),
        })
    }

    /// 标记一个读线程结束（幂等，不会减到负数）。
    fn done(&self) {
        let mut remaining = self
            .remaining
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if *remaining > 0 {
            *remaining -= 1;
            if *remaining == 0 {
                self.cv.notify_all();
            }
        }
    }

    /// 等待所有读线程结束；超时返回当前状态，避免后代持有管道导致永久阻塞。
    fn wait_idle(&self, timeout: Duration) {
        let remaining = self
            .remaining
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let _ = self
            .cv
            .wait_timeout_while(remaining, timeout, |remaining| *remaining > 0)
            .unwrap_or_else(PoisonError::into_inner);
    }
}
