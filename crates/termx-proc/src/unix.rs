//! Unix/Linux PTY 后端：直接使用操作系统 PTY（`openpty` + `fork`）。
//!
//! 关键不变量：
//! - 子进程在 `fork` 后只调用异步信号安全的系统调用（`setsid` / `ioctl` /
//!   `dup2` / `chdir` / `execve`），所有字符串、参数指针与环境块都在 `fork`
//!   前准备好，避免在多线程父进程的子进程里分配内存；
//! - exec 状态通过带 `FD_CLOEXEC` 的管道回传：exec 成功时管道随映像替换自动
//!   关闭（父进程读到 EOF），失败时子进程写入 errno 并以 127 退出，因此
//!   `spawn` 能同步返回 [`ProcError::SpawnFailed`]，不会留下“已启动但不可用”
//!   的会话；
//! - 子进程 `setsid()` 后成为新会话首进程，进程组 ID 等于自身 PID，随后
//!   通过 `TIOCSCTTY` 把从端绑定为控制终端；`terminate` 只对该进程组发信号，
//!   且显式排除调用者自身的进程组；
//! - master 端由会话持有，读线程持有 `dup` 出来的副本：`terminate` 关闭会话
//!   副本不会影响读线程，子进程全部退出后 master 读取返回 `EIO`/0，读线程
//!   自然结束；
//! - 退出事件由 wait 线程在 `waitpid` 成功后、读线程排空输出后发送一次，
//!   保证 `Output` 事件先于终态事件。

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::{CString, c_char, c_int};
use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::pty::{Winsize, openpty};
use nix::sys::signal::{Signal, kill, killpg};
use nix::sys::termios::{LocalFlags, SetArg, tcgetattr, tcsetattr};
use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, Pid, fork, getpgrp};
use termx_protocol::ProcNode;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, warn};

use crate::{OutputStream, ProcError, ProcEvent, PtyConfig, normalize_signal};

/// 事件通道容量；读线程满时通过 `blocking_send` 施加背压。
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// 读线程单次 `read` 的最大字节数。
const READ_BUFFER_SIZE: usize = 8 * 1024;

/// `terminate` 发出 SIGTERM 后的宽限时长；超时仍存活则升级为 SIGKILL。
const TERMINATE_GRACE: Duration = Duration::from_millis(300);

/// `terminate` 轮询进程组是否存活的间隔。
const TERMINATE_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// `terminate` 等待 wait 线程回收子进程的上限。
const EXIT_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// wait 线程等待读线程排空输出的上限（防止后代进程持有从端导致永久阻塞）。
const READER_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// 进程树节点 `cmd` 的最大字符数（超出部分截断）。
const MAX_CMD_CHARS: usize = 200;

/// 未设置 `PATH` 时使用的默认搜索路径（与 `execvp` 的兜底一致）。
const DEFAULT_PATH: &str = "/usr/local/bin:/usr/bin:/bin";

/// Unix PTY 会话。
pub struct PtySession {
    /// 根进程 ID。
    pid: u32,
    /// 根进程所在进程组 ID（`setsid` 后等于 `pid`）。
    pgid: u32,
    /// 根进程创建的新会话 ID（`setsid` 后等于 `pid`）。
    ///
    /// 单独保存而不是在 `tree()` 里读 `/proc/<root>/stat`，这样根进程退出后
    /// 仍能找回同一会话的后台作业。
    session: u32,
    /// PTY 主端；`terminate` / `Drop` 时关闭。
    master: Option<OwnedFd>,
    /// wait 线程完成 `waitpid` 后置位，用于 `terminate` 等待回收。
    exit_gate: Arc<Gate>,
    /// `terminate` 是否已执行（幂等标记）。
    terminated: bool,
}

impl PtySession {
    /// 在 PTY 中启动子进程并返回会话与事件接收端。
    ///
    /// # 参数
    /// - `cfg`：可执行程序、参数、工作目录、环境覆盖与初始窗口尺寸。
    ///
    /// # 错误
    /// 参数为空、PTY/管道创建失败、`fork` 失败、exec 失败（如程序不存在）
    /// 或线程创建失败时返回 [`ProcError`]；失败路径会回收子进程，不留下孤儿。
    pub fn spawn(cfg: PtyConfig) -> Result<(Self, Receiver<ProcEvent>), ProcError> {
        if cfg.program.trim().is_empty() {
            return Err(ProcError::SpawnFailed {
                message: "program 不能为空".to_string(),
            });
        }

        let plan = ExecPlan::new(&cfg)?;
        let winsize = make_winsize(cfg.cols, cfg.rows);
        let pty = openpty(Some(&winsize), None).map_err(|err| ProcError::PtyFailed {
            message: format!("openpty 失败: {err}"),
        })?;

        // 只有 termx-pwsh 需要这项配置：它依赖 .NET Console 的 DSR（`ESC[6n`）
        // 光标查询，而前端应答若走行缓冲/回显会被当成用户输入。bash/cat/sh 等
        // 程序自行管理 termios，保持 PTY 默认值，避免影响它们的行编辑与回显。
        if is_termx_pwsh(&cfg.program) {
            configure_slave_terminal(&pty.slave)?;
        }

        let (status_read, status_write) =
            nix::unistd::pipe().map_err(|err| ProcError::PtyFailed {
                message: format!("创建 exec 状态管道失败: {err}"),
            })?;
        set_cloexec(status_read.as_raw_fd())?;
        set_cloexec(status_write.as_raw_fd())?;

        let handles = ChildFds {
            master: pty.master.as_raw_fd(),
            slave: pty.slave.as_raw_fd(),
            status_read: status_read.as_raw_fd(),
            status_write: status_write.as_raw_fd(),
        };

        // SAFETY: fork 前已准备好全部参数；子进程分支只调用异步信号安全的
        // 系统调用并用 `_exit` 结束，不会运行 Rust 析构或分配内存。
        let child = match unsafe { fork() } {
            Ok(ForkResult::Parent { child }) => child,
            Ok(ForkResult::Child) => child_exec(handles, &plan),
            Err(err) => {
                return Err(ProcError::SpawnFailed {
                    message: format!("fork 失败: {err}"),
                });
            }
        };

        drop(pty.slave);
        drop(status_write);

        match read_exec_status(status_read.as_raw_fd()) {
            Ok(None) => {}
            Ok(Some(errno)) => {
                let _ = waitpid(child, None);
                return Err(ProcError::SpawnFailed {
                    message: format!(
                        "启动 {} 失败: {}",
                        cfg.program,
                        io::Error::from_raw_os_error(errno)
                    ),
                });
            }
            Err(err) => {
                let _ = killpg(child, Signal::SIGKILL);
                let _ = waitpid(child, None);
                return Err(ProcError::Io(err));
            }
        }
        drop(status_read);

        let pgid = child.as_raw() as u32;
        let reader_gate = Gate::new();
        let exit_gate = Gate::new();
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

        let reader_master = match pty.master.try_clone() {
            Ok(master) => master,
            Err(err) => {
                let _ = killpg(child, Signal::SIGKILL);
                let _ = waitpid(child, None);
                return Err(ProcError::PtyFailed {
                    message: format!("复制 PTY 主端失败: {err}"),
                });
            }
        };

        let reader = std::thread::Builder::new()
            .name("termx-pty-read".to_string())
            .spawn({
                let tx = tx.clone();
                let gate = Arc::clone(&reader_gate);
                move || read_loop(reader_master, tx, gate)
            });
        if let Err(err) = reader {
            let _ = killpg(child, Signal::SIGKILL);
            let _ = waitpid(child, None);
            return Err(ProcError::Io(err));
        }

        let waiter = std::thread::Builder::new()
            .name("termx-pty-wait".to_string())
            .spawn({
                let gate = Arc::clone(&reader_gate);
                let exit_gate = Arc::clone(&exit_gate);
                move || wait_loop(child, tx, gate, exit_gate)
            });
        if let Err(err) = waiter {
            let _ = killpg(child, Signal::SIGKILL);
            let _ = waitpid(child, None);
            return Err(ProcError::Io(err));
        }

        let session = Self {
            pid: child.as_raw() as u32,
            pgid,
            // 子进程 `setsid()` 后会话 ID 等于自身 PID。
            session: pgid,
            master: Some(pty.master),
            exit_gate,
            terminated: false,
        };
        Ok((session, rx))
    }

    /// 根进程 ID。
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// 进程组 ID；子进程调用 `setsid` 后即为根进程 PID。
    pub fn pgid(&self) -> u32 {
        self.pgid
    }

    /// 终端前台进程组 ID（`tcgetpgrp(master)`）。
    ///
    /// # 错误
    /// 会话已关闭时返回 [`ProcError::NotRunning`]；终端无前台进程组时返回
    /// [`ProcError::PtyFailed`]。
    pub fn foreground_pgrp(&self) -> Result<u32, ProcError> {
        let master = self.master.as_ref().ok_or(ProcError::NotRunning)?;
        match nix::unistd::tcgetpgrp(master) {
            Ok(pgrp) => Ok(pgrp.as_raw() as u32),
            Err(err) => Err(ProcError::PtyFailed {
                message: format!("tcgetpgrp 失败: {err}"),
            }),
        }
    }

    /// 向终端输入写入字节；Ctrl-C 由调用方写入 `0x03`。
    pub fn write(&mut self, data: &[u8]) -> Result<(), ProcError> {
        if data.is_empty() {
            return Ok(());
        }
        let master = self.master.as_ref().ok_or(ProcError::NotRunning)?;
        let mut offset = 0usize;
        while offset < data.len() {
            match nix::unistd::write(master, &data[offset..]) {
                Ok(0) => {
                    return Err(ProcError::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "写入 PTY 未写完",
                    )));
                }
                Ok(written) => offset += written,
                Err(Errno::EINTR) => continue,
                Err(err) => return Err(ProcError::Io(io::Error::from(err))),
            }
        }
        Ok(())
    }

    /// 调整 PTY 窗口大小（`TIOCSWINSZ`）。
    pub fn resize(&mut self, cols: u16, rows: u16) -> Result<(), ProcError> {
        let master = self.master.as_ref().ok_or(ProcError::NotRunning)?;
        let winsize = make_winsize(cols, rows);
        // SAFETY: `master` 是有效的 PTY 主端 fd，`winsize` 指向栈上合法的
        // `struct winsize`，ioctl 只读取该结构。
        let ret = unsafe {
            libc::ioctl(
                master.as_raw_fd(),
                libc::TIOCSWINSZ as _,
                &winsize as *const Winsize,
            )
        };
        if ret < 0 {
            return Err(ProcError::PtyFailed {
                message: format!("TIOCSWINSZ 失败: {}", io::Error::last_os_error()),
            });
        }
        Ok(())
    }

    /// 向子进程组发送指定信号（`SIGINT` / `SIGTERM` / `SIGKILL`）。
    ///
    /// 只接受这三种信号及其 `INT` / `TERM` / `KILL` 别名；未知信号返回
    /// [`ProcError::UnsupportedSignal`]。根进程已被回收或进程组无效/与调用者
    /// 自身相同时拒绝发送，避免 PID 复用误杀。
    pub fn signal(&mut self, name: &str) -> Result<(), ProcError> {
        let canonical =
            normalize_signal(name).ok_or_else(|| ProcError::UnsupportedSignal(name.to_string()))?;
        let signal = match canonical {
            "SIGINT" => Signal::SIGINT,
            "SIGTERM" => Signal::SIGTERM,
            _ => Signal::SIGKILL,
        };

        if self.exit_gate.is_done() {
            return Err(ProcError::NotRunning);
        }
        let pgid = self.pgid;
        if pgid <= 1 || pgid == getpgrp().as_raw() as u32 {
            return Err(ProcError::Internal {
                message: "拒绝向无效或调用者自身进程组发送信号".to_string(),
            });
        }

        match killpg(Pid::from_raw(pgid as i32), signal) {
            Ok(()) => Ok(()),
            // 进程组已不存在：等价于信号已送达（幂等）。
            Err(Errno::ESRCH) => Ok(()),
            Err(err) => Err(ProcError::PtyFailed {
                message: format!("发送 {canonical} 失败: {err}"),
            }),
        }
    }

    /// 终止整个进程组并关闭主端（幂等）。
    ///
    /// 先向子进程组发送 `SIGTERM`，在 [`TERMINATE_GRACE`] 内轮询存活状态，
    /// 仍存活则升级为 `SIGKILL`。始终跳过调用者自身的进程组，避免误伤。
    pub fn terminate(&mut self) -> Result<(), ProcError> {
        if self.terminated {
            return Ok(());
        }
        self.terminated = true;

        // 根进程已被 wait 线程回收时不再按 pgid 发信号，避免 PID 复用误杀。
        if !self.exit_gate.is_done() {
            self.signal_group();
        }
        // 后台作业可能自建进程组但仍在同一会话内；按 /proc 会话关系补杀，
        // 避免 `killpg` 漏掉它们形成孤儿。
        self.signal_session_remainder();
        self.master.take();
        self.exit_gate.wait_done(EXIT_WAIT_TIMEOUT);
        Ok(())
    }

    /// 采集从根进程出发的进程树（`/proc/<pid>/stat`）。
    ///
    /// `cmd` 优先取 `/proc/<pid>/cmdline`，为空时回退到 `comm`，并做控制字符
    /// 清理与长度截断。除父进程链外，还会补入与根进程同一会话的后代，避免
    /// 孤儿被重新挂到 init 后丢失。
    pub fn tree(&self) -> Result<Vec<ProcNode>, ProcError> {
        tree_for(self.pid, self.session)
    }

    /// 向子进程组发 SIGTERM → 宽限轮询 → SIGKILL；不触碰自身进程组。
    fn signal_group(&self) {
        let pgid = self.pgid;
        if pgid <= 1 || pgid == getpgrp().as_raw() as u32 {
            debug!(pgid, "跳过终止：进程组无效或与调用者相同");
            return;
        }

        let pgrp = Pid::from_raw(pgid as i32);
        if let Err(err) = killpg(pgrp, Signal::SIGTERM) {
            if err != Errno::ESRCH {
                debug!(pgid, error = %err, "发送 SIGTERM 失败");
            }
        }

        let deadline = Instant::now() + TERMINATE_GRACE;
        let mut alive = group_alive(pgrp);
        while alive && Instant::now() < deadline {
            std::thread::sleep(TERMINATE_POLL_INTERVAL);
            alive = group_alive(pgrp);
        }
        if alive {
            if let Err(err) = killpg(pgrp, Signal::SIGKILL) {
                if err != Errno::ESRCH {
                    debug!(pgid, error = %err, "发送 SIGKILL 失败");
                }
            }
        }
    }

    /// 对同一会话内、但不在根进程组里的残留后代补发 SIGTERM → SIGKILL。
    ///
    /// 后台作业若自建进程组，`killpg(root_pgid)` 不会命中；这里按
    /// `/proc/<pid>/stat` 的 session 字段收敛，避免留下孤儿。
    fn signal_session_remainder(&self) {
        if self.session <= 1 {
            return;
        }

        let mut members = session_members(self.session);
        if members.is_empty() {
            return;
        }
        for pid in &members {
            signal_one(*pid, Signal::SIGTERM);
        }

        let deadline = Instant::now() + TERMINATE_GRACE;
        while !members.is_empty() && Instant::now() < deadline {
            std::thread::sleep(TERMINATE_POLL_INTERVAL);
            members = session_members(self.session);
        }
        for pid in members {
            signal_one(pid, Signal::SIGKILL);
        }
    }
}

/// 采集指定根进程的进程树；PTY 与 stdio 后端共用。
///
/// `root_session` 非 0 时额外补入同会话后代（PTY 的 `setsid` 会话）；stdio
/// 后端没有独立会话，传 0 只按父子链遍历，避免把无关进程算进树里。
pub(crate) fn tree_for(root_pid: u32, root_session: u32) -> Result<Vec<ProcNode>, ProcError> {
    let entries = match std::fs::read_dir("/proc") {
        Ok(entries) => entries,
        // 非 Linux 的 Unix（如 macOS）没有 /proc：返回空树而不是报错。
        Err(_) => return Ok(Vec::new()),
    };

    let mut stats: HashMap<u32, ProcStat> = HashMap::new();
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if let Some(stat) = read_proc_stat(pid) {
            stats.insert(pid, stat);
        }
    }

    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    for stat in stats.values() {
        children.entry(stat.ppid).or_default().push(stat.pid);
    }

    // 从根 PID 出发按父子关系遍历。根进程已退出时仍要展开它的子节点，
    // 因为子进程的 `ppid` 在父进程退出后不会改变。
    let mut nodes = Vec::new();
    let mut visited: HashSet<u32> = HashSet::new();
    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(root_pid);
    while let Some(pid) = queue.pop_front() {
        if !visited.insert(pid) {
            continue;
        }
        if let Some(stat) = stats.get(&pid) {
            nodes.push(to_node(stat));
        }
        if let Some(kids) = children.get(&pid) {
            queue.extend(kids.iter().copied());
        }
    }

    // 根进程退出后，后台作业可能已被重新挂到 init，但会话 ID 仍是根 PID；
    // 按会话补全才能覆盖这类后代。
    if root_session != 0 {
        for stat in stats.values() {
            if stat.session == root_session && visited.insert(stat.pid) {
                nodes.push(to_node(stat));
            }
        }
    }
    Ok(nodes)
}

/// 向单个进程发送信号；进程已不存在或无权时忽略。
fn signal_one(pid: u32, signal: Signal) {
    if pid <= 1 || pid == std::process::id() {
        return;
    }
    if let Err(err) = kill(Pid::from_raw(pid as i32), signal) {
        if err != Errno::ESRCH {
            debug!(pid, ?signal, error = %err, "向会话成员发送信号失败");
        }
    }
}

/// 返回指定会话内仍存活的非僵尸进程（不含调用者自身）。
fn session_members(session: u32) -> Vec<u32> {
    let mut members = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return members;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Some(stat) = read_proc_stat(pid) else {
            continue;
        };
        if stat.session == session && stat.state != 'Z' && stat.pid != std::process::id() {
            members.push(stat.pid);
        }
    }
    members
}

impl Drop for PtySession {
    fn drop(&mut self) {
        if !self.terminated {
            let _ = self.terminate();
        }
        // 幂等关闭主端，避免 fd 泄漏。
        self.master.take();
    }
}

/// 一次性闸门：由线程置位，其它线程可带超时等待。
struct Gate {
    /// 是否已完成。
    done: Mutex<bool>,
    /// 状态变化通知。
    cv: Condvar,
}

impl Gate {
    /// 创建未置位的闸门。
    fn new() -> Arc<Self> {
        Arc::new(Self {
            done: Mutex::new(false),
            cv: Condvar::new(),
        })
    }

    /// 置位并唤醒所有等待者（幂等）。
    fn mark_done(&self) {
        let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        *done = true;
        self.cv.notify_all();
    }

    /// 是否已置位。
    fn is_done(&self) -> bool {
        *self.done.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 等待置位，超时返回当前状态。
    fn wait_done(&self, timeout: Duration) -> bool {
        let done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        let (done, _) = self
            .cv
            .wait_timeout_while(done, timeout, |flag| !*flag)
            .unwrap_or_else(PoisonError::into_inner);
        *done
    }
}

/// fork 后子进程使用的原始 fd 集合。
struct ChildFds {
    /// PTY 主端（子进程侧关闭）。
    master: RawFd,
    /// PTY 从端（绑定为 0/1/2）。
    slave: RawFd,
    /// exec 状态管道读端（子进程侧关闭）。
    status_read: RawFd,
    /// exec 状态管道写端（exec 失败时写入 errno）。
    status_write: RawFd,
}

/// 一次 exec 所需的全部数据；`fork` 前构造，避免子进程分配内存。
struct ExecPlan {
    /// 已解析的可执行文件路径。
    path: CString,
    /// `argv` 字符串（保持存活）。
    _argv: Vec<CString>,
    /// 环境变量条目（保持存活）。
    _envp: Vec<CString>,
    /// 工作目录；`None` 表示继承父进程。
    cwd: Option<CString>,
    /// `argv` 指针数组（以 NULL 结尾）。
    argv_ptrs: Vec<*const c_char>,
    /// `envp` 指针数组（以 NULL 结尾）。
    envp_ptrs: Vec<*const c_char>,
}

impl ExecPlan {
    /// 解析程序路径、构造 argv/envp 与工作目录。
    fn new(cfg: &PtyConfig) -> Result<Self, ProcError> {
        let env_entries = collect_env(cfg.env.as_ref());
        let path_env =
            lookup_path(&env_entries).unwrap_or_else(|| DEFAULT_PATH.as_bytes().to_vec());

        let program = CString::new(cfg.program.as_str()).map_err(|_| ProcError::SpawnFailed {
            message: "program 含 NUL 字节".to_string(),
        })?;
        let executable = if cfg.program.contains('/') {
            program.clone()
        } else {
            resolve_in_path(&cfg.program, &path_env)?
        };

        let mut argv = Vec::with_capacity(cfg.args.len() + 1);
        argv.push(program);
        for arg in &cfg.args {
            argv.push(
                CString::new(arg.as_str()).map_err(|_| ProcError::SpawnFailed {
                    message: "参数含 NUL 字节".to_string(),
                })?,
            );
        }

        let envp: Vec<CString> = env_entries
            .values()
            .filter(|entry| !entry.is_empty())
            .map(|entry| CString::new(entry.as_slice()).unwrap_or_default())
            .collect();

        let cwd = cfg
            .cwd
            .as_ref()
            .map(|dir| {
                CString::new(dir.as_str()).map_err(|_| ProcError::SpawnFailed {
                    message: "工作目录含 NUL 字节".to_string(),
                })
            })
            .transpose()?;

        let argv_ptrs = argv
            .iter()
            .map(|arg| arg.as_ptr())
            .chain(std::iter::once(std::ptr::null()))
            .collect();
        let envp_ptrs = envp
            .iter()
            .map(|entry| entry.as_ptr())
            .chain(std::iter::once(std::ptr::null()))
            .collect();

        Ok(Self {
            path: executable,
            _argv: argv,
            _envp: envp,
            cwd,
            argv_ptrs,
            envp_ptrs,
        })
    }
}

/// 子进程执行体：建立会话/控制终端、重定向标准 fd、chdir、exec。
///
/// 任何一步失败都把 errno 写回状态管道，然后以 127 退出；本函数不返回。
fn child_exec(fds: ChildFds, plan: &ExecPlan) -> ! {
    // SAFETY: 以下调用均为异步信号安全的系统调用；fd 与指针在 fork 前已就绪，
    // 失败路径直接 `_exit`，不会运行 Rust 析构函数。
    unsafe {
        libc::close(fds.status_read);
        libc::close(fds.master);

        // 标准 fd 在父进程可能被关闭，此时管道端点可能落在 0..=2；先复制到
        // 高位 fd，避免随后的 dup2 覆盖它导致父进程误判 exec 成功。
        let mut status_write = fds.status_write;
        if status_write <= 2 {
            let dup = libc::fcntl(status_write, libc::F_DUPFD_CLOEXEC, 10);
            if dup < 0 {
                child_fail(status_write, io::Error::last_os_error());
            }
            status_write = dup;
        }

        if libc::setsid() < 0 {
            child_fail(status_write, io::Error::last_os_error());
        }
        // 把从端绑定为当前会话的控制终端。
        if libc::ioctl(fds.slave, libc::TIOCSCTTY as _, 0) < 0 {
            child_fail(status_write, io::Error::last_os_error());
        }
        for target in 0..=2 {
            if libc::dup2(fds.slave, target) < 0 {
                child_fail(status_write, io::Error::last_os_error());
            }
        }
        if fds.slave > 2 {
            libc::close(fds.slave);
        }

        // 关闭父进程继承的其它 fd（只保留 0..=2 与 exec 状态管道），并恢复
        // 中断相关信号默认行为，避免子进程继承父进程的 SIG_IGN / 屏蔽字。
        close_inherited_fds(status_write);
        reset_child_signals();

        if let Some(cwd) = plan.cwd.as_ref() {
            if libc::chdir(cwd.as_ptr()) < 0 {
                child_fail(status_write, io::Error::last_os_error());
            }
        }

        libc::execve(
            plan.path.as_ptr(),
            plan.argv_ptrs.as_ptr(),
            plan.envp_ptrs.as_ptr(),
        );
        child_fail(status_write, io::Error::last_os_error())
    }
}

/// 关闭子进程从父进程继承的多余 fd，保留标准 fd 与 `keep`。
///
/// 优先使用 Linux `close_range` 系统调用（无需分配、适合 fork 后调用）；
/// 内核不支持时回退到按 `RLIMIT_NOFILE` 逐个 `close`。
///
/// # Safety
/// 只能在 fork 后的子进程中调用；`keep` 必须是仍打开且需要保留的 fd。
unsafe fn close_inherited_fds(keep: RawFd) {
    unsafe {
        #[cfg(target_os = "linux")]
        {
            if close_range_around(keep) {
                return;
            }
        }

        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        let max = if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            // 防止异常巨大上限导致长时间循环；超出部分通常并不存在。
            limit.rlim_cur.min(1_048_576) as RawFd
        } else {
            1024
        };
        let mut fd = 3;
        while fd < max {
            if fd != keep {
                libc::close(fd);
            }
            fd += 1;
        }
    }
}

/// 用 `close_range` 关闭 `keep` 两侧的 fd；两个区间都成功时返回 `true`。
///
/// # Safety
/// 只能在 fork 后的子进程中调用。
#[cfg(target_os = "linux")]
unsafe fn close_range_around(keep: RawFd) -> bool {
    unsafe {
        if keep > 3 && libc::syscall(libc::SYS_close_range, 3u32, (keep - 1) as u32, 0u32) != 0 {
            return false;
        }
        let first = (keep as u32).saturating_add(1);
        libc::syscall(libc::SYS_close_range, first, u32::MAX, 0u32) == 0
    }
}

/// 恢复子进程的 SIGPIPE / SIGINT 默认行为并清空继承的信号屏蔽字。
///
/// # Safety
/// 只能在 fork 后的子进程中调用；仅使用异步信号安全的 libc 接口。
unsafe fn reset_child_signals() {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
        libc::signal(libc::SIGINT, libc::SIG_DFL);

        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigprocmask(libc::SIG_SETMASK, &set, std::ptr::null_mut());
    }
}

/// 把 errno 写入状态管道后以 127 退出（不返回）。
fn child_fail(status_fd: RawFd, err: io::Error) -> ! {
    let errno = err.raw_os_error().unwrap_or(libc::EIO) as c_int;
    let bytes = errno.to_ne_bytes();
    // SAFETY: `bytes` 是栈上有效缓冲区，fd 是仍打开的管道写端；写失败无补救手段。
    unsafe {
        let _ = libc::write(status_fd, bytes.as_ptr().cast(), bytes.len());
        libc::_exit(127)
    }
}

/// 读取 exec 状态：`Ok(None)` 表示 exec 成功（EOF），`Ok(Some(errno))` 表示失败。
fn read_exec_status(fd: RawFd) -> io::Result<Option<i32>> {
    let mut buffer = [0u8; std::mem::size_of::<i32>()];
    let mut filled = 0usize;
    while filled < buffer.len() {
        // SAFETY: `fd` 在本次调用期间保持打开，缓冲区与长度合法。
        let read = unsafe {
            libc::read(
                fd,
                buffer[filled..].as_mut_ptr().cast(),
                buffer.len() - filled,
            )
        };
        if read == 0 {
            return if filled == 0 {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "exec 状态管道提前关闭",
                ))
            };
        }
        if read < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        filled += read as usize;
    }
    Ok(Some(i32::from_ne_bytes(buffer)))
}

/// 后台读线程：阻塞读取输出直到 EOF（`EIO`/0），然后标记读线程完成。
fn read_loop(master: OwnedFd, tx: Sender<ProcEvent>, gate: Arc<Gate>) {
    let mut buffer = vec![0u8; READ_BUFFER_SIZE];
    loop {
        match nix::unistd::read(&master, &mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                let chunk = buffer[..read].to_vec();
                let event = ProcEvent::Output {
                    data: chunk,
                    stream: OutputStream::Pty,
                };
                if tx.blocking_send(event).is_err() {
                    // 接收端已丢弃：停止读取并让 fd 随作用域关闭。
                    break;
                }
            }
            Err(Errno::EINTR) => continue,
            Err(Errno::EIO) => break,
            Err(err) => {
                debug!(error = %err, "PTY 输出读取结束");
                break;
            }
        }
    }
    gate.mark_done();
}

/// 后台 wait 线程：回收子进程，等读线程排空后发送唯一的终态事件。
fn wait_loop(child: Pid, tx: Sender<ProcEvent>, reader_gate: Arc<Gate>, exit_gate: Arc<Gate>) {
    let event = match waitpid(child, None) {
        Ok(WaitStatus::Exited(_, code)) => ProcEvent::Exited(code),
        Ok(WaitStatus::Signaled(_, signal, _)) => ProcEvent::Signaled(signal as i32),
        Ok(other) => {
            debug!(status = ?other, "非预期的 wait 状态，按退出码 -1 处理");
            ProcEvent::Exited(-1)
        }
        Err(err) => {
            warn!(error = %err, "waitpid 失败，按退出码 -1 处理");
            ProcEvent::Exited(-1)
        }
    };

    reader_gate.wait_done(READER_DRAIN_TIMEOUT);
    exit_gate.mark_done();
    let _ = tx.blocking_send(event);
}

/// 进程组内是否仍有进程（`killpg(pgrp, 0)`）。
fn group_alive(pgrp: Pid) -> bool {
    match killpg(pgrp, None::<Signal>) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        // EPERM 表示进程组存在但无权限，按存活处理。
        Err(_) => true,
    }
}

/// 对已打开的 fd 设置 `FD_CLOEXEC`。
fn set_cloexec(fd: RawFd) -> Result<(), ProcError> {
    // SAFETY: `fd` 有效；F_SETFD 只修改描述符标志。
    let ret = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    if ret < 0 {
        return Err(ProcError::PtyFailed {
            message: format!("设置 FD_CLOEXEC 失败: {}", io::Error::last_os_error()),
        });
    }
    Ok(())
}

/// 判断程序是否为 termx-pwsh（只比较 basename，允许绝对路径）。
///
/// 该配置只对 termx-pwsh 生效，不能改其他程序（bash/cat/sh）的 termios：
/// 它们自己会在交互模式下配置行缓冲与回显。
fn is_termx_pwsh(program: &str) -> bool {
    std::path::Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "termx-pwsh")
}

/// 关闭 PTY 从端的行缓冲与回显，保留信号和输出处理。
///
/// 只清除输入侧语义（`ECHO` / `ECHONL` / `ICANON`）：前端按 DSR（`ESC[6n`）
/// 返回的 `ESC[row;colR` 若仍回显，就会被行编辑器当成用户键入（表现为
/// `lsls`、`1R6`）；关闭 `ICANON` 同时让该应答无需换行即可被读到。`ISIG`
/// 与 `OPOST`（含 `ONLCR`）保持不变，故 Ctrl-C 与换行输出不受影响——这里
/// 不能用 `cfmakeraw`，它会连 `OPOST` 一并清掉。
///
/// termios 属于终端设备本身而非单个 fd，fork 前设置在从端上即可让子进程
/// `dup2` 得到的 0/1/2 继承同一份配置。
///
/// # 参数
/// - `slave`：`openpty` 返回、尚未 fork 的从端句柄。
///
/// # 错误
/// `tcgetattr` / `tcsetattr` 失败时返回 [`ProcError::PtyFailed`]。
fn configure_slave_terminal(slave: impl AsFd) -> Result<(), ProcError> {
    let mut attrs = tcgetattr(&slave).map_err(|err| ProcError::PtyFailed {
        message: format!("读取 PTY 从端 termios 失败: {err}"),
    })?;
    attrs
        .local_flags
        .remove(LocalFlags::ECHO | LocalFlags::ECHONL | LocalFlags::ICANON);
    tcsetattr(&slave, SetArg::TCSANOW, &attrs).map_err(|err| ProcError::PtyFailed {
        message: format!("设置 PTY 从端 termios 失败: {err}"),
    })?;
    Ok(())
}

/// 把列/行转换为 `winsize`，限制在 1..=u16::MAX。
fn make_winsize(cols: u16, rows: u16) -> Winsize {
    Winsize {
        ws_row: rows.max(1),
        ws_col: cols.max(1),
        ws_xpixel: 0,
        ws_ypixel: 0,
    }
}

/// 以父环境为基线、应用覆盖项后按变量名排序，返回完整的 `NAME=VALUE` 字节串。
fn collect_env(overrides: Option<&BTreeMap<String, String>>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut entries: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (name, value) in std::env::vars_os() {
        let name = name.as_bytes().to_vec();
        let mut entry = name.clone();
        entry.push(b'=');
        entry.extend_from_slice(value.as_bytes());
        entries.insert(name, entry);
    }
    if let Some(overrides) = overrides {
        for (name, value) in overrides {
            let name_bytes = name.as_bytes().to_vec();
            let mut entry = name_bytes.clone();
            entry.push(b'=');
            entry.extend_from_slice(value.as_bytes());
            entries.insert(name_bytes, entry);
        }
    }
    entries
}

/// 从环境条目中取出 `PATH` 的值。
fn lookup_path(entries: &BTreeMap<Vec<u8>, Vec<u8>>) -> Option<Vec<u8>> {
    entries
        .get(b"PATH".as_slice())
        .and_then(|entry| entry.strip_prefix(b"PATH="))
        .map(|value| value.to_vec())
}

/// 在 `PATH` 中查找可执行文件；找不到时返回 `SpawnFailed`。
fn resolve_in_path(program: &str, path_env: &[u8]) -> Result<CString, ProcError> {
    let program_bytes = program.as_bytes();
    for dir in path_env.split(|&byte| byte == b':') {
        let mut candidate = if dir.is_empty() {
            Vec::new()
        } else {
            let mut dir_bytes = dir.to_vec();
            dir_bytes.push(b'/');
            dir_bytes
        };
        candidate.extend_from_slice(program_bytes);
        let Ok(candidate_c) = CString::new(candidate) else {
            continue;
        };
        // SAFETY: `candidate_c` 是合法的 NUL 结尾路径；access 只读该字符串。
        if unsafe { libc::access(candidate_c.as_ptr(), libc::X_OK) } == 0 {
            return Ok(candidate_c);
        }
    }
    Err(ProcError::SpawnFailed {
        message: format!("在 PATH 中找不到可执行程序: {program}"),
    })
}

/// `/proc/<pid>/stat` 中与进程树相关的字段。
struct ProcStat {
    /// 进程 ID。
    pid: u32,
    /// 进程状态字符（`R`/`S`/`Z` 等）。
    state: char,
    /// 父进程 ID。
    ppid: u32,
    /// 进程组 ID。
    pgid: u32,
    /// 会话 ID。
    session: u32,
    /// 控制终端设备号（解析但不对外暴露）。
    _tty_nr: i64,
    /// 进程名（可能含空格/括号）。
    comm: String,
}

/// 解析单个 `/proc/<pid>/stat`；格式异常时返回 `None`。
fn read_proc_stat(pid: u32) -> Option<ProcStat> {
    let raw = std::fs::read(format!("/proc/{pid}/stat")).ok()?;
    // comm 位于第一对括号内，且可能包含空格与右括号，因此取最后一个 ')'。
    let open = raw.iter().position(|&byte| byte == b'(')?;
    let close = raw.iter().rposition(|&byte| byte == b')')?;
    if close <= open {
        return None;
    }
    let comm = String::from_utf8_lossy(&raw[open + 1..close]).into_owned();
    let rest = std::str::from_utf8(&raw[close + 1..]).ok()?;
    let mut fields = rest.split_whitespace();
    let state = fields.next()?.chars().next()?;
    let ppid = fields.next()?.parse::<u32>().ok()?;
    let pgid = fields.next()?.parse::<u32>().ok()?;
    let session = fields.next()?.parse::<u32>().ok()?;
    let _tty_nr = fields.next()?.parse::<i64>().ok()?;
    Some(ProcStat {
        pid,
        state,
        ppid,
        pgid,
        session,
        _tty_nr,
        comm,
    })
}

/// 把 `ProcStat` 转换为协议节点，并读取命令行。
fn to_node(stat: &ProcStat) -> ProcNode {
    ProcNode {
        pid: stat.pid,
        ppid: stat.ppid,
        pgid: stat.pgid,
        cmd: read_cmd(stat.pid, &stat.comm),
    }
}

/// 读取命令行（NUL 分隔），为空时回退到 `comm`，再做清理与截断。
fn read_cmd(pid: u32, comm: &str) -> String {
    let text = match std::fs::read(format!("/proc/{pid}/cmdline")) {
        Ok(raw) if !raw.is_empty() => {
            let replaced: Vec<u8> = raw
                .iter()
                .map(|&byte| if byte == 0 { b' ' } else { byte })
                .collect();
            String::from_utf8_lossy(&replaced).trim_end().to_string()
        }
        _ => comm.to_string(),
    };
    sanitize_cmd(&text)
}

/// 清理命令行：控制字符替换为空格，并按字符数截断。
fn sanitize_cmd(text: &str) -> String {
    let cleaned: String = text
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect();
    if cleaned.chars().count() > MAX_CMD_CHARS {
        let truncated: String = cleaned.chars().take(MAX_CMD_CHARS).collect();
        format!("{truncated}…")
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use nix::pty::openpty;
    use nix::sys::termios::{LocalFlags, OutputFlags, tcgetattr};

    use super::{configure_slave_terminal, is_termx_pwsh};

    #[test]
    fn termx_pwsh_detection_is_scoped_to_basename() {
        for matched in ["termx-pwsh", "/usr/local/bin/termx-pwsh", "bin/termx-pwsh"] {
            assert!(is_termx_pwsh(matched), "{matched} 应匹配 termx-pwsh");
        }

        for unmatched in [
            "pwsh",
            "/usr/bin/bash",
            "cat",
            "termx-pwsh-old",
            "termx-pwsh.exe",
        ] {
            assert!(!is_termx_pwsh(unmatched), "{unmatched} 不应匹配 termx-pwsh");
        }
    }

    #[test]
    fn slave_disables_echo_and_canonical_mode() {
        let pty = openpty(None, None).expect("openpty 失败");
        let before = tcgetattr(&pty.slave).expect("读取初始 termios 失败");
        assert!(
            before.local_flags.contains(LocalFlags::ECHO),
            "默认 PTY 从端应带回显，测试前提不成立"
        );

        configure_slave_terminal(&pty.slave).expect("配置从端失败");

        let after = tcgetattr(&pty.slave).expect("读取配置后 termios 失败");
        for flag in [LocalFlags::ECHO, LocalFlags::ECHONL, LocalFlags::ICANON] {
            assert!(!after.local_flags.contains(flag), "{flag:?} 应已关闭");
        }
        // Ctrl-C 仍由终端驱动转成 SIGINT。
        assert!(after.local_flags.contains(LocalFlags::ISIG), "ISIG 应保留");
        // 输出处理保持原样，换行仍经 ONLCR 转成 CRLF。
        assert!(
            after.output_flags.contains(OutputFlags::OPOST),
            "OPOST 应保留"
        );
        assert!(
            after.output_flags.contains(OutputFlags::ONLCR),
            "ONLCR 应保留"
        );
    }
}
