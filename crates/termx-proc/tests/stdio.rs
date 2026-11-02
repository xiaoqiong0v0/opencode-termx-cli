//! 管道 stdio 集成测试：独立 stdout/stderr、交互 stdin、退出码与终止。
//!
//! Windows 用 `cmd.exe /C`，Unix 用 `/bin/sh -c`；stdio 不分配 TTY，
//! 因此命令与测试都按管道语义编写。所有用例带超时，避免挂死。

#![cfg(any(windows, unix))]

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use termx_proc::{OutputStream, ProcError, ProcEvent, PtyConfig, StdioSession};
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, timeout};

/// 单次等待上限。
const TEST_TIMEOUT: Duration = Duration::from_secs(20);

/// 终态事件。
#[derive(Debug, PartialEq, Eq)]
enum Terminal {
    /// 正常退出码。
    Exited(i32),
    /// 终止信号编号。
    Signaled(i32),
}

/// 收集到的输出与终态。
struct Collected {
    /// stdout 管道内容。
    stdout: Vec<u8>,
    /// stderr 管道内容。
    stderr: Vec<u8>,
    /// 终态事件。
    terminal: Terminal,
}

/// 消费事件直到终态，并按 `stream` 分流输出。
async fn collect(rx: &mut Receiver<ProcEvent>) -> Collected {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let terminal = timeout(TEST_TIMEOUT, async {
        loop {
            match rx.recv().await {
                Some(ProcEvent::Output {
                    data,
                    stream: OutputStream::Stdout,
                }) => stdout.extend_from_slice(&data),
                Some(ProcEvent::Output {
                    data,
                    stream: OutputStream::Stderr,
                }) => stderr.extend_from_slice(&data),
                Some(ProcEvent::Output {
                    stream: OutputStream::Pty,
                    ..
                }) => panic!("stdio 不应产生 pty 流"),
                Some(ProcEvent::Exited(code)) => return Terminal::Exited(code),
                Some(ProcEvent::Signaled(signal)) => return Terminal::Signaled(signal),
                None => panic!("事件通道在终态前关闭"),
            }
        }
    })
    .await
    .expect("等待进程退出超时");

    Collected {
        stdout,
        stderr,
        terminal,
    }
}

/// 平台相关的 `cmd` 包装：Windows 用 `cmd.exe /C`，Unix 用 `/bin/sh -c`。
fn shell_config(script: &str) -> PtyConfig {
    #[cfg(windows)]
    let (program, args) = (
        "cmd.exe".to_string(),
        vec!["/C".to_string(), script.to_string()],
    );
    #[cfg(unix)]
    let (program, args) = (
        "/bin/sh".to_string(),
        vec!["-c".to_string(), script.to_string()],
    );

    PtyConfig {
        program,
        args,
        ..PtyConfig::default()
    }
}

/// 平台相关的 echo 脚本：stdout/stderr 各写一个标记后以 5 退出。
fn echo_script() -> &'static str {
    #[cfg(windows)]
    {
        "echo out_hi & echo err_hi 1>&2 & exit 5"
    }
    #[cfg(unix)]
    {
        "echo out_hi; echo err_hi 1>&2; exit 5"
    }
}

/// 平台相关的交互脚本：读一行 stdin 并回显 `got:<行>`。
///
/// Windows 使用延迟展开 `!X!`：`cmd /C` 会在执行 `set /p` 前展开整行的
/// `%X%`，普通 `%X%` 会读到空值，无法稳定验证“写入后回显”。
fn interactive_script() -> &'static str {
    #[cfg(windows)]
    {
        "set /p X=& echo got:!X!"
    }
    #[cfg(unix)]
    {
        "read line; echo got:$line"
    }
}

/// 平台相关的交互启动配置（Windows 需要 `/V:ON` 开启延迟展开）。
fn interactive_config() -> PtyConfig {
    #[cfg(windows)]
    {
        PtyConfig {
            program: "cmd.exe".to_string(),
            args: vec![
                "/V:ON".to_string(),
                "/C".to_string(),
                interactive_script().to_string(),
            ],
            ..PtyConfig::default()
        }
    }
    #[cfg(unix)]
    {
        shell_config(interactive_script())
    }
}

/// 平台相关的长驻命令。
fn long_running_config() -> PtyConfig {
    #[cfg(windows)]
    {
        shell_config("ping -n 30 127.0.0.1 > nul")
    }
    #[cfg(unix)]
    {
        shell_config("sleep 30 & wait")
    }
}

/// 按 pid 判断进程是否仍存活（僵尸不算存活）。
fn pid_alive(pid: u32) -> bool {
    #[cfg(windows)]
    {
        let filter = format!("PID eq {pid}");
        let Ok(output) = std::process::Command::new("tasklist")
            .args(["/FI", &filter, "/NH", "/FO", "CSV"])
            .output()
        else {
            return false;
        };
        String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
    }
    #[cfg(unix)]
    {
        let Ok(raw) = std::fs::read(format!("/proc/{pid}/stat")) else {
            return false;
        };
        let Some(close) = raw.iter().rposition(|&byte| byte == b')') else {
            return false;
        };
        let Ok(rest) = std::str::from_utf8(&raw[close + 1..]) else {
            return false;
        };
        rest.split_whitespace().next() != Some("Z")
    }
}

#[tokio::test]
async fn stdout_and_stderr_are_separated_and_exit_code_is_reported() {
    let (session, mut rx) = StdioSession::spawn(shell_config(echo_script())).expect("启动失败");

    assert!(session.pid() > 0, "pid 应为正数");
    assert_eq!(session.pgid(), session.pid(), "pgid 应等于根 pid");
    assert_eq!(
        session.foreground_pgrp().expect("查询前台进程组失败"),
        None,
        "stdio 没有前台进程组"
    );
    let mut session = session;
    session.resize(100, 40).expect("resize 应空操作成功");

    let collected = collect(&mut rx).await;
    let stdout = String::from_utf8_lossy(&collected.stdout);
    let stderr = String::from_utf8_lossy(&collected.stderr);
    assert!(stdout.contains("out_hi"), "stdout 缺少标记: {stdout:?}");
    assert!(stderr.contains("err_hi"), "stderr 缺少标记: {stderr:?}");
    assert_eq!(collected.terminal, Terminal::Exited(5), "退出码应为 5");
}

#[tokio::test]
async fn stdin_write_is_read_by_child() {
    let (mut session, mut rx) = StdioSession::spawn(interactive_config()).expect("启动失败");

    #[cfg(windows)]
    let input = b"abc\r\n";
    #[cfg(unix)]
    let input = b"abc\n";
    session.write(input).expect("写入 stdin 失败");

    let collected = collect(&mut rx).await;
    let stdout = String::from_utf8_lossy(&collected.stdout);
    assert!(stdout.contains("got:abc"), "stdout 缺少回显: {stdout:?}");
    assert_eq!(
        collected.terminal,
        Terminal::Exited(0),
        "交互脚本应正常退出"
    );
}

/// 子进程不读 stdin 时，写队列填满必须立即返回 `WriteQueueFull`（可映射
/// `limit_exceeded`），而不是在调用线程上阻塞等待管道可写。
#[tokio::test]
async fn write_queue_full_returns_error_without_blocking() {
    let (mut session, _rx) =
        StdioSession::spawn_with_write_queue(long_running_config(), 1).expect("启动失败");
    // 单块足够大：writer 线程写满内核管道缓冲后会阻塞，队列随即填满。
    let chunk = vec![b'x'; 64 * 1024];

    timeout(TEST_TIMEOUT, async {
        let mut saw_full = false;
        for _ in 0..512 {
            let started = Instant::now();
            match session.write(&chunk) {
                Ok(()) => {}
                Err(ProcError::WriteQueueFull) => {
                    assert!(
                        started.elapsed() < Duration::from_secs(2),
                        "队满返回耗时过长，疑似阻塞: {:?}",
                        started.elapsed()
                    );
                    saw_full = true;
                    break;
                }
                Err(error) => panic!("非预期错误: {error:?}"),
            }
        }
        assert!(saw_full, "连续写入后写队列应已满");
    })
    .await
    .expect("写队列满用例超时");

    session.terminate().expect("terminate 失败");
}

#[tokio::test]
async fn terminate_kills_child_and_reaps_it() {
    let (mut session, mut rx) = StdioSession::spawn(long_running_config()).expect("启动失败");
    let pid = session.pid();
    assert!(pid_alive(pid), "启动后进程应存活");

    session.terminate().expect("terminate 失败");

    // 等 wait 线程回收并确认 pid 消失（Unix 上 killpg 同时清理整组）。
    timeout(Duration::from_secs(10), async {
        loop {
            if !pid_alive(pid) {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("terminate 后进程仍存活");

    let collected = collect(&mut rx).await;
    match collected.terminal {
        Terminal::Exited(_) | Terminal::Signaled(_) => {}
    }
}

/// 测试探针可执行文件路径（由 Cargo 在集成测试中注入）。
const PROBE_BIN: &str = env!("CARGO_BIN_EXE_termx-stdio-probe");

/// 进程内唯一的临时目录计数器。
static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// 测试用临时目录；Drop 时尽力清理。
struct TempDir(PathBuf);

impl TempDir {
    /// 在系统临时目录下创建唯一子目录。
    fn new() -> Self {
        let unique = format!(
            "termx-stdio-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&path).expect("创建临时目录失败");
        Self(path)
    }

    /// 目录路径。
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 等待 `pid` 对应进程消失；超时即测试失败。
async fn wait_until_dead(pid: u32) {
    timeout(Duration::from_secs(10), async {
        while pid_alive(pid) {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("进程 {pid} 未在超时内消失"));
}

/// 读取输出直到 stdout 含 `needle`，返回 `(stdout, stderr)` 文本。
async fn read_until(rx: &mut Receiver<ProcEvent>, needle: &str) -> (String, String) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    timeout(TEST_TIMEOUT, async {
        loop {
            match rx.recv().await {
                Some(ProcEvent::Output {
                    data,
                    stream: OutputStream::Stdout,
                }) => {
                    stdout.extend_from_slice(&data);
                    if String::from_utf8_lossy(&stdout).contains(needle) {
                        return;
                    }
                }
                Some(ProcEvent::Output {
                    data,
                    stream: OutputStream::Stderr,
                }) => stderr.extend_from_slice(&data),
                Some(ProcEvent::Output {
                    stream: OutputStream::Pty,
                    ..
                }) => panic!("stdio 不应产生 pty 流"),
                Some(ProcEvent::Exited(code)) => {
                    panic!("进程在找到 {needle:?} 前退出: {code}")
                }
                Some(ProcEvent::Signaled(signal)) => {
                    panic!("进程在找到 {needle:?} 前被信号 {signal} 终止")
                }
                None => panic!("事件通道在找到 {needle:?} 前关闭"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("等待 stdout 标记 {needle:?} 超时"));

    (
        String::from_utf8_lossy(&stdout).into_owned(),
        String::from_utf8_lossy(&stderr).into_owned(),
    )
}

/// 从探针输出中解析 `key=<值>` 行。
fn parse_field<'a>(stdout: &'a str, key: &str) -> &'a str {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("输出缺少 {key}=: {stdout:?}"))
}

/// env/cwd 生效且参数不经 shell：探针直接看到原样参数、环境变量与工作目录。
#[tokio::test]
async fn probe_receives_cwd_env_and_literal_args() {
    let dir = TempDir::new();
    std::fs::write(dir.path().join("marker.txt"), b"ok").expect("写入标记文件失败");

    let mut env = BTreeMap::new();
    env.insert("TERMX_PROBE_ENV".to_string(), "probe-value".to_string());
    let config = PtyConfig {
        program: PROBE_BIN.to_string(),
        // 含 `;` 与 `&`：若后端额外套了 shell，参数会被拆分/解释。
        args: vec!["--probe".to_string(), "a;b & c".to_string()],
        cwd: Some(dir.path().to_string_lossy().into_owned()),
        env: Some(env),
        ..PtyConfig::default()
    };

    let (mut session, mut rx) = StdioSession::spawn(config).expect("启动失败");
    let (stdout, stderr) = read_until(&mut rx, "arg=").await;

    assert_eq!(parse_field(&stdout, "marker"), "yes", "cwd 未生效");
    assert_eq!(parse_field(&stdout, "env"), "probe-value", "env 未生效");
    assert_eq!(
        parse_field(&stdout, "arg"),
        "a;b & c",
        "参数被改写（不应经过 shell）"
    );

    let collected = collect(&mut rx).await;
    assert_eq!(collected.terminal, Terminal::Exited(0));
    let _ = session.resize(80, 24);
    assert!(stderr.is_empty(), "探针不应写 stderr: {stderr:?}");
}

/// `terminate` 清理整棵进程树：根进程与其后代都必须消失。
#[tokio::test]
async fn terminate_kills_descendants() {
    let config = PtyConfig {
        program: PROBE_BIN.to_string(),
        args: vec!["--child".to_string()],
        ..PtyConfig::default()
    };
    let (mut session, mut rx) = StdioSession::spawn(config).expect("启动失败");
    let root = session.pid();

    let (stdout, _) = read_until(&mut rx, "child=").await;
    let child: u32 = parse_field(&stdout, "child")
        .trim()
        .parse()
        .expect("child 不是合法 pid");
    assert!(pid_alive(root), "根进程应存活");
    assert!(pid_alive(child), "后代进程应存活");

    session.terminate().expect("terminate 失败");
    wait_until_dead(root).await;
    wait_until_dead(child).await;
}

/// 长驻子进程在会话 Drop 后被清理，不随测试进程残留。
#[tokio::test]
async fn drop_terminates_long_running_child() {
    let (session, rx) = StdioSession::spawn(long_running_config()).expect("启动失败");
    let pid = session.pid();
    assert!(pid_alive(pid), "启动后进程应存活");

    drop(session);
    drop(rx);
    wait_until_dead(pid).await;
}

/// 终态事件只发一次：输出结束后通道关闭，期间只有一条 Exited/Signaled。
#[tokio::test]
async fn terminal_event_is_emitted_exactly_once() {
    let (session, mut rx) = StdioSession::spawn(shell_config(echo_script())).expect("启动失败");
    assert!(session.pid() > 0);

    let mut terminal_events = 0_usize;
    let mut outputs = 0_usize;
    timeout(TEST_TIMEOUT, async {
        while let Some(event) = rx.recv().await {
            match event {
                ProcEvent::Output { .. } => outputs += 1,
                ProcEvent::Exited(_) | ProcEvent::Signaled(_) => terminal_events += 1,
            }
        }
    })
    .await
    .expect("等待事件通道关闭超时");

    assert!(outputs > 0, "应至少收到一条输出");
    assert_eq!(terminal_events, 1, "终态事件必须唯一");
}
