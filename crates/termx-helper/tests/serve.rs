//! `termx-helper` sidecar 集成测试：起真实二进制，经 stdin/stdout 走 JSON Lines。
//!
//! 所有读取都带超时；`Helper::drop` 会杀掉子进程，测试不会遗留 helper。

#![cfg(any(windows, unix))]

use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use termx_protocol::HELPER_PROTOCOL_VERSION;
use termx_protocol::helper::{HelperEvent, HelperRequest};

/// 测试二进制路径（由 Cargo 注入）。
const HELPER_BIN: &str = env!("CARGO_BIN_EXE_termx-helper");
/// 单帧读取上限。
const IO_TIMEOUT: Duration = Duration::from_secs(30);
/// 轮询进程是否消失的上限。
const PID_TIMEOUT: Duration = Duration::from_secs(15);
/// 轮询间隔。
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// helper 子进程客户端。
struct Helper {
    /// 子进程。
    child: Child,
    /// stdin；关闭即模拟 EOF。
    stdin: Option<ChildStdin>,
    /// stdout 逐行读取结果。
    lines: Receiver<String>,
    /// stdout 读取线程。
    reader: Option<JoinHandle<()>>,
}

impl Helper {
    /// 启动 `termx-helper serve` 并接管 stdin/stdout。
    fn start() -> Self {
        let mut child = Command::new(HELPER_BIN)
            .arg("serve")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("启动 termx-helper 失败");

        let stdin = child.stdin.take().expect("helper stdin 缺失");
        let stdout = child.stdout.take().expect("helper stdout 缺失");
        let (tx, lines) = channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => {
                        if tx.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            child,
            stdin: Some(stdin),
            lines,
            reader: Some(reader),
        }
    }

    /// 写入一条请求并立即 flush。
    fn send(&mut self, request: &HelperRequest) {
        let line = serde_json::to_string(request).expect("请求序列化失败");
        self.send_raw(&line);
    }

    /// 写入原始一行（用于构造非法帧）。
    fn send_raw(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().expect("stdin 已关闭");
        writeln!(stdin, "{line}").expect("写入 helper stdin 失败");
        stdin.flush().expect("flush helper stdin 失败");
    }

    /// 关闭 stdin，触发 helper 的 EOF 收尾。
    fn close_stdin(&mut self) {
        self.stdin.take();
    }

    /// 读取下一条协议帧并解析为 [`HelperEvent`]。
    fn next_event(&self) -> HelperEvent {
        let line = match self.lines.recv_timeout(IO_TIMEOUT) {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => panic!("等待 helper 事件超时"),
            Err(RecvTimeoutError::Disconnected) => panic!("helper stdout 在事件到达前关闭"),
        };
        serde_json::from_str(&line).unwrap_or_else(|err| panic!("非法 helper 帧 {line:?}: {err}"))
    }

    /// 持续读取直到谓词命中，返回收集到的事件。
    fn collect_until<F>(&self, mut predicate: F) -> Vec<HelperEvent>
    where
        F: FnMut(&HelperEvent) -> bool,
    {
        let mut events = Vec::new();
        loop {
            let event = self.next_event();
            let done = predicate(&event);
            events.push(event);
            if done {
                return events;
            }
        }
    }

    /// 读取直到累计输出包含 `needle`，返回事件与已解码文本。
    fn read_until_text(&self, needle: &str) -> (Vec<HelperEvent>, String) {
        let mut events = Vec::new();
        let mut bytes = Vec::new();
        loop {
            let event = self.next_event();
            if let HelperEvent::Output { data, .. } = &event {
                bytes.extend_from_slice(&STANDARD.decode(data).expect("output.data 非 base64"));
            }
            let text = String::from_utf8_lossy(&bytes).into_owned();
            let done = text.contains(needle);
            events.push(event);
            if done {
                return (events, text);
            }
        }
    }

    /// 完成 `hello` 握手并返回 `hello_ok`；失败时 panic。
    fn handshake(&mut self) -> HelperEvent {
        self.send(&HelperRequest::Hello {
            proto: HELPER_PROTOCOL_VERSION.to_string(),
            version: "tests".to_string(),
        });
        let event = self.next_event();
        assert!(
            matches!(event, HelperEvent::HelloOk { .. }),
            "握手失败: {event:?}"
        );
        event
    }

    /// 等待子进程退出（带上限）。
    fn wait_exit(&mut self, timeout: Duration) -> Option<i32> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return status.code(),
                Ok(None) if Instant::now() < deadline => thread::sleep(POLL_INTERVAL),
                Ok(None) | Err(_) => return None,
            }
        }
    }
}

impl Drop for Helper {
    fn drop(&mut self) {
        self.stdin.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

/// 从事件中取 `started.pid`。
fn started_pid(events: &[HelperEvent]) -> u32 {
    events
        .iter()
        .find_map(|event| match event {
            HelperEvent::Started { pid, .. } => Some(*pid),
            _ => None,
        })
        .unwrap_or_else(|| panic!("缺少 started 事件: {events:?}"))
}

/// 汇总全部 output 字节并转成文本。
fn output_text(events: &[HelperEvent]) -> String {
    let mut bytes = Vec::new();
    for event in events {
        if let HelperEvent::Output { data, .. } = event {
            bytes.extend_from_slice(&STANDARD.decode(data).expect("output.data 非 base64"));
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// 校验一次性回显命令：`started`、标记输出、`exited{4}` 且无 `error`。
fn assert_echo_exit(events: &[HelperEvent]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, HelperEvent::Error { .. })),
        "不应出现 error: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, HelperEvent::Started { pid, .. } if *pid > 0)),
        "缺少 started: {events:?}"
    );
    let text = output_text(events);
    assert!(text.contains("hello_helper"), "输出缺少标记: {text:?}");

    let exit_code = events.iter().find_map(|event| match event {
        HelperEvent::Exited { code } => Some(*code),
        _ => None,
    });
    assert_eq!(exit_code, Some(4), "退出码应为 4: {events:?}");
}

/// 交互式 shell 的 `spawn` 请求。
fn interactive_shell() -> HelperRequest {
    #[cfg(windows)]
    let cmd = "cmd.exe".to_string();
    #[cfg(unix)]
    let cmd = "/bin/sh".to_string();

    HelperRequest::Spawn {
        cmd,
        args: Vec::new(),
        cwd: None,
        env: None,
        cols: Some(80),
        rows: Some(24),
    }
}

/// 一次性回显命令的 `spawn` 请求：输出标记后以退出码 4 结束。
fn spawn_echo() -> HelperRequest {
    #[cfg(windows)]
    let (cmd, args) = (
        "cmd.exe".to_string(),
        vec!["/C".to_string(), "echo hello_helper & exit 4".to_string()],
    );
    #[cfg(unix)]
    let (cmd, args) = (
        "/bin/sh".to_string(),
        vec!["-c".to_string(), "echo hello_helper; exit 4".to_string()],
    );

    HelperRequest::Spawn {
        cmd,
        args,
        cwd: None,
        env: None,
        cols: Some(80),
        rows: Some(24),
    }
}

/// 长驻进程的 `spawn` 请求。
fn spawn_long() -> HelperRequest {
    #[cfg(windows)]
    let (cmd, args) = (
        "cmd.exe".to_string(),
        vec!["/C".to_string(), "ping -n 60 127.0.0.1 > nul".to_string()],
    );
    #[cfg(unix)]
    let (cmd, args) = (
        "/bin/sh".to_string(),
        vec!["-c".to_string(), "sleep 60".to_string()],
    );

    HelperRequest::Spawn {
        cmd,
        args,
        cwd: None,
        env: None,
        cols: Some(80),
        rows: Some(24),
    }
}

/// 打印版本应输出 `termx-helper/1`。
#[test]
fn version_flag_prints_helper_version() {
    let output = Command::new(HELPER_BIN)
        .arg("--version")
        .output()
        .expect("运行 --version 失败");

    assert!(output.status.success(), "--version 应以 0 退出");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.trim(), "termx-helper/1", "版本串必须与协议名一致");
}

/// 正确 proto 的首帧应返回 hello_ok 与固定能力列表。
#[test]
fn hello_ok_advertises_capabilities() {
    let mut helper = Helper::start();
    let event = helper.handshake();

    match event {
        HelperEvent::HelloOk {
            proto,
            capabilities,
        } => {
            assert_eq!(proto, HELPER_PROTOCOL_VERSION);
            assert!(capabilities.iter().any(|cap| cap == "pty"));
            assert!(capabilities.iter().any(|cap| cap == "exit_status"));
        }
        other => panic!("期望 hello_ok，实际 {other:?}"),
    }
}

/// proto 不匹配应回 `version_mismatch` 并退出。
#[test]
fn hello_proto_mismatch_reports_error_and_exits() {
    let mut helper = Helper::start();
    helper.send(&HelperRequest::Hello {
        proto: "termx-helper/999".to_string(),
        version: "tests".to_string(),
    });

    match helper.next_event() {
        HelperEvent::Error { code, .. } => {
            assert_eq!(
                code,
                termx_protocol::ErrorCode::VersionMismatch,
                "应返回 version_mismatch"
            );
        }
        other => panic!("期望 error，实际 {other:?}"),
    }

    let code = helper
        .wait_exit(IO_TIMEOUT)
        .expect("proto 错误后 helper 应退出");
    assert_ne!(code, 0, "协议错误应以非零码退出");
}

/// `spawn` 一次性命令：收到 output 与 exited{4}。
#[test]
fn spawn_echo_reports_output_and_exit_code() {
    let mut helper = Helper::start();
    helper.handshake();
    helper.send(&spawn_echo());

    let events = helper.collect_until(|event| {
        matches!(
            event,
            HelperEvent::Exited { .. } | HelperEvent::Signaled { .. } | HelperEvent::Error { .. }
        )
    });
    assert_echo_exit(&events);
}

/// 交互：input 回显 → resize 不报错 → terminate 后进程消失。
#[test]
fn interactive_input_resize_and_terminate() {
    #[cfg(windows)]
    let enter = "\r\n";
    #[cfg(unix)]
    let enter = "\n";

    let mut helper = Helper::start();
    helper.handshake();
    helper.send(&interactive_shell());

    let started = helper.collect_until(|event| {
        matches!(
            event,
            HelperEvent::Started { .. } | HelperEvent::Error { .. }
        )
    });
    let pid = started_pid(&started);

    // 输入回显并执行，输出必须包含标记。
    let command = format!("echo termx_input_ok{enter}");
    helper.send(&HelperRequest::Input {
        data: STANDARD.encode(command.as_bytes()),
    });
    let (events, text) = helper.read_until_text("termx_input_ok");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, HelperEvent::Error { .. })),
        "input 不应报错: {events:?}"
    );
    assert!(
        text.contains("termx_input_ok"),
        "输出缺少 input 标记: {text:?}"
    );

    // resize 之后再写一条命令；若 resize 失败，error 会先于该输出到达。
    helper.send(&HelperRequest::Resize {
        cols: 100,
        rows: 30,
    });
    let command = format!("echo termx_resize_ok{enter}");
    helper.send(&HelperRequest::Input {
        data: STANDARD.encode(command.as_bytes()),
    });
    let (events, text) = helper.read_until_text("termx_resize_ok");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, HelperEvent::Error { .. })),
        "resize 不应报错: {events:?}"
    );
    assert!(
        text.contains("termx_resize_ok"),
        "输出缺少 resize 标记: {text:?}"
    );

    // terminate 后应出现终态事件，且根进程消失。
    helper.send(&HelperRequest::Terminate);
    let terminal = helper.collect_until(|event| {
        matches!(
            event,
            HelperEvent::Exited { .. } | HelperEvent::Signaled { .. } | HelperEvent::Error { .. }
        )
    });
    assert!(
        !terminal
            .iter()
            .any(|event| matches!(event, HelperEvent::Error { .. })),
        "terminate 不应报错: {terminal:?}"
    );
    assert!(
        wait_pid_gone(pid, PID_TIMEOUT),
        "terminate 后进程仍存活: {pid}"
    );
}

/// `terminate` 长驻进程后不留孤儿，且随后的 `spawn` 不被旧会话阻塞。
#[test]
fn terminate_long_running_then_spawn_again() {
    let mut helper = Helper::start();
    helper.handshake();
    helper.send(&spawn_long());

    let started = helper.collect_until(|event| {
        matches!(
            event,
            HelperEvent::Started { .. } | HelperEvent::Error { .. }
        )
    });
    let pid = started_pid(&started);
    assert!(
        wait_pid_alive(pid, Duration::from_secs(5)),
        "长驻进程未启动"
    );

    helper.send(&HelperRequest::Terminate);
    let terminal = helper.collect_until(|event| {
        matches!(
            event,
            HelperEvent::Exited { .. } | HelperEvent::Signaled { .. } | HelperEvent::Error { .. }
        )
    });
    assert!(
        !terminal
            .iter()
            .any(|event| matches!(event, HelperEvent::Error { .. })),
        "terminate 不应报错: {terminal:?}"
    );
    assert!(
        wait_pid_gone(pid, PID_TIMEOUT),
        "terminate 后进程仍存活: {pid}"
    );

    // 旧会话已终止：同一 helper 再次 spawn 不应返回 bad_request。
    helper.send(&spawn_echo());
    let events = helper.collect_until(|event| {
        matches!(
            event,
            HelperEvent::Exited { .. } | HelperEvent::Signaled { .. } | HelperEvent::Error { .. }
        )
    });
    assert_echo_exit(&events);
}

/// `tree` 请求应返回当前会话进程树；非 Linux 平台返回空列表。
#[test]
fn tree_reports_session_processes() {
    let mut helper = Helper::start();
    helper.handshake();
    helper.send(&spawn_long());

    let started = helper.collect_until(|event| {
        matches!(
            event,
            HelperEvent::Started { .. } | HelperEvent::Error { .. }
        )
    });
    let _pid = started_pid(&started);

    helper.send(&HelperRequest::Tree);
    let events = helper.collect_until(|event| matches!(event, HelperEvent::Tree { .. }));
    let processes = events
        .iter()
        .find_map(|event| match event {
            HelperEvent::Tree { processes } => Some(processes),
            _ => None,
        })
        .expect("缺少 tree 事件");

    // 只有 Linux 通过 /proc 采集进程树；其它平台按协议返回空列表。
    #[cfg(target_os = "linux")]
    assert!(
        processes.iter().any(|node| node.pid == _pid),
        "进程树应包含根进程 {_pid}: {processes:?}"
    );
    #[cfg(not(target_os = "linux"))]
    assert!(
        processes.is_empty(),
        "非 Linux 平台应返回空树: {processes:?}"
    );

    helper.send(&HelperRequest::Terminate);
    let _ = helper.collect_until(|event| {
        matches!(
            event,
            HelperEvent::Exited { .. } | HelperEvent::Signaled { .. } | HelperEvent::Error { .. }
        )
    });
}

/// stdin EOF 应让 helper 终止会话并以 0 退出，不留孤儿。
#[test]
fn stdin_eof_cleans_session() {
    let mut helper = Helper::start();
    helper.handshake();
    helper.send(&spawn_long());

    let started = helper.collect_until(|event| {
        matches!(
            event,
            HelperEvent::Started { .. } | HelperEvent::Error { .. }
        )
    });
    let pid = started_pid(&started);
    assert!(
        wait_pid_alive(pid, Duration::from_secs(5)),
        "长驻进程未启动"
    );

    helper.close_stdin();
    let code = helper.wait_exit(IO_TIMEOUT).expect("EOF 后 helper 应退出");
    assert_eq!(code, 0, "EOF 正常收尾应以 0 退出");
    assert!(
        wait_pid_gone(pid, PID_TIMEOUT),
        "EOF 收尾后进程仍存活: {pid}"
    );
}

/// 轮询直到进程消失。
fn wait_pid_gone(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !pid_alive(pid) {
            return true;
        }
        thread::sleep(POLL_INTERVAL);
    }
    !pid_alive(pid)
}

/// 轮询直到进程出现。
fn wait_pid_alive(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pid_alive(pid) {
            return true;
        }
        thread::sleep(POLL_INTERVAL);
    }
    pid_alive(pid)
}

/// 直接核对 pid 是否存在（不依赖进程树采集）。
#[cfg(windows)]
fn pid_alive(pid: u32) -> bool {
    let filter = format!("PID eq {pid}");
    let Ok(output) = Command::new("tasklist")
        .args(["/FI", &filter, "/NH", "/FO", "CSV"])
        .output()
    else {
        return false;
    };
    String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
}

/// 直接核对 pid 是否存在（不依赖进程树采集）。
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}
