//! 集成测试共用的 `termx-helper serve` 子进程客户端。

#![allow(dead_code)]

use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{Receiver, RecvTimeoutError, channel},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use termx_protocol::HELPER_PROTOCOL_VERSION;
use termx_protocol::helper::{HelperEvent, HelperRequest};

/// 测试二进制路径（由 Cargo 注入）。
pub const HELPER_BIN: &str = env!("CARGO_BIN_EXE_termx-helper");
/// 单帧读取上限。
pub const IO_TIMEOUT: Duration = Duration::from_secs(30);
/// 轮询进程是否消失的上限。
pub const PID_TIMEOUT: Duration = Duration::from_secs(15);
/// 轮询间隔。
pub const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// helper 子进程客户端。
pub struct Helper {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    reader: Option<JoinHandle<()>>,
}

impl Helper {
    /// 启动 `termx-helper serve` 并接管 stdin/stdout。
    pub fn start() -> Self {
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
    pub fn send(&mut self, request: &HelperRequest) {
        let line = serde_json::to_string(request).expect("请求序列化失败");
        self.send_raw(&line);
    }

    /// 写入原始一行（用于构造非法帧）。
    pub fn send_raw(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().expect("stdin 已关闭");
        writeln!(stdin, "{line}").expect("写入 helper stdin 失败");
        stdin.flush().expect("flush helper stdin 失败");
    }

    /// 关闭 stdin，触发 helper 的 EOF 收尾。
    pub fn close_stdin(&mut self) {
        self.stdin.take();
    }

    /// 读取下一条协议帧并解析为 [`HelperEvent`]。
    pub fn next_event(&self) -> HelperEvent {
        self.next_event_within(IO_TIMEOUT)
            .expect("等待 helper 事件超时")
    }

    /// 在 `timeout` 内读取下一条协议帧；超时或 stdout 关闭返回 `None`。
    pub fn next_event_within(&self, timeout: Duration) -> Option<HelperEvent> {
        let line = match self.lines.recv_timeout(timeout) {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => return None,
            Err(RecvTimeoutError::Disconnected) => return None,
        };
        serde_json::from_str(&line).unwrap_or_else(|err| panic!("非法 helper 帧 {line:?}: {err}"))
    }

    /// 持续读取直到谓词命中，返回收集到的事件。
    pub fn collect_until<F>(&self, mut predicate: F) -> Vec<HelperEvent>
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
    pub fn read_until_text(&self, needle: &str) -> (Vec<HelperEvent>, String) {
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
    pub fn handshake(&mut self) -> HelperEvent {
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
    pub fn wait_exit(&mut self, timeout: Duration) -> Option<i32> {
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
