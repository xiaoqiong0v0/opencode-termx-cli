//! shell 带外事件通道（Windows named pipe / Unix domain socket）。
//!
//! `termx-pwsh` 在启动时读取宿主注入的 `TERMX_EVENT_ENDPOINT` 与
//! `TERMX_EVENT_TOKEN`，连接后先发一行 token 握手，再按 JSON Lines 发送
//! `A/B/C/D/O/X` 事件。通道不占用 TCP 端口。
//!
//! 本 crate 只负责传输与帧解析；`C`/`D`/`O` 的 command-id + transcript 代次语义
//! 由 service 的 `ShellEventTracker` 实现。
//!
//! `O` 携带命令输出（`id` + base64 `data`），与 `C`/`D` 走同一条 FIFO 队列，
//! 因此输出先于命令结束事件到达；prompt 与命令行回显不属于命令输出。
//!
//! 安全与健壮性：
//! - token 使用常量时间比较，日志不输出 token 或 endpoint 中的随机名；
//! - 只 accept 一次，握手/连接有短超时；
//! - 单帧上限 [`MAX_FRAME_BYTES`]，超限即断开；
//! - 通道失败/断开只让调用方退化为静默判定，不伪造退出码；
//! - Unix socket 位于 0700 私有目录内（socket 0600），任务结束或取消时清理。

use std::{fmt::Write as _, io, sync::Arc, time::Duration};

use serde::Deserialize;
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, BufReader},
    sync::mpsc,
    task::AbortHandle,
    time::timeout,
};

pub use termx_protocol::shell_events::{ShellEvent, ShellEventKind};

/// 注入终端的 endpoint 环境变量名。
pub const EVENT_ENDPOINT_ENV: &str = "TERMX_EVENT_ENDPOINT";
/// 注入终端的 token 环境变量名。
pub const EVENT_TOKEN_ENV: &str = "TERMX_EVENT_TOKEN";
/// 单帧最大字节数（JSON Lines 的一行）。
pub const MAX_FRAME_BYTES: usize = 4096;
/// 等待宿主连接的超时。
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// 等待 token 握手行的超时。
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// 事件接收队列容量；满时丢弃新事件，绝不阻塞 prompt。
pub const EVENT_QUEUE_CAPACITY: usize = 256;

/// 打开的事件通道；`endpoint`/`token` 在 spawn 前注入终端环境。
pub struct ShellEventChannel {
    endpoint: String,
    token: String,
    receiver: mpsc::Receiver<ShellEvent>,
    cancel: ShellEventCancel,
}

impl ShellEventChannel {
    /// 绑定随机 named pipe / Unix socket 并启动一次性监听任务。
    ///
    /// 返回时监听端点已存在，可在随后 spawn 的终端中连接。
    pub fn open() -> io::Result<Self> {
        let token = random_hex(32);
        let (sender, receiver) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        let (endpoint, handle) = spawn_listener(Arc::new(token.clone()), sender)?;
        Ok(Self {
            endpoint,
            token,
            receiver,
            cancel: ShellEventCancel {
                abort: handle.abort_handle(),
            },
        })
    }

    /// 注入终端环境的 endpoint（`pipe:<name>` 或 `unix:<path>`）。
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// 注入终端环境的 token；调用方不得写入日志。
    pub fn token(&self) -> &str {
        &self.token
    }

    /// 拆出事件接收端与取消句柄；取消句柄 drop 时关闭通道并清理 socket。
    pub fn split(self) -> (mpsc::Receiver<ShellEvent>, ShellEventCancel) {
        (self.receiver, self.cancel)
    }
}

/// 通道取消/清理句柄。
///
/// 由持有者长期保存：drop 时 abort 监听任务；Unix socket 与私有目录随
/// 监听任务中的清理守卫一并删除，Windows named pipe 随句柄关闭自动消失。
pub struct ShellEventCancel {
    abort: AbortHandle,
}

impl Drop for ShellEventCancel {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// 与 `termx-pwsh` 握手帧对应。
#[derive(Debug, Deserialize)]
struct WireHello {
    #[serde(default)]
    v: u8,
    token: String,
}

/// JSON Lines 事件帧。
#[derive(Debug, Deserialize)]
struct WireEvent {
    #[serde(default)]
    v: u8,
    event: String,
    #[serde(default)]
    code: Option<i32>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    started_at_ms: Option<i64>,
    #[serde(default)]
    ended_at_ms: Option<i64>,
    #[serde(default)]
    data: Option<String>,
    /// `D` 表示命令有 `O` 帧因队列满/关闭被丢弃。
    #[serde(default)]
    dropped: bool,
}

/// 监听端只持有 `AsyncRead`；服务端不需要回写。
async fn serve_stream<S>(
    stream: S,
    token: Arc<String>,
    sender: mpsc::Sender<ShellEvent>,
) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(stream);
    let handshake = match timeout(HANDSHAKE_TIMEOUT, read_frame(&mut reader)).await {
        Ok(Ok(Some(line))) => line,
        _ => return Ok(()),
    };
    let hello: WireHello = serde_json::from_slice(&handshake).map_err(invalid_data)?;
    if hello.v != 1 || !token_matches(&token, &hello.token) {
        return Ok(());
    }

    while let Some(line) = read_frame(&mut reader).await? {
        if line.is_empty() {
            continue;
        }
        let Ok(wire) = serde_json::from_slice::<WireEvent>(&line) else {
            continue;
        };
        if wire.v != 1 {
            continue;
        }
        let Some(kind) = ShellEventKind::parse(&wire.event) else {
            continue;
        };
        let event = ShellEvent {
            kind,
            code: wire.code,
            id: wire.id,
            command: wire.command,
            started_at_ms: wire.started_at_ms,
            ended_at_ms: wire.ended_at_ms,
            data: wire.data,
            dropped: wire.dropped,
        };
        if sender.send(event).await.is_err() {
            break;
        }
    }
    Ok(())
}

/// 读取一行，最多 [`MAX_FRAME_BYTES`]；超限返回 `InvalidData`。
async fn read_frame<R>(reader: &mut R) -> io::Result<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut frame = Vec::new();
    loop {
        let buffer = reader.fill_buf().await?;
        if buffer.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Ok(Some(frame))
            };
        }
        if let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
            if frame.len() + position > MAX_FRAME_BYTES {
                return Err(frame_too_large());
            }
            frame.extend_from_slice(&buffer[..position]);
            reader.consume(position + 1);
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
        if frame.len() + buffer.len() > MAX_FRAME_BYTES {
            return Err(frame_too_large());
        }
        let consumed = buffer.len();
        frame.extend_from_slice(buffer);
        reader.consume(consumed);
    }
}

/// 常量时间比较 token；长度不同直接拒绝。
fn token_matches(expected: &str, actual: &str) -> bool {
    let expected = expected.as_bytes();
    let actual = actual.as_bytes();
    expected.len() == actual.len() && bool::from(expected.ct_eq(actual))
}

/// JSON 解析失败映射为 `InvalidData`。
fn invalid_data(error: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// 帧超限错误。
fn frame_too_large() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "shell event frame too large")
}

/// 生成 `2 * bytes` 个十六进制字符的随机串。
fn random_hex(bytes: usize) -> String {
    use rand::Rng as _;

    let mut raw = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut raw);
    let mut out = String::with_capacity(bytes * 2);
    for byte in raw {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(windows)]
fn spawn_listener(
    token: Arc<String>,
    sender: mpsc::Sender<ShellEvent>,
) -> io::Result<(String, tokio::task::JoinHandle<()>)> {
    use tokio::net::windows::named_pipe::ServerOptions;

    let name = format!("termx-events-{}-{}", std::process::id(), random_hex(8));
    let path = format!(r"\\.\pipe\{name}");
    let mut options = ServerOptions::new();
    options
        .first_pipe_instance(true)
        .reject_remote_clients(true);
    let server = options.create(&path)?;

    let handle = tokio::spawn(async move {
        if let Ok(Ok(())) = timeout(CONNECT_TIMEOUT, server.connect()).await {
            let _ = serve_stream(server, token, sender).await;
        }
    });
    Ok((format!("pipe:{name}"), handle))
}

#[cfg(unix)]
fn spawn_listener(
    token: Arc<String>,
    sender: mpsc::Sender<ShellEvent>,
) -> io::Result<(String, tokio::task::JoinHandle<()>)> {
    use std::os::unix::fs::PermissionsExt;

    let directory = std::env::temp_dir().join(format!(
        "termx-events-{}-{}",
        std::process::id(),
        random_hex(8)
    ));
    std::fs::create_dir(&directory)?;
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
    let socket_path = directory.join("events.sock");
    let listener = match tokio::net::UnixListener::bind(&socket_path) {
        Ok(listener) => listener,
        Err(error) => {
            let _ = std::fs::remove_dir_all(&directory);
            return Err(error);
        }
    };
    let _ = std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600));
    let cleanup = UnixSocketCleanup {
        directory: directory.clone(),
        socket_path: socket_path.clone(),
    };
    let endpoint = format!("unix:{}", socket_path.display());

    let handle = tokio::spawn(async move {
        let _cleanup = cleanup;
        if let Ok(Ok((stream, _))) = timeout(CONNECT_TIMEOUT, listener.accept()).await {
            let _ = serve_stream(stream, token, sender).await;
        }
    });
    Ok((endpoint, handle))
}

/// Unix socket 清理：任务结束或被 abort 时删除 socket 与私有目录。
#[cfg(unix)]
struct UnixSocketCleanup {
    directory: std::path::PathBuf,
    socket_path: std::path::PathBuf,
}

#[cfg(unix)]
impl Drop for UnixSocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
        let _ = std::fs::remove_dir(&self.directory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `O` 的 base64 data 必须能解析；旧事件没有 data 时按 None 兼容。
    #[test]
    fn wire_event_parses_output_data_and_legacy_events() {
        let output: WireEvent =
            serde_json::from_str(r#"{"v":1,"event":"O","id":"cmd-1","data":"aGVsbG8="}"#)
                .expect("O 事件应可解析");
        assert_eq!(output.event, "O");
        assert_eq!(output.id.as_deref(), Some("cmd-1"));
        assert_eq!(output.data.as_deref(), Some("aGVsbG8="));

        let legacy: WireEvent =
            serde_json::from_str(r#"{"v":1,"event":"D","code":0,"id":"cmd-1"}"#)
                .expect("旧 D 事件应兼容");
        assert_eq!(legacy.event, "D");
        assert_eq!(legacy.data, None);
        assert!(!legacy.dropped);

        let dropped: WireEvent =
            serde_json::from_str(r#"{"v":1,"event":"D","code":0,"id":"cmd-1","dropped":true}"#)
                .expect("带 dropped 的 D 事件应可解析");
        assert!(dropped.dropped);
    }
}
