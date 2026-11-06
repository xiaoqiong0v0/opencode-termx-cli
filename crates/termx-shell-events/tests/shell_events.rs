//! 共享事件通道的传输边界：只 accept 一次、单帧上限。

#![cfg(any(windows, unix))]

use std::time::Duration;

#[cfg(unix)]
use termx_shell_events::ShellEventKind;
use termx_shell_events::{MAX_FRAME_BYTES, ShellEventChannel};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    time::timeout,
};

/// 连接 channel 暴露的端点。
#[cfg(windows)]
async fn connect(endpoint: &str) -> Box<dyn AsyncWrite + Unpin + Send> {
    use tokio::net::windows::named_pipe::ClientOptions;

    let name = endpoint.strip_prefix("pipe:").expect("pipe 端点");
    let client = ClientOptions::new()
        .open(format!(r"\\.\pipe\{name}"))
        .expect("连接 named pipe");
    Box::new(client)
}

/// 连接 channel 暴露的端点。
#[cfg(unix)]
async fn connect(endpoint: &str) -> Box<dyn AsyncWrite + Unpin + Send> {
    let path = endpoint.strip_prefix("unix:").expect("unix 端点");
    let stream = tokio::net::UnixStream::connect(path)
        .await
        .expect("连接 Unix socket");
    Box::new(stream)
}

/// 写一行原始文本。
async fn send_line(stream: &mut Box<dyn AsyncWrite + Unpin + Send>, line: &str) {
    stream.write_all(line.as_bytes()).await.expect("写帧");
    stream.write_all(b"\n").await.expect("写换行");
    stream.flush().await.expect("flush");
}

/// 只服务第一条连接；第二条连接不会产生任何事件。
///
/// Windows named pipe 只注册单实例，第二条连接会直接被系统拒绝；因此该
/// 语义只在 Unix UDS 上验证。
#[cfg(unix)]
#[tokio::test]
async fn only_first_connection_is_served() {
    let channel = ShellEventChannel::open().expect("打开通道");
    let endpoint = channel.endpoint().to_string();
    let token = channel.token().to_string();
    let (mut receiver, _cancel) = channel.split();

    let mut first = connect(&endpoint).await;
    send_line(&mut first, &format!(r#"{{"v":1,"token":"{token}"}}"#)).await;
    send_line(&mut first, r#"{"v":1,"event":"C","id":"cmd-1"}"#).await;
    let event = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("等待 C 超时")
        .expect("应收到 C");
    assert_eq!(event.kind, ShellEventKind::CommandStart);

    let mut second = connect(&endpoint).await;
    send_line(&mut second, &format!(r#"{{"v":1,"token":"{token}"}}"#)).await;
    send_line(&mut second, r#"{"v":1,"event":"A"}"#).await;

    drop(first);
    let result = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("等待通道结束超时");
    assert!(result.is_none(), "第二条连接不应投递事件");
}

/// 超过 [`MAX_FRAME_BYTES`] 的帧直接断开，不产生事件。
#[tokio::test]
async fn oversized_frame_is_rejected() {
    let channel = ShellEventChannel::open().expect("打开通道");
    let endpoint = channel.endpoint().to_string();
    let token = channel.token().to_string();
    let (mut receiver, _cancel) = channel.split();

    let mut stream = connect(&endpoint).await;
    send_line(&mut stream, &format!(r#"{{"v":1,"token":"{token}"}}"#)).await;

    let padding = "x".repeat(MAX_FRAME_BYTES);
    let oversized = format!(r#"{{"v":1,"event":"A","padding":"{padding}"}}"#);
    assert!(oversized.len() > MAX_FRAME_BYTES);
    send_line(&mut stream, &oversized).await;

    let result = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("等待断开超时");
    assert!(result.is_none(), "超限帧不应投递事件");
}
