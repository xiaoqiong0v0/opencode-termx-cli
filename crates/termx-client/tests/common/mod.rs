//! termx-client 集成测试脚手架：起真实 service（含真实 PTY/ConPTY）并等待事件。

#![allow(dead_code)]

use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use termx_client::ClientConfig;
use termx_core::Registry;
use termx_protocol::{CreateParams, Event, EventKind, Mode, TerminalLocator};
use termx_service::{Service, ServiceConfig};
use tokio::{
    sync::{broadcast, oneshot},
    time::{Instant, timeout},
};

/// 常规等待上限。
pub const WAIT: Duration = Duration::from_secs(10);

/// 测试服务守卫：Drop 时停止 accept 循环并中断句柄。
pub struct TestService {
    /// 实际监听地址。
    pub addr: SocketAddr,
    /// 服务端 token。
    pub token: String,
    /// 配置目录；discover 测试可直接读取。
    pub config_dir: PathBuf,
    /// 共享 registry，测试可持锁阻塞请求以验证收尾。
    pub registry: Arc<Mutex<Registry>>,
    /// 优雅停机信号。
    shutdown: Option<oneshot::Sender<()>>,
    /// 承载服务运行时的线程。
    handle: Option<std::thread::JoinHandle<()>>,
}

impl TestService {
    /// 用临时配置目录启动真实后端服务。
    pub async fn start(config_dir: &Path) -> Self {
        // 显式指定 token 文件，避免测试进程环境里的 TERMX_TOKEN 改变服务端 token。
        let config =
            ServiceConfig::new(config_dir.to_path_buf()).with_token_file(config_dir.join("token"));
        Self::start_with_config(config_dir, config).await
    }

    /// 用自定义 [`ServiceConfig`] 启动真实后端服务。
    pub async fn start_with_config(config_dir: &Path, config: ServiceConfig) -> Self {
        let service = Service::bind(config).await.expect("服务绑定失败");
        let addr = service.local_addr();
        let registry = service.registry();
        let token = fs::read_to_string(service.config().token_path())
            .expect("token 文件缺失")
            .trim()
            .to_string();

        // 服务跑在独立线程的 runtime 上：测试可阻塞 registry 而不影响客户端 runtime。
        let (shutdown, stop_rx) = oneshot::channel();
        let handle = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("创建服务测试 runtime 失败");
            let result = runtime.block_on(service.run_until(async move {
                let _ = stop_rx.await;
            }));
            assert!(result.is_ok(), "服务运行期错误: {result:?}");
        });
        Self {
            addr,
            token,
            config_dir: config_dir.to_path_buf(),
            registry,
            shutdown: Some(shutdown),
            handle: Some(handle),
        }
    }

    /// WebSocket 端点。
    pub fn endpoint(&self) -> String {
        match self.addr {
            SocketAddr::V4(addr) => format!("ws://{addr}"),
            SocketAddr::V6(addr) => format!("ws://[{addr}]"),
        }
    }

    /// 指向本服务的客户端配置。
    pub fn client_config(&self) -> ClientConfig {
        ClientConfig::new(self.endpoint(), self.token.clone(), "client-test")
    }

    /// 优雅停机并等待服务线程退出。
    pub async fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(handle) = self.handle.take() {
            let joined = tokio::task::spawn_blocking(move || handle.join());
            let _ = timeout(WAIT, joined).await;
        }
    }
}

impl Drop for TestService {
    fn drop(&mut self) {
        // 只发停止信号；线程句柄 detach，避免在 Drop 里阻塞。
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// 终端定位器：只按 `terminal_id` 定位。
pub fn locator(terminal_id: &str) -> TerminalLocator {
    TerminalLocator {
        terminal_id: Some(terminal_id.to_string()),
        ..TerminalLocator::default()
    }
}

/// 交互式 PTY 创建参数：Windows 用 `cmd.exe`，Unix 用 `/bin/sh`。
#[cfg(windows)]
pub fn pty_params(root_session_id: &str, name: &str) -> CreateParams {
    CreateParams {
        mode: Mode::Pty,
        cmd: "cmd.exe".to_string(),
        args: Vec::new(),
        cwd: None,
        env: None,
        cols: Some(120),
        rows: Some(30),
        root_session_id: root_session_id.to_string(),
        name: name.to_string(),
        force: false,
        idle_timeout: None,
        ssh: None,
    }
}

/// 交互式 PTY 创建参数：Windows 用 `cmd.exe`，Unix 用 `/bin/sh`。
#[cfg(unix)]
pub fn pty_params(root_session_id: &str, name: &str) -> CreateParams {
    CreateParams {
        mode: Mode::Pty,
        cmd: "/bin/sh".to_string(),
        args: Vec::new(),
        cwd: None,
        env: None,
        cols: Some(120),
        rows: Some(30),
        root_session_id: root_session_id.to_string(),
        name: name.to_string(),
        force: false,
        idle_timeout: None,
        ssh: None,
    }
}

/// 等待下一个满足条件的事件；超时或通道关闭时 panic。
pub async fn next_event_matching<F>(
    events: &mut broadcast::Receiver<Event>,
    mut predicate: F,
) -> Event
where
    F: FnMut(&Event) -> bool,
{
    let deadline = Instant::now() + WAIT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "等待事件超时");
        match timeout(remaining, events.recv()).await {
            Ok(Ok(event)) if predicate(&event) => return event,
            Ok(Ok(_)) => {}
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
            Ok(Err(broadcast::error::RecvError::Closed)) => panic!("事件通道已关闭"),
            Err(_) => panic!("等待事件超时"),
        }
    }
}

/// 某终端 output 事件的解码字节；非 output 返回 `None`。
pub fn output_bytes(event: &Event) -> Option<Vec<u8>> {
    match &event.kind {
        EventKind::Output { data, .. } => Some(
            BASE64_STANDARD
                .decode(data)
                .expect("output data 不是合法 base64"),
        ),
        _ => None,
    }
}

/// 等待指定终端的 output 累计包含 `needle`。
pub async fn wait_output_contains(
    events: &mut broadcast::Receiver<Event>,
    terminal_id: &str,
    needle: &[u8],
) {
    let mut collected = Vec::new();
    loop {
        let event = next_event_matching(events, |event| event.terminal_id == terminal_id).await;
        let Some(bytes) = output_bytes(&event) else {
            continue;
        };
        collected.extend_from_slice(&bytes);
        if collected
            .windows(needle.len())
            .any(|window| window == needle)
        {
            return;
        }
    }
}
