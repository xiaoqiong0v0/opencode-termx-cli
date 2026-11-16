//! `handle_create` 布尔连线测试：远端 pwsh 后端（`backend.shell_events()` 为
//! true）创建后 `event_channel_active` / `transcript_output_from_events`
//! 必须为 true（`server.rs:2203` / `:2210-2211`），PTY 输出不直写 transcript。
//!
//! `remote_o_data.rs` 直接驱动 `apply_event` 绕过了这条连线；本文件用自定义
//! mock 工厂（后端声明 `shell_events` 能力、保留 `ProcEvent` 发送端）经真实
//! `terminal.create` + `ingest_proc_events` 路径锁定：SSH 参数下本地事件通道
//! 必然为 `None`，`event_channel_active` 只能来自 `remote_events`，因此
//! `structured_events=false` 或 PTY 回显双写都意味着连线被改坏。

mod common;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use common::{TestService, call, connect, create_terminal, handshake_root};
use tempfile::tempdir;
use termx_core::{BackendFactory, CoreError, MockBackend, SpawnedBackend, TerminalBackend};
use termx_proc::{OutputStream, ProcEvent};
use termx_protocol::shell_events::{ShellEvent, ShellEventKind};
use termx_protocol::{
    CreateParams, ProcNode, StatusResult, TerminalLocator, TranscriptCommand, TranscriptSource,
    TranscriptStatus,
};
use termx_service::{RemoteInjectionConfig, ServiceConfig};
use tokio::sync::mpsc::{self, Sender};

/// 握手用根会话 id。
const ROOT_SESSION_ID: &str = "ses-remote-o-wiring";

/// 声明 `shell_events` 能力的 mock 后端：其余能力与 [`MockBackend`] 完全一致。
struct RemoteEventsBackend {
    /// 被代理的 mock 后端。
    inner: MockBackend,
}

impl RemoteEventsBackend {
    /// 创建包装后端。
    fn new() -> Self {
        Self {
            inner: MockBackend::new(),
        }
    }
}

impl TerminalBackend for RemoteEventsBackend {
    fn pid(&self) -> u32 {
        self.inner.pid()
    }

    fn pgid(&self) -> u32 {
        self.inner.pgid()
    }

    fn foreground_pgrp(&self) -> Option<u32> {
        self.inner.foreground_pgrp()
    }

    fn tree(&self) -> Vec<ProcNode> {
        self.inner.tree()
    }

    fn write(&self, data: &[u8]) -> Result<(), CoreError> {
        self.inner.write(data)
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<(), CoreError> {
        self.inner.resize(cols, rows)
    }

    fn signal(&self, name: &str) -> Result<(), CoreError> {
        self.inner.signal(name)
    }

    fn terminate(&self) {
        self.inner.terminate();
    }

    /// 关键覆写：模拟远端 helper 已建立事件通道（`started.events=true`）。
    fn shell_events(&self) -> bool {
        true
    }
}

/// mock 工厂：spawn 出 [`RemoteEventsBackend`]，并把 `ProcEvent` 发送端留给
/// 测试按序注入 PTY 输出与 shell 事件（对应远端 helper 转发流）。
#[derive(Default)]
struct RemoteEventsFactory {
    /// 每次 spawn 保留一个发送端；测试取出后按序发送事件。
    senders: Mutex<Vec<Sender<ProcEvent>>>,
}

impl BackendFactory for RemoteEventsFactory {
    fn spawn(&self, _params: &CreateParams) -> Result<SpawnedBackend, CoreError> {
        let (sender, receiver) = mpsc::channel(64);
        self.senders
            .lock()
            .expect("factory senders 锁")
            .push(sender);
        Ok((Arc::new(RemoteEventsBackend::new()), receiver))
    }
}

/// 构造一条 shell 事件；`command` 仅 `C` 使用，`data` 仅 `O` 使用。
fn shell_event(
    kind: ShellEventKind,
    code: Option<i32>,
    id: &str,
    command: Option<String>,
    data: Option<String>,
) -> ProcEvent {
    ProcEvent::ShellEvent(ShellEvent {
        kind,
        code,
        id: Some(id.to_string()),
        command,
        started_at_ms: None,
        ended_at_ms: None,
        data,
        dropped: false,
    })
}

/// 轮询直到命令带退出码（ingest 任务异步应用事件），返回该命令。
async fn wait_command_done(service: &TestService, terminal_id: &str) -> TranscriptCommand {
    let locator = TerminalLocator {
        terminal_id: Some(terminal_id.to_string()),
        root_session_id: None,
        name: None,
    };
    for _ in 0..100 {
        {
            let registry = service.registry.lock().expect("registry 锁");
            let commands = registry
                .transcript(&locator, None)
                .expect("读取 transcript")
                .commands;
            if let Some(command) = commands.first() {
                if command.exit_code.is_some() {
                    return command.clone();
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("超时未等到 D 事件应用");
}

#[tokio::test]
async fn remote_pwsh_backend_wires_transcript_output_from_events() {
    let dir = tempdir().expect("临时目录");
    let factory = Arc::new(RemoteEventsFactory::default());
    let factory_for_service = Arc::clone(&factory) as Arc<dyn BackendFactory>;
    let mut service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()).with_remote_injection(
            RemoteInjectionConfig::new().with_helper_local("./termx-helper"),
        ),
        Some(factory_for_service),
    )
    .await;
    let mut client = connect(&service).await;
    handshake_root(&mut client, &service.token, ROOT_SESSION_ID).await;

    let created = create_terminal(
        &mut client,
        "wiring-create",
        serde_json::json!({
            "mode": "pty",
            "cmd": "termx-pwsh",
            "args": [],
            "root_session_id": ROOT_SESSION_ID,
            "name": "remote-o-wiring",
            "allow_injection": true,
            "ssh": {
                "host": "example.com",
                "port": 22,
                "user": "alice",
                "auth": { "type": "password", "password": "secret" }
            }
        }),
    )
    .await;

    // 连线断言一（server.rs:2203）：SSH 下本地通道为 None，`structured_events`
    // 为 true 只能来自 `remote_events`——若连线退回 `event_channel.is_some()`
    // 则此处为 false。
    let status = call(
        &mut client,
        "wiring-status",
        "terminal.status",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(status.ok, "terminal.status 应成功: {status:?}");
    let status_result: StatusResult =
        serde_json::from_value(status.result.expect("缺少 status result"))
            .expect("StatusResult 解析失败");
    assert!(
        status_result.snapshot.structured_events,
        "远端 pwsh 后端 shell_events()=true 必须计入 event_channel_active"
    );

    // 连线断言二（server.rs:2210-2211）：`transcript_output_from_events=true`
    // → `ingest_proc_events` 以 `record_pty_transcript=false` 运行，PTY 回显
    // 进兜底、`O` 为准；若连线被改坏，PTY 回显会直写进 transcript 输出。
    let sender = factory
        .senders
        .lock()
        .expect("factory senders 锁")
        .pop()
        .expect("spawn 应保留发送端");
    sender
        .send(shell_event(
            ShellEventKind::CommandStart,
            None,
            "cmd-wiring",
            Some("Write-Output hi".to_string()),
            None,
        ))
        .await
        .expect("发送 C");
    sender
        .send(ProcEvent::Output {
            data: b"PS /tmp> Write-Output hi\r\nhi\r\n".to_vec(),
            stream: OutputStream::Pty,
        })
        .await
        .expect("发送 PTY 回显");
    sender
        .send(shell_event(
            ShellEventKind::CommandOutput,
            None,
            "cmd-wiring",
            None,
            Some(STANDARD.encode("hi\n")),
        ))
        .await
        .expect("发送 O");
    sender
        .send(shell_event(
            ShellEventKind::CommandEnd,
            Some(0),
            "cmd-wiring",
            None,
            None,
        ))
        .await
        .expect("发送 D");
    drop(sender);

    let command = wait_command_done(&service, &created.terminal_id).await;
    assert_eq!(command.status, TranscriptStatus::Done);
    assert_eq!(command.exit_code, Some(0));
    assert_eq!(command.source, Some(TranscriptSource::ShellIntegration));
    assert_eq!(
        command.output, "hi",
        "PTY 回显不得直写 transcript（transcript_output_from_events 连线）"
    );
    assert!(
        !command.incomplete,
        "PTY 全为 O 回显副本时不得标 incomplete"
    );
    assert!(!command.best_effort);

    service.shutdown().await;
}
