//! M4b 集成测试：输出摄取触发 OSC 交互识别，ticker 采集前台进程组推送
//! `interactive_changed`，并确保不会把“等待输入”误判为终态。
//!
//! Windows 的 ConPTY 没有前台进程组语义，因此只跑 OSC 用例；Unix 额外验证
//! `sh` 作业的前台进程组进入/退出。

#![cfg(any(windows, unix))]

mod common;

#[path = "support/interaction_probe.rs"]
mod interaction_probe;

use std::time::Duration;

use common::{
    TestService, call, connect, create_terminal, handshake, next_event, next_event_matching,
};
use serde_json::json;
use termx_protocol::{Confidence, EventKind};
use termx_service::ServiceConfig;
use tokio::time::timeout;

/// 观察“不应再出现某类事件”的静默窗口；放宽以减少慢机器抖动。
const QUIET_WINDOW: Duration = Duration::from_secs(1);

/// 探针子进程入口：父测试以 `--exact probe_child` 重新执行本测试二进制，
/// 从而避免为测试单独发布一个 `[[bin]]`。
#[test]
fn probe_child() {
    interaction_probe::run_if_child();
}

/// 构造探针终端的创建参数。
fn probe_params(root_session_id: &str, name: &str, mode: &str) -> serde_json::Value {
    let mut env = serde_json::Map::new();
    env.insert(
        interaction_probe::MODE_ENV.to_string(),
        serde_json::Value::String(mode.to_string()),
    );
    json!({
        "mode": "pty",
        "cmd": std::env::current_exe()
            .expect("测试二进制路径")
            .to_string_lossy()
            .into_owned(),
        "args": ["--exact", interaction_probe::CHILD_TEST, "--nocapture"],
        "env": env,
        "root_session_id": root_session_id,
        "name": name,
    })
}

/// 构造交互配置：快速 tick、较长静默阈值，避免启发式干扰 OSC 用例。
fn interaction_config(dir: &std::path::Path) -> ServiceConfig {
    ServiceConfig::new(dir.to_path_buf())
        .with_interaction_tick_interval(Duration::from_millis(40))
        .with_interaction_no_output_threshold(Duration::from_secs(10))
        .with_interaction_repeat_interval(Duration::from_secs(10))
}

/// 是否为终态事件。
fn is_terminal_event(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Exited { .. } | EventKind::Interrupted { .. } | EventKind::Error { .. }
    )
}

/// 是否为交互类事件。
fn is_interaction_event(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::WaitingInput { .. } | EventKind::InteractiveChanged { .. }
    )
}

/// OSC `133;A` 应产生 high/shell_integration 的等待输入事件。
#[tokio::test]
async fn osc_prompt_start_emits_high_confidence_waiting_input() {
    let dir = tempfile::tempdir().expect("创建临时目录失败");
    let mut service = TestService::start_with_config(interaction_config(dir.path())).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "i-001",
        probe_params("ses-i", "osc-prompt", "--prompt"),
    )
    .await;

    let attach = call(
        &mut client,
        "i-002",
        "terminal.attach",
        json!({ "terminal_id": created.terminal_id, "cursor": created.cursor }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    let event = next_event_matching(&mut client, |event| {
        matches!(event.kind, EventKind::WaitingInput { .. })
    })
    .await;
    match event.kind {
        EventKind::WaitingInput { confidence, reason } => {
            assert_eq!(confidence, Confidence::High);
            assert_eq!(reason, "shell_integration");
        }
        other => panic!("期望 waiting_input，实际 {other:?}"),
    }

    // 探针仍在运行：窗口内不应出现任何终态事件（不得把等待输入当完成）。
    let terminal = timeout(QUIET_WINDOW, async {
        loop {
            if is_terminal_event(&next_event(&mut client).await.kind) {
                return;
            }
        }
    })
    .await;
    assert!(terminal.is_err(), "等待输入期间不应产生终态事件");

    let terminate = call(
        &mut client,
        "i-003",
        "terminal.terminate",
        json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(terminate.ok, "terminate 应成功: {terminate:?}");
    service.shutdown().await;
}

/// 等待输入期间仍可 `terminal.send`，且输出/事件继续流动、不产生终态。
#[tokio::test]
async fn waiting_input_still_accepts_send_and_keeps_events_flowing() {
    let dir = tempfile::tempdir().expect("创建临时目录失败");
    let mut service = TestService::start_with_config(interaction_config(dir.path())).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "i-011",
        probe_params("ses-i", "waiting-send", "--echo"),
    )
    .await;

    let attach = call(
        &mut client,
        "i-012",
        "terminal.attach",
        json!({ "terminal_id": created.terminal_id, "cursor": created.cursor }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    // 先进入等待输入态。
    next_event_matching(&mut client, |event| {
        matches!(event.kind, EventKind::WaitingInput { .. })
    })
    .await;

    let claim = call(
        &mut client,
        "i-013",
        "terminal.claim",
        json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(claim.ok, "claim 应成功: {claim:?}");

    let send = call(
        &mut client,
        "i-014",
        "terminal.send",
        json!({ "terminal_id": created.terminal_id, "text": "hello", "enter": true }),
    )
    .await;
    assert!(send.ok, "等待输入期间 send 应成功: {send:?}");

    // 探针回显输入，说明终端仍在正常输出事件。
    let output = next_event_matching(&mut client, |event| {
        matches!(event.kind, EventKind::Output { .. })
    })
    .await;
    assert!(matches!(output.kind, EventKind::Output { .. }));

    // 等待输入不是终态：窗口内不得出现 exited/interrupted/error。
    let terminal = timeout(QUIET_WINDOW, async {
        loop {
            if is_terminal_event(&next_event(&mut client).await.kind) {
                return;
            }
        }
    })
    .await;
    assert!(terminal.is_err(), "send 后不应把等待输入误判为终态");

    let terminate = call(
        &mut client,
        "i-015",
        "terminal.terminate",
        json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(terminate.ok, "terminate 应成功: {terminate:?}");
    service.shutdown().await;
}

/// `interaction_enabled=false` 时不产生任何交互事件（OSC 与 ticker 都关闭）。
#[tokio::test]
async fn disabled_interaction_emits_no_interaction_events() {
    let dir = tempfile::tempdir().expect("创建临时目录失败");
    let config = interaction_config(dir.path()).with_interaction_enabled(false);
    let mut service = TestService::start_with_config(config).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "i-021",
        probe_params("ses-i", "disabled", "--prompt"),
    )
    .await;

    let attach = call(
        &mut client,
        "i-022",
        "terminal.attach",
        json!({ "terminal_id": created.terminal_id, "cursor": created.cursor }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    let observed = timeout(QUIET_WINDOW, async {
        loop {
            let event = next_event(&mut client).await;
            if is_interaction_event(&event.kind) {
                return;
            }
            if is_terminal_event(&event.kind) {
                return;
            }
        }
    })
    .await;
    assert!(observed.is_err(), "关闭交互识别后不应收到任何交互事件");

    let terminate = call(
        &mut client,
        "i-023",
        "terminal.terminate",
        json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(terminate.ok, "terminate 应成功: {terminate:?}");
    service.shutdown().await;
}

/// Unix：`sh` 执行前台作业时前台进程组切换到子进程，结束后回到根进程组。
#[cfg(unix)]
#[tokio::test]
async fn unix_foreground_job_emits_interactive_changed() {
    let dir = tempfile::tempdir().expect("创建临时目录失败");
    let mut service = TestService::start_with_config(interaction_config(dir.path())).await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    let created = create_terminal(
        &mut client,
        "i-101",
        json!({
            "mode": "pty",
            "cmd": "/bin/sh",
            "root_session_id": "ses-i",
            "name": "shell-job",
        }),
    )
    .await;

    let attach = call(
        &mut client,
        "i-102",
        "terminal.attach",
        json!({ "terminal_id": created.terminal_id, "cursor": created.cursor }),
    )
    .await;
    assert!(attach.ok, "attach 应成功: {attach:?}");

    let claim = call(
        &mut client,
        "i-103",
        "terminal.claim",
        json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(claim.ok, "claim 应成功: {claim:?}");

    // 等 shell 完成初始化后再提交前台作业。
    tokio::time::sleep(Duration::from_millis(200)).await;
    let send = call(
        &mut client,
        "i-104",
        "terminal.send",
        json!({ "terminal_id": created.terminal_id, "text": "sleep 5\n" }),
    )
    .await;
    assert!(send.ok, "send 应成功: {send:?}");

    let active = next_event_matching(&mut client, |event| {
        matches!(
            event.kind,
            EventKind::InteractiveChanged { active: true, .. }
        )
    })
    .await;
    match active.kind {
        EventKind::InteractiveChanged {
            active: true,
            pid,
            pgid,
            command,
            confidence,
        } => {
            assert_eq!(confidence, Confidence::Medium);
            assert!(pid.is_some() && pgid.is_some());
            assert!(
                command.as_deref().is_some_and(|cmd| cmd.contains("sleep")),
                "命令应包含 sleep: {command:?}"
            );
        }
        other => panic!("期望 active interactive_changed，实际 {other:?}"),
    }

    // 同一前台持续采样不应重复推送 `interactive_changed{active:true}`。
    let repeated = timeout(QUIET_WINDOW, async {
        loop {
            let event = next_event(&mut client).await;
            if matches!(
                event.kind,
                EventKind::InteractiveChanged { active: true, .. }
            ) {
                return;
            }
            if is_terminal_event(&event.kind) {
                return;
            }
        }
    })
    .await;
    assert!(
        repeated.is_err(),
        "同一前台不应重复发 interactive_changed: {repeated:?}"
    );

    // Ctrl-C 字节由 PTY 行规程投递给前台进程组。
    let interrupt = call(
        &mut client,
        "i-105",
        "terminal.send",
        json!({ "terminal_id": created.terminal_id, "data": "Aw==" }),
    )
    .await;
    assert!(interrupt.ok, "Ctrl-C 应发送成功: {interrupt:?}");

    let inactive = next_event_matching(&mut client, |event| {
        matches!(
            event.kind,
            EventKind::InteractiveChanged { active: false, .. }
        )
    })
    .await;
    match inactive.kind {
        EventKind::InteractiveChanged {
            active: false,
            confidence,
            ..
        } => assert_eq!(confidence, Confidence::Medium),
        other => panic!("期望 inactive interactive_changed，实际 {other:?}"),
    }

    let terminate = call(
        &mut client,
        "i-106",
        "terminal.terminate",
        json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(terminate.ok, "terminate 应成功: {terminate:?}");
    service.shutdown().await;
}
