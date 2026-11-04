//! 无存活会话保活集成测试：真实 ticker + PTY 后端。
//!
//! 新语义：只有 `root_session_id` 的会话心跳能让终端保活；Web/终端附着
//! 不参与。用短 TTL/超时验证：
//! - 持续心跳不终止，停止心跳后按超时终止；
//! - 旧的空参数 heartbeat 不影响终端保活；
//! - `no_children` 在用户子进程存在时续期，子进程退出后终止；
//! - `any` 有子进程也终止；`never` 永不因无会话终止。

#![cfg(any(windows, unix))]

mod common;

use std::time::{Duration, Instant};

use common::{Client, TestService, call, connect, create_terminal, handshake, next_event_matching};
use tempfile::tempdir;
use termx_protocol::{EventKind, LifecycleState, StatusResult};
use termx_service::{ClientIdlePolicy, ServiceConfig};
use tokio::time::{sleep, timeout};

/// ticker 周期。
const TICK: Duration = Duration::from_millis(20);
/// 无存活会话超时；足够覆盖创建/查询的状态开销。
const CLIENT_TIMEOUT: Duration = Duration::from_millis(200);
/// 会话心跳 TTL；测试使用短值，避免等待默认 90s。
const SESSION_TTL: Duration = Duration::from_millis(150);

/// 不产生用户子进程的长运行 shell。
#[cfg(windows)]
fn idle_shell_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "cmd.exe",
        "args": [],
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 不产生用户子进程的长运行 shell。
#[cfg(unix)]
fn idle_shell_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "/bin/sh",
        "args": [],
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 先产生一个用户子进程，子进程退出后根 shell 继续驻留。
#[cfg(windows)]
fn child_then_idle_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "cmd.exe",
        "args": ["/C", "ping -n 4 127.0.0.1 > nul & pause"],
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 先产生一个用户子进程，子进程退出后根 shell 继续驻留。
#[cfg(unix)]
fn child_then_idle_params(root_session_id: &str, name: &str) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "/bin/sh",
        "args": ["-c", "sleep 2; exec sh"],
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 读取 `terminal.status` 快照。
async fn status(client: &mut Client, id: &str, terminal_id: &str) -> StatusResult {
    let response = call(
        client,
        id,
        "terminal.status",
        serde_json::json!({ "terminal_id": terminal_id }),
    )
    .await;
    assert!(response.ok, "terminal.status 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 status result"))
        .expect("StatusResult 解析失败")
}

/// 轮询直到状态匹配；超时返回最后一次状态。
async fn wait_state(
    client: &mut Client,
    prefix: &str,
    terminal_id: &str,
    expected: LifecycleState,
    max: Duration,
) -> LifecycleState {
    let deadline = Instant::now() + max;
    loop {
        let snapshot = status(client, &format!("{prefix}-status"), terminal_id)
            .await
            .snapshot;
        if snapshot.state == expected || Instant::now() >= deadline {
            return snapshot.state;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// 发送新式会话心跳。
async fn heartbeat(client: &mut Client, root_session_id: &str) {
    let response = call(
        client,
        "heartbeat",
        "heartbeat",
        serde_json::json!({ "root_session_id": root_session_id }),
    )
    .await;
    assert!(response.ok, "heartbeat 应成功: {response:?}");
}

#[tokio::test]
async fn heartbeat_keeps_terminal_alive_and_stopping_terminates() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf())
            .with_idle_tick_interval(TICK)
            .with_client_idle_timeout(CLIENT_TIMEOUT)
            .with_client_idle_policy(ClientIdlePolicy::NoChildren)
            .with_session_ttl(SESSION_TTL),
    )
    .await;

    let mut observer = connect(&service).await;
    handshake(&mut observer, &service.token).await;
    let (subscribe, _) = common::call_collect(
        &mut observer,
        "c-subscribe-all",
        "terminal.subscribe",
        serde_json::json!({ "all": true }),
    )
    .await;
    assert!(subscribe.ok, "全局订阅应成功: {subscribe:?}");

    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;
    let root = "ses-heartbeat";
    let created = create_terminal(
        &mut client,
        "c-create-heartbeat",
        idle_shell_params(root, "heartbeat"),
    )
    .await;

    // 持续心跳覆盖超时窗口的时间：不应终止。
    for _ in 0..8 {
        heartbeat(&mut client, root).await;
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        status(&mut client, "c-status-live", &created.terminal_id)
            .await
            .snapshot
            .state,
        LifecycleState::Running,
        "有心跳时不应终止"
    );

    // 停止心跳：TTL + 超时后终止，事件来源 client_timeout。
    let interrupted = timeout(
        Duration::from_secs(5),
        next_event_matching(&mut observer, |event| {
            event.terminal_id == created.terminal_id
                && matches!(
                    &event.kind,
                    EventKind::Interrupted { source, .. } if source == "client_timeout"
                )
        }),
    )
    .await
    .expect("等待 client_timeout 事件超时");
    assert!(matches!(
        interrupted.kind,
        EventKind::Interrupted {
            ref signal,
            ref source
        } if signal == "SIGTERM" && source == "client_timeout"
    ));
    assert_eq!(
        wait_state(
            &mut client,
            "c-heartbeat",
            &created.terminal_id,
            LifecycleState::Interrupted,
            Duration::from_secs(3),
        )
        .await,
        LifecycleState::Interrupted
    );
}

#[tokio::test]
async fn legacy_empty_heartbeat_does_not_keep_terminal_alive() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf())
            .with_idle_tick_interval(TICK)
            .with_client_idle_timeout(CLIENT_TIMEOUT)
            .with_client_idle_policy(ClientIdlePolicy::NoChildren)
            .with_session_ttl(SESSION_TTL),
    )
    .await;

    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;
    let created = create_terminal(
        &mut client,
        "c-create-legacy",
        idle_shell_params("ses-legacy", "legacy"),
    )
    .await;

    // 旧客户端只发空参数心跳：不建立会话心跳，终端仍会超时。
    for _ in 0..4 {
        let response = call(
            &mut client,
            "heartbeat-legacy",
            "heartbeat",
            serde_json::json!({}),
        )
        .await;
        assert!(response.ok);
        sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        wait_state(
            &mut client,
            "c-legacy",
            &created.terminal_id,
            LifecycleState::Interrupted,
            Duration::from_secs(3),
        )
        .await,
        LifecycleState::Interrupted,
        "空参数 heartbeat 不应阻止无会话终止"
    );
}

#[tokio::test]
async fn no_children_keeps_alive_until_child_exits_then_terminates() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf())
            .with_idle_tick_interval(TICK)
            .with_client_idle_timeout(CLIENT_TIMEOUT)
            .with_client_idle_policy(ClientIdlePolicy::NoChildren)
            .with_session_ttl(SESSION_TTL),
    )
    .await;

    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;
    let created = create_terminal(
        &mut client,
        "c-create-child",
        child_then_idle_params("ses-child", "child"),
    )
    .await;

    // 用户子进程仍在运行：到期后应续期而不是终止。
    sleep(Duration::from_millis(1000)).await;
    assert_eq!(
        status(&mut client, "c-status-child", &created.terminal_id)
            .await
            .snapshot
            .state,
        LifecycleState::Running,
        "存在用户子进程时 no_children 仍应保活"
    );

    // 子进程退出后重新计时，最终终止。
    assert_eq!(
        wait_state(
            &mut client,
            "c-child",
            &created.terminal_id,
            LifecycleState::Interrupted,
            Duration::from_secs(10),
        )
        .await,
        LifecycleState::Interrupted,
        "子进程退出后应按超时终止"
    );
}

#[tokio::test]
async fn any_policy_terminates_even_with_children() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf())
            .with_idle_tick_interval(TICK)
            .with_client_idle_timeout(CLIENT_TIMEOUT)
            .with_client_idle_policy(ClientIdlePolicy::Any)
            .with_session_ttl(SESSION_TTL),
    )
    .await;

    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;
    let created = create_terminal(
        &mut client,
        "c-create-any",
        child_then_idle_params("ses-any", "any"),
    )
    .await;

    assert_eq!(
        wait_state(
            &mut client,
            "c-any",
            &created.terminal_id,
            LifecycleState::Interrupted,
            Duration::from_secs(3),
        )
        .await,
        LifecycleState::Interrupted,
        "any 策略不应等待子进程退出"
    );
}

#[tokio::test]
async fn never_policy_never_terminates_without_session() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf())
            .with_idle_tick_interval(TICK)
            .with_client_idle_timeout(CLIENT_TIMEOUT)
            .with_client_idle_policy(ClientIdlePolicy::Never)
            .with_session_ttl(SESSION_TTL),
    )
    .await;

    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;
    let created = create_terminal(
        &mut client,
        "c-create-never",
        idle_shell_params("ses-never", "never"),
    )
    .await;

    sleep(Duration::from_millis(800)).await;
    assert_eq!(
        status(&mut client, "c-status-never", &created.terminal_id)
            .await
            .snapshot
            .state,
        LifecycleState::Running,
        "never 策略不应因无会话终止"
    );
}

#[tokio::test]
async fn session_closed_removes_liveness_immediately() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf())
            .with_idle_tick_interval(TICK)
            .with_client_idle_timeout(CLIENT_TIMEOUT)
            .with_client_idle_policy(ClientIdlePolicy::NoChildren)
            .with_session_ttl(Duration::from_secs(30)),
    )
    .await;

    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;
    let root = "ses-closed";
    let created = create_terminal(
        &mut client,
        "c-create-closed",
        idle_shell_params(root, "closed"),
    )
    .await;
    heartbeat(&mut client, root).await;

    // 会话关闭后无需等待 TTL，直接按超时终止。
    let closed = call(
        &mut client,
        "c-session-closed",
        "session_closed",
        serde_json::json!({ "root_session_id": root }),
    )
    .await;
    assert!(closed.ok, "session_closed 应成功: {closed:?}");

    assert_eq!(
        wait_state(
            &mut client,
            "c-closed",
            &created.terminal_id,
            LifecycleState::Interrupted,
            Duration::from_secs(3),
        )
        .await,
        LifecycleState::Interrupted
    );
}
