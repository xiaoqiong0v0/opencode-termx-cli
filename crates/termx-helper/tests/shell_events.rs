//! helper 侧 shell 事件通道（Unix）：started.events、事件顺序、socket 清理与坏 token。

#![cfg(unix)]

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[path = "support/helper_client.rs"]
mod helper_client;
#[path = "support/shell_event_probe.rs"]
mod shell_event_probe;

use helper_client::Helper;
use termx_protocol::helper::{HelperEvent, HelperRequest};

/// 探针 steady-state 保留时间；足够父测试完成 terminate。
const PROBE_HOLD_MS: &str = "30000";
/// 错误 token 场景的保留时间。
const BAD_TOKEN_HOLD_MS: &str = "2000";
/// 等待 socket 被清理的上限。
const SOCKET_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
/// 轮询间隔。
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// 构造拉起 fake child 的 `spawn` 请求。
fn spawn_probe(record: &Path, extra_env: &[(&str, &str)]) -> HelperRequest {
    let exe = std::env::current_exe()
        .expect("测试二进制路径")
        .display()
        .to_string();
    let mut env = BTreeMap::new();
    env.insert(
        "TERMX_EVENT_PROBE_RECORD".to_string(),
        record.display().to_string(),
    );
    for (key, value) in extra_env {
        env.insert((*key).to_string(), (*value).to_string());
    }
    HelperRequest::Spawn {
        cmd: exe,
        args: vec![
            "--exact".to_string(),
            shell_event_probe::PROBE_TEST_NAME.to_string(),
            "--nocapture".to_string(),
        ],
        cwd: None,
        env: Some(env),
        cols: Some(120),
        rows: Some(30),
        events: true,
    }
}

/// 提取 `started.events`。
fn started_events(events: &[HelperEvent]) -> Option<bool> {
    events.iter().find_map(|event| match event {
        HelperEvent::Started { events, .. } => Some(*events),
        _ => None,
    })
}

/// 提取 shell 事件序列。
fn shell_events(events: &[HelperEvent]) -> Vec<(String, Option<i32>, Option<String>)> {
    events
        .iter()
        .filter_map(|event| match event {
            HelperEvent::ShellEvent {
                event, code, id, ..
            } => Some((event.clone(), *code, id.clone())),
            _ => None,
        })
        .collect()
}

/// 读取探针记录的 endpoint 路径。
fn recorded_socket(record: &Path) -> PathBuf {
    let endpoint = std::fs::read_to_string(record).expect("探针未记录 endpoint");
    let socket = endpoint
        .strip_prefix("unix:")
        .unwrap_or_else(|| panic!("endpoint 不是 unix: {endpoint:?}"));
    PathBuf::from(socket)
}

/// 轮询等待路径消失。
fn wait_until_missing(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !path.exists() {
            return true;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    !path.exists()
}

/// 探针入口：由父测试以 `--exact` 重新执行测试二进制触发。
#[test]
fn shell_event_probe_child() {
    shell_event_probe::run();
}

#[test]
fn events_are_forwarded_in_order_and_socket_is_cleaned() {
    let dir = tempfile::tempdir().expect("创建临时目录");
    let record = dir.path().join("endpoint.txt");

    let mut helper = Helper::start();
    helper.handshake();
    helper.send(&spawn_probe(
        &record,
        &[("TERMX_EVENT_PROBE_HOLD_MS", PROBE_HOLD_MS)],
    ));

    let events = helper.collect_until(|event| match event {
        HelperEvent::ShellEvent { event, .. } => event == "D",
        HelperEvent::Exited { .. } | HelperEvent::Signaled { .. } | HelperEvent::Error { .. } => {
            true
        }
        _ => false,
    });

    assert!(
        !events
            .iter()
            .any(|event| matches!(event, HelperEvent::Error { .. })),
        "不应出现 error: {events:?}"
    );
    assert_eq!(
        started_events(&events),
        Some(true),
        "started.events 必须诚实上报通道已建立: {events:?}"
    );

    let shell = shell_events(&events);
    let names: Vec<&str> = shell.iter().map(|(name, _, _)| name.as_str()).collect();
    assert_eq!(names, ["A", "B", "C", "D"], "事件顺序错误: {shell:?}");
    assert_eq!(shell[2].2.as_deref(), Some("cmd-1"), "C 应携带 command-id");
    assert_eq!(shell[3].1, Some(7), "D 应携带退出码");
    assert_eq!(shell[3].2.as_deref(), Some("cmd-1"), "D 应携带 command-id");

    let socket = recorded_socket(&record);
    assert!(
        socket.exists(),
        "terminate 前 socket 应存在: {}",
        socket.display()
    );
    helper.send(&HelperRequest::Terminate);
    assert!(
        wait_until_missing(&socket, SOCKET_CLEANUP_TIMEOUT),
        "terminate 后 socket 应被清理: {}",
        socket.display()
    );
}

#[test]
fn wrong_token_delivers_no_events() {
    let dir = tempfile::tempdir().expect("创建临时目录");
    let record = dir.path().join("endpoint.txt");

    let mut helper = Helper::start();
    helper.handshake();
    helper.send(&spawn_probe(
        &record,
        &[
            ("TERMX_EVENT_PROBE_HOLD_MS", BAD_TOKEN_HOLD_MS),
            ("TERMX_EVENT_PROBE_BAD_TOKEN", "1"),
        ],
    ));

    let deadline = Instant::now() + Duration::from_secs(2);
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        let Some(event) = helper.next_event_within(remaining) else {
            break;
        };
        assert!(
            !matches!(event, HelperEvent::ShellEvent { .. }),
            "错误 token 不应投递事件: {event:?}"
        );
    }

    helper.send(&HelperRequest::Terminate);
}
