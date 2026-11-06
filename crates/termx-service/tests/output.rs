//! `terminal.output`：raw 输出尾部/倒搜，以及结构化能力字段在 list/status 中透传。

#![cfg(any(windows, unix))]

mod common;

use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use common::{TestService, call, connect, create_terminal, handshake};
use tempfile::tempdir;
use termx_core::{MockFactory, TerminalId};
use termx_protocol::{
    EventKind, ListItem, OutputResult, Response, StatusResult, Stream, from_json,
};
use termx_service::ServiceConfig;

/// 解析 `terminal.list`。
fn parse_list(response: Response) -> Vec<ListItem> {
    assert!(response.ok, "terminal.list 应成功: {response:?}");
    from_json(
        &serde_json::to_string(&response.result.expect("缺少 list result"))
            .expect("序列化 list result"),
    )
    .expect("ListItem 解析失败")
}

/// 解析 `terminal.status`。
fn parse_status(response: Response) -> StatusResult {
    assert!(response.ok, "terminal.status 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 status result"))
        .expect("StatusResult 解析失败")
}

/// 解析 `terminal.output`。
fn parse_output(response: Response) -> OutputResult {
    assert!(response.ok, "terminal.output 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 output result"))
        .expect("OutputResult 解析失败")
}

/// 创建 mock 终端后直接注入两段输出（不经过 PTY）。
async fn create_with_output(service: &TestService) -> String {
    let mut client = connect(service).await;
    handshake(&mut client, &service.token).await;
    let created = create_terminal(
        &mut client,
        "c-output-create",
        serde_json::json!({
            "mode": "pty",
            "cmd": "mock",
            "args": [],
            "root_session_id": "ses-output",
            "name": "output",
            "cols": 120,
            "rows": 30
        }),
    )
    .await;

    {
        let mut registry = service.registry.lock().expect("registry 锁");
        let id = TerminalId::new(created.terminal_id.clone());
        for data in ["alpha one\nbeta alpha two\n", "gamma alpha\n"] {
            registry
                .report_event(
                    &id,
                    EventKind::Output {
                        data: BASE64_STANDARD.encode(data.as_bytes()),
                        stream: Stream::Stdout,
                    },
                )
                .expect("注入输出失败");
        }
    }
    created.terminal_id
}

/// 普通终端（无结构化事件）也能尾部读取与倒搜。
#[tokio::test]
async fn terminal_output_works_for_raw_only_terminal() {
    let dir = tempdir().expect("创建临时目录失败");
    let mut service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;
    let terminal_id = create_with_output(&service).await;

    {
        let mut client = connect(&service).await;
        handshake(&mut client, &service.token).await;

        let list = parse_list(
            call(
                &mut client,
                "c-output-list",
                "terminal.list",
                serde_json::json!({}),
            )
            .await,
        );
        let item = list
            .iter()
            .find(|item| item.terminal_id == terminal_id)
            .expect("终端应在列表");
        assert!(!item.structured_events, "普通终端不应声明结构化事件");

        let tail = parse_output(
            call(
                &mut client,
                "c-output-tail",
                "terminal.output",
                serde_json::json!({ "terminal_id": terminal_id, "tail_lines": 2 }),
            )
            .await,
        );
        assert_eq!(tail.text, "beta alpha two\ngamma alpha\n");

        let found = parse_output(
            call(
                &mut client,
                "c-output-search",
                "terminal.output",
                serde_json::json!({
                    "terminal_id": terminal_id,
                    "pattern": "alpha",
                    "nth": 2,
                    "only_matches": true
                }),
            )
            .await,
        );
        assert_eq!(found.text, "beta alpha two");

        // 绝对行号接口不存在：未知方法应返回 protocol_error/bad_request 之一。
        let unknown = call(
            &mut client,
            "c-output-line",
            "terminal.read_line",
            serde_json::json!({ "terminal_id": terminal_id, "line": 1 }),
        )
        .await;
        assert!(!unknown.ok, "不应存在绝对行号 API");
    }

    service.shutdown().await;
}

/// 结构化能力字段出现在 list/status，并可用于 Web 决定是否显示信息页。
#[tokio::test]
async fn structured_events_flag_is_exposed_and_independent_of_output() {
    let dir = tempdir().expect("创建临时目录失败");
    let mut service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;
    let terminal_id = create_with_output(&service).await;

    // 模拟真实带外事件通道建立后的条目（测试直接置位，不启动 termx-pwsh）。
    // 结构化终端只读 transcript；这里补一条命令输出作为 read/search 的文本源。
    {
        let mut registry = service.registry.lock().expect("registry 锁");
        let id = TerminalId::new(terminal_id.clone());
        registry.get_mut(&id).expect("终端存在").shell_events = true;
        let now = time::OffsetDateTime::now_utc();
        registry
            .record_transcript_command(&id, "echo hi", now)
            .expect("记录命令");
        registry
            .record_transcript_output(&id, "echo hi\r\nhi\r\n", now)
            .expect("记录输出");
    }

    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;
    let list = parse_list(
        call(
            &mut client,
            "c-structured-list",
            "terminal.list",
            serde_json::json!({}),
        )
        .await,
    );
    let item = list
        .iter()
        .find(|item| item.terminal_id == terminal_id)
        .expect("终端应在列表");
    assert!(item.structured_events, "结构化终端应在列表中标记能力");

    let snapshot = parse_status(
        call(
            &mut client,
            "c-structured-status",
            "terminal.status",
            serde_json::json!({ "terminal_id": terminal_id }),
        )
        .await,
    )
    .snapshot;
    assert!(snapshot.structured_events, "快照应携带结构化能力");

    let tail = parse_output(
        call(
            &mut client,
            "c-structured-tail",
            "terminal.output",
            serde_json::json!({ "terminal_id": terminal_id, "tail_chars": 2 }),
        )
        .await,
    );
    assert_eq!(tail.text, "hi", "结构化终端应读 transcript 输出");

    // 之前注入的 raw PTY（含 alpha）不得泄漏到 read/search。
    let raw_only = parse_output(
        call(
            &mut client,
            "c-structured-raw",
            "terminal.output",
            serde_json::json!({ "terminal_id": terminal_id, "pattern": "alpha" }),
        )
        .await,
    );
    assert_eq!(raw_only.text, "", "结构化终端不得回退到 raw PTY");

    service.shutdown().await;
}

/// 未知终端返回 not_found。
#[tokio::test]
async fn terminal_output_rejects_unknown_terminal() {
    let dir = tempdir().expect("创建临时目录失败");
    let mut service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;
    let response = call(
        &mut client,
        "c-output-missing",
        "terminal.output",
        serde_json::json!({ "terminal_id": "missing", "tail_lines": 1 }),
    )
    .await;
    assert!(!response.ok);
    assert_eq!(
        response.error.expect("缺少 error").code,
        termx_protocol::ErrorCode::NotFound
    );
    service.shutdown().await;
}
