//! `@web` 保留会话与启动信息：创建、无会话豁免、快照/列表字段与访问隔离。
#![cfg(any(windows, unix))]

mod common;

use std::time::Duration;

use common::{
    TestService, call, connect, create_terminal, handshake, handshake_root, next_response,
};
use futures_util::SinkExt;
use tempfile::tempdir;
use termx_protocol::{
    CreateResult, ErrorCode, LifecycleState, ListItem, PROTOCOL_VERSION, Response, StatusResult,
    WEB_ROOT_SESSION_ID,
};
use termx_service::{ClientIdlePolicy, ServiceConfig};
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;

/// 长运行 shell；`cwd` 与 `args` 用于验证启动信息回显。
#[cfg(windows)]
fn shell_params(root_session_id: &str, name: &str, cwd: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "cmd.exe",
        "args": ["/Q"],
        "cwd": cwd,
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 长运行 shell；`cwd` 与 `args` 用于验证启动信息回显。
#[cfg(unix)]
fn shell_params(root_session_id: &str, name: &str, cwd: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": "/bin/sh",
        "args": ["-i"],
        "cwd": cwd,
        "root_session_id": root_session_id,
        "name": name,
        "cols": 120,
        "rows": 30,
    })
}

/// 平台对应的启动程序与参数（与服务端回显比较）。
#[cfg(windows)]
fn expected_command() -> (&'static str, Vec<String>) {
    ("cmd.exe", vec!["/Q".to_string()])
}

/// 平台对应的启动程序与参数（与服务端回显比较）。
#[cfg(unix)]
fn expected_command() -> (&'static str, Vec<String>) {
    ("/bin/sh", vec!["-i".to_string()])
}

/// 解析 `terminal.list`。
fn parse_list(response: Response) -> Vec<ListItem> {
    assert!(response.ok, "terminal.list 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 list result")).expect("ListItem 解析失败")
}

/// 解析 `terminal.status`。
fn parse_status(response: Response) -> StatusResult {
    assert!(response.ok, "terminal.status 应成功: {response:?}");
    serde_json::from_value(response.result.expect("缺少 status result"))
        .expect("StatusResult 解析失败")
}

/// 断言错误码。
fn expect_error(response: Response, code: ErrorCode) {
    assert!(!response.ok, "应失败: {response:?}");
    assert_eq!(response.error.expect("缺少 error").code, code);
}

/// Web 资源必须包含远端注入授权流程（静态检查，不启动 SSH）。
#[test]
fn web_assets_expose_remote_injection_consent() {
    let app = include_str!("../src/web_assets/app.js");
    let index = include_str!("../src/web_assets/index.html");
    assert!(
        app.contains("consent_required"),
        "app.js 应处理 consent_required"
    );
    assert!(
        app.contains("allow_injection"),
        "app.js 重试应附带 allow_injection"
    );
    assert!(
        index.contains("termx-consent-dialog"),
        "index.html 应包含远端注入确认弹窗"
    );
}

/// 无结构化事件能力的终端必须隐藏/禁用信息页；能力来自 `structured_events`。
#[test]
fn web_hides_info_view_for_non_structured_terminals() {
    let app = include_str!("../src/web_assets/app.js");
    let css = include_str!("../src/web_assets/app.css");
    assert!(
        app.contains("structured_events === true"),
        "app.js 应按 structured_events 判定能力"
    );
    assert!(
        app.contains("elements.tabInfo.hidden") && app.contains("elements.tabInfo.disabled"),
        "app.js 应隐藏并禁用信息页入口"
    );
    assert!(
        app.contains("syncStructuredCapability"),
        "app.js 应在选择/刷新时同步能力"
    );
    assert!(
        css.contains(".termx-tab[hidden]"),
        "app.css 必须让 hidden 覆盖 inline-flex"
    );
}

/// 每个 terminal_id 必须有独立 xterm 实例与 DOM 容器，禁止单一全局 term 共用。
#[test]
fn web_keeps_per_terminal_xterm_views() {
    let app = include_str!("../src/web_assets/app.js");
    let index = include_str!("../src/web_assets/index.html");
    let css = include_str!("../src/web_assets/app.css");

    assert!(
        app.contains("var views = new Map()"),
        "app.js 应使用 Map<terminal_id, TerminalView>"
    );
    assert!(
        !app.contains("var term = null;"),
        "app.js 不应保留单一全局 xterm 实例"
    );
    assert!(
        app.contains("function createTerminalView(terminalId)")
            && app.contains("views.set(terminalId, view)"),
        "每个 terminal_id 应创建并登记独立视图"
    );
    assert!(
        app.contains("terminal.onData(function (data)")
            && app.contains("handleInput(terminalId, data)"),
        "onData 必须绑定创建时的 terminal_id，而不是切换后的 currentTerminalId"
    );
    assert!(
        app.contains("view.term.write(base64ToBytes(event.data))"),
        "输出事件应按 terminal_id 写入对应实例"
    );
    assert!(
        app.contains("terminal_id: view.terminalId"),
        "resize 应使用视图自己的 terminal_id"
    );
    assert!(
        app.contains("function disposeTerminalView(terminalId)")
            && app.contains("view.term.dispose()"),
        "终止/删除/force 替换时应释放实例"
    );
    assert!(
        app.contains("function pruneTerminalViews(items)") && app.contains("view.cursor"),
        "清理缺失视图并使用每终端独立游标"
    );
    assert!(
        !app.contains("term.clear()"),
        "不应再用全局 term.clear() 做终端隔离"
    );
    assert!(
        index.contains("termx-term-host")
            && css.contains(".termx-term-view")
            && css.contains(".termx-term-view-hidden"),
        "终端容器应支持每个 terminal_id 独立显隐"
    );
}

/// SSH 创建表单：固定 termx-pwsh、默认私钥提示、helper 字段移除、回环主机默认信任。
#[test]
fn web_ssh_form_converges_runtime_and_helper_fields() {
    let app = include_str!("../src/web_assets/app.js");
    let index = include_str!("../src/web_assets/index.html");

    assert!(
        !index.contains("termx-create-ssh-runtime") && !index.contains("系统 shell"),
        "SSH 表单不应再提供系统 shell 运行环境"
    );
    assert!(
        index.contains("留空自动尝试 ~/.ssh/id_ed25519 / ~/.ssh/id_rsa"),
        "私钥路径应提示留空探测顺序"
    );
    assert!(
        index.contains("不接入 shell 事件")
            && index.contains("read --tail-lines/--tail-chars/search"),
        "自定义程序区应说明 raw-only 与 read/search 获取方式"
    );
    for removed in [
        "termx-create-helper-path",
        "termx-create-upload-helper",
        "termx-create-helper-local",
        "termx-create-helper-sha",
        "termx-create-ssh-cmd",
    ] {
        assert!(!index.contains(removed), "index.html 不应再包含 {removed}");
    }

    assert!(
        app.contains("params.cmd = \"termx-pwsh\""),
        "app.js 应固定把 termx-pwsh 映射为 cmd"
    );
    assert!(
        !app.contains("私钥路径不能为空") && app.contains("{ type: \"key\", path: path }"),
        "私钥路径应允许留空并交给 service 探测"
    );
    assert!(
        !app.contains("helper_local_path = ") && !app.contains("upload_helper:"),
        "app.js 不应再传 helper_* 字段"
    );
    assert!(
        !app.contains("isLoopbackHost")
            && app.contains("host_key_policy: acceptAny ? \"accept_any\" : \"ask\"")
            && index.contains("高级 / 不安全选项"),
        "SSH 默认应为 Ask，AcceptAny 只能放在高级/不安全选项里"
    );
    assert!(
        app.contains("host_key_unknown") && app.contains("trusted_host_key"),
        "app.js 应处理未知主机密钥确认并附带一次性指纹"
    );
}

#[tokio::test]
async fn admin_can_create_web_terminal_and_read_launch_info() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let cwd = tempdir().expect("创建工作目录失败");
    let cwd_text = cwd.path().to_string_lossy().to_string();

    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;

    let created = create_terminal(
        &mut admin,
        "c-web-create",
        shell_params(WEB_ROOT_SESSION_ID, "web", Some(&cwd_text)),
    )
    .await;

    let list = parse_list(
        call(
            &mut admin,
            "c-web-list",
            "terminal.list",
            serde_json::json!({}),
        )
        .await,
    );
    let item = list
        .iter()
        .find(|item| item.terminal_id == created.terminal_id)
        .expect("@web 终端应出现在管理员列表");
    let (command, args) = expected_command();
    assert_eq!(item.root_session_id, WEB_ROOT_SESSION_ID);
    assert_eq!(item.command, command);
    assert_eq!(item.args, args);
    assert_eq!(item.cwd.as_deref(), Some(cwd_text.as_str()));

    let snapshot = parse_status(
        call(
            &mut admin,
            "c-web-status",
            "terminal.status",
            serde_json::json!({ "terminal_id": created.terminal_id }),
        )
        .await,
    )
    .snapshot;
    assert_eq!(snapshot.command, command);
    assert_eq!(snapshot.args, args);
    assert_eq!(snapshot.cwd.as_deref(), Some(cwd_text.as_str()));
}

#[tokio::test]
async fn web_terminal_skips_clientless_expiry() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start_with_config(
        ServiceConfig::new(dir.path().to_path_buf())
            .with_idle_tick_interval(Duration::from_millis(20))
            .with_client_idle_timeout(Duration::from_millis(100))
            .with_client_idle_policy(ClientIdlePolicy::Any)
            .with_session_ttl(Duration::from_millis(50)),
    )
    .await;

    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;
    let created: CreateResult = create_terminal(
        &mut admin,
        "c-web-idle-create",
        shell_params(WEB_ROOT_SESSION_ID, "web-idle", None),
    )
    .await;

    // 超过 TTL + 超时仍应运行：@web 不做无会话扫描。
    sleep(Duration::from_millis(400)).await;
    let snapshot = parse_status(
        call(
            &mut admin,
            "c-web-idle-status",
            "terminal.status",
            serde_json::json!({ "terminal_id": created.terminal_id }),
        )
        .await,
    )
    .snapshot;
    assert_eq!(snapshot.state, LifecycleState::Running);
}

#[tokio::test]
async fn non_admin_cannot_see_or_access_web_terminals() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;

    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;
    let created = create_terminal(
        &mut admin,
        "c-web-iso-create",
        shell_params(WEB_ROOT_SESSION_ID, "web-iso", None),
    )
    .await;

    let mut normal = connect(&service).await;
    handshake_root(&mut normal, &service.token, "ses-other").await;
    let list = parse_list(
        call(
            &mut normal,
            "c-web-iso-list",
            "terminal.list",
            serde_json::json!({}),
        )
        .await,
    );
    assert!(
        list.iter()
            .all(|item| item.terminal_id != created.terminal_id),
        "普通会话不应看到 @web 终端"
    );
    expect_error(
        call(
            &mut normal,
            "c-web-iso-status",
            "terminal.status",
            serde_json::json!({ "terminal_id": created.terminal_id }),
        )
        .await,
        ErrorCode::PermissionDenied,
    );

    // 普通连接不得把 @web 作为自身 root。
    let mut spoof = connect(&service).await;
    let request = serde_json::json!({
        "id": "h-web-spoof",
        "method": "hello",
        "params": {
            "proto": PROTOCOL_VERSION,
            "token": service.token,
            "client": "io-test",
            "root_session_id": WEB_ROOT_SESSION_ID,
        }
    })
    .to_string();
    spoof
        .send(Message::Text(request.into()))
        .await
        .expect("发送 hello 失败");
    let response = next_response(&mut spoof).await;
    expect_error(response, ErrorCode::BadRequest);
}

/// 管理员 `session.list` 应包含 `@web`，供 Web 两层选择使用。
#[tokio::test]
async fn session_list_exposes_web_root() {
    let dir = tempdir().expect("创建临时目录失败");
    let service = TestService::start(dir.path()).await;
    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;
    create_terminal(
        &mut admin,
        "c-web-session-create",
        shell_params(WEB_ROOT_SESSION_ID, "web-session", None),
    )
    .await;

    let response = call(
        &mut admin,
        "c-web-session-list",
        "session.list",
        serde_json::json!({}),
    )
    .await;
    assert!(response.ok, "session.list 应成功: {response:?}");
    let result: termx_protocol::SessionListResult =
        serde_json::from_value(response.result.expect("缺少 session.list result"))
            .expect("SessionListResult 解析失败");
    let web = result
        .sessions
        .iter()
        .find(|session| session.root_session_id == WEB_ROOT_SESSION_ID)
        .expect("session.list 应包含 @web");
    assert_eq!(web.terminals, 1);
    assert!(!web.live);
}
