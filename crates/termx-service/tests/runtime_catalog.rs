//! Runtime catalog 集成测试：`runtime.list` 与 `terminal.create(runtime_id)`。
//!
//! 使用 `MockFactory`，不启动真实 `termx-pwsh`；manifest 指向临时目录里的假
//! 可执行文件，验证的是 catalog 解析、路径安全与参数覆盖链路。

mod common;

use std::{path::Path, sync::Arc};

use common::{TestService, call, connect, handshake, handshake_root};
use tempfile::tempdir;
use termx_core::{MockFactory, TerminalId};
use termx_protocol::{
    CreateResult, ErrorCode, ListItem, Response, RuntimeListResult, TerminalLocator,
    TranscriptSource, TranscriptStatus, from_json, to_json,
};
use termx_runtime::{RuntimeEntry, RuntimeManifest, current_arch, current_platform};
use termx_service::ServiceConfig;
use termx_service::shell_events::{ShellEvent, ShellEventKind, ShellEventTracker, apply_event};
use time::OffsetDateTime;

/// 写入 runtime manifest 与假 bin，返回 manifest 中的 bin 绝对路径。
fn install_runtime(config_dir: &Path) -> std::path::PathBuf {
    let runtime_dir = config_dir.join("runtimes");
    let bin_dir = runtime_dir.join("bin");
    std::fs::create_dir_all(&bin_dir).expect("创建 runtime bin 目录失败");
    let bin_path = bin_dir.join("termx-pwsh-fake.exe");
    std::fs::write(&bin_path, b"fake runtime").expect("写假 runtime 失败");
    let bash_bin = bin_dir.join("termx-bash-fake.exe");
    std::fs::write(&bash_bin, b"fake bash runtime").expect("写假 bash runtime 失败");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for path in [&bin_path, &bash_bin] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
                .expect("设置执行权限失败");
        }
    }

    let other_platform = if current_platform() == "windows" {
        "linux"
    } else {
        "windows"
    };
    let manifest = RuntimeManifest {
        runtimes: vec![
            RuntimeEntry {
                id: "termx-pwsh".to_string(),
                name: "termx-pwsh".to_string(),
                version: "0.1.0".to_string(),
                platform: current_platform(),
                arch: current_arch(),
                bin: "bin/termx-pwsh-fake.exe".to_string(),
                args: vec!["--no-profile".to_string()],
            },
            RuntimeEntry {
                id: "termx-bash".to_string(),
                name: "termx-bash".to_string(),
                version: "0.1.0".to_string(),
                platform: current_platform(),
                arch: current_arch(),
                bin: "bin/termx-bash-fake.exe".to_string(),
                args: vec!["--no-profile".to_string()],
            },
            RuntimeEntry {
                id: "termx-missing".to_string(),
                name: "termx-missing".to_string(),
                version: "0.1.0".to_string(),
                platform: current_platform(),
                arch: current_arch(),
                bin: "bin/does-not-exist.exe".to_string(),
                args: Vec::new(),
            },
            RuntimeEntry {
                id: "other-platform".to_string(),
                name: "other-platform".to_string(),
                version: "0.1.0".to_string(),
                platform: other_platform.to_string(),
                arch: current_arch(),
                bin: "bin/termx-pwsh-fake.exe".to_string(),
                args: Vec::new(),
            },
        ],
    };
    std::fs::write(
        runtime_dir.join("index.json"),
        to_json(&manifest).expect("序列化 manifest 失败"),
    )
    .expect("写 manifest 失败");
    bin_path
}

/// 解析 `terminal.list`。
fn parse_list(response: Response) -> Vec<ListItem> {
    assert!(response.ok, "terminal.list 应成功: {response:?}");
    from_json(
        &to_json(&response.result.expect("缺少 list result")).expect("序列化 list result 失败"),
    )
    .expect("ListItem 解析失败")
}

/// 断言错误码。
fn expect_error(response: Response, code: ErrorCode) {
    assert!(!response.ok, "应失败: {response:?}");
    assert_eq!(response.error.expect("缺少 error").code, code);
}

#[tokio::test]
async fn runtime_list_is_readable_and_hides_bin_path() {
    let dir = tempdir().expect("创建临时目录失败");
    install_runtime(dir.path());
    let service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;

    for admin in [true, false] {
        let mut client = connect(&service).await;
        if admin {
            handshake(&mut client, &service.token).await;
        } else {
            handshake_root(&mut client, &service.token, "ses-rt-list").await;
        }
        let response = call(
            &mut client,
            "c-rt-list",
            "runtime.list",
            serde_json::json!({}),
        )
        .await;
        assert!(response.ok, "runtime.list 应成功: {response:?}");
        let text = to_json(&response.result.expect("缺少 runtime.list result")).expect("序列化");
        assert!(!text.contains("bin"), "runtime.list 不应暴露 bin: {text}");
        let result: RuntimeListResult = from_json(&text).expect("RuntimeListResult 解析失败");
        let runtime = result
            .runtimes
            .iter()
            .find(|runtime| runtime.id == "termx-pwsh")
            .expect("应包含 termx-pwsh");
        assert!(runtime.available);
        assert_eq!(runtime.platform, current_platform());
        assert!(
            result
                .runtimes
                .iter()
                .all(|runtime| runtime.id != "other-platform")
        );
        let missing = result
            .runtimes
            .iter()
            .find(|runtime| runtime.id == "termx-missing")
            .expect("缺失文件的兼容项仍应返回");
        assert!(!missing.available);
    }
}

#[tokio::test]
async fn create_with_runtime_id_uses_manifest_bin_and_args() {
    let dir = tempdir().expect("创建临时目录失败");
    let bin_path = install_runtime(dir.path());
    let service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;
    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;

    let created: CreateResult = {
        let response = call(
            &mut admin,
            "c-rt-create",
            "terminal.create",
            serde_json::json!({
                "mode": "pty",
                "runtime_id": "termx-pwsh",
                "root_session_id": "ses-rt",
                "name": "rt",
                "cols": 120,
                "rows": 30
            }),
        )
        .await;
        assert!(response.ok, "runtime 创建应成功: {response:?}");
        from_json(&to_json(&response.result.expect("缺少 create result")).expect("序列化"))
            .expect("CreateResult 解析失败")
    };

    let list = parse_list(
        call(
            &mut admin,
            "c-rt-list-created",
            "terminal.list",
            serde_json::json!({}),
        )
        .await,
    );
    let item = list
        .iter()
        .find(|item| item.terminal_id == created.terminal_id)
        .expect("新终端应出现在列表");
    assert_eq!(item.runtime_id.as_deref(), Some("termx-pwsh"));
    assert_eq!(std::path::PathBuf::from(&item.command), bin_path);
    assert_eq!(item.args, vec!["--no-profile".to_string()]);
    assert!(
        item.structured_events,
        "本地 termx-pwsh 建立事件通道后应声明结构化能力"
    );
}

/// termx-bash 本地 runtime 应声明结构化事件；直接启动 bash 保持 raw-only。
#[tokio::test]
async fn termx_bash_is_structured_while_plain_bash_is_raw_only() {
    let dir = tempdir().expect("创建临时目录失败");
    install_runtime(dir.path());
    let mut service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;
    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;

    let created: CreateResult = {
        let response = call(
            &mut admin,
            "c-bash-create",
            "terminal.create",
            serde_json::json!({
                "mode": "pty",
                "runtime_id": "termx-bash",
                "root_session_id": "ses-bash",
                "name": "bash",
                "cols": 120,
                "rows": 30
            }),
        )
        .await;
        assert!(response.ok, "termx-bash 创建应成功: {response:?}");
        from_json(&to_json(&response.result.expect("缺少 create result")).expect("序列化"))
            .expect("CreateResult 解析失败")
    };

    let list = parse_list(
        call(
            &mut admin,
            "c-bash-list",
            "terminal.list",
            serde_json::json!({}),
        )
        .await,
    );
    let item = list
        .iter()
        .find(|item| item.terminal_id == created.terminal_id)
        .expect("termx-bash 终端应出现在列表");
    assert_eq!(item.runtime_id.as_deref(), Some("termx-bash"));
    assert!(item.structured_events, "termx-bash 应声明结构化事件");

    let terminal_id = TerminalId::new(created.terminal_id.clone());
    {
        let registry = service.registry.lock().expect("registry 锁");
        assert!(
            registry
                .records_transcript_on_send(&by_id(&created.terminal_id))
                .expect("查询 termx-bash"),
            "termx-bash 应保留 send-time 记录，避免首段输出先于 C 被丢弃"
        );
    }

    let started = OffsetDateTime::now_utc();
    let ended = started + time::Duration::milliseconds(100);
    let mut tracker = ShellEventTracker::new();
    {
        let mut registry = service.registry.lock().expect("registry 锁");
        let start = ShellEvent {
            kind: ShellEventKind::CommandStart,
            code: None,
            id: Some("bash-1".to_string()),
            command: Some("echo hi".to_string()),
            started_at_ms: Some(millis(started)),
            ended_at_ms: None,
            data: None,
            dropped: false,
        };
        apply_event(&mut registry, &terminal_id, &mut tracker, &start, started).expect("C 事件");
    }
    {
        let mut registry = service.registry.lock().expect("registry 锁");
        let end = ShellEvent {
            kind: ShellEventKind::CommandEnd,
            code: Some(7),
            id: Some("bash-1".to_string()),
            command: None,
            started_at_ms: None,
            ended_at_ms: Some(millis(ended)),
            data: None,
            dropped: false,
        };
        assert!(
            apply_event(&mut registry, &terminal_id, &mut tracker, &end, ended).expect("D 事件")
        );
    }
    {
        let registry = service.registry.lock().expect("registry 锁");
        let commands = registry
            .transcript(&by_id(&created.terminal_id), None)
            .expect("读取 termx-bash transcript")
            .commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command, "echo hi");
        assert_eq!(commands[0].status, TranscriptStatus::Done);
        assert_eq!(commands[0].exit_code, Some(7));
        assert_eq!(commands[0].source, Some(TranscriptSource::ShellIntegration));
    }

    // 普通 bash（无 runtime_id）没有带外事件，必须保持 raw-only。
    let plain: CreateResult = {
        let response = call(
            &mut admin,
            "c-plain-bash",
            "terminal.create",
            serde_json::json!({
                "mode": "pty",
                "cmd": "bash",
                "args": [],
                "root_session_id": "ses-plain-bash",
                "name": "plain-bash",
                "cols": 120,
                "rows": 30
            }),
        )
        .await;
        assert!(response.ok, "普通 bash 创建应成功: {response:?}");
        from_json(&to_json(&response.result.expect("缺少 create result")).expect("序列化"))
            .expect("CreateResult 解析失败")
    };
    let list = parse_list(
        call(
            &mut admin,
            "c-plain-bash-list",
            "terminal.list",
            serde_json::json!({}),
        )
        .await,
    );
    let plain_item = list
        .iter()
        .find(|item| item.terminal_id == plain.terminal_id)
        .expect("普通 bash 终端应出现在列表");
    assert!(
        !plain_item.structured_events,
        "普通 bash 不应声明结构化事件，保持 raw-only"
    );
    {
        let registry = service.registry.lock().expect("registry 锁");
        assert!(
            registry
                .records_transcript_on_send(&by_id(&plain.terminal_id))
                .expect("查询普通 bash"),
            "普通 bash 仍应保留 send-time 记录"
        );
    }

    service.shutdown().await;
}

/// 以 terminal_id 定位。
fn by_id(terminal_id: &str) -> TerminalLocator {
    TerminalLocator {
        terminal_id: Some(terminal_id.to_string()),
        root_session_id: None,
        name: None,
    }
}

/// Unix 毫秒时间戳。
fn millis(at: OffsetDateTime) -> i64 {
    i64::try_from(at.unix_timestamp_nanos() / 1_000_000).expect("毫秒时间戳应在范围内")
}

#[tokio::test]
async fn event_channel_terminal_pre_records_pending_before_command_start() {
    let dir = tempdir().expect("创建临时目录失败");
    install_runtime(dir.path());
    let mut service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;
    let mut client = connect(&service).await;
    handshake(&mut client, &service.token).await;

    // 事件通道终端：send 先落 pending，C 到达后按代次认领，避免首段输出丢失。
    let response = call(
        &mut client,
        "c-events-create",
        "terminal.create",
        serde_json::json!({
            "mode": "pty",
            "runtime_id": "termx-pwsh",
            "root_session_id": "ses-events",
            "name": "events",
            "cols": 120,
            "rows": 30
        }),
    )
    .await;
    assert!(response.ok, "事件通道终端创建应成功: {response:?}");
    let created: CreateResult =
        from_json(&to_json(&response.result.expect("缺少 create result")).expect("序列化"))
            .expect("CreateResult 解析失败");
    let claimed = call(
        &mut client,
        "c-events-claim",
        "terminal.claim",
        serde_json::json!({ "terminal_id": created.terminal_id }),
    )
    .await;
    assert!(claimed.ok, "claim 应成功: {claimed:?}");
    let sent = call(
        &mut client,
        "c-events-send",
        "terminal.send",
        serde_json::json!({
            "terminal_id": created.terminal_id,
            "text": "run",
            "enter": true,
        }),
    )
    .await;
    assert!(sent.ok, "send 应成功: {sent:?}");

    {
        let registry = service.registry.lock().expect("registry 锁");
        assert!(
            registry
                .records_transcript_on_send(&by_id(&created.terminal_id))
                .expect("查询事件通道终端"),
            "事件通道终端应保留 send-time pending"
        );
        let result = registry
            .transcript(&by_id(&created.terminal_id), None)
            .expect("读取 transcript");
        assert_eq!(result.commands.len(), 1, "send 应预记录 pending 命令");
        assert_eq!(result.commands[0].command, "run");
        assert_eq!(result.commands[0].status, TranscriptStatus::Running);
    }

    // 宿主 C(command,start) 认领 pending；D 用宿主结束时间结束。
    let started = OffsetDateTime::now_utc();
    let ended = started + time::Duration::milliseconds(100);
    let terminal_id = TerminalId::new(created.terminal_id.clone());
    let mut tracker = ShellEventTracker::new();
    {
        let mut registry = service.registry.lock().expect("registry 锁");
        let start = ShellEvent {
            kind: ShellEventKind::CommandStart,
            code: None,
            id: Some("cmd-1".to_string()),
            command: Some("run".to_string()),
            started_at_ms: Some(millis(started)),
            ended_at_ms: None,
            data: None,
            dropped: false,
        };
        assert!(
            !apply_event(&mut registry, &terminal_id, &mut tracker, &start, started)
                .expect("C 事件")
        );
        let end = ShellEvent {
            kind: ShellEventKind::CommandEnd,
            code: Some(7),
            id: Some("cmd-1".to_string()),
            command: None,
            started_at_ms: None,
            ended_at_ms: Some(millis(ended)),
            data: None,
            dropped: false,
        };
        assert!(
            apply_event(&mut registry, &terminal_id, &mut tracker, &end, ended).expect("D 事件")
        );
    }
    {
        let registry = service.registry.lock().expect("registry 锁");
        let commands = registry
            .transcript(&by_id(&created.terminal_id), None)
            .expect("读取 transcript")
            .commands;
        assert_eq!(commands.len(), 1, "C 应新建而不是重复插入");
        assert_eq!(commands[0].command, "run");
        assert_eq!(commands[0].status, TranscriptStatus::Done);
        assert_eq!(commands[0].exit_code, Some(7));
        assert_eq!(commands[0].source, Some(TranscriptSource::ShellIntegration));
    }

    // 非事件通道终端仍保留 send-time 记录。
    let response = call(
        &mut client,
        "c-plain-create",
        "terminal.create",
        serde_json::json!({
            "mode": "pty",
            "cmd": "mock",
            "args": [],
            "root_session_id": "ses-plain",
            "name": "plain",
            "cols": 120,
            "rows": 30
        }),
    )
    .await;
    assert!(response.ok, "普通终端创建应成功: {response:?}");
    let plain: CreateResult =
        from_json(&to_json(&response.result.expect("缺少 create result")).expect("序列化"))
            .expect("CreateResult 解析失败");
    let claimed = call(
        &mut client,
        "c-plain-claim",
        "terminal.claim",
        serde_json::json!({ "terminal_id": plain.terminal_id }),
    )
    .await;
    assert!(claimed.ok, "claim 应成功: {claimed:?}");
    let sent = call(
        &mut client,
        "c-plain-send",
        "terminal.send",
        serde_json::json!({
            "terminal_id": plain.terminal_id,
            "text": "echo hi",
            "enter": true,
        }),
    )
    .await;
    assert!(sent.ok, "send 应成功: {sent:?}");
    {
        let registry = service.registry.lock().expect("registry 锁");
        let commands = registry
            .transcript(&by_id(&plain.terminal_id), None)
            .expect("读取 transcript")
            .commands;
        assert_eq!(commands.len(), 1, "普通终端应在 send 时记录命令");
        assert_eq!(commands[0].command, "echo hi");
    }

    service.shutdown().await;
}

#[tokio::test]
async fn runtime_errors_do_not_fall_back_to_custom_cmd() {
    let dir = tempdir().expect("创建临时目录失败");
    install_runtime(dir.path());
    let service = TestService::start_with_config_and_factory(
        ServiceConfig::new(dir.path().to_path_buf()),
        Some(Arc::new(MockFactory)),
    )
    .await;
    let mut admin = connect(&service).await;
    handshake(&mut admin, &service.token).await;

    let base = serde_json::json!({
        "mode": "pty",
        "root_session_id": "ses-rt",
        "name": "rt-err",
        "cols": 120,
        "rows": 30
    });
    let mut unknown = base.clone();
    unknown["runtime_id"] = serde_json::json!("unknown");
    expect_error(
        call(&mut admin, "c-rt-unknown", "terminal.create", unknown).await,
        ErrorCode::NotFound,
    );

    let mut conflict = base.clone();
    conflict["runtime_id"] = serde_json::json!("termx-pwsh");
    conflict["cmd"] = serde_json::json!("cmd.exe");
    expect_error(
        call(&mut admin, "c-rt-conflict", "terminal.create", conflict).await,
        ErrorCode::BadRequest,
    );

    let mut missing = base.clone();
    missing["runtime_id"] = serde_json::json!("termx-missing");
    expect_error(
        call(&mut admin, "c-rt-missing", "terminal.create", missing).await,
        ErrorCode::BadRequest,
    );

    let mut incompatible = base;
    incompatible["runtime_id"] = serde_json::json!("other-platform");
    expect_error(
        call(
            &mut admin,
            "c-rt-incompatible",
            "terminal.create",
            incompatible,
        )
        .await,
        ErrorCode::BadRequest,
    );
}
