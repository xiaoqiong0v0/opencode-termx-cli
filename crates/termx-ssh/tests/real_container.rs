//! 真实 SSH 容器集成测试（env-gated）。
//!
//! 环境变量：`TERMX_TEST_SSH_HOST`、`TERMX_TEST_SSH_PORT`、`TERMX_TEST_SSH_USER`、
//! `TERMX_TEST_SSH_PASSWORD`。任一缺失时直接跳过（不 fail）。
//!
//! helper 上传测试还需要 `TERMX_TEST_SSH_HELPER_BIN` 指向 Linux 静态 helper；
//! 版本不匹配测试使用本地临时脚本，不依赖该变量。
//!
//! 远端 shell 事件测试（T4）额外需要 `TERMX_TEST_SSH_EVENT_PROBE` 指定容器内可用的
//! 解释器（如 `python3`）；未设置时跳过，不 fail。
//!
//! 密码只从环境读取，测试失败信息不打印配置/凭据。

use std::{
    env,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use tempfile::tempdir;
use termx_proc::ProcEvent;
use termx_protocol::{
    SshAuth, SshConfig,
    injection::{InjectionConsentMode, InjectionPlan},
    shell_events::ShellEventKind,
};
use termx_ssh::{HelperProgram, SshConnectRequest, SshSession};
use tokio::{sync::mpsc::Receiver, time::Instant};

/// 单条事件等待上限。
const WAIT: Duration = Duration::from_secs(20);

/// 远端临时目录前缀；与 `termx-ssh` 的 `mktemp` 模板保持一致。
const REMOTE_TEMP_PATTERN: &str = "/tmp/termx-helper.*";

/// 容器内 shell 事件探针（python3）。
///
/// 读取 helper 注入的 `TERMX_EVENT_ENDPOINT` / `TERMX_EVENT_TOKEN`，连接 UDS 后
/// 发送 hello 与 C/D（D 携带 code=7、id=cmd-1）；发送后短暂停留，确保事件先于
/// 进程退出被 helper 转发。token 只在子进程内存中使用，不打印、不落盘。
const SHELL_EVENT_PROBE_SCRIPT: &str = r#"import json, os, socket, sys, time

endpoint = os.environ.get("TERMX_EVENT_ENDPOINT", "")
token = os.environ.get("TERMX_EVENT_TOKEN", "")
if not endpoint or not token:
    print("hello_missing_event_env", flush=True)
    sys.exit(3)

path = endpoint[len("unix:"):] if endpoint.startswith("unix:") else endpoint
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.settimeout(5)
sock.connect(path)
stream = sock.makefile("w", encoding="utf-8")


def send(frame):
    stream.write(json.dumps(frame) + "\n")
    stream.flush()


send({"v": 1, "token": token})
send({"v": 1, "event": "C", "id": "cmd-1"})
time.sleep(0.1)
send({"v": 1, "event": "D", "id": "cmd-1", "code": 7})
print("hello_shell_events_ssh", flush=True)
time.sleep(1.0)
sock.close()
"#;

/// 上传测试串行锁：清理断言通过对比前后目录集合判断，并行执行会互相干扰。
static HELPER_UPLOAD_LOCK: Mutex<()> = Mutex::new(());

/// 串行执行上传相关测试（忽略锁中毒，避免首个失败掩盖后续结果）。
fn lock_helper_uploads() -> MutexGuard<'static, ()> {
    HELPER_UPLOAD_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

/// 从环境变量构造测试配置；缺失时返回 `None`。
fn test_config() -> Option<SshConfig> {
    let host = env::var("TERMX_TEST_SSH_HOST").ok()?;
    let port = env::var("TERMX_TEST_SSH_PORT").ok()?.parse::<u16>().ok()?;
    let user = env::var("TERMX_TEST_SSH_USER").ok()?;
    let password = env::var("TERMX_TEST_SSH_PASSWORD").ok()?;
    Some(SshConfig {
        host,
        port,
        user,
        auth: SshAuth::Password { password },
        helper_path: None,
        helper_local_path: None,
        helper_sha256: None,
        upload_helper: false,
        // 测试容器使用临时主机密钥，显式接受。
        accept_any_host_key: true,
        host_key_policy: None,
        trusted_host_key: None,
        known_hosts_path: None,
        injection: InjectionPlan::default(),
    })
}

/// 跳过测试时输出一致提示。
fn skip() -> Option<SshConfig> {
    let config = test_config();
    if config.is_none() {
        eprintln!("跳过 SSH 集成测试：未设置 TERMX_TEST_SSH_* 环境变量");
    }
    config
}

/// 测试专用 helper 安装目标：按进程号隔离，避免影响容器内其他文件。
fn test_helper_path(tag: &str) -> String {
    format!("/tmp/termx-helper-{tag}-{}", std::process::id())
}

/// helper 测试配置：在基础配置上启用本地上传（安装到测试专用路径）。
///
/// 需要额外设置 `TERMX_TEST_SSH_HELPER_BIN` 指向 musl 静态 helper；缺失时跳过。
fn helper_config() -> Option<SshConfig> {
    let mut config = skip()?;
    let binary = match env::var("TERMX_TEST_SSH_HELPER_BIN") {
        Ok(binary) if !binary.trim().is_empty() => binary,
        _ => {
            eprintln!("跳过 SSH helper 测试：未设置 TERMX_TEST_SSH_HELPER_BIN");
            return None;
        }
    };
    config.helper_local_path = Some(binary);
    config.upload_helper = true;
    config.helper_path = Some(test_helper_path("test"));
    // 测试直接调用 `termx-ssh`，需自行提供注入授权（service 会从请求装配）。
    config.injection = InjectionPlan {
        consent: InjectionConsentMode::Allow,
        allow: true,
        ..Default::default()
    };
    Some(config)
}

/// 远端 shell 事件探针解释器（容器内为 `python3`）；未设置时跳过。
fn shell_event_probe() -> Option<String> {
    match env::var("TERMX_TEST_SSH_EVENT_PROBE") {
        Ok(interpreter) if !interpreter.trim().is_empty() => Some(interpreter),
        _ => {
            eprintln!("跳过 SSH shell 事件测试：未设置 TERMX_TEST_SSH_EVENT_PROBE");
            None
        }
    }
}

/// 从 helper 配置派生出纯 fallback 配置，用于独立核对远端状态。
fn fallback_config(config: &SshConfig) -> SshConfig {
    let mut plain = config.clone();
    plain.helper_path = None;
    plain.helper_local_path = None;
    plain.helper_sha256 = None;
    plain.upload_helper = false;
    plain
}

/// 等待终态事件（`Exited` / `Signaled`），期间可把输出收集到 `output`。
fn collect_until_terminal(
    runtime: &tokio::runtime::Runtime,
    events: &mut Receiver<ProcEvent>,
    output: Option<&mut Vec<u8>>,
    deadline: Instant,
) -> Option<ProcEvent> {
    let mut output = output;
    runtime.block_on(async {
        loop {
            match tokio::time::timeout_at(deadline, events.recv()).await {
                Ok(Some(ProcEvent::Output { data, .. })) => {
                    if let Some(buffer) = output.as_deref_mut() {
                        buffer.extend_from_slice(&data);
                    }
                }
                Ok(Some(terminal)) => return Some(terminal),
                Ok(None) | Err(_) => return None,
            }
        }
    })
}

/// 用独立 fallback 会话执行远端命令，返回（解码后的输出，终态事件）。
fn run_remote(config: &SshConfig, command: &str) -> (String, Option<ProcEvent>) {
    let (session, mut events) =
        SshSession::connect(config, SshConnectRequest::exec_no_pty(command))
            .expect("远端命令连接失败");
    assert!(!session.helper(), "远端命令应使用 fallback 会话");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("测试运行时");
    let mut output = Vec::new();
    let terminal = collect_until_terminal(
        &runtime,
        &mut events,
        Some(&mut output),
        Instant::now() + WAIT,
    );
    (String::from_utf8_lossy(&output).to_string(), terminal)
}

/// 列出远端当前存在的临时 helper 目录。
fn list_temp_dirs(config: &SshConfig) -> Vec<String> {
    let command = format!("sh -c 'ls -d {REMOTE_TEMP_PATTERN} 2>/dev/null || true'");
    let (output, terminal) = run_remote(config, &command);
    assert!(
        matches!(terminal, Some(ProcEvent::Exited(0))),
        "列目录应成功，实际 {terminal:?}"
    );
    output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

/// 等待本次上传创建的临时目录被清理：结果集合不得出现连接前不存在的新目录。
fn assert_no_new_temp_dirs(config: &SshConfig, before: &[String]) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let after = list_temp_dirs(config);
        let leftover: Vec<&String> = after
            .iter()
            .filter(|dir| !before.iter().any(|old| old == *dir))
            .collect();
        if leftover.is_empty() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "上传结束后仍有临时目录残留: {leftover:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// exec：输出包含标记、退出码为 3。
#[test]
fn exec_reports_output_and_exit_code() {
    let Some(config) = skip() else {
        return;
    };
    let (session, mut events) = SshSession::connect(
        &config,
        SshConnectRequest::exec("sh -c 'echo hello_ssh; exit 3'", 80, 24),
    )
    .expect("SSH 连接/认证失败");
    assert!(session.peer().contains('@'));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("测试运行时");
    let mut output = Vec::new();
    let terminal = collect_until_terminal(
        &runtime,
        &mut events,
        Some(&mut output),
        Instant::now() + WAIT,
    );

    assert!(
        String::from_utf8_lossy(&output).contains("hello_ssh"),
        "应收到远端输出，实际 {} 字节",
        output.len()
    );
    match terminal {
        Some(ProcEvent::Exited(3)) => {}
        other => panic!("应收到退出码 3，实际 {other:?}"),
    }
}

/// shell：write 生效、resize 可调用、terminate 后有终态。
#[test]
fn shell_supports_write_resize_and_terminate() {
    let Some(config) = skip() else {
        return;
    };
    let (session, mut events) =
        SshSession::connect(&config, SshConnectRequest::shell(80, 24)).expect("SSH shell 连接失败");

    session.write(b"echo hi\n").expect("写入失败");
    session.resize(100, 40).expect("resize 失败");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("测试运行时");
    let deadline = Instant::now() + WAIT;
    let mut output = Vec::new();
    let mut saw_hi = false;
    runtime.block_on(async {
        while let Ok(Some(ProcEvent::Output { data, .. })) =
            tokio::time::timeout_at(deadline, events.recv()).await
        {
            output.extend_from_slice(&data);
            if String::from_utf8_lossy(&output).contains("hi") {
                saw_hi = true;
                break;
            }
        }
    });
    assert!(saw_hi, "应收到 shell 回显 hi，实际 {} 字节", output.len());

    session.terminate();
    let deadline = Instant::now() + WAIT;
    let terminal = runtime.block_on(async {
        loop {
            match tokio::time::timeout_at(deadline, events.recv()).await {
                Ok(Some(ProcEvent::Exited(_) | ProcEvent::Signaled(_))) => return true,
                Ok(Some(_)) => continue,
                Ok(None) => return true,
                Err(_) => return false,
            }
        }
    });
    assert!(terminal, "terminate 后应在超时内结束事件流");
}

/// helper 上传 + 握手：输出标记、真实退出码、能力标记（默认临时目录分支）。
#[test]
fn helper_upload_reports_output_and_exit_code() {
    let _guard = lock_helper_uploads();
    let Some(config) = helper_config() else {
        return;
    };
    let plain = fallback_config(&config);
    let before = list_temp_dirs(&plain);

    let request = SshConnectRequest::from_create(
        "sh",
        &[
            "-c".to_string(),
            "echo hello_helper_ssh; exit 3".to_string(),
        ],
        80,
        24,
    );
    let (session, mut events) =
        SshSession::connect(&config, request).expect("helper SSH 连接/握手失败");

    assert!(session.helper(), "应进入 helper 模式");
    let capabilities = session.capabilities();
    assert!(capabilities.helper, "capabilities.helper 应为 true");
    assert!(!capabilities.best_effort, "helper 模式不应标记 bestEffort");
    assert!(session.pid() > 0, "helper 应返回远端 pid");
    assert!(session.pgid() > 0, "helper 应返回远端 pgid");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("测试运行时");
    let mut output = Vec::new();
    let terminal = collect_until_terminal(
        &runtime,
        &mut events,
        Some(&mut output),
        Instant::now() + WAIT,
    );

    assert!(
        String::from_utf8_lossy(&output).contains("hello_helper_ssh"),
        "应收到 helper 模式输出，实际 {} 字节",
        output.len()
    );
    match terminal {
        Some(ProcEvent::Exited(3)) => {}
        other => panic!("helper 模式应收到退出码 3，实际 {other:?}"),
    }

    // 安装到测试专用路径：上传成功，会话结束后不再产生临时目录。
    assert_no_new_temp_dirs(&plain, &before);
}

/// helper 模式未显式提供 TERM 时，远端 PTY 应拿到默认 `xterm-256color`。
#[test]
fn helper_injects_default_term() {
    let _guard = lock_helper_uploads();
    let Some(config) = helper_config() else {
        return;
    };
    let request = SshConnectRequest::from_create(
        "sh",
        &[
            "-c".to_string(),
            "printf 'TERM=%s\\n' \"$TERM\"".to_string(),
        ],
        80,
        24,
    );
    let (session, mut events) =
        SshSession::connect(&config, request).expect("helper shell 连接失败");
    assert!(session.helper(), "应使用 helper 会话");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("测试运行时");
    let mut output = Vec::new();
    let terminal = collect_until_terminal(
        &runtime,
        &mut events,
        Some(&mut output),
        Instant::now() + WAIT,
    );
    let text = String::from_utf8_lossy(&output);
    assert!(
        text.contains("TERM=xterm-256color"),
        "应注入默认 TERM，实际 {text:?}"
    );
    assert!(
        matches!(terminal, Some(ProcEvent::Exited(0))),
        "命令应以 0 退出，实际 {terminal:?}"
    );
}

/// helper 交互：pid/pgid/tree、input、resize、terminate 后经远端 `/proc` 确认无残留。
#[test]
fn helper_interactive_reports_tree_and_terminates() {
    let _guard = lock_helper_uploads();
    let Some(config) = helper_config() else {
        return;
    };
    let plain = fallback_config(&config);
    let before = list_temp_dirs(&plain);
    let (session, mut events) = SshSession::connect(&config, SshConnectRequest::shell(80, 24))
        .expect("helper shell 连接失败");

    let pid = session.pid();
    let pgid = session.pgid();
    assert!(pid > 0 && pgid > 0, "helper 应返回 pid/pgid");
    assert_eq!(session.foreground_pgrp(), Some(pgid));

    let tree = session.tree();
    assert!(
        tree.iter().any(|node| node.pid == pid),
        "终止前进程树应包含根进程 {pid}: {tree:?}"
    );

    session.write(b"echo termx_helper_hi\n").expect("写入失败");
    session.resize(120, 30).expect("resize 失败");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("测试运行时");
    let deadline = Instant::now() + WAIT;
    let mut output = Vec::new();
    let saw = runtime.block_on(async {
        while let Ok(Some(event)) = tokio::time::timeout_at(deadline, events.recv()).await {
            if let ProcEvent::Output { data, .. } = event {
                output.extend_from_slice(&data);
                if String::from_utf8_lossy(&output).contains("termx_helper_hi") {
                    return true;
                }
            }
        }
        false
    });
    assert!(saw, "应收到 helper shell 回显，实际 {} 字节", output.len());

    session.terminate();
    let terminal = collect_until_terminal(&runtime, &mut events, None, Instant::now() + WAIT);
    assert!(terminal.is_some(), "terminate 后应在超时内收到终态");

    // 已退出的 session 上 `tree()` 只会返回空（无法区分“无残留”和“查询失败”），
    // 因此用第二个 fallback 会话直接检查远端 /proc。
    let command = format!(
        "sh -c 'i=0; while [ -d /proc/{pid} ]; do i=$((i+1)); [ $i -ge 100 ] && {{ echo alive; exit 1; }}; sleep 0.1; done; echo gone'"
    );
    let (output, terminal) = run_remote(&plain, &command);
    assert!(
        output.contains("gone") && matches!(terminal, Some(ProcEvent::Exited(0))),
        "terminate 后根进程 {pid} 应消失，远端输出 {output:?}，终态 {terminal:?}"
    );

    assert_no_new_temp_dirs(&plain, &before);
}

/// T4：真实 sshd + helper + fake child，验证带外 shell 事件经 SSH 回传。
///
/// 子进程（容器内 python3）读取 helper 注入的 endpoint/token，连接 UDS 后发送
/// hello + C/D；断言 `ProcEvent::ShellEvent` 的顺序、code/id、普通输出仍到达，
/// 且 `SshSession::shell_events()` 为 true。
#[test]
fn helper_shell_events_arrive_over_ssh() {
    let _guard = lock_helper_uploads();
    let Some(config) = helper_config() else {
        return;
    };
    let Some(interpreter) = shell_event_probe() else {
        return;
    };

    let mut request = SshConnectRequest::exec("", 80, 24);
    request.events = true;
    request.program = Some(HelperProgram {
        cmd: interpreter,
        args: vec!["-c".to_string(), SHELL_EVENT_PROBE_SCRIPT.to_string()],
    });

    let (session, mut events) =
        SshSession::connect(&config, request).expect("shell 事件 SSH 连接/握手失败");
    assert!(session.helper(), "应进入 helper 模式");
    assert!(
        session.shell_events(),
        "helper 应声明 shell_events 能力且 started.events=true"
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("测试运行时");
    let deadline = Instant::now() + WAIT;
    let mut output = Vec::new();
    let mut shell_events = Vec::new();
    let mut terminal = None;
    runtime.block_on(async {
        loop {
            match tokio::time::timeout_at(deadline, events.recv()).await {
                Ok(Some(ProcEvent::Output { data, .. })) => output.extend_from_slice(&data),
                Ok(Some(ProcEvent::ShellEvent(event))) => shell_events.push(event),
                Ok(Some(other)) => {
                    terminal = Some(other);
                    break;
                }
                Ok(None) | Err(_) => break,
            }
        }
    });

    assert!(
        String::from_utf8_lossy(&output).contains("hello_shell_events_ssh"),
        "普通输出仍应到达，实际 {} 字节",
        output.len()
    );

    let kinds: Vec<ShellEventKind> = shell_events.iter().map(|event| event.kind).collect();
    assert_eq!(
        kinds,
        [ShellEventKind::CommandStart, ShellEventKind::CommandEnd],
        "shell 事件顺序错误: {kinds:?}"
    );
    assert_eq!(shell_events[0].id.as_deref(), Some("cmd-1"), "C 应携带 id");
    assert_eq!(shell_events[0].code, None, "C 不应携带退出码");
    assert_eq!(shell_events[1].code, Some(7), "D 应携带退出码 7");
    assert_eq!(shell_events[1].id.as_deref(), Some("cmd-1"), "D 应携带 id");
    assert!(
        matches!(terminal, Some(ProcEvent::Exited(0))),
        "探针应正常退出，实际 {terminal:?}"
    );
}

/// helper_path 不可用：直接失败，不回退 fallback。
#[test]
fn helper_path_failure_fails_directly() {
    let Some(mut config) = skip() else {
        return;
    };
    config.helper_path = Some("/nonexistent/termx-helper".to_string());

    let request = SshConnectRequest::exec("sh -c 'echo hello_no_fallback; exit 0'", 80, 24);
    let error = SshSession::connect(&config, request)
        .expect_err("helper 不可用时应直接失败，而不是回退 fallback");
    match error {
        termx_ssh::SshError::InjectionFailed { path, .. } => {
            assert_eq!(path, "/nonexistent/termx-helper");
        }
        other => panic!("应为 InjectionFailed，实际 {other:?}"),
    }
}

/// 非上传模式：会话结束后不得删除用户配置的 `helper_path`。
#[test]
fn helper_path_is_never_deleted() {
    let Some(mut config) = skip() else {
        return;
    };
    let path = format!("/tmp/termx-user-helper-{}", std::process::id());
    let script = "#!/bin/sh\nIFS= read -r _line || true\nprintf '%s\\n' '{\"type\":\"hello_ok\",\"proto\":\"termx-helper/999\",\"capabilities\":[]}'\n";
    let encoded = STANDARD.encode(script);
    let setup = format!("sh -c 'printf %s {encoded} | base64 -d > {path}; chmod 700 {path}'");
    let (_, terminal) = run_remote(&config, &setup);
    assert!(
        matches!(terminal, Some(ProcEvent::Exited(0))),
        "应成功写入用户路径，实际 {terminal:?}"
    );

    let plain = fallback_config(&config);
    config.helper_path = Some(path.clone());
    let request = SshConnectRequest::exec("sh -c 'echo hello_keep_path; exit 0'", 80, 24);
    let (session, mut events) =
        SshSession::connect(&config, request).expect("用户 helper_path 连接失败");
    assert!(!session.helper(), "伪造 helper 版本不符时应回退");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("测试运行时");
    let terminal = collect_until_terminal(&runtime, &mut events, None, Instant::now() + WAIT);
    assert!(
        matches!(terminal, Some(ProcEvent::Exited(0))),
        "回退后仍应执行命令，实际 {terminal:?}"
    );

    let (output, terminal) = run_remote(
        &plain,
        &format!("sh -c 'test -f {path} && echo still_there'"),
    );
    assert!(
        output.contains("still_there") && matches!(terminal, Some(ProcEvent::Exited(0))),
        "会话结束不得删除用户 helper_path，输出 {output:?}，终态 {terminal:?}"
    );

    let (_, terminal) = run_remote(&plain, &format!("rm -f {path}"));
    assert!(
        matches!(terminal, Some(ProcEvent::Exited(0))),
        "测试收尾删除失败，实际 {terminal:?}"
    );
}

/// sha256 不符：直接失败，不上传 helper，也不产生临时目录。
#[test]
fn helper_sha_mismatch_fails_directly() {
    let _guard = lock_helper_uploads();
    let Some(mut config) = helper_config() else {
        return;
    };
    config.helper_sha256 = Some("f".repeat(64));
    let plain = fallback_config(&config);
    let before = list_temp_dirs(&plain);

    let request = SshConnectRequest::exec("sh -c 'echo hello_sha_should_not_run; exit 0'", 80, 24);
    let error = SshSession::connect(&config, request)
        .expect_err("sha 不符时应直接失败，而不是回退 fallback");
    match error {
        termx_ssh::SshError::InjectionFailed { path, message } => {
            assert_eq!(path, config.helper_path.clone().unwrap_or_default());
            assert!(message.contains("sha256"), "原因应说明摘要不符: {message}");
        }
        other => panic!("应为 InjectionFailed，实际 {other:?}"),
    }
    assert_no_new_temp_dirs(&plain, &before);
}

/// 版本不匹配：握手失败后回退 fallback（安装的脚本保留，不删除）。
#[test]
fn helper_version_mismatch_falls_back_and_cleans_up() {
    let _guard = lock_helper_uploads();
    let Some(mut config) = skip() else {
        return;
    };
    // 伪造一个只回 hello_ok（错误 proto）的远端 helper。
    let dir = tempdir().expect("本地临时目录");
    let script = dir.path().join("fake-helper.sh");
    std::fs::write(
        &script,
        b"#!/bin/sh\nIFS= read -r _line || true\nprintf '%s\\n' '{\"type\":\"hello_ok\",\"proto\":\"termx-helper/999\",\"capabilities\":[]}'\n",
    )
    .expect("写入伪造 helper");
    config.helper_local_path = Some(script.to_string_lossy().to_string());
    config.upload_helper = true;
    // 上传模式必须给出安装目标与本次授权，否则缺少 helper_path 会直接失败。
    config.helper_path = Some(test_helper_path("version"));
    config.injection = InjectionPlan {
        consent: InjectionConsentMode::Allow,
        allow: true,
        ..Default::default()
    };
    let plain = fallback_config(&config);

    let before = list_temp_dirs(&plain);
    let request = SshConnectRequest::exec("sh -c 'echo hello_version_fallback; exit 0'", 80, 24);
    let (session, mut events) =
        SshSession::connect(&config, request).expect("版本不匹配时的 fallback 连接失败");

    let capabilities = session.capabilities();
    assert!(!capabilities.helper, "版本不匹配时不应启用 helper");
    assert!(capabilities.best_effort, "版本不匹配应回退 bestEffort");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("测试运行时");
    let mut output = Vec::new();
    let terminal = collect_until_terminal(
        &runtime,
        &mut events,
        Some(&mut output),
        Instant::now() + WAIT,
    );
    assert!(
        String::from_utf8_lossy(&output).contains("hello_version_fallback"),
        "版本不匹配回退后仍应可执行，实际 {} 字节",
        output.len()
    );
    assert!(
        matches!(terminal, Some(ProcEvent::Exited(0))),
        "版本不匹配回退应收到退出码 0，实际 {terminal:?}"
    );
    assert_no_new_temp_dirs(&plain, &before);
}
