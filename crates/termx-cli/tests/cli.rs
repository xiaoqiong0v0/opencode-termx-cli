//! termx-cli 端到端测试：真实 service + CLI 子进程。

mod common;

use std::time::{Duration, Instant};

use common::{CliOutput, TestService, run_cli, run_cli_bare};

/// 等待上限内的轮询间隔。
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// 等待 read 出现标记的最长时间。
const MARKER_TIMEOUT: Duration = Duration::from_secs(10);

/// `exec` 保留子进程退出码并按流拆分输出。
#[tokio::test]
async fn exec_preserves_exit_code_and_streams() {
    let mut service = TestService::start().await;
    let output = run_cli(&service, &exec_args()).await;

    assert_eq!(output.code, Some(5), "stderr={}", output.stderr_text());
    assert!(
        output.stdout_text().contains("out"),
        "stdout={:?}",
        output.stdout_text()
    );
    assert!(
        output.stderr_text().contains("err"),
        "stderr={:?}",
        output.stderr_text()
    );
    service.shutdown().await;
}

/// `local --follow` 能拿到 started/output/exited。
#[tokio::test]
async fn local_follow_reports_lifecycle() {
    let mut service = TestService::start().await;
    let output = run_cli(&service, &local_follow_args()).await;
    let combined = format!("{}{}", output.stdout_text(), output.stderr_text());

    assert_eq!(output.code, Some(7), "stderr={}", output.stderr_text());
    assert!(combined.contains("started"), "输出={combined:?}");
    assert!(combined.contains("local-ok"), "输出={combined:?}");
    assert!(combined.contains("exited"), "输出={combined:?}");
    service.shutdown().await;
}

/// `list` / `status` / `claim` + `send` + `terminate` 基本链路。
#[tokio::test]
async fn list_status_claim_send_terminate_round_trip() {
    let mut service = TestService::start().await;
    let shell = default_shell();
    let created = run_cli(
        &service,
        &["local", "--json", "--name", "chain", "--cmd", &shell],
    )
    .await;
    assert_eq!(created.code, Some(0), "stderr={}", created.stderr_text());
    let terminal_id = created.stdout_json()["terminal_id"]
        .as_str()
        .expect("缺少 terminal_id")
        .to_string();

    let status = run_cli(&service, &["status", "--name", "chain", "--json"]).await;
    assert_eq!(status.code, Some(0), "stderr={}", status.stderr_text());
    assert_eq!(status.stdout_json()["terminal_id"], terminal_id);

    let list = run_cli(&service, &["list", "--json"]).await;
    assert_eq!(list.code, Some(0), "stderr={}", list.stderr_text());
    assert!(
        list.stdout_json()
            .as_array()
            .expect("list 结果应为数组")
            .iter()
            .any(|item| item["terminal_id"] == terminal_id),
        "list={}",
        list.stdout_text()
    );

    let claim = run_cli(&service, &["claim", "--name", "chain", "--json"]).await;
    assert_eq!(claim.code, Some(0), "stderr={}", claim.stderr_text());
    assert!(
        claim.stdout_json()["owner"]
            .as_str()
            .is_some_and(|owner| !owner.is_empty()),
        "claim={}",
        claim.stdout_text()
    );

    let send = run_cli(
        &service,
        &[
            "send",
            "--name",
            "chain",
            "--force",
            "--text",
            "echo marker-123",
            "--enter",
        ],
    )
    .await;
    assert_eq!(send.code, Some(0), "stderr={}", send.stderr_text());

    let read = wait_for_marker(&service, &["--name", "chain"], "marker-123").await;
    assert!(read.contains("marker-123"), "read={read:?}");

    let terminate = run_cli(&service, &["terminate", "--name", "chain", "--force"]).await;
    assert_eq!(
        terminate.code,
        Some(0),
        "stderr={}",
        terminate.stderr_text()
    );

    let stopped = run_cli(&service, &["status", "--name", "chain", "--json"]).await;
    assert_eq!(stopped.code, Some(0), "stderr={}", stopped.stderr_text());
    assert_ne!(
        stopped.stdout_json()["state"],
        "running",
        "terminate 后不应仍为 running"
    );
    service.shutdown().await;
}

/// 仅用 `--terminal-id` 走 claim/read/send/terminate。
#[tokio::test]
async fn terminal_id_read_send_round_trip() {
    let mut service = TestService::start().await;
    let shell = default_shell();
    let created = run_cli(
        &service,
        &["local", "--json", "--name", "by-id", "--cmd", &shell],
    )
    .await;
    assert_eq!(created.code, Some(0), "stderr={}", created.stderr_text());
    let terminal_id = created.stdout_json()["terminal_id"]
        .as_str()
        .expect("缺少 terminal_id")
        .to_string();

    let claim = run_cli(
        &service,
        &["claim", "--terminal-id", terminal_id.as_str(), "--json"],
    )
    .await;
    assert_eq!(claim.code, Some(0), "stderr={}", claim.stderr_text());
    assert!(
        claim.stdout_json()["owner"]
            .as_str()
            .is_some_and(|owner| !owner.is_empty()),
        "claim={}",
        claim.stdout_text()
    );

    // send 会在同一连接内自动 claim；--force 避免上一连接关闭时的 lease 释放竞态。
    let send = run_cli(
        &service,
        &[
            "send",
            "--terminal-id",
            terminal_id.as_str(),
            "--force",
            "--text",
            "echo id-marker-456",
            "--enter",
        ],
    )
    .await;
    assert_eq!(send.code, Some(0), "stderr={}", send.stderr_text());

    let read = wait_for_marker(
        &service,
        &["--terminal-id", terminal_id.as_str()],
        "id-marker-456",
    )
    .await;
    assert!(read.contains("id-marker-456"), "read={read:?}");

    let terminate = run_cli(
        &service,
        &[
            "terminate",
            "--terminal-id",
            terminal_id.as_str(),
            "--force",
        ],
    )
    .await;
    assert_eq!(
        terminate.code,
        Some(0),
        "stderr={}",
        terminate.stderr_text()
    );
    service.shutdown().await;
}

/// `read --tail-lines/--tail-chars` 与 `search` 直接读取 raw 输出。
#[tokio::test]
async fn read_tail_and_search_round_trip() {
    let mut service = TestService::start().await;
    let shell = default_shell();
    let created = run_cli(
        &service,
        &["local", "--json", "--name", "tail-search", "--cmd", &shell],
    )
    .await;
    assert_eq!(created.code, Some(0), "stderr={}", created.stderr_text());

    // send 自动 claim；--force 规避上一连接释放 lease 的竞态。
    let send = run_cli(
        &service,
        &[
            "send",
            "--name",
            "tail-search",
            "--force",
            "--text",
            "echo tail-search-marker",
            "--enter",
        ],
    )
    .await;
    assert_eq!(send.code, Some(0), "stderr={}", send.stderr_text());
    wait_for_marker(&service, &["--name", "tail-search"], "tail-search-marker").await;

    let tail_lines = run_cli(
        &service,
        &["read", "--name", "tail-search", "--tail-lines", "50"],
    )
    .await;
    assert_eq!(
        tail_lines.code,
        Some(0),
        "stderr={}",
        tail_lines.stderr_text()
    );
    assert!(
        tail_lines.stdout_text().contains("tail-search-marker"),
        "tail-lines={:?}",
        tail_lines.stdout_text()
    );
    assert!(
        !tail_lines.stdout_text().contains('\u{1b}'),
        "tail-lines 不应包含 ANSI 控制序列: {:?}",
        tail_lines.stdout_text()
    );

    let tail_chars = run_cli(
        &service,
        &[
            "read",
            "--name",
            "tail-search",
            "--tail-chars",
            "4096",
            "--json",
        ],
    )
    .await;
    assert_eq!(
        tail_chars.code,
        Some(0),
        "stderr={}",
        tail_chars.stderr_text()
    );
    assert!(
        tail_chars.stdout_json()["text"]
            .as_str()
            .is_some_and(|text| text.contains("tail-search-marker")),
        "tail-chars={}",
        tail_chars.stdout_text()
    );

    let search = run_cli(
        &service,
        &[
            "search",
            "--name",
            "tail-search",
            "--pattern",
            "tail-search-marker",
            "--only-matches",
        ],
    )
    .await;
    assert_eq!(search.code, Some(0), "stderr={}", search.stderr_text());
    assert!(
        search.stdout_text().contains("tail-search-marker"),
        "search={:?}",
        search.stdout_text()
    );
    assert!(
        !search.stdout_text().contains('\u{1b}'),
        "search 不应返回 ANSI 控制序列: {:?}",
        search.stdout_text()
    );

    let terminate = run_cli(&service, &["terminate", "--name", "tail-search"]).await;
    assert_eq!(
        terminate.code,
        Some(0),
        "stderr={}",
        terminate.stderr_text()
    );
    service.shutdown().await;
}

/// 未知终端返回非零并报告 `not_found`。
#[tokio::test]
async fn unknown_terminal_reports_not_found() {
    let mut service = TestService::start().await;
    let output = run_cli(&service, &["status", "--terminal-id", "no-such-terminal"]).await;

    assert_ne!(output.code, Some(0));
    assert!(
        output.stderr_text().contains("not_found"),
        "stderr={}",
        output.stderr_text()
    );
    service.shutdown().await;
}

/// `resize` / `attach` / `detach` / `claim` / `release` / `signal` 均可用 `--json` 调用。
#[tokio::test]
async fn terminal_ops_json_round_trip() {
    let mut service = TestService::start().await;
    let shell = default_shell();
    let created = run_cli(
        &service,
        &["local", "--json", "--name", "ops", "--cmd", &shell],
    )
    .await;
    assert_eq!(created.code, Some(0), "stderr={}", created.stderr_text());
    let terminal_id = created.stdout_json()["terminal_id"]
        .as_str()
        .expect("缺少 terminal_id")
        .to_string();

    let resize = run_cli(
        &service,
        &[
            "resize",
            "--terminal-id",
            terminal_id.as_str(),
            "--cols",
            "100",
            "--rows",
            "40",
            "--json",
        ],
    )
    .await;
    assert_eq!(resize.code, Some(0), "stderr={}", resize.stderr_text());
    assert_eq!(resize.stdout_json()["ok"], true);

    let attach = run_cli(
        &service,
        &[
            "attach",
            "--terminal-id",
            terminal_id.as_str(),
            "--cursor",
            "0",
            "--json",
        ],
    )
    .await;
    assert_eq!(attach.code, Some(0), "stderr={}", attach.stderr_text());
    assert_eq!(attach.stdout_json()["snapshot"]["terminal_id"], terminal_id);

    let detach = run_cli(
        &service,
        &["detach", "--terminal-id", terminal_id.as_str(), "--json"],
    )
    .await;
    assert_eq!(detach.code, Some(0), "stderr={}", detach.stderr_text());
    assert_eq!(detach.stdout_json()["ok"], true);

    let claim = run_cli(
        &service,
        &["claim", "--terminal-id", terminal_id.as_str(), "--json"],
    )
    .await;
    assert_eq!(claim.code, Some(0), "stderr={}", claim.stderr_text());

    // CLI 一条命令一条连接：claim 的连接关闭时 lease 已被服务端释放，
    // 因此独立进程的 release 预期返回 permission_denied（路径仍被覆盖）。
    let release = run_cli(
        &service,
        &["release", "--terminal-id", terminal_id.as_str(), "--json"],
    )
    .await;
    assert_eq!(release.code, Some(1), "stderr={}", release.stderr_text());
    assert!(
        release.stderr_text().contains("permission_denied"),
        "stderr={}",
        release.stderr_text()
    );

    let signal = run_cli(
        &service,
        &[
            "signal",
            "--terminal-id",
            terminal_id.as_str(),
            "--signal",
            "SIGINT",
            "--json",
        ],
    )
    .await;
    assert_eq!(signal.code, Some(0), "stderr={}", signal.stderr_text());
    assert_eq!(signal.stdout_json()["ok"], true);

    let terminate = run_cli(
        &service,
        &[
            "terminate",
            "--terminal-id",
            terminal_id.as_str(),
            "--force",
            "--json",
        ],
    )
    .await;
    assert_eq!(
        terminate.code,
        Some(0),
        "stderr={}",
        terminate.stderr_text()
    );
    service.shutdown().await;
}

/// `exec --pty` 走 PTY 模式并能拿到输出/退出码。
#[tokio::test]
async fn exec_pty_streams_output() {
    let mut service = TestService::start().await;
    let output = run_cli(&service, &exec_pty_args()).await;
    assert_eq!(output.code, Some(0), "stderr={}", output.stderr_text());
    assert!(
        output.stdout_text().contains("pty-ok"),
        "stdout={:?}",
        output.stdout_text()
    );
    service.shutdown().await;
}

/// `--json` 模式下 `exec` 每个事件都是单行 JSON。
#[tokio::test]
async fn exec_json_emits_event_lines() {
    let mut service = TestService::start().await;
    let output = run_cli(&service, &exec_json_args()).await;
    assert_eq!(output.code, Some(0), "stderr={}", output.stderr_text());

    let mut saw_exited = false;
    for line in output.stdout_text().lines().filter(|line| !line.is_empty()) {
        let value: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|error| panic!("非法 JSON 行 {line:?}: {error}"));
        if value["type"] == "exited" {
            saw_exited = true;
        }
    }
    assert!(
        saw_exited,
        "JSON 事件中应包含 exited，stdout={}",
        output.stdout_text()
    );
    service.shutdown().await;
}

/// `prune` 能清理已退出终端。
#[tokio::test]
async fn prune_removes_exited_terminal() {
    let mut service = TestService::start().await;
    let created = run_cli(&service, &quick_exit_args()).await;
    assert_eq!(created.code, Some(0), "stderr={}", created.stderr_text());
    let terminal_id = created.stdout_json()["terminal_id"]
        .as_str()
        .expect("缺少 terminal_id")
        .to_string();

    wait_for_state(&service, &terminal_id, "exited").await;
    let prune = run_cli(&service, &["prune", "--states", "exited", "--json"]).await;
    assert_eq!(prune.code, Some(0), "stderr={}", prune.stderr_text());
    let prune_json = prune.stdout_json();
    let removed = prune_json["removed"].as_array().expect("removed 应为数组");
    assert!(
        removed
            .iter()
            .any(|item| item.as_str() == Some(terminal_id.as_str())),
        "prune removed={}",
        prune.stdout_text()
    );

    let status = run_cli(&service, &["status", "--terminal-id", terminal_id.as_str()]).await;
    assert_ne!(status.code, Some(0), "prune 后 status 应失败");
    service.shutdown().await;
}

/// `terminate`/`detach` 缺 locator 且未给 `--all` 时拒绝执行；`--all` 才逐个处理。
#[tokio::test]
async fn terminate_and_detach_require_explicit_target() {
    let mut service = TestService::start().await;
    let shell = default_shell();
    let created = run_cli(
        &service,
        &["local", "--json", "--name", "guard", "--cmd", &shell],
    )
    .await;
    assert_eq!(created.code, Some(0), "stderr={}", created.stderr_text());

    let terminate_missing = run_cli(&service, &["terminate"]).await;
    assert_eq!(
        terminate_missing.code,
        Some(2),
        "stderr={}",
        terminate_missing.stderr_text()
    );
    assert!(
        terminate_missing.stderr_text().contains("--all"),
        "stderr={}",
        terminate_missing.stderr_text()
    );

    let detach_missing = run_cli(&service, &["detach"]).await;
    assert_eq!(
        detach_missing.code,
        Some(2),
        "stderr={}",
        detach_missing.stderr_text()
    );
    assert!(
        detach_missing.stderr_text().contains("--all"),
        "stderr={}",
        detach_missing.stderr_text()
    );

    // 被拒绝的调用不得隐式关闭终端。
    let alive = run_cli(&service, &["status", "--name", "guard", "--json"]).await;
    assert_eq!(alive.code, Some(0), "stderr={}", alive.stderr_text());
    assert_eq!(alive.stdout_json()["state"], "running");

    let detach_all = run_cli(&service, &["detach", "--all", "--json"]).await;
    assert_eq!(
        detach_all.code,
        Some(0),
        "stderr={}",
        detach_all.stderr_text()
    );
    assert_eq!(detach_all.stdout_json()["ok"], true);

    let terminate_all = run_cli(&service, &["terminate", "--all", "--json"]).await;
    assert_eq!(
        terminate_all.code,
        Some(0),
        "stderr={}",
        terminate_all.stderr_text()
    );
    assert_eq!(terminate_all.stdout_json()["ok"], true);

    let stopped = run_cli(&service, &["status", "--name", "guard", "--json"]).await;
    assert_eq!(stopped.code, Some(0), "stderr={}", stopped.stderr_text());
    assert_ne!(stopped.stdout_json()["state"], "running");
    service.shutdown().await;
}

/// `--all` 与 locator 同时给出视为用法错误；未知名称返回 `not_found`。
#[tokio::test]
async fn all_conflicts_with_locator_and_unknown_name_is_not_found() {
    let mut service = TestService::start().await;

    for command in ["terminate", "detach"] {
        let conflict = run_cli(&service, &[command, "--name", "x", "--all"]).await;
        assert_eq!(conflict.code, Some(2), "stderr={}", conflict.stderr_text());
        assert!(
            conflict.stderr_text().contains("同时使用"),
            "stderr={}",
            conflict.stderr_text()
        );

        let missing = run_cli(&service, &[command, "--name", "no-such-name"]).await;
        assert_ne!(missing.code, Some(0));
        assert!(
            missing.stderr_text().contains("not_found"),
            "stderr={}",
            missing.stderr_text()
        );
    }
    service.shutdown().await;
}

/// `prune` 必须显式给过滤或 `--all`；`--all` 清理全部终态。
#[tokio::test]
async fn prune_requires_explicit_selector_and_supports_all() {
    let mut service = TestService::start().await;
    let created = run_cli(&service, &quick_exit_args()).await;
    assert_eq!(created.code, Some(0), "stderr={}", created.stderr_text());
    let terminal_id = created.stdout_json()["terminal_id"]
        .as_str()
        .expect("缺少 terminal_id")
        .to_string();
    wait_for_state(&service, &terminal_id, "exited").await;

    let missing = run_cli(&service, &["prune"]).await;
    assert_eq!(missing.code, Some(2), "stderr={}", missing.stderr_text());
    assert!(
        missing.stderr_text().contains("--all"),
        "stderr={}",
        missing.stderr_text()
    );

    let prune_all = run_cli(&service, &["prune", "--all", "--json"]).await;
    assert_eq!(
        prune_all.code,
        Some(0),
        "stderr={}",
        prune_all.stderr_text()
    );
    let removed = prune_all.stdout_json()["removed"]
        .as_array()
        .expect("removed 应为数组")
        .clone();
    assert!(
        removed
            .iter()
            .any(|item| item.as_str() == Some(terminal_id.as_str())),
        "prune --all removed={}",
        prune_all.stdout_text()
    );
    service.shutdown().await;
}

/// 其余终端级子命令缺 locator 时统一返回用法错误（退出码 2）。
#[tokio::test]
async fn terminal_commands_require_locator() {
    let mut service = TestService::start().await;
    let cases: &[&[&str]] = &[
        &["status"],
        &["read", "--cursor", "0"],
        &["send", "--text", "x"],
        &["resize", "--cols", "80", "--rows", "24"],
        &["signal", "--signal", "SIGINT"],
        &["attach"],
        &["claim"],
        &["release"],
    ];
    for args in cases {
        let output = run_cli(&service, args).await;
        assert_eq!(
            output.code,
            Some(2),
            "args={args:?} stderr={}",
            output.stderr_text()
        );
        assert!(
            output.stderr_text().contains("--terminal-id"),
            "args={args:?} stderr={}",
            output.stderr_text()
        );
    }
    service.shutdown().await;
}

/// 用法错误与未实现子命令返回退出码 2。
#[tokio::test]
async fn usage_errors_and_unimplemented_ssh_exit_with_two() {
    let mut service = TestService::start().await;

    let missing_payload = run_cli(&service, &["send", "--name", "x"]).await;
    assert_eq!(
        missing_payload.code,
        Some(2),
        "stderr={}",
        missing_payload.stderr_text()
    );

    let ssh = run_cli(&service, &["ssh", "host"]).await;
    assert_eq!(ssh.code, Some(2), "stderr={}", ssh.stderr_text());
    assert!(
        ssh.stderr_text().contains("M6 未实现"),
        "stderr={}",
        ssh.stderr_text()
    );
    service.shutdown().await;
}

/// 缺少 `--config-dir` / `--endpoint` 时返回用法错误并提示，不再回退平台目录。
#[tokio::test]
async fn requires_explicit_config_dir_or_endpoint() {
    let output = run_cli_bare(&["list", "--json"]).await;
    assert_eq!(output.code, Some(2), "stderr={}", output.stderr_text());
    let stderr = output.stderr_text();
    assert!(stderr.contains("--config-dir"), "stderr={stderr}");
    assert!(stderr.contains("--endpoint"), "stderr={stderr}");
    assert!(stderr.contains("不再推导"), "stderr={stderr}");
}

/// `--config-dir` 单独即可连接（不依赖环境变量）。
#[tokio::test]
async fn config_dir_flag_connects_without_env() {
    let mut service = TestService::start().await;
    let config_dir = service.config_dir.to_string_lossy().into_owned();
    let output = run_cli_bare(&["--config-dir", config_dir.as_str(), "list", "--json"]).await;
    assert_eq!(output.code, Some(0), "stderr={}", output.stderr_text());
    assert!(
        output.stdout_json().as_array().is_some(),
        "list 结果应为数组: {}",
        output.stdout_text()
    );
    service.shutdown().await;
}

/// `--endpoint` + `--token` 无需配置目录即可连接。
#[tokio::test]
async fn endpoint_and_token_work_without_config_dir() {
    let mut service = TestService::start().await;
    let endpoint = service.endpoint();
    let token = service.token.clone();
    let output = run_cli_bare(&[
        "--endpoint",
        endpoint.as_str(),
        "--token",
        token.as_str(),
        "list",
        "--json",
    ])
    .await;
    assert_eq!(output.code, Some(0), "stderr={}", output.stderr_text());
    assert!(
        output.stdout_json().as_array().is_some(),
        "list 结果应为数组: {}",
        output.stdout_text()
    );
    service.shutdown().await;
}

/// 只给 `--endpoint` 且没有 token 时返回用法错误。
#[tokio::test]
async fn endpoint_without_token_or_config_dir_is_usage_error() {
    let output = run_cli_bare(&["--endpoint", "ws://127.0.0.1:1", "list", "--json"]).await;
    assert_eq!(output.code, Some(2), "stderr={}", output.stderr_text());
    let stderr = output.stderr_text();
    assert!(stderr.contains("--token"), "stderr={stderr}");
    assert!(stderr.contains("TERMX_TOKEN"), "stderr={stderr}");
}

/// 平台相关的 `exec` 参数。
fn exec_args() -> Vec<&'static str> {
    #[cfg(windows)]
    {
        vec![
            "exec",
            "--",
            "cmd.exe",
            "/C",
            "echo out & echo err 1>&2 & exit /B 5",
        ]
    }
    #[cfg(not(windows))]
    {
        vec!["exec", "--", "sh", "-c", "echo out; echo err 1>&2; exit 5"]
    }
}

/// 平台相关的 `local --follow` 参数。
fn local_follow_args() -> Vec<&'static str> {
    #[cfg(windows)]
    {
        vec![
            "local",
            "--follow",
            "--cmd",
            "cmd.exe",
            "--",
            "/C",
            "echo local-ok & exit 7",
        ]
    }
    #[cfg(not(windows))]
    {
        vec![
            "local",
            "--follow",
            "--cmd",
            "sh",
            "--",
            "-c",
            "echo local-ok; exit 7",
        ]
    }
}

/// 平台默认 shell。
fn default_shell() -> String {
    #[cfg(windows)]
    {
        "cmd.exe".to_string()
    }
    #[cfg(not(windows))]
    {
        "/bin/sh".to_string()
    }
}

/// 用给定 locator 反复从 cursor=0 读取，直到看到标记或超时。
async fn wait_for_marker(service: &TestService, locator: &[&str], marker: &str) -> String {
    let deadline = Instant::now() + MARKER_TIMEOUT;
    loop {
        let mut args = vec!["read"];
        args.extend_from_slice(locator);
        args.extend_from_slice(&["--cursor", "0"]);
        let output: CliOutput = run_cli(service, &args).await;
        let text = output.stdout_text();
        if text.contains(marker) {
            return text;
        }
        assert!(
            Instant::now() < deadline,
            "等待 read 输出超时；stderr={}",
            output.stderr_text()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// 轮询 `status --json` 直到终端进入指定生命周期状态。
async fn wait_for_state(service: &TestService, terminal_id: &str, expected: &str) {
    let deadline = Instant::now() + MARKER_TIMEOUT;
    loop {
        let output = run_cli(service, &["status", "--terminal-id", terminal_id, "--json"]).await;
        assert_eq!(
            output.code,
            Some(0),
            "status 失败: {}",
            output.stderr_text()
        );
        if output.stdout_json()["state"] == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "等待终端进入 {expected} 超时；当前={}",
            output.stdout_text()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// 平台相关的 `exec --pty` 参数。
fn exec_pty_args() -> Vec<&'static str> {
    #[cfg(windows)]
    {
        vec!["exec", "--pty", "--", "cmd.exe", "/C", "echo pty-ok"]
    }
    #[cfg(not(windows))]
    {
        vec!["exec", "--pty", "--", "sh", "-c", "echo pty-ok"]
    }
}

/// 平台相关的 `exec --json` 参数。
fn exec_json_args() -> Vec<&'static str> {
    #[cfg(windows)]
    {
        vec!["exec", "--json", "--", "cmd.exe", "/C", "echo json-ok"]
    }
    #[cfg(not(windows))]
    {
        vec!["exec", "--json", "--", "sh", "-c", "echo json-ok"]
    }
}

/// 快速自然退出的 `local` 参数（用于 prune）。
fn quick_exit_args() -> Vec<&'static str> {
    #[cfg(windows)]
    {
        vec![
            "local", "--json", "--name", "prune-me", "--cmd", "cmd.exe", "--", "/C", "exit 0",
        ]
    }
    #[cfg(not(windows))]
    {
        vec![
            "local", "--json", "--name", "prune-me", "--cmd", "/bin/sh", "--", "-c", "exit 0",
        ]
    }
}
