//! `local` 子命令：创建本地 PTY，可选持续跟随事件。

use std::sync::Arc;

use tokio::sync::broadcast;

use termx_client::Client;
use termx_protocol::{
    CreateParams, CreateResult, DetachParams, Event, EventKind, IdleTimeout, IdleTimeoutAction,
    IdleTimeoutUnit, Mode, TerminalLocator,
};

use crate::{
    cli::{Cli, LocalArgs},
    commands::{
        closed_error, connect, new_terminal_name, parse_env, resync_events, root_session_id,
        spawn_heartbeat,
    },
    error::CliError,
    output::{Printer, print_json},
};

/// 执行 `local` 子命令。
pub async fn run(cli: &Cli, args: &LocalArgs) -> Result<i32, CliError> {
    let client = Arc::new(connect(cli).await?);
    let root = root_session_id(cli);
    // 先订阅再 create/attach，避免回放事件因无接收者被丢弃。
    let mut events = client.events();
    let created = client.create(build_params(cli, args, &root)?).await?;
    print_created(&created, cli.json)?;

    if !args.follow {
        client.close().await;
        return Ok(0);
    }

    let heartbeat = spawn_heartbeat(Arc::clone(&client), root);
    let result = follow(&client, &mut events, &created, cli.json).await;
    heartbeat.abort();
    client.close().await;
    result
}

/// 组装 PTY 创建参数；`root_session_id` 同时用于 `hello` 与心跳。
fn build_params(
    cli: &Cli,
    args: &LocalArgs,
    root_session_id: &str,
) -> Result<CreateParams, CliError> {
    let runtime_id = cli.runtime.clone();
    if runtime_id.is_some() && (args.cmd.is_some() || !args.args.is_empty()) {
        return Err(CliError::Usage(
            "--runtime 与 --cmd/参数不能同时使用".to_string(),
        ));
    }
    // 选内置 runtime 时让服务端完全按 manifest 解析 cmd/args；这里只清空请求值。
    let (cmd, cmd_args) = if runtime_id.is_some() {
        (String::new(), Vec::new())
    } else {
        (
            args.cmd.clone().unwrap_or_else(default_shell),
            args.args.clone(),
        )
    };
    Ok(CreateParams {
        mode: Mode::Pty,
        cmd,
        args: cmd_args,
        cwd: args.cwd.clone(),
        env: parse_env(&args.env)?,
        cols: Some(args.cols),
        rows: Some(args.rows),
        root_session_id: root_session_id.to_string(),
        name: new_terminal_name(cli, "local"),
        force: cli.force,
        idle_timeout: build_idle_timeout(args),
        ssh: None,
        allow_injection: false,
        runtime_id,
    })
}

/// 组装 `--idle-timeout` / `--idle-action`；0 或缺省表示永不超时。
fn build_idle_timeout(args: &LocalArgs) -> Option<IdleTimeout> {
    let value = args.idle_timeout.filter(|value| *value > 0)?;
    Some(IdleTimeout {
        value,
        unit: IdleTimeoutUnit::S,
        action: Some(args.idle_action.unwrap_or(IdleTimeoutAction::Detach)),
    })
}

/// 输出创建结果。
fn print_created(created: &CreateResult, json: bool) -> Result<(), CliError> {
    if json {
        return print_json(created);
    }
    println!(
        "terminal_id={} cursor={}",
        created.terminal_id, created.cursor
    );
    Ok(())
}

/// 附着并持续输出事件，直到终态、idle 断开或 Ctrl-C。
async fn follow(
    client: &Client,
    events: &mut broadcast::Receiver<Event>,
    created: &CreateResult,
    json: bool,
) -> Result<i32, CliError> {
    let locator = TerminalLocator {
        terminal_id: Some(created.terminal_id.clone()),
        root_session_id: None,
        name: None,
    };
    let mut cursor = 0_u64;
    resync_events(client, &locator, cursor).await?;

    let printer = Printer::new(json, true);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let _ = client.detach(DetachParams { locator }).await;
                return Ok(0);
            }
            received = events.recv() => {
                match received {
                    Ok(event) if event.terminal_id == created.terminal_id => {
                        // 重新 attach 会重放部分事件；按 seq 去重，避免重复渲染。
                        if event.seq <= cursor {
                            continue;
                        }
                        cursor = event.seq;
                        if let Some(end) = printer.render_event(&event)? {
                            return Ok(end.exit_code());
                        }
                        if is_idle_detach(&event.kind) {
                            return Ok(0);
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // 广播通道丢事件后按最后已处理 seq 重新 attach 补齐，
                        // 不能继续空转等待可能已被丢掉的终态事件。
                        resync_events(client, &locator, cursor).await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => return Err(closed_error()),
                }
            }
        }
    }
}

/// 只把 idle_timeout 的 detached 视为结束，避免历史 detach 事件误停。
fn is_idle_detach(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Detached { reason, .. } if reason == "idle_timeout"
    )
}

/// 默认 shell：Windows 用 `%COMSPEC%`，Unix 用 `$SHELL`。
fn default_shell() -> String {
    #[cfg(windows)]
    {
        std::env::var("COMSPEC")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "cmd.exe".to_string())
    }
    #[cfg(not(windows))]
    {
        std::env::var("SHELL")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "/bin/sh".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command, DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS};

    /// 构造只用于 `build_params` 的 CLI；其余字段取默认值。
    fn make_cli(runtime: Option<&str>, cmd: Option<&str>, args: Vec<&str>) -> Cli {
        Cli {
            root_session_id: None,
            name: None,
            force: false,
            config_dir: None,
            token: None,
            endpoint: None,
            runtime: runtime.map(str::to_string),
            json: false,
            command: Command::Local(LocalArgs {
                cmd: cmd.map(str::to_string),
                cwd: None,
                env: Vec::new(),
                cols: DEFAULT_PTY_COLS,
                rows: DEFAULT_PTY_ROWS,
                idle_timeout: None,
                idle_action: None,
                follow: false,
                args: args.into_iter().map(str::to_string).collect(),
            }),
        }
    }

    /// 从命令行取出 local 参数，避免测试里重复解构。
    fn local_args(cli: &Cli) -> &LocalArgs {
        match &cli.command {
            Command::Local(args) => args,
            _ => unreachable!("测试只构造 local"),
        }
    }

    #[test]
    fn runtime_is_forwarded_and_cmd_args_are_cleared() {
        let cli = make_cli(Some("termx-pwsh"), None, Vec::new());
        let params = build_params(&cli, local_args(&cli), "ses-1").expect("参数应构造成功");
        assert_eq!(params.runtime_id.as_deref(), Some("termx-pwsh"));
        assert!(params.cmd.is_empty(), "内置 runtime 不应带 cmd");
        assert!(params.args.is_empty(), "内置 runtime 不应带 args");
    }

    #[test]
    fn runtime_conflicts_with_custom_cmd_or_args() {
        let cli = make_cli(Some("termx-pwsh"), Some("cmd.exe"), Vec::new());
        assert!(matches!(
            build_params(&cli, local_args(&cli), "ses-1"),
            Err(CliError::Usage(_))
        ));

        let cli = make_cli(Some("termx-pwsh"), None, vec!["-NoLogo"]);
        assert!(matches!(
            build_params(&cli, local_args(&cli), "ses-1"),
            Err(CliError::Usage(_))
        ));
    }

    #[test]
    fn no_runtime_keeps_legacy_cmd_behavior() {
        let cli = make_cli(None, Some("cmd.exe"), vec!["/Q"]);
        let params = build_params(&cli, local_args(&cli), "ses-1").expect("参数应构造成功");
        assert_eq!(params.runtime_id, None);
        assert_eq!(params.cmd, "cmd.exe");
        assert_eq!(params.args, vec!["/Q".to_string()]);
    }
}
