//! `local` 子命令：创建本地 PTY，可选持续跟随事件。

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
    },
    error::CliError,
    output::{Printer, print_json},
};

/// 执行 `local` 子命令。
pub async fn run(cli: &Cli, args: &LocalArgs) -> Result<i32, CliError> {
    let client = connect(cli).await?;
    // 先订阅再 create/attach，避免回放事件因无接收者被丢弃。
    let mut events = client.events();
    let created = client.create(build_params(cli, args)?).await?;
    print_created(&created, cli.json)?;

    if !args.follow {
        client.close().await;
        return Ok(0);
    }

    let result = follow(&client, &mut events, &created, cli.json).await;
    client.close().await;
    result
}

/// 组装 PTY 创建参数。
fn build_params(cli: &Cli, args: &LocalArgs) -> Result<CreateParams, CliError> {
    Ok(CreateParams {
        mode: Mode::Pty,
        cmd: args.cmd.clone().unwrap_or_else(default_shell),
        args: args.args.clone(),
        cwd: args.cwd.clone(),
        env: parse_env(&args.env)?,
        cols: Some(args.cols),
        rows: Some(args.rows),
        root_session_id: root_session_id(cli),
        name: new_terminal_name(cli, "local"),
        force: cli.force,
        idle_timeout: build_idle_timeout(args),
        ssh: None,
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
