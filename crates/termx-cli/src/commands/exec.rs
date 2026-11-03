//! `exec` 子命令：一次性执行命令并透传退出码。

use tokio::sync::broadcast;

use termx_client::Client;
use termx_protocol::{CreateParams, Event, Mode, TerminalLocator};

use crate::{
    cli::{Cli, DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS, ExecArgs},
    commands::{
        closed_error, connect, new_terminal_name, parse_env, resync_events, root_session_id,
    },
    error::CliError,
    output::Printer,
};

/// 执行 `exec` 子命令。
pub async fn run(cli: &Cli, args: &ExecArgs) -> Result<i32, CliError> {
    let client = connect(cli).await?;
    // 先订阅再 create/attach，确保回放事件不会因无接收者丢失。
    let mut events = client.events();
    let created = client.create(build_params(cli, args)?).await?;
    let terminal_id = created.terminal_id.clone();
    let locator = TerminalLocator {
        terminal_id: Some(terminal_id.clone()),
        root_session_id: None,
        name: None,
    };
    let mut cursor = 0_u64;
    resync_events(&client, &locator, cursor).await?;

    let code = wait_for_exit(
        &client,
        &mut events,
        &terminal_id,
        &locator,
        &mut cursor,
        cli.json,
    )
    .await;
    client.close().await;
    code
}

/// 组装创建参数：首项为程序，其余为参数。
fn build_params(cli: &Cli, args: &ExecArgs) -> Result<CreateParams, CliError> {
    let (program, program_args) = args
        .cmd
        .split_first()
        .ok_or_else(|| CliError::Usage("exec 需要 -- 之后的命令".to_string()))?;
    let pty = args.pty;
    Ok(CreateParams {
        mode: if pty { Mode::Pty } else { Mode::Stdio },
        cmd: program.clone(),
        args: program_args.to_vec(),
        cwd: args.cwd.clone(),
        env: parse_env(&args.env)?,
        cols: pty.then_some(DEFAULT_PTY_COLS),
        rows: pty.then_some(DEFAULT_PTY_ROWS),
        root_session_id: root_session_id(cli),
        name: new_terminal_name(cli, "exec"),
        force: cli.force,
        idle_timeout: None,
        ssh: None,
    })
}

/// 消费事件直到终态；人类模式只写原始 output，不打印元信息。
async fn wait_for_exit(
    client: &Client,
    events: &mut broadcast::Receiver<Event>,
    terminal_id: &str,
    locator: &TerminalLocator,
    cursor: &mut u64,
    json: bool,
) -> Result<i32, CliError> {
    let printer = Printer::new(json, false);
    loop {
        match events.recv().await {
            Ok(event) if event.terminal_id == terminal_id => {
                // 重新 attach 会重放部分事件；按 seq 去重，避免重复输出。
                if event.seq <= *cursor {
                    continue;
                }
                *cursor = event.seq;
                if let Some(end) = printer.render_event(&event)? {
                    return Ok(end.exit_code());
                }
            }
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(_)) => {
                // 广播丢失事件后按 cursor 重新 attach 补齐；终态事件不可丢。
                resync_events(client, locator, *cursor).await?;
            }
            Err(broadcast::error::RecvError::Closed) => return Err(closed_error()),
        }
    }
}
