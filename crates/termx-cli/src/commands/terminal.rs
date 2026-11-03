//! 终端级子命令：status/list/read/send/resize/signal/terminate/prune/attach 等。

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use termx_client::Client;
use termx_protocol::{
    AttachParams, ClaimParams, ClaimResult, DetachParams, ListParams, PruneParams, ReadParams,
    ReleaseParams, ResizeParams, SendParams, SignalParams, SimpleResult, StatusParams,
    TerminalLocator, TerminateParams,
};

use crate::{
    cli::{
        AttachArgs, Cli, DetachArgs, ListArgs, LocatorArgs, PruneArgs, ReadArgs, ResizeArgs,
        SendArgs, SignalArgs, TerminateArgs, parse_prune_basis,
    },
    commands::{connect, locator},
    error::CliError,
    output::{print_json, print_list, print_snapshot, write_stdout},
};

/// `status`：打印状态快照。
pub async fn status(cli: &Cli, args: &LocatorArgs) -> Result<i32, CliError> {
    let locator = locator(cli, args.terminal_id.as_deref())?;
    let client = connect(cli).await?;
    let result = client.status(StatusParams { locator }).await?;
    if cli.json {
        print_json(&result.snapshot)?;
    } else {
        print_snapshot(&result.snapshot)?;
    }
    client.close().await;
    Ok(0)
}

/// `list`：列出终端。
pub async fn list(cli: &Cli, args: &ListArgs) -> Result<i32, CliError> {
    let client = connect(cli).await?;
    let states = (!args.states.is_empty()).then(|| args.states.clone());
    let items = client
        .list(ListParams {
            states,
            limit: args.limit,
        })
        .await?;
    if cli.json {
        print_json(&items)?;
    } else {
        print_list(&items)?;
    }
    client.close().await;
    Ok(0)
}

/// `read`：按游标只读回放 output。
pub async fn read(cli: &Cli, args: &ReadArgs) -> Result<i32, CliError> {
    let locator = locator(cli, args.locator.terminal_id.as_deref())?;
    let client = connect(cli).await?;
    let result = client
        .read(ReadParams {
            locator,
            cursor: args.cursor,
            max_bytes: args.max_bytes,
        })
        .await?;
    if cli.json {
        print_json(&result)?;
    } else {
        if let Some(reset) = result.reset {
            eprintln!("output 无法完整回放，已从基线继续: {reset:?}");
        }
        for chunk in &result.chunks {
            let bytes = BASE64_STANDARD
                .decode(&chunk.data)
                .map_err(|error| CliError::Usage(format!("chunk 不是合法 base64: {error}")))?;
            write_stdout(&bytes)?;
        }
        eprintln!("cursor={}", result.cursor);
    }
    client.close().await;
    Ok(0)
}

/// `send`：写入文本或原始字节。
///
/// CLI 是「一次调用一条连接」：`terminal.send` 要求发送方持有输入 lease，
/// 因此这里先在同一连接上 claim（`--force` 可抢占他人 lease），再发送。
pub async fn send(cli: &Cli, args: &SendArgs) -> Result<i32, CliError> {
    let locator = locator(cli, args.locator.terminal_id.as_deref())?;
    let client = connect(cli).await?;
    client
        .claim(ClaimParams {
            locator: locator.clone(),
            force: cli.force,
        })
        .await?;
    let result = client
        .send(SendParams {
            locator,
            data: args.data.clone(),
            text: args.text.clone(),
            enter: args.enter,
        })
        .await?;
    print_simple(&result, cli.json)?;
    client.close().await;
    Ok(0)
}

/// `resize`：调整 PTY 尺寸。
pub async fn resize(cli: &Cli, args: &ResizeArgs) -> Result<i32, CliError> {
    let locator = locator(cli, args.locator.terminal_id.as_deref())?;
    let client = connect(cli).await?;
    let result = client
        .resize(ResizeParams {
            locator,
            cols: args.cols,
            rows: args.rows,
        })
        .await?;
    print_simple(&result, cli.json)?;
    client.close().await;
    Ok(0)
}

/// `signal`：向终端进程组发送信号。
pub async fn signal(cli: &Cli, args: &SignalArgs) -> Result<i32, CliError> {
    let locator = locator(cli, args.locator.terminal_id.as_deref())?;
    let client = connect(cli).await?;
    let result = client
        .signal(SignalParams {
            locator,
            signal: args.signal.clone(),
        })
        .await?;
    print_simple(&result, cli.json)?;
    client.close().await;
    Ok(0)
}

/// `terminate`：终止终端（幂等）；`--all` 时逐个终止全部终端。
pub async fn terminate(cli: &Cli, args: &TerminateArgs) -> Result<i32, CliError> {
    let target = resolve_target(
        cli,
        args.locator.terminal_id.as_deref(),
        args.all,
        "terminate",
    )?;
    let client = connect(cli).await?;
    match target {
        Target::One(locator) => {
            let result = client
                .terminate(TerminateParams {
                    locator,
                    force: cli.force,
                })
                .await?;
            print_simple(&result, cli.json)?;
        }
        Target::All => {
            terminate_all(&client, cli.force).await?;
            print_simple(&SimpleResult { ok: true }, cli.json)?;
        }
    }
    client.close().await;
    Ok(0)
}

/// `prune`：清理终态终端；必须显式给出过滤或 `--all`。
pub async fn prune(cli: &Cli, args: &PruneArgs) -> Result<i32, CliError> {
    if !args.all && args.states.is_empty() && args.older_than.is_none() {
        return Err(CliError::Usage(
            "prune 需要至少提供 --states、--older-than 或 --all 之一（不会默认清理全部）"
                .to_string(),
        ));
    }
    if args.all && (!args.states.is_empty() || args.older_than.is_some()) {
        return Err(CliError::Usage(
            "prune 的 --all 不能与 --states/--older-than 同时使用".to_string(),
        ));
    }
    let client = connect(cli).await?;
    let states = (!args.states.is_empty()).then(|| args.states.clone());
    let basis = match args.basis.as_deref() {
        Some(value) => Some(parse_prune_basis(value).map_err(CliError::Usage)?),
        None => None,
    };
    let result = client
        .prune(PruneParams {
            states,
            older_than: args.older_than,
            basis,
            all: args.all,
        })
        .await?;
    if cli.json {
        print_json(&result)?;
    } else {
        println!("removed={}", result.removed.len());
        for terminal_id in &result.removed {
            println!("{terminal_id}");
        }
    }
    client.close().await;
    Ok(0)
}

/// `attach`：附着并返回快照/游标。
pub async fn attach(cli: &Cli, args: &AttachArgs) -> Result<i32, CliError> {
    let locator = locator(cli, args.locator.terminal_id.as_deref())?;
    let client = connect(cli).await?;
    let result = client
        .attach(AttachParams {
            locator,
            cursor: args.cursor,
        })
        .await?;
    if cli.json {
        print_json(&result)?;
    } else {
        if let Some(reset) = result.reset {
            eprintln!("output 无法完整回放，已从基线继续: {reset:?}");
        }
        println!("cursor={}", result.cursor);
        print_snapshot(&result.snapshot)?;
    }
    client.close().await;
    Ok(0)
}

/// `detach`：断开附着；`--all` 时逐个断开全部终端。
pub async fn detach(cli: &Cli, args: &DetachArgs) -> Result<i32, CliError> {
    let target = resolve_target(cli, args.locator.terminal_id.as_deref(), args.all, "detach")?;
    let client = connect(cli).await?;
    match target {
        Target::One(locator) => {
            let result = client.detach(DetachParams { locator }).await?;
            print_simple(&result, cli.json)?;
        }
        Target::All => {
            detach_all(&client).await?;
            print_simple(&SimpleResult { ok: true }, cli.json)?;
        }
    }
    client.close().await;
    Ok(0)
}

/// `claim`：取得输入 lease。
pub async fn claim(cli: &Cli, args: &LocatorArgs) -> Result<i32, CliError> {
    let locator = locator(cli, args.terminal_id.as_deref())?;
    let client = connect(cli).await?;
    let result = client
        .claim(ClaimParams {
            locator,
            force: cli.force,
        })
        .await?;
    print_claim(&result, cli.json)?;
    client.close().await;
    Ok(0)
}

/// `release`：释放输入 lease。
pub async fn release(cli: &Cli, args: &LocatorArgs) -> Result<i32, CliError> {
    let locator = locator(cli, args.terminal_id.as_deref())?;
    let client = connect(cli).await?;
    let result = client.release(ReleaseParams { locator }).await?;
    print_simple(&result, cli.json)?;
    client.close().await;
    Ok(0)
}

/// `terminate`/`detach` 的目标：单个终端或显式全部。
enum Target {
    /// 由 locator 定位单个终端。
    One(TerminalLocator),
    /// 显式 `--all`：逐个处理全部终端。
    All,
}

/// 解析目标：必须给出 locator，或显式 `--all`；两者同时给出视为用法错误。
fn resolve_target(
    cli: &Cli,
    terminal_id: Option<&str>,
    all: bool,
    command: &str,
) -> Result<Target, CliError> {
    let has_locator = terminal_id.is_some_and(|value| !value.is_empty())
        || cli.name.as_deref().is_some_and(|value| !value.is_empty());
    match (has_locator, all) {
        (true, true) => Err(CliError::Usage(format!(
            "{command} 的 --all 不能与 --terminal-id/--name 同时使用"
        ))),
        (false, false) => Err(CliError::Usage(format!(
            "{command} 需要 --terminal-id <ID> 或 --name <NAME>；关闭全部请显式加 --all"
        ))),
        (false, true) => Ok(Target::All),
        (true, false) => Ok(Target::One(locator(cli, terminal_id)?)),
    }
}

/// 先 list 再逐个终止全部终端（`terminate --all`）。
async fn terminate_all(client: &Client, force: bool) -> Result<(), CliError> {
    let items = client
        .list(ListParams {
            states: None,
            limit: None,
        })
        .await?;
    for item in items {
        client
            .terminate(TerminateParams {
                locator: TerminalLocator {
                    terminal_id: Some(item.terminal_id),
                    ..TerminalLocator::default()
                },
                force,
            })
            .await?;
    }
    Ok(())
}

/// 先 list 再逐个 detach 全部终端（`detach --all`）。
async fn detach_all(client: &Client) -> Result<(), CliError> {
    let items = client
        .list(ListParams {
            states: None,
            limit: None,
        })
        .await?;
    for item in items {
        client
            .detach(DetachParams {
                locator: TerminalLocator {
                    terminal_id: Some(item.terminal_id),
                    ..TerminalLocator::default()
                },
            })
            .await?;
    }
    Ok(())
}

/// 输出 `{ ok }` 结果。
fn print_simple(result: &SimpleResult, json: bool) -> Result<(), CliError> {
    if json {
        return print_json(result);
    }
    println!("ok={}", result.ok);
    Ok(())
}

/// 输出 claim 结果。
fn print_claim(result: &ClaimResult, json: bool) -> Result<(), CliError> {
    if json {
        return print_json(result);
    }
    println!("owner={} lease={}", result.owner, result.lease);
    Ok(())
}
