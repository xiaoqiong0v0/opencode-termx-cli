//! 终端级子命令：status/list/read/send/resize/signal/terminate/prune/attach 等。

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

use termx_protocol::{
    AttachParams, ClaimParams, ClaimResult, DetachParams, ListParams, PruneParams, ReadParams,
    ReleaseParams, ResizeParams, SendParams, SignalParams, SimpleResult, StatusParams,
    TerminateParams,
};

use crate::{
    cli::{
        AttachArgs, Cli, ListArgs, LocatorArgs, PruneArgs, ReadArgs, ResizeArgs, SendArgs,
        SignalArgs, TerminateArgs, parse_prune_basis,
    },
    commands::{connect, locator},
    error::CliError,
    output::{print_json, print_list, print_snapshot, write_stdout},
};

/// `status`：打印状态快照。
pub async fn status(cli: &Cli, args: &LocatorArgs) -> Result<i32, CliError> {
    let client = connect(cli).await?;
    let result = client
        .status(StatusParams {
            locator: locator(cli, args.terminal_id.as_deref())?,
        })
        .await?;
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
    let client = connect(cli).await?;
    let result = client
        .read(ReadParams {
            locator: locator(cli, args.locator.terminal_id.as_deref())?,
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
    let client = connect(cli).await?;
    let locator = locator(cli, args.locator.terminal_id.as_deref())?;
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
    let client = connect(cli).await?;
    let result = client
        .resize(ResizeParams {
            locator: locator(cli, args.locator.terminal_id.as_deref())?,
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
    let client = connect(cli).await?;
    let result = client
        .signal(SignalParams {
            locator: locator(cli, args.locator.terminal_id.as_deref())?,
            signal: args.signal.clone(),
        })
        .await?;
    print_simple(&result, cli.json)?;
    client.close().await;
    Ok(0)
}

/// `terminate`：终止终端（幂等）。
pub async fn terminate(cli: &Cli, args: &TerminateArgs) -> Result<i32, CliError> {
    let client = connect(cli).await?;
    let result = client
        .terminate(TerminateParams {
            locator: locator(cli, args.locator.terminal_id.as_deref())?,
            force: cli.force,
        })
        .await?;
    print_simple(&result, cli.json)?;
    client.close().await;
    Ok(0)
}

/// `prune`：清理终态终端。
pub async fn prune(cli: &Cli, args: &PruneArgs) -> Result<i32, CliError> {
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
    let client = connect(cli).await?;
    let result = client
        .attach(AttachParams {
            locator: locator(cli, args.locator.terminal_id.as_deref())?,
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

/// `detach`：断开附着。
pub async fn detach(cli: &Cli, args: &LocatorArgs) -> Result<i32, CliError> {
    let client = connect(cli).await?;
    let result = client
        .detach(DetachParams {
            locator: locator(cli, args.terminal_id.as_deref())?,
        })
        .await?;
    print_simple(&result, cli.json)?;
    client.close().await;
    Ok(0)
}

/// `claim`：取得输入 lease。
pub async fn claim(cli: &Cli, args: &LocatorArgs) -> Result<i32, CliError> {
    let client = connect(cli).await?;
    let result = client
        .claim(ClaimParams {
            locator: locator(cli, args.terminal_id.as_deref())?,
            force: cli.force,
        })
        .await?;
    print_claim(&result, cli.json)?;
    client.close().await;
    Ok(0)
}

/// `release`：释放输入 lease。
pub async fn release(cli: &Cli, args: &LocatorArgs) -> Result<i32, CliError> {
    let client = connect(cli).await?;
    let result = client
        .release(ReleaseParams {
            locator: locator(cli, args.terminal_id.as_deref())?,
        })
        .await?;
    print_simple(&result, cli.json)?;
    client.close().await;
    Ok(0)
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
