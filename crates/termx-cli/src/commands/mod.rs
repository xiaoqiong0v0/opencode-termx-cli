//! 子命令分发与共享辅助函数。

mod exec;
mod local;
mod terminal;

use std::collections::BTreeMap;

use termx_client::{Client, ClientError};
use termx_protocol::{AttachParams, TerminalLocator};

use crate::{
    cli::{Cli, Command},
    config::{ConnectionOverrides, resolve_client_config, resolve_root_session_id},
    error::CliError,
};

/// 按子命令分发，返回进程退出码。
pub async fn run(cli: &Cli) -> Result<i32, CliError> {
    match &cli.command {
        Command::Local(args) => local::run(cli, args).await,
        Command::Exec(args) => exec::run(cli, args).await,
        Command::Status(args) => terminal::status(cli, args).await,
        Command::List(args) => terminal::list(cli, args).await,
        Command::Read(args) => terminal::read(cli, args).await,
        Command::Send(args) => terminal::send(cli, args).await,
        Command::Resize(args) => terminal::resize(cli, args).await,
        Command::Signal(args) => terminal::signal(cli, args).await,
        Command::Terminate(args) => terminal::terminate(cli, args).await,
        Command::Prune(args) => terminal::prune(cli, args).await,
        Command::Attach(args) => terminal::attach(cli, args).await,
        Command::Detach(args) => terminal::detach(cli, args).await,
        Command::Claim(args) => terminal::claim(cli, args).await,
        Command::Release(args) => terminal::release(cli, args).await,
        Command::Ssh(_) => Err(CliError::Unsupported(
            "M6 未实现：ssh 子命令尚未提供".to_string(),
        )),
    }
}

/// 按全局参数建立客户端连接。
pub(crate) async fn connect(cli: &Cli) -> Result<Client, CliError> {
    let config = resolve_client_config(ConnectionOverrides {
        config_dir: cli.config_dir.as_deref(),
        token: cli.token.as_deref(),
        endpoint: cli.endpoint.as_deref(),
    })?;
    Ok(Client::connect(config).await?)
}

/// 解析当前 root session id。
pub(crate) fn root_session_id(cli: &Cli) -> String {
    resolve_root_session_id(cli.root_session_id.as_deref())
}

/// 新建终端使用的名称：显式 `--name`，否则 `<prefix>-<pid>`。
pub(crate) fn new_terminal_name(cli: &Cli, prefix: &str) -> String {
    cli.name
        .clone()
        .unwrap_or_else(|| format!("{prefix}-{}", std::process::id()))
}

/// 构造 locator：`--terminal-id` 优先，其次全局 `--name` + root session。
pub(crate) fn locator(cli: &Cli, terminal_id: Option<&str>) -> Result<TerminalLocator, CliError> {
    if let Some(terminal_id) = terminal_id.filter(|value| !value.is_empty()) {
        return Ok(TerminalLocator {
            terminal_id: Some(terminal_id.to_string()),
            root_session_id: None,
            name: None,
        });
    }
    let Some(name) = cli.name.as_deref().filter(|value| !value.is_empty()) else {
        return Err(CliError::Usage(
            "需要 --terminal-id <ID> 或 --name <NAME> 之一".to_string(),
        ));
    };
    Ok(TerminalLocator {
        terminal_id: None,
        root_session_id: Some(root_session_id(cli)),
        name: Some(name.to_string()),
    })
}

/// 把重复的 `--env K=V` 解析为协议要求的 JSON 对象 map。
pub(crate) fn parse_env(pairs: &[String]) -> Result<Option<BTreeMap<String, String>>, CliError> {
    if pairs.is_empty() {
        return Ok(None);
    }
    let mut map = BTreeMap::new();
    for pair in pairs {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| CliError::Usage(format!("--env 需要 K=V 形式: {pair}")))?;
        if key.is_empty() {
            return Err(CliError::Usage(format!("--env 的键不能为空: {pair}")));
        }
        map.insert(key.to_string(), value.to_string());
    }
    Ok(Some(map))
}

/// 把已关闭连接的错误转成 CLI 错误。
pub(crate) fn closed_error() -> CliError {
    CliError::Client(ClientError::Closed)
}

/// 从 `cursor` 重新 attach，补齐广播 `Lagged` 丢掉的事件。
///
/// 调用方保持自己的 `cursor` 不变：回放事件会进入广播通道，按 `seq` 推进即可；
/// 若把游标直接跳到返回的基线，反而会把刚回放的终态事件当成已处理而跳过。
/// 若历史已不可回放（`reset`）且终端同时处于终态，则明确报错退出，避免永久
/// 等待一个永远不会再出现的事件。
pub(crate) async fn resync_events(
    client: &Client,
    locator: &TerminalLocator,
    cursor: u64,
) -> Result<(), CliError> {
    let attached = client
        .attach(AttachParams {
            locator: locator.clone(),
            cursor: Some(cursor),
        })
        .await?;
    if let Some(reset) = attached.reset {
        if attached.snapshot.state.is_terminal() {
            return Err(CliError::Client(ClientError::Transport(format!(
                "事件回放不可用({reset:?})且终端已结束，无法确定退出结果；请改用 status/read 查询"
            ))));
        }
        eprintln!("事件回放不完整({reset:?})，已从服务端当前基线继续");
    }
    Ok(())
}
