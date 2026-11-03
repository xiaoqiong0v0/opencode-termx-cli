//! clap 子命令与参数定义。
//!
//! 全局参数（`--root-session-id` / `--name` / `--force` / `--config-dir` /
//! `--token` / `--endpoint` / `--json`）在子命令前后都可出现。

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use termx_protocol::{IdleTimeoutAction, IdleTimeoutUnit, LifecycleState, PruneBasis, TimeSpan};

/// PTY 默认列数。
pub const DEFAULT_PTY_COLS: u16 = 120;
/// PTY 默认行数。
pub const DEFAULT_PTY_ROWS: u16 = 30;

/// 终端定位参数：`--terminal-id <id>`，或退回全局 `--name`。
#[derive(Debug, Args)]
pub struct LocatorArgs {
    /// 终端内部 ID；给出时优先于 `--name`。
    #[arg(long = "terminal-id", value_name = "ID")]
    pub terminal_id: Option<String>,
}

/// `local` 子命令参数。
#[derive(Debug, Args)]
pub struct LocalArgs {
    /// 启动程序；缺省为 `%COMSPEC%`（Windows）/ `$SHELL`（Unix）。
    #[arg(long, value_name = "PROG")]
    pub cmd: Option<String>,
    /// 工作目录。
    #[arg(long, value_name = "DIR")]
    pub cwd: Option<String>,
    /// 环境变量增量，可重复，形如 `K=V`。
    #[arg(long = "env", value_name = "K=V")]
    pub env: Vec<String>,
    /// 初始列数。
    #[arg(long, default_value_t = DEFAULT_PTY_COLS)]
    pub cols: u16,
    /// 初始行数。
    #[arg(long, default_value_t = DEFAULT_PTY_ROWS)]
    pub rows: u16,
    /// 空闲超时秒数；0 或缺省表示永不超时。
    #[arg(long = "idle-timeout", value_name = "SECONDS")]
    pub idle_timeout: Option<u64>,
    /// 空闲超时动作；给出 `--idle-timeout` 时缺省为 `detach`。
    #[arg(
        long = "idle-action",
        value_name = "detach|terminate",
        value_parser = parse_idle_action,
        requires = "idle_timeout"
    )]
    pub idle_action: Option<IdleTimeoutAction>,
    /// 创建后附着并持续输出事件，直到终端结束或 Ctrl-C。
    #[arg(long)]
    pub follow: bool,
    /// 传给 `--cmd` 程序的参数（`--` 之后的全部内容）。
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, num_args = 0..)]
    pub args: Vec<String>,
}

/// `exec` 子命令参数。
#[derive(Debug, Args)]
pub struct ExecArgs {
    /// 使用 PTY 模式；缺省为 stdio 管道模式。
    #[arg(long)]
    pub pty: bool,
    /// 工作目录。
    #[arg(long, value_name = "DIR")]
    pub cwd: Option<String>,
    /// 环境变量增量，可重复，形如 `K=V`。
    #[arg(long = "env", value_name = "K=V")]
    pub env: Vec<String>,
    /// 命令及参数；`--` 后至少给一项。
    #[arg(
        required = true,
        trailing_var_arg = true,
        allow_hyphen_values = true,
        num_args = 1..
    )]
    pub cmd: Vec<String>,
}

/// `list` 子命令参数。
#[derive(Debug, Args)]
pub struct ListArgs {
    /// 按生命周期状态过滤，逗号分隔，如 `running,exited`。
    #[arg(long, value_delimiter = ',', value_parser = parse_state)]
    pub states: Vec<LifecycleState>,
    /// 返回条数上限。
    #[arg(long)]
    pub limit: Option<u32>,
}

/// `read` 子命令参数。
#[derive(Debug, Args)]
pub struct ReadArgs {
    /// 终端定位参数。
    #[command(flatten)]
    pub locator: LocatorArgs,
    /// 起始游标（必填）。
    #[arg(long)]
    pub cursor: u64,
    /// 单次读取的解码后字节上限。
    #[arg(long = "max-bytes")]
    pub max_bytes: Option<u64>,
}

/// `send` 子命令参数：`--text` 与 `--data` 二选一。
#[derive(Debug, Args)]
#[command(group(
    clap::ArgGroup::new("payload")
        .required(true)
        .multiple(false)
        .args(["text", "data"])
))]
pub struct SendArgs {
    /// 终端定位参数。
    #[command(flatten)]
    pub locator: LocatorArgs,
    /// UTF-8 文本。
    #[arg(long)]
    pub text: Option<String>,
    /// base64 编码的原始字节。
    #[arg(long)]
    pub data: Option<String>,
    /// 追加换行（pty 为 `\r`，stdio 为 `\n`）。
    #[arg(long)]
    pub enter: bool,
}

/// `resize` 子命令参数。
#[derive(Debug, Args)]
pub struct ResizeArgs {
    /// 终端定位参数。
    #[command(flatten)]
    pub locator: LocatorArgs,
    /// 列数。
    #[arg(long)]
    pub cols: u16,
    /// 行数。
    #[arg(long)]
    pub rows: u16,
}

/// `signal` 子命令参数。
#[derive(Debug, Args)]
pub struct SignalArgs {
    /// 终端定位参数。
    #[command(flatten)]
    pub locator: LocatorArgs,
    /// 信号名。
    #[arg(long, value_parser = ["SIGINT", "SIGTERM", "SIGKILL"])]
    pub signal: String,
}

/// `terminate` 子命令参数；`--force` 由全局参数提供。
#[derive(Debug, Args)]
pub struct TerminateArgs {
    /// 终端定位参数。
    #[command(flatten)]
    pub locator: LocatorArgs,
    /// 显式终止全部终端；不能与 `--terminal-id`/`--name` 同时使用。
    #[arg(long)]
    pub all: bool,
}

/// `detach` 子命令参数。
#[derive(Debug, Args)]
pub struct DetachArgs {
    /// 终端定位参数。
    #[command(flatten)]
    pub locator: LocatorArgs,
    /// 显式断开全部终端；不能与 `--terminal-id`/`--name` 同时使用。
    #[arg(long)]
    pub all: bool,
}

/// `prune` 子命令参数。
#[derive(Debug, Args)]
pub struct PruneArgs {
    /// 按生命周期状态过滤，逗号分隔。
    #[arg(long, value_delimiter = ',', value_parser = parse_state)]
    pub states: Vec<LifecycleState>,
    /// 早于该时长才清理，如 `30s` / `500ms`。
    #[arg(long = "older-than", value_parser = parse_time_span)]
    pub older_than: Option<TimeSpan>,
    /// 清理基准。
    #[arg(long, value_parser = ["last_activity", "created_at"])]
    pub basis: Option<String>,
    /// 显式清理全部终态终端；不能与 `--states`/`--older-than` 同时使用。
    #[arg(long)]
    pub all: bool,
}

/// `attach` 子命令参数。
#[derive(Debug, Args)]
pub struct AttachArgs {
    /// 终端定位参数。
    #[command(flatten)]
    pub locator: LocatorArgs,
    /// 起始游标；缺省只返回当前快照。
    #[arg(long)]
    pub cursor: Option<u64>,
}

/// `ssh` 子命令参数（M6 前仅占位）。
#[derive(Debug, Args)]
pub struct SshArgs {
    /// 预留的目标与参数。
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, num_args = 0..)]
    pub args: Vec<String>,
}

/// 子命令集合。
#[derive(Debug, Subcommand)]
pub enum Command {
    /// 创建本地 PTY 终端（默认 shell）。
    Local(LocalArgs),
    /// 一次性执行命令并按子进程退出码退出。
    Exec(ExecArgs),
    /// 查询终端状态快照。
    Status(LocatorArgs),
    /// 列出终端。
    List(ListArgs),
    /// 增量读取 output。
    Read(ReadArgs),
    /// 向终端写入文本或字节。
    Send(SendArgs),
    /// 调整 PTY 尺寸。
    Resize(ResizeArgs),
    /// 向终端发送信号。
    Signal(SignalArgs),
    /// 终止终端。
    Terminate(TerminateArgs),
    /// 清理终态终端。
    Prune(PruneArgs),
    /// 附着终端并返回快照/游标。
    Attach(AttachArgs),
    /// 断开附着。
    Detach(DetachArgs),
    /// 取得输入 lease。
    Claim(LocatorArgs),
    /// 释放输入 lease。
    Release(LocatorArgs),
    /// SSH 终端（M6 实现）。
    Ssh(SshArgs),
}

/// 顶层 CLI 定义。
#[derive(Debug, Parser)]
#[command(name = "termx-cli", version, about = "termx 本机终端客户端")]
pub struct Cli {
    /// opencode 会话 ID；缺省取 `TERMX_ROOT_SESSION_ID`，再缺省 `default`。
    #[arg(long, global = true, value_name = "ID")]
    pub root_session_id: Option<String>,
    /// 会话内名称；用于创建命名终端或按名定位。
    #[arg(long, global = true, value_name = "NAME")]
    pub name: Option<String>,
    /// 同名终端已存在时强制重建；`claim`/`terminate` 时强制操作。
    #[arg(long, global = true)]
    pub force: bool,
    /// 配置目录（含 `port`/`token`），覆盖 `TERMX_CONFIG_DIR`。
    #[arg(long, global = true, value_name = "DIR")]
    pub config_dir: Option<PathBuf>,
    /// 直接提供 token，覆盖 `TERMX_TOKEN` 与 token 文件。
    #[arg(long, global = true, value_name = "TOKEN")]
    pub token: Option<String>,
    /// 直接指定 WebSocket 地址，覆盖端口文件发现。
    #[arg(long, global = true, value_name = "URL")]
    pub endpoint: Option<String>,
    /// 事件/结果输出为单行 JSON。
    #[arg(long, global = true)]
    pub json: bool,
    /// 子命令。
    #[command(subcommand)]
    pub command: Command,
}

/// 解析生命周期状态（snake_case）。
fn parse_state(input: &str) -> Result<LifecycleState, String> {
    match input {
        "created" => Ok(LifecycleState::Created),
        "running" => Ok(LifecycleState::Running),
        "exiting" => Ok(LifecycleState::Exiting),
        "exited" => Ok(LifecycleState::Exited),
        "interrupted" => Ok(LifecycleState::Interrupted),
        "error" => Ok(LifecycleState::Error),
        other => Err(format!(
            "未知状态 {other:?}；可用值：created/running/exiting/exited/interrupted/error"
        )),
    }
}

/// 解析 `30s` / `500ms` 形式的时长；无单位按秒处理。
fn parse_time_span(input: &str) -> Result<TimeSpan, String> {
    let trimmed = input.trim();
    let (value, unit) = if let Some(value) = trimmed.strip_suffix("ms") {
        (value, IdleTimeoutUnit::Ms)
    } else if let Some(value) = trimmed.strip_suffix('s') {
        (value, IdleTimeoutUnit::S)
    } else {
        (trimmed, IdleTimeoutUnit::S)
    };
    let value: u64 = value
        .parse()
        .map_err(|_| format!("时长非法 {input:?}；示例：30s / 500ms"))?;
    Ok(TimeSpan { value, unit })
}

/// 解析 `local --idle-action`。
fn parse_idle_action(input: &str) -> Result<IdleTimeoutAction, String> {
    match input {
        "detach" => Ok(IdleTimeoutAction::Detach),
        "terminate" => Ok(IdleTimeoutAction::Terminate),
        other => Err(format!("未知空闲动作 {other:?}；可用值：detach/terminate")),
    }
}

/// 解析 `prune --basis`。
pub fn parse_prune_basis(input: &str) -> Result<PruneBasis, String> {
    match input {
        "last_activity" => Ok(PruneBasis::LastActivity),
        "created_at" => Ok(PruneBasis::CreatedAt),
        other => Err(format!(
            "未知基准 {other:?}；可用值：last_activity/created_at"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_time_span_supports_seconds_and_millis() {
        assert_eq!(
            parse_time_span("30s").expect("秒"),
            TimeSpan {
                value: 30,
                unit: IdleTimeoutUnit::S
            }
        );
        assert_eq!(
            parse_time_span("500ms").expect("毫秒"),
            TimeSpan {
                value: 500,
                unit: IdleTimeoutUnit::Ms
            }
        );
        assert_eq!(
            parse_time_span("7").expect("无单位").unit,
            IdleTimeoutUnit::S
        );
        assert!(parse_time_span("abc").is_err());
    }

    #[test]
    fn parse_state_accepts_snake_case_only() {
        assert_eq!(
            parse_state("running").expect("running"),
            LifecycleState::Running
        );
        assert_eq!(
            parse_state("interrupted").expect("interrupted"),
            LifecycleState::Interrupted
        );
        assert!(parse_state("Running").is_err());
    }

    #[test]
    fn parse_prune_basis_maps_two_values() {
        assert_eq!(
            parse_prune_basis("last_activity").expect("last_activity"),
            PruneBasis::LastActivity
        );
        assert!(parse_prune_basis("created").is_err(), "未知基准应报错");
    }

    #[test]
    fn parse_idle_action_maps_two_values() {
        assert_eq!(
            parse_idle_action("detach").expect("detach"),
            IdleTimeoutAction::Detach
        );
        assert_eq!(
            parse_idle_action("terminate").expect("terminate"),
            IdleTimeoutAction::Terminate
        );
        assert!(parse_idle_action("kill").is_err(), "未知动作应报错");
    }
}
