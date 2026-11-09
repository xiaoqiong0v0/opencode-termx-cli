//! helper 平台适配层。
//!
//! `core` 只调用这里的统一接口；平台差异（Unix PTY/UDS/termios、Windows
//! ConPTY/named pipe、进程树能力）全部收敛在 `unix` / `windows` 模块，
//! 不在 core 中按平台分叉。

use std::collections::BTreeMap;

use termx_proc::{ProcEvent, PtySession};
use termx_shell_events::{EVENT_ENDPOINT_ENV, EVENT_TOKEN_ENV, ShellEventChannel};
use tokio::sync::mpsc::Receiver;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub(crate) use unix::{open_event_channel, spawn, tree_supported};
#[cfg(windows)]
pub(crate) use windows::{open_event_channel, spawn, tree_supported};

/// 把事件通道 endpoint/token 注入子进程环境。
///
/// token 只进入子进程环境，不写日志；通道创建失败由平台模块降级返回 `None`。
fn inject_event_env(channel: &ShellEventChannel, env: &mut Option<BTreeMap<String, String>>) {
    let env = env.get_or_insert_with(BTreeMap::new);
    env.insert(
        EVENT_ENDPOINT_ENV.to_string(),
        channel.endpoint().to_string(),
    );
    env.insert(EVENT_TOKEN_ENV.to_string(), channel.token().to_string());
}

/// 平台 PTY/ConPTY 启动结果。
pub(crate) type SpawnedPty = (PtySession, Receiver<ProcEvent>);
