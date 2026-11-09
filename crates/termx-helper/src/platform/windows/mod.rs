//! Windows 平台适配：ConPTY 由 `termx-proc` 负责，事件通道使用命名管道。

use std::collections::BTreeMap;

use termx_proc::{ProcError, PtyConfig};
use termx_shell_events::ShellEventChannel;
use tracing::warn;

use super::{SpawnedPty, inject_event_env};

/// 在 ConPTY 中启动子进程；控制台细节由 `termx-proc` 的平台实现处理。
pub(crate) fn spawn(config: PtyConfig) -> Result<SpawnedPty, ProcError> {
    termx_proc::PtySession::spawn(config)
}

/// 创建命名管道事件通道并把 endpoint/token 注入子进程环境。
///
/// 创建失败只降级（`started.events=false`），绝不阻断 spawn。
pub(crate) fn open_event_channel(
    requested: bool,
    env: &mut Option<BTreeMap<String, String>>,
) -> Option<ShellEventChannel> {
    if !requested {
        return None;
    }
    match ShellEventChannel::open() {
        Ok(channel) => {
            inject_event_env(&channel, env);
            Some(channel)
        }
        Err(error) => {
            warn!(%error, "shell 事件命名管道创建失败，降级为无事件");
            None
        }
    }
}

/// Windows helper 不提供 `/proc` 语义的进程树。
pub(crate) fn tree_supported() -> bool {
    false
}
