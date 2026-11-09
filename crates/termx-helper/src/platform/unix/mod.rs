//! Unix 平台适配：PTY/termios 由 `termx-proc` 负责，事件通道使用私有目录
//! 下的 Unix domain socket。

use std::collections::BTreeMap;

use termx_proc::{ProcError, PtyConfig};
use termx_shell_events::ShellEventChannel;
use tracing::warn;

use super::{SpawnedPty, inject_event_env};

/// 在 PTY 中启动子进程；termios 细节由 `termx-proc` 的平台实现处理。
pub(crate) fn spawn(config: PtyConfig) -> Result<SpawnedPty, ProcError> {
    termx_proc::PtySession::spawn(config)
}

/// 创建 Unix 事件通道并把 endpoint/token 注入子进程环境。
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
            warn!(%error, "shell 事件通道创建失败，降级为无事件");
            None
        }
    }
}

/// helper 的进程树采集只在 Linux 上通过 `/proc` 提供。
pub(crate) fn tree_supported() -> bool {
    cfg!(target_os = "linux")
}
