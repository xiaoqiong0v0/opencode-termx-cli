//! 终端后端抽象。
//!
//! F2 只提供 [`MockBackend`]：pid/pgid 来自进程内原子计数器，不创建真实进程。
//! M2 起由 `termx-proc` 提供 PTY / stdio 实现。

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// mock 后端的 pid/pgid 起始值。
pub const MOCK_PID_START: u32 = 1000;

/// 进程内 mock pid 分配器；从 [`MOCK_PID_START`] 起递增。
static NEXT_MOCK_PID: AtomicU32 = AtomicU32::new(MOCK_PID_START);

/// 终端后端能力：F2 只要求暴露 pid/pgid 与终止入口。
pub trait TerminalBackend: Send + Sync {
    /// 主进程 ID。
    fn pid(&self) -> u32;
    /// 进程组 ID。
    fn pgid(&self) -> u32;
    /// 终止后端进程（幂等）。
    fn terminate(&self);
}

/// 不创建真实进程的 mock 后端，供 F2 registry / service 测试使用。
#[derive(Debug)]
pub struct MockBackend {
    /// 分配到的 pid；mock 中 pgid 与 pid 相同。
    pid: u32,
    /// 是否已调用过 [`TerminalBackend::terminate`]。
    terminated: AtomicBool,
}

impl MockBackend {
    /// 从全局原子计数器分配新的 pid/pgid。
    pub fn new() -> Self {
        Self {
            pid: NEXT_MOCK_PID.fetch_add(1, Ordering::Relaxed),
            terminated: AtomicBool::new(false),
        }
    }

    /// 是否已被终止；测试用于验证 `force` 先终止旧终端。
    pub fn was_terminated(&self) -> bool {
        self.terminated.load(Ordering::Relaxed)
    }
}

impl Default for MockBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl TerminalBackend for MockBackend {
    fn pid(&self) -> u32 {
        self.pid
    }

    fn pgid(&self) -> u32 {
        self.pid
    }

    fn terminate(&self) {
        self.terminated.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_backend_allocates_increasing_pids_and_tracks_termination() {
        let first = MockBackend::new();
        let second = MockBackend::new();

        assert!(first.pid() >= MOCK_PID_START);
        assert!(second.pid() > first.pid());
        assert_eq!(first.pid(), first.pgid());
        assert!(!first.was_terminated());

        first.terminate();
        assert!(first.was_terminated());
        assert!(!second.was_terminated());
    }
}
