//! 本地 PTY / ConPTY 与进程管理（M0 占位）。
//!
//! M2 起在 Linux 直接绑定 `openpt/grantpt/unlockpt/termios/ioctl`，
//! Windows 绑定 `CreatePseudoConsole` 系列接口；本阶段不引入平台依赖。

/// 进程组标识。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessGroupId(pub u32);

/// 平台 PTY/进程后端抽象占位；M2 起由各平台实现填充。
pub trait PtyBackend {
    /// 后端名称，用于日志与诊断。
    fn name(&self) -> &'static str;
}
