//! `termx-helper` 的 stdio sidecar 服务端实现。
//!
//! 协议见 `docs/specs/PROTOCOL.md` §11：
//! - stdin 每行一个 [`HelperRequest`]，首帧必须是 `hello`；
//! - stdout 每行一个 [`HelperEvent`]，**只承载协议 JSON**；
//! - 日志一律写 stderr（tracing）；
//! - 同一时刻只维护一个 PTY 会话，`spawn` 会先清理上一会话；
//! - stdin EOF 或进程退出前终止整棵进程树，确保无孤儿。
//!
//! 结构：`core` 只放协议、握手、事件转发与生命周期；平台差异（Unix
//! PTY/UDS、Windows ConPTY/named pipe、进程树能力）在 `platform` 中由
//! `cfg` 选择模块，core 不按平台分叉。

mod core;
mod platform;

pub use core::{EXIT_INTERNAL, EXIT_OK, EXIT_PROTOCOL, serve};
