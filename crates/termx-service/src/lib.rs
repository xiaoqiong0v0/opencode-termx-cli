//! termx-service：本机 daemon。
//!
//! M1/F1 提供环回 WebSocket 传输、token 鉴权与 `hello` 握手；M1/F2 增加共享
//! registry 与 `terminal.create` / `terminal.list` / `terminal.status`；M1/F3
//! 增加 attach/detach/subscribe 与事件推送（读写解耦）；M1/F4 增加
//! `TerminalLocator`、连接级 `client_id` 与输入 lease；M2c1 把 `mode=pty`
//! 接到 `termx-proc` 的真实 PTY/ConPTY，并由连接层启动事件摄取任务。
//! `stdio` 模式与输入类 RPC（send/resize/signal）留到 M2c2/M3。
//!
//! 入口分两层：本 crate（`lib`）承载可测试的服务实现，`main.rs` 只做配置加载、
//! tracing 初始化与事件循环启动。

mod config;
mod server;

pub use config::{
    ConfigError, DEFAULT_EVENT_BUFFER_CAPACITY, DEFAULT_IDLE_TICK_INTERVAL,
    DEFAULT_MAX_FRAME_BYTES, PORT_FILE_NAME, ServiceConfig, TOKEN_FILE_NAME,
};
pub use server::{Service, ServiceError};
