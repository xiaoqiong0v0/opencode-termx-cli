//! termx 客户端库（M5a）。
//!
//! 供 `termx-cli` 与 opencode adapter 共用：服务发现、WebSocket 连接、
//! `hello` 握手、请求/响应按 `id` 配对、事件广播与连接收尾。
//!
//! 设计边界：
//! - 本 crate **不解析终端渲染状态**，只转交协议类型与原始 output；
//! - 只依赖 `termx-protocol`，不依赖 `termx-service`（集成测试除外）；
//! - 连接断开时 pending 请求统一返回 [`ClientError::Closed`]，事件接收者读到 `Closed`；
//!   服务端连接级错误（`id == ""`）会留存为 [`Client::last_error`]，后续请求返回同一错误；
//! - 连接前强制校验 endpoint 为 `127.0.0.1` / `[::1]`，绝不向外部地址发送 token；
//! - **`attach` / `subscribe` 前必须先调用 [`Client::events`]**：回放事件经广播通道
//!   投递，没有接收者时会被丢弃。
//!
//! 典型用法：
//! ```no_run
//! # async fn example() -> Result<(), termx_client::ClientError> {
//! use termx_client::{Client, ClientConfig};
//!
//! // 配置目录必须显式：`discover()` 只读非空 `TERMX_CONFIG_DIR`，
//! // 也可用 `discover_from_dir(dir, env)` 直接传入目录。
//! let client = Client::connect(ClientConfig::discover()?).await?;
//! let mut events = client.events();
//! let terminals = client.list(Default::default()).await?;
//! let _ = (terminals, events.recv().await);
//! client.close().await;
//! # Ok(())
//! # }
//! ```

mod client;
mod config;
mod error;

pub use client::Client;
pub use config::{
    ClientConfig, DEFAULT_CLIENT_NAME, DEFAULT_REQUEST_TIMEOUT, PORT_FILE_NAME,
    TERMX_CONFIG_DIR_ENV, TERMX_TOKEN_ENV, TOKEN_FILE_NAME,
};
pub use error::ClientError;
