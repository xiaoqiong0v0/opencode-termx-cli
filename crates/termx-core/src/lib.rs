//! 会话状态机、registry、事件缓冲与终端后端抽象。
//!
//! 依赖方向：本 crate 位于 `protocol` 之上，`service` / `cli` 之下。
//! F2 承载 `(root_session_id, name)` 会话键、事件 seq/ring buffer 与 mock 后端；
//! F3 增加事件摄取、attach/detach 与 cursor 回放；F4 增加 locator 与输入 lease。
//! 当前不创建真实 PTY / 子进程，真实后端留到 M2。

mod backend;
mod error;
mod event;
mod registry;
mod terminal;

pub use backend::{MOCK_PID_START, MockBackend, TerminalBackend};
pub use error::CoreError;
pub use event::{DEFAULT_EVENT_BUFFER_CAPACITY, EventBuffer, EventRecord, now_rfc3339};
pub use registry::{DETACH_REASON_DISCONNECT, INITIAL_CURSOR, Registry};
pub use terminal::{TerminalEntry, TerminalId};
