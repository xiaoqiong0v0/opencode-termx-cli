//! 会话状态机、registry、事件缓冲与终端后端抽象。
//!
//! 依赖方向：本 crate 位于 `protocol` / `proc` 之上，`service` / `cli` 之下。
//! F2 承载 `(root_session_id, name)` 会话键、事件 seq/ring buffer 与 mock 后端；
//! F3 增加事件摄取、attach/detach 与 cursor 回放；F4 增加 locator 与输入 lease；
//! M2c1 接入 `termx-proc` 的真实 PTY/ConPTY 后端（`PtyFactory` / `PtyBackend`）；
//! M2c2 增加输入写入、缩放、信号与两阶段终止（锁内校验、锁外调后端）。

mod backend;
mod error;
mod event;
mod registry;
mod terminal;

pub use backend::{
    BackendFactory, DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS, MOCK_PID_START, MockBackend, MockFactory,
    PtyBackend, PtyFactory, SpawnedBackend, TerminalBackend,
};
pub use error::CoreError;
pub use event::{
    DEFAULT_EVENT_BUFFER_CAPACITY, EventBuffer, EventRecord, format_rfc3339, now_rfc3339,
    parse_rfc3339,
};
pub use registry::{
    DETACH_REASON_DISCONNECT, ExitOutcome, IDLE_DETACH_REASON, INITIAL_CURSOR, IdleAction,
    PreparedSignal, Registry, SignalAction, TIMEOUT_SOURCE,
};
pub use terminal::{PendingInterrupt, TerminalEntry, TerminalId};
