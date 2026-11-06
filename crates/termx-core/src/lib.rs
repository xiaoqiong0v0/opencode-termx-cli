//! 会话状态机、registry、事件缓冲与终端后端抽象。
//!
//! 依赖方向：本 crate 位于 `protocol` / `proc` 之上，`service` / `cli` 之下。
//! F2 承载 `(root_session_id, name)` 会话键、事件 seq/ring buffer 与 mock 后端；
//! F3 增加事件摄取、attach/detach 与 cursor 回放；F4 增加 locator 与输入 lease；
//! M2c1 接入 `termx-proc` 的真实 PTY/ConPTY 后端（`PtyFactory` / `PtyBackend`）；
//! M2c2 增加输入写入、缩放、信号与两阶段终止（锁内校验、锁外调后端）；
//! M3a 增加 stdio 管道后端（`StdioFactory` / `StdioBackend`）与按模式路由；
//! M3b 增加 `terminal.read`、输出淘汰 `output_dropped` 与有界订阅队列背压；
//! M4b 增加交互识别状态机（OSC 133/633 + 前台进程组 + 无输出启发式）。

mod backend;
mod error;
mod event;
mod interaction;
mod output;
mod registry;
mod terminal;

pub use backend::{
    BackendFactory, DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS, MOCK_PID_START, MockBackend, MockFactory,
    ModeFactory, PtyBackend, PtyFactory, SpawnedBackend, SshBackend, SshFactory, StdioBackend,
    StdioFactory, TerminalBackend,
};
pub use error::CoreError;
pub use event::{
    DEFAULT_EVENT_BUFFER_CAPACITY, EventBuffer, EventRecord, format_rfc3339, now_rfc3339,
    parse_rfc3339,
};
pub use interaction::{
    DEFAULT_NO_OUTPUT_THRESHOLD, DEFAULT_REPEAT_INTERVAL, InteractionAnalyzer, InteractionConfig,
    InteractionUpdate, InteractiveTarget, REASON_NO_OUTPUT, REASON_SHELL_INTEGRATION,
};
pub use output::{clean_raw_output, search, tail_chars, tail_lines};
pub use registry::{
    CLIENT_TIMEOUT_SOURCE, ClientlessCandidate, DEFAULT_READ_MAX_BYTES,
    DEFAULT_SUBSCRIBER_QUEUE_CAPACITY, DETACH_REASON_DISCONNECT, ExitOutcome, IDLE_DETACH_REASON,
    INITIAL_CURSOR, IdleAction, PreparedSignal, Registry, TIMEOUT_SOURCE,
};
pub use terminal::{PendingInterrupt, TerminalEntry, TerminalId};

mod transcript;
pub use transcript::{
    DEFAULT_QUIET_FINALIZE, DEFAULT_TRANSCRIPT_CAPACITY, TranscriptBuffer, clean_output,
};
