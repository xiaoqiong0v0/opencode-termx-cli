# 并发交互点清单（ASYNC）

本文件登记 `termx-service` / `termx-core` / `termx-proc` 中所有**并发交互点**（共享状态、保护手段、顺序契约与位置）。新增或改动并发路径时同批更新本文件。

核心红线：**绝不持 registry 锁跨 `await`，也不在持锁期间调用可能阻塞的后端方法**。需要后端操作时，先在锁内校验并 clone `Arc<dyn TerminalBackend>`，释放锁后再执行。

## 1. 总表

| # | 共享状态 | 保护手段 | 顺序契约 | 位置 |
| --- | --- | --- | --- | --- |
| 1 | `Registry`（终端条目、双索引、订阅簿、每终端 `seq`/ring） | `Arc<std::sync::Mutex<Registry>>` | 锁内只做同步短操作；锁外 `spawn` / `write` / `resize` / `signal` / `terminate` / `tree` / `foreground_pgrp`（统一 `spawn_blocking`） | `crates/termx-service/src/server.rs`、`crates/termx-core/src/registry.rs` |
| 2 | `PtySession`（master fd / ConPTY 句柄 / 写端） | `PtyBackend` 内 `std::sync::Mutex<PtySession>` | 写、缩放、信号、终止互斥；同一时刻只有一个后端操作 | `crates/termx-core/src/backend.rs`、`crates/termx-proc/src/{unix,windows}.rs` |
| 3 | 终端事件序号 `seq`、ring buffer 与 `pending_interrupt` | 同 #1 的 registry 锁 | `report_event` / `report_exit` / `publish` 在锁内递增 `seq`、写 ring、投递订阅；终态只发一次，终态后所有事件（含 output）丢弃 | `crates/termx-core/src/registry.rs`、`event.rs` |
| 4 | 订阅簿（终端级 + 全局订阅 + 排除集） | 同 #1 的 registry 锁 | 注册/回放/投递/清理全部在锁内完成，避免回放空窗或重复投递 | `crates/termx-core/src/registry.rs` |
| 5 | `ProcEvent` 摄取任务 | 每终端独立 `tokio::spawn`；通道 `mpsc::Receiver` | 输出走 `report_event`；退出统一映射 `ExitOutcome::{Code,Signal}` 后走 `report_exit`，由 `pending_interrupt` 决定 interrupted/exited；收到终态后退出 | `crates/termx-service/src/server.rs`（`ingest_proc_events`） |
| 6 | WebSocket 读 / 写 / 事件转发任务 | 读循环串行处理请求；writer 与 forwarder 各一任务，经无界队列解耦 | 读任务把响应入 out 队列；core 事件经 forwarder 入同一队列；writer 串行写帧保证单连接帧顺序，但**不保证事件先于对应响应**（如 `terminate` 的 interrupted 可能早于或晚于 terminate 响应） | `crates/termx-service/src/server.rs`（`serve_connection` / `write_loop` / `forward_events`） |
| 7 | 连接断开清理 | 同一把 registry 锁 | **先 `release_all(lease)`（对每个释放的 lease 广播 `lease_revoked`），再 `detach_all(订阅)`**；随后关闭 `event_tx` / out 队列并等待 writer/forwarder 结束 | `crates/termx-service/src/server.rs`（`serve_connection` 收尾） |
| 8 | `PtySession` 读/等待/退出监视线程 | 线程间 `Arc<Gate>` / `Arc<Mutex<Option<PseudoConsole>>>` | 退出事件在输出排空后只发一次；ConPTY 由监视线程关闭伪控制台迫使 EOF | `crates/termx-proc/src/unix.rs`、`windows.rs` |
| 9 | 空闲检查 ticker（`last_activity` / `idle_timeout`） | `tokio::time::interval` 任务 + 同 #1 的 registry 锁 | 每 tick 在锁内 `expire_idle`：`detach` 在锁内投递 `detached{idle_timeout}` 并移除终端级订阅，`terminate` 在锁内落终态并取出后端 `Arc`；后端终止在锁外 `spawn_blocking`，每个后端独立 `tokio::spawn` 以并行终止；周期可由 `TERMX_IDLE_TICK_MS` 覆盖；shutdown 时通过 `watch` 停止并 `await` ticker | `crates/termx-service/src/server.rs`（`run_idle_ticker`）、`crates/termx-service/src/config.rs`、`crates/termx-core/src/registry.rs` |
| 10 | `terminal.prune` 删除条目 | 同 #1 的 registry 锁 | 锁内只做索引/订阅/排除集同步删除；仅终态终端可删；不调用后端 | `crates/termx-core/src/registry.rs`（`prune`） |

## 2. `terminal.send` / `resize` / `signal` / `terminate` 的锁纪律

- `terminal.status`：锁内 `status_parts` 取基础快照与运行中后端 `Arc`；锁外 `spawn_blocking(fill_runtime_info)` 调用 `foreground_pgrp` / `tree` 补运行信息；终态或后端缺省时保留最后快照。
- `terminal.send`：锁内 `Registry::prepare_send` 校验 **终态 → `already_terminal`、无 lease/非 owner → `permission_denied`**，并 clone 后端；锁外 `spawn_blocking(backend.write)`。`data`（base64）优先于 `text`，`enter=true` 追加 `\r`（`server.rs::send_payload`）。
- `terminal.resize`：锁内 `prepare_resize` 校验非终态并记录新尺寸，锁外调用后端；终态 → `already_terminal`。
- `terminal.signal`：锁内 `prepare_signal` 校验非终态；`SIGINT` 记录 `pending_interrupt=(SIGINT, control-c)` 并返回 `WriteCtrlC`；`SIGTERM`/`SIGKILL` 记录 `pending_interrupt=(<信号名>, signal)` 后返回 `Send(name)`；锁外 `spawn_blocking(PreparedSignal::execute)`。进程退出时 `report_exit` 优先按 pending 落 `interrupted`（Windows 无 `Signaled` 事件，也靠此统一语义）。未知信号 → `bad_request`（`CoreError::UnsupportedSignal`）。
- `terminal.terminate`：锁内 `prepare_terminate` 先落 `interrupted{signal:"SIGTERM",source:"terminate"}`、记录 pending、清 lease（只一次，幂等），锁外调用后端终止。重复 terminate 返回 `ok:true` 且不再发终态事件。
- `force` 替换：`register_create` 在锁内先落旧终端 `interrupted{source:"terminate"}`、清 lease、从 `by_key`/`by_id`/订阅簿摘除并取出后端 `Arc`，锁外再终止；旧摄取任务后续事件因终态被忽略。
- 同步便捷入口 `Registry::{send,resize,signal,terminate}` 会在**调用方已持有的锁内**直接调用后端，只允许测试/单进程工具使用；service 一律走 `prepare_*`。
- `terminal.prune`：锁内按终态 + `states` + `older_than`/`basis` 选出待删 ID，随后同步移除 `by_id`/`by_key`/终端级订阅/全局排除项；不触碰后端。
- 空闲 ticker：`expire_idle` 只在锁内做判定与事件投递（非阻塞 send）；`IdleAction::Terminate` 携带的后端 `Arc` 在锁外 `spawn_blocking(terminate)`。`detach` 动作会在锁内把 `last_activity` 刷成 `now`，避免同一空闲周期重复触发。

## 3. 事件摄取与连接任务边界

- 摄取任务只在 `report_event` 前后短暂取锁，不在锁内 `await`；事件经 `UnboundedSender<Event>` 投递到连接级转发任务。
- 转发任务把 `Event` 序列化为 JSON 帧后写入连接级 `out_tx`；writer 串行消费，保证同一连接帧顺序。
- 终端被 `force` 替换或删除后，`report_event` 返回 `not_found`，摄取任务自行退出，避免无限重试。
- 断连顺序固定：`release_all`（逐终端广播 `lease_revoked`）→ `detach_all` → 关闭 `event_tx`/`out_tx` → 等待 writer/forwarder 结束。先释放 lease 可避免终端被永久锁死；再 detach 可避免 registry 持有失效 sender。

## 4. 检查方式

- 代码审查：`server.rs` 中所有 `lock_registry(...)` 作用域内不得出现 `.await`，也不得调用 `TerminalBackend::{write,resize,signal,terminate,tree,foreground_pgrp}`。
- 测试：`cargo test -p termx-core --test io`（lease 门禁 / terminate 幂等）、`cargo test -p termx-service --test send_terminate`（真实 PTY/ConPTY、无孤儿）。
- 空闲/清理：`cargo test -p termx-core --test idle_prune`（detach/terminate/prune 与订阅、排除集清理）、`cargo test -p termx-service --test idle_prune`（ticker 真跑、杀进程树、活跃终端不误判、detach 后不再收事件）。
- 改动上述任一交互点时，必须同批更新本表的「顺序契约」与对应测试。
