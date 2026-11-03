# 并发交互点清单（ASYNC）

本文件登记 `termx-service` / `termx-core` / `termx-proc` 中所有**并发交互点**（共享状态、保护手段、顺序契约与位置）。新增或改动并发路径时同批更新本文件。

核心红线：**绝不持 registry 锁跨 `await`，也不在持锁期间调用可能阻塞的后端方法**。需要后端操作时，先在锁内校验并 clone `Arc<dyn TerminalBackend>`，释放锁后再执行。

## 1. 总表

| # | 共享状态 | 保护手段 | 顺序契约 | 位置 |
| --- | --- | --- | --- | --- |
| 1 | `Registry`（终端条目、双索引、订阅簿、每终端 `seq`/ring） | `Arc<std::sync::Mutex<Registry>>` | 锁内只做同步短操作；锁外 `spawn` / `write` / `resize` / `signal` / `terminate` / `tree` / `foreground_pgrp`（统一 `spawn_blocking`） | `crates/termx-service/src/server.rs`、`crates/termx-core/src/registry.rs` |
| 2 | `PtySession`（master fd / ConPTY 句柄 / 写端） | `PtyBackend` 内 `std::sync::Mutex<PtySession>` | 写、缩放、信号、终止互斥；同一时刻只有一个后端操作 | `crates/termx-core/src/backend.rs`、`crates/termx-proc/src/{unix,windows}.rs` |
| 3 | 终端事件序号 `seq`、ring buffer 与 `pending_interrupt` | 同 #1 的 registry 锁 | `report_event` / `report_exit` / `publish` 在锁内递增 `seq`、写 ring、投递订阅；终态只发一次，终态后所有事件（含 output）丢弃。ring 容量只约束可淘汰的输出类记录（`output`/`output_dropped`），满时淘汰最旧者并立即落成 `output_dropped`（占用独立 `seq`）；`started`/交互/等待输入/attach/detach/lease/终态/capabilities **永不淘汰且不设额外上限**：状态事件持续写入时 ring 按 FIFO 持续增长，直到终端被 `prune`/移除。输出丢弃同时维护独立高水位（曾丢弃的最大 `to_seq`），marker 自身被淘汰后仍能判定游标 `too_old` | `crates/termx-core/src/registry.rs`、`event.rs` |
| 4 | 订阅簿（终端级 + 全局订阅 + 排除集 + 限流 watch） | 同 #1 的 registry 锁 | 注册/回放/投递/清理全部在锁内完成，避免回放空窗或重复投递。订阅发送端为 `tokio::sync::mpsc::Sender<Event>` 有界通道（容量来自 `ServiceConfig::subscriber_queue_capacity`），投递一律 `try_send`：**任何事件**队满都立即移除该订阅者并向连接级 `watch` 置位，由 service 发 `limit_exceeded` 后断开；不做订阅者级丢输出/补 marker，事件 `seq` 始终取自身单调值、不回退 | `crates/termx-core/src/registry.rs`、`crates/termx-service/src/server.rs` |
| 5 | `ProcEvent` 摄取任务 | 每终端独立 `tokio::spawn`；通道 `mpsc::Receiver` | 输出走 `report_event`；退出统一映射 `ExitOutcome::{Code,Signal}` 后走 `report_exit`，由 `pending_interrupt` 决定 interrupted/exited；收到终态后退出 | `crates/termx-service/src/server.rs`（`ingest_proc_events`） |
| 6 | WebSocket 读 / 写 / 事件转发任务 | 读循环串行处理请求；writer 与 forwarder 各一任务；事件走有界订阅队列，响应/关闭帧走无界 out 队列 | 读任务把响应入 out 队列；core 事件经有界队列由 forwarder 消费并入 out 队列；writer 串行写帧保证单连接帧顺序，但**不保证事件先于对应响应**（如 `terminate` 的 interrupted 可能早于或晚于 terminate 响应）。forwarder 同时 `select!` 连接级限流 `watch`：置位后先发 `error{limit_exceeded,retryable:true}`，再置 shutdown `watch`；读循环见 shutdown 停止并走收尾 | `crates/termx-service/src/server.rs`（`serve_connection` / `write_loop` / `forward_events`） |
| 7 | 连接断开清理 | 同一把 registry 锁 | **先 `release_all(lease)`（对每个释放的 lease 广播 `lease_revoked`），再 `detach_all(订阅)`，最后 `unregister_client`（注销限流 watch）**；随后关闭 `event_tx` / out 队列并等待 writer/forwarder 结束 | `crates/termx-service/src/server.rs`（`serve_connection` 收尾） |
| 8 | `PtySession` 读/等待/退出监视线程 | 线程间 `Arc<Gate>` / `Arc<Mutex<Option<PseudoConsole>>>` | 退出事件在输出排空后只发一次；ConPTY 由监视线程关闭伪控制台迫使 EOF | `crates/termx-proc/src/unix.rs`、`windows.rs` |
| 9 | 空闲检查 ticker（`last_activity` / `idle_timeout`） | `tokio::time::interval` 任务 + 同 #1 的 registry 锁 | 每 tick 在锁内 `expire_idle`：`detach` 在锁内投递 `detached{idle_timeout}` 并移除终端级订阅，`terminate` 在锁内落终态并取出后端 `Arc`；后端终止在锁外 `spawn_blocking`，每个后端独立 `tokio::spawn` 以并行终止；`last_activity` **只**由 `started`/`output`/`attached` 事件与 `send`/`resize`（显式 `touch`）刷新，交互/状态/终态事件不刷新，避免周期性 `waiting_input` 让超时永不触发；周期可由 `TERMX_IDLE_TICK_MS` 覆盖；shutdown 时通过 `watch` 停止并 `await` ticker | `crates/termx-service/src/server.rs`（`run_idle_ticker`）、`crates/termx-service/src/config.rs`、`crates/termx-core/src/registry.rs`、`crates/termx-core/src/terminal.rs` |
| 10 | `terminal.prune` 删除条目 | 同 #1 的 registry 锁 | 锁内只做索引/订阅/排除集同步删除；仅终态终端可删；不调用后端 | `crates/termx-core/src/registry.rs`（`prune`） |
| 11 | `terminal.read` 只读扫描 | 同 #1 的 registry 锁 | 锁内只读 ring（不注册/不修改订阅、不推进消费状态）；**只返回 `output`**，状态/终态由 attach/subscribe 回放；游标早于可回放下界（ring 首 seq 之前，或小于曾丢弃输出的高水位 `dropped_through`）返回 `too_old` + 基线；返回游标为本次扫描到的最后 `seq`（含被跳过的状态事件）；单条 output 超 `max_bytes` 仍返回并前进游标 | `crates/termx-core/src/registry.rs`（`read` / `read_after`） |
| 12 | `StdioSession` stdin 有界写队列 | `SyncSender<Vec<u8>>`（容量来自 `ServiceConfig::stdio_write_queue_capacity`）+ 专用 writer 线程 | `write` 只做非阻塞 `try_send`，永不持锁 `write_all`：队满返回 `WriteQueueFull`（映射 `limit_exceeded`），断连返回 `NotRunning`；writer 线程顺序消费并 `write_all`+flush，队列关闭后退出并关闭 stdin | `crates/termx-proc/src/stdio.rs`、`crates/termx-core/src/backend.rs`、`crates/termx-service/src/config.rs` |
| 13 | stdio 子进程收尾（终止/释放） | `StdioSession` 自有句柄 + `terminated` 幂等标记；Windows `JobHandle`（`KILL_ON_JOB_CLOSE`） | `terminate`/`Drop` 顺序固定：先关闭写队列（或已关闭则跳过）→ Unix `killpg(SIGTERM)` 宽限后 `SIGKILL`；Windows `TerminateJobObject` 后关闭 Job 句柄兜底 → wait 线程回收并发唯一终态。service `run_until` 停机时先停 ticker/accept，再锁外遍历 `collect_backends()` 逐个 best-effort 终止 | `crates/termx-proc/src/stdio.rs`、`crates/termx-proc/src/windows.rs`、`crates/termx-service/src/server.rs`（`run_until`） |
| 14 | 每终端 `InteractionAnalyzer`（OSC 扫描器、静默计时、前台交互对象、节流签名） | 同 #1 的 registry 锁 | 输出摄取在**同一把 registry 锁内**先 `report_event(output)` 再 `observe_output`：只做 OSC 增量解析与事件投递，不调用后端、不跨 `await`。交互 ticker 在锁内只取 `running_backends()` 快照；`foreground_pgrp`/`tree` 在锁外 `spawn_blocking` 采集，随后每终端单独取锁调用 `sample_interaction`。仅状态变化或节流到期才写事件，永不产生终态事件 | `crates/termx-core/src/interaction.rs`、`crates/termx-core/src/registry.rs`、`crates/termx-service/src/server.rs`（`ingest_proc_events` / `run_interaction_ticker`） |
| 15 | 交互识别 ticker | `tokio::time::interval` 任务 + 共享 `watch` 停止信号 | 周期缺省 250ms（`TERMX_INTERACTION_TICK_MS`），无输出阈值缺省 5s、重复间隔缺省 30s，分别由 `TERMX_INTERACTION_NO_OUTPUT_MS` / `TERMX_INTERACTION_REPEAT_MS` 覆盖，`TERMX_INTERACTION_ENABLED` 可整体关闭；关闭时 ticker 只等停止信号。每轮先取 `running_backends()` 快照，再**为每个终端独立 `tokio::spawn` 采样任务**：采集在 `spawn_blocking` + `timeout(2s)` 内完成，单个 `tree()` 卡住只影响自身；等待采样结果时 `select!` 停止信号，停机不等卡住任务。与空闲 ticker 共用 `run_until` 的 `watch`，停机时两 ticker 都 `await` 退出后才终止后端 | `crates/termx-service/src/server.rs`（`run_interaction_ticker` / `sample_terminal_interaction`）、`crates/termx-service/src/config.rs` |
| 16 | `termx-client`：写端 sink、pending 配对表、已用请求 ID、事件广播发送端、连接级错误 | 写端 `tokio::sync::Mutex<SplitSink>`；pending 为 `std::sync::Mutex<HashMap<id, oneshot::Sender>>`；`used_ids` 为 `std::sync::Mutex<HashSet<String>>`；events 为 `std::sync::Mutex<Option<broadcast::Sender<Event>>>`；`last_error` 为 `std::sync::Mutex<Option<ProtocolError>>`；`AtomicBool closed` | 请求先查 `last_error` 与 closed，再**在已用 ID 表中占位（连接内唯一，显式 ID 完成后也不释放）**，随后在 pending 表内二次校验并插入，最后取 sink 串行写帧；响应等待受 `ClientConfig::request_timeout` 约束，超时移除 pending 并返回 `Timeout`。单 reader 任务读帧：带 `id` 的响应移出 pending 并 `send`；`id == ""` 的连接级错误写入 `last_error` 后结束读循环；无 `id` 的帧广播。收尾 `shutdown()` 幂等：先置 closed → 取走 events 发送端（关闭通道）→ 清空 pending 并统一回 `Closed`，不清 `last_error`。std 锁不跨 `await`；仅 sink 的 tokio 锁在发送期间持有。`close()` 收尾后发 Close 帧并中断 reader；Drop 同样收尾。连接前 endpoint 还要求环回字面量，拒绝外部主机以免泄露 token | `crates/termx-client/src/client.rs`、`crates/termx-client/src/config.rs` |
| 17 | `termx-helper` 请求状态：当前 `PtySession`、转发任务句柄、`running` 标记 | 单任务串行（`run_loop` 独占 `HelperState`）+ `Arc<AtomicBool>` | stdin 逐行解析后**串行**处理；`spawn` 前若有活动会话返回 `bad_request`，否则先 `cleanup()`；PTY 操作经 `session_op` 临时移出会话、在 `spawn_blocking` 内执行后放回；`running=false` 只由转发任务/清理路径/成功的 `terminate` 置位（`terminate` 返回即代表进程树已回收，随后 `spawn` 不会被旧转发任务阻塞，会话句柄保留以支持重复 `terminate` 幂等）；判断题：`crates/termx-helper/src/lib.rs` 中 `HelperState` 方法内不得出现“先持会话再 `await`”的结构 | `crates/termx-helper/src/lib.rs`（`run_loop` / `HelperState`） |
| 18 | `termx-helper` PTY 事件转发 | 每会话独立 `tokio` 任务 + `mpsc::Receiver<ProcEvent>` + 无界 `UnboundedSender<HelperEvent>` | 转发任务只持有事件接收端，不触碰 `PtySession`；按 `termx-proc` 事件顺序编码 `output`/`exited`/`signaled` 并入队；事件流关闭（会话释放）后置 `running=false` 并退出；终态事件唯一由 `termx-proc` 保证 | `crates/termx-helper/src/lib.rs`（`spawn_forwarder` / `translate_event`） |
| 19 | `termx-helper` stdout 输出 | 单写任务 `UnboundedReceiver<HelperEvent>` | 串行 `write_all` + `\n` + `flush`，保证 JSON Lines 帧不交错；stdout 只写协议帧，日志固定 stderr；收尾顺序：请求循环退出 → `cleanup()`（terminate + await 转发任务）→ drop `out_tx` → await writer | `crates/termx-helper/src/lib.rs`（`write_loop` / `serve`） |
| 20 | `termx-ssh` helper 分发/握手/驱动 | 专用 tokio 运行时单驱动任务；`SshSession` 持有无界控制通道 `UnboundedSender<SshControl>`；`tree()` 用 `std::sync::mpsc` 一次性回复；上传阶段 `UploadGuard` 独占 `client::Handle` | 连接/认证/上传/`hello`/`spawn` 在驱动任务启动前完成，结果经 `sync_channel(1)` 回传；建连超时 30s、helper 分发/握手整体超时 60s、单步 10s，外层初始化等待 95s，**内层先于外层超时**；`UploadGuard` 在失败时显式 `rm -rf` 本次 `mktemp -d` 目录，取消/超时由 `Drop` 把连接移入后台任务完成清理与断开，绝不触碰 `helper_path`；`SshSession::pid/pgid/capabilities` 只读快照，`tree()` 同步发送请求并 `recv_timeout(2s)`，由驱动任务按请求顺序配对 `tree` 事件；驱动任务独占通道读写，控制消息串行下发；`terminate` 后最多等 5s 终态，超时按 best-effort 收尾；hash/上传校验只用路径与摘要，不含凭据 | `crates/termx-ssh/src/lib.rs`、`crates/termx-ssh/src/helper.rs` |

## 2. `terminal.send` / `resize` / `signal` / `terminate` 的锁纪律

- `terminal.status`：锁内 `status_parts` 取基础快照与运行中后端 `Arc`；锁外 `spawn_blocking(fill_runtime_info)` 调用 `foreground_pgrp` / `tree` 补运行信息；终态或后端缺省时保留最后快照。
- `terminal.send`：锁内 `Registry::prepare_send` 校验 **终态 → `already_terminal`、无 lease/非 owner → `permission_denied`**，并 clone 后端；锁外 `spawn_blocking(backend.write)`。`data`（base64）优先于 `text`，`enter=true` 追加 `\r`（pty）/`\n`（stdio）（`server.rs::send_payload`/`enter_byte`）。stdio 后端 `write` 只入有界队列，不阻塞。
- `terminal.resize`：锁内 `prepare_resize` 校验非终态并记录新尺寸，锁外调用后端；终态 → `already_terminal`。
- `terminal.signal`：锁内 `prepare_signal` 只校验非终态并 clone 后端，**不记录** `pending_interrupt`；锁外 `spawn_blocking(PreparedSignal::execute)` 调后端 `signal`。**只有后端成功返回后**才在下一把锁内 `confirm_signal` 落 `pending_interrupt`（`SIGINT` → `source:control-c`，其它 → `source:signal`）；失败（未知信号、进程已退出等）不记录，避免把随后正常退出误报为 `interrupted`。Windows stdio 的 `SIGINT`/`SIGTERM`/`SIGKILL` 统一走 Job 进程树终止；pty 的 `SIGINT` 仍写 `0x03`。进程退出的终态映射见 `report_exit`。
- `terminal.terminate`：锁内 `prepare_terminate` 先落 `interrupted{signal:"SIGTERM",source:"terminate"}`、记录 pending、清 lease（只一次，幂等），锁外调用后端终止。重复 terminate 返回 `ok:true` 且不再发终态事件。
- `force` 替换：`register_create` 在锁内先落旧终端 `interrupted{source:"terminate"}`、清 lease、从 `by_key`/`by_id`/订阅簿摘除并取出后端 `Arc`，锁外再终止；旧摄取任务后续事件因终态被忽略。
- 同步便捷入口 `Registry::{send,resize,signal,terminate}` 会在**调用方已持有的锁内**直接调用后端，只允许测试/单进程工具使用；service 一律走 `prepare_*`。
- `terminal.prune`：锁内按终态 + `states` + `older_than`/`basis` 选出待删 ID，随后同步移除 `by_id`/`by_key`/终端级订阅/全局排除项；不触碰后端。
- 空闲 ticker：`expire_idle` 只在锁内做判定与事件投递（非阻塞 send）；`IdleAction::Terminate` 携带的后端 `Arc` 在锁外 `spawn_blocking(terminate)`。`detach` 动作会在锁内把 `last_activity` 刷成 `now`，避免同一空闲周期重复触发。
- 交互识别：`ingest_proc_events` 的 `Output` 分支在同一锁内先写 output 事件、再把原始字节喂给 `InteractionAnalyzer`（OSC 133/633 增量解析 + 静默计时）；`A`/`B`/`D` 产 `high/shell_integration` 等待候选，`C` 清除候选；`sample_interaction` 只接收锁外采集好的 `fg`/`tree`，锁内不调用任何后端方法。`no_output` 的 `medium` 仅在“前台 pgrp 指向根组之外的子进程”时使用；`fg=None`（stdio/无 TTY）或仍为根组保持 `low`，不伪造交互证据。
- 交互 ticker：每轮先在锁内克隆运行中后端 `Arc`，再为每个终端独立 `tokio::spawn` 采样任务（内部 `spawn_blocking` 采集 + `timeout`），并行提交 `sample_interaction`；等待采样结果时 `select!` 停止信号，`enabled=false` 时轮询体不执行，ticker 直接等待停止信号。`waiting_input`/`interactive_changed` 均为非终态，不影响 `exited`/`interrupted` 的唯一性。

## 3. 事件摄取与连接任务边界

- 摄取任务只在 `report_event` 前后短暂取锁，不在锁内 `await`；事件经**有界** `mpsc::Sender<Event>` 投递到连接级转发任务。
- 转发任务把 `Event` 序列化为 JSON 帧后写入连接级 `out_tx`；writer 串行消费，保证同一连接帧顺序。
- 终端被 `force` 替换或删除后，`report_event` 返回 `not_found`，摄取任务自行退出，避免无限重试。
- 断连顺序固定：`release_all`（逐终端广播 `lease_revoked`）→ `detach_all` → `unregister_client` → 关闭 `event_tx`/`out_tx` → 等待 writer/forwarder 结束。先释放 lease 可避免终端被永久锁死；再 detach 可避免 registry 持有失效 sender。
- 订阅队列背压（M3b）：`try_send` 非阻塞。**任何事件**队满 → 移除订阅者并触发限流 watch，forwarder 发连接级 `error{id:"",code:"limit_exceeded",retryable:true}` 后断开（不伪装成正常 `detached`，也不在订阅者侧补合成 marker）。`seq` 始终取事件自身单调值，绝不回退；队列满导致的断连会让客户端按 cursor 重连补齐。终端 ring 的 `output_dropped` 是另一层机制：只由 ring 淘汰 output 触发，marker 用新 `seq`、闭区间只含实际被丢 output。
- stdio 收尾顺序（M3）：`StdioSession::Drop`/`terminate` 先终止子进程（Unix `killpg`；Windows `TerminateJobObject` + 关闭 Job 触发 `KILL_ON_JOB_CLOSE`）并关闭写队列；writer 线程退出时关闭 stdin，避免调用方在锁内阻塞 `write_all`。service `run_until` 停机顺序：停 accept → 停 ticker 并 `await` → 锁内 `collect_backends()` → 锁外逐个终止。

## 4. termx-helper 线程边界（M6a 本地 stdio）

- **stdin 请求循环**：`run_loop` 独占 `HelperState`，逐行解析 `HelperRequest` 后串行处理；首帧 `hello` 校验失败即发 `error` 并退出。请求之间不共享锁。
- **PTY 操作**：`spawn` 与每次 `input` / `resize` / `signal` / `terminate` 都经 `session_op` 把 `PtySession` 移出 `Option`，在 `spawn_blocking` 中执行后放回；运行时线程不直接调用可能阻塞的 PTY 方法。
- **事件转发任务**：每会话一个 `tokio` 任务消费 `termx-proc` 的 `Receiver<ProcEvent>`，只做 base64 与流映射后入无界发送队列；不持有 `PtySession`。事件流关闭时置 `running=false`。
- **stdout 写任务**：单任务消费 `HelperEvent` 队列，串行写一行 JSON 并 flush；stdout 只含协议帧，tracing 固定 stderr。
- **收尾顺序**：stdin EOF / 读错误 → 跳出循环 → `cleanup()`（`spawn_blocking(terminate)` → `await` 转发任务）→ `drop(out_tx)` → `await` writer。`serve` 任一路径都走该顺序，确保 PTY 与进程树被释放、无孤儿。
- **顺序契约**：同一时刻只有一个 PTY 会话；活动会话期间的 `spawn` 直接 `bad_request`；`exited` / `signaled` 由 `termx-proc` 保证唯一，且输出在终态前排空。

## 5. 检查方式

- 代码审查：`server.rs` 中所有 `lock_registry(...)` 作用域内不得出现 `.await`，也不得调用 `TerminalBackend::{write,resize,signal,terminate,tree,foreground_pgrp}`。
- 测试：`cargo test -p termx-core --test io`（lease 门禁 / terminate 幂等）、`cargo test -p termx-service --test send_terminate`（真实 PTY/ConPTY、无孤儿）。
- 空闲/清理：`cargo test -p termx-core --test idle_prune`（detach/terminate/prune 与订阅、排除集清理）、`cargo test -p termx-service --test idle_prune`（ticker 真跑、杀进程树、活跃终端不误判、detach 后不再收事件）。
- 读取/背压：`cargo test -p termx-core --test read --test backpressure`（只返回 output、chunks/cursor、增量、too_old、max_bytes 与单条超额、输出淘汰闭区间、状态事件保留、慢订阅者限流）、`cargo test -p termx-service --test pty`（真实 PTY 退出后 read）、`cargo test -p termx-service --test hello slow_subscriber`（`limit_exceeded` 断连）、`cargo test -p termx-service --test hello max_terminals`（AC-16）。
- stdio：`cargo test -p termx-proc --test stdio`（stdout/stderr 分离、终态唯一、env/cwd/不经 shell、killpg/Job 终止与后代消失、drop 收尾）、`cargo test -p termx-service --test stdio`（service 退出终止长驻子进程、`enter` 换行）。
- 交互识别：`cargo test -p termx-core --test interaction`（OSC 置信度/reason、前台 pgrp 变化、节流、终态不受影响）、`cargo test -p termx-service --test interaction`（真实 PTY 中 OSC prompt → `waiting_input{high,shell_integration}`，Unix 前台作业 → `interactive_changed`，期间无 `exited`）。
- 客户端：`cargo test -p termx-client --test lifecycle --test concurrency`（hello/错误映射、真实 PTY 收发、并发配对、事件与响应并行、断开后 pending 返回 `Closed`）。
- helper：`cargo test -p termx-helper`（`--version`、hello/proto 错误、spawn 输出与退出码、input/resize/terminate、stdin EOF 清理）；手工检查 `termx-helper serve` 的 stdout 不含日志行。
- 改动上述任一交互点时，必须同批更新本表的「顺序契约」与对应测试。
