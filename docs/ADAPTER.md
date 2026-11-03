# opencode adapter 映射（termx_* 工具 → RPC）

adapter 是**薄层**：只把模型工具调用映射为 `termx-service` 的 RPC，并订阅事件做异步通知；**不解析终端渲染状态**，不持有终端对象。实际插件可在独立仓库实现（Node/TS 或 Rust 客户端均可复用 `termx-client`），本文件定义契约。

## 1. 连接与鉴权
- 发现：读取配置目录下 `port` 与 `token`（`TERMX_CONFIG_DIR` 可覆盖），组装 `ws://127.0.0.1:<port>`。
- 令牌优先级：`--token-file` > `TERMX_TOKEN` > 默认 token 文件；显式 `--endpoint` 只替换地址，不改变 token 优先级。
- 首帧必须 `hello{proto:"termx/1", token, client}`；失败错误：`auth_failed` / `version_mismatch`。
- 只连环回：客户端在 connect 前校验 endpoint 主机必须是 `127.0.0.1` / `[::1]`，其他主机直接拒绝，绝不发送 token。

## 2. 会话键
- 定位键：`(root_session_id, name)`；`terminal_id` 为内部唯一标识。
- 同名且无 `force` → `session_exists`（错误里带旧 `terminal_id`），adapter 应把说明透传给用户/模型。
- `force=true` 覆盖：旧终端落 `interrupted{source:"terminate"}`，再创建新终端。
- 只有**终端级方法**（`attach`/`detach`/`claim`/`release`/`status`/`read`/`send`/`resize`/`signal`/`terminate`）使用 `TerminalLocator`：`terminal_id` 优先，否则 `{root_session_id, name}`；`hello`/`terminal.list`/`terminal.prune`/`subscribe` 等非终端级方法不使用 locator。

## 3. 工具映射
| 工具 | RPC | 关键参数 | 返回/行为 |
| --- | --- | --- | --- |
| `termx_local` | `terminal.create` → `terminal.attach` | `mode:"pty"`, `cmd`(默认 shell), `cwd/env`, `cols/rows`, `idle_timeout`, `root_session_id`, `name`, `force` | `terminal_id` + 初始 `cursor`；adapter 订阅事件 |
| `termx_exec` | `terminal.create` → `attach(cursor=0)` → 等终态 | `mode:"stdio"`（或 `pty`）、`cmd/args` | 返回合并 output、`exit_code`/`signal`；不阻塞其它工具 |
| `termx_ssh` | M6 | — | 暂返回未实现 |
| `termx_list` | `terminal.list` | `states/limit` | 终端摘要 |
| `termx_status` | `terminal.status` | `locator` | `snapshot`（pid/pgid/foreground/tree/interactive/confidence） |
| `termx_read` | `terminal.read` | `locator`, `cursor`, `max_bytes` | 仅 output chunks；状态事件走订阅 |
| `termx_send` | `terminal.send` | `locator`, `text`/`data`, `enter` | 需 lease（`claim` 或 adapter 自动 claim） |
| `termx_resize` | `terminal.resize` | `locator`, `cols/rows` | pty 生效；stdio 空操作 |
| `termx_signal` | `terminal.signal` | `locator`, `signal` | `SIGINT`/`SIGTERM`/`SIGKILL` |
| `termx_terminate` | `terminal.terminate` | `locator`, `force` | 杀进程树、落终态 |
| `termx_prune` | `terminal.prune` | `states/older_than/basis` | 删除终态，返回 removed 列表 |
| `termx_claim` / `termx_release` | `terminal.claim` / `release` | `locator`, `force` | 输入 lease |
| `termx_attach` / `termx_detach` | `terminal.attach` / `detach` | `locator`, `cursor` | 重连/只断开，不杀终端 |

## 4. 事件 → 模型
- **先 `events()` 再 `subscribe` / `attach`**：`termx-client` 把回放事件写入广播通道，没有接收者时会被直接丢弃；adapter 必须先注册接收者。
- 订阅 `subscribe` / `attach` 回放，按 `seq` 去重；收到广播 `Lagged` 时按最后已处理 `cursor` 重新 `attach` 补齐。
- `waiting_input{confidence,reason}`、`interactive_changed{active,...}`：**异步通知**模型，不阻塞等待命令结束。
- `output`：增量；`output_dropped`：表示缺口；状态/终态事件不可丢。
- `exited{exit_code}` / `interrupted{signal,source}` / `error{code}`：终态唯一。
- 断线重连带最后已处理 `cursor`；服务端可返回 `reset`（清本地视图后继续）。

## 5. 会话生命周期
- opencode 会话关闭/重启只 `detach`；终端继续运行，按 `(root_session_id,name)` 或 `terminal_id` re-attach。
- 默认 `idle_timeout=0` 永不超时；配置正数时明确 `detach` 或 `terminate`。
- service 重启后连接关闭：客户端会看到事件通道 `Closed`、pending 请求返回 `Closed`；adapter 应将其视为 `service_lost` 并重建连接（v1 不发送合成的 `error{service_lost}` 事件）。

## 6. 安全与兼容
- 不记录/打印 token、原始输入输出、密钥。
- 旧 `term_cli` **不复用**；新工具名统一 `termx_*`，语义按本文件。
- 不把文本完成标记当主判据；以进程状态/终态事件为准。
