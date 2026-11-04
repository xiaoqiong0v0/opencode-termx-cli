# opencode adapter 映射（termx_cli 单工具 → RPC）

adapter 是**薄层**：只把模型工具调用映射为 `termx-service` 的 RPC，并订阅事件做异步通知；**不解析终端渲染状态**，不持有终端对象。插件只暴露一个工具 **`termx_cli`**（参数 `{ args: string }`，完整命令行字符串），避免与旧全局 `term_cli` 重名；子命令解析规则与旧项目一致（单/双引号、空引号、未闭合引号报错）。本仓参考实现位于 `opencode-plugin/termx/src`；本文件定义契约，其他实现可复用 `termx-client`。

## 1. 连接与鉴权
- 发现：读取配置目录下 `port` 与 `token`（`TERMX_CONFIG_DIR` 可覆盖），组装 `ws://127.0.0.1:<port>`。
- 令牌优先级：`--token-file` > `TERMX_TOKEN` > 默认 token 文件；显式 `--endpoint` 只替换地址，不改变 token 优先级。
- 首帧必须 `hello{proto:"termx/1", token, client, root_session_id}`；普通连接缺 `root_session_id` 会被拒绝（`bad_request`）。失败错误：`auth_failed` / `version_mismatch`。
- **每个 opencode 会话一条独立 WebSocket 连接**：连接级身份在 `hello` 固定为 `root_session_id`，不能在后续请求中切换。插件按 `endpoint + token + rootSessionId(+admin)` 缓存连接，工具调用与心跳必须使用该会话自己的连接。
- Web 代理是管理连接：`hello{admin:true}`（不带 `root_session_id`），可查看/操作全部会话。`admin` 目前是客户端声明、服务端不额外鉴权：持有同一 token 的本机进程都可声明；会话隔离用于防误操作，不构成同用户恶意进程的安全边界。
- 只连环回：客户端在 connect 前校验 endpoint 主机必须是 `127.0.0.1` / `[::1]`，其他主机直接拒绝，绝不发送 token。

## 2. 会话键与隔离
- 定位键：`(root_session_id, name)`；`terminal_id` 为内部唯一标识。
- 终端默认**仅同 root 会话可访问**：普通连接按 `terminal_id` 访问其它会话的终端返回 `permission_denied`；`list` / `prune` 自动限定本会话，`session.list` 与 `subscribe(all=true)` 仅管理员可用。
- 每个 opencode 会话独立连接；会话 A 的连接不能代 B 发心跳/关闭通知，也不能把终端迁移到第三会话（`rebind` 目标必须是自身会话或管理员连接）。
- `@web` 是 Web 未绑定终端的保留 `root_session_id`：普通会话不可见/不可访问，也不得把它作为自身 `root_session_id`；该会话终端豁免无会话超时，只能由管理员手动终止或 `rebind` 到真实会话/迁回 `@web`（解绑）。
- 同名且无 `force` → `session_exists`（错误里带旧 `terminal_id`），adapter 应把说明透传给用户/模型。
- `force=true` 覆盖：旧终端落 `interrupted{source:"terminate"}`，再创建新终端。
- 跨会话迁移（`terminal.rebind`）：把绑定键改到新的 `(root_session_id, name)`，保留 `terminal_id`、进程与 lease；目标键冲突且无 `force` → `session_exists`（带目标旧 ID），`force=true` → 先终止目标旧终端；终态终端拒绝。迁移后 clientless 计时清零，由**目标会话**的心跳重新保活。
- 只有**终端级方法**（`attach`/`detach`/`claim`/`release`/`status`/`read`/`send`/`resize`/`signal`/`terminate`）使用 `TerminalLocator`：`terminal_id` 优先，否则 `{root_session_id, name}`；`hello`/`terminal.list`/`terminal.prune`/`subscribe` 等非终端级方法不使用 locator。

## 3. 工具映射

工具只有 `termx_cli`，`args` 为一个完整命令行字符串；首 token 为子命令。所有终端级命令用 `-n <name>` 定位（默认 `default`），定位键为 `(root_session_id=当前 opencode sessionID, name)`。`help` 输出完整用法（en/zh 跟随 `toolLang`）。

| 子命令 | RPC | 关键参数 | 返回/行为 |
| --- | --- | --- | --- |
| `help` | — | — | 完整用法 |
| `local ["<cmd>"]` | `terminal.create` | `mode:"pty"`, `cmd`（缺省平台 shell）, `-c cwd`, `--env K=V`, `--cols/--rows`, `-n name`, `--force` | `terminal_id` + 初始 `cursor`；终端常驻 |
| `ssh <user@host[:port]>` | `terminal.create`（`ssh`） | `--password` / `--key`+`--passphrase`, `--any-host-key`, `--helper` 或 `--upload-helper --helper-local`, `--helper-sha`, `--cols/--rows`, `-n name`, `--force` | `terminal_id` + 初始 `cursor`；`cmd` 固定 `"shell"`。`--password`/`--passphrase` 会进入工具参数并可能被会话历史记录，优先用私钥/agent |
| `exec "<command>"` | `terminal.claim` → `terminal.status` → `terminal.send` | `-n name`, `-w`, `--timeout ms` | 默认提交后返回；`-w` 记录基线 cursor，轮询 `terminal.read` 直到约 1s 静默/超时/终态，返回新增 output（非精确命令边界） |
| `run "<cmd>"` | `terminal.create` → `subscribe` → 等终态 | `--pty`（缺省 stdio）, `--cwd`, `--env`, `--timeout ms`, `-n name`（缺省 `run-<ts>`）, `--force` | 返回合并 output 与 `[exit=N]`/`[signal=...]`；`<cmd>` 按词表拆分后直接执行 |
| `read` | `terminal.read` | `-n name`, `--cursor N`, `--max-bytes N` | 仅 output chunks；缺省用插件内每终端 cursor 增量，返回 `[cursor=N]` |
| `transcript` | `terminal.transcript` | `-n name`, `--json` | 命令记录（`started_at`/`ended_at`/`status`/清洗后 `output`）；默认文本，`--json` 返回 `{commands, revision}` |
| `send "<text>"` | `terminal.claim` → `terminal.send` | `-n name`, `--enter` | 写入文本；默认自动 claim（失败不阻断 send） |
| `resize` | `terminal.resize` | `-n name`, `--cols`, `--rows` | pty 生效；stdio 空操作 |
| `signal` | `terminal.signal` | `-n name`, `SIGINT|SIGTERM|SIGKILL` | 发送信号 |
| `status` | `terminal.status` | `-n name` | `snapshot` |
| `list` | `terminal.list` | `--limit N`, `--states s1,s2` | 终端摘要 |
| `attach` | `terminal.attach` | `-n name`, `--cursor N` | `cursor` + `snapshot` |
| `detach` | `terminal.detach` | `-n name`, `--all` | 只断客户端；`--all` 与 `-n` 互斥 |
| `claim` / `release` | `terminal.claim` / `release` | `-n name`, `--force`（仅 claim） | 输入 lease |
| `terminate` | `terminal.terminate` | `-n name`, `--all`, `--force` | 杀进程树、落终态；`--all` 只处理非终态 |
| `prune` | `terminal.prune` | `--all` 或 `--states` / `--older-than`（秒）, `--basis` | 删除终态，返回 `removed`；必须显式指定过滤 |
| `rebind` | `terminal.list` → `terminal.rebind` | `-n name`（先解析 terminal_id）, `--target-root`, `--new-name`, `--force` | 迁移绑定键；**必须先 `ctx.ask`（permission `termx_rebind`）由用户批准**，拒绝/异常返回“已取消迁移” |

## 4. 事件 → 模型
- **先 `events()` 再 `subscribe` / `attach`**：`termx-client` 把回放事件写入广播通道，没有接收者时会被直接丢弃；adapter 必须先注册接收者。
- 订阅 `subscribe` / `attach` 回放，按 `seq` 去重；收到广播 `Lagged` 时按最后已处理 `cursor` 重新 `attach` 补齐。
- `waiting_input{confidence,reason}`、`interactive_changed{active,...}`：**异步通知**模型，不阻塞等待命令结束。
- `output`：增量；`output_dropped`：表示缺口；状态/终态事件不可丢。
- `exited{exit_code}` / `interrupted{signal,source}` / `error{code}`：终态唯一。
- 断线重连带最后已处理 `cursor`；服务端可返回 `reset`（清本地视图后继续）。
- `rebound{old_root_session_id,new_root_session_id,old_name,new_name}`：绑定键已迁移；本地若缓存旧键应更新，`terminal_id` 不变。
- Web 默认「信息」Tab 展示 service 记录的命令（起止时间、`running`/`done`/`exited`/`interrupted`/`error` 状态、清洗后输出；无提示符/回显，轮询 `terminal.transcript`），「原始控制台」Tab 保留 xterm/ANSI；两视图共用同一终端与输入 lease。
- Web header 采用两级选择（`session.list` 会话 + `@web` 其他/未绑定，再按 `root_session_id` 过滤 `terminal.list`）；信息页顶部展示 `state`/`pid`/`mode`/启动命令/`cwd`。
- Web「新建终端」提交 `terminal.create`（`mode:"pty"`，local 或 ssh）；绑定「其他/未绑定」时 `root_session_id` 为 `@web`。成功刷新两级选择并选中新终端，失败在弹窗内提示；密码只随请求发送，不落页面/日志/localStorage。

## 5. 会话生命周期
- 插件为每个活跃 opencode 会话保持独立连接并每 30s 发 `heartbeat {root_session_id}`；`session.deleted` 时用该会话自己的连接发 `session_closed` 后关闭连接。
- opencode 会话关闭/重启只 `detach`；终端继续运行，但无存活会话心跳时按 `no_children`（默认 5 分钟）终止；`never`/`any` 可配，`@web` 未绑定终端豁免。之后按 `(root_session_id,name)` 或 `terminal_id` re-attach（跨会话需 `rebind` 或管理员连接）。
- 默认 `idle_timeout=0` 永不超时；配置正数时明确 `detach` 或 `terminate`。
- service 重启后连接关闭：客户端会看到事件通道 `Closed`、pending 请求返回 `Closed`；adapter 应将其视为 `service_lost` 并重建连接（v1 不发送合成的 `error{service_lost}` 事件）。

## 6. 安全与兼容
- 不记录/打印 token、原始输入输出、密钥。
- 旧全局 `term_cli` **不复用**；本插件只提供 `termx_cli` 单工具（子命令语义按本文件），避免工具重名。
- 不把文本完成标记当主判据；以进程状态/终态事件为准。
