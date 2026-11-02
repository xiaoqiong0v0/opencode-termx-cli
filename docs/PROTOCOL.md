# 协议规格：termx protocol v1（草案，待评审）

本文件是**术语、状态、事件、RPC 的唯一权威**。`REQUIREMENTS.md` 与 `ARCHITECTURE.md` 中出现的状态/事件名以本文为准。

## 1. 范围
- 客户端（opencode adapter / Web / CLI）与 `termx-service` 之间的本机通信。
- `termx-service` 与远端 `termx-helper` 的握手与降级（简化版，复用同一事件语义）。
- v0 **仅本机**：WebSocket 绑定环回地址（`127.0.0.1` / `::1`），不监听外部地址、不做跨机访问（见 `REQUIREMENTS.md` OQ-2）。

## 2. 传输
- v0 统一使用**本机 WebSocket**：`ws://127.0.0.1:<port>`（IPv6 为 `ws://[::1]:<port>`）。
- 仅绑定环回地址，拒绝非环回连接；端口默认值由配置提供，避免硬编码。
- 帧：每个 WebSocket **text frame 承载一个完整 JSON 对象**（纯 JSON 文本；若前置含换行可容忍）。二进制载荷用 base64。
- 单帧上限可配置；超限视为 `bad_request`。
- WebSocket 便于 Web 客户端与 CLI/adapter 复用同一协议；不提供 Unix socket / named pipe（后续如需可再评估）。

## 3. 鉴权
- 服务启动生成随机 token，写入仅当前用户可读的 token 文件。
- token 文件约定路径：Linux/macOS `$XDG_CONFIG_HOME/termx/token`（缺省 `~/.config/termx/token`）；Windows `%APPDATA%\termx\token`；权限 `0600` / 当前用户 ACL。
- 发现优先级：`--token-file` > `TERMX_TOKEN` 环境变量 > 默认 token 文件。
- token 在 service 重启时轮换，旧 token 失效；不写入日志。
- token 通过 `hello` 的 `token` 字段校验（也可用 `Authorization: Bearer <token>` 头）。
- 仅接受环回地址连接；非环回在 WebSocket 握手阶段拒绝。
- 握手失败返回 `auth_failed` 并断开；不提供匿名访问。

## 4. 消息框架
三类消息：request / response / event。

```jsonc
// request
{ "id": "c-001", "method": "terminal.create", "params": { /* ... */ } }

// response ok
{ "id": "c-001", "ok": true, "result": { /* ... */ } }

// response err
{ "id": "c-001", "ok": false, "error": { "code": "not_found", "message": "...", "retryable": false } }

// event
{ "type": "output", "terminal_id": "01J...", "seq": 42, "ts": "2026-11-02T09:00:00.000Z", "data": "..." }
```

- `id` 由客户端生成，同一连接内唯一，用于配对请求/响应。
- 事件无 `id`；以 `terminal_id` + `seq` 定位。
- 时间戳 `ts` 为 RFC3339 UTC。
- 每个 WebSocket text frame 一个 JSON；不使用长连接分帧拼接。

## 5. 生命周期与状态
### 5.1 三个正交维度
| 维度 | 取值 | 说明 |
| --- | --- | --- |
| 生命周期 | `created` → `running` → `exiting` → `exited` / `interrupted` / `error` | 终态唯一，之后拒绝输入 |
| 交互 | `none` / `interactive` / `waiting_input` | 运行期子状态，由事件驱动，非生命周期 |
| 附着 | `attached` / `detached` | **每客户端**维度，不影响终端运行 |

### 5.2 终态
- 终态集合：`exited`（自然退出，带 `exit_code`）、`interrupted`（信号/控制中断，带 `signal`）、`error`（失败，带 `code`）。
- 终态事件只发一次；后续请求按 §13 固定契约返回，不再发终态。

## 6. RPC 方法
| 方法 | 参数 | 结果 |
| --- | --- | --- |
| `hello` | `{ proto, token, client }` | `{ proto, server, capabilities }` |
| `terminal.create` | `{ mode: "pty"\|"stdio", cmd, args?, cwd?, env?, cols?, rows?, root_session_id, name, force?, idle_timeout?: {value, unit:"s"\|"ms", action?: "detach"\|"terminate"} }` | `{ terminal_id, cursor }` |
| `terminal.claim` | `{ terminal_id, force? }` | `{ owner, lease }` |
| `terminal.release` | `{ terminal_id }` | `{ ok }` |
| `terminal.attach` | `{ terminal_id, cursor? }` | `{ cursor, snapshot, reset? }` |
| `terminal.detach` | `{ terminal_id }` | `{ ok }` |
| `terminal.list` | `{ states?, limit? }` | `[{ terminal_id, root_session_id, name, state, mode, pid?, idle_timeout?, last_activity?, created_at }]` |
| `terminal.status` | `{ terminal_id }` | `{ snapshot }` |
| `terminal.read` | `{ terminal_id, cursor, max_bytes? }` | `{ chunks, cursor, reset? }` |
| `terminal.send` | `{ terminal_id, data(base64)?, text?, enter? }` | `{ ok }` |
| `terminal.resize` | `{ terminal_id, cols, rows }` | `{ ok }` |
| `terminal.signal` | `{ terminal_id, signal }` | `{ ok }` |
| `terminal.terminate` | `{ terminal_id, force? }` | `{ ok }` |
| `terminal.prune` | `{ states?, older_than?: {value, unit:"s"\|"ms"}, basis?: "last_activity"\|"created_at" }` | `{ removed }` |
| `subscribe` | `{ terminals?: [{terminal_id, cursor?}], all?: boolean }` | `{ cursors: [{terminal_id, cursor, reset?}] }` |
| `unsubscribe` | `{ terminals? }` | `{ ok }` |

约束：
- `pty` 必带 `cols/rows`（可默认）；`stdio` 忽略尺寸。
- `env?` 为 **JSON 对象 map**（如 `{"TERM":"xterm-256color"}`），不是元组/二元数组。
- `idle_timeout` 省略或 `value=0` 表示**永不超时**；`action` 仅在 `value>0` 时必填，`value=0` 可缺省。默认 `0`。该规则由 `IdleTimeout::validate` 校验，`terminal.create`（M1）调用。
- **会话键**为 `(root_session_id, name)`；`terminal_id` 为内部全局唯一标识。`name` 禁止 `/`、`\`、空串。
- 同名处理：同一 `(root_session_id, name)` 已存在时，`create` 不带 `force` → `session_exists`（错误载荷的 `terminal_id` 字段携带旧终端 ID，并附说明）；`force=true` → 终止旧终端（落 `interrupted{source:"terminate"}`）后新建。
- 分阶段定位：v0/M0 除 `create` 外的方法参数以 `terminal_id` 为准；M1 起这些方法参数引入 `TerminalLocator`（支持 `{root_session_id, name}`；两者都给时以 `terminal_id` 为准），届时同步更新表内参数。
- `terminal.create` 返回初始 `cursor`（创建时的事件序列位置）；客户端据此 `attach` 可回放 `started` 与首屏 output，避免 create→attach 空窗丢事件。
- `terminal.send`：`data` 与 `text` 二选一（同传取 `data`）；`text` 按 UTF-8；`enter=true` 追加换行（`\r`，pty）/`\n`（stdio）。
- `terminal.claim` 取得输入 lease；同一终端同一时刻只有一个 lease owner。`force=true` 抢占（旧 owner 收 `permission_denied`/`lease_revoked`）。未 claim 或非 owner 的 `send` 返回 `permission_denied`。
- `terminal.read`：`cursor` 早于缓冲下界时返回 `reset` + 基线 `cursor`，客户端清空本地 output 视图后继续。
- `attach` 不带 `cursor`：返回当前 `snapshot` + 当前基线 `cursor`（不补发历史 output，仅快照）。
- `attach` 到终态终端：允许只读（返回 `snapshot` 与剩余 output）。

## 7. 事件
| `type` | 关键字段 | 说明 |
| --- | --- | --- |
| `started` | `pid, pgid, mode` | 进程已创建 |
| `output` | `data(base64), stream` | 输出增量；`stream`=`pty`/`stdout`/`stderr` |
| `output_dropped` | `from_seq, to_seq` | 背压丢弃的最旧 output 区间；状态事件不丢 |
| `waiting_input` | `confidence, reason` | 疑似等待输入；非终态 |
| `interactive_changed` | `active, pid, pgid, command, confidence` | 交互进入/切换/退出 |
| `attached` | `client_id` | 某客户端附着 |
| `detached` | `client_id, reason` | 某客户端断开（含 idle_timeout） |
| `lease_revoked` | `client_id` | 输入 lease 被抢占/释放 |
| `exited` | `exit_code` | **终态** |
| `interrupted` | `signal, source` | **终态** |
| `error` | `code, message, retryable` | **终态**（含 `service_lost`） |
| `capabilities` | `helper, version, bestEffort` | SSH/helper 能力与降级标识 |

- 所有事件共用每终端单调 `seq`；`output_dropped` 也占用 `seq`，使缺口可检测。
- `output_dropped.from_seq`/`to_seq` 为**闭区间**（两端都包含），即被丢弃的是 `from_seq..=to_seq`。
- `interactive_changed.active=true` 表示前台进入交互对象；`active=false` 表示退回 shell/结束交互。
- `waiting_input` 表示疑似等待输入，必须带 `confidence`（`low`/`medium`/`high`）与 `reason`（常见取值：`prompt`/`stdin_blocked`/`no_output`/`shell_integration`/`heuristic`）。
- `interrupted.signal` 为信号名字符串（如 `SIGINT`/`SIGTERM`）；`source` 常见取值 `control-c`/`signal`/`timeout`/`terminate`。
- `reason` 与 `source` 按字符串处理：未知取值**原样透传**（前向兼容），客户端不得因未识别值拒绝事件。
- 快照 `snapshot` 结构：
  ```jsonc
  { "terminal_id": "...", "state": "running", "mode": "pty", "pid": 123, "pgid": 123,
    "ppid": 1, "foreground_pgrp": 123, "tree": [ /* {pid,ppid,pgid,cmd} */ ],
    "interactive": "none", "confidence": "low", "cols": 120, "rows": 30, "cursor": 42 }
  ```
- `terminal.read.chunks`：`[{ seq, ts, data(base64) }]`，按 `seq` 升序。

## 8. 序号、游标与重连（责任划分）
- **服务端**维护每终端事件缓冲与单调 `seq`（从 1 开始），并负责按游标回放。
- **客户端**只需在本地记录「最后已处理 seq」作为 `cursor`，重连时上报；不依赖客户端重建状态。
- `attach`/`subscribe`/`read` 带 `cursor`：
  - 可回放：服务端按缓冲补发并返回新 `cursor`。
  - 不可回放（超出缓冲）：返回 `reset:{reason}` + 基线 `cursor`；客户端清空本地视图后继续。
- **空基线合法**：无历史 ≠ 无会话；禁止用「data 为空」推断会话不存在。
- 终态后 `exited/interrupted/error` 可通过 `read` 回放，但不再重复推送。

## 9. 背压与流控
- 终端有界输出缓冲；对端慢消费时丢弃**最旧 output**，并发出 `output_dropped{from_seq,to_seq}`；`started/interactive*/waiting_input/attached/detached/lease_revoked/exited/interrupted/error/capabilities` 永不丢弃。
- 每客户端独立发送队列与上限；超限断连并标 `error`（`limit_exceeded`）或 `detached` + 原因。
- 读写解耦：PTY 写入不被慢读者阻塞；写入队列有上限，超限拒绝 `send`（`limit_exceeded`）。

## 10. 错误码
| code | 场景 | retryable |
| --- | --- | --- |
| `auth_failed` | token 错误 | false |
| `version_mismatch` | 协议/helper 版本不匹配 | false |
| `bad_request` | 参数/帧非法 | false |
| `not_found` | 终端不存在/已清理 | false |
| `already_terminal` | 对终态终端做写操作 | false |
| `session_exists` | 同名 `(root_session_id, name)` 已存在且未带 `force` | false |
| `permission_denied` | 非 lease owner / 权限不足 | false |
| `limit_exceeded` | 超过会话/缓冲/队列上限 | true |
| `spawn_failed` | 进程创建失败 | false |
| `pty_failed` | PTY/ConPTY 创建失败 | false |
| `ssh_connect_failed` | SSH 连接失败 | true |
| `helper_failed` | helper 握手/启动失败 | true |
| `protocol_error` | 帧/状态机违例 | false |
| `service_lost` | 服务崩溃/重启（客户端合成） | true（重建） |
| `internal` | 未分类 | true |

## 11. 版本协商
- 主协议 `proto: "termx/1"`；helper 协议 `termx-helper/1`。
- 主协议不匹配 → `version_mismatch` 断开。
- helper 握手（最小集）：
  ```jsonc
  // client -> helper
  { "method": "helper.hello", "params": { "proto": "termx-helper/1", "sha256": "…", "version": "…" } }
  // helper -> client
  { "method": "helper.hello_ok", "result": { "proto": "termx-helper/1", "capabilities": ["pty","proc_tree","exit_status"] } }
  ```
  不匹配 / sha256 不符 / 启动失败 / 超时 → 降级 `capabilities{helper:false, bestEffort:true}`，主流程继续。
- 新增字段向后兼容；破坏性变更升主版本。

## 12. 安全
- 不记录原始输入/输出与密钥；事件日志只留元数据与 seq。
- 密码/口令类输入默认不回显存储；service 不落盘原始输入输出。
- base64 仅用于传输，不做加密；传输安全性依赖本机 socket/pipe 权限 + token。
- helper 传输需 sha256 校验；不在命令行/日志暴露凭据。

## 13. 终态后操作契约（固定，避免歧义）
| 操作 | 未知 terminal_id/键 | 已知且已终态 |
| --- | --- | --- |
| `attach` / `status` / `read` / `list` | `not_found` | 允许只读，返回快照/剩余 output |
| `send` / `resize` / `signal` / `claim` / `release` | `not_found` | `already_terminal` |
| `terminate` | `not_found` | `ok:true`（幂等） |
| `prune` | 不影响 | 可清理 |
