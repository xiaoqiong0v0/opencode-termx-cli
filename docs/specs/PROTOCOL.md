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

### 2.1 Web 网关（W1）
- `termx-service` 在 `--web-port > 0` 时额外启动 HTTP + WebSocket 网关；`/ws` 由网关代理到内部环回 service（`ws://127.0.0.1:<service_port>`）。
- **浏览器不持有内部 token**：网关先向内部服务发送 `hello`（`client:"termx-web"`、`admin:true`，不带 `root_session_id`），再把内部返回的 `hello` 响应转发给浏览器；后续帧原样双向转发。Web 代理是管理连接，可查看/操作全部会话的终端。
- 默认只绑定 `127.0.0.1`，回环请求免登录；`--web-allow-remote true` 时绑定 `0.0.0.0`，非回环请求必须通过 `/login` 建立会话 Cookie（`termx_web`）。
- 远程访问必须配置密码（环境变量 `TERMX_WEB_PASSWORD`）；未配置时登录必失败。密码与内部 token 均不写入日志或命令行。
- `/ws` 升级前校验 Host/Origin，防浏览器跨站 WebSocket 劫持（CSWSH）：仅本机模式要求 Host 为回环主机（`127.0.0.1` / `localhost` / `[::1]`），带 Origin 时必须与 Host 同源；远程模式要求 Origin 存在且同源。校验失败返回 403，不升级。

## 3. 鉴权
- 服务启动生成随机 token，写入仅当前用户可读的 token 文件。
- token 文件位于服务启动时**显式指定**的配置目录（`--config-dir`，或非空 `TERMX_CONFIG_DIR`）下的 `token`；不推导平台默认目录。Unix 权限 `0600`。Windows v0 依赖**环回绑定 + token** 保证本机访问面，文件 ACL 收紧列为后续项（实现当前继承目录 ACL，不夸大已收紧）。
- 服务端 token 优先级：`--token-file` > 非空 `TERMX_TOKEN` > `<config-dir>/token`（缺失时生成）。
- 客户端必须显式提供配置目录或 endpoint（`--config-dir` / `--endpoint` / 非空 `TERMX_CONFIG_DIR`），不会回退到 `XDG_CONFIG_HOME`、`%APPDATA%` 等平台默认目录。客户端 token 优先级：`--token` > `TERMX_TOKEN` 环境变量 > `<config-dir>/token`。
- token 持久化于 token 文件，首次启动生成；如需轮换，删除 token 文件或使用显式轮换（后续），旧 token 随即失效；不写入日志。
- token 通过 `hello` 的 `token` 字段校验，也可用 `Authorization: Bearer <token>` 头；两者同时给出时**头优先**。
- token 文件采用同目录临时文件 + rename 原子写入，避免读到半截内容。
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
- **连接级错误帧**：服务端在无法归属到具体请求时发送 `{ "id": "", "ok": false, "error": {...} }`。客户端以 `id` 是否为空区分「连接级错误」与「请求响应」。当前用于订阅事件队列溢出：先发 `id:""`、`code:"limit_exceeded"`、`retryable:true` 的错误帧，随后关闭连接；客户端应视为连接级失败，重连后按 `cursor` 补齐。未来新增连接级错误同样复用该形状（`id` 留空），不引入独立 close reason。

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
| `hello` | `{ proto, token, client, root_session_id?, admin? }` | `{ proto, server, capabilities }`；`capabilities.runtime_catalog` 表示支持 runtime RPC |
| `heartbeat` | `{ root_session_id? }` | `{ ok }` |
| `session_closed` | `{ root_session_id }` | `{ ok }` |
| `session.list` | `{}` | `{ sessions: [{ root_session_id, live, last_seen_ms?, terminals }] }` |
| `runtime.list` | `{}` | `{ runtimes: [{ id, name, version, platform, arch, available }] }`；只含当前平台兼容项，不含本机路径 |
| `ssh.history.list` | `{}` | `{ entries: [{ host, port, user, auth_type, use_count, last_used }] }`；仅管理员，按 `last_used` 降序，不含密码/口令/私钥路径 |
| `ssh.history.remove` | `{ host, port, user }` | `{ removed }`；仅管理员，按 host+port+user 删除单条 |
| `ssh.history.clear` | `{}` | `{ removed }`；仅管理员，清空全部历史 |
| `terminal.create` | `{ mode: "pty"\|"stdio", cmd?, args?, cwd?, env?, cols?, rows?, root_session_id, name, force?, idle_timeout?: {value, unit:"s"\|"ms", action?: "detach"\|"terminate"}, ssh?: { host, port, user, auth, helper_path?, helper_local_path?, helper_sha256?, upload_helper?, host_key_policy?, trusted_host_key? }, runtime_id?, allow_injection? }` | `{ terminal_id, cursor }` |
| `terminal.claim` | `{ locator, force? }` | `{ owner, lease }` |
| `terminal.release` | `{ locator }` | `{ ok }` |
| `terminal.attach` | `{ locator, cursor? }` | `{ cursor, snapshot, reset? }` |
| `terminal.detach` | `{ locator }` | `{ ok }` |
| `terminal.list` | `{ states?, limit? }` | `[{ terminal_id, root_session_id, name, state, structured_events, mode, runtime_id?, command, args, cwd?, pid?, idle_timeout?, last_activity?, created_at }]`；`limit` 取最近活动的前 N 条 |
| `terminal.status` | `{ locator }` | `{ snapshot }`；`snapshot.structured_events` 表示是否有带外 shell 事件能力 |
| `terminal.read` | `{ locator, cursor, max_bytes? }` | `{ chunks, cursor, reset? }` |
| `terminal.output` | `{ locator, tail_lines? \| tail_chars? \| pattern?, nth?, only_matches? }` | `{ text }`；按命令输出文本做尾部截取或倒搜（结构化终端读 transcript，raw-only 先清洗），不返回行号 |
| `terminal.transcript` | `{ locator, limit? }` | `{ commands: [{command, started_at, ended_at?, status, output, exit_code?, source?, best_effort?}], revision }` |
| `terminal.send` | `{ locator, data(base64)?, text?, enter? }` | `{ ok }` |
| `terminal.resize` | `{ locator, cols, rows }` | `{ ok }` |
| `terminal.signal` | `{ locator, signal }` | `{ ok }` |
| `terminal.rebind` | `{ locator, root_session_id, name?, force? }` | `{ terminal_id, root_session_id, name }` |
| `terminal.terminate` | `{ locator, force? }` | `{ ok }` |
| `terminal.prune` | `{ states?, older_than?: {value, unit:"s"\|"ms"}, basis?: "last_activity"\|"created_at", all?: boolean }` | `{ removed }` |
| `subscribe` | `{ terminals?: [{terminal_id, cursor?}], all?: boolean }` | `{ cursors: [{terminal_id, cursor, reset?}] }` |
| `unsubscribe` | `{ terminals? }` | `{ ok }` |

约束：
- `pty` 必带 `cols/rows`（可默认）；`stdio` 忽略尺寸。
- `runtime_id?`：从服务端 `<config_dir>/runtimes/index.json` 解析内置 runtime。此时 `cmd`/`args` 必须缺省或为空；服务端用 manifest 的 `bin`/`args` 覆盖，不解回退到请求里的自定义命令。未知 ID → `not_found`；平台不兼容、路径越界、文件缺失/不可执行 → `bad_request`。无 `runtime_id` 时保持旧 `cmd`/`args` 行为。P1 只读本地目录，不下载/不远程传输。
- `ssh?`：存在时 `mode` 必须为 `pty`，由 SSH 后端建立远端会话；`cmd` 为空或 `"shell"` 时申请交互式 shell，否则把 `cmd + args` 按 POSIX 单引号规则拼成远端命令（helper 模式下直接作为 `spawn.cmd/args`，不经 shell 二次分词）。
  - `auth`：`{ type:"password", password }` 或 `{ type:"key", path, passphrase? }`；`path` 为空时由 service 在创建时依次探测本机 `~/.ssh/id_ed25519`、`~/.ssh/id_rsa`，显式路径优先，两者都不存在返回 `bad_request`。`Debug` 输出遮蔽密码/口令，凭据不落日志。
  - `host_key_policy`：`ask`（缺省）/ `strict` / `accept_any`。`ask` 下未知主机返回 `host_key_unknown` 与指纹，客户端确认后以同一 host key 的 `trusted_host_key` 重试；`strict` 只接受 known_hosts 已存在的匹配项；`accept_any` 仅高级/测试，不写 known_hosts。密钥变化返回 `host_key_changed`，必须硬失败，不允许重试绕过。known_hosts 位于 service 配置目录。
  - helper 启用判定：`helper_path` 非空，或 `upload_helper=true`。路径只允许绝对路径或 `~/...`（`~` 为远端用户 home），拒绝空白/控制字符/`..`/相对路径。
    - `upload_helper` 缺省 `false`：`helper_path` 指向**远端已存在**的 helper；连接前只读预检目录/文件/可执行/`helper_sha256`，有效直接使用，缺失或不符合时回退 fallback，**不写任何文件**。
    - `upload_helper=true`：必须提供 `helper_local_path`；`helper_path` 可选，表示**持久安装目标**（缺省由服务端注入计划补齐）。写前先做同样的只读预检，已存在且摘要/可执行检查通过则直接复用，不触发授权。
    - 需要写入时按 `remote.injection.consent` 决策：`deny` → 不写并回退；`ask` 且请求未带 `allow_injection=true` → 返回 `consent_required`（`error.injection` 列出 host/user、目标路径、大小、sha256、动作与原因），不 mkdir、不写；`allow` 或 `allow_injection=true` → 持久安装。
    - 持久安装：`mkdir -p -m 700` 父目录 → 同目录 `mktemp` → 上传 → `sha256sum` 校验 → `chmod 700` → 旧文件改名为 `.old.<ts>` → `mv` 原子替换；不删除已安装文件。
    - 非法组合（`upload_helper=true` 缺 `helper_local_path`、`helper_path` 非法、未上传却设 `helper_local_path`、`helper_sha256` 缺 `helper_path`）一律 `bad_request`。
  - helper 成功时 `started` 带远端真实 `pid`/`pgid`，`foreground_pgrp` 为根进程组，`tree` 由 helper 提供，`cwd`/`env` 生效。
  - helper 不可用（未配置、预检失败、注入不可用、上传/摘要/握手/版本/`spawn` 失败）：关闭 helper 通道并回退 fallback（`helper:false`、`bestEffort:true`、`version:"russh-fallback"`），不阻塞普通命令。`consent_required` 例外：必须原样返回给客户端，不 fallback。请求包含 `cwd`/`env` 时因 fallback 无法应用而返回 `bad_request`（SSH 层同样拒绝，不只依赖 core 工厂）。
  - fallback 远端 `pid`/`pgid` 为 `0`；`foreground_pgrp` 缺省、`tree` 为空；无 `exit-status`（仅 channel 关闭）时退出码为 `-1`（best-effort）。
  - 终端级能力经 `attach`/`status` 的 `snapshot.capabilities` 暴露（见 §7）；`hello.capabilities` 是服务级声明，不代表任一终端的 helper 状态。
- `ssh.history.*`：仅 `admin:true` 连接可用（普通连接返回 `permission_denied`）。历史只在 SSH `terminal.create` 成功后写入；失败/取消/`consent_required`/主机密钥未知或变化均不写。去重键 `host + port + user`，成功一次 `use_count += 1` 并刷新 `last_used`；超限按 `use_count` 最小淘汰，相同 `use_count` 淘汰 `last_used` 最旧。存储为 `<config_dir>/ssh_history.json`（Unix `0600`、原子写、跨进程锁、损坏按空处理），字段只含 `host/port/user/auth_type/use_count/last_used`，**不含**密码、私钥口令或私钥路径。
- `env?` 为 **JSON 对象 map**（如 `{"TERM":"xterm-256color"}`），不是元组/二元数组。
- `idle_timeout` 省略或 `value=0` 表示**永不超时**；`action` 仅在 `value>0` 时必填，`value=0` 可缺省。默认 `0`。该规则由 `IdleTimeout::validate` 校验，`terminal.create` 在创建终端前调用。
- `last_activity` 由 `terminal.create` 写入的 `started` 事件初始化；此后在 `output`、`terminal.send`、`terminal.attach`、`terminal.resize` 时刷新为服务端当前时间（RFC3339 UTC）；`idle_timeout` 的 `detach` 动作会在发送 `detached{reason:"idle_timeout"}` 后刷新它，避免同一空闲周期重复触发。
- `terminal.prune` 只清理终态终端，且**必须显式指定过滤**：`states` 非空、`older_than` 或 `all=true` 至少一个；三者都缺省（或 `states` 为空数组且未给 `older_than`）时返回 `bad_request`，**绝不隐式清理全部终态**。`all=true` 表示清理全部终态终端，并忽略其它过滤；`basis` 缺省为 `last_activity`（缺失时回退 `created_at`），`older_than` 缺省表示不做时间过滤；结果 `removed` 为被删除的 `terminal_id` 列表（数量 = 其长度）。
- `session.list`：返回已知 opencode 会话，合并会话心跳表中 TTL 内的存活项与 registry 中所有终端的 `root_session_id`（去重、按 `root_session_id` 升序）。`live` 仅表示 TTL 内收到过 `heartbeat {root_session_id}`；`last_seen_ms` 为距最近心跳的毫秒数，仅存活项提供；`terminals` 为该会话当前终端数。不含 token 等敏感信息。**仅 `admin:true` 连接可用**，普通连接返回 `permission_denied`。
- **连接级访问身份（会话隔离）**：`hello` 必须给出非空 `root_session_id` 或 `admin:true`，两者都没有 → `bad_request` 并断开；`admin:true` 时忽略 `root_session_id`。
  - **保留会话 `@web`**：`root_session_id == "@web"` 表示 Web 未绑定终端。普通连接不得以 `@web` 作为自身 `root_session_id`（`hello` → `bad_request`），也不可见/不可访问该会话下的终端；只有 `admin:true`（Web 代理）可见/可操作。终端可手动 `terminate`，或由管理员 `rebind` 到真实会话/从真实会话迁回 `@web`（视为解绑）。
  - 普通连接只能访问 `root_session_id` 与自身一致的终端；所有终端级方法在解析 locator 后统一鉴权，跨会话返回 `permission_denied`（不泄露目标是否存在）。
  - `terminal.create` 的 `root_session_id` 必须是自身会话（管理员可指定任意会话）；`terminal.list` / `terminal.prune` 只作用于自身会话。
  - `terminal.subscribe(all=true)` 仅管理员可用；普通连接只能显式订阅本会话终端。
  - `heartbeat` / `session_closed` 携带的 `root_session_id` 必须与连接会话一致（管理员不受限）。
  - `terminal.rebind` 采用「目标会话拉取」授权：普通连接只能把终端迁移到**自己的** `root_session_id`，管理员可迁移到任意会话；不要求调用方当前能访问源终端。
  - `admin` 只用于 Web 代理等管理连接，普通客户端不得自行声明。连接身份在 `hello` 时固定，后续帧不能改变。
- **会话键**为 `(root_session_id, name)`；`terminal_id` 为内部全局唯一标识。`name` 禁止 `/`、`\`、空串，且长度上限为 **64** 个字符（按 Unicode 字符计数）。
- 同名处理：同一 `(root_session_id, name)` 已存在时，`create` 不带 `force` → `session_exists`（错误载荷的 `terminal_id` 字段携带旧终端 ID，并附说明）；`force=true` → 终止旧终端（落 `interrupted{source:"terminate"}`）后新建。
- 终端定位：除 `terminal.create` 外，终端级方法的 `locator` 均为 `TerminalLocator`，形状为 `{ terminal_id?, root_session_id?, name? }`（JSON 中平铺，不嵌套 `locator` 键）。
- `terminal.rebind`：把绑定键从旧 `(root_session_id, name)` 迁移到新键，**保留** `terminal_id`、进程、lease、订阅与事件缓冲；迁移后重置该终端的 clientless 计时，由新会话心跳重新判定保活。授权见上文「连接级访问身份」：普通连接目标会话必须是自身，管理员不限。`name` 省略表示保留原名；`name` 不同即视为无冲突（可重命名）。目标键已存在时：无 `force` → `session_exists`（错误载荷 `terminal_id` 为目标旧终端 ID）；`force=true` → 先终止目标旧终端（`interrupted{source:"terminate"}`）再迁移，旧后端在锁外终止。终态终端 → `already_terminal`。
  - `rebind` 的 `root_session_id` 与 `name` 是**目标键**，因此其定位器只使用 `terminal_id`；旧键可由 `terminal_id` 唯一确定，不额外提供旧会话键字段。
  - 成功迁移（键确实变化）时追加一条 `rebound` 事件；新键与旧键相同时只重置 clientless 计时，不产生事件。
  - `terminal_id` 存在时优先；否则使用 `(root_session_id, name)` 查会话键。
  - 两者同时给出时以 `terminal_id` 为准，不回退到会话键。
  - 定位字段全部缺省 → `bad_request`；解析不到终端 → `not_found`。
- 连接标识：服务端为每条连接生成唯一 `client_id`（ULID），用于 `attached`/`detached`/`lease_revoked` 事件与输入 lease 归属；`hello.client` 仅作为日志标签，不参与任何键或归属判断。
- `terminal.create` 返回初始 `cursor`，固定为 `0`（`started` 的 seq=1 之前的位置）；客户端据此 `attach(cursor=0)` 可回放 `started` 与首屏 output，避免 create→attach 空窗丢事件。
- `terminal.send`：`data` 与 `text` 二选一（同传取 `data`）；`text` 按 UTF-8；`enter=true` 追加换行（`\r`，pty）/`\n`（stdio）。
- `terminal.claim` 取得输入 lease；同一终端同一时刻只有一个 lease owner。`force=true` 抢占时向订阅者广播 `lease_revoked{old_client_id}`，随后授予新 owner。未 claim 或非 owner 的 `send` 返回 `permission_denied`。
- `terminal.transcript`：读取 service 侧记录的信息页**命令记录**（**不是事件流**）。
  - 每条命令：`command`（仅 `terminal.send` 成功且 `enter=true`，去掉末尾 CR/LF）、`started_at`/`ended_at`（RFC3339 UTC）、`status`、`output`（已清洗）。`enter=false` 的按键/密码不记录。
  - `status`：`running` → `done`（新命令到来、静默达到阈值或收到 shell marker）/ `exited` / `interrupted` / `error`（终端终态）。
  - **二级执行不拆**：嵌套 shell、`bash -c`、`pwsh -Command`、子命令与 pipeline 都归属父命令；不为子 shell 生成独立记录，`exit_code` 取父命令/管道最终结果。
  - 输出清洗：去 ANSI 与控制字符；CR/CRLF 统一为 LF；首行包含命令文本时视为回显删除；删除末尾提示符行（cmd `...>`、PowerShell `PS ...>`、bash/zsh `user@host:...$/#`）；去首尾空行，保留其余原文。
  - 每终端有界（默认 **200** 条，`TERMX_TRANSCRIPT_CAPACITY` 覆盖），超出淘汰最旧命令。
  - 单条命令 `output` 最多保留 **64 KiB**（含截断标记）；超出后按 UTF-8 字符边界截断并只追加一次 `\n[output truncated]\n`，后续输出不再追加。
  - `limit` 缺省/0 返回全部保留记录，否则返回最近 N 条（仍按时间升序）；`revision` 在每次记录/追加/结束时递增，供页面判断是否重渲染。
  - 鉴权与其它终端级方法一致（按终端 `root_session_id`，`admin` 例外）。
- `terminal.read`：**只回放 `output` 事件**，不返回 `started`/状态/终态事件；状态与终态由 `attach`/`subscribe` 回放。返回的 `cursor` 为本次扫描到的最后 `seq`（会越过被跳过的状态事件），客户端以它继续增量读取。
  - `cursor` 早于缓冲下界（或所需输出已被背压淘汰）时返回 `reset` + 基线 `cursor`，客户端清空本地 output 视图后继续。
  - `max_bytes` 为本次累计 `data` **解码后**字节上限；`null`/缺省/`0` 取服务端默认。按 `seq` 升序累加，达到上限即在下一条前停止；**单条 output 解码后超过上限时仍返回该条并前进游标**，避免反复拿到空结果形成活锁。
  - `read` 是只读操作：不注册/修改订阅，也不推进任何消费状态，可重复调用。
- `terminal.output`：对**命令输出文本**做尾部截取或倒搜，不返回行号；行指命令输出行，不是终端显示行。文本来源随终端能力切换：
  - 结构化终端（`snapshot.structured_events=true`，带外 shell 事件）：只用 transcript 中按时间顺序拼接的清洗后命令输出（无 prompt/ANSI），命令块之间以换行分隔；没有 transcript 命令/输出时为空，**不退回 raw PTY**。
  - raw-only 终端：拼接当前保留的 `output` 事件，剥离 ANSI/OSC 控制序列并处理 `\r` 重绘、CRLF 与退格；没有 shell 事件可用，**无法可靠剔除 prompt**（已知限制）。
  - `tail_lines`：最后 N 行；历史不足时返回全部；末尾换行不额外算一行。
  - `tail_chars`：最后最多 N 个 Unicode 字符；先拼接字节再按 UTF-8 解码，避免 chunk 边界切断多字节字符。
  - `pattern` + `nth`：从末尾倒搜第 N 个字面量匹配（`nth=1` 为最后一个）；默认返回从该匹配到末尾，`only_matches=true` 只返回匹配所在行（不跨命令输出块）。无匹配返回空串。
  - 三组条件互斥；历史被背压裁剪后只基于当前保留文本计算，旧位置不保证可用。
- `attach` 不带 `cursor`：返回当前 `snapshot` + 当前基线 `cursor`（不补发历史 output，仅快照）。
- `attach` 到终态终端：允许只读（返回 `snapshot` 与剩余 output）。
- `subscribe` 的 `terminals` 与 `all` 同时给出时，结果按 `terminal_id` 去重；任一目标解析失败则整体失败，不注册任何订阅、不回放事件。
- `detach`/`unsubscribe` 某终端后，该终端从该客户端的 `subscribe_all` 全局投递中排除；再次 `attach`/`subscribe` 该终端时恢复。
- **终端附着语义**：单终端「有客户端附着」只统计该终端的终端级 `attach`/`subscribe` 订阅；`subscribe_all` 的全局订阅**不计入**任何单终端附着。连接关闭时服务端 `detach_all` 清理该连接的全部终端级订阅。附着只影响输入 lease 与事件投递，**不参与终端保活**。
- **会话保活（S1）**：「客户端」指 opencode 会话心跳（按 `root_session_id`），不是 Web 或连接。`heartbeat {root_session_id}` 每 30s 刷新服务端存活时间，TTL 默认 **90s**（`TERMX_SESSION_TTL_MS` 覆盖）；`session_closed {root_session_id}` 立即移除。终端的 `root_session_id` 在 TTL 内无心跳时开始计时，超过 `client_idle_timeout`（默认 **300000ms**，`0` 关闭）后按策略处理：
  - `no_children`（默认）：若终端仍有**用户子进程**（排除根进程与 conhost/winpty/OpenConsole 等控制台辅助进程）则继续保活，子进程退出后重新计时；否则终止。
  - `any`：到期即终止，不检查子进程。
  - `never`：永不因无会话终止。
   - 终止统一落 `interrupted{signal:"SIGTERM", source:"client_timeout"}`。
   - **`@web` 豁免**：`root_session_id == "@web"` 的终端不参与无会话超时扫描（不记录 `clientless_since`），只能由管理员手动 `terminate`。
   - Web 附着不发送会话心跳，因此不会让终端保活；旧的空参数 `heartbeat {}` 仅保留为连接级心跳，不影响终端保活。
- `heartbeat` 可携带 `root_session_id` 刷新会话存活时间；空参数形式兼容旧客户端，但不再影响终端保活。
- CLI 在 `local --follow` 与长 `exec` 期间每 30s 发送 `heartbeat {root_session_id}`（root 与 `hello` 一致）；发送失败只停止心跳，不中断命令与输出。

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
| `rebound` | `old_root_session_id, new_root_session_id, old_name, new_name` | 绑定键迁移完成；`terminal_id`/进程/lease 不变 |
| `capabilities` | `helper, version, bestEffort` | 某终端的 SSH/helper 能力与降级标识（终端级） |

- `capabilities` 的层级语义：
  - `hello.capabilities` 是**服务级**声明（当前 `helper:false`、`version` 为协议版本、`runtime_catalog:true`），只表示服务自身能力，不代表任何终端的 helper 状态；旧客户端缺省 `runtime_catalog:false`；
  - 终端级能力只通过 `attach`/`status` 的 `snapshot.capabilities` 或该终端的事件流暴露；客户端不得用 `hello.capabilities` 推断某个 SSH 终端是否启用 helper。
  - 带外 shell 事件通道（本地 named pipe / 远端 helper）是 service 内部判定，不新增 `Capabilities` 字段；该终端是否真正具备结构化事件由 `snapshot.structured_events` / `terminal.list[].structured_events` 暴露（旧服务端缺省 `false`）。Web 只在该字段为 `true` 时显示信息页，普通程序/shell 仅保留原始控制台。
- `snapshot.foreground_pgrp` 以后端上报为准：helper 模式为会话根进程组（当前实现不随前台作业切换），fallback 无值（字段缺省）。
- 所有事件共用每终端单调 `seq`；`output_dropped` 作为**新事件**分配自己的单调 `seq`（不复用被丢输出的 `seq`），使缺口可检测。
- `output_dropped.from_seq`/`to_seq` 为**闭区间**（两端都包含），即被丢弃的是 `from_seq..=to_seq`。
- 区间只包含**实际被丢弃的 `output`**：状态事件永不丢弃；若被淘汰的 output 之间夹有状态事件，服务端按连续区间拆成多条 `output_dropped`，每条区间互不重叠且按 `from_seq` 升序。
- `interactive_changed.active=true` 表示前台进入交互对象；`active=false` 表示退回 shell/结束交互。
  - 置信度：前台进程组从根进程组变为子进程组、子进程组之间切换或回到根进程组，均为 `medium`；只在变化时推送，不随采样周期重复。
- `waiting_input` 表示疑似等待输入，必须带 `confidence`（`low`/`medium`/`high`）与 `reason`（常见取值：`prompt`/`stdin_blocked`/`no_output`/`shell_integration`/`heuristic`）。
  - `shell_integration`（OSC 133/633 prompt 开始/结束或命令结束）固定为 `high`；命令开始（`C`）会清除等待候选。
  - `no_output` 由静默启发式产生：前台存在子进程时为 `medium`，仅 shell 静默时为 `low`；
  - 同一 `(confidence, reason)` 默认按节流间隔重复投递；`waiting_input` **不是终态**，也不会清除历史提示，命令开始执行后不再产生新的等待事件。
  - `stdin_blocked` 需要真实阻塞态证据（`wchan`/fd 等），当前版本不伪造该 reason，保留给具备阻塞态采集的平台实现。
- `interrupted.signal` 为信号名字符串（如 `SIGINT`/`SIGTERM`）；无法识别的信号名映射为 `"UNKNOWN"`，不猜测为 `SIGTERM`。`source` 常见取值 `control-c`/`signal`/`timeout`/`terminate`/`client_timeout`。
- `reason` 与 `source` 按字符串处理：未知取值**原样透传**（前向兼容），客户端不得因未识别值拒绝事件。
- 快照 `snapshot` 结构：
  ```jsonc
  { "terminal_id": "...", "state": "running", "mode": "pty",
    "runtime_id": "termx-pwsh",
    "command": "/bin/bash", "args": ["-l"], "cwd": "/home/me",
    "pid": 123, "pgid": 123, "ppid": 1, "foreground_pgrp": 123,
    "tree": [ /* {pid,ppid,pgid,cmd} */ ],
    "interactive": "none", "confidence": "low", "cols": 120, "rows": 30, "cursor": 42 }
  ```
  - `command` / `args` / `cwd` 为 `terminal.create` 的原值（SSH 也保留原始 `cmd`/`args`），供 Web 信息页展示；`cwd` 未指定时缺省。`terminal.list` 的条目同样包含这三个字段。
  - 使用 `runtime_id` 时，持久化的 `command`/`args` 是解析后的 manifest `bin`/`args`；原始 `runtime_id` 单独保留（旧终端缺省）。
- `terminal.read.chunks`：`[{ seq, ts, data(base64) }]`，按 `seq` 升序。
- `terminal.transcript.commands`：
  ```jsonc
  [{ "command": "ls", "started_at": "2026-01-01T00:00:00Z",
     "ended_at": "2026-01-01T00:00:01Z", "status": "done", "output": "a.txt\nb.txt",
     "exit_code": 0, "source": "shell_integration", "best_effort": false }]
  ```
  `ended_at` 在 `running` 时为 `null`；`output` 已由 service 清洗。
  - `exit_code`：可选退出码；无法确定时缺省（不得伪造）。旧客户端可忽略。
  - `source`：结束证据来源，`shell_integration` / `process_group` / `unknown`；未结束时缺省。
  - `best_effort`：结束判定是否为 best-effort（如静默推断）；缺省 `false`。
  - 三个字段均向后兼容：旧 JSON 缺失时分别默认为 `null`/`null`/`false`，且默认值不参与序列化。
  - **P1 本地单行 marker（当前默认关闭）**：命令内 marker 会被 shell 回显并写入历史，污染 raw/历史记录；`TRANSCRIPT_MARKER_ENABLED=false` 时完全不注入，精确 `exit_code` 暂不产出，等待 hook-based integration。开启时才在本地 `pty` 终端满足 `enter=true`、`text` 存在且 `data` 缺省时，服务端按 shell 追加不可见 OSC 序列（不改变 transcript 记录的原始 `command`）：
    - bash/zsh/sh：`<cmd>; __termx_ec=$?; printf '\033]133;D;%s\007' "$__termx_ec"`；
    - pwsh/powershell：先取 `$?`，再回落 `$LASTEXITCODE`，最后回落到 `1`，用 `[Console]::Write` 输出同一 `133;D` marker。
    - transcript 增量扫描输出中的 `133;D`/`633;D`，收到后以 `source=shell_integration` 结束当前命令并写入真实 `exit_code`；marker 文本在 `output` 清洗时去掉。静默结束不覆盖已写入的退出码；已静默结束的命令可被随后到达的真实 marker 覆盖。
    - **显式降级**（不追加 marker、退回静默判定、不伪造 `done`）：`enter=false`、`data`（base64）、`stdio`、SSH 终端（无论启动程序）、`cmd.exe`/未知 shell、多行文本、末尾为 `\`/`&`/`|`/`;`、含未闭合引号（简单检测）。
    - 单行与引号判定是保守启发式（非完整 shell 解析）；无法确定时宁可跳过。

## 8. 序号、游标与重连（责任划分）
- **服务端**维护每终端事件缓冲与单调 `seq`（从 1 开始），并负责按游标回放。
- **客户端**只需在本地记录「最后已处理 seq」作为 `cursor`，重连时上报；不依赖客户端重建状态。
- `attach`/`subscribe` 带 `cursor`：回放**全部**事件（`output` + 状态/终态），并按缓冲返回新 `cursor`。
- `read` 带 `cursor`：**只回放 `output`**；状态/终态不通过 `read` 返回。返回的 `cursor` 是本次扫描到的最后 `seq`（可能跳过区间内的状态事件），下一次从该 `cursor` 继续。
- 不可回放（超出缓冲）：`read`/`attach`/`subscribe` 返回 `reset:{reason}` + 基线 `cursor`；客户端清空本地视图后继续。
- 历史回放建议：先用 `terminal.read` 按 `cursor` 分段取历史，再用 `subscribe` 从 read 返回的游标接实时。`attach`/`subscribe` 的待回放事件超过订阅者队列余量时返回 `reset:"buffer_overflow"` + 基线 `cursor`，**跳过历史回放但保持订阅与连接**，客户端应改用 `read` 补齐。
- **空基线合法**：无历史 ≠ 无会话；禁止用「data 为空」推断会话不存在。
- 终态事件（`exited`/`interrupted`/`error`）对同一在线订阅者只推送一次；新 `attach`/`subscribe` 仍可按 `cursor` 回放该事件（`read` 不返回）。
- `terminal.transcript` 不使用事件 `seq`/游标：`revision` 只增不减，页面可整表重渲染；命令条数由服务端容量淘汰。

## 9. 背压与流控
- 终端有界输出缓冲；对端慢消费时丢弃**最旧 output**，并发出 `output_dropped{from_seq,to_seq}`（marker 使用新的单调 `seq`，闭区间只含实际被丢 output）；`started/interactive*/waiting_input/attached/detached/lease_revoked/exited/interrupted/error/capabilities` 永不丢弃。
- 每客户端独立发送队列与上限；**任何**事件投递时队满，服务端立即移除该订阅者、发送连接级 `error{code:"limit_exceeded",retryable:true}`（`id:""`）后断开；`seq` 不回退，也不在订阅者侧补发合成 marker。
- 读写解耦：PTY 与 stdio 的写入都不被慢读者阻塞。stdio stdin 由有界写队列 + 专用 writer 线程承载，`write` 只做非阻塞入队；队满时 `terminal.send` 返回 `limit_exceeded`。

## 10. 错误码
| code | 场景 | retryable |
| --- | --- | --- |
| `auth_failed` | token 错误 | false |
| `version_mismatch` | 协议/helper 版本不匹配 | false |
| `bad_request` | 参数/帧非法 | false |
| `not_found` | 终端不存在/已清理 | false |
| `already_terminal` | 对终态终端做写操作 | false |
| `session_exists` | 同名 `(root_session_id, name)` 已存在且未带 `force`（`create` 或 `rebind`） | false |
| `permission_denied` | 非 lease owner / 权限不足 | false |
| `limit_exceeded` | 超过会话/缓冲/队列上限 | true |
| `spawn_failed` | 进程创建失败 | false |
| `pty_failed` | PTY/ConPTY 创建失败 | false |
| `ssh_connect_failed` | SSH 连接失败 | true |
| `host_key_unknown` | 未知主机密钥；`error.host_key` 带算法/指纹 | false（确认后重试） |
| `host_key_changed` | 主机密钥与 known_hosts 不一致，疑似中间人 | false |
| `helper_failed` | helper 握手/启动失败 | true |
| `protocol_error` | 帧/状态机违例 | false |
| `consent_required` | 远端注入需要用户授权；`error.injection` 描述将写入的文件 | false |
| `service_lost` | 服务崩溃/重启（客户端合成） | true（重建） |
| `internal` | 未分类 | true |

**注入错误映射**：`termx-ssh` 的 `ConsentRequired` 经 core 原样映射为 `consent_required`，`error.injection` 填 `host`/`user`/`writes`（路径、大小、sha256、动作、原因）；不得降级为 `bad_request` 或 fallback。`InjectionFailed`（路径展开失败、预检失败、deny、目标为目录/不可写、mkdir/mktemp/上传/sha/chmod/mv 失败）直接导致 `terminal.create` 失败，经 core 映射为 `bad_request`（消息含目标路径与原因，不含凭据），**不回退 fallback**。只有 helper 协议/版本/握手等普通错误才允许 fallback。服务端只校验本次 `terminal.create.allow_injection`，不写会话缓存、不维护主机信任表，下次创建重新判定。

## 11. 版本协商
- 主协议 `proto: "termx/1"`；helper 协议 `termx-helper/1`。
- 主协议不匹配 → `version_mismatch` 断开。
- helper 本地 stdio（M6a）：
  - 传输：stdin/stdout 各一行一个 UTF-8 JSON 对象（JSON Lines）；**stdout 只承载协议帧**，日志一律 stderr。
  - 首帧必须是 `hello` 且 `proto` 匹配；否则回 `error` 后退出（`bad_request` / `version_mismatch`）。
  - 请求以 `method` 为标签，响应/事件以 `type` 为标签；字段 snake_case。
  - `input.data` / `output.data` 为 base64；`output.stream` 复用主协议 `Stream`（helper 固定 `pty`）；`error.code` 复用主协议错误码表，避免两套词表漂移。
  - `input` / `resize` / `signal` / `terminate` 成功**不回 ack**；失败才回 `error`。终态由 `exited` / `signaled` 唯一表达。
  - 同一时刻只允许一个 PTY 会话；活动会话未结束时再次 `spawn` → `bad_request`。`terminate` 返回后会话即视为结束，可直接 `spawn` 新会话（重复 `terminate` 幂等）。
  - stdin EOF / helper 退出：终止整棵进程树并释放 PTY，不留孤儿。
  ```jsonc
  // client -> helper（首帧）
  { "method": "hello", "proto": "termx-helper/1", "version": "0.1.0" }
  // helper -> client
  { "type": "hello_ok", "proto": "termx-helper/1", "capabilities": ["pty","proc_tree","exit_status","shell_events"] }

  // 请求
  { "method": "spawn", "cmd": "bash", "args": ["-l"], "cwd": "/tmp", "env": { "TERM": "xterm-256color" }, "cols": 120, "rows": 30 }
  { "method": "input", "data": "bHMNCg==" }
  { "method": "resize", "cols": 120, "rows": 30 }
  { "method": "signal", "signal": "SIGINT" }
  { "method": "terminate" }
  { "method": "tree" }

  // 响应/事件
  { "type": "started", "pid": 123, "pgid": 123 }
  { "type": "output", "data": "aGVsbG8=", "stream": "pty" }
  { "type": "exited", "code": 0 }
  { "type": "signaled", "signal": "SIGTERM" }
  { "type": "shell_event", "event": "D", "code": 7, "id": "cmd-1" }
  { "type": "tree", "processes": [{ "pid": 123, "ppid": 100, "pgid": 123, "cmd": "sh" }] }
  { "type": "error", "code": "spawn_failed", "message": "启动失败" }
  ```
- `tree` 为同步请求/响应：helper 按请求顺序返回一条 `tree` 事件；无活动会话时 `processes` 为空。客户端按发送顺序配对（同一连接内串行 tree 请求）。
- helper 随 SSH 通道分发、sha256 校验与失败降级属 M6b-2；M6a 只定义并实现本地 stdio 协议。
- M6b-1 已实现 **无 helper 的 SSH fallback**（russh 客户端）：`terminal.create` 带 `ssh` 时直接 `exec` 或 `request_pty + request_shell`，能力固定为 `capabilities{helper:false, bestEffort:true, version:"russh-fallback"}`。
- M6b-2 已实现 **helper 分发/握手/降级**：
  - 上传模式（`helper_local_path` + `upload_helper=true`）：远端 `mktemp -d /tmp/termx-helper.XXXXXXXXXX` 创建唯一私有目录（700），把本地二进制写入目录内固定文件名 `termx-helper`（`set -C` 拒绝复用已存在路径，`umask 077`），随后 `chmod 700` 并用 `sha256sum` 校验；
  - 非上传模式：只使用 `helper_path` 指向的远端已存在文件（可选 `sha256sum` 校验），不写远端；
  - 校验通过后 `exec <helper> serve`，握手 `hello`/`hello_ok`（`proto` 必须为 `termx-helper/1`），再 `spawn` 取得 `started.pid/pgid`；
  - 成功时能力为 `{helper:true, version:"termx-helper/1", bestEffort:false}`，`pid`/`pgid`/`tree` 由 helper 提供，`foreground_pgrp` 为根进程组，`cwd`/`env` 生效；
  - 上传失败、摘要不符、握手失败、版本不符或 `spawn` 失败：关闭 helper 通道、清理本次临时目录并回退 fallback（`helper:false`、`bestEffort:true`），不阻塞普通命令；请求包含 `cwd`/`env` 时因 fallback 无法应用而返回明确错误。
- **helper 带外 shell 事件（能力协商，不升协议版本）**：
  - `hello_ok.capabilities` 含 `shell_events` 表示 helper 支持事件通道；旧 helper 无该能力时客户端不下发事件请求，行为与之前一致。
  - `spawn.events`（缺省 `false`）仅在客户端需要且 helper 声明能力时置 `true`；`started.events`（缺省 `false`）回报本次是否实际建立通道，创建失败只降级、不阻断 spawn。
  - Unix 远端：helper 为每个会话创建私有临时目录（`0700`）与 Unix domain socket（`0600`），生成随机 token，并通过 `TERMX_EVENT_ENDPOINT=unix:<path>` / `TERMX_EVENT_TOKEN` 注入子进程环境；只 accept 一次、首行校验 token、单帧上限，会话结束后清理 socket。不在 stdout 输出事件，也不新增 TCP 端口。
  - 子进程按 JSON Lines 发送 `A/B/C/D`；helper 转成 `shell_event{event, code?, id?}` 协议帧。Windows 远端 named pipe 暂不启用（仅保留编译分支）。
  - `termx-ssh` 把 `shell_event` 映射为 `ProcEvent::ShellEvent`；service 复用本地通道的 command-id + transcript 代次判定，`D` 命中时以 `source: "shell_integration"` 写入真实 `exit_code`，不匹配/迟到 `D` 被忽略。
  - 关闭 in-band OSC 的判定是严格三级门槛：`ssh + pty + basename(cmd) == "termx-pwsh"`（后端快照 `shell_events()`）、helper 声明 `shell_events`、`started.events=true`；任一不满足（含旧 helper、普通 SSH、fallback）都保持 in-band 解析，不伪造退出码。
  - 公开 WebSocket `Capabilities` 不新增字段；事件通道状态只影响 service 内部 transcript 判定。
- 新增字段向后兼容；破坏性变更升主版本。

## 12. 安全
- 不记录原始输入/输出与密钥；事件日志只留元数据与 seq。
- Web 登录密码只从 `TERMX_WEB_PASSWORD` 读取，常量时间比较；会话只存内存，不落盘。
- 密码/口令类输入默认不回显存储；service 不落盘原始输入输出。
- base64 仅用于传输，不做加密；传输安全性依赖本机 socket/pipe 权限 + token。
- helper 分发时必须 sha256 校验，且只写本次连接创建的私有临时目录：
  - 目录由远端 `mktemp -d /tmp/termx-helper.XXXXXXXXXX` 创建并 `chmod 700`，目录内固定文件名 `termx-helper`；写入使用 `set -C`（noclobber）+ `umask 077`，**拒绝复用/覆盖已存在路径**；随后 `chmod 700` + `sha256sum` 校验。
  - 清理只对本次创建且格式校验通过的目录执行 `rm -rf --`；任一步失败、上传成功但握手前超时/取消、会话终态或连接断开都会清理。清理由上传阶段的守卫兜底（取消时把连接移入后台清理任务），不依赖调用方补救。
  - **绝不覆盖或删除用户配置的 `helper_path`**；非上传模式不写远端，`helper_path` 仅在不上传（远端已存在）模式下使用。
  - 不在命令行/日志暴露凭据；上传/校验/清理命令只拼接路径与十六进制摘要（M6a 本地 stdio 不涉及分发）。
- SSH 密码/私钥口令只经 `terminal.create` 参数传输并仅存于进程内存；`SshConfig`/`SshAuth` 的 `Debug` 输出遮蔽凭据，不写文件、不进日志，也不使用系统 `ssh`/`scp`。上传/校验命令只拼接路径与十六进制摘要，不含凭据。

## 13. 终态后操作契约（固定，避免歧义）
| 操作 | 未知 terminal_id/键 | 已知且已终态 |
| --- | --- | --- |
| `attach` / `status` / `read` / `list` | `not_found` | 允许只读，返回快照/剩余 output |
| `send` / `resize` / `signal` / `claim` / `release` | `not_found` | `already_terminal` |
| `rebind` | `not_found` | `already_terminal` |
| `terminate` | `not_found` | `ok:true`（幂等） |
| `prune` | 不影响 | 可清理 |
