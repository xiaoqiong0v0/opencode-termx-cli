# 配置：opencode-termx 插件与 termx-service

## 1. 全局配置（插件读取）
- 路径：`~/.config/opencode/termx.jsonc`（JSONC；首次运行由插件生成模板）。
- 测试/覆盖：环境变量 `TERMX_CONFIG` 指向自定义配置文件。
- 插件是**唯一读取方**；service/CLI 只接受插件/用户显式传入的参数。

### 字段
| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `toolLang` | `"en"` | 工具描述/错误文案语言：`en`/`zh`；`TERMX_TOOL_LANG` 覆盖 |
| `webLang` | `"en"` | Web UI 语言预留；`TERMX_WEB_LANG` 覆盖 |
| `service.autostart` | `true` | service 不可连时是否由插件自动拉起；插件加载时后台预热（不阻塞 opencode），首次工具调用复用/重试 |
| `service.bin` | `"termx-service"` | service 可执行文件（命令名或绝对路径） |
| `service.configDir` | 空 | token/port 目录；空 = `<插件缓存>/config` |
| `service.port` | `0` | 监听端口；0 = OS 分配 |
| `service.tokenFile` | 空 | token 文件；空 = `<configDir>/token` |
| `service.endpoint` | 空 | 显式 `ws://...`；非空时优先且不自动启动 |
| `web.port` | `0` | Web 服务端口；`0` = 不启动 |
| `web.allowRemote` | `false` | 是否允许远程访问；`false` 只绑定 `127.0.0.1`（回环免登录） |
| `web.password` | 空 | Web 登录密码；启动方通过环境变量 `TERMX_WEB_PASSWORD` 传给 service，不进命令行/日志 |
| `log.maxDays` | `7` | service 文件日志保留天数 |
| `limits.maxTerminals` | `16` | 同时运行终端上限（传给 service env） |
| `limits.sshHistoryLimit` | `64` | SSH 连接历史条数上限；`0` 关闭（传给 `TERMX_SSH_HISTORY_LIMIT`） |
| `limits.clientIdlePolicy` | `"no_children"` | 无存活会话到期策略：`never` / `no_children` / `any`（传给 `TERMX_CLIENT_IDLE_POLICY`） |
| `limits.clientIdleTimeoutMs` | `300000` | 无存活会话超时（毫秒，默认 5 分钟）；`0` 关闭（传给 `TERMX_CLIENT_IDLE_TIMEOUT_MS`） |
| `interaction.tickMs` | `250` | 交互识别 ticker 周期 |
| `interaction.noOutputMs` | `5000` | 无输出判定阈值 |
| `injection.consent` | `"ask"` | 本地/远端共同默认授权：`ask`（缺 `allow_injection=true` 时返回 `consent_required`）/ `allow` / `deny` |
| `injection.root` | `"~/.termx"` | 默认注入根目录；本地展开本机 home，远端展开远端 home |
| `injection.helperLocal` | 空 | 本地 controller 上的 helper 产物路径；空表示不自动上传，使用远端预装 helper |
| `injection.paths.helper` | `<root>/bin/termx-helper` | 远端 helper 安装/预装目标 |
| `injection.paths.pwsh` | `<root>/runtimes/pwsh` | pwsh runtime 目录（本轮预留） |
| `injection.paths.bash` | `<root>/runtimes/bash` | bash runtime 目录（本轮预留） |
| `injection.paths.sh` | `<root>/runtimes/sh` | sh runtime 目录（本轮预留） |
| `local.injection` / `remote.injection` | 继承 `injection` | 可选覆盖；字段级合并，`paths` 按 key 覆盖。旧的 `remote.injection` 仍兼容 |

### 环境变量覆盖
- `TERMX_CONFIG`：配置文件路径。
- `TERMX_CONFIG_DIR`：覆盖 `service.configDir`。
- `TERMX_ENDPOINT`：覆盖 `service.endpoint`。
- `TERMX_TOKEN`：覆盖 token 文件。
- `TERMX_TOOL_LANG` / `TERMX_WEB_LANG`：语言。
- `TERMX_WEB_PASSWORD`：覆盖 `web.password`（测试/临时用；插件仅在非空时转传给 service）。
- 其余 service 调参（`TERMX_MAX_TERMINALS`、`TERMX_SSH_HISTORY_LIMIT`、`TERMX_CLIENT_IDLE_POLICY`、`TERMX_CLIENT_IDLE_TIMEOUT_MS`、`TERMX_INTERACTION_*`）由插件按配置转成 env 传给子进程。
- `TERMX_SSH_HISTORY_LIMIT`：SSH 连接历史条数上限，默认 `64`；`0` 表示禁用历史记录与列表。
- `TERMX_CLIENT_IDLE_TIMEOUT_MS`：终端所属 `root_session_id` 无会话心跳的时长上限（毫秒），默认 `300000`（5 分钟），`0` 关闭；与按活动时间的 `idle_timeout` 独立。
- `TERMX_CLIENT_IDLE_POLICY`：到期策略，默认 `no_children`（仍有用户子进程则继续保活）；`any` 到期即终止；`never` 永不因无会话终止。
- `TERMX_SESSION_TTL_MS`：会话心跳 TTL（毫秒），默认 `90000`（90 秒）；超过该时间未收到 `heartbeat {root_session_id}` 视为会话结束。
- 插件先合并顶层 `injection` 与 `local.injection` / `remote.injection`，再把远端生效值转成下面的 env。
- `TERMX_REMOTE_INJECTION_CONSENT`：`ask`/`allow`/`deny`，默认 `ask`；非法值保留默认并 warn。
- `TERMX_REMOTE_INJECTION_ROOT`：注入根目录，默认 `~/.termx`；设置后按 `<root>/bin/termx-helper` 与 `<root>/runtimes/{pwsh,bash,sh}` 重新派生路径。
- `TERMX_REMOTE_INJECTION_{HELPER,PWSH,BASH,SH}`：单独覆盖对应路径；非法路径保留原值并 warn，在连接前由路径校验给出明确错误。
- `TERMX_REMOTE_INJECTION_HELPER_LOCAL`：本地 controller 上的 helper 产物路径；空表示不自动上传。
- 远端路径规则：只允许绝对路径或 `~/...`；拒绝空值、空白/控制字符、`..` 组件与相对路径。`helperLocal` 是本机路径，不走该规则。

## 2. 插件缓存与日志
- 缓存/日志根：`~/.opencode/plugins-cache/@xiaoqiong0v0/opencode-termx/termx-service/`。
- service 文件日志：`<该目录>/service-YYYY-MM-DD.log`（UTC）。
- 保留：启动时删除超过 `log.maxDays` 的 `service-*.log`。
- 开关：读取 `~/.config/opencode/plugin-logger.jsonc` 的 `enabled`（默认 false），由插件传给 service 的 `--log-enabled`。
- 插件自身日志使用 `@xiaoqiong0v0/opencode-plugin-logger`。

## 3. termx-service 显式参数
```
termx-service --config-dir <DIR> [--port <u16>] [--token-file <FILE>]
              [--log-dir <DIR>] [--log-enabled true|false] [--log-max-days <N>]
              [--web-port <u16>] [--web-allow-remote true|false]
```
- `--config-dir` 必填（或显式 `TERMX_CONFIG_DIR`）；**无平台默认目录**。
- token 缺省 `<config-dir>/token`；port 文件 `<config-dir>/port`；runtime catalog 缺省 `<config-dir>/runtimes/index.json`。
- `--log-enabled true` 时必须给 `--log-dir`；`--log-max-days` 默认 7。
- `--web-port` 缺省 `0`（不启动 Web）；`--web-allow-remote` 缺省 `false`。
- Web 登录密码**只**从环境变量 `TERMX_WEB_PASSWORD` 读取，不进命令行与日志。
- 插件自动启动时按上述参数显式传入（bin 来自 `service.bin`）。

### Web 绑定与鉴权
- `web.port = 0`：不启动 Web 服务（默认）。
- `web-allow-remote=false`（默认）：绑定 `127.0.0.1:<web-port>`，回环请求免登录。
- `web-allow-remote=true`：绑定 `0.0.0.0:<web-port>`；非回环请求必须登录。
- `/ws` 升级前校验 Host/Origin：仅本机模式要求 Host 为回环主机且 Origin（若有）同源；远程模式要求 Origin 存在且同源；失败返回 403。
- 访问方式：本机 `http://127.0.0.1:<web.port>`；开启远程后从其他机器访问 `http://<主机>:<web.port>`，未登录会跳转登录页 `/login`。
- 登录：`POST /login`（form `password`）成功后写入 Cookie `termx_web=<随机>; HttpOnly; SameSite=Strict; Path=/`，会话仅存内存。
- 未配置 `TERMX_WEB_PASSWORD`（即 `web.password` 为空）时远程登录必失败并返回明确提示。
- 登录会话在内存中保留 12 小时，过期后需重新登录；没有登出与登录限速。远程 Web 为明文 HTTP，仅建议在可信内网使用，或置于 HTTPS 反代之后。
- 插件连接前校验 endpoint 主机必须是 `127.0.0.1` 或 `[::1]`；外部地址直接拒绝，避免 token 发往非环回主机。
- 路由：`GET /`（首页）、`GET /login`、`POST /login`、`GET /static/*`、`GET /ws`（代理到内部 service，注入 `hello`，浏览器不接触 token）。
- 页面默认「信息」Tab（每条命令+输出为一组：命令行左侧开始时间，输出末行左侧 `+耗时` 与退出状态，无输出补空白行；轮询 `terminal.transcript`）。有 `exit_code` 时显示 `✓ 0`/`✗ N`；无退出码的静默降级显示 `~`。可切到「原始控制台」Tab 使用现有 xterm；切换不销毁终端。
- 所有状态/错误消息写入**底部状态栏**，在「信息」和「原始控制台」两个 Tab 下都可见；`info`/`success`/`warning`/`error` 四色，点击关闭，同一消息关闭后重复上报不再自动弹回；WebSocket 重连成功清除。**不使用 toast**；高频进度/等待态不刷屏；标题栏不显示状态文本。
- **两级选择**：header 依次选择 opencode 会话（`session.list` + 固定项「其他/未绑定」`@web`）和该会话下的终端（`terminal.list` 按 `root_session_id` 过滤；显示名称/状态/pid）。刷新、迁移（`rebound`）与新建后自动刷新并保留/选中当前终端。
- **信息页顶部**：显示当前终端的 `state`、`pid`、`mode`、启动命令（`command` + `args`）与 `cwd`（来自 `terminal.list` / `terminal.status` 快照）。
- **新建终端**：header「新建终端」打开表单，绑定会话（含「其他/未绑定」`@web`）、名称（默认 `default`）、类型 `local`/`ssh`。
  - `local`：程序、参数（空格分隔，支持单/双引号）、cwd、环境变量（每行 `KEY=VALUE`）、cols/rows。
  - `ssh`：host/port/user、密码或私钥路径/口令（私钥路径可留空：service 依次探测本机 `~/.ssh/id_ed25519`、`~/.ssh/id_rsa`，显式路径优先）、固定结构化程序 `termx-pwsh` 或 `termx-bash`（普通 ssh 请用 `local` 自定义程序，raw-only）、cols/rows；helper 路径/上传/摘要由 service 按注入配置自动处理。主机密钥默认 `ask`：未知主机弹出指纹确认，匹配 known_hosts 直接连接，指纹变化硬失败；`accept_any` 仅在高级/不安全选项中显式开启，且不写 known_hosts。回环地址不再默认静默接受。
  - **最近连接**：SSH 区块顶部提供下拉（`ssh.history.list`），选择后只填充 host/port/user/auth_type，**不填充**密码、私钥口令或私钥路径，并清空表单里已有的秘密输入；空历史或读取失败时隐藏下拉，不阻塞创建。
  - `local` 自定义 cmd/args 不接入 shell 事件：只显示原始控制台，不生成信息页/transcript；模型用 `read --tail-lines`/`--tail-chars`/`search` 获取输出。
  - 提交 `terminal.create`（`mode:"pty"`）；绑定「其他/未绑定」时 `root_session_id` 使用保留值 `@web`。成功后刷新两级选择并选中新终端，失败在弹窗内显示错误。
  - **密码/口令只随本次创建请求发送**（内存表单 → WebSocket），不写页面文本、日志或 localStorage；关闭弹窗时清空。
- **`@web` 生命周期**：该会话下的终端不参与无会话超时（clientless 扫描跳过），只能手动终止；非管理员（普通 opencode 会话连接）不可见/不可访问。迁移弹窗也可把终端迁到 `@web`（管理员，等同解绑）。

## 3.1 单实例锁与 port/token 一致性
- 每个 `config_dir` 只允许一个 termx-service：启动时先对 `<config-dir>/service.lock` 取**排他文件锁**，再写 token、绑定主监听/Web，最后原子写 `port`。
- 锁被占用时启动失败（`ServiceError::AlreadyRunning`），**不覆盖**已有 `port`/`token`；锁由进程持有的文件句柄维持，进程退出（含崩溃）由操作系统自动释放，无需手工清理。
- `port` 使用“同目录临时文件 + rename”原子替换：绑定中途失败不会留下半写文件，也不会破坏上一次成功写入的内容。
- 插件 autostart 的并发容错：子进程若因单实例锁立即退出，插件会在宽限期内重新读取 `port`/`token` 并握手一次；成功则复用已运行实例，不再报“connection refused”。

## 3.2 Runtime catalog（P1）

- 目录：`<config_dir>/runtimes/`（service 也可用 `ServiceConfig::with_runtime_dir` 覆盖；无新增必填 CLI 参数）。
- 索引：`<config_dir>/runtimes/index.json`；service 启动时读取一次，缺失/非法只记录警告并保持空 catalog，不影响旧 `cmd/args` 创建。
- P1 **不下载、不远程传输**；只使用本机已存在的文件。

```json
{
  "runtimes": [
    {
      "id": "termx-pwsh",
      "name": "termx-pwsh",
      "version": "0.1.0",
      "platform": "windows",
      "arch": "x86_64",
      "bin": "bin/termx-pwsh.exe",
      "args": []
    },
    {
      "id": "termx-bash",
      "name": "termx-bash",
      "version": "0.1.0",
      "platform": "linux",
      "arch": "x86_64",
      "bin": "bin/termx-bash",
      "args": ["--noprofile"]
    }
  ]
}
```

- 结构化事件 runtime：`termx-pwsh`、`termx-bash`。只有这两个 ID 的本地 PTY 会创建带外事件通道；直接启动的 `bash`/`sh`/`pwsh` 仍是 raw-only。
- `bin` 必须是 `runtimes/` 下的相对路径；拒绝绝对路径、`..`、盘符前缀与 symlink 越界。
- 启动前检查文件存在且可执行；Windows 按 `.exe/.cmd/.bat/.com`，Unix 检查执行位。
- `platform` / `arch` 与 `std::env::consts` 对齐：`windows|linux|macos`、`x86_64|aarch64`。
- `runtime.list` 只返回当前平台兼容项；`available` 表示 bin 存在且可执行（不暴露绝对路径）。
- `terminal.create.runtime_id` 与自定义 `cmd/args` 互斥；未给 `runtime_id` 时保持旧行为。

## 3.3 SSH 连接历史

- 路径：`<config_dir>/ssh_history.json`；锁文件 `<config_dir>/ssh_history.json.lock`。
- 上限：`ssh_history_limit` / `TERMX_SSH_HISTORY_LIMIT`，默认 `64`；`0` 禁用。
- 权限：Unix `0600`；Windows 继承目录 ACL（不额外夸大已收紧）。
- 写入：同目录临时文件 + `rename` 原子替换；同进程互斥 + 跨进程排他文件锁串行化读-改-写。
- 损坏：JSON 解析失败按空历史处理并记录警告，不阻断 `terminal.create`；下一次成功记录会覆盖为合法文件。
- 记录字段：`host`、`port`、`user`、`auth_type`（`password` / `key`）、`use_count`、`last_used`（RFC3339 UTC）。
- **不保存**密码、私钥口令、私钥内容或私钥路径。
- 行为：仅 SSH `terminal.create` 成功后记录；失败、取消、`consent_required`、主机密钥未知/变化等路径不记录。
- 去重键：`host + port + user`；重复成功连接只 `use_count += 1` 并刷新 `last_used`。
- 淘汰：超限先淘汰 `use_count` 最小者；相同 `use_count` 淘汰 `last_used` 最旧者。
- 列表：`ssh.history.list` 仅管理员（Web 代理）可用，按 `last_used` 降序；可选用 `ssh.history.remove` / `ssh.history.clear`。

## 4. termx-cli / termx-client
- 必须显式 `--config-dir` 或 `--endpoint`（或非空 `TERMX_CONFIG_DIR`）；无平台默认。
- token 优先级：`--token` > `TERMX_TOKEN` > `<config-dir>/token`。
- 端口读 `<config-dir>/port`。
- **CLI 调用者会话 ID**：`--root-session-id <ID>`（全局参数）> `TERMX_ROOT_SESSION_ID` > `default`；作为 `hello.root_session_id` 发送，决定 CLI 能访问哪些终端（仅同会话，`list`/`prune` 也只作用于该会话）。CLI 不声明 `admin`。
- **内置 runtime**：`termx-cli local --runtime <ID>`（全局参数，可放在子命令前/后）设置 `CreateParams.runtime_id`，并清空 `cmd/args`；若同时给 `--cmd` 或尾随参数则直接报用法错误。未给 `--runtime` 时保持旧默认 shell 行为。`termx-cli runtime list` 暂未实现（可通过 Web/协议 `runtime.list` 查询）。
- 插件按当前工具调用的 opencode `sessionID` 建立/复用**独立连接**（`endpoint + token + rootSessionId` 为缓存键），并每 30s 发 `heartbeat {root_session_id}`；`session.deleted` 时用该会话连接发 `session_closed` 后关闭。Web 代理使用 `admin:true`，不经插件。
- CLI 在 `local --follow` 与长 `exec` 期间同样每 30s 发送 `heartbeat {root_session_id}`；心跳失败仅停止心跳，不中断命令与输出。

### 跨会话迁移与心跳
- `terminal.rebind` 把终端绑定键改到新的 `root_session_id` 后，**旧会话心跳不再保活该终端**；service 会清零 `clientless_since`，从下一轮 idle ticker 起按新会话是否存活重新计时。
- 因此迁移到另一个 opencode 会话后，该会话必须持续发送 `heartbeat {root_session_id}`（插件按工具调用会话自动发送）；否则超过 `clientIdleTimeoutMs`（或 `session_ttl`）后终端会被 `client_timeout` 终止。
- Web 手动迁移不会自动为目标会话发心跳：目标会话需由 opencode 插件侧活跃，或把 `limits.clientIdleTimeoutMs` 设为 `0` 关闭该检查。
- Web 页面用「迁移…」弹窗完成同一操作：目标会话下拉来自 `session.list`（标记 live/终端数/当前会话），确认后调用 `terminal.rebind`，同样不会自动为目标会话发心跳。

## 5. 语言
- `toolLang`：插件工具描述与错误文案（en/zh）。
- `webLang`：Web UI 预留。
- service/CLI 运行日志语言当前固定（中文/英文混合）；后续可扩展。
