# opencode-termx-cli

用 Rust 重写终端会话/进程监督层的独立项目，目标是为本地终端与 SSH 终端提供**真实进程状态**、**精确退出/中断**和**交互事件**，而不是依赖输出文本中的完成标记。

> 当前状态：**M0–M6 已完成**——协议、service（WS/token/hello）、本地 PTY/stdio、交互识别、`termx-client`/`termx-cli`、SSH（russh）与 `termx-helper` 分发/回退均已落地；Windows/WSL 与真实 SSH 容器测试通过。先读 `docs/REQUIREMENTS.md`、`docs/PROTOCOL.md` 与 `docs/ADAPTER.md`。

> 架构：**中心化 termx service**。终端归服务所有；opencode 模型/Web/CLI 都是客户端，经**本机 WebSocket（环回）+ token** 访问。opencode 会话重启只 detach，不杀终端。

## 为什么独立
- 与现有 `opencode-ssh-tool`（Node/TypeScript 插件）解耦，避免 npm 插件构建链与 Rust 二进制/跨平台发布互相牵制。
- 终端/PTY/进程树/SSH 属于独立运行时能力；中心 service 常驻，opencode 只做客户端 adapter。
- 不再按当前插件的“Bun/Node 进程内会话”架构实现；Web/模型都改为访问 service。

## 目标能力
- 本地 PTY：启动 shell/程序，保留 cwd/env/交互；进程组、子进程树、exit code、signal 精确可见。
- SSH：PTY 会话；远端允许安装可选 `termx-helper` 时获得与本地同级的进程/命令状态；helper 不可用时回退 shell integration + best-effort。
- 交互识别：普通程序也可能按 stdin/stdout 方式交互；运行中程序可能切换（shell → python → sudo/ssh）。进入/退出交互时产生事件，模型不再同步等待。
- 事件化：`started` / `output` / `output_dropped` / `waiting_input` / `interactive_changed` / `attached` / `detached` / `lease_revoked` / `exited` / `interrupted` / `error`（含 `service_lost`）/ `capabilities`。
- 生命周期：默认 `idle_timeout=0`（永不超时）；opencode 会话重启只 detach，不中断终端；用户可配置超时/显式断开。

## 文档
- `docs/REQUIREMENTS.md`：目标、场景、范围冻结、功能/非功能需求、决策记录（已确认）、可测验收。
- `docs/PROTOCOL.md`：**术语/状态/事件/RPC 的唯一权威**（传输、鉴权、序号/游标、背压、错误码、版本）。
- `docs/DEV_PLAN.md`：技术选型、crate 结构、里程碑 M0–M6、门禁、测试策略、风险。
- `docs/ARCHITECTURE.md`：分层、事件状态机、依赖与质量门禁。
- `docs/REFERENCE_AND_PITFALLS.md`：旧实现可参考点、已知坑、设计约束。
- `docs/ADAPTER.md`：opencode `termx_*` 工具 → RPC 映射与事件通知契约。
- `docs/ASYNC.md`：并发交互点清单（共享状态/保护手段/顺序契约）。

## 实现顺序
见 `docs/DEV_PLAN.md` 的里程碑 M0–M6：工程骨架 → service 骨架 → 本地 PTY → stdio/流控 → 交互识别 → CLI/adapter → SSH/helper。

## opencode 插件（项目内）
- **包名**：`@xiaoqiong0v0/opencode-termx`（`private: true`，当前为本地插件，不发布）。
- **源码（跟踪）**：`opencode-plugin/termx/`（`src/termx.ts` + `src/termx-rpc.mjs` + `package.json` + `tsconfig.json`）。
- **生成**：`node scripts/build-opencode-plugin.mjs` → 用 esbuild 打包为 `.opencode/plugins/termx.js`，并写 `.opencode/package.json`。
- **全局配置**：`~/.config/opencode/termx.jsonc`（首次生成模板；语言、service autostart/端口/token、web 端口、日志保留、限额）。详见 `docs/CONFIG.md`。
- **自动启动**：service 不可连时插件按配置拉起 `termx-service`，显式传 configDir/port/token/日志目录与开关；日志落 `~/.opencode/plugins-cache/@xiaoqiong0v0/opencode-termx/termx-service/`。
- **类型检查/构建**：`cd opencode-plugin/termx && npm run typecheck`、`npm run build`（产出 `dist/`，`npm pack --dry-run` 校验发布内容）。
- **日志**：使用 `@xiaoqiong0v0/opencode-plugin-logger`；开关见 `~/.config/opencode/plugin-logger.jsonc` 的 `enabled`（默认关）。
- **Web**：默认仅本机 `http://127.0.0.1:<web.port>`（回环免登录）；远程需 `web.allowRemote=true` + `web.password` 并在 `/login` 登录；`web.port=0` 不启动。
- `.opencode/` 由 opencode 管理并已 gitignore（插件产物与 `node_modules` 不入库）；源码改动后需重新生成。
- 加载：opencode 自动发现 `.opencode/plugins/`；本项目**不需要根 `opencode.json`**（`plugin` 数组仅用于 npm/远程 spec，勿登记本地文件）。
- 前置：先显式启动 `termx-service --config-dir <DIR> [--log-dir <DIR> --log-enabled true]`（配置目录不再推导平台默认；`TERMX_CONFIG_DIR` 仅作为显式来源）。
- 工具：`termx_exec` / `termx_local` / `termx_ssh` / `termx_list` / `termx_status` / `termx_read` / `termx_send` / `termx_resize` / `termx_signal` / `termx_terminate` / `termx_prune` / `termx_attach` / `termx_detach` / `termx_claim` / `termx_release`。
- 加载时机：opencode 启动时加载；修改插件/配置或重新生成后需重启 opencode。
- 自测：`node scripts/termx-plugin-selftest.mjs`（需 service 运行，直接引用跟踪源码）。

## 实现边界：自研终端，不把 PTY 当黑盒
- **不引入 node-pty**；也不把 `portable-pty` 之类的现成封装当终端实现。
- Linux/macOS：直接调用 PTY 系统接口（`/dev/ptmx`、`grantpt/unlockpt`、termios/ioctl）管理主从端。
- Windows：直接绑定 `CreatePseudoConsole` / `ResizePseudoConsole` / `ClosePseudoConsole`，配合 `STARTUPINFOEX` 与 Job Object；ConPTY 只作为字节传输层。
- **自研**：VT/ANSI 解析、屏幕/滚动缓冲、光标/模式、键盘编码、粘贴/焦点、进程组/前台进程、事件状态机。
- 终端核心跑在**独立 daemon 进程**，绝不继承 opencode 的 stdin/stdout/控制终端；CLI/opencode 通过 WebSocket（环回）/JSON 事件通信。
- 如果连 OS PTY/ConPTY 都不允许：只能支持普通程序的 stdin/stdout 管道模式，无法可靠支持 vim/top/ssh 密码提示等全屏交互程序；这是范围裁剪，不是实现细节。

## CLI 用法（M5b）
`termx-cli` 必须显式给出 `--config-dir <DIR>` 或 `--endpoint <URL>` 之一（也可用非空 `TERMX_CONFIG_DIR`），**不再推导平台默认配置目录**。只给 `--config-dir` 时读取其中的 `port` 与 `token` 自动连接；只给 `--endpoint` 时必须提供 `--token` 或非空 `TERMX_TOKEN`（或同时给 `--config-dir` 读取 token 文件）。token 优先级：`--token` > `TERMX_TOKEN` > `<config-dir>/token`；endpoint 必须是 `ws://127.0.0.1:<port>` 或 `ws://[::1]:<port>`。全局参数 `--root-session-id`（默认 `TERMX_ROOT_SESSION_ID` 或 `default`）、`--name`、`--force`、`--json` 在子命令前后都可写。

```sh
# 创建并跟随一个命名 PTY（默认 %COMSPEC% / $SHELL）
termx-cli local --name build --follow

# 空闲 30 分钟后断开（终端继续运行）；动作可改为 terminate
termx-cli local --name build --idle-timeout 1800 --idle-action detach

# 一次性执行命令：stdout/stderr 按原字节输出，退出码透传（中断为 130）
termx-cli exec -- sh -c 'echo out; echo err 1>&2; exit 3'
termx-cli exec --pty -- cmd.exe /C "echo hi"

# 终端操作：--name 与 --root-session-id 组合定位，或直接用 --terminal-id
termx-cli list
termx-cli status --name build
termx-cli read --name build --cursor 0 --max-bytes 65536
termx-cli send --name build --text "make test" --enter   # 同一连接内自动 claim
termx-cli resize --name build --cols 120 --rows 30
termx-cli terminate --name build --force

# 关闭全部必须显式 --all；不传名称/定位不会隐式关闭任何终端
termx-cli terminate --all --force
termx-cli detach --all

# prune 不会默认清空：--states / --older-than / --all 至少给一个
termx-cli prune --states exited
termx-cli prune --all

# 机器可读：事件/结果为单行 JSON（output 以 base64 承载）
termx-cli --json list
```

CLI 是「一次调用一条连接」：`send` 会在同一连接内先自动 `claim` 再发送；`--force` 可抢占他人持有的 lease。终端级子命令必须给 `--terminal-id` 或 `--name`；`terminate`/`detach` 只有显式 `--all` 才会批量处理，`prune` 必须显式给过滤或 `--all`。`ssh` 子命令在 M6 之前返回「M6 未实现」并以非零码退出。

## 构建与测试
要求 stable Rust ≥ 1.85（edition 2024）。仓库提交 `Cargo.lock`，本机与 CI 门禁统一使用 `--locked`：
```sh
cargo test --workspace --locked
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
```

Windows 本机可用 `scripts/ci.ps1` 一次跑完上述三项门禁。