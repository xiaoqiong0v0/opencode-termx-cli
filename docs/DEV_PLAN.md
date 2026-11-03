# 开发计划：opencode-termx-cli

日期：2026-11-02
状态：**M0–M6 已实现并通过 Windows/WSL + 真实 SSH 容器测试**
依赖：`docs/REQUIREMENTS.md`（范围/验收）、`docs/PROTOCOL.md`（协议权威）

## 1. 原则
- 先做对所有权与进程语义，再做交互识别，最后 SSH/helper。
- 每阶段有可运行产物 + 可验收门禁，未过门禁不进下一阶段。
- 不以文本完成标记作为主判据；不引入 `node-pty`/`portable-pty` 作为终端实现。
- 文档（协议/事件）先于实现冻结，实现与文档不一致时以协议为准并同步文档。

## 2. 技术选型（已定）
| 项 | 选择 | 理由 |
| --- | --- | --- |
| 运行时 | `tokio` | async、跨平台、任务隔离 |
| 传输 | `tokio-tungstenite`（WebSocket，仅绑环回） | 本机 WS + token；Web/CLI/adapter 复用同一协议，兼容性好 |
| 序列化 | `serde` + `serde_json` | 每 WS text frame 一个 JSON |
| Linux PTY | `nix`/`rustix`（openpt/grantpt/unlockpt/termios/ioctl） | 直接绑 OS 接口 |
| Windows | `windows`(windows-rs)：`CreatePseudoConsole`/`ResizePseudoConsole`/`ClosePseudoConsole`、Job Object | 直接绑 ConPTY |
| 进程树 | Linux `/proc/<pid>/stat`+`/proc/*/fd`；Windows ToolHelp/Job | 采集 pgrp/ppid/foreground |
| SSH | `russh`（嵌入式，不用系统 `ssh`） | 避免不同系统行为不一致；可控 PTY/exit/helper channel |
| 错误 | `thiserror` | 分层错误 |
| 日志 | `tracing`（结构化、脱敏） | 可观测 |
| CLI | `clap` | 子命令 |
| 时间/ID | `time`/`ulid` 或 `uuid`(v7) | 全局唯一 terminal_id |

> 门禁：`cargo fmt --check`、`cargo clippy --all-targets --locked -- -D warnings`、`cargo test --workspace --locked`。

## 3. Crate 结构（workspace）
```text
termx/
├─ Cargo.toml                # workspace
├─ crates/
│  ├─ termx-protocol/        # 消息/事件/错误码/版本（无 OS 依赖，可单测）
│  ├─ termx-terminal/        # 自研 VT/ANSI 解析、屏幕/滚动缓冲、键盘编码
│  ├─ termx-proc/            # PTY/ConPTY、进程组/树、wait/exit/signal、stdio
│  ├─ termx-ssh/             # SSH 连接、PTY、helper 握手/降级
│  ├─ termx-core/            # 会话状态机、registry、事件总线、背压
│  ├─ termx-service/         # daemon：传输、鉴权、RPC、订阅
│  ├─ termx-client/          # 客户端库：发现/hello/请求配对/事件广播（CLI 与 adapter 共用）
│  ├─ termx-cli/             # 客户端：local/ssh/exec/status/read/send/...
│  └─ termx-helper/          # 远端 sidecar（可单独构建/交叉编译）
└─ adr/                      # 架构决策记录（选型留痕）
```
依赖方向：`protocol` ← `terminal`/`proc`/`ssh` ← `core` ← `service`；`termx-client` 只依赖 `protocol`，由 `cli`/adapter 复用；`helper` 只依赖 `protocol`+`proc` 子集。

## 4. 里程碑
### M0 工程骨架
- 交付：workspace、空 crate、CI 门禁脚本、`termx-protocol` 类型与事件枚举、测试夹具。
- 门禁：`cargo test` 通过（协议序列化/反序列化、终态分类单测）；fmt/clippy 干净。

### M1 service 骨架
- 交付：daemon 启动、**本机 WebSocket（环回）+ token**、`hello`、会话键 `(root_session_id, name)` 与同名 `--force` 冲突处理、`terminal.list`、`attach/detach`（无真实 PTY 用 mock）、`subscribe` + cursor 回放、方法参数引入 `TerminalLocator` 并支持 `(root_session_id, name)` 定位。
- 门禁：AC-10（service_lost）/AC-12（cursor 回放）/AC-15（鉴权）/AC-17（同名冲突）；多客户端 attach/detach/重连；空基线合法；非环回拒绝；seq 单调、终态唯一由事件缓冲测试覆盖。

### M2 本地 PTY（核心）
- 交付：`pty` 模式创建、`started/output`、真实 `exited/interrupted`、`status` 见 pid/pgid/foreground/树、`send`/`resize`/Ctrl-C、`terminate` 杀进程组、关闭无孤儿；`idle_timeout`（detach/terminate）后台 ticker 与 `terminal.prune`。
- 门禁：AC-1/2/3/7/8/13/14（含 AC-7/8/14 的空闲超时与清理）；无孤儿/zombie；后台读线程与写解耦。

### M3 stdio + 流控
- 交付：`stdio` 模式、`terminal.read`、背压/丢弃/`output_dropped`、每客户端队列上限。
- 门禁：AC-4/11/16；慢读者不阻塞写；状态事件不丢；`output_dropped` 可检测。

### M4 交互识别
- 交付：前台进程组/进程树/TTY/prompt(OSC133/633)/stdin 读组合；`interactive_changed` + `waiting_input`（confidence/reason）。
- 门禁：AC-5；误报可控（low/medium/high）；不自动完成。

### M5 CLI + adapter
- 交付：CLI 全子命令；opencode adapter 薄层（`termx_*` 工具）；Web 客户端订阅同一协议。
- 门禁：端到端 US-1~US-6；WebSocket JSON 事件可回放。
- 已完成（M5b）：`termx-cli` 子命令（`local` / `exec` / `status` / `list` / `read` / `send` / `resize` / `signal` / `terminate` / `prune` / `attach` / `detach` / `claim` / `release`）与集成测试；`ssh` 返回 M6 未实现错误。`exec` 透传子进程退出码，`interrupted` 固定 130；`send` 在单次连接内自动 claim（`--force` 可抢占他人 lease）。
- 安全约束（防误操作）：终端级子命令必须给 `--terminal-id` 或 `--name`；`terminate`/`detach` 不传定位即用法错误（非零退出），批量关闭必须显式 `--all` 且不得与定位同用；`prune` 必须显式给 `--states`/`--older-than`/`--all` 之一，服务端对空过滤且 `all=false` 返回 `bad_request`，不得默认清理全部终态。

### M6 SSH + helper
- 交付：`russh` SSH PTY、exit/断线终态；helper 分发/校验/握手/降级；`capabilities`/`bestEffort`。
- 门禁：AC-9；helper 缺失/失败回退不阻塞；`capabilities.bestEffort` 正确。
- **M6b-1 已完成（fallback）**：`CreateParams.ssh`（host/port/user/`SshAuth`/helper_path/helper_local_path/helper_sha256/upload_helper/accept_any_host_key，Debug 遮蔽凭据）；`termx-ssh` 用 russh 0.63.1 连接/认证、`request_pty + request_shell` 或 `exec`、write/resize/signal/terminate、channel EOF/Close 或断连终态（有 exit-status 用，否则 `-1` best-effort）；`termx-core` 的 `SshBackend`/`SshFactory` 与 `ModeFactory` 路由；service 复用同一事件摄取；`snapshot.capabilities` 暴露 `{helper:false,bestEffort:true}`。真实容器测试（exec/shell/service）env-gated。
- **M6b-2 已完成（helper）**：`SshConfig` 增加 `helper_local_path`/`helper_sha256`/`upload_helper`；上传模式由远端 `mktemp -d /tmp/termx-helper.XXXXXXXXXX` 创建 700 私有目录，写入固定文件名 `termx-helper`（`set -C` 拒绝复用、`umask 077`），随后 `chmod 700` + `sha256sum` 校验；非上传模式只使用 `helper_path`（可选摘要校验）且**绝不删除/覆盖该文件**；`exec <helper> serve` 后完成 `hello`/`hello_ok` 与 `spawn`/`started` 握手。helper 模式下 input/resize/signal/terminate/tree 全部走 helper 协议，`pid`/`pgid`/`tree` 与 `cwd`/`env` 生效、`foreground_pgrp` 为根进程组，能力为 `{helper:true,version,bestEffort:false}`；上传/摘要/版本/握手/spawn 任一失败关闭通道并回退 fallback，本次临时目录在失败、握手前超时/取消（`UploadGuard` 兜底）、会话终态或断连时清理；非法 helper 配置统一 `bad_request`。`termx-protocol::helper` 增加 `tree` 请求与 `tree` 事件；`termx-helper` 在 Linux 读 `/proc` 返回进程树（非 Linux 空列表）。真实容器测试（上传/握手/pid/tree/input/resize/terminate/错误 helper_path/sha 不符/版本不符/临时目录清理/helper_path 不被删除）env-gated。

## 5. 阶段风险
| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| ConPTY 与 Unix PTY 语义差异大 | 平台分支复杂 | `termx-proc` 抽象 trait，分平台实现 + 各自集成测试 |
| 持久 shell 无单命令 exit | 无法精确命令级退出 | shell integration(OSC133) 或 helper 包装；不承诺无 helper 精度 |
| 等待输入无法直接观测 | 误报 | 组合信号 + confidence + 不自动完成 |
| 永不超时积累孤儿 | 资源泄漏 | `prune`/容量上限/`terminate` |
| service 崩溃丢终端 | 客户端悬挂 | `service_lost` 终态；后续再评估接管 |
| SSH 无 helper | 远端树不可见 | `bestEffort:true`，明确裁剪 |
| 仅本机 WS 的可达性 | 外部无法接入（设计如此） | 明确仅本地；跨机后续评估，不弱化环回/ token |

## 6. 测试策略
- 单元：协议序列化、状态机、seq/cursor、背压、错误码。
- 集成：真实 PTY（`sh`/`pwsh`）、`exit 7`、`kill -TERM`、Ctrl-C、子进程树、关闭无孤儿。
- stdio：`cat`/自定义 echo 程序、EOF、等待输入。
- 交互：shell→python→exit；shell→sudo（fake）→回退。
- SSH：本机 sshd/容器；helper 有/无/失败/断线。M6b-1 的真实连接测试由 `TERMX_TEST_SSH_HOST/PORT/USER/PASSWORD` 驱动，未设置时跳过；密码只在运行时注入环境变量，不写入文件或日志。
- 平台矩阵：Windows、Linux 为 v0 必过；macOS 后补。
- 红线：测试不得以文本标记/HTTP 200 作为完成证据。

## 7. 质量门禁（每阶段）
1. `cargo fmt --check` 通过。
2. `cargo clippy --all-targets --locked -- -D warnings` 通过。
3. `cargo test --workspace --locked` 通过，新增关键路径有回归。
4. 协议变更同步 `PROTOCOL.md`；需求变更同步 `REQUIREMENTS.md`。
5. 事件顺序/终态唯一/无泄漏纳入真机验证。

## 8. 交付与回滚
- 代码按里程碑提交；文档变更随里程碑同批。
- 选型/架构变化写 `adr/`，可回溯。
- 未过门禁不合并下一阶段。
