# 需求：opencode-termx-cli（Rust 终端/进程监督）

日期：2026-11-02
状态：需求与决策已确认（OQ-1~6 已定）；协议/开发计划随实现定稿
定位：独立 Rust 项目，为中心化 `termx-service` 提供本地与 SSH 终端/进程监督能力。

> 术语、状态与事件命名以 `docs/PROTOCOL.md` 为唯一权威；本文引用其名称，不重复定义。
> 实现路线见 `docs/DEV_PLAN.md`；已知坑与旧项目参考见 `docs/REFERENCE_AND_PITFALLS.md`。

## 1. 目标
- 用 OS 进程/PTY/ConPTY 状态得到真实 `exit code`、`signal`、子进程树与交互状态，替代旧插件的「文本完成标记」。
- 交互程序进入等待输入时异步产生事件通知模型，不阻塞等待命令结束。
- 终端会话归常驻 `termx-service` 所有，opencode 模型 / Web / CLI 均为客户端。

## 2. 核心原则（不可违背）
1. **服务拥有终端**：会话、PTY、进程树、缓冲、事件全部在 service；客户端不持有终端对象。
2. **opencode 重启不杀终端**：客户端断开只是 `detach`，重连按 `terminal_id` re-attach。
3. **默认按活动时间永不超时**：`idle_timeout = 0`；配正数时行为必须显式（`detach` 或 `terminate`）。另有**无存活 opencode 会话超时**：默认 **5 分钟**（`TERMX_CLIENT_IDLE_TIMEOUT_MS` 覆盖，`0` 关闭），以 `root_session_id` 的会话心跳为准（TTL 90s）；默认策略 `no_children`：到期时仍有用户子进程则继续保活，子进程退出后重新计时；`any` 到期即终止；`never` 永不因无会话终止。Web/终端附着不参与保活。
4. **进程状态优先**：退出/信号以 OS wait/进程状态为准；文本提示符只辅助交互判定。
5. **PTY 只是字节通道**：直接绑 Linux PTY / Windows ConPTY；VT/ANSI 解析、屏幕缓冲、键盘编码、前台进程组、事件状态机自研。
6. **控制终端隔离**：service 不继承 opencode 的 stdin/stdout/控制终端。
7. **两种连接模式**：`pty`（全屏交互）与 `stdio`（普通程序管道），不强行给普通程序造 TTY。
8. **终态唯一**：`exited`/`interrupted`/`error` 只发一次。

## 3. 用户场景（v0 必须覆盖）
| ID | 场景 | 期望 |
| --- | --- | --- |
| US-1 | 模型执行耗时命令后返回控制权 | 收到 `started`→`output`*→`exited(exit_code)`，模型不阻塞 |
| US-2 | 会话内 shell → python REPL | 收到 `interactive_changed`，`pid/pgid/command` 更新，`active=true` |
| US-3 | 交互程序等待输入（如 `sudo` 密码、`read`） | 收到 `waiting_input`（带 confidence/reason），模型可继续 `send` |
| US-4 | 向终端发送 Ctrl-C | 收到 `interrupted(signal)`，前台下发，非 `exit 0`、非超时 |
| US-5 | 关闭/重启 opencode 后重连 | 终端仍在运行；`attach` 后补发历史事件并继续实时输出 |
| US-6 | 普通程序按 stdin/stdout 交互（非 TTY） | `stdio` 模式下可读写，等待输入产生 `waiting_input`，不强行造 PTY |
| US-7 | SSH 连接主机，无 helper | 可执行/输入/读取，事件标 `bestEffort: true`；连接断开有终态 |
| US-8 | 服务崩溃/重启 | 客户端收到 `error(code=service_lost)` 终态，不悬挂 |
| US-9 | 同名会话重复创建 | 无 `--force` 返回 `session_exists` 并说明；带 `--force` 覆盖（旧终端终止，新终端 `started`） |

## 4. v0 范围（冻结）
**做**
- `termx-service`：会话 registry、**本机 WebSocket（环回地址）** + token 鉴权、attach/detach、claim/release、list/prune、事件流、cursor 订阅。
- `termx-core`（编排 `termx-proc`/`termx-ssh`）：本地 PTY、进程组/前台进程组、wait/exit/signal、子进程树（Linux `/proc`，Windows Job/进程 API）、`stdio` 模式、交互状态机、SSH（嵌入式优先）+ 可选 helper 降级。
- `termx-cli`：`local` / `ssh` / `exec` / `status` / `read` / `send` / `resize` / `terminate`，JSON 事件（本机 WebSocket）。
- 交互识别：前台进程组/进程树/TTY/prompt(OSC 133/633)/stdin 读的组合启发式，输出 `confidence` + `reason`。
- opencode adapter（薄层）+ Web 客户端仅消费同一协议。

**不做（v0）**
- 不替换 `opencode-ssh-tool` 插件、SFTP、传输任务、权限系统。
- Web 双视图（信息 transcript / 原始控制台）已纳入 v0；不做完整终端模拟器 UI（多标签、分屏等）；不做跨平台安装器/自动升级。
- 不承诺无 helper 时的远端精确进程树。
- 不做 service 重启后的终端接管/恢复（v0 明确 `service_lost`）。

## 5. 功能需求（FR）
### 5.1 服务生命周期
- FR-S1 创建/列出/查询/终止/清理终端；`terminal_id` 由 service 生成，全局唯一。
- FR-S2 `attach`/`detach` 为客户端动作，支持同一终端多客户端；输入冲突用 `claim`/`release` 的 owner/lease 解决（同一时刻单一 lease owner，`force` 可抢占）。
- FR-S3 `detach` 仅断开该客户端，不杀终端；`terminate` 杀进程树并落终态；`prune` 清理终态/孤儿。
- FR-S4 `idle_timeout` 默认 0；正数时按配置 `detach`/`terminate`，事件可区分。
- FR-S4b 无存活 opencode 会话默认 **5 分钟** → 按 `no_children` 策略处理（仍有用户子进程则续期，否则终止并杀进程树），终止事件 `interrupted{signal:"SIGTERM", source:"client_timeout"}`；`TERMX_CLIENT_IDLE_TIMEOUT_MS=0` 关闭，`TERMX_CLIENT_IDLE_POLICY=never|no_children|any` 可切换；会话心跳 TTL 默认 90s。与 `idle_timeout` 独立。
- FR-S5 `prune` 清理已终态/孤儿会话（默认按最后活动时间）；设最大会话数与资源上限。
- FR-S6 会话键为 `(root_session_id, name)`（主会话 id + 会话名称）；`terminal_id` 为内部全局唯一标识。同名且无 `--force` 时拒绝并返回说明；带 `--force` 时覆盖（终止旧同名终端后新建）。

### 5.2 终端与进程监督
- FR-P1 本地 PTY 启动 shell/程序，保留 cwd/env；真实 `exit_code`/`signal`。
- FR-P2 采集并暴露 `pid/pgid/ppid/foreground_pgrp/子进程树`。
- FR-P3 Ctrl-C 写入 PTY 由驱动发前台进程组；观察前台下发与返回 shell，区分自然退出与被 kill。
- FR-P4 关闭会话必须杀**进程组**并清理子进程，关闭后无孤儿/zombie。
- FR-P5 `stdio` 模式：stdin/stdout/stderr 管道，非 TTY。

### 5.3 交互识别
- FR-I1 持续识别交互状态（`none`/`interactive`/`waiting_input`）变化，进入/退出/切换发 `interactive_changed`（含 `active/pid/pgid/command/confidence`）。
- FR-I2 等待输入产生 `waiting_input`（`confidence` + `reason`），不得声称 100% 准确。
- FR-I3 交互对象不限于 shell（python/sudo/ssh/REPL/普通程序）。
- FR-I4 误报处理：宁可 `confidence: low`，不自动中断、不自动判定完成。

### 5.4 SSH 与 helper
- FR-H1 SSH PTY 会话；`exit status` 与断线终态。
- FR-H2 可选远端 `termx-helper`：能力/版本握手、校验和、失败降级。
- FR-H3 helper 不可用/失败 → 回退 shell integration，事件 `bestEffort: true`，主流程不阻塞。
- FR-H4 无 helper 时仅 best-effort，不承诺远端子进程树精度。

### 5.5 CLI 与客户端
- FR-C1 CLI 与 Web/adapter 使用同一协议；JSON 事件（每 WebSocket frame 一个）。
- FR-C2 adapter 仅做工具调用 → RPC 映射，不解析终端渲染状态。
- FR-C3 多客户端输出广播有序；服务端维护每终端事件缓冲与 `seq`，客户端上报最后已处理 `cursor`，由服务端回放或返回 `reset`。

## 6. 非功能需求（NFR）
| ID | 类别 | 要求 |
| --- | --- | --- |
| NFR-1 | 并发/运行时 | async 运行时（推荐 `tokio`），每终端独立任务；多终端并发不互相阻塞 |
| NFR-2 | 性能 | 输出吞吐满足交互程序实时性；PTY 读→事件推送有界延迟；写与读解耦 |
| NFR-3 | 资源上限 | 可配置最大终端数、每终端输出缓冲上限、事件队列上限；超限按策略拒绝/截断并告警 |
| NFR-4 | 安全 | 主 service 默认仅本机 WebSocket（绑定 `127.0.0.1`/`::1` 环回）；Web 默认仅本机，远程需显式开启并密码登录；WebSocket 校验 Host/Origin；token 鉴权；不记录原始输入/输出与密钥；日志脱敏 |
| NFR-5 | 可观测 | 结构化日志（不含载荷）、会话/进程/事件度量、`status`/诊断命令 |
| NFR-6 | 跨平台 | Windows（ConPTY/Job）与 Linux（PTY/进程组）为 v0 必做；macOS 后补 |
| NFR-7 | 可靠性 | 终态唯一；service 崩溃客户端收敛；无孤儿进程；断线不悬挂 |
| NFR-8 | 可测试 | 关键路径有回归测试；测试不依赖文本标记/HTTP 200 作为完成证据 |
| NFR-9 | 可配置 | 超时、缓冲、上限、日志级别走配置，不硬编码 |

## 7. 决策记录（已确认）
| ID | 问题 | 决策 | 理由 |
| --- | --- | --- | --- |
| OQ-1 | SSH 库 | 嵌入 `russh`，**不用系统 `ssh`** | 避免不同系统行为不一致；可控 PTY/exit/helper channel |
| OQ-2 | 传输/鉴权 | **主 service 默认仅本机 WebSocket**（绑定 `127.0.0.1`/`::1` 环回）+ token 校验；Web 默认仅本机，远程需显式开启并密码登录，并校验 Host/Origin | 默认安全 + WebSocket 兼容性好（Web/CLI/adapter 统一）；远程为显式 opt-in |
| OQ-3 | helper 安装 | 随宿主同版本二进制，经 SSH 传临时目录 + sha256 校验 + 协议版本握手；失败回退；不改远端 shell 配置 | 版本强绑定、可校验、可回退 |
| OQ-4 | 等待输入判定 | 组合信号（前台进程状态/阻塞、无输出窗口、prompt/OSC133、stdin 读），输出 `confidence+reason` | 单信号误报高；组合 + 置信度最稳 |
| OQ-5 | 会话标识/同名 | 会话键 `(root_session_id, name)`；`terminal_id` 内部全局唯一；同名无 `--force` 拒绝并说明，带 `--force` 覆盖（终止旧终端） | 与 opencode 主会话对齐，避免误覆盖 |
| OQ-6 | adapter/Web 复用 | 新工具名 `termx_*`；**旧工具只参考不复用**（无兼容层） | 新架构语义不同，避免误迁移旧坑 |

## 8. 验收标准（可测用例）
> 每条给出操作与期望事件序列；以事件名/字段与 OS 真值判定，不以文本标记。

| ID | 用例 | 期望 |
| --- | --- | --- |
| AC-1 | 本地执行 `exit 7` | `started` → `exited{exit_code:7}`，终态唯一 |
| AC-2 | `kill -TERM $$` / Ctrl-C | `interrupted{signal}`；非 `exit 0`、非超时 |
| AC-3 | 多子进程树后关闭 | `status` 见树；关闭后无孤儿/zombie |
| AC-4 | `cat` / 自定义 stdio 程序 | 可读写；等待输入 `waiting_input`；EOF/退出终态 |
| AC-5 | shell → python → exit | `interactive_changed{active:true,pid/pgid/command}` 更新 |
| AC-6 | opencode 断开→重连 | 终端存活；re-attach 补发历史并继续实时 |
| AC-7 | `idle_timeout>0` 且 `detach` | 仅断连，进程继续；事件可区分 |
| AC-8 | `idle_timeout>0` 且 `terminate` | 杀进程树并落终态 |
| AC-9 | SSH 有/无 helper | 有=同本地；无=`bestEffort:true` 且可执行/输入/读取 |
| AC-10 | service 崩溃/重启 | 客户端收 `error{code:service_lost}` |
| AC-11 | 事件顺序与背压 | 顺序正确；输出可截断，状态/终态不丢；seq 单调 |
| AC-12 | 重连 cursor | 空基线合法；服务端按缓冲回放，超出则 `reset`，不误判无会话 |
| AC-13 | 两客户端输入冲突 | 未持有 lease 的 `send` 返回 `permission_denied`；`claim(force)` 抢占后旧 owner 收 `lease_revoked` |
| AC-14 | `prune` 清理 | 仅清理终态/孤儿；按 `last_activity` 或 `created_at` 生效，返回被删除的 `terminal_id` 列表（数量 = 其长度） |
| AC-15 | 错误 token 握手 | 返回 `auth_failed` 并断开；不建立会话 |
| AC-16 | 超资源上限 | 新建/缓冲/队列超限返回 `limit_exceeded`（`retryable=true`），不崩溃 |
| AC-17 | 同名会话重复创建 | 无 `--force` 返回 `session_exists` + 说明（含旧 `terminal_id`）；带 `--force` 旧终端终止、新终端 `started` |
| AC-18 | 无存活会话 ≥ 5 分钟（可配短超时） | 按策略处理：`no_children` 有用户子进程则保活、子进程退出后重新计时并终止；`any` 直接终止；`never` 不终止。终止落 `interrupted{signal:"SIGTERM", source:"client_timeout"}`；会话心跳可阻止终止 |

## 9. 参考
- 实现路线：`docs/DEV_PLAN.md`
- 协议规格：`docs/PROTOCOL.md`
- 旧项目参考与坑：`docs/REFERENCE_AND_PITFALLS.md`
