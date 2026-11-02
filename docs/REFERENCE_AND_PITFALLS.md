# 参考实现与已知坑

本文写给 `opencode-termx-cli` 的新会话；参考实现为旧项目 `opencode-ssh-tool`（v0.7.5，commit `481cca8`）。新项目不要复制旧实现，只借鉴其**已验证的语义**与踩坑。

## 0. PTY/ConPTY 与“自研终端”的边界
- Linux PTY 是内核接口（`/dev/ptmx` + termios/ioctl）；Windows 全屏交互程序依赖 ConPTY（`CreatePseudoConsole`）。**应用层无法完全绕开 OS 伪终端**去支持 vim/top/ssh 密码提示这类控制台程序。
- 本项目所说的“自研终端”：
  - 不引入 `node-pty` / `portable-pty`（含“非黑盒”使用）作为终端实现；
  - 直接绑定 OS PTY/ConPTY API，把它当字节通道；
  - 自研 VT/ANSI 解析、屏幕/滚动缓冲、光标/模式、键盘编码、粘贴/焦点、前台进程组与事件状态机。
- **node-pty 导致 opencode 界面混乱/无法输入**：优先怀疑控制终端与 stdio 继承（子进程拿到/污染 opencode 的 TTY、`/dev/tty` 读取、前台进程组抢占），不是 PTY 语义本身。新实现必须把终端核心放进独立 daemon，绝不继承 opencode 的 stdin/stdout/控制终端。
- 若严格禁止 OS PTY/ConPTY：只能支持普通程序的 stdin/stdout 管道（行模式），全屏交互程序不在范围内；必须在需求上明确裁剪。
- 事件与输入输出都要经过协议层；daemon 崩溃/断线必须有终态，不能让 opencode TUI 卡住。


## 1. 旧项目结构（只读参考）
> 以下路径/行号均指旧仓库 `opencode-ssh-tool`（v0.7.5），不是本项目。
- `src/base-session.ts`：PTY 抽象、缓冲区、完成/中断/语法错误收尾、history、`peekBuffer`/`readBuffer`。
- `src/session.ts`：SSH 连接、PTY shell、SFTP、远程语法预检。
- `src/local-session.ts`：本地/容器 PTY。
- `src/index.ts`：工具入口、会话 Map、connect/local/exec/read/send、权限、状态文件、root 路由。
- `src/agent.ts`：Web/agent 事件流、raw 游标/代次、cmdStart/cmdDone。
- `src/server.ts`：独立 Web 服务、raw/transcript 快照、订阅、传输任务。
- `docs/design/结构设计.md`：旧行为最完整的伪代码说明。
- `docs/design/read读取语义说明.md`：read 消费/快照语义。
- `docs/design/直接输入与保活心跳说明.md`：终端输入与心跳。
- `docs/design/多行命令包组与raw渲染修复说明.md`：多行/raw 渲染坑。
- `docs/design/中断探针修复说明.md`：Ctrl-C/探针历史。

## 2. 旧实现的关键参考点
### 2.1 完成判定是字符串协议，不是进程状态
- 完成标记：`src/base-session.ts:11-12`（`DONE_TAG`/`PROBE_TAG`），`detectLastDoneMarker` 在 `:166` 附近。
- 命令包装：`src/base-session.ts:600` 的 `markerCmd`；命令末尾执行 `command ; markerCmd`。
- 收尾：`_completeRunning`（约 `:350`）写 history、done、busy；`readBuffer` 在 `:392` 也会触发收尾。
- 痛点：
  - shell 语法错误使整行不执行，标记永不出现 → 旧项目加语法错误识别/静默收尾；Rust 不要再用文本标记作为主判据。
  - 交互程序/REPL 不退出，标记不会出现 → 旧项目靠“静默窗口/动画/后台 watch”猜测，不可靠。
  - 命令输出里若出现类似标记文本，可能误判；必须用 nonce/seq，Rust 仍建议用进程状态。
  - `Ctrl-C` 是终端输入 `\x03`，不保证等于某个进程收到 SIGINT；旧项目用 PROBE 探针补发，复杂且易注入。
  - 多行命令/引号/管道会使标记与命令不同生命周期。

### 2.2 远程语法预检（可复用思路）
- `src/session.ts:300` `precheckSyntax`；`_execSyntaxCheck` `:469`（bash/zsh），`_execPwshSyntaxCheck` `:571`。
- 独立 exec channel，不占 PTY、不写 history、不翻转 busy；失败/超时 fail-open。
- 新项目若做 SSH，可保留“发送前语法错误”能力，但不要在 PTY shell 内做；用独立 channel/helper。

### 2.3 read 快照与消费
- `src/base-session.ts:379` `peekBuffer()`：只读当前快照，不改 `_buffer/_cursor/_streamPos`、不写 history、不翻转 busy。
- `src/base-session.ts:392` `readBuffer()`：旧消费语义，命中完成标记会收尾并切片。
- 旧项目默认 busy 读非消费快照，显式 `-s buffer` 才消费；这是为避免“读一次把输出吃掉”。
- 新项目建议事件流天然增量，不要再用共享 buffer 游标；保留“已读/未读”语义时明确 contract。

### 2.4 raw/transcript 重连
- `src/agent.ts:217-230`：`pushStreams` 以 `generation` 判断同名会话重建；新代次从 0 重读并带 `reset`。
- `src/server.ts` raw 订阅曾用 `r?.data` 判断；新代次空 `rawBuf` 被误判为无实时流，回退旧 history，导致 Web 不刷新、新输出接旧内容（v0.7.5 已修为 `if (r)`）。
- 设计新项目时：**空基线是合法状态**；断线/重连/会话重建必须带 generation/reset；不要用“data 为空”推断无会话。
- raw 与 transcript 不要共用消费游标；历史/实时 buffer 分开。

### 2.5 会话路由与同名冲突
- `src/index.ts:330` `findRoot`：旧插件按根 opencode 会话路由，主子会话共享终端。
- `src/index.ts:409/471` connect/local；`src/index.ts:939` CLI 路由；`src/index.ts:290` 同名跨 SSH/local 清理。
- 旧项目坑：
  - SSH 与 local 各一张 Map，`resolveSession` 优先 SSH；同名双 map 会导致 read/exec 落到旧会话。
  - 同名跨 transport 现已做清理/二次校验，但历史目录 `local-<name>` 与 SSH 名 `local-<name>` 有前缀冲突；新项目应从协议层禁止同一 `sessionID+name` 多 owner，或给 namespace 加不可出现在名字中的分隔符。
  - 不同 root 会话理论上隔离；若出现跨 root 命中，记录 caller sid、root sid、实际 session owner，不能靠猜。
- 新项目约定：会话键为 `(root_session_id, name)`（主会话 id + 会话名称），`terminal_id` 为内部全局唯一标识；同名且无 `--force` 拒绝并说明，带 `--force` 才覆盖（终止旧终端）。name 不允许 `/`、`\`、空；不要只用名字。

## 3. 本地 PTY / 进程监督
- 必须区分：
  - **shell 进程**（长驻）与 **命令进程**（shell 的子进程）。
  - 前台进程组（foreground pgrp）与整个进程树。
  - PTY master/slave、控制终端、`SIGWINCH`。
- Unix：
  - `waitpid` 只能等直接子进程；持久 shell 不会给每条命令 exit status。
  - 命令退出应通过 shell integration（OSC 133/633）或“每条命令独立子进程 + 保留 shell 状态”的自定义协议；不要只等 shell。
  - Ctrl-C 写入 PTY 后由终端驱动发给前台 pgrp；需要观察前台进程退出/返回 shell，不能假定 SIGINT 只发给一个 PID。
  - 关闭会话必须杀**进程组**并清理子进程，避免孤儿/zombie；区分自然 exit 与被 kill。
  - Linux `/proc/<pid>/stat` 的 pgrp/session/tty、`wchan` 可辅助；race/reparent 会使树变化，事件要带快照时间。
- Windows：
  - ConPTY 语义与 Unix PTY 不同；Job Object 管理子进程树；Ctrl-C 通过控制台事件/输入，不是 Unix signal。
  - 关闭 Job/进程树，避免残留 conhost/子进程。
- 普通程序 stdio 模式：
  - 不是所有连接都是终端；可能只有 stdin/stdout/stderr 管道，程序按行/字节交互。
  - 没有 TTY 时许多程序改变缓冲/提示行为；不要强行套 PTY。
  - 进程退出可由 wait 精确得到；等待输入只能启发式（阻塞 read/wchan/无输出/提示符），必须标 confidence。

## 4. 交互识别（核心难点）
- **PTY 不会直接告诉你“正在等待输入”**。可观测信号：
  - 前台进程组是否变化；前台进程是否 TTY；进程状态（sleep/blocked）、`wchan`/fd；
  - 输出提示符/OSC 133/633；近期是否有 stdin 读；
  - 进程树变化（shell → python/sudo/ssh/REPL）；
  - 无输出 + 前台子进程存活 ≠ 一定等待输入（长编译也如此）。
- 交互对象可能是普通程序，不是 shell；因此不要只匹配 shell prompt。
- 交互程序会变化：会话内 foreground pgrp/子进程变化时发 `interactive_changed`，携带 pid/pgrp/command/confidence；模型据此决定是否输入。
- 进入交互时立即发 `interactive_changed{active:true}` 并让调用方返回控制权，**不要等待命令退出**；后续输出仍通过事件流增量发送；疑似等待输入另发 `waiting_input`。
- 避免误报：宁可 `confidence: low` + `reason`，不要自动中断/自动判定完成。
- 退出交互：前台进程退出/回 shell/提示符恢复时发 `interactive_changed{active:false}`；不要只看静默。
- 对模型：事件需是人可读摘要（“python REPL 正在等待输入，pid=…”），但不要泄露密钥/完整敏感输出。

## 5.1 中心化 service vs 旧插件（重要差异）
- 旧插件：`sshSessions`/`localSessions` 在 opencode 插件进程内，按 root opencode session 分 Map；opencode 退出/session 删除可能清理终端。新项目**不要继承**这套 ownership。
- 新 service：terminal 是全局资源，由 service 生成 ID；opencode 模型、Web、CLI 只 attach/detach。opencode 会话重启不杀终端；默认 `idle_timeout=0` 永不超时。
- 因此旧项目的 `findRoot`、root/child 共享、按 opencode session cleanup 逻辑只作为**坑参考**，不应移植为 service 会话模型。
- 多客户端：必须定义 attach 数量、输入 owner/lease、输出广播与 cursor；不能假设只有一个模型调用方。
- 永不超时的代价：孤儿/资源累积；需要 `list/prune/terminate`、容量上限、鉴权；默认永不超时 ≠ 无清理策略。
- service 重启：v0 明确丢终端，客户端收到 `service_lost`；不要假装能恢复旧 PTY。
- 事件/历史：服务端维护每终端 buffer 与单调 `seq` 并负责回放；客户端只上报最后已处理 `cursor`，不可回放时由服务端返回 `reset`。空基线与 `reset` 语义见 `PROTOCOL.md` §8。
## 5. SSH / helper
- 远端 helper 允许安装；必须：
  - 能力握手/版本号；协议不匹配则回退。
  - 校验和/路径/权限；不把密码写入命令行或日志。
  - helper 不可用、启动失败、超时 → `bestEffort: true`，回退 shell integration；主流程不阻塞。
  - helper 自身管理远端 PTY/进程组，才能精确得到远端命令 exit/子进程；仅靠 SSH channel exec 只能得到该 channel 的退出码。
- 无 helper：
  - 只能是 shell integration + 输出/提示符启发式；明确标 `bestEffort`。
  - SSH 连接断开要给出终态；不要无限 busy。
- 远端进程树：
  - 需要 helper 在远端采集；仅客户端无法看到远端子进程。
  - 传输/命令审批/日志脱敏与旧项目一致：不记录命令载荷中的密码/密钥。

## 6. 事件/协议建议
- JSON 事件（每 WebSocket text frame 一个；以 `PROTOCOL.md` 为准）：
  - `type`/`terminal_id`/`seq`/`ts`
  - `pid`/`pgid`/`ppid`/`foreground_pgrp`/`exit_code`/`signal`
  - `state`/`confidence`/`reason`
  - `data` 只用于 output，原始字节以 base64 传输或明确 UTF-8 边界。
- 顺序：started → output* → interactive_changed*/waiting_input* → exited/interrupted；终态只发一次。
- 背压：输出可丢/可截断，但状态事件不可丢；Web/模型读取与 PTY 写入解耦。
- 重连：generation/reset；空基线合法；不要用 history 补成实时流。
- 安全：事件/日志不包含密钥；输入回显可能含密码，默认不记录原始输入。

## 7. 测试设计
- 本地：真实 PTY 启动 `sh`/`pwsh`；`exit 7`；`kill -TERM $$`；`Ctrl-C`；多子进程树；关闭后无孤儿。
- 普通程序：`cat`/自定义 echo server 通过 pipe 交互；等待输入事件；EOF/退出。
- 交互变化：shell → python → exit；shell → sudo（不实际提权可用 fake）→ 返回。
- SSH：本机 sshd/容器；helper 存在/不存在；helper 失败回退；断线终态。
- 事件：顺序、终态唯一、空基线、重连 reset、背压。
- 不用“HTTP 200/文本标记”当完成证据。

## 8. 旧项目已知未解问题（不要继承）
- 文本完成标记对语法错误/交互程序/多行命令天然不稳；Rust 应以进程状态/协议为主。
- raw 空基线回退历史（v0.7.5 已修）是一个典型“空状态被误判为不存在”的坑。
- 同名跨 transport/前缀目录冲突；新项目用唯一 terminal ID 和保留 namespace。
- root/child 共享终端是旧插件的产品选择，不是终端层必须行为；新项目应让 adapter 决定共享策略。
- 旧项目对跨 root 命中的诊断日志有限；新项目从一开始记录 caller session/root/owner。
