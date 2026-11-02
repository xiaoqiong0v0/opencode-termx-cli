# opencode-termx-cli

用 Rust 重写终端会话/进程监督层的独立项目，目标是为本地终端与 SSH 终端提供**真实进程状态**、**精确退出/中断**和**交互事件**，而不是依赖输出文本中的完成标记。

> 当前状态：需求与关键决策（OQ-1~6）已确认；M0 工程骨架（Cargo workspace、协议类型、CI 门禁）已建立，进入 M1（service 骨架）。协议/开发计划仍随实现定稿。先读 `docs/REQUIREMENTS.md` 与 `docs/PROTOCOL.md`。

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

## 实现顺序
见 `docs/DEV_PLAN.md` 的里程碑 M0–M6：工程骨架 → service 骨架 → 本地 PTY → stdio/流控 → 交互识别 → CLI/adapter → SSH/helper。

## 实现边界：自研终端，不把 PTY 当黑盒
- **不引入 node-pty**；也不把 `portable-pty` 之类的现成封装当终端实现。
- Linux/macOS：直接调用 PTY 系统接口（`/dev/ptmx`、`grantpt/unlockpt`、termios/ioctl）管理主从端。
- Windows：直接绑定 `CreatePseudoConsole` / `ResizePseudoConsole` / `ClosePseudoConsole`，配合 `STARTUPINFOEX` 与 Job Object；ConPTY 只作为字节传输层。
- **自研**：VT/ANSI 解析、屏幕/滚动缓冲、光标/模式、键盘编码、粘贴/焦点、进程组/前台进程、事件状态机。
- 终端核心跑在**独立 daemon 进程**，绝不继承 opencode 的 stdin/stdout/控制终端；CLI/opencode 通过 WebSocket（环回）/JSON 事件通信。
- 如果连 OS PTY/ConPTY 都不允许：只能支持普通程序的 stdin/stdout 管道模式，无法可靠支持 vim/top/ssh 密码提示等全屏交互程序；这是范围裁剪，不是实现细节。

## 构建与测试
要求 stable Rust ≥ 1.85（edition 2024）。仓库提交 `Cargo.lock`，本机与 CI 门禁统一使用 `--locked`：
```sh
cargo test --workspace --locked
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
```

Windows 本机可用 `scripts/ci.ps1` 一次跑完上述三项门禁。