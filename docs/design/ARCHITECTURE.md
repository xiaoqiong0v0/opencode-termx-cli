# 架构：中心化 termx service

目标不是“包一层 node-pty”，而是自研终端/进程监督栈。OS PTY/ConPTY 只作为字节通道；终端会话归中心服务所有，不归 opencode 会话所有。

## 1. 总体结构
```text
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ opencode     │   │ Web          │   │ CLI / 其他   │
│ 模型/工具     │   │ 浏览器        │   │ 客户端        │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │ WebSocket(环回)   │                 │
       └───────────────────┼─────────────────┘
                           ▼
                 ┌──────────────────┐
                 │ termx-service    │  中心化常驻服务
                 │ 会话/进程/事件    │
                 ├──────────────────┤
                 │ termx-terminal   │ 自研 VT/ANSI/屏幕/输入
                 │ termx-proc       │ PTY/ConPTY、进程组、wait/exit/signal
                 │ termx-ssh        │ SSH + helper 握手/降级
                 │ termx-protocol   │ schema/版本/背压/鉴权
                 └────────┬─────────┘
                          │ 可选
                 ┌────────▼─────────┐
                 │ termx-helper     │ 远端 sidecar
                 └──────────────────┘
```

## 2. 核心原则
1. **服务拥有终端**：会话、PTY、进程树、buffer、事件全部在 `termx-service`；opencode/Web/CLI 只是客户端。
2. **opencode 只与服务交互**：模型通过 opencode 工具 adapter 发 RPC；不直接持有终端对象，不依赖插件进程生命周期。
3. **opencode 会话重启不杀终端**：客户端断开只是 detach；终端继续运行，重连后按 terminal ID re-attach。
4. **默认永不超时**：`idle_timeout = 0`（默认）表示永不因空闲断开；用户可配置正数超时。超时只 detach/回收会话，必须显式定义是“仅断开客户端”还是“终止进程”。
5. **显式生命周期**：`detach`（仅断连）/ `terminate`（杀进程树）/ `prune`（清理终态）分开；不能用 opencode `session.deleted` 隐式杀终端。
6. **控制终端隔离**：service 不继承 opencode 的 stdin/stdout/控制终端；所有 I/O 走协议，避免 node-pty 式 TUI 污染。
7. **进程状态优先**：exit/signal 以 OS wait/进程状态为准；文本提示符只辅助交互，不作为主完成判据。
8. **PTY 只是字节通道**：Linux 直接 `/dev/ptmx` + termios/ioctl；Windows 直接 ConPTY + Job Object；屏幕状态由自研 emulator 维护。
9. **两种连接模式**：`pty`（终端/全屏程序）与 `stdio`（普通程序管道）；不强行给普通程序造 TTY。
10. **终态唯一**：`exited`/`interrupted`/`error` 只发一次；service 重启也必须让客户端收敛。

## 3. 会话生命周期
生命周期与附着是正交维度（权威定义见 `PROTOCOL.md` §5）：
```text
生命周期：created -> running -> exiting -> exited | interrupted | error
附着（每客户端）：attached <-> detached（不影响生命周期）
idle_timeout > 0：按配置仅 detach 或 terminate
```
- `detach` 只是客户端动作；终端继续运行，re-attach 按 `terminal_id` + `cursor` 恢复。
- terminal ID 由 service 生成并持久；opencode/root/child 会话不参与 ownership。
- 客户端连接只建立 attachment：`attach(terminal_id)` / `detach`。
- `opencode` 会话关闭/重启：只 detach；默认不杀终端。
- `idle_timeout` 默认 0（永不）；配置为正数时，达到阈值后的行为必须在配置中明确：
  - `detach`：只断开客户端，进程继续；
  - `terminate`：终止进程树并落终态。
- 服务重启：现存 PTY 进程是否保留取决于 service 自身是否能接管；v0 明确“service 重启会丢失终端”，并让客户端收到 `service_lost` 终态。后续可做 supervisor/恢复。
- 永不超时意味着孤儿会累积：必须提供 `list`/`prune`/显式 `terminate`，并设最大会话数/资源上限。

## 4. 客户端
- `opencode adapter`：薄层，把模型工具调用映射为 service RPC；不解析终端渲染状态。
- `Web`：订阅同一 service 事件；raw/transcript/命令输入都走 service，不直连本地 PTY。
- `CLI`：调试/运维客户端，和 Web 使用同一协议。
- `termx-client`：CLI 与 opencode adapter 共用的客户端库，负责服务发现、`hello`、按 `id` 配对请求与 `broadcast` 事件；不解析终端渲染状态。
- 多客户端：同一 terminal 可多 attach；输入冲突需定义 owner/lease，至少保证输出广播顺序。

## 5. 事件状态机
状态/事件命名以 `docs/specs/PROTOCOL.md` 为唯一权威；此处只述维度关系（三个正交维度）：

```text
生命周期: created -> running -> exiting -> exited | interrupted | error   (终态唯一)
交互:     none <-> interactive <-> waiting_input                          (运行期子状态)
附着:     attached / detached                                             (每客户端，不影响运行)
```

事件（详见 PROTOCOL §7）：
```text
started -> output* -> interactive_changed* -> waiting_input*
        -> (attached | detached)*
        -> exited | interrupted | error
```
- `waiting_input` 不等于 exited；模型可继续 `send`。
- 交互进入/切换/退出统一用 `interactive_changed{active,pid,pgid,command,confidence}`。
- 终态只发一次；终态后拒绝输入，除非显式新建/重连。

## 6. 背压与持久化
- 输出可有界丢弃；状态/终态事件不可丢。
- 每 terminal 单调 seq；多客户端从 cursor 订阅，断线重连补发/重置。
- 默认不把命令/输出写日志；历史落盘需显式开关并脱敏。
- 本机 WebSocket 仅绑定环回地址并做 token 鉴权，避免任意进程/外部注入命令（见 `PROTOCOL.md` §2–§3）。

## 7. 依赖与质量门禁
- 不引入 `node-pty`/`portable-pty` 作为终端实现；OS API 直接绑定。
- 远端 helper 与主协议版本绑定；helper 缺失/失败回退，不阻塞。
- `cargo fmt --check`、`cargo clippy --all-targets --locked -- -D warnings`、`cargo test --workspace --locked`。
- 平台矩阵：Windows ConPTY、Linux PTY、macOS PTY（后续）。
- 真机：exit/signal、Ctrl-C、子进程树、交互切换、detach/attach、service 重启、无孤儿。
