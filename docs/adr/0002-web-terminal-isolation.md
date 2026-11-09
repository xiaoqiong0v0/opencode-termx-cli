# ADR 0002：Web 终端 xterm 隔离与 CPR 处理

- 状态：已采纳
- 日期：2026-11-09
- 关联：`docs/design/ARCHITECTURE.md` §4、`crates/termx-service/src/web_assets/app.js`

## 背景

Web 早期只有一个全局 xterm 实例，所有 `terminal_id` 切换复用同一 buffer/cursor。旧终端的 DSR 响应、提示符和行编辑状态会污染新终端，表现为输入出现在顶部、回车执行错误命令、新旧终端内容串台。

尝试过在 ConsoleHost 增加 PSReadLine CPR provider；provider 虽能消费部分 DSR 响应，但在真实 Web 中反而造成 `pwd/>`、输出顺序错位等回归。通过 `TERMX_PSREADLINE_CURSOR_PROVIDER=0` 对照确认是 provider 引起。

## 决策

- Web 按 `terminal_id` 维护独立 `TerminalView`/xterm 实例；切换只显隐，不共用 buffer。
- 服务重连、force 重连、终端删除/替换时释放旧 view，重建干净实例。
- 不依赖 `term.clear()` 做终端隔离。
- 不在 Web 过滤 DSR，不用 focus/清屏补丁修输入。
- ConsoleHost 只保留官方 PSReadLine 路径；provider 方案撤回。
- 远端 `TERM` 缺失时由 helper 注入 `xterm-256color`；显式 TERM 保留。

## 后果

- 多终端切换不再串 buffer/cursor。
- 已有坏 view 不会自动修复，必须重建/重连。
- PSReadLine 的 cursor/CPR 兼容仍需在 ConsoleHost 层谨慎处理，不能用 provider 强行替换。

## 验证

- `iso1`/`iso2` 多终端输入内容互不串。
- providerless 远端：`cd /` 后 `pwd`、`ls` 正常。
- `cargo test -p termx-service --test web_root` 通过。
