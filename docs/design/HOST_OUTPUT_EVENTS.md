# 宿主输出事件（O）与模型输出

## 目标

普通命令的输入/输出必须与 xterm 显示解耦：transcript 不再等待 ConPTY 渲染/PTY 尾段，也不使用 OSC、token、sleep 或字符匹配。

## 事件顺序

```text
C  -> command-id 建立，绑定 transcript 代次
O  -> 宿主 stdout/stderr 文本（base64 UTF-8），重复多次
D  -> 命令结束，使用 C 的 command-id
```

- `O` 由 `ConsoleHostUserInterface` 的输出路径发出；只在该命令 active id 存在时发送。
- 命令外的 prompt、输入回显发生在 `C` 前或 `D` 后，没有 active id，因此不会成为 `O`。
- 命令执行期间的交互输入（如 `Read-Host` 的提示与用户按键回显）仍在 active id 内，**目前会作为 `O` 被记录**：它属于该命令的可见输出，不是独立的 prompt。
- `C/O/D` 共用同一条 JSON Lines FIFO；service 按 command-id/代次追加，D 结束命令。
- `O` 不产生 xterm output 事件；xterm 继续消费 PTY 流。
- `D` 可携带 `dropped=true`：宿主事件队列满/关闭导致至少一个 `O` 帧被丢弃。
  service 只在 `D` 与当前 `C`（command-id + 代次）匹配时把它落到该命令上，
  标记 `incomplete=true`；不伪造退出码、不改变 `best_effort`。

## transcript 来源

- 本地 `termx-pwsh` 默认 `transcript_output_from_events=true`：PTY 输出只显示，不追加 transcript。
- `O` 来源终端的命令**只由宿主 `D` 结束**：1s 静默判定完全不生效，避免 `Start-Sleep 3` 这类长时间无输出的命令在日志里提前变成 `best_effort done` 并返回空输出。
- native/直接 console 命令没有 `O` 时，service 把 `C..D` 的 PTY 输出收进有界兜底缓冲；`D` 时若没有任何 `O`，并入 transcript。出现 `O` 则丢弃兜底，避免双写。
- 混合 host + native 的命令（例如先 `Write-Output` 再调 native 程序）一旦同时出现 `O` 与 PTY 兜底，兜底仍会被丢弃（避免重复），但该命令会标记 `incomplete=true`：native 段文本可能缺失。`incomplete` 只是“文本可能不完整”的显式标记，不改变命令状态/退出码。
- 事件通道结束（宿主退出/管道断开）时，service 把该终端切回非事件输出模式（`output_from_events=false`）：不会再有 `O`/`D`，下一次 idle tick 的静默判定可提升已收集的 PTY 兜底。
- 旧端/显式 `TERMX_PWSH_PTY_TRANSCRIPT=1` 才走旧 PTY transcript + `allow_late_output`，不是新端正常路径；该变量是 **service 进程级**开关，影响该进程后续创建的所有终端，不是单终端参数。
- TUI/全屏程序仍由 PTY 显示；屏幕重绘不是文本，不进入 transcript，也不伪装成文本输出。

## 模型输出

`exec` 默认提交后最多同步等待 `exec.timeoutMs`（默认 5s，可用 `--timeout ms` 覆盖；`--async` 立即返回）。`-w` 已移除。

完成判定：transcript 代次推进、最后一条就是本次发送的命令（文本一致，且命令数增加或 `started_at` 变化，避免同名上一条/被 `X` 撤销的 pending 冒充），且 `status != "running"`；`O` 来源终端只由 `D` 结束（静默判定不生效），cmd/SSH/旧路径的静默判定（`best_effort=true`）仍算完成，否则没有真实 `D` 的路径只能等到超时。

- 正常成功且有输出：只返回输出正文，不带 command/id/cursor；
- 正常成功但输出为空：本地化「命令已完成，没有输出。」；
- `best_effort=true`：首行 `[termx exec] state=done best_effort=true`，空行后输出（或空输出提示），明确完成判定不确定；
- `incomplete=true`：正常成功也加首行 `[termx exec] state=done incomplete=true`；与失败/`best_effort` 同时存在时追加在同一首行（`exit_code=N incomplete=true` / `best_effort=true incomplete=true`），空行后输出；`read` 对应 `incomplete=true` 首行字段与 `[incomplete]` 结果块标记；
- 错误/中断/非零退出：首行 `[termx exec] state=error|interrupted exit_code=N`，空行后输出；
- 超时仍在运行：本地化「运行超过 N 秒，未中断，用 status/read 继续查」；不伪造完成，也不杀进程；
- 无匹配结果 / 等待期间断开：本地化提示。

`exec` 不再返回 JSON 特殊结果，也没有 `output_type` 信封。

`read` 默认只读当前/最后一条命令：

- busy 时先最多等待 `exec.timeoutMs`（`--timeout ms` 覆盖），完成则直接返回结果；
- 仍 busy 且未显式 `-s buffer`：只返回 `[termx read] ... state=busy` 状态首行，不给 buffer 快照；
- `-s buffer`：显式读取当前命令实时快照；空闲时回退到最后一条已完成命令；
- `-s history`：读最后一条已完成命令；busy 时返回 `source=history state=busy note=completed-only` 首行 + 最后一条已完成命令的输出（没有则本地化「暂时没有输出。」），绝不把运行中命令当结果；
- 正常 idle 结果只返回输出正文（`--includeCommand` 可在前面加 `$ command`），空输出返回本地化「暂时没有输出。」；
- busy/超时/error/`best_effort` 才显示 `[termx read] terminal=... source=... state=... note=...` 首行，空行后是输出/快照；失败判定（`error`/`interrupted`/非零退出码）优先于 `best_effort`；
- `-s` 与旧 `--cursor/--max-bytes/--tail-lines/--tail-chars` 互斥：同时给出直接报参数冲突，不静默忽略 `-s`。

`read` 不再合并多条历史命令，也不再默认返回 cursor 或浏览器 URL；旧 `--cursor/--max-bytes/--tail-lines/--tail-chars` 仍保留为显式读取路径（不能与 `-s` 混用）。

## 已知限制

- 宿主事件队列是有界的（`BlockingCollection` 容量 1024）。队列满时 `TryAdd` 失败并丢弃该帧（含 O 文本），不阻塞提示符、也不无限增长；宿主在下一条 `D` 上带 `dropped=true`，service 把该命令标记为 `incomplete=true`。极端突发输出仍可能缺少尾部文本，可通过 `terminal.read` 的 PTY raw 流补看。事件管道断开后 `_pendingOutput` 在 `Run()` 退出时清零并唤醒 drain 等待者，后续命令不再空等满 2s。
- `D` 的 O 队列 drain 最多等 2s，并且与入队 `D` 一起在 `_commandLock` 内完成：其他线程不能在 drain 后、D 前再入队 O；超时只影响顺序保证，D 仍会入队。
- service 侧 PTY 顺序屏障失败/超时只记录警告并仍然应用 `D`：命令边界与退出码不丢，同时把该命令标记为 `incomplete=true`。
- 命令执行中的交互输入（如 `Read-Host` 的提示与按键回显）仍在 active id 内，会作为 `O` 记录；它们属于该命令的可见输出，不是独立 prompt。
- 远端 helper 事件通道目前不转发 `O` 的 `data`：远端结构化终端的 transcript 仍走 PTY 兜底路径，`incomplete` 只反映本地 `O`/PTY 混合与丢弃。
- 单条命令 transcript 输出上限 64 KiB；超限时保留前段并追加 `[output truncated]`，`incomplete` 不会因此自动置位（截断标记自身即证据）。

## 已知残余风险（不伪造状态）

- 混合 host + native、宿主队列丢弃、屏障失败/超时都会把命令标记为 `incomplete=true`，表示 transcript 文本**可能不完整**；不写 `best_effort`、不伪造退出码，命令状态仍来自真实 `D`。
- 需要完整文本时以 PTY raw 流（`terminal.read`）为准，它保留 xterm 实际收到的字节。

## 范围

- 覆盖：PowerShell host 输出（`Write-Output`/`Write-Host`/错误流）、可捕获的 native 文本（PTY 兜底）。
- 不覆盖：TUI 全屏重绘作为文本、主动脱离当前 console/新 pty、其他句柄。
- 不向 stdout/OSC 注入任何 marker；不使用 sleep/固定窗口。

## 验证

- 无 sleep 矩阵 10/10，`exitCode=0`，尾标记完整。
- 自定义 prompt 不进 transcript。
- native `cmd /c echo native-hello` transcript 有文本。
- 混合 `Write-Output host; cmd /c echo native`：`incomplete=true`，且 host/native 文本不重复。
- 纯 `Write-Output` 与纯 native 命令均 `incomplete=false`。
- 10 KiB 输出完整。
- `cargo test`、C# Release、插件 typecheck。
