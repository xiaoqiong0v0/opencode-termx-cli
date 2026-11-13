# @xiaoqiong0v0/opencode-termx

opencode 插件：通过本机 `termx-service` 提供单个 **`termx_cli`** 终端工具（本地 PTY / stdio / SSH），事件与进程状态由 service 负责。参数只有 `{ args: string }`：一条完整命令行，首 token 为子命令。

## 前置与配置

- 默认配置：`~/.config/opencode/termx.jsonc`（首次运行自动生成模板；`TERMX_CONFIG` 可覆盖路径）。
- 插件加载时按配置**后台预热** `termx-service`（不 await、不阻塞 opencode；已有手动实例会被探测复用），
  首次工具调用复用同一启动 Promise 并在失败时重试。启动时显式传 `--config-dir/--port/--token-file/--log-*`；
  配置目录默认在 `~/.opencode/plugins-cache/@xiaoqiong0v0/opencode-termx/termx-service/config`。
- 若设置 `service.endpoint` 则直接连接、不自动启动；`service.autostart=false` 时只复用已运行实例。
- 常用环境覆盖：`TERMX_CONFIG_DIR`、`TERMX_ENDPOINT`、`TERMX_TOKEN`、`TERMX_TOOL_LANG`（en/zh）。
- `exec.timeoutMs`（默认 `5000`）与 `exec.pollMs`（默认 `1000`）控制 `exec` 默认同步等待上限与 busy 轮询间隔。
- Web：默认仅本机 `http://127.0.0.1:<web.port>`（回环免登录，`web.port=0` 不启动）；远程需 `web.allowRemote=true` + `web.password`，登录页 `/login`。
- opencode 会自动安装插件依赖（`.opencode/node_modules`）。

## 工具

只暴露 `termx_cli`，`args` 为完整命令行字符串。执行 `termx_cli help` 获得完整用法（en/zh 跟随 `toolLang`）。

常用子命令：

- `local ["<cmd>"]` / `ssh <user@host[:port]>`：创建常驻终端（`-n` 命名，默认 `default`）。
- `exec "<command>" [--base64 | --file PATH] [--async] [--timeout ms]`：向已存在终端发送命令；默认最多同步等待 `exec.timeoutMs`（默认 5s，可用 `--timeout` 覆盖）直到本次命令 transcript 完成。正常成功只返回输出正文；空输出、错误/中断、非零退出、静默完成（`best_effort`）、输出不完整（`incomplete=true`）、超时分别用本地化提示或 `[termx exec] ...` 首行说明，超时不中断进程。`incomplete=true` 表示记录文本可能缺少 host/native 段（混合输出、事件帧被丢弃或顺序屏障失败），退出码/状态仍真实。`--async` 立即返回；`-w` 已移除。多行/多条命令文本**不会**被合并成一条命令：每条实际执行的命令都是独立 transcript 记录，`exec` 只能匹配其中一条；需要收集结果时用 `read --last N`，必要时用 `--async`。
- `read [-n name] [--last N | -s buffer|history] [--timeout ms] [--includeCommand]`：默认只读当前/最后一条命令；busy 时先等待 `exec.timeoutMs`；仍 busy 且未显式 `-s` 时只返回 `[termx read] ... state=busy`，`-s buffer` 给当前快照，`-s history` 给 `note=completed-only` 首行 + 最后一条已完成命令的输出。`--last N`（`-l N`）按块返回最后 N 条**已完成**命令（`status != running`）的输出，每块以稳定标记 `--- termx result i/N ---` 开头，标记内只放紧凑状态 flags（如 `[error exit_code=1]`、`[interrupted]`、`[best_effort]`、`[incomplete]`）；绝不包含命令文本、`$ command`、cursor、终端 URL 或浏览器 URL。正常 idle 只返回标记与输出，不显示 `[termx read]` 首行；有界等待后仍 busy 时使用 `[termx read] ... source=history state=busy note=completed-only`。输出已知不完整时首行/标记带 `incomplete=true` / `[incomplete]`。`--last` 不能与 `-s`、`--includeCommand` 或旧读取参数混用（会报参数冲突）；`N < 1` 直接报错。正常 idle 只返回输出（`--includeCommand` 加 `$ command`）；失败（error/interrupted/非零退出）优先于 `best_effort` 显示状态首行。旧 `--cursor/--max-bytes/--tail-lines/--tail-chars` 仍可用，但不能与 `-s` 或 `--last` 混用。
- `transcript`：命令记录（起止时间、状态、清洗后输出，`incomplete=true` 表示文本可能缺失）。已知限制：Read-Host 等交互提示会作为命令输出记录；远端 helper 事件暂不携带 `O` 文本（只走 PTY 路径）；单条命令输出上限 64 KiB（超限带截断标记）。
- `run "<cmd>" [--pty]`：一次性创建、等待终态；成功只返回输出正文，退出码只出现在错误/中断 JSON（`output_type:"error"|"interrupted"`）里，空输出返回 `output_type:"empty"`。
- `send "<text>" [--enter] [--base64 | --file PATH]` / `status` / `list` / `terminate` / `prune` 等：管理与写入终端。
- `rebind --target-root <id>`：迁移绑定到其它 opencode 会话，必须经用户批准。

### 载荷来源（exec / send）

三种来源互斥：内联文本（默认）、`--base64 <b64>`、`--file <path>`。

- `--base64` 允许参数里带空白/换行（先压紧再校验），只接受标准字母表且长度是 4 的倍数。
- `--file` 读**插件进程本机**的 UTF-8 文件（不是远端文件）；`~`、`~/`、`~\` 展开为 home，相对路径按插件 cwd 解析。
- 两者都严格解码：剥掉一个前导 UTF-8 BOM；非法 UTF-8 或含 NUL（多为 UTF-16/二进制）直接报错，不静默发送。
- `exec` 对 base64/file 载荷做 CRLF→LF 并去掉外沿空白（内部换行保留）；`send` 按解码后的文本原样写入（不解释转义、不归一换行）。
- 空载荷沿用“缺少命令/文本”错误；回执与错误只给来源/长度/路径，不回显载荷内容。

分词规则与旧 `term_cli` 一致：支持单/双引号与空引号，未闭合引号直接报错。

## 日志

插件使用 `@xiaoqiong0v0/opencode-plugin-logger`；日志开关见 `~/.config/opencode/plugin-logger.jsonc` 的 `enabled`（默认关闭），并透传给 service。

service 文件日志默认写到 `~/.opencode/plugins-cache/@xiaoqiong0v0/opencode-termx/termx-service/service-YYYY-MM-DD.log`，保留天数由配置 `log.maxDays`（默认 7）控制。

## 安全

- 只连接本机 `127.0.0.1` 的 service；token 从配置目录读取，不打印。
- 不记录终端原始输入/输出；错误日志不含密码/口令。
