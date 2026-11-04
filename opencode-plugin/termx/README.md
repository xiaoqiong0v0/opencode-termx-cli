# @xiaoqiong0v0/opencode-termx

opencode 插件：通过本机 `termx-service` 提供单个 **`termx_cli`** 终端工具（本地 PTY / stdio / SSH），事件与进程状态由 service 负责。参数只有 `{ args: string }`：一条完整命令行，首 token 为子命令。

## 前置与配置

- 默认配置：`~/.config/opencode/termx.jsonc`（首次运行自动生成模板；`TERMX_CONFIG` 可覆盖路径）。
- 插件按配置自动启动 `termx-service`（显式传 `--config-dir/--port/--token-file/--log-*`），
  配置目录默认在 `~/.opencode/plugins-cache/@xiaoqiong0v0/opencode-termx/termx-service/config`。
- 若设置 `service.endpoint` 则直接连接、不自动启动；`service.autostart=false` 时只复用已运行实例。
- 常用环境覆盖：`TERMX_CONFIG_DIR`、`TERMX_ENDPOINT`、`TERMX_TOKEN`、`TERMX_TOOL_LANG`（en/zh）。
- Web：默认仅本机 `http://127.0.0.1:<web.port>`（回环免登录，`web.port=0` 不启动）；远程需 `web.allowRemote=true` + `web.password`，登录页 `/login`。
- opencode 会自动安装插件依赖（`.opencode/node_modules`）。

## 工具

只暴露 `termx_cli`，`args` 为完整命令行字符串。执行 `termx_cli help` 获得完整用法（en/zh 跟随 `toolLang`）。

常用子命令：

- `local ["<cmd>"]` / `ssh <user@host[:port]>`：创建常驻终端（`-n` 命名，默认 `default`）。
- `exec "<command>" [-w]`：向已存在终端发送命令；`-w` 等待约 1 秒静默后返回新增输出（非精确命令边界）。
- `run "<cmd>" [--pty]`：一次性创建、等待终态，返回输出与退出码。
- `read [-n name]` / `send "<text>"` / `status` / `list` / `terminate` / `prune` 等：管理与读写终端。
- `rebind --target-root <id>`：迁移绑定到其它 opencode 会话，必须经用户批准。

分词规则与旧 `term_cli` 一致：支持单/双引号与空引号，未闭合引号直接报错。

## 日志

插件使用 `@xiaoqiong0v0/opencode-plugin-logger`；日志开关见 `~/.config/opencode/plugin-logger.jsonc` 的 `enabled`（默认关闭），并透传给 service。

service 文件日志默认写到 `~/.opencode/plugins-cache/@xiaoqiong0v0/opencode-termx/termx-service/service-YYYY-MM-DD.log`，保留天数由配置 `log.maxDays`（默认 7）控制。

## 安全

- 只连接本机 `127.0.0.1` 的 service；token 从配置目录读取，不打印。
- 不记录终端原始输入/输出；错误日志不含密码/口令。
