# termx 框架重构方案：Runtime 自带 + 下拉选择

> 状态：设计提案，未改代码目录。目标是把“从 PTY 猜测语义”改成“运行时自己上报语义”。
>
> P1 已落地最小闭环：`crates/termx-runtime` 读取 `<config_dir>/runtimes/index.json`，服务端提供只读 `runtime.list`，`terminal.create.runtime_id` 解析 manifest 的 `bin/args`；Web 新建终端与 `termx-cli local --runtime` 已接入。**不下载、不远程传输**，只使用本机已有文件。

## 1. 目标

- 终端启动时不依赖系统 shell；从 Runtime 下拉框选择自带运行时。
- Runtime 按平台/版本单独发布、按需下载/上传、缓存复用，**不自动删除**。
- 命令边界、退出码、cwd 由运行时上报（OSC 133/633），不再靠 1s 静默/提示符正则。
- 不再把 marker 拼进用户命令；raw/history 不出现内部标记。
- 本地和远程（SSH）使用同一套 Runtime 协议。
- 没有 Runtime 或 hook 丢失时显式降级 `best_effort`，不伪造 `done`。

## 2. 核心原则

1. **语义由运行时产生**：屏幕、光标、prompt、命令、exit 由运行时/终端仿真器维护。
2. **命令文本不注入**：禁止命令内 marker；history/raw 必须干净。
3. **自带依赖**：不假设目标机器有指定 shell/版本；Runtime 独立发布。
4. **缓存优先**：同 artifact hash 已存在就复用；升级安装新版本目录，不删旧版本。
5. **可验证**：hash + 签名校验通过才执行；未校验不运行。
6. **降级诚实**：hook/OSC 缺失时标 `best_effort`，不假装精确。

### 2.1 二级执行：先不拆

- 嵌套 shell、`bash -c`、`pwsh -Command`、子命令、pipeline 都视为**父命令的一部分**。
- 不递归注入 hook/marker，不为子 shell 生成独立 transcript 记录。
- 顶层命令的 `exit_code` 取父命令/管道最终结果；子命令内部状态不追踪。
- 交互式子 shell 继续运行期间，父命令保持 `running`；退出后按顶层结果收尾。
- 该范围与旧项目一致；后续如需递归追踪，另立阶段。

## 3. 架构分层

```text
Web / CLI / opencode plugin
          │  runtime_id + create/send/transcript
          ▼
     termx-service
       ├─ session/PTY/SSH 管理
       ├─ runtime catalog/download/cache/transfer
       ├─ OSC 事件解析 → transcript 状态机
       └─ VT 屏幕/raw 输出
          │
          ▼
     自带 Runtime（termx-pwsh / termx-shell wrapper / system fallback）
          │  OSC 133/633: prompt/command/exit/cwd
          ▼
       PTY / SSH
```

## 4. Runtime Artifact

### 4.1 产物 ID

```text
<kind>/<version>/<os>-<arch>[-<libc>]
pwsh/7.6.9/win-x64
pwsh/7.6.9/linux-x64-gnu
pwsh/7.6.9/linux-arm64-musl
shell/1.0.0/linux-x64-gnu
```

### 4.2 Manifest 字段

P1 使用本地最小 manifest（`<config_dir>/runtimes/index.json`）：

```json
{
  "runtimes": [
    {
      "id": "termx-pwsh",
      "name": "termx-pwsh",
      "version": "0.1.0",
      "platform": "windows",
      "arch": "x86_64",
      "bin": "bin/termx-pwsh.exe",
      "args": []
    }
  ]
}
```

- `bin` 必须是 runtime 目录内的相对路径；拒绝绝对路径、`..` 与 symlink 越界。
- 启动前检查存在与可执行；缺失/不兼容/越界 → `bad_request`，未知 ID → `not_found`，不回退到请求里的自定义命令。
- P1 不包含 `sha256`/`url`/`signature`；这些字段留到下载/传输阶段。

后续完整 artifact 字段（目标形态）：

```json
{
  "id": "pwsh/7.6.9/win-x64",
  "kind": "pwsh",
  "version": "7.6.9",
  "os": "windows",
  "arch": "x86_64",
  "libc": null,
  "protocol": 2,
  "min_plugin": "0.2.0",
  "sha256": "...",
  "size": 123456789,
  "url": "https://...",
  "signature": "...",
  "bin": "pwsh.exe",
  "args": ["-NoLogo"],
  "event_protocol": "termx-osc-v1"
}
```

- 每个 artifact 单独版本、单独发布；manifest 可合并成一个索引。
- 签名使用固定公钥（嵌入插件/服务）；hash/签名失败拒绝执行。
- 不把全平台产物塞进 npm 包；按需下载到缓存。

### 4.3 缓存布局

本地：

```text
<plugin-cache>/runtimes/<artifact-id>/
  manifest.json
  .complete
  bin/...
```

远程：

```text
Linux:   $XDG_CACHE_HOME/termx/runtime/<artifact-id>/
         ~/.cache/termx/runtime/<artifact-id>/
Windows: %LOCALAPPDATA%\termx\runtime\<artifact-id>\
```

- 原子安装：临时目录 → 校验 → rename；`.complete` 标记。
- 同 id/hash 已存在直接复用；升级安装新目录。
- 默认不删除；显式 `runtime.prune` 才清理。
- 并发锁；损坏可重新下载/上传。

## 5. 本地/远程流程

### 本地
1. 插件/服务按 OS/arch 选择 artifact。
2. 缓存命中 → 启动；未命中 → 下载/校验/安装。
3. 启动自定义运行时；用户 profile 可选加载。
4. 运行时发 OSC；service 记录命令/exit/cwd。
5. 无 runtime 或启动失败 → 系统 shell + `best_effort`。

### 本地 out-of-band 事件通道（本轮）

- 仅 `runtime_id=termx-pwsh`、本地、`mode=pty`：service 在 spawn 前创建 Windows named pipe / Unix domain socket，注入 `TERMX_EVENT_ENDPOINT`/`TERMX_EVENT_TOKEN`，并关闭该终端的 in-band OSC 解析。
- `termx-pwsh` 读取 env 后清除 token 再连接；stdout 不再写 `133/633`。事件 `A/B/C/D` 走独立通道，`D` 必须匹配当前 `C` 的 command-id。
- 通道建立失败、断开或超时未连接：不阻断 shell，transcript 退化为静默 `best_effort`，不伪造 `exit_code`。
- 远端 helper 转发不在本轮：SSH 仍走现有 OSC/best-effort，不做端口转发。

### 远程 SSH
1. 探测目标 OS/arch/libc（`uname`、Windows 版本）。
2. 按目标选择 artifact；本地缓存没有则下载。
3. SFTP 上传到临时目录 → chmod → 校验 → rename 到目标缓存。
4. hash 已存在则不重复上传；不删旧版本。
5. 启动运行时；失败降级到系统 shell + `best_effort`。
6. 上传完整运行时需用户授权（首次询问/策略配置）。

## 6. Protocol v2

### `terminal.create`

```json
{
  "mode": "pty",
  "runtime_id": "pwsh/7.6.9/win-x64",
  "profile": "user",
  "cwd": "...",
  "env": {},
  "cols": 120,
  "rows": 30
}
```

- `runtime_id` 缺省 = `system`（降级）。
- `profile`: `user` / `clean`。
- service 只执行 manifest 中的 bin，不按 PATH 猜。

### `terminal.transcript`

```json
{
  "command_id": 12,
  "command": "ls",
  "started_at": "...",
  "ended_at": "...",
  "status": "done",
  "exit_code": 0,
  "source": "shell_integration",
  "best_effort": false,
  "cwd": "D:\\...",
  "output": "...",
  "output_truncated": false,
  "raw_available": true
}
```

- `source`: `shell_integration` / `process_group` / `unknown`
- 无 exit_code 时 `best_effort=true`，UI 显示 `~`。
- `hello` 增加 capabilities：`runtime_catalog` / `transcript_v2` / `vt_screen`。

### Runtime RPC
- `runtime.list`：**P1 已实现**；返回当前平台兼容项 `{id,name,version,platform,arch,available}`，不含绝对路径，admin/普通连接均可读。
- `runtime.ensure`：下载/上传/校验/复用（**未实现**）。
- `runtime.prune`：显式清理旧版本（**未实现**）。
- 认证、隔离沿用现有 service 会话模型。

## 7. UI / CLI / 插件

### Web 新建终端
下拉框：

1. 连接：本地 / SSH
2. Runtime：pwsh / bash / zsh / system（cmd 仅 system）
3. 版本：可用 artifact
4. Profile：用户配置 / 干净环境
5. 程序、cwd、env、cols/rows

P1 实际表单：Runtime 下拉固定第一项“自定义 cmd/args”；其余项来自 `runtime.list`，只显示 `available=true` 的兼容项。选中内置 runtime 时隐藏/禁用程序与参数输入并提交 `runtime_id`；无可用 runtime 时保持旧表单，不阻塞创建。未缓存/下载提示留到后续阶段。

### CLI
```text
# P1 已实现
termx-cli local --runtime termx-pwsh

# 后续
termx-cli ssh host --runtime pwsh/7.6.9/linux-x64-gnu
termx-cli runtime list
termx-cli runtime ensure <id>
```

### 插件
- `termx_cli` 不变；新增 `runtime` 参数/子命令。
- 默认 runtime 可配置；首次使用提示下载/上传。

## 8. 目标目录

目标不是一个大 Cargo 包，而是**多个独立项目/产物**，共享 Rust 库由根 Cargo workspace 管：

```text
/
  Cargo.toml                 # 只收 Rust workspace 成员
  termx-cli/                 # Cargo 二进制项目（独立发布）
  termx-service/             # Cargo 二进制项目
  termx-helper/              # Cargo 二进制项目
  termx-core/                # Cargo lib
  termx-protocol/            # Cargo lib
  termx-terminal/            # Cargo lib（VT/OSC）
  termx-runtime/             # Cargo lib（catalog/download/cache/transfer）
  termx-proc/                # Cargo lib
  termx-ssh/                 # Cargo lib
  termx-client/              # Cargo lib
  terminals/termx-pwsh/      # PowerShell fork 子模块（ConsoleHost 改造，独立构建）
  termx-bash/                # 独立 runtime 项目（Rust/C；先 wrapper，后续可 fork）
  termx-sh/                  # 独立 runtime 项目（Rust/C；能力探测 + 降级）
  termx-web/                 # 静态 Web 资源（构建时嵌入 service）
    index.html
    app.js
    app.css
    xterm.*
  opencode-plugin/termx/     # Node/npm 项目
  runtimes/
    index.json               # artifact manifest 索引
    pwsh/                    # 构建产物/发布索引（不入源码）
    bash/
    sh/
  docs/
  scripts/
  tests/
```

项目边界：

| 项目 | 技术 | 产物 |
|---|---|---|
| `termx-cli` | Rust/Cargo | `termx-cli` 二进制，独立版本 |
| `termx-service` | Rust/Cargo | service 二进制 |
| `termx-helper` | Rust/Cargo | 远端 helper |
| `termx-core` 等 | Rust/Cargo | 共享库，版本随 workspace |
| `termx-pwsh` | PowerShell fork（C#/C++） | 改造后的 `pwsh` runtime，独立版本 |
| `termx-bash` | Rust 或 C | bash 入口/runtime；先 wrapper，后评估 fork |
| `termx-sh` | Rust 或 C | sh 入口/runtime；能力不足时 best_effort |
| `termx-web` | HTML/JS/CSS | 静态资源，构建时嵌入 service |
| `opencode-plugin/termx` | TypeScript/Node | npm 包 |

说明：
- 迁移目录是独立阶段，先不移动现有代码；确认后逐项目搬。
- `termx-cli` 不再放在 `crates/` 下，而是顶层独立 Cargo 项目；根 `Cargo.toml` 只做 workspace。
- `termx-pwsh` 是 PowerShell fork 子模块，pin 到 `v7.6.9`；`origin` 指向 fork、`upstream` 保留官方仓库，不混入 Cargo workspace。宿主 OSC 改造在 `src/Microsoft.PowerShell.ConsoleHost/`。
- `termx-bash`/`termx-sh` 先做入口/wrapper/integration；是否 fork 完整 bash/sh 另立决策。
- Runtime 构建产物按平台/版本发布到 artifact 仓库，不把全平台二进制提交进 Git。

## 9. 实施阶段

- **P0**：Protocol v2 + `termx-runtime` catalog/manifest/cache 抽象；根 workspace 只保留 Rust 库/服务。
- **P1**：`termx-pwsh` PowerShell fork + Windows x64 runtime + 下拉框；无 marker、OSC 事件、真实 exit。当前已 pin `v7.6.9`、完成 ConsoleHost OSC 补丁并通过 service 集成测试；已落地本地 manifest/catalog、`runtime.list`、`runtime_id` 创建、Web/CLI 选择；下载/传输与真实 artifact 校验仍在后续阶段。
- **P2**：`termx-bash` 独立项目（先 wrapper/integration）；`termx-sh` 能力探测 + 降级；cmd system-only。
- **P3**：远程探测/artifact 传输/缓存复用；失败降级。
- **P4**：`termx-terminal` VT 屏幕渲染替换输出正则；raw/info 一致。
- **P5**：多项目目录迁移、独立版本/发布、文档、兼容旧客户端、全量测试。

## 10. 待拍板

1. 只做定制 pwsh，还是 bash/zsh 也做定制 Runtime？
2. `cmd` 接受 system-only 吗？
3. Runtime 下载/远程上传：首次询问还是允许自动？
4. 缓存保留：永不自动删，还是保留最近 N 个版本 + 显式 prune？
5. 目录迁移：按目标布局一次搬，还是先加新 crate/目录、逐步迁移？

## 11. 验收

- Runtime 下拉可选；缺 artifact 时明确提示/下载，不静默用错 shell。
- 本地/远程同 artifact hash 复用，不重复删除/上传。
- 无 marker；raw/history 干净。
- 有 Runtime/OSC 时 `exit_code` 真实；无则 `best_effort`。
- 下载/上传失败、noexec、权限不足、架构不匹配均有明确降级。
