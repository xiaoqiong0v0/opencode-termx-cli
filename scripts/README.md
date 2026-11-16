# scripts 工具索引

| 工具 | 用途 | 用法 | 依赖 |
| --- | --- | --- | --- |
| `build-opencode-plugin.mjs` | 把插件源码构建到 opencode 自动发现目录（`.opencode/plugins`） | `node scripts/build-opencode-plugin.mjs` | node（依赖插件目录内 esbuild） |
| `ci.ps1` | 本机 CI 门禁（fmt/clippy/test，与 `.github/workflows/ci.yml` 一致） | `pwsh scripts/ci.ps1` | cargo |
| `termx-plugin-selftest.mjs` | 插件传输层自测：连 service 跑 list/exec/status/terminate | `node scripts/termx-plugin-selftest.mjs <config-dir>`（或 `TERMX_CONFIG_DIR` / `TERMX_ENDPOINT`） | node |
| `run-termx-e2e.ps1` | service e2e 统一入口：构建 → 逐套件启动 termx-service 跑 e2e → 按套件隔离 config 并收尾 | `pwsh scripts/run-termx-e2e.ps1 [-Suite payload\|read-last\|incomplete\|all] [-Port 8170] [-TargetDir <dir>] [-RuntimeSource <dir>] [-SkipBuild]` | cargo、node、npm；另需一份 runtime catalog 部署（`.tmp/termx-e2e-runtime`，含 `index.json` + `termx-pwsh/`，首次使用放置或用 `-RuntimeSource` 指向已有部署） |

## scripts/e2e/

service 级 e2e 断言脚本，由 `run-termx-e2e.ps1` 驱动（传 `<config-dir>`），也可单独手动跑（需先起 service）：

| 脚本 | 用途 | 依赖 |
| --- | --- | --- |
| `e2e/payload-e2e.mjs` | exec/send 的 `--base64`/`--file` 载荷来源、冲突与编码错误（`node scripts/e2e/payload-e2e.mjs <config-dir>`） | node、插件 dist（先 `npm run build`） |
| `e2e/read-last-e2e.mjs` | `read --last N` 多命令结果块、泄漏防护、参数校验与 busy 等待上界 | node、插件 dist（先 `npm run build`） |
| `e2e/incomplete-e2e.mjs` | 混合/纯 O/纯 native 输出时 transcript 的 incomplete 标记语义 | node、插件 dist（先 `npm run build`） |
