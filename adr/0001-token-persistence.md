# ADR 0001：token 持久化、首次生成与显式轮换

- 状态：已采纳
- 日期：2026-11-02
- 关联：`docs/PROTOCOL.md` §3、`crates/termx-service/src/config.rs`

## 背景

`termx-service` 仅监听环回地址，用 token 防止本机其他进程随意连接。需要回答三个问题：

1. token 是否在服务重启后保持不变；
2. 首次启动如何生成；
3. 需要更换 token 时如何操作。

## 决策

- **持久化**：token 写入用户配置目录下的 token 文件（Linux/macOS 默认
  `$XDG_CONFIG_HOME/termx/token` 或 `~/.config/termx/token`，Windows 默认
  `%APPDATA%\termx\token`）。服务重启后复用同一 token，客户端无需每次重配。
- **首次生成**：token 文件不存在或内容为空时，生成 32 字节随机数的十六进制串
  （64 字符）并写盘；Unix 权限收紧到 `0600`。
- **发现优先级**：`--token-file <path>` > `TERMX_TOKEN` 环境变量 > 默认 token 文件。
  显式文件与环境变量用于临时/受控部署，均不改变默认文件内容。
- **显式轮换**：删除或替换 token 文件后重启服务即可轮换；不提供隐式自动轮换。
  旧 token 在服务重启加载新文件后失效。
- **原子写入**：先写同目录临时文件并 `sync_all`，再 rename 覆盖目标文件，避免
  并发读取到半截 token。

## 理由

- 常驻服务与客户端分离，token 持久化才能支持客户端重连与 opencode 重启。
- 本机环回 + token 已能覆盖 v0 威胁模型；自动轮换会增加客户端协调成本。
- 原子写入避免崩溃或并发启动时留下不完整 token 文件。
- Windows 上 v0 依赖环回绑定 + token；文件 ACL 收紧列为后续项，当前不宣称已实现。

## 影响

- 客户端需从同一 token 文件或环境变量获取 token。
- 轮换需要显式删除/替换文件并重启服务。
- 后续若要支持多用户或跨机访问，需要重新评估 token 生命周期与 ACL。
