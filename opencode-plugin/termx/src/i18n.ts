// 插件文案国际化：en/zh 字典 + 占位符替换。
//
// 工具描述只写使用契约；service 返回的原始错误文本不翻译。
// `webLang` 为后续 Web UI 预留，当前只导出解析能力。

import type { Lang } from "./config.js";

/** 英文完整用法（模型自学手册）。 */
const HELP_EN = `termx_cli: single CLI tool for termx terminals.

Usage:
  termx_cli help
  termx_cli local ["<cmd>"] [-n name] [-c cwd] [--env K=V]... [--cols N] [--rows N] [--force]
  termx_cli ssh <user@host[:port]> [-n name] [--password PW | --key PATH [--passphrase PP]] [--any-host-key] [--helper PATH | --upload-helper --helper-local PATH [--helper-sha SHA]] [--cols N] [--rows N] [--force]
  termx_cli exec "<command>" [-n name] [-w] [--timeout ms]
  termx_cli run "<cmd>" [-n name] [--pty] [--cwd dir] [--env K=V]... [--timeout ms] [--force]
  termx_cli read [-n name] [--cursor N] [--max-bytes N]
  termx_cli transcript [-n name] [--json]
  termx_cli send "<text>" [-n name] [--enter]
  termx_cli resize [-n name] --cols N --rows N
  termx_cli signal [-n name] SIGINT|SIGTERM|SIGKILL
  termx_cli status [-n name]
  termx_cli list [--limit N] [--states s1,s2]
  termx_cli attach [-n name] [--cursor N]
  termx_cli detach [-n name] [--all]
  termx_cli claim [-n name] [--force]
  termx_cli release [-n name]
  termx_cli terminate [-n name] [--all] [--force]
  termx_cli prune [--all | --states s1,s2 | --older-than N] [--basis last_activity|created_at]
  termx_cli rebind [--terminal-id ID] [-n name] [--target-root <root_session_id>] [--new-name name] [--force]

Rules:
  - Each opencode session has its own connection and can only access its own terminals.
    terminal.list/status/read/send/... never cross sessions; get terminal_id from Web or the
    original session when migrating.
  - Terminals are located by the current opencode session plus name; -n defaults to "default".
  - local/ssh create persistent terminals; exec/read/send/resize/signal/status/... address them by -n.
  - local without <cmd> starts the platform default shell. A command line is split like a shell
    word list (quotes supported) and executed directly; use "bash -lc '<command>'" for shell syntax.
  - exec sends to an existing terminal and returns after submitting. -w records a cursor, waits
    about 1s of silence (or timeout/terminal end), then returns new output. This is not an exact
    command boundary.
  - read continues from the per-terminal cursor unless --cursor is given.
  - transcript returns recorded commands (start/end time, status, cleaned output) for the
    information page; --json returns the raw structure. Prompts and command echo are removed.
  - Use -- before a positional that starts with "-".
  - rebind defaults the target to the current session (the target session pulls the terminal);
    use --terminal-id from Web or the original session. It always requires user approval;
    denial or failure returns "Rebind cancelled".`;

/** 中文完整用法。 */
const HELP_ZH = `termx_cli：termx 终端的单工具 CLI。

用法：
  termx_cli help
  termx_cli local ["<cmd>"] [-n name] [-c cwd] [--env K=V]... [--cols N] [--rows N] [--force]
  termx_cli ssh <user@host[:port]> [-n name] [--password PW | --key PATH [--passphrase PP]] [--any-host-key] [--helper PATH | --upload-helper --helper-local PATH [--helper-sha SHA]] [--cols N] [--rows N] [--force]
  termx_cli exec "<command>" [-n name] [-w] [--timeout ms]
  termx_cli run "<cmd>" [-n name] [--pty] [--cwd dir] [--env K=V]... [--timeout ms] [--force]
  termx_cli read [-n name] [--cursor N] [--max-bytes N]
  termx_cli transcript [-n name] [--json]
  termx_cli send "<text>" [-n name] [--enter]
  termx_cli resize [-n name] --cols N --rows N
  termx_cli signal [-n name] SIGINT|SIGTERM|SIGKILL
  termx_cli status [-n name]
  termx_cli list [--limit N] [--states s1,s2]
  termx_cli attach [-n name] [--cursor N]
  termx_cli detach [-n name] [--all]
  termx_cli claim [-n name] [--force]
  termx_cli release [-n name]
  termx_cli terminate [-n name] [--all] [--force]
  termx_cli prune [--all | --states s1,s2 | --older-than N] [--basis last_activity|created_at]
  termx_cli rebind [--terminal-id ID] [-n name] [--target-root <root_session_id>] [--new-name name] [--force]

规则：
  - 每个 opencode 会话使用独立连接，只能访问本会话终端；list/status/read/send 等不跨会话。
    迁移时请从 Web 或原会话获取 terminal_id。
  - 终端按当前 opencode 会话 + name 定位；-n 默认 default。
  - local/ssh 创建常驻终端；exec/read/send/resize/signal/status 等用 -n 定位。
  - local 不带 <cmd> 时启动平台默认 shell。命令行按 shell 词表规则拆词（支持引号）后直接执行；
    需要 shell 语法时用 "bash -lc '<command>'"。
  - exec 向已存在终端发送命令，提交后即返回。-w 会记录 cursor，等待约 1 秒静默
    （或超时/终态）后返回新增输出；这不是精确命令边界。
  - read 默认从该终端的增量 cursor 继续；可用 --cursor 指定。
  - transcript 返回信息页记录的命令（起止时间、状态、清洗后输出）；--json 返回原始结构。
    提示符与命令回显已由 service 去除。
  - 以 - 开头的 positional 请放在 -- 之后。
  - rebind 默认把目标设为当前会话（目标会话自己拉取）；跨会话请用从 Web/原会话取得的
    --terminal-id。必须经用户批准；拒绝或异常返回“已取消迁移”。`;

/** 英文文案（键集合的唯一定义源）。 */
const en = {
  "tool.cli.desc":
    "Single CLI tool. Pass one complete command line; run `help` for subcommands and flags.",

  "cli.help": HELP_EN,

  "cli.err.unclosedQuote": "Error: unclosed quote in args",
  "cli.err.unknownCommand": "Error: unknown subcommand: {cmd}",
  "cli.err.missingCommand": "Error: command text is required",
  "cli.err.missingText": "Error: text is required",
  "cli.err.unknownFlag": "Error: unknown option: {flag}",
  "cli.err.missingValue": "Error: option requires a value: {flag}",
  "cli.err.badNumber": "Error: invalid number for {flag}: {value}",
  "cli.err.badEnv": "Error: --env must be K=V: {value}",
  "cli.err.badTarget": "Error: invalid SSH target: {target} (expected user@host[:port])",
  "cli.err.needUser": "Error: SSH target requires a user: {target}",
  "cli.err.badAuth": "Error: use either --password or --key, not both",
  "cli.err.badHelper":
    "Error: invalid helper options (use --helper, or --upload-helper with --helper-local)",
  "cli.err.badSignal": "Error: signal must be SIGINT, SIGTERM or SIGKILL",
  "cli.err.missingColsRows": "Error: --cols and --rows are required",
  "cli.err.pruneExplicit":
    "Error: prune requires --states, --older-than or --all explicitly",
  "cli.err.allConflict": "Error: --all cannot be combined with -n",
  "cli.err.rebindTarget": "Error: rebind target session is empty",
  "cli.err.rebindNotFound":
    "Error: no running terminal matches the given name; pass --terminal-id from Web or the original session",
  "cli.err.rebindCancelled": "Rebind cancelled",
  "cli.err.connection": "Error: not connected to termx-service",

  "cli.ok.submitted": "submitted",
  "cli.ok.detachedAll": "detached={count}",
  "cli.ok.terminatedAll": "terminated={count}",
  "cli.read.reset": "[reset: {reason}]",
  "cli.transcript.empty": "(no command records)",

  "err.prefix": "termx error",
  "err.autostartDisabled":
    "termx service is unavailable and service.autostart=false (configDir={dir})",
  "err.serviceNotRunning":
    "termx service is not running: {path} was not found (start it first or set service.endpoint)",
  "err.tokenMissing":
    "termx token is missing: {path} (you can provide it with TERMX_TOKEN)",
  "err.portInvalid": "termx port file is invalid: {path} ({value})",
  "err.spawnFailed": "failed to start termx-service (bin={bin}): {message}",
  "err.serviceExited":
    "termx-service exited before becoming ready (bin={bin}, {code}); no running instance could be reused",
  "err.startTimeout":
    "timed out waiting for termx-service to start (bin={bin}, configDir={dir}, logDir={log})",
  "err.endpointInvalid": "termx endpoint is invalid: {endpoint}",
} as const;

/** 文案键。 */
export type MessageKey = keyof typeof en;

/** 中文文案；必须覆盖英文全部键。 */
const zh: Record<MessageKey, string> = {
  "tool.cli.desc": "单工具 CLI。传入一条完整命令行；执行 `help` 查看子命令与参数。",

  "cli.help": HELP_ZH,

  "cli.err.unclosedQuote": "错误：args 中存在未闭合的引号",
  "cli.err.unknownCommand": "错误：未知子命令：{cmd}",
  "cli.err.missingCommand": "错误：必须提供命令文本",
  "cli.err.missingText": "错误：必须提供文本",
  "cli.err.unknownFlag": "错误：未知参数：{flag}",
  "cli.err.missingValue": "错误：参数缺少取值：{flag}",
  "cli.err.badNumber": "错误：参数 {flag} 的数字非法：{value}",
  "cli.err.badEnv": "错误：--env 必须为 K=V：{value}",
  "cli.err.badTarget": "错误：SSH 目标非法：{target}（应为 user@host[:port]）",
  "cli.err.needUser": "错误：SSH 目标必须包含用户：{target}",
  "cli.err.badAuth": "错误：--password 与 --key 只能二选一",
  "cli.err.badHelper":
    "错误：helper 参数组合非法（用 --helper，或用 --upload-helper 搭配 --helper-local）",
  "cli.err.badSignal": "错误：signal 只能是 SIGINT、SIGTERM 或 SIGKILL",
  "cli.err.missingColsRows": "错误：必须提供 --cols 与 --rows",
  "cli.err.pruneExplicit":
    "错误：prune 必须显式给 --states、--older-than 或 --all",
  "cli.err.allConflict": "错误：--all 不能与 -n 同时使用",
  "cli.err.rebindTarget": "错误：迁移目标会话为空",
  "cli.err.rebindNotFound":
    "错误：没有匹配该名称的运行中终端；请用 --terminal-id 传入从 Web 或原会话获取的 ID",
  "cli.err.rebindCancelled": "已取消迁移",
  "cli.err.connection": "错误：未连接到 termx-service",

  "cli.ok.submitted": "已提交",
  "cli.ok.detachedAll": "detached={count}",
  "cli.ok.terminatedAll": "terminated={count}",
  "cli.read.reset": "[reset: {reason}]",
  "cli.transcript.empty": "（无命令记录）",

  "err.prefix": "termx 错误",
  "err.autostartDisabled":
    "termx service 不可用，且 service.autostart=false（configDir={dir}）",
  "err.serviceNotRunning":
    "termx service 未运行：找不到 {path}（请先启动，或设置 service.endpoint）",
  "err.tokenMissing": "termx token 缺失：{path}（可通过 TERMX_TOKEN 提供）",
  "err.portInvalid": "termx 端口文件非法：{path}（{value}）",
  "err.spawnFailed": "无法启动 termx-service（bin={bin}）：{message}",
  "err.serviceExited":
    "termx-service 在就绪前退出（bin={bin}，{code}），且未找到可复用的运行中实例",
  "err.startTimeout":
    "等待 termx-service 启动超时（bin={bin}，configDir={dir}，logDir={log}）",
  "err.endpointInvalid": "termx endpoint 非法：{endpoint}",
};

/** 翻译函数。 */
export type Translator = (
  key: MessageKey,
  vars?: Record<string, string | number>,
) => string;

/**
 * 按语言创建翻译函数。
 *
 * 未知键回退英文，再回退键名本身；占位符形式为 `{name}`。
 */
export function createI18n(lang: Lang): Translator {
  const dict = lang === "zh" ? zh : en;
  return (key, vars) => {
    const template = dict[key] ?? en[key] ?? key;
    if (!vars) return template;
    return template.replace(/\{(\w+)\}/g, (match, name: string) =>
      Object.prototype.hasOwnProperty.call(vars, name) ? String(vars[name]) : match,
    );
  };
}

/** 预留：Web UI 语言与工具语言使用同一套语言码。 */
export function resolveWebLang(lang: Lang): Lang {
  return lang === "zh" ? "zh" : "en";
}
