// 插件文案国际化：en/zh 字典 + 占位符替换。
//
// 工具描述只写使用契约；service 返回的原始错误文本不翻译。
// `webLang` 为后续 Web UI 预留，当前只导出解析能力。

import type { Lang } from "./config.js";

/** 英文完整用法（模型自学手册）。 */
const HELP_EN = `termx_cli: single CLI tool for termx terminals.

Usage:
  termx_cli help
  termx_cli local ["<cmd>"] [--runtime ID] [-n name] [-c cwd] [--env K=V]... [--cols N] [--rows N] [--force]
  termx_cli ssh <user@host[:port]> [-n name] [--password PW | --key PATH [--passphrase PP]] [--any-host-key] [--helper PATH | --upload-helper --helper-local PATH [--helper-sha SHA]] [--allow-injection] [--cols N] [--rows N] [--force]
  termx_cli exec "<command>" [-n name] [--async] [--timeout ms] [--base64 | --file PATH]
  termx_cli run "<cmd>" [-n name] [--pty] [--cwd dir] [--env K=V]... [--timeout ms] [--force]
  termx_cli read [-n name] [--last N | -s buffer|history] [--timeout ms] [--includeCommand]
            [legacy: --cursor N [--max-bytes N] | --tail-lines N | --tail-chars N; not with --last/-s]
  termx_cli search [-n name] --pattern <pattern> [--nth N] [--only-matches]
  termx_cli transcript [-n name] [--json]
  termx_cli send "<text>" [-n name] [--enter] [--base64 | --file PATH]
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
  - exec sends to an existing terminal and waits up to exec.timeoutMs (default 5s; --timeout
    overrides) for that command's transcript to finish. A normal success returns only the output
    text; empty output, errors, best-effort completion and timeouts add a "[termx exec] ..." first
    line. It does not echo the command or use silence as a command boundary. --async returns
    immediately; -w has been removed. Multi-line or multiple-command text is NOT merged into one
    command: each executed command is recorded as a separate transcript record, exec may only
    match one record, and read --last N collects their results (use --async if needed).
  - exec/send payload sources: inline text (default), --base64 <b64>, or --file PATH.
    --base64 accepts standard base64 with optional whitespace/newlines and must decode as
    UTF-8; --file reads a file on the plugin host (not the remote host) and expands a leading
    ~. Both strip one leading UTF-8 BOM and reject invalid UTF-8 or NUL instead of sending;
    they are mutually exclusive. exec normalizes CRLF to LF and trims outer whitespace while
    keeping inner newlines; send writes the decoded text as-is (no escape decoding, no
    newline normalization). Payload content is never echoed in receipts or errors.
  - read returns only the current/last command output. When busy it first waits up to
    exec.timeoutMs (--timeout overrides); if still running it returns a status header only unless
    -s buffer is given. -s history reads the last completed command; -s buffer reads the live
    snapshot (idle falls back to the last completed command). --includeCommand prefixes
    "$ <command>". Normal idle results are output only; busy/timeout/error/best_effort add a
    "[termx read] ..." first line. --last N (-l N) returns the outputs of the last N completed
    commands (status != running) as blocks marked "--- termx result i/N ---"; it never includes
    command text, cursor, terminal URL or browser URL. Normal idle has no header; if still busy
    after the bounded wait the header uses note=completed-only. --last cannot be combined with
    -s, --includeCommand, or legacy flags. Legacy --cursor/--max-bytes/--tail-lines/--tail-chars
    still work but are not the default path.
  - search scans raw output backwards. --nth 1 is the last match; default returns from that
    match to the end; --only-matches returns just the matched line. No line numbers or
    absolute positions are returned, and trimmed history may make old positions unavailable.
  - transcript returns recorded commands (start/end time, status, cleaned output) for the
    information page; --json returns the raw structure. Prompts and command echo are removed.
    It is only meaningful for structured terminals (for example termx-pwsh); use read/search
    for ordinary programs and shells.
  - Use -- before a positional that starts with "-".
  - --allow-injection authorizes writing the helper/shell scripts to the remote host when
    creating the SSH terminal; it applies to this creation only and is not persisted.
  - rebind defaults the target to the current session (the target session pulls the terminal);
    use --terminal-id from Web or the original session. It always requires user approval;
    denial or failure returns "Rebind cancelled".`;

/** 中文完整用法。 */
const HELP_ZH = `termx_cli：termx 终端的单工具 CLI。

用法：
  termx_cli help
  termx_cli local ["<cmd>"] [--runtime ID] [-n name] [-c cwd] [--env K=V]... [--cols N] [--rows N] [--force]
  termx_cli ssh <user@host[:port]> [-n name] [--password PW | --key PATH [--passphrase PP]] [--any-host-key] [--helper PATH | --upload-helper --helper-local PATH [--helper-sha SHA]] [--allow-injection] [--cols N] [--rows N] [--force]
  termx_cli exec "<command>" [-n name] [--async] [--timeout ms] [--base64 | --file PATH]
  termx_cli run "<cmd>" [-n name] [--pty] [--cwd dir] [--env K=V]... [--timeout ms] [--force]
  termx_cli read [-n name] [--last N | -s buffer|history] [--timeout ms] [--includeCommand]
            [legacy: --cursor N [--max-bytes N] | --tail-lines N | --tail-chars N; not with --last/-s]
  termx_cli search [-n name] --pattern <pattern> [--nth N] [--only-matches]
  termx_cli transcript [-n name] [--json]
  termx_cli send "<text>" [-n name] [--enter] [--base64 | --file PATH]
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
  - exec 向已存在终端发送命令，默认最多同步等待 exec.timeoutMs（默认 5 秒，可用 --timeout
    覆盖）直到该命令 transcript 结束。正常成功只返回输出正文；空输出、错误、静默完成和超时
    会在首行加 “[termx exec] ...”。不回显命令，也不用静默猜测命令边界。--async 立即返回；
    -w 已移除。多行或多条命令的文本不会被 exec 合并成一条命令：每条实际执行的命令都是独立的
    transcript 记录，exec 只能匹配其中一条；需要收集结果时用 read --last N。
  - exec/send 的载荷来源：内联文本（默认）、--base64 <b64>、--file PATH。--base64 允许
    空白/换行，按标准字母表解码且结果必须是 UTF-8；--file 读取插件进程本机文件（不是远端），
    开头的 ~ 会展开。两者都剥掉一个前导 UTF-8 BOM，非法 UTF-8 或含 NUL 时报错而不发送；
    两者互斥。exec 把 CRLF 归一为 LF 并去掉外沿空白（内部换行保留）；send 按解码后的文本
    原样写入（不解释转义、不归一换行）。回执与错误信息都不回显载荷内容。
  - read 只返回当前/最后一条命令的输出。busy 时先最多等待 exec.timeoutMs（--timeout 覆盖）；
    仍在运行且未显式 -s buffer 时只返回状态首行。-s history 读最后一条已完成命令；
    -s buffer 读实时快照（空闲时回退到最后一条已完成命令）。--includeCommand 会在输出前加
    “$ <command>”。正常 idle 只返回输出；busy/超时/error/best_effort 才加 “[termx read] ...”
    首行。--last N（-l N）按块返回最后 N 条已完成命令（status != running）的输出，块标记为
    “--- termx result i/N ---”；绝不包含命令文本、cursor、终端 URL 或浏览器 URL。正常 idle
    不显示首行；等待上限后仍 busy 时首行为 note=completed-only。--last 不能与 -s、
    --includeCommand 或旧参数同时使用。legacy --cursor/--max-bytes/--tail-lines/--tail-chars
    仍可用，但不是默认路径。
  - search 从末尾倒搜原始输出；--nth 1 为最后一个匹配。默认返回该匹配到末尾；
    --only-matches 只返回匹配所在行。不返回行号或绝对位置，历史被裁剪后旧位置不保证可用。
  - transcript 返回信息页记录的命令（起止时间、状态、清洗后输出）；--json 返回原始结构。
    提示符与命令回显已由 service 去除。只有结构化终端（如 termx-pwsh）才有信息页；
    普通程序/shell 请用 read/search。
  - 以 - 开头的 positional 请放在 -- 之后。
  - --allow-injection 表示本次创建 SSH 终端时允许向远端写入 helper/shell 脚本；
    仅对本次创建生效，不做持久化保存。
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
  "cli.err.badRuntimeArgs": "Error: --runtime cannot be combined with a command",
  "cli.err.missingText": "Error: text is required",
  "cli.err.unknownFlag": "Error: unknown option: {flag}",
  "cli.err.missingValue": "Error: option requires a value: {flag}",
  "cli.err.badNumber": "Error: invalid number for {flag}: {value}",
  "cli.err.readMode":
    "Error: use exactly one of --cursor, --tail-lines or --tail-chars (and --max-bytes only with --cursor)",
  "cli.err.badSource": "Error: -s source must be buffer or history: {value}",
  "cli.err.readSourceConflict":
    "Error: -s cannot be combined with legacy --cursor/--max-bytes/--tail-lines/--tail-chars",
  "cli.err.badLast": "Error: --last must be >= 1",
  "cli.err.readLastConflict":
    "Error: --last cannot be combined with -s, --includeCommand, or legacy --cursor/--max-bytes/--tail-lines/--tail-chars",
  "cli.err.missingPattern": "Error: --pattern is required",
  "cli.err.badNth": "Error: --nth must be >= 1",
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
  "cli.err.payloadConflict": "Error: use either --base64 or --file, not both",
  "cli.err.payloadBadBase64":
    "Error: invalid base64 payload (standard alphabet, length must be a multiple of 4)",
  "cli.err.payloadBadEncoding":
    "Error: payload is not valid UTF-8 or contains NUL (likely UTF-16/binary); save it as UTF-8",
  "cli.err.payloadFileError": "Error: cannot read file: {path}",

  "exec.async":
    "Command submitted in async mode; it is still running. Use termx_cli status to check completion, then termx_cli read.",
  "exec.timeout":
    "Command still running after {seconds}s; it was not interrupted. Use termx_cli status to check completion, then termx_cli read.",
  "exec.noOutput": "Command completed with no output.",
  "exec.noResult":
    "Command finished but no matching result was recorded (terminal may have changed). Check status.",
  "exec.disconnected": "Terminal disconnected while waiting for the command result.",
  "exec.wRemoved":
    "-w has been removed. exec waits up to exec.timeoutMs by default; use --async for immediate return.",
  "read.noOutput": "No output yet.",
  "read.disconnected": "Terminal disconnected before output could be read.",

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
  "cli.err.badRuntimeArgs": "错误：--runtime 不能与命令/参数同时使用",
  "cli.err.missingText": "错误：必须提供文本",
  "cli.err.unknownFlag": "错误：未知参数：{flag}",
  "cli.err.missingValue": "错误：参数缺少取值：{flag}",
  "cli.err.badNumber": "错误：参数 {flag} 的数字非法：{value}",
  "cli.err.readMode":
    "错误：--cursor、--tail-lines、--tail-chars 只能三选一（--max-bytes 仅配合 --cursor）",
  "cli.err.badSource": "错误：-s 来源只能是 buffer 或 history：{value}",
  "cli.err.readSourceConflict":
    "错误：-s 不能与旧参数 --cursor/--max-bytes/--tail-lines/--tail-chars 同时使用",
  "cli.err.badLast": "错误：--last 必须 >= 1",
  "cli.err.readLastConflict":
    "错误：--last 不能与 -s、--includeCommand 或旧参数 --cursor/--max-bytes/--tail-lines/--tail-chars 同时使用",
  "cli.err.missingPattern": "错误：必须提供 --pattern",
  "cli.err.badNth": "错误：--nth 必须 >= 1",
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
  "cli.err.payloadConflict": "错误：--base64 与 --file 只能二选一",
  "cli.err.payloadBadBase64":
    "错误：base64 载荷不合法（标准字母表，长度须为 4 的倍数）",
  "cli.err.payloadBadEncoding":
    "错误：载荷不是合法 UTF-8 或含 NUL（可能是 UTF-16/二进制）；请保存为 UTF-8",
  "cli.err.payloadFileError": "错误：无法读取文件：{path}",

  "exec.async":
    "命令已按异步模式提交，仍在该终端运行；用 termx_cli status 查完成状态，再用 termx_cli read 读取输出。",
  "exec.timeout":
    "命令运行超过 {seconds} 秒仍未结束（未中断）；用 termx_cli status 查完成状态，再用 termx_cli read 读取输出。",
  "exec.noOutput": "命令已完成，没有输出。",
  "exec.noResult":
    "命令已结束，但没有记录到对应结果（终端状态可能已变化）。请用 status 检查。",
  "exec.disconnected": "等待命令结果时终端已断开。",
  "exec.wRemoved":
    "-w 已移除。exec 默认最多等待 exec.timeoutMs；需要立即异步请用 --async。",
  "read.noOutput": "暂时没有输出。",
  "read.disconnected": "读取输出前终端已断开。",

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
