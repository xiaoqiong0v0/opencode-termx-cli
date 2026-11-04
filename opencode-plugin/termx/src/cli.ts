// 单工具 CLI：分词、参数解析与子命令分发。
//
// 只负责把命令行映射为 termx-service RPC；不解析终端渲染状态。
// 所有子命令都按 `(root_session_id=ctx.sessionID, name)` 定位，name 默认 default。

import createLogger from "@xiaoqiong0v0/opencode-plugin-logger";
import { parseCliArgs, splitCliArgs } from "./cli-args.js";
import type { MessageKey, Translator } from "./i18n.js";
import { fromB64 } from "./termx-rpc.mjs";
import type { TermxClient, TermxEvent } from "./termx-rpc.mjs";

const log = createLogger("opencode-termx");

/** 会话内默认终端名。 */
const DEFAULT_NAME = "default";
/** PTY 默认列数。 */
const DEFAULT_COLS = 120;
/** PTY 默认行数。 */
const DEFAULT_ROWS = 30;
/** exec -w 判定静默的阈值（毫秒）。 */
const WAIT_SILENCE_MS = 1_000;
/** exec -w 轮询间隔（毫秒）。 */
const WAIT_POLL_MS = 250;
/** run 默认等待超时（毫秒）。 */
const DEFAULT_RUN_TIMEOUT_MS = 60_000;
/** exec -w 单次读取的字节上限。 */
const WAIT_MAX_BYTES = 65_536;
/** 允许的信号。 */
const SUPPORTED_SIGNALS = new Set(["SIGINT", "SIGTERM", "SIGKILL"]);
/** 终态集合。 */
const TERMINAL_STATES = new Set(["exited", "interrupted", "error"]);
/** SSH 目标：user@host[:port]，host 支持 IPv6 方括号。 */
const SSH_TARGET_RE = /^(?:([^@]+)@)?(\[[^\]]+\]|[^:]+)(?::(\d+))?$/;

/** 权限审批输入（与 opencode ToolContext.ask 对齐）。 */
export interface AskInput {
  permission: string;
  patterns: string[];
  always: string[];
  metadata: Record<string, unknown>;
}

/** CLI 执行上下文（只依赖需要的字段）。 */
export interface CliContext {
  sessionID: string;
  ask(input: AskInput): Promise<void>;
}

/** 插件注入的依赖。 */
export interface CliDeps {
  t: Translator;
  client(ctx: CliContext): Promise<TermxClient>;
  /** 每终端增量读取游标，key 为 `rootSessionId + NUL + name`。 */
  cursors: Map<string, number>;
}

/** 可本地化的 CLI 失败。 */
interface CliFailure {
  key: MessageKey;
  vars?: Record<string, string | number>;
}

/** 参数取值类型。 */
type FlagType = "string" | "boolean" | "list";

/** 单个参数定义。 */
interface FlagDef {
  short?: string;
  type: FlagType;
}

/** 子命令参数表。 */
type FlagSpec = Record<string, FlagDef>;

/** 解析后的参数值。 */
type FlagValues = Record<string, string | boolean | string[]>;

/** 参数解析结果。 */
type FlagsResult =
  | { ok: true; values: FlagValues; positionals: string[] }
  | { ok: false; failure: CliFailure };

/** 单个数字参数解析结果。 */
type IntResult =
  | { ok: true; value: number | undefined }
  | { ok: false; failure: CliFailure };

/** 环境变量解析结果。 */
type EnvResult =
  | { ok: true; value: Record<string, string> | undefined }
  | { ok: false; failure: CliFailure };

/** PTY 尺寸解析结果。 */
type SizeResult =
  | { ok: true; cols?: number; rows?: number }
  | { ok: false; text: string };

/** SSH 目标解析结果。 */
type SshTargetResult =
  | { ok: true; user: string; host: string; port: number }
  | { ok: false; text: string };

/** 子命令执行环境。 */
interface CommandEnv {
  client: TermxClient;
  ctx: CliContext;
  t: Translator;
  deps: CliDeps;
}

/** 子命令处理函数。 */
type Handler = (tokens: string[], env: CommandEnv) => Promise<string>;

interface CreateResult {
  terminal_id: string;
  cursor: number;
}

interface SnapshotView {
  state?: string;
  cursor?: number;
  [key: string]: unknown;
}

interface StatusResult {
  snapshot?: SnapshotView;
}

interface OutputChunk {
  seq?: number;
  data?: string;
}

interface ReadResult {
  chunks?: OutputChunk[];
  cursor?: number;
  reset?: string | { reason?: string };
}

interface ListItem {
  terminal_id: string;
  root_session_id: string;
  name: string;
  state: string;
  [key: string]: unknown;
}

/** `terminal.transcript` 返回的单条命令。 */
interface TranscriptCommand {
  command: string;
  started_at: string;
  ended_at?: string | null;
  status?: string;
  output?: string;
}

/** `terminal.transcript` 结果。 */
interface TranscriptResult {
  commands?: TranscriptCommand[];
  revision?: number;
}

/** 一次读取/等待的输出快照。 */
interface CollectedOutput {
  output: string;
  cursor: number;
  state?: string;
  resetReason?: string;
}

/** 一次性 run 的终态结果。 */
interface RunOutcome extends CollectedOutput {
  exitCode?: number;
  signal?: string;
  errorCode?: string;
}

/** 终端创建参数。 */
interface CreateOptions {
  mode: "pty" | "stdio";
  cmd: string;
  args: string[];
  cwd?: string;
  env?: Record<string, string>;
  cols?: number;
  rows?: number;
  name: string;
  force: boolean;
  ssh?: Record<string, unknown>;
}

/** 全部子命令名。 */
const COMMANDS = new Set([
  "help",
  "local",
  "ssh",
  "exec",
  "run",
  "read",
  "transcript",
  "send",
  "resize",
  "signal",
  "status",
  "list",
  "attach",
  "detach",
  "claim",
  "release",
  "terminate",
  "prune",
  "rebind",
]);

/**
 * 解析一条完整命令行并分发子命令。
 *
 * @param raw termx_cli 的 args 字符串
 * @param ctx opencode 工具上下文
 * @param deps 插件依赖（连接、文案、游标）
 * @returns 面向模型的文本输出
 */
export async function handleCliCommand(
  raw: string,
  ctx: CliContext,
  deps: CliDeps,
): Promise<string> {
  const parsed = parseCliArgs(raw);
  if (parsed.unclosedQuote) return fail(deps.t, { key: "cli.err.unclosedQuote" });

  const [cmd, ...rest] = parsed.tokens;
  if (!cmd || cmd === "help") return deps.t("cli.help");
  if (!COMMANDS.has(cmd)) {
    return fail(deps.t, { key: "cli.err.unknownCommand", vars: { cmd } });
  }

  try {
    const client = await deps.client(ctx);
    const env: CommandEnv = { client, ctx, t: deps.t, deps };
    const handler = HANDLERS[cmd];
    return await handler(rest, env);
  } catch (error) {
    return fmtError(deps.t, error);
  }
}

/** 子命令分发表。 */
const HANDLERS: Record<string, Handler> = {
  local: doLocal,
  ssh: doSsh,
  exec: doExec,
  run: doRun,
  read: doRead,
  transcript: doTranscript,
  send: doSend,
  resize: doResize,
  signal: doSignal,
  status: doStatus,
  list: doList,
  attach: doAttach,
  detach: doDetach,
  claim: doClaim,
  release: doRelease,
  terminate: doTerminate,
  prune: doPrune,
  rebind: doRebind,
};

/** 输出本地化失败并附完整用法。 */
function fail(t: Translator, failure: CliFailure): string {
  return `${t(failure.key, failure.vars)}\n\n${t("cli.help")}`;
}

/** 统一错误文本（只输出 message/code，不带原始参数）。 */
function fmtError(t: Translator, error: unknown): string {
  const err = error as
    | { code?: string; message?: string; terminalId?: string }
    | undefined;
  const message = err?.message || String(error);
  const suffix = err?.terminalId ? ` (terminal_id=${err.terminalId})` : "";
  log.error("termx cli 错误", message);
  return `${t("err.prefix")}${err?.code ? `[${err.code}]` : ""}: ${message}${suffix}`;
}

/**
 * 解析子命令参数。
 *
 * 支持 `--flag value`、`--flag=value`、`-n value`、`--bool` 与 `--` 终止符；
 * `list` 类型可重复出现并收集为数组。
 */
function parseFlags(tokens: string[], spec: FlagSpec): FlagsResult {
  const shortToLong = new Map<string, string>();
  for (const [long, def] of Object.entries(spec)) {
    if (def.short) shortToLong.set(def.short, long);
  }

  const values: FlagValues = {};
  const positionals: string[] = [];
  let stopped = false;

  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (stopped || token === "-" || !token.startsWith("-")) {
      positionals.push(token);
      continue;
    }
    if (token === "--") {
      stopped = true;
      continue;
    }

    const consumed = consumeFlag(tokens, i, spec, shortToLong, values);
    if (!consumed.ok) return consumed;
    i = consumed.nextIndex;
  }

  return { ok: true, values, positionals };
}

/** 消费一个参数（含其取值）；返回下一个待处理下标。 */
function consumeFlag(
  tokens: string[],
  index: number,
  spec: FlagSpec,
  shortToLong: Map<string, string>,
  values: FlagValues,
): { ok: true; nextIndex: number } | { ok: false; failure: CliFailure } {
  const token = tokens[index];
  const parsed = parseFlagToken(token, shortToLong);
  if (!parsed) {
    return { ok: false, failure: { key: "cli.err.unknownFlag", vars: { flag: token } } };
  }
  const def = spec[parsed.name];
  if (!def) {
    return {
      ok: false,
      failure: { key: "cli.err.unknownFlag", vars: { flag: `--${parsed.name}` } },
    };
  }

  if (def.type === "boolean") {
    values[parsed.name] = parsed.inline === undefined ? true : parsed.inline === "true";
    return { ok: true, nextIndex: index };
  }

  let value = parsed.inline;
  let nextIndex = index;
  if (value === undefined) {
    const next = tokens[index + 1];
    if (next === undefined || (next.startsWith("-") && next !== "-")) {
      return {
        ok: false,
        failure: { key: "cli.err.missingValue", vars: { flag: `--${parsed.name}` } },
      };
    }
    value = next;
    nextIndex = index + 1;
  }

  if (def.type === "list") {
    const current = values[parsed.name];
    values[parsed.name] = Array.isArray(current) ? [...current, value] : [value];
  } else {
    values[parsed.name] = value;
  }
  return { ok: true, nextIndex };
}

/** 拆分一个参数 token；未知短参数返回 null。 */
function parseFlagToken(
  token: string,
  shortToLong: Map<string, string>,
): { name: string; inline?: string } | null {
  if (token.startsWith("--")) {
    const eq = token.indexOf("=");
    return eq >= 0
      ? { name: token.slice(2, eq), inline: token.slice(eq + 1) }
      : { name: token.slice(2) };
  }
  const long = shortToLong.get(token.slice(1));
  return long ? { name: long } : null;
}

/** 取字符串参数。 */
function textValue(values: FlagValues, key: string): string | undefined {
  const value = values[key];
  return typeof value === "string" ? value : undefined;
}

/** 取布尔参数。 */
function boolValue(values: FlagValues, key: string): boolean {
  return values[key] === true;
}

/** 取可重复参数。 */
function listValue(values: FlagValues, key: string): string[] {
  const value = values[key];
  return Array.isArray(value) ? value : [];
}

/** 读取可选整数参数。 */
function readInt(values: FlagValues, key: string): IntResult {
  const raw = textValue(values, key);
  if (raw === undefined) return { ok: true, value: undefined };
  const value = Number(raw);
  if (!Number.isInteger(value)) {
    return {
      ok: false,
      failure: { key: "cli.err.badNumber", vars: { flag: `--${key}`, value: raw } },
    };
  }
  return { ok: true, value };
}

/** 把 `K=V` 列表转为环境变量 map。 */
function envMap(items: string[]): EnvResult {
  if (!items.length) return { ok: true, value: undefined };
  const out: Record<string, string> = {};
  for (const item of items) {
    const index = item.indexOf("=");
    if (index <= 0) {
      return { ok: false, failure: { key: "cli.err.badEnv", vars: { value: item } } };
    }
    out[item.slice(0, index)] = item.slice(index + 1);
  }
  return { ok: true, value: out };
}

/** 取会话内名称；缺省 default。 */
function nameOf(values: FlagValues): string {
  return textValue(values, "name") || DEFAULT_NAME;
}

/** 当前 opencode 会话 ID（与心跳使用同一键）。 */
function rootId(ctx: CliContext): string {
  return ctx.sessionID || process.env.TERMX_ROOT_SESSION_ID || "default";
}

/** 构造终端定位参数。 */
function locator(ctx: CliContext, name: string): Record<string, string> {
  return { root_session_id: rootId(ctx), name };
}

/** 增量游标 Map 的键。 */
function cursorKey(ctx: CliContext, name: string): string {
  return `${rootId(ctx)}\u0000${name}`;
}

/** 解析命令行词表：单个 positional 视为整条命令字符串再分词。 */
function commandWords(positionals: string[]): { cmd: string; args: string[] } | null {
  if (!positionals.length) return null;
  const words = positionals.length === 1 ? splitCliArgs(positionals[0]) : positionals;
  if (!words.length || !words[0]) return null;
  return { cmd: words[0], args: words.slice(1) };
}

/** 平台默认 shell。 */
function defaultShell(): string {
  if (process.platform === "win32") return process.env.COMSPEC || "cmd.exe";
  return process.env.SHELL || "/bin/sh";
}

/** 创建终端并返回 id/游标。 */
async function createTerminal(
  client: TermxClient,
  ctx: CliContext,
  options: CreateOptions,
): Promise<CreateResult> {
  const pty = options.mode === "pty";
  const params: Record<string, unknown> = {
    mode: options.mode,
    cmd: options.cmd,
    args: options.args,
    cwd: options.cwd ?? null,
    env: options.env ?? null,
    cols: pty ? options.cols ?? DEFAULT_COLS : null,
    rows: pty ? options.rows ?? DEFAULT_ROWS : null,
    root_session_id: rootId(ctx),
    name: options.name,
    force: options.force,
    idle_timeout: null,
  };
  if (options.ssh) params.ssh = options.ssh;
  return client.request<CreateResult>("terminal.create", params);
}

/** local：创建常驻本地 PTY。 */
async function doLocal(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    cwd: { short: "c", type: "string" },
    env: { type: "list" },
    cols: { type: "string" },
    rows: { type: "string" },
    force: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const size = resolveSize(parsed.values, env.t);
  if (!size.ok) return size.text;
  const environment = envMap(listValue(parsed.values, "env"));
  if (!environment.ok) return fail(env.t, environment.failure);

  const words = commandWords(parsed.positionals);
  const name = nameOf(parsed.values);
  const created = await createTerminal(env.client, env.ctx, {
    mode: "pty",
    cmd: words?.cmd ?? defaultShell(),
    args: words?.args ?? [],
    cwd: textValue(parsed.values, "cwd"),
    env: environment.value,
    cols: size.cols,
    rows: size.rows,
    name,
    force: boolValue(parsed.values, "force"),
  });
  // 同名重建后旧游标失效，从新终端基线继续。
  env.deps.cursors.set(cursorKey(env.ctx, name), created.cursor);
  return `terminal_id=${created.terminal_id} cursor=${created.cursor}`;
}

/** ssh：创建常驻 SSH PTY。 */
async function doSsh(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    password: { type: "string" },
    key: { type: "string" },
    passphrase: { type: "string" },
    "any-host-key": { type: "boolean" },
    helper: { type: "string" },
    "upload-helper": { type: "boolean" },
    "helper-local": { type: "string" },
    "helper-sha": { type: "string" },
    cols: { type: "string" },
    rows: { type: "string" },
    force: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const target = resolveSshTarget(parsed.positionals, env.t);
  if (!target.ok) return target.text;
  const size = resolveSize(parsed.values, env.t);
  if (!size.ok) return size.text;

  const auth = buildSshAuth(parsed.values, env.t);
  if (typeof auth === "string") return auth;
  const helper = buildSshHelper(parsed.values, env.t);
  if (typeof helper === "string") return helper;

  const name = nameOf(parsed.values);
  const created = await createTerminal(env.client, env.ctx, {
    mode: "pty",
    cmd: "shell",
    args: [],
    cols: size.cols,
    rows: size.rows,
    name,
    force: boolValue(parsed.values, "force"),
    ssh: {
      host: target.host,
      port: target.port,
      user: target.user,
      auth,
      helper_path: helper.helperPath,
      helper_local_path: helper.helperLocalPath,
      helper_sha256: helper.helperSha256,
      upload_helper: helper.uploadHelper,
      accept_any_host_key: boolValue(parsed.values, "any-host-key"),
    },
  });
  env.deps.cursors.set(cursorKey(env.ctx, name), created.cursor);
  return `terminal_id=${created.terminal_id} cursor=${created.cursor}`;
}

/** 解析 PTY 尺寸参数。 */
function resolveSize(values: FlagValues, t: Translator): SizeResult {
  const cols = readInt(values, "cols");
  if (!cols.ok) return { ok: false, text: fail(t, cols.failure) };
  const rows = readInt(values, "rows");
  if (!rows.ok) return { ok: false, text: fail(t, rows.failure) };
  return { ok: true, cols: cols.value, rows: rows.value };
}

/** 解析 SSH 目标并校验 user。 */
function resolveSshTarget(positionals: string[], t: Translator): SshTargetResult {
  const raw = positionals[0];
  if (!raw) return { ok: false, text: fail(t, { key: "cli.err.badTarget", vars: { target: "" } }) };
  const target = parseSshTarget(raw);
  if (!target) return { ok: false, text: fail(t, { key: "cli.err.badTarget", vars: { target: raw } }) };
  if (!target.user) return { ok: false, text: fail(t, { key: "cli.err.needUser", vars: { target: raw } }) };
  return { ok: true, user: target.user, host: target.host, port: target.port };
}

/** 解析 `user@host[:port]`。 */
function parseSshTarget(
  target: string,
): { user?: string; host: string; port: number } | null {
  const match = target.match(SSH_TARGET_RE);
  if (!match || !match[2]) return null;
  const host = match[2].startsWith("[") ? match[2].slice(1, -1) : match[2];
  return {
    user: match[1],
    host,
    port: match[3] ? Number(match[3]) : 22,
  };
}

/** 构造 SSH auth；参数非法时返回已本地化的错误文本。 */
function buildSshAuth(
  values: FlagValues,
  t: Translator,
): Record<string, unknown> | string {
  const password = textValue(values, "password");
  const key = textValue(values, "key");
  if (password !== undefined && key !== undefined) return fail(t, { key: "cli.err.badAuth" });
  if (key) {
    return { Key: { path: key, passphrase: textValue(values, "passphrase") ?? null } };
  }
  return { Password: password ?? "" };
}

/** 构造 helper 参数；组合非法时返回已本地化的错误文本。 */
function buildSshHelper(
  values: FlagValues,
  t: Translator,
): {
  helperPath: string | null;
  helperLocalPath: string | null;
  helperSha256: string | null;
  uploadHelper: boolean;
} | string {
  const helper = textValue(values, "helper");
  const upload = boolValue(values, "upload-helper");
  const local = textValue(values, "helper-local");
  const sha = textValue(values, "helper-sha");
  const invalid = { key: "cli.err.badHelper" } as const;

  if (upload && helper) return fail(t, invalid);
  if (upload && !local) return fail(t, invalid);
  if (!upload && local) return fail(t, invalid);
  if (!upload && !helper && sha) return fail(t, invalid);
  if (helper !== undefined && !helper.trim()) return fail(t, invalid);

  return {
    helperPath: upload ? null : helper ?? null,
    helperLocalPath: upload ? local ?? null : null,
    helperSha256: sha ?? null,
    uploadHelper: upload,
  };
}

/** exec：向已存在终端发送命令；-w 时等待静默后返回输出。 */
async function doExec(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    wait: { short: "w", type: "boolean" },
    timeout: { type: "string" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const command = parsed.positionals.join(" ").trim();
  if (!command) return fail(env.t, { key: "cli.err.missingCommand" });
  const timeout = readInt(parsed.values, "timeout");
  if (!timeout.ok) return fail(env.t, timeout.failure);

  const name = nameOf(parsed.values);
  const target = locator(env.ctx, name);
  await claimQuietly(env.client, target);
  const baseline = await currentCursor(env.client, target);
  await env.client.request("terminal.send", { ...target, text: command, enter: true });

  if (!boolValue(parsed.values, "wait")) return env.t("cli.ok.submitted");

  const result = await collectQuietOutput(
    env.client,
    target,
    baseline,
    timeout.value ?? DEFAULT_RUN_TIMEOUT_MS,
  );
  env.deps.cursors.set(cursorKey(env.ctx, name), result.cursor);
  return formatOutput(env.t, result);
}

/** run：一次性创建终端，等待终态并返回输出/退出码。 */
async function doRun(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    pty: { type: "boolean" },
    cwd: { type: "string" },
    env: { type: "list" },
    timeout: { type: "string" },
    force: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const words = commandWords(parsed.positionals);
  if (!words) return fail(env.t, { key: "cli.err.missingCommand" });
  const timeout = readInt(parsed.values, "timeout");
  if (!timeout.ok) return fail(env.t, timeout.failure);
  const environment = envMap(listValue(parsed.values, "env"));
  if (!environment.ok) return fail(env.t, environment.failure);

  const mode = boolValue(parsed.values, "pty") ? "pty" : "stdio";
  const name = textValue(parsed.values, "name") || `run-${Date.now()}`;
  const created = await createTerminal(env.client, env.ctx, {
    mode,
    cmd: words.cmd,
    args: words.args,
    cwd: textValue(parsed.values, "cwd"),
    env: environment.value,
    cols: DEFAULT_COLS,
    rows: DEFAULT_ROWS,
    name,
    force: boolValue(parsed.values, "force"),
  });
  env.deps.cursors.set(cursorKey(env.ctx, name), created.cursor);
  const outcome = await waitForTerminal(
    env.client,
    created,
    timeout.value ?? DEFAULT_RUN_TIMEOUT_MS,
  );
  return formatRun(env.t, outcome);
}

/** read：增量读取终端 output。 */
async function doRead(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    cursor: { type: "string" },
    "max-bytes": { type: "string" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const cursor = readInt(parsed.values, "cursor");
  if (!cursor.ok) return fail(env.t, cursor.failure);
  const maxBytes = readInt(parsed.values, "max-bytes");
  if (!maxBytes.ok) return fail(env.t, maxBytes.failure);

  const name = nameOf(parsed.values);
  const target = locator(env.ctx, name);
  const start = cursor.value ?? env.deps.cursors.get(cursorKey(env.ctx, name)) ?? 0;
  const result = await readOutput(env.client, target, start, maxBytes.value);
  env.deps.cursors.set(cursorKey(env.ctx, name), result.cursor);
  return formatOutput(env.t, result);
}

/** transcript：读取信息页命令记录（命令 + 起止时间 + 状态 + 清洗后输出）。 */
async function doTranscript(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    json: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const target = locator(env.ctx, nameOf(parsed.values));
  const result = await env.client.request<TranscriptResult>(
    "terminal.transcript",
    target,
  );
  if (boolValue(parsed.values, "json")) return JSON.stringify(result);

  const commands = result.commands || [];
  if (!commands.length) return env.t("cli.transcript.empty");
  const blocks = commands.map((command) => {
    const started = clockOf(command.started_at);
    const ended = command.ended_at ? clockOf(command.ended_at) : "…";
    let block = `${started} → ${ended} [${command.status || "unknown"}]\n${command.command}`;
    if (command.output) block += `\n${command.output}`;
    return block;
  });
  return blocks.join("\n\n");
}

/** 从 RFC3339 时间取 `HH:MM:SS`；格式异常时原样返回。 */
function clockOf(timestamp: string): string {
  const match = /T(\d\d:\d\d:\d\d)/.exec(timestamp || "");
  return match ? match[1] : timestamp || "";
}

/** send：向终端写入文本。 */
async function doSend(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    enter: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const text = parsed.positionals.join(" ");
  if (!text) return fail(env.t, { key: "cli.err.missingText" });

  const target = locator(env.ctx, nameOf(parsed.values));
  await claimQuietly(env.client, target);
  await env.client.request("terminal.send", {
    ...target,
    text,
    enter: boolValue(parsed.values, "enter"),
  });
  return "ok";
}

/** resize：调整 PTY 尺寸。 */
async function doResize(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    cols: { type: "string" },
    rows: { type: "string" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const cols = readInt(parsed.values, "cols");
  if (!cols.ok) return fail(env.t, cols.failure);
  const rows = readInt(parsed.values, "rows");
  if (!rows.ok) return fail(env.t, rows.failure);
  if (cols.value === undefined || rows.value === undefined) {
    return fail(env.t, { key: "cli.err.missingColsRows" });
  }

  const target = locator(env.ctx, nameOf(parsed.values));
  return JSON.stringify(
    await env.client.request("terminal.resize", {
      ...target,
      cols: cols.value,
      rows: rows.value,
    }),
  );
}

/** signal：发送信号。 */
async function doSignal(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, { name: { short: "n", type: "string" } });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const signal = parsed.positionals[0];
  if (!signal || !SUPPORTED_SIGNALS.has(signal)) {
    return fail(env.t, { key: "cli.err.badSignal" });
  }
  const target = locator(env.ctx, nameOf(parsed.values));
  return JSON.stringify(
    await env.client.request("terminal.signal", { ...target, signal }),
  );
}

/** status：终端状态快照。 */
async function doStatus(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, { name: { short: "n", type: "string" } });
  if (!parsed.ok) return fail(env.t, parsed.failure);
  const target = locator(env.ctx, nameOf(parsed.values));
  return JSON.stringify(await env.client.request("terminal.status", target));
}

/** list：列出终端。 */
async function doList(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    limit: { type: "string" },
    states: { type: "string" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const limit = readInt(parsed.values, "limit");
  if (!limit.ok) return fail(env.t, limit.failure);
  const states = splitCsv(textValue(parsed.values, "states"));
  return JSON.stringify(
    await env.client.request("terminal.list", {
      states: states.length ? states : null,
      limit: limit.value ?? null,
    }),
  );
}

/** attach：附着并返回快照/游标。 */
async function doAttach(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    cursor: { type: "string" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const cursor = readInt(parsed.values, "cursor");
  if (!cursor.ok) return fail(env.t, cursor.failure);
  const target = locator(env.ctx, nameOf(parsed.values));
  return JSON.stringify(
    await env.client.request("terminal.attach", {
      ...target,
      cursor: cursor.value ?? null,
    }),
  );
}

/** detach：只断开客户端附着。 */
async function doDetach(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    all: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  if (boolValue(parsed.values, "all")) {
    if (textValue(parsed.values, "name") !== undefined) {
      return fail(env.t, { key: "cli.err.allConflict" });
    }
    const list = await env.client.request<ListItem[]>("terminal.list", {});
    for (const item of list || []) {
      await env.client.request("terminal.detach", { terminal_id: item.terminal_id });
    }
    return env.t("cli.ok.detachedAll", { count: (list || []).length });
  }

  const target = locator(env.ctx, nameOf(parsed.values));
  return JSON.stringify(await env.client.request("terminal.detach", target));
}

/** claim：取得输入 lease。 */
async function doClaim(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    force: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const target = locator(env.ctx, nameOf(parsed.values));
  return JSON.stringify(
    await env.client.request("terminal.claim", {
      ...target,
      force: boolValue(parsed.values, "force"),
    }),
  );
}

/** release：释放输入 lease。 */
async function doRelease(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, { name: { short: "n", type: "string" } });
  if (!parsed.ok) return fail(env.t, parsed.failure);
  const target = locator(env.ctx, nameOf(parsed.values));
  return JSON.stringify(await env.client.request("terminal.release", target));
}

/** terminate：终止终端（可全部）。 */
async function doTerminate(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    all: { type: "boolean" },
    force: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  if (boolValue(parsed.values, "all")) {
    if (textValue(parsed.values, "name") !== undefined) {
      return fail(env.t, { key: "cli.err.allConflict" });
    }
    const list = await env.client.request<ListItem[]>("terminal.list", {});
    const alive = (list || []).filter((item) => !TERMINAL_STATES.has(item.state));
    for (const item of alive) {
      await env.client.request("terminal.terminate", {
        terminal_id: item.terminal_id,
        force: boolValue(parsed.values, "force"),
      });
    }
    return env.t("cli.ok.terminatedAll", { count: alive.length });
  }

  const target = locator(env.ctx, nameOf(parsed.values));
  return JSON.stringify(
    await env.client.request("terminal.terminate", {
      ...target,
      force: boolValue(parsed.values, "force"),
    }),
  );
}

/** prune：清理终态终端。 */
async function doPrune(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    states: { type: "string" },
    "older-than": { type: "string" },
    basis: { type: "string" },
    all: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const olderThan = readInt(parsed.values, "older-than");
  if (!olderThan.ok) return fail(env.t, olderThan.failure);
  const states = splitCsv(textValue(parsed.values, "states"));
  const all = boolValue(parsed.values, "all");
  if (!all && !states.length && olderThan.value === undefined) {
    return fail(env.t, { key: "cli.err.pruneExplicit" });
  }
  const basis = textValue(parsed.values, "basis");

  return JSON.stringify(
    await env.client.request("terminal.prune", {
      states: states.length ? states : null,
      older_than:
        olderThan.value !== undefined ? { value: olderThan.value, unit: "s" } : null,
      basis: basis ?? null,
      all,
    }),
  );
}

/** rebind：经用户批准后把终端绑定迁移到当前 opencode 会话（或显式目标会话）。 */
async function doRebind(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    "terminal-id": { type: "string" },
    name: { short: "n", type: "string" },
    "target-root": { type: "string" },
    "new-name": { type: "string" },
    force: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  // 服务端只允许「目标会话自己拉取」或管理员迁移：缺省目标即当前会话。
  const targetRoot = (textValue(parsed.values, "target-root") || rootId(env.ctx)).trim();
  if (!targetRoot) return fail(env.t, { key: "cli.err.rebindTarget" });

  const explicitId = textValue(parsed.values, "terminal-id");
  const terminalId =
    explicitId ||
    (await findRunningTerminal(env.client, env.ctx, nameOf(parsed.values)));
  if (!terminalId) return fail(env.t, { key: "cli.err.rebindNotFound" });

  const newName = textValue(parsed.values, "new-name");
  const force = boolValue(parsed.values, "force");
  const approved = await approveRebind(env.ctx, {
    terminalId,
    targetRoot,
    newName: newName ?? "",
    force,
  });
  if (!approved) return env.t("cli.err.rebindCancelled");

  return JSON.stringify(
    await env.client.request("terminal.rebind", {
      terminal_id: terminalId,
      root_session_id: targetRoot,
      name: newName ?? null,
      force,
    }),
  );
}

/** 请求用户批准 rebind；拒绝或审批通道异常返回 false。 */
async function approveRebind(
  ctx: CliContext,
  params: {
    terminalId: string;
    targetRoot: string;
    newName: string;
    force: boolean;
  },
): Promise<boolean> {
  try {
    await ctx.ask({
      permission: "termx_rebind",
      patterns: [params.terminalId],
      always: [],
      metadata: {
        terminal_id: params.terminalId,
        target_root_session_id: params.targetRoot,
        new_name: params.newName,
        force: params.force,
      },
    });
    return true;
  } catch {
    return false;
  }
}

/** 按 (当前会话, name) 找到运行中的 terminal_id。 */
async function findRunningTerminal(
  client: TermxClient,
  ctx: CliContext,
  name: string,
): Promise<string | null> {
  const list = await client.request<ListItem[]>("terminal.list", {});
  const root = rootId(ctx);
  const hit = (list || []).find(
    (item) =>
      item.name === name &&
      item.root_session_id === root &&
      !TERMINAL_STATES.has(item.state),
  );
  return hit?.terminal_id ?? null;
}

/** 逗号分隔值转数组（去空、去重）。 */
function splitCsv(value: string | undefined): string[] {
  if (!value) return [];
  return [...new Set(value.split(",").map((part) => part.trim()).filter(Boolean))];
}

/** claim 失败不阻断 send（可能已持有 lease）。 */
async function claimQuietly(
  client: TermxClient,
  target: Record<string, string>,
): Promise<void> {
  try {
    await client.request("terminal.claim", { ...target, force: false });
  } catch {
    // 已在其它 owner 或终端已终态时，交给 send 返回明确错误。
  }
}

/** 取当前基线下游标（status.snapshot.cursor）。 */
async function currentCursor(
  client: TermxClient,
  target: Record<string, string>,
): Promise<number> {
  try {
    const status = await client.request<StatusResult>("terminal.status", target);
    const cursor = status?.snapshot?.cursor;
    return typeof cursor === "number" ? cursor : 0;
  } catch {
    return 0;
  }
}

/** 读取一段 output 并推进游标。 */
async function readOutput(
  client: TermxClient,
  target: Record<string, string>,
  cursor: number,
  maxBytes?: number,
): Promise<CollectedOutput> {
  const result = await client.request<ReadResult>("terminal.read", {
    ...target,
    cursor,
    max_bytes: maxBytes ?? null,
  });
  return collectChunks(result, cursor);
}

/** 轮询读取直到静默/终态/超时。 */
async function collectQuietOutput(
  client: TermxClient,
  target: Record<string, string>,
  startCursor: number,
  timeoutMs: number,
): Promise<CollectedOutput> {
  let collected: CollectedOutput = { output: "", cursor: startCursor };
  let lastOutputAt = Date.now();
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const result = await readOutput(client, target, collected.cursor, WAIT_MAX_BYTES);
    if (result.output) lastOutputAt = Date.now();
    collected = {
      output: collected.output + result.output,
      cursor: result.cursor,
      state: result.state,
      resetReason: collected.resetReason ?? result.resetReason,
    };

    collected.state = await terminalState(client, target);
    if (collected.state && TERMINAL_STATES.has(collected.state)) break;
    if (Date.now() - lastOutputAt >= WAIT_SILENCE_MS) break;
    await delay(WAIT_POLL_MS);
  }

  return collected;
}

/** 单次 read 响应转文本。 */
function collectChunks(result: ReadResult, fallbackCursor: number): CollectedOutput {
  let output = "";
  const chunks = Array.isArray(result?.chunks) ? result.chunks : [];
  for (const chunk of chunks) output += fromB64(chunk.data).toString("utf8");
  return {
    output,
    cursor: typeof result?.cursor === "number" ? result.cursor : fallbackCursor,
    resetReason: resetReasonOf(result),
  };
}

/** 提取 reset 原因。 */
function resetReasonOf(result: ReadResult): string | undefined {
  const reset = result?.reset;
  if (!reset) return undefined;
  if (typeof reset === "string") return reset;
  return reset.reason ?? "unknown";
}

/** 查询终端状态。 */
async function terminalState(
  client: TermxClient,
  target: Record<string, string>,
): Promise<string | undefined> {
  const status = await client.request<StatusResult>("terminal.status", target);
  return status?.snapshot?.state;
}

/** 一次性 run：订阅事件并等待终态。 */
async function waitForTerminal(
  client: TermxClient,
  created: CreateResult,
  timeoutMs: number,
): Promise<RunOutcome> {
  let output = "";
  const done = client.waitEvent(
    (event) =>
      event.terminal_id === created.terminal_id && TERMINAL_STATES.has(event.type),
    timeoutMs,
  );
  const off = client.onEvent((event) => {
    if (event.terminal_id !== created.terminal_id) return;
    if (event.type === "output") {
      output += fromB64(event.data as string | undefined).toString("utf8");
    } else if (event.type === "output_dropped") {
      output += `\n[output_dropped ${event.from_seq}..${event.to_seq}]`;
    }
  });

  try {
    await client.request("subscribe", {
      terminals: [{ terminal_id: created.terminal_id, cursor: created.cursor }],
    });
    const event = await done;
    return {
      output,
      cursor: created.cursor,
      state: event.type,
      exitCode: exitCodeOf(event),
      signal: typeof event.signal === "string" ? event.signal : undefined,
      errorCode: typeof event.code === "string" ? event.code : undefined,
    };
  } finally {
    off();
  }
}

/** 终态事件 → 退出码。 */
function exitCodeOf(event: TermxEvent): number | undefined {
  if (event.type === "exited" && typeof event.exit_code === "number") {
    return event.exit_code;
  }
  if (event.type === "interrupted") return 130;
  return undefined;
}

/** read/exec-w 输出格式。 */
function formatOutput(t: Translator, result: CollectedOutput): string {
  const parts: string[] = [];
  if (result.resetReason) {
    parts.push(t("cli.read.reset", { reason: result.resetReason }));
  }
  if (result.output) parts.push(stripTrailingNewline(result.output));
  parts.push(`[cursor=${result.cursor}]`);
  return parts.join("\n");
}

/** run 输出格式（附退出码/信号；一次性终端不输出 cursor）。 */
function formatRun(t: Translator, outcome: RunOutcome): string {
  const parts: string[] = [];
  if (outcome.resetReason) {
    parts.push(t("cli.read.reset", { reason: outcome.resetReason }));
  }
  if (outcome.output) parts.push(stripTrailingNewline(outcome.output));
  if (outcome.exitCode !== undefined) parts.push(`[exit=${outcome.exitCode}]`);
  else if (outcome.signal) parts.push(`[signal=${outcome.signal}]`);
  else if (outcome.errorCode) parts.push(`[error=${outcome.errorCode}]`);
  else parts.push(`[state=${outcome.state ?? "unknown"}]`);
  return parts.join("\n");
}

/** 去掉末尾一个换行（兼容 CRLF），避免 cursor 行前出现空行。 */
function stripTrailingNewline(text: string): string {
  if (text.endsWith("\r\n")) return text.slice(0, -2);
  if (text.endsWith("\n")) return text.slice(0, -1);
  return text;
}

/** 定时等待。 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
