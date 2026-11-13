// 单工具 CLI：分词、参数解析与子命令分发。
//
// 只负责把命令行映射为 termx-service RPC；不解析终端渲染状态。
// 所有子命令都按 `(root_session_id=ctx.sessionID, name)` 定位，name 默认 default。

import createLogger from "@xiaoqiong0v0/opencode-plugin-logger";
import { parseCliArgs, splitCliArgs } from "./cli-args.js";
import type { ExecConfig } from "./config.js";
import type { MessageKey, Translator } from "./i18n.js";
import { normalizeExecText, PayloadError, resolvePayload } from "./payload.js";
import type { ResolvedPayload } from "./payload.js";
import {
  formatReadHeader,
  formatReadResult,
  formatResultBlock,
  joinResultBlocks,
  lastItem,
} from "./read-output.js";
import type { ReadHeaderNote, ReadHeaderState, ReadSource } from "./read-output.js";
import { fromB64 } from "./termx-rpc.mjs";
import type { TermxClient, TermxEvent } from "./termx-rpc.mjs";
import { waitForIdle } from "./wait.js";

const log = createLogger("opencode-termx");

/** 会话内默认终端名。 */
const DEFAULT_NAME = "default";
/** PTY 默认列数。 */
const DEFAULT_COLS = 120;
/** PTY 默认行数。 */
const DEFAULT_ROWS = 30;
/** run 默认等待超时（毫秒）。 */
const DEFAULT_RUN_TIMEOUT_MS = 60_000;
/** 成功但没有任何可捕获文本时的自解释结果（仅 run 使用）。 */
const EMPTY_OUTPUT_JSON = JSON.stringify({
  output_type: "empty",
  message: "no captured command output",
  read_with: "terminal.read",
});
/** exec 首行前缀；状态字段为协议化标识，不翻译。 */
const EXEC_HEADER = "[termx exec]";

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
  /** exec/read 默认同步等待与轮询配置。 */
  exec: ExecConfig;
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

/** read 来源解析结果。 */
type SourceResult =
  | { ok: true; value?: ReadSource }
  | { ok: false; failure: CliFailure };

/** transcript 命令列表、末条命令、最后一条已完成命令与 busy 判定。 */
interface TranscriptView {
  /** 本次读取到的全部命令（按时间顺序）。 */
  commands: TranscriptCommand[];
  last?: TranscriptCommand;
  /** 最后一条 `status !== "running"` 的命令；busy 时 `-s history` 用它。 */
  lastCompleted?: TranscriptCommand;
  busy: boolean;
}

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


interface OutputChunk {
  seq?: number;
  data?: string;
}

interface ReadResult {
  chunks?: OutputChunk[];
  cursor?: number;
  reset?: string | { reason?: string };
}

/** `terminal.output` 结果：尾部截取或倒搜后的原文。 */
interface OutputResult {
  text?: string;
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
  exit_code?: number | null;
  best_effort?: boolean;
  output?: string;
}

/** `terminal.transcript` 结果。 */
interface TranscriptResult {
  commands?: TranscriptCommand[];
  revision?: number;
}

/** exec -w 发送前快照，用于确认轮询到的是本次命令而非上一条同名命令。 */
interface TranscriptBaseline {
  revision: number;
  count: number;
  last?: TranscriptCommand;
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
  runtimeId?: string;
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
  "search",
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
  search: doSearch,
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

/**
 * 载荷解析错误 → 本地化失败（不回显 base64/文件内容，文件错误只回显用户给的路径）。
 * @param error 载荷解析错误
 * @param file 用户传入的 --file 值
 * @returns 可交给 fail() 的失败描述
 */
function payloadFailure(error: PayloadError, file?: string): CliFailure {
  if (error.code === "conflict") return { key: "cli.err.payloadConflict" };
  if (error.code === "base64") return { key: "cli.err.payloadBadBase64" };
  if (error.code === "decode") return { key: "cli.err.payloadBadEncoding" };
  return { key: "cli.err.payloadFileError", vars: { path: file ?? "" } };
}

/**
 * 按 --base64 / --file 解析载荷；失败时返回已本地化的错误文本。
 * 日志只记错误码与来源，绝不记录载荷内容。
 */
async function readPayload(
  positionals: string[],
  values: FlagValues,
  t: Translator,
): Promise<{ ok: true; payload: ResolvedPayload } | { ok: false; text: string }> {
  const file = textValue(values, "file");
  try {
    const payload = await resolvePayload(positionals.join(" "), {
      base64: boolValue(values, "base64"),
      file,
    });
    return { ok: true, payload };
  } catch (error) {
    if (error instanceof PayloadError) {
      log.warn("termx 载荷解析失败", error.code);
      return { ok: false, text: fail(t, payloadFailure(error, file)) };
    }
    throw error;
  }
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
  if (options.runtimeId) params.runtime_id = options.runtimeId;
  if (options.ssh) params.ssh = options.ssh;
  return client.request<CreateResult>("terminal.create", params);
}

/** local：创建常驻本地 PTY。 */
async function doLocal(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    cwd: { short: "c", type: "string" },
    env: { type: "list" },
    runtime: { type: "string" },
    cols: { type: "string" },
    rows: { type: "string" },
    force: { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const runtimeId = textValue(parsed.values, "runtime");
  const size = resolveSize(parsed.values, env.t);
  if (!size.ok) return size.text;
  const environment = envMap(listValue(parsed.values, "env"));
  if (!environment.ok) return fail(env.t, environment.failure);

  const words = commandWords(parsed.positionals);
  if (runtimeId && words) {
    return fail(env.t, { key: "cli.err.badRuntimeArgs" });
  }
  const name = nameOf(parsed.values);
  const created = await createTerminal(env.client, env.ctx, {
    mode: "pty",
    cmd: runtimeId ? "" : words?.cmd ?? defaultShell(),
    args: runtimeId ? [] : words?.args ?? [],
    cwd: textValue(parsed.values, "cwd"),
    env: environment.value,
    cols: size.cols,
    rows: size.rows,
    name,
    force: boolValue(parsed.values, "force"),
    runtimeId,
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
    "allow-injection": { type: "boolean" },
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
      // 仅本次创建生效，默认 false，不写入任何持久化状态。
      allow_injection: boolValue(parsed.values, "allow-injection"),
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

/** exec：向已存在终端发送命令，默认等待本次命令的 transcript 完成。 */
async function doExec(tokens: string[], env: CommandEnv): Promise<string> {
  // -w/--waitResult 已移除：显式拒绝，避免旧调用静默退回异步。
  if (usesRemovedWaitFlag(tokens)) {
    return `${env.t("exec.wRemoved")}\n\n${env.t("cli.help")}`;
  }
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    async: { type: "boolean" },
    timeout: { type: "string" },
    base64: { type: "boolean" },
    file: { type: "string" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const payload = await readPayload(parsed.positionals, parsed.values, env.t);
  if (!payload.ok) return payload.text;

  // 文本模式保持原行为（join + trim）；base64/file 归一 CRLF 并去掉外沿空白，
  // 否则末尾换行会在 enter 之后多提交一条空命令。
  const command =
    payload.payload.source === "text"
      ? parsed.positionals.join(" ").trim()
      : normalizeExecText(payload.payload.text);
  if (!command) return fail(env.t, { key: "cli.err.missingCommand" });
  const timeout = readInt(parsed.values, "timeout");
  if (!timeout.ok) return fail(env.t, timeout.failure);

  const waitMs = Math.max(0, timeout.value ?? env.deps.exec.timeoutMs);
  const asyncMode = boolValue(parsed.values, "async") || waitMs <= 0;
  const name = nameOf(parsed.values);
  const target = locator(env.ctx, name);

  let before: TranscriptResult;
  try {
    before = await env.client.request<TranscriptResult>("terminal.transcript", target);
  } catch (error) {
    const disconnected = goneMessage(env.t, error);
    if (disconnected) return disconnected;
    throw error;
  }
  const baseline = transcriptBaseline(before);

  await claimQuietly(env.client, target);
  try {
    await env.client.request("terminal.send", { ...target, text: command, enter: true });
  } catch (error) {
    const disconnected = goneMessage(env.t, error);
    if (disconnected) return disconnected;
    throw error;
  }

  if (asyncMode) return env.t("exec.async");

  const startedAt = Date.now();
  let current: TranscriptCommand | undefined;
  while (true) {
    let result: TranscriptResult;
    try {
      result = await env.client.request<TranscriptResult>("terminal.transcript", target);
    } catch (error) {
      const disconnected = goneMessage(env.t, error);
      if (disconnected) return disconnected;
      throw error;
    }
    const commands = result.commands || [];
    const last = commands[commands.length - 1];
    // 代次推进只说明 transcript 变了；还必须确认末条就是刚发送的命令，
    // 否则同名上一条/被 X 撤销的 pending 会被误判为完成。静默判定
    // （best_effort）也算完成，否则没有真实 D 的 cmd/SSH/旧路径会等到超时。
    if (
      (result.revision ?? 0) > baseline.revision &&
      isSentCommand(last, commands.length, command, baseline)
    ) {
      if (last && last.status !== "running") return formatExecResult(env.t, last);
      current = last;
    } else if ((result.revision ?? 0) > baseline.revision) {
      // 宿主 X/多行撤销可能先建 pending 再删除；不要再用已撤销的记录。
      current = undefined;
    }
    if (Date.now() >= startedAt + waitMs) break;
    await delay(env.deps.exec.pollMs);
  }
  if (current) {
    return env.t("exec.timeout", { seconds: elapsedSeconds(startedAt) });
  }
  return env.t("exec.noResult");
}

/** 是否显式使用了已移除的 -w/--waitResult（`--` 之后的字面量不算）。 */
function usesRemovedWaitFlag(tokens: string[]): boolean {
  const end = tokens.indexOf("--");
  const head = end >= 0 ? tokens.slice(0, end) : tokens;
  return head.includes("-w") || head.includes("--waitResult");
}

/** 取发送前的 transcript 基线。 */
function transcriptBaseline(result: TranscriptResult): TranscriptBaseline {
  const commands = result.commands || [];
  return {
    revision: result.revision ?? 0,
    count: commands.length,
    last: commands[commands.length - 1],
  };
}

/**
 * 轮询到的末条命令是否就是刚发送的那一条。
 *
 * 同名命令可能已存在于上一条记录：仅 `command` 相等不够。发送后记录数应
 * 增加（send-time pending 或宿主 `C` 都会新增）；容量满或同毫秒新增时以
 * `started_at` 变化兜底。宿主 `X` 删除 pending 时计数不增且末条仍是发送前
 * 的同名记录，因此不会回退到上一条命令。
 */
function isSentCommand(
  last: TranscriptCommand | undefined,
  count: number,
  command: string,
  baseline: TranscriptBaseline,
): boolean {
  if (!last || last.command !== command) return false;
  if (!baseline.last) return true;
  return count > baseline.count || last.started_at !== baseline.last.started_at;
}

/**
 * 判断是否确实由本次调用完成，并生成 exec 结果。
 *
 * 正常成功只返回输出正文；best_effort/错误/中断才加 `[termx exec]` 首行。
 */
function formatExecResult(t: Translator, command: TranscriptCommand): string {
  const output = stripTrailingNewline(command.output ?? "");
  const body = output || t("exec.noOutput");
  const exitCode = typeof command.exit_code === "number" ? command.exit_code : undefined;
  const failed =
    command.status === "error" ||
    command.status === "interrupted" ||
    (exitCode !== undefined && exitCode !== 0);
  if (failed) {
    const state = command.status === "interrupted" ? "interrupted" : "error";
    return `${EXEC_HEADER} state=${state} exit_code=${exitCode ?? "unknown"}\n\n${body}`;
  }
  if (command.best_effort) {
    return `${EXEC_HEADER} state=done best_effort=true\n\n${body}`;
  }
  return output || t("exec.noOutput");
}

/** 终端已不存在/已断开时的本地化文案；其它错误返回 null。 */
function goneMessage(t: Translator, error: unknown): string | null {
  if (!isGoneError(error)) return null;
  return t("exec.disconnected");
}

/** 终端被清理或连接关闭：等待结果期间的“断开”判定。 */
function isGoneError(error: unknown): boolean {
  const value = error as { code?: unknown; message?: unknown } | undefined;
  const code = typeof value?.code === "string" ? value.code : "";
  if (code === "not_found" || code === "already_terminal") return true;
  const message = typeof value?.message === "string" ? value.message : String(error);
  return message.includes("连接已关闭") || message.includes("连接错误");
}

/** 等待秒数（向上取整到至少 1 秒，用于超时提示）。 */
function elapsedSeconds(startedAt: number): number {
  return Math.max(1, Math.round((Date.now() - startedAt) / 1000));
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

/** read：默认读取当前/最后一条命令；legacy flags 保留原始读取路径。 */
async function doRead(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    source: { short: "s", type: "string" },
    includeCommand: { type: "boolean" },
    timeout: { short: "t", type: "string" },
    last: { short: "l", type: "string" },
    cursor: { type: "string" },
    "max-bytes": { type: "string" },
    "tail-lines": { type: "string" },
    "tail-chars": { type: "string" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const last = readInt(parsed.values, "last");
  if (!last.ok) return fail(env.t, last.failure);
  if (last.value !== undefined) {
    if (last.value < 1) return fail(env.t, { key: "cli.err.badLast" });
    // 多命令模式绝不写命令文本：显式拒绝所有单条读取/回显参数。
    if (hasReadLastConflict(parsed.values)) {
      return fail(env.t, { key: "cli.err.readLastConflict" });
    }
    const lastTimeout = readInt(parsed.values, "timeout");
    if (!lastTimeout.ok) return fail(env.t, lastTimeout.failure);
    const lastWaitMs = Math.max(0, lastTimeout.value ?? env.deps.exec.timeoutMs);
    const lastName = nameOf(parsed.values);
    return readLast(
      env,
      locator(env.ctx, lastName),
      lastName,
      last.value,
      lastWaitMs,
    );
  }

  const source = readSource(parsed.values);
  if (!source.ok) return fail(env.t, source.failure);
  const cursor = readInt(parsed.values, "cursor");
  if (!cursor.ok) return fail(env.t, cursor.failure);
  const maxBytes = readInt(parsed.values, "max-bytes");
  if (!maxBytes.ok) return fail(env.t, maxBytes.failure);
  const tailLines = readInt(parsed.values, "tail-lines");
  if (!tailLines.ok) return fail(env.t, tailLines.failure);
  const tailChars = readInt(parsed.values, "tail-chars");
  if (!tailChars.ok) return fail(env.t, tailChars.failure);

  const name = nameOf(parsed.values);
  const target = locator(env.ctx, name);
  const legacy =
    cursor.value !== undefined ||
    maxBytes.value !== undefined ||
    tailLines.value !== undefined ||
    tailChars.value !== undefined;
  // 旧读取参数与 `-s` 语义互斥：不静默忽略 `-s`，直接报参数冲突。
  if (legacy && textValue(parsed.values, "source") !== undefined) {
    return fail(env.t, { key: "cli.err.readSourceConflict" });
  }
  if (legacy) {
    return legacyRead(env, target, {
      cursor: cursor.value,
      maxBytes: maxBytes.value,
      tailLines: tailLines.value,
      tailChars: tailChars.value,
    });
  }

  const timeout = readInt(parsed.values, "timeout");
  if (!timeout.ok) return fail(env.t, timeout.failure);
  const waitMs = Math.max(0, timeout.value ?? env.deps.exec.timeoutMs);
  return readCurrent(
    env,
    target,
    name,
    source.value,
    boolValue(parsed.values, "includeCommand"),
    waitMs,
  );
}

/** `--last` 与单条读取/命令回显参数互斥（按原始出现判定，忽略取值是否合法）。 */
function hasReadLastConflict(values: FlagValues): boolean {
  return (
    textValue(values, "source") !== undefined ||
    boolValue(values, "includeCommand") ||
    textValue(values, "cursor") !== undefined ||
    textValue(values, "max-bytes") !== undefined ||
    textValue(values, "tail-lines") !== undefined ||
    textValue(values, "tail-chars") !== undefined
  );
}

/** 解析 `-s buffer|history`；缺省返回 undefined。 */
function readSource(values: FlagValues): SourceResult {
  const raw = textValue(values, "source");
  if (raw === undefined) return { ok: true, value: undefined };
  if (raw === "buffer" || raw === "history") return { ok: true, value: raw };
  return { ok: false, failure: { key: "cli.err.badSource", vars: { value: raw } } };
}

/** legacy：显式 cursor/tail 读取（保留旧语义，不作为默认路径）。 */
async function legacyRead(
  env: CommandEnv,
  target: Record<string, string>,
  options: {
    cursor?: number;
    maxBytes?: number;
    tailLines?: number;
    tailChars?: number;
  },
): Promise<string> {
  const tailMode = options.tailLines !== undefined || options.tailChars !== undefined;
  if (tailMode) {
    if (options.cursor !== undefined || options.maxBytes !== undefined) {
      return fail(env.t, { key: "cli.err.readMode" });
    }
    if (options.tailLines !== undefined && options.tailChars !== undefined) {
      return fail(env.t, { key: "cli.err.readMode" });
    }
    const result = await env.client.request<OutputResult>("terminal.output", {
      ...target,
      tail_lines: options.tailLines ?? null,
      tail_chars: options.tailChars ?? null,
    });
    return result.text || "";
  }

  const name = target.name;
  const start =
    options.cursor ?? env.deps.cursors.get(cursorKey(env.ctx, name)) ?? 0;
  const result = await readOutput(env.client, target, start, options.maxBytes);
  env.deps.cursors.set(cursorKey(env.ctx, name), result.cursor);
  return formatOutput(env.t, result);
}

/**
 * 读取 transcript；busy 时按同一有界等待策略等到 idle 或超时。
 *
 * 完成判定与 exec 一致：最后一条命令不再 running 即视为完成。
 */
async function loadTranscriptView(
  env: CommandEnv,
  target: Record<string, string>,
  waitMs: number,
): Promise<TranscriptView> {
  let view = await readTranscript(env.client, target);
  if (view.busy && waitMs > 0) {
    await waitForIdle(
      async () => (await readTranscript(env.client, target)).busy,
      waitMs,
      env.deps.exec.pollMs,
    );
    view = await readTranscript(env.client, target);
  }
  return view;
}

/**
 * 默认 read：先等待 busy 结束；仍 busy 时只给状态（或显式 buffer 快照）。
 *
 * 正常 idle 只返回最后一条已完成命令的输出；best_effort/error 才加首行。
 */
async function readCurrent(
  env: CommandEnv,
  target: Record<string, string>,
  name: string,
  source: ReadSource | undefined,
  includeCommand: boolean,
  waitMs: number,
): Promise<string> {
  const noOutput = env.t("read.noOutput");
  let view: TranscriptView;
  try {
    view = await loadTranscriptView(env, target, waitMs);
  } catch (error) {
    if (!isGoneError(error)) throw error;
    const header = formatReadHeader(name, source ?? "status", "error", "disconnected");
    const message = env.t("read.disconnected");
    return formatReadResult(header, message, message);
  }

  if (view.busy) {
    if (source === "buffer") {
      const snapshot = stripTrailingNewline(view.last?.output ?? "");
      const header = formatReadHeader(name, "buffer", "busy", "partial");
      return formatReadResult(header, snapshot, noOutput);
    }
    if (source === "history") {
      // busy 时 history 只给最后一条已完成命令，绝不把运行中命令当结果。
      const header = formatReadHeader(name, "history", "busy", "completed-only");
      return formatReadResult(
        header,
        commandOutput(view.lastCompleted, includeCommand, noOutput),
        noOutput,
      );
    }
    return formatReadHeader(name, "status", "busy", "timeout");
  }
  return formatIdleRead(name, includeCommand, view.last, noOutput);
}

/**
 * `--last N`：返回最后 N 条已完成命令的输出（绝不包含命令文本）。
 *
 * 与默认 read 共用同一有界等待；仍 busy 时加 `note=completed-only` 首行，
 * 只输出已完成命令。正常 idle 只给结果块，不加 `[termx read]` 首行。
 */
async function readLast(
  env: CommandEnv,
  target: Record<string, string>,
  name: string,
  last: number,
  waitMs: number,
): Promise<string> {
  const noOutput = env.t("read.noOutput");
  let view: TranscriptView;
  try {
    view = await loadTranscriptView(env, target, waitMs);
  } catch (error) {
    if (!isGoneError(error)) throw error;
    const header = formatReadHeader(name, "status", "error", "disconnected");
    const message = env.t("read.disconnected");
    return formatReadResult(header, message, message);
  }

  const completed = view.commands.filter((command) => command.status !== "running");
  const body = formatLastResults(completed.slice(-last), noOutput);
  if (view.busy) {
    const header = formatReadHeader(name, "history", "busy", "completed-only");
    return formatReadResult(header, body, noOutput);
  }
  return formatReadResult("", body, noOutput, false);
}

/** 把已选中的已完成命令格式化为稳定结果块（绝不写命令文本）。 */
function formatLastResults(
  commands: TranscriptCommand[],
  noOutput: string,
): string {
  return joinResultBlocks(
    commands.map((command, index) =>
      formatResultBlock(
        index + 1,
        commands.length,
        stripTrailingNewline(command.output ?? ""),
        noOutput,
        {
          status: command.status,
          exitCode: command.exit_code,
          bestEffort: command.best_effort,
        },
      ),
    ),
  );
}

/** 读取 transcript 末条命令并判定 busy（last.status === "running"）。 */
async function readTranscript(
  client: TermxClient,
  target: Record<string, string>,
): Promise<TranscriptView> {
  const result = await client.request<TranscriptResult>("terminal.transcript", target);
  const commands = result.commands || [];
  const last = lastItem(commands);
  let lastCompleted: TranscriptCommand | undefined;
  for (let index = commands.length - 1; index >= 0; index -= 1) {
    if (commands[index].status !== "running") {
      lastCompleted = commands[index];
      break;
    }
  }
  return { commands, last, lastCompleted, busy: last?.status === "running" };
}

/** 单条命令的展示文本：`--includeCommand` 时前缀 `$ command`。 */
function commandOutput(
  command: TranscriptCommand | undefined,
  includeCommand: boolean,
  noOutput: string,
): string {
  if (!command) return "";
  const body = stripTrailingNewline(command.output ?? "");
  return includeCommand ? `$ ${command.command}\n${body || noOutput}` : body;
}

/** 正常 idle 结果：默认只给输出；best_effort/error 才显示首行。 */
function formatIdleRead(
  name: string,
  includeCommand: boolean,
  last: TranscriptCommand | undefined,
  noOutput: string,
): string {
  const output = commandOutput(last, includeCommand, noOutput);
  const note = idleNote(last);
  if (!note) return formatReadResult("", output, noOutput, false);
  const state: ReadHeaderState = note === "best_effort" ? "idle" : "error";
  const header = formatReadHeader(name, "history", state, note);
  return formatReadResult(header, output, noOutput);
}

/**
 * idle 末条命令的提示类型；正常成功返回 undefined。
 *
 * 先判失败再判 best_effort：静默完成的命令若退出码非零，必须显示 failed，
 * 不能被降级成只是“完成判定不确定”。
 */
function idleNote(last: TranscriptCommand | undefined): ReadHeaderNote | undefined {
  if (!last) return undefined;
  const exitCode = typeof last.exit_code === "number" ? last.exit_code : undefined;
  const failed =
    last.status === "error" ||
    last.status === "interrupted" ||
    (exitCode !== undefined && exitCode !== 0);
  if (failed) return "failed";
  return last.best_effort ? "best_effort" : undefined;
}


/** search：从末尾倒搜原始输出；`--nth 1` 为最后一个匹配。 */
async function doSearch(tokens: string[], env: CommandEnv): Promise<string> {
  const parsed = parseFlags(tokens, {
    name: { short: "n", type: "string" },
    pattern: { type: "string" },
    nth: { type: "string" },
    "only-matches": { type: "boolean" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const pattern = textValue(parsed.values, "pattern");
  if (!pattern) return fail(env.t, { key: "cli.err.missingPattern" });
  const nth = readInt(parsed.values, "nth");
  if (!nth.ok) return fail(env.t, nth.failure);
  if (nth.value !== undefined && nth.value < 1) {
    return fail(env.t, { key: "cli.err.badNth" });
  }

  const target = locator(env.ctx, nameOf(parsed.values));
  const result = await env.client.request<OutputResult>("terminal.output", {
    ...target,
    pattern,
    nth: nth.value ?? 1,
    only_matches: boolValue(parsed.values, "only-matches"),
  });
  return result.text || "";
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
    base64: { type: "boolean" },
    file: { type: "string" },
  });
  if (!parsed.ok) return fail(env.t, parsed.failure);

  const payload = await readPayload(parsed.positionals, parsed.values, env.t);
  if (!payload.ok) return payload.text;

  // base64/file 原样写入：不解释转义、不归一换行（raw 载荷按解码后的字节发送）。
  const text = payload.payload.text;
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

/** read 输出格式（附游标）。 */
function formatOutput(t: Translator, result: CollectedOutput): string {
  const parts: string[] = [];
  if (result.resetReason) {
    parts.push(t("cli.read.reset", { reason: result.resetReason }));
  }
  if (result.output) parts.push(stripTrailingNewline(result.output));
  parts.push(`[cursor=${result.cursor}]`);
  return parts.join("\n");
}

/** run 输出格式：成功只给正文；异常/空结果用自解释 JSON。 */
function formatRun(t: Translator, outcome: RunOutcome): string {
  const output = stripTrailingNewline(outcome.output ?? "");
  if (outcome.errorCode || outcome.signal || (outcome.exitCode ?? 0) !== 0) {
    return JSON.stringify({
      output_type: outcome.signal ? "interrupted" : "error",
      exit_code: outcome.exitCode ?? null,
      signal: outcome.signal ?? null,
      message: outcome.errorCode ?? (outcome.signal ? "command interrupted" : "command failed"),
      output,
    });
  }
  if (!output) return EMPTY_OUTPUT_JSON;
  if (outcome.resetReason) return JSON.stringify({ output_type: "text", output, message: t("cli.read.reset", { reason: outcome.resetReason }) });
  return output;
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
