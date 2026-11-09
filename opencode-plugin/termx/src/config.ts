// 插件全局配置：读取 JSONC、首次生成模板、默认值与环境变量覆盖。
//
// 约定：
// - 配置文件默认 `~/.config/opencode/termx.jsonc`，可用 `TERMX_CONFIG` 覆盖（测试用）。
// - 文件不存在时写模板；解析失败回退默认值并记录插件日志。
// - service 的配置目录/端口/token/endpoint 全部显式解析，不推导平台默认目录。
// - 日志目录固定为插件缓存根，日期文件由 termx-service 生成。

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import createLogger from "@xiaoqiong0v0/opencode-plugin-logger";

const log = createLogger("opencode-termx");

/** 支持的语言。 */
export type Lang = "en" | "zh";

/** termx-service 连接与启动配置（路径均已解析为绝对路径）。 */
export interface ServiceConfig {
  /** 不可连接时是否自动拉起 service。 */
  autostart: boolean;
  /** service 可执行文件（命令名或绝对路径）。 */
  bin: string;
  /** token/port 文件目录。 */
  configDir: string;
  /** 监听端口；0 表示由系统分配。 */
  port: number;
  /** token 文件路径。 */
  tokenFile: string;
  /** 显式 endpoint；非空时不 autostart。 */
  endpoint: string;
}

/** Web 服务配置（传给 termx-service）。 */
export interface WebConfig {
  /** 监听端口；0 表示不启动 Web 服务。 */
  port: number;
  /** 是否允许远程访问；false 只绑定 127.0.0.1（回环免登录）。 */
  allowRemote: boolean;
  /** 登录密码；空表示未设置（远程访问时无法登录）。 */
  password: string;
}

/** 无存活会话到期策略。 */
export type ClientIdlePolicy = "never" | "no_children" | "any";

/** 远端注入授权策略：ask 每次确认 / allow 直接允许 / deny 一律拒绝。 */
export type InjectionConsent = "ask" | "allow" | "deny";

/** 远端注入路径覆盖；空串表示使用 `<root>` 下的默认路径。 */
export interface InjectionPaths {
  helper: string;
  pwsh: string;
  bash: string;
  sh: string;
}

/** 注入配置（本地/远端共用的结构：授权策略、根目录与各脚本路径）。 */
export interface InjectionConfig {
  /** 写入授权策略：ask 每次确认 / allow 直接允许 / deny 一律拒绝。 */
  consent: InjectionConsent;
  /** 目标主机根目录；`~` 由远端 shell 展开。 */
  root: string;
  /** 各脚本路径（默认按 `root` 派生，见 `deriveInjectionPaths`）。 */
  paths: InjectionPaths;
  /** 本地 controller 上的 helper 产物路径；空串表示不自动上传。 */
  helperLocal: string;
}

/** 本地相关配置。 */
export interface LocalConfig {
  /** 本地注入配置：按字段覆盖顶层 `injection`。 */
  injection: InjectionConfig;
}

/** 远端相关配置。 */
export interface RemoteConfig {
  /** 远端注入配置：按字段覆盖顶层 `injection`。 */
  injection: InjectionConfig;
}

/** 插件完整配置。 */
export interface TermxConfig {
  toolLang: Lang;
  webLang: Lang;
  service: ServiceConfig;
  /** Web 服务配置：`password` 只经子进程环境变量传递。 */
  web: WebConfig;
  /** 注入默认值：`local.injection` / `remote.injection` 未覆盖的字段继承这里。 */
  injection: InjectionConfig;
  /** 本地注入配置（合并顶层默认值后的生效值）。 */
  local: LocalConfig;
  /** 远端注入配置（合并顶层默认值后的生效值）。 */
  remote: RemoteConfig;
  log: { maxDays: number };
  limits: {
    maxTerminals: number;
    sshHistoryLimit: number;
    clientIdlePolicy: ClientIdlePolicy;
    clientIdleTimeoutMs: number;
  };
  interaction: { tickMs: number; noOutputMs: number };
  /** 插件缓存根，也是 service 文件日志目录。 */
  logDir: string;
  /** 是否启用插件文件日志（来自 plugin-logger.jsonc）。 */
  loggerEnabled: boolean;
}

/** 插件缓存根目录名（`~/.opencode/plugins-cache/...`）。 */
const CACHE_OWNER = "@xiaoqiong0v0";
const CACHE_PLUGIN = "opencode-termx";
const CACHE_SERVICE = "termx-service";
/** 默认 service 命令名。 */
const DEFAULT_SERVICE_BIN = "termx-service";
/** 默认日志保留天数。 */
const DEFAULT_LOG_MAX_DAYS = 7;
/** 默认同时运行终端上限。 */
const DEFAULT_MAX_TERMINALS = 16;
/** 默认 SSH 连接历史条数上限；0 表示禁用。 */
const DEFAULT_SSH_HISTORY_LIMIT = 64;
/** 默认无存活会话超时（毫秒）；0 表示关闭。 */
const DEFAULT_CLIENT_IDLE_TIMEOUT_MS = 300_000;
/** 默认无存活会话到期策略。 */
const DEFAULT_CLIENT_IDLE_POLICY: ClientIdlePolicy = "no_children";
/** 默认交互识别周期（毫秒）。 */
const DEFAULT_TICK_MS = 250;
/** 默认无输出阈值（毫秒）。 */
const DEFAULT_NO_OUTPUT_MS = 5000;
/** 默认 Web 端口；0 表示不启动 Web 服务。 */
const DEFAULT_WEB_PORT = 0;
/** 默认只允许本机访问 Web。 */
const DEFAULT_WEB_ALLOW_REMOTE = false;
/** 默认远端注入策略：每次创建 SSH 时询问。 */
const DEFAULT_INJECTION_CONSENT: InjectionConsent = "ask";
/** 默认远端注入根目录（远端用户主目录下的 .termx）。 */
const DEFAULT_INJECTION_ROOT = "~/.termx";

/** 首次生成到配置文件的模板。 */
const TEMPLATE = `{
  // 工具描述与错误文案语言：en 或 zh（环境变量 TERMX_TOOL_LANG 优先）
  "toolLang": "en",
  // Web UI 语言预留（环境变量 TERMX_WEB_LANG 优先）
  "webLang": "en",
  "service": {
    // 无法连接时是否自动启动 termx-service
    "autostart": true,
    // service 可执行文件（命令名或绝对路径）
    "bin": "${DEFAULT_SERVICE_BIN}",
    // token/port 文件目录；留空使用 <插件缓存>/config
    "configDir": "",
    // 监听端口；0 表示由系统分配
    "port": 0,
    // token 文件；留空使用 <configDir>/token
    "tokenFile": "",
    // 显式 WebSocket endpoint；非空时优先且不自动启动
    "endpoint": ""
  },
  "web": {
    // Web 服务端口；0 表示不启动 Web 服务
    "port": ${DEFAULT_WEB_PORT},
    // 是否允许远程访问 Web；false 只绑定 127.0.0.1（回环免登录）
    "allowRemote": ${DEFAULT_WEB_ALLOW_REMOTE},
    // Web 登录密码；远程访问必填（经环境变量传给 service，不写命令行/日志）
    "password": ""
  },
  // 注入默认值：创建会话时可能向目标主机写入 helper/shell 脚本
  "injection": {
    // 写入授权策略：ask 每次确认 / allow 直接允许 / deny 一律拒绝（非法值回退 ask）
    "consent": "${DEFAULT_INJECTION_CONSENT}",
    // 注入根目录；~ 表示目标用户主目录
    "root": "${DEFAULT_INJECTION_ROOT}",
    // 本地 controller 上的 helper 产物路径；留空表示不自动上传（使用远端预装 helper）
    "helperLocal": "",
    // 各脚本路径；留空按 <root> 派生（<root>/bin/termx-helper、<root>/runtimes/{pwsh,bash,sh}）
    "paths": {
      "helper": "",
      "pwsh": "",
      "bash": "",
      "sh": ""
    }
  },
  // 可选覆盖：local.injection / remote.injection 结构与 injection 相同，
  // 标量非空才覆盖，paths 逐字段覆盖，未覆盖字段继承 injection。例如：
  // "local": { "injection": { "consent": "allow" } },
  // "remote": { "injection": { "root": "/opt/termx", "paths": { "helper": "/opt/termx/bin/helper" } } },
  "log": { "maxDays": ${DEFAULT_LOG_MAX_DAYS} },
  "limits": {
    // 同时运行终端上限
    "maxTerminals": ${DEFAULT_MAX_TERMINALS},
    // SSH 连接历史条数上限；0 表示禁用
    "sshHistoryLimit": ${DEFAULT_SSH_HISTORY_LIMIT},
    // 无存活 opencode 会话时的到期策略：never / no_children / any
    "clientIdlePolicy": "${DEFAULT_CLIENT_IDLE_POLICY}",
    // 无存活 opencode 会话超时（毫秒）；0 表示关闭
    "clientIdleTimeoutMs": ${DEFAULT_CLIENT_IDLE_TIMEOUT_MS}
  },
  "interaction": { "tickMs": ${DEFAULT_TICK_MS}, "noOutputMs": ${DEFAULT_NO_OUTPUT_MS} }
}
`;

/** 当前进程缓存；避免每次工具调用都读文件。 */
let cached: TermxConfig | null = null;

/** 全局 opencode 配置目录（`~/.config/opencode`）。 */
export function globalConfigDir(): string {
  return join(homedir(), ".config", "opencode");
}

/** termx 配置文件路径；`TERMX_CONFIG` 仅用于测试覆盖。 */
export function configFilePath(): string {
  const override = envText("TERMX_CONFIG");
  return override ? toAbsolute(override) : join(globalConfigDir(), "termx.jsonc");
}

/** plugin-logger 配置文件路径。 */
export function loggerConfigPath(): string {
  return join(globalConfigDir(), "plugin-logger.jsonc");
}

/** 插件缓存根（同时作为 service 日志目录）。 */
export function cacheDir(): string {
  return join(homedir(), ".opencode", "plugins-cache", CACHE_OWNER, CACHE_PLUGIN, CACHE_SERVICE);
}

/**
 * 读取 `plugin-logger.jsonc` 的 `enabled`。
 *
 * 文件不存在或解析失败一律视为关闭；不在此处写文件（由 logger 自己处理）。
 */
export function loggerEnabled(): boolean {
  const path = loggerConfigPath();
  if (!existsSync(path)) return false;
  try {
    const parsed = parseJsonc(readFileSync(path, "utf8"));
    return asRecord(parsed).enabled === true;
  } catch (error) {
    log.warn("读取 plugin-logger 配置失败，按关闭处理", errorText(error));
    return false;
  }
}

/**
 * 加载插件配置。
 *
 * `reload=true` 时忽略进程内缓存重新读取（测试/调试用）。
 */
export function loadConfig(options: { reload?: boolean } = {}): TermxConfig {
  if (cached && !options.reload) return cached;

  const path = configFilePath();
  const raw = readConfig(path);
  const config = normalizeConfig(raw);
  cached = config;
  log.info(`已加载 termx 配置：${path}`);
  return config;
}

/** 读取配置文件；不存在时写模板，解析失败回退空对象。 */
function readConfig(path: string): unknown {
  try {
    if (!existsSync(path)) {
      mkdirSync(dirname(path), { recursive: true });
      writeFileSync(path, TEMPLATE, "utf8");
      log.info(`已生成 termx 配置模板：${path}`);
      return {};
    }
    return parseJsonc(readFileSync(path, "utf8"));
  } catch (error) {
    log.warn(`读取 termx 配置失败，已回退默认值：${path}`, errorText(error));
    return {};
  }
}

/** 把原始配置归一为完整配置（缺字段/类型错误均回退默认）。 */
function normalizeConfig(raw: unknown): TermxConfig {
  const root = asRecord(raw);
  const service = asRecord(root.service);
  const web = asRecord(root.web);
  const baseInjection = asRecord(root.injection);
  const localInjection = asRecord(asRecord(root.local).injection);
  const remoteInjection = asRecord(asRecord(root.remote).injection);
  const logCfg = asRecord(root.log);
  const limits = asRecord(root.limits);
  const interaction = asRecord(root.interaction);

  const envConfigDir = envText("TERMX_CONFIG_DIR");
  const configDir = toAbsolute(
    envConfigDir || asText(service.configDir, "") || join(cacheDir(), "config"),
  );
  const tokenFile = asText(service.tokenFile, "").trim()
    ? toAbsolute(asText(service.tokenFile, ""))
    : join(configDir, "token");
  const endpoint = envText("TERMX_ENDPOINT") || asText(service.endpoint, "").trim();

  return {
    toolLang: resolveLang("TERMX_TOOL_LANG", root.toolLang),
    webLang: resolveLang("TERMX_WEB_LANG", root.webLang),
    service: {
      autostart: asBool(service.autostart, true),
      bin: asText(service.bin, "").trim() || DEFAULT_SERVICE_BIN,
      configDir,
      port: asInt(service.port, 0, 0, 65535),
      tokenFile,
      endpoint,
    },
    web: {
      port: asInt(web.port, DEFAULT_WEB_PORT, 0, 65535),
      allowRemote: asBool(web.allowRemote, DEFAULT_WEB_ALLOW_REMOTE),
      password: resolveWebPassword(web.password),
    },
    injection: resolveInjection(baseInjection, {}),
    local: { injection: resolveInjection(baseInjection, localInjection) },
    remote: { injection: resolveInjection(baseInjection, remoteInjection) },
    log: { maxDays: asInt(logCfg.maxDays, DEFAULT_LOG_MAX_DAYS, 1, 3650) },
    limits: {
      maxTerminals: asInt(limits.maxTerminals, DEFAULT_MAX_TERMINALS, 1, 4096),
      sshHistoryLimit: asInt(limits.sshHistoryLimit, DEFAULT_SSH_HISTORY_LIMIT, 0, 4096),
      clientIdlePolicy: asClientIdlePolicy(limits.clientIdlePolicy),
      clientIdleTimeoutMs: asInt(
        limits.clientIdleTimeoutMs,
        DEFAULT_CLIENT_IDLE_TIMEOUT_MS,
        0,
        86_400_000,
      ),
    },
    interaction: {
      tickMs: asInt(interaction.tickMs, DEFAULT_TICK_MS, 1, 3_600_000),
      noOutputMs: asInt(interaction.noOutputMs, DEFAULT_NO_OUTPUT_MS, 1, 86_400_000),
    },
    logDir: cacheDir(),
    loggerEnabled: loggerEnabled(),
  };
}

/**
 * 合并注入配置：`base` 为顶层 `injection`，`override` 为 local/remote 的可选覆盖。
 *
 * 标量只有非空/合法才覆盖；`paths` 逐字段覆盖；都没有给出时按生效根目录派生。
 */
function resolveInjection(
  base: Record<string, unknown>,
  override: Record<string, unknown>,
): InjectionConfig {
  const baseRoot = asText(base.root, "").trim();
  const overrideRoot = asText(override.root, "").trim();
  const root = overrideRoot || baseRoot || DEFAULT_INJECTION_ROOT;
  const derived = deriveInjectionPaths(root);
  const basePaths = asRecord(base.paths);
  const overridePaths = asRecord(override.paths);
  const pick = (field: keyof InjectionPaths): string =>
    asText(overridePaths[field], "").trim() || asText(basePaths[field], "").trim() || derived[field];
  return {
    consent: asInjectionConsent(override.consent, asInjectionConsent(base.consent)),
    root,
    // helperLocal 是本地路径：非空时展开为绝对路径，空串保持“不自动上传”。
    helperLocal: resolveHelperLocal(
      asText(override.helperLocal, "").trim() || asText(base.helperLocal, "").trim(),
    ),
    paths: {
      helper: pick("helper"),
      pwsh: pick("pwsh"),
      bash: pick("bash"),
      sh: pick("sh"),
    },
  };
}

/** 本地 helper 路径：空串原样返回，否则展开 `~` 并转绝对路径。 */
function resolveHelperLocal(value: string): string {
  return value ? toAbsolute(value) : "";
}

/** 由注入根目录派生默认脚本路径（与 termx-service 的默认值一致）。 */
function deriveInjectionPaths(root: string): InjectionPaths {
  const base = root.replace(/[\\/]+$/, "") || DEFAULT_INJECTION_ROOT;
  return {
    helper: `${base}/bin/termx-helper`,
    pwsh: `${base}/runtimes/pwsh`,
    bash: `${base}/runtimes/bash`,
    sh: `${base}/runtimes/sh`,
  };
}

/**
 * Web 密码解析：`TERMX_WEB_PASSWORD`（测试/临时覆盖）> 配置 > 空。
 *
 * 纯空白视为未设置；非空白时保留原值（密码可能含空格）。
 */
function resolveWebPassword(value: unknown): string {
  const fromEnv = envText("TERMX_WEB_PASSWORD");
  if (fromEnv) return fromEnv;
  const fromConfig = asText(value, "");
  return fromConfig.trim() ? fromConfig : "";
}

/** 语言解析：环境变量 > 配置 > 默认 en。 */
function resolveLang(envName: string, value: unknown): Lang {
  return normalizeLang(envText(envName)) ?? normalizeLang(value) ?? "en";
}

/** 只接受 en/zh（含 zh-CN、en-US 之类的前缀形式）。 */
function normalizeLang(value: unknown): Lang | null {
  if (typeof value !== "string") return null;
  const lang = value.trim().toLowerCase();
  if (lang === "en" || lang.startsWith("en-")) return "en";
  if (lang === "zh" || lang.startsWith("zh-")) return "zh";
  return null;
}

/** 读取并去除空白的环境变量；空串视为未设置。 */
function envText(name: string): string {
  return (process.env[name] ?? "").trim();
}

/** 取字符串，否则用默认值。 */
function asText(value: unknown, fallback: string): string {
  return typeof value === "string" ? value : fallback;
}

/** 取布尔值，否则用默认值。 */
function asBool(value: unknown, fallback: boolean): boolean {
  return typeof value === "boolean" ? value : fallback;
}

/** 取范围内的整数，否则用默认值。 */
function asInt(value: unknown, fallback: number, min: number, max: number): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < min || value > max) {
    return fallback;
  }
  return value;
}

/** 取无存活会话策略；无法识别回退默认值。 */
function asClientIdlePolicy(value: unknown): ClientIdlePolicy {
  if (typeof value !== "string") return DEFAULT_CLIENT_IDLE_POLICY;
  const policy = value.trim().toLowerCase();
  if (policy === "never" || policy === "any" || policy === "no_children") {
    return policy;
  }
  if (policy === "no-children" || policy === "nochildren") return "no_children";
  return DEFAULT_CLIENT_IDLE_POLICY;
}

/** 取注入授权策略：只接受 ask/allow/deny，其他值回退 `fallback`（默认 ask）。 */
function asInjectionConsent(
  value: unknown,
  fallback: InjectionConsent = DEFAULT_INJECTION_CONSENT,
): InjectionConsent {
  if (typeof value !== "string") return fallback;
  const consent = value.trim().toLowerCase();
  if (consent === "ask" || consent === "allow" || consent === "deny") return consent;
  return fallback;
}

/** 把非对象归一为空对象。 */
function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

/** 展开 `~` 并解析为绝对路径。 */
function toAbsolute(path: string): string {
  if (path === "~") return homedir();
  if (path.startsWith("~/") || path.startsWith("~\\")) {
    return resolve(join(homedir(), path.slice(2)));
  }
  return resolve(path);
}

/** 错误对象转短文本，避免把敏感内容写进日志。 */
function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * 宽松 JSONC 解析：支持 `//`、`/* *\/` 注释与尾逗号。
 *
 * 注释只在字符串外识别；解析失败抛出原始错误，由调用方回退默认值。
 */
export function parseJsonc(text: string): unknown {
  const withoutComments = stripComments(text);
  return JSON.parse(removeTrailingCommas(withoutComments));
}

/** 把注释替换为空格（保留换行与字符串内容）。 */
function stripComments(text: string): string {
  let out = "";
  let inString = false;
  let escaped = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    const next = text[i + 1];

    if (inString) {
      out += ch;
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }

    if (ch === '"') {
      inString = true;
      out += ch;
      continue;
    }

    if (ch === "/" && next === "/") {
      while (i < text.length && text[i] !== "\n") {
        out += " ";
        i += 1;
      }
      if (i < text.length) out += "\n";
      continue;
    }

    if (ch === "/" && next === "*") {
      i += 2;
      out += "  ";
      while (i < text.length && !(text[i] === "*" && text[i + 1] === "/")) {
        out += text[i] === "\n" ? "\n" : " ";
        i += 1;
      }
      if (i < text.length) {
        out += "  ";
        i += 1;
      }
      continue;
    }

    out += ch;
  }
  return out;
}

/** 删除对象/数组末尾多余逗号（字符串内不处理）。 */
function removeTrailingCommas(text: string): string {
  let out = "";
  let inString = false;
  let escaped = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];

    if (inString) {
      out += ch;
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }

    if (ch === '"') {
      inString = true;
      out += ch;
      continue;
    }

    if (ch === ",") {
      let j = i + 1;
      while (j < text.length && /\s/.test(text[j])) j += 1;
      if (text[j] === "}" || text[j] === "]") continue;
    }

    out += ch;
  }
  return out;
}
