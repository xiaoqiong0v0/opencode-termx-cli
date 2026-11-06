// termx-service 连接解析与自动启动。
//
// 所有路径都来自插件配置（显式传参），不推导平台默认目录。
// 连接优先级：显式 endpoint > 已有 configDir/port+token > 自动启动。

import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import createLogger from "@xiaoqiong0v0/opencode-plugin-logger";
import type { TermxConfig } from "./config.js";
import { createI18n } from "./i18n.js";
import { TermxClient } from "./termx-rpc.mjs";

const log = createLogger("opencode-termx");

/** 等待 service 写文件的轮询间隔与超时。 */
const POLL_INTERVAL_MS = 100;
const START_TIMEOUT_MS = 10_000;
/** 探测已有 service 的连接超时。 */
const PROBE_TIMEOUT_MS = 1_500;
/**
 * 子进程提前退出后的复用宽限。
 *
 * 并发 autostart 时，抢到单实例锁的实例可能稍后才写 port；输掉的实例退出后
 * 先等一小段时间再探活，能直接复用赢家而不是误报启动失败。
 */
const EXIT_GRACE_MS = 2_000;

/** 连接信息。 */
export interface TermxConnection {
  endpoint: string;
  token: string;
}

/** 本次 autostart 子进程的运行状态（用于区分 AlreadyRunning 退出与真实启动失败）。 */
interface AutostartState {
  /** spawn 自身失败（例如 bin 不存在）的错误信息。 */
  error: string | null;
  /** 子进程是否已退出。 */
  exited: boolean;
  /** 退出码；被信号终止时为 null。 */
  exitCode: number | null;
  /** 终止信号；正常退出时为 null。 */
  exitSignal: NodeJS.Signals | null;
  /** 退出时间（毫秒时间戳）；未退出时为 0。 */
  exitedAt: number;
}

/**
 * 解析或拉起 termx-service，返回可连接的 endpoint/token。
 *
 * 不会记录 token；失败时抛出带 bin/路径信息的明确错误。
 */
export async function ensureConnection(config: TermxConfig): Promise<TermxConnection> {
  const t = createI18n(config.toolLang);
  const service = config.service;

  // 1) 显式 endpoint 优先，完全不 autostart。
  if (service.endpoint) {
    return { endpoint: service.endpoint, token: requireToken(config, t) };
  }

  // 2) 已有 port/token 且能握手成功，直接复用。
  const existing = readExistingConnection(config);
  if (existing) {
    if (await probe(existing)) {
      log.info(`复用已运行的 termx-service：${existing.endpoint}`);
      return existing;
    }
    log.warn(`已有 termx port/token 无法连接，尝试重新启动：${existing.endpoint}`);
  }

  // 3) 未启用自动启动时给出明确错误。
  if (!service.autostart) {
    throw new Error(t("err.autostartDisabled", { dir: service.configDir }));
  }

  return startService(config, t);
}

/** 读取已存在的 port/token；缺任意一个返回 null。 */
function readExistingConnection(config: TermxConfig): TermxConnection | null {
  const port = tryReadPort(config);
  const token = readOptionalToken(config);
  if (port === null || !token) return null;
  return { endpoint: `ws://127.0.0.1:${port}`, token };
}

/** 尝试握手；失败返回 false（不抛错，便于回退 autostart）。 */
async function probe(connection: TermxConnection): Promise<boolean> {
  let client: TermxClient | null = null;
  try {
    client = await TermxClient.connect({
      url: connection.endpoint,
      token: connection.token,
      client: "opencode-termx-probe",
      timeoutMs: PROBE_TIMEOUT_MS,
      // 健康检查借用固定会话身份；不参与保活，连接随即关闭。
      rootSessionId: "termx-probe",
    });
    return true;
  } catch {
    return false;
  } finally {
    client?.close();
  }
}

/** 启动 service 并等待 port/token 就绪。 */
async function startService(
  config: TermxConfig,
  t: ReturnType<typeof createI18n>,
): Promise<TermxConnection> {
  const service = config.service;
  const args = buildServiceArgs(config);

  log.info(
    `启动 termx-service：bin=${service.bin} configDir=${service.configDir} logDir=${config.logDir} ` +
      `logEnabled=${config.loggerEnabled} webPort=${config.web.port} ` +
      `webAllowRemote=${config.web.allowRemote} webPassword=${config.web.password ? "set" : "unset"}`,
  );

  const state: AutostartState = {
    error: null,
    exited: false,
    exitCode: null,
    exitSignal: null,
    exitedAt: 0,
  };
  try {
    const child = spawn(service.bin, args, {
      detached: true,
      stdio: "ignore",
      windowsHide: true,
      env: buildServiceEnv(config),
    });
    // 必须监听 error，否则 spawn 失败会变成未捕获异常。
    child.once("error", (error: Error) => {
      state.error = error.message;
      log.error("termx-service 启动失败", error.message);
    });
    // 退出不代表失败：并发 autostart 时，未抢到单实例锁的实例会立即退出，
    // 此时应复用赢家写出的 port/token。
    child.once("exit", (code: number | null, signal: NodeJS.Signals | null) => {
      state.exited = true;
      state.exitCode = code;
      state.exitSignal = signal;
      state.exitedAt = Date.now();
      log.warn(
        `termx-service 已退出（code=${code ?? "null"} signal=${signal ?? "null"}），` +
          "将检查是否已有实例在运行",
      );
    });
    child.unref();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(t("err.spawnFailed", { bin: service.bin, message }));
  }

  return waitForReady(config, state, t);
}

/** 组装启动参数；Web 密码只走环境变量，绝不进命令行。 */
function buildServiceArgs(config: TermxConfig): string[] {
  const service = config.service;
  return [
    "--config-dir",
    service.configDir,
    "--port",
    String(service.port),
    "--token-file",
    service.tokenFile,
    "--log-dir",
    config.logDir,
    "--log-enabled",
    String(config.loggerEnabled),
    "--log-max-days",
    String(config.log.maxDays),
    "--web-port",
    String(config.web.port),
    "--web-allow-remote",
    String(config.web.allowRemote),
  ];
}

/**
 * 组装子进程环境。
 *
 * 密码只在配置了才注入；远端注入的路径覆盖为空表示“用 service 默认值”，因此也不注入。
 * 本地 helper 路径为空表示“不自动上传”，此时不注入。
 */
function buildServiceEnv(config: TermxConfig): NodeJS.ProcessEnv {
  const injection = config.remote.injection;
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    TERMX_MAX_TERMINALS: String(config.limits.maxTerminals),
    TERMX_CLIENT_IDLE_POLICY: config.limits.clientIdlePolicy,
    TERMX_CLIENT_IDLE_TIMEOUT_MS: String(config.limits.clientIdleTimeoutMs),
    TERMX_INTERACTION_TICK_MS: String(config.interaction.tickMs),
    TERMX_INTERACTION_NO_OUTPUT_MS: String(config.interaction.noOutputMs),
    TERMX_REMOTE_INJECTION_CONSENT: injection.consent,
    TERMX_REMOTE_INJECTION_ROOT: injection.root,
  };
  if (config.web.password) env.TERMX_WEB_PASSWORD = config.web.password;
  if (injection.helperLocal) {
    env.TERMX_REMOTE_INJECTION_HELPER_LOCAL = injection.helperLocal;
  }
  if (injection.paths.helper) env.TERMX_REMOTE_INJECTION_HELPER = injection.paths.helper;
  if (injection.paths.pwsh) env.TERMX_REMOTE_INJECTION_PWSH = injection.paths.pwsh;
  if (injection.paths.bash) env.TERMX_REMOTE_INJECTION_BASH = injection.paths.bash;
  if (injection.paths.sh) env.TERMX_REMOTE_INJECTION_SH = injection.paths.sh;
  return env;
}

/**
 * 轮询 port/token 文件并握手，最多等待 START_TIMEOUT_MS。
 *
 * 子进程提前退出时先给并发启动的赢家一个短暂宽限：重新读取 port/token 并
 * 探活，成功则复用；只有确实没有可用实例时才报启动失败。
 */
async function waitForReady(
  config: TermxConfig,
  state: AutostartState,
  t: ReturnType<typeof createI18n>,
): Promise<TermxConnection> {
  const service = config.service;
  const start = Date.now();

  while (Date.now() - start < START_TIMEOUT_MS) {
    if (state.error) {
      throw new Error(t("err.spawnFailed", { bin: service.bin, message: state.error }));
    }

    const existing = readExistingConnection(config);
    if (existing && (await probe(existing))) {
      log.info(`termx-service 已就绪：${existing.endpoint}`);
      return existing;
    }

    if (state.exited && Date.now() - state.exitedAt >= EXIT_GRACE_MS) {
      const reused = await reuseRunningInstance(config);
      if (reused) return reused;
      throw new Error(
        t("err.serviceExited", {
          bin: service.bin,
          code: formatExit(state),
        }),
      );
    }

    await delay(POLL_INTERVAL_MS);
  }

  // 超时边界：并发启动的赢家可能刚写好文件，最后再读一次并探活。
  const reused = await reuseRunningInstance(config);
  if (reused) return reused;

  // 区分「端口文件内容非法」与「根本没写出来」，便于定位。
  const portPath = join(service.configDir, "port");
  if (existsSync(portPath)) {
    const value = readFileSync(portPath, "utf8").trim();
    if (!isValidPort(value)) {
      throw new Error(t("err.portInvalid", { path: portPath, value }));
    }
  }

  throw new Error(
    t("err.startTimeout", {
      bin: service.bin,
      dir: service.configDir,
      log: config.logDir,
    }),
  );
}

/** 重新读取 port/token 并探活一次；成功才复用并发启动的实例。 */
async function reuseRunningInstance(
  config: TermxConfig,
): Promise<TermxConnection | null> {
  const existing = readExistingConnection(config);
  if (!existing) return null;
  if (!(await probe(existing))) return null;
  log.warn(`termx-service 由其它实例启动，复用：${existing.endpoint}`);
  return existing;
}

/** 退出状态的可读描述（用于错误文案）。 */
function formatExit(state: AutostartState): string {
  if (state.exitSignal) return `signal=${state.exitSignal}`;
  return `code=${state.exitCode ?? "null"}`;
}

/** 读取端口文件；不存在或内容非法返回 null（轮询期间容忍半写状态）。 */
function tryReadPort(config: TermxConfig): number | null {
  const path = join(config.service.configDir, "port");
  if (!existsSync(path)) return null;
  const value = readFileSync(path, "utf8").trim();
  return isValidPort(value) ? Number(value) : null;
}

/** 端口取值是否为 1..65535 的整数。 */
function isValidPort(value: string): boolean {
  const port = Number(value);
  return Number.isInteger(port) && port >= 1 && port <= 65535;
}

/** 读取 token：TERMX_TOKEN > token 文件；缺失返回 null。 */
function readOptionalToken(config: TermxConfig): string | null {
  const fromEnv = (process.env.TERMX_TOKEN ?? "").trim();
  if (fromEnv) return fromEnv;

  const path = config.service.tokenFile;
  if (!existsSync(path)) return null;
  const token = readFileSync(path, "utf8").trim();
  return token || null;
}

/** 读取 token，缺失时报错。 */
function requireToken(
  config: TermxConfig,
  t: ReturnType<typeof createI18n>,
): string {
  const token = readOptionalToken(config);
  if (!token) {
    throw new Error(t("err.tokenMissing", { path: config.service.tokenFile }));
  }
  return token;
}

/** 定时等待。 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
