// opencode 项目内插件：把 termx-service 的 RPC 暴露为单工具 `termx_cli`。
//
// 工具只接受完整命令行字符串；子命令解析与 RPC 映射见 `cli.ts`。
// 配置在插件初始化时读取（仅本地文件，不阻塞）；service.autostart=true 时
// 后台预热 service（不 await），已有手动启动的 service 会直接复用。

import { tool } from "@opencode-ai/plugin";
import type { Plugin, PluginModule } from "@opencode-ai/plugin";
import createLogger from "@xiaoqiong0v0/opencode-plugin-logger";
import { handleCliCommand } from "./cli.js";
import type { CliDeps } from "./cli.js";
import { loadConfig } from "./config.js";
import type { TermxConfig } from "./config.js";
import { createI18n } from "./i18n.js";
import type { Translator } from "./i18n.js";
import { ensureConnection } from "./service.js";
import type { TermxConnection } from "./service.js";
import { getClient, HEARTBEAT_INTERVAL_MS } from "./termx-rpc.mjs";
import type { TermxClient } from "./termx-rpc.mjs";

const z = tool.schema;
const log = createLogger("opencode-termx");

/** 连接管理器：启动时预热 service，首次工具调用复用同一 Promise。 */
interface ConnectionManager {
  /** 后台预启动/探测 service；不阻塞插件加载。 */
  warmup(): void;
  /** 取指定 opencode 会话的独立连接。 */
  client(rootSessionId: string): Promise<TermxClient>;
}

/** 创建连接管理器；`warmup` 与首次 `client` 共用同一启动 Promise。 */
function createConnection(config: TermxConfig): ConnectionManager {
  let pending: Promise<TermxConnection> | null = null;

  const resolve = (): Promise<TermxConnection> => {
    if (!pending) {
      pending = ensureConnection(config).catch((error: unknown) => {
        pending = null;
        throw error;
      });
    }
    return pending;
  };

  return {
    warmup(): void {
      // 与旧项目一致：刻意不 await，启动慢/失败都不阻塞插件加载。
      void resolve().catch((error: unknown) => {
        log.warn(
          "termx-service 后台预热失败（首次工具调用会重试）",
          error instanceof Error ? error.message : String(error),
        );
      });
    },
    async client(rootSessionId: string): Promise<TermxClient> {
      const connection = await resolve();
      try {
        return await getClient({ ...connection, rootSessionId });
      } catch {
        // 服务可能已重启（端口/token 变化）；重新解析后再试一次。
        pending = null;
        return getClient({ ...(await resolve()), rootSessionId });
      }
    },
  };
}

/** 从 opencode 事件中取 `session.deleted` 的会话 ID；其它事件返回 null。 */
function deletedSessionId(event: unknown): string | null {
  const value = event as
    | {
        type?: unknown;
        properties?: { sessionID?: unknown; info?: { id?: unknown } };
      }
    | undefined;
  if (value?.type !== "session.deleted") return null;
  const direct = value.properties?.sessionID;
  if (typeof direct === "string" && direct) return direct;
  const nested = value.properties?.info?.id;
  return typeof nested === "string" && nested ? nested : null;
}

const server: Plugin = async () => {
  log.loaded();
  // 只读本地配置（同步、极小），不在这里连接或启动 service。
  const config = loadConfig();
  const t: Translator = createI18n(config.toolLang);
  const connection = createConnection(config);
  // 参考旧项目：插件初始化即后台启动 service，不阻塞 opencode；已有实例会被 probe 复用。
  if (config.service.autostart) connection.warmup();
  /** read 的每终端增量游标。 */
  const cursors = new Map<string, number>();

  // 会话心跳：每个 opencode 会话使用自己的连接；Web 不经过插件，不发心跳。
  const sessions = new Set<string>();
  const sessionTimers = new Map<string, ReturnType<typeof setInterval>>();
  const sessionClients = new Map<string, TermxClient>();

  const sendSessionHeartbeat = (sessionID: string): void => {
    const client = sessionClients.get(sessionID);
    if (!client) return;
    // 心跳失败不打断工具调用；TTL 到期后由 service 自行终止终端。
    void client.heartbeat(sessionID).catch(() => {});
  };

  const startSessionHeartbeat = (client: TermxClient, sessionID: string): void => {
    sessionClients.set(sessionID, client);
    sendSessionHeartbeat(sessionID);
    if (sessions.has(sessionID)) return;
    sessions.add(sessionID);
    const timer = setInterval(() => sendSessionHeartbeat(sessionID), HEARTBEAT_INTERVAL_MS);
    timer.unref?.();
    sessionTimers.set(sessionID, timer);
  };

  const stopSessionHeartbeat = (sessionID: string): void => {
    sessions.delete(sessionID);
    const timer = sessionTimers.get(sessionID);
    if (timer) {
      clearInterval(timer);
      sessionTimers.delete(sessionID);
    }
    // 会话已删除：用该会话自己的连接通知服务端，然后关闭它。
    const client = sessionClients.get(sessionID);
    sessionClients.delete(sessionID);
    if (client) {
      void client
        .sessionClosed(sessionID)
        .catch(() => {})
        .finally(() => client.close());
    }
  };

  const deps: CliDeps = {
    t,
    cursors,
    exec: config.exec,
    async client(ctx): Promise<TermxClient> {
      const root = ctx.sessionID || process.env.TERMX_ROOT_SESSION_ID || "default";
      const client = await connection.client(root);
      startSessionHeartbeat(client, root);
      return client;
    },
  };

  return {
    // session.deleted：通知 service 立即移除存活记录（不等 90s TTL）。
    async event({ event }: { event: unknown }) {
      const sessionID = deletedSessionId(event);
      if (sessionID) stopSessionHeartbeat(sessionID);
    },
    async dispose() {
      for (const timer of sessionTimers.values()) clearInterval(timer);
      sessionTimers.clear();
      sessions.clear();
      for (const client of sessionClients.values()) client.close();
      sessionClients.clear();
      cursors.clear();
    },
    tool: {
      // 单工具 CLI：`args` 是完整命令行字符串（如 `local "bash" -n dev`）。
      termx_cli: tool({
        description: t("tool.cli.desc"),
        args: {
          args: z.string().optional().describe(t("tool.cli.desc")),
        },
        async execute(args, ctx) {
          return handleCliCommand(args.args ?? "help", ctx, deps);
        },
      }),
    },
  };
};

const plugin: PluginModule = { id: "termx", server };
export default plugin;
