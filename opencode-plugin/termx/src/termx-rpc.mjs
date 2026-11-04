// termx 客户端传输层（纯 Node，无 opencode 依赖，便于单独自测）。
// 约定：每个 WebSocket text frame 一个 JSON；hello 后按 id 配对 Response，无 id 帧视为 Event。
//
// 本模块不推导平台默认配置目录：调用方必须显式给出 configDir 或 endpoint/token。

import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

/** 连接心跳与会话心跳间隔（毫秒）。 */
export const HEARTBEAT_INTERVAL_MS = 30_000;
/** 单次心跳请求超时（毫秒）；失败只停止心跳，不打断工具调用。 */
const HEARTBEAT_TIMEOUT_MS = 5_000;

/**
 * 只允许连接环回 endpoint，避免把 token 发往外部地址。
 * 接受 `127.0.0.1` 与 IPv6 `::1`；不接受 `localhost`（避免依赖 DNS 解析）。
 *
 * @param {string} endpoint WebSocket endpoint
 */
function assertLoopbackEndpoint(endpoint) {
  let url;
  try {
    url = new URL(endpoint);
  } catch {
    throw new Error(`termx endpoint 非法：${endpoint}`);
  }
  if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new Error(`termx endpoint 协议必须是 ws/wss：${endpoint}`);
  }
  if (url.username || url.password) {
    throw new Error(`termx endpoint 不能包含凭据：${endpoint}`);
  }
  const host = url.hostname.toLowerCase();
  if (host !== "127.0.0.1" && host !== "::1" && host !== "[::1]") {
    throw new Error(`termx endpoint 必须是环回地址：${endpoint}`);
  }
}

/**
 * 从显式配置目录读取 endpoint/token。
 * token 优先级：TERMX_TOKEN > token 文件。
 *
 * @param {string} configDir token/port 文件所在目录
 * @param {string} [tokenFile] 自定义 token 文件（默认 `<configDir>/token`）
 * @returns {{ endpoint: string, token: string }}
 */
export function discoverFrom(configDir, tokenFile) {
  if (!configDir) throw new Error("termx 配置目录不能为空");
  const portFile = join(configDir, "port");
  if (!existsSync(portFile)) {
    throw new Error(`termx service 未运行：找不到 ${portFile}`);
  }
  const port = readFileSync(portFile, "utf8").trim();
  const file = tokenFile || join(configDir, "token");
  const token = (process.env.TERMX_TOKEN || "").trim() || readTokenFile(file);
  if (!token) throw new Error(`termx token 缺失：${file}`);
  return { endpoint: `ws://127.0.0.1:${port}`, token };
}

/** 读取 token 文件；不存在或为空返回空串。 */
function readTokenFile(file) {
  if (!existsSync(file)) return "";
  return readFileSync(file, "utf8").trim();
}

/**
 * 用显式 endpoint/token 建立连接（含 hello 握手）。
 *
 * `rootSessionId` 决定连接级访问身份（每个 opencode 会话独立连接）；
 * `admin` 仅 Web 代理等管理场景使用。
 *
 * @param {{ endpoint: string, token: string, client?: string, timeoutMs?: number, rootSessionId?: string, admin?: boolean }} options
 * @returns {Promise<TermxClient>}
 */
export async function connectWith({
  endpoint,
  token,
  client = "opencode-termx-plugin",
  timeoutMs,
  rootSessionId,
  admin = false,
}) {
  if (!endpoint) throw new Error("termx endpoint 不能为空");
  if (!token) throw new Error("termx token 不能为空");
  return TermxClient.connect({ url: endpoint, token, client, timeoutMs, rootSessionId, admin });
}

/** 极简 RPC 客户端：hello + 请求/响应配对 + 事件回调。 */
export class TermxClient {
  #ws;
  #pending = new Map();
  #handlers = new Set();
  #nextId = 0;
  #closed = false;
  #heartbeat = null;
  #rootSessionId;

  constructor(ws, rootSessionId) {
    this.#ws = ws;
    this.#rootSessionId = rootSessionId;
    ws.onmessage = (ev) => this.#onMessage(ev.data);
    ws.onclose = () => this.#failAll(new Error("termx 连接已关闭"));
    ws.onerror = () => this.#failAll(new Error("termx 连接错误"));
  }

  /** 连接并完成 hello 握手。 */
  static async connect({
    url,
    token,
    client = "opencode-termx-plugin",
    timeoutMs = 10000,
    rootSessionId,
    admin = false,
  }) {
    assertLoopbackEndpoint(url);
    const ws = new WebSocket(url);
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`连接超时：${url}`)), timeoutMs);
      ws.onopen = () => { clearTimeout(t); resolve(); };
      ws.onerror = () => { clearTimeout(t); reject(new Error(`连接失败：${url}`)); };
    });
    const c = new TermxClient(ws, rootSessionId);
    const hello = { proto: "termx/1", token, client };
    if (rootSessionId) hello.root_session_id = rootSessionId;
    if (admin) hello.admin = true;
    await c.request("hello", hello, timeoutMs);
    // hello 成功后才启动心跳，避免握手失败的空连接继续发帧。
    c.#startHeartbeat();
    return c;
  }

  /** 连接是否已关闭。 */
  get closed() {
    return this.#closed;
  }

  /**
   * 发起 RPC。
   * @param {string} method
   * @param {any} params
   * @param {number} timeoutMs
   */
  request(method, params, timeoutMs = 30000) {
    if (this.#closed) return Promise.reject(new Error("termx 连接已关闭"));
    const id = `p-${++this.#nextId}-${Date.now()}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(id);
        reject(new Error(`termx 请求超时：${method}`));
      }, timeoutMs);
      this.#pending.set(id, { resolve, reject, timer });
      this.#ws.send(JSON.stringify({ id, method, params }));
    });
  }

  /**
   * 会话心跳：刷新服务端 `root_session_id` 的存活时间，用于终端保活。
   * @param {string} rootSessionId
   */
  heartbeat(rootSessionId) {
    if (!rootSessionId) return Promise.resolve();
    return this.request("heartbeat", { root_session_id: rootSessionId }, HEARTBEAT_TIMEOUT_MS);
  }

  /**
   * 会话已关闭：立即移除服务端存活记录，无需等待 TTL。
   * @param {string} rootSessionId
   */
  sessionClosed(rootSessionId) {
    if (!rootSessionId) return Promise.resolve();
    return this.request("session_closed", { root_session_id: rootSessionId }, HEARTBEAT_TIMEOUT_MS);
  }

  /** 订阅事件（返回取消函数）。 */
  onEvent(handler) {    this.#handlers.add(handler);
    return () => this.#handlers.delete(handler);
  }

  /** 等待满足条件的首个事件。 */
  waitEvent(predicate, timeoutMs = 30000) {
    return new Promise((resolve, reject) => {
      const off = this.onEvent((ev) => {
        if (!predicate(ev)) return;
        clearTimeout(timer);
        off();
        resolve(ev);
      });
      const timer = setTimeout(() => { off(); reject(new Error("termx 等待事件超时")); }, timeoutMs);
    });
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    this.#stopHeartbeat();
    try { this.#ws.close(); } catch { /* 忽略关闭错误 */ }
    this.#failAll(new Error("termx 连接已关闭"));
  }

  /** 连接建立后启动定时心跳；unref 避免拖住 Node 进程退出。 */
  #startHeartbeat() {
    if (this.#heartbeat || this.#closed) return;
    const timer = setInterval(() => {
      if (this.#closed) {
        this.#stopHeartbeat();
        return;
      }
      // 心跳失败只停止心跳，不关闭连接、不影响正在进行的工具调用。
      // 携带本连接的 root_session_id，由该会话的连接负责保活。
      const params = this.#rootSessionId ? { root_session_id: this.#rootSessionId } : {};
      this.request("heartbeat", params, HEARTBEAT_TIMEOUT_MS).catch(() => this.#stopHeartbeat());
    }, HEARTBEAT_INTERVAL_MS);
    timer.unref?.();
    this.#heartbeat = timer;
  }

  /** 停止心跳；连接关闭或心跳失败时调用，可重复。 */
  #stopHeartbeat() {
    if (!this.#heartbeat) return;
    clearInterval(this.#heartbeat);
    this.#heartbeat = null;
  }

  #onMessage(raw) {
    let msg;
    try { msg = JSON.parse(typeof raw === "string" ? raw : Buffer.from(raw).toString("utf8")); }
    catch { return; }
    if (msg && msg.id) {
      const p = this.#pending.get(msg.id);
      if (!p) return;
      this.#pending.delete(msg.id);
      clearTimeout(p.timer);
      if (msg.ok) p.resolve(msg.result);
      else {
        const e = new Error(msg.error?.message || "termx 错误");
        e.code = msg.error?.code;
        e.retryable = msg.error?.retryable;
        // session_exists 会携带冲突的旧 terminal_id。
        e.terminalId = msg.error?.terminal_id;
        p.reject(e);
      }
      return;
    }
    if (msg && msg.type) {
      for (const h of this.#handlers) {
        try { h(msg); } catch { /* 单个处理器失败不影响其它 */ }
      }
    }
  }

  #failAll(err) {
    this.#stopHeartbeat();
    for (const [, p] of this.#pending) {
      clearTimeout(p.timer);
      p.reject(err);
    }
    this.#pending.clear();
    this.#closed = true;
  }
}

/**
 * 进程内连接缓存：按 endpoint + token + rootSessionId(+admin) 复用。
 *
 * 每个 opencode 会话必须有独立 WS 连接（连接级调用者身份来自 hello），
 * 因此不能只按 endpoint/token 复用。token 只用于键名比较，不写日志、不外泄。
 *
 * @param {{ endpoint: string, token: string, client?: string, timeoutMs?: number, rootSessionId?: string, admin?: boolean }} connection
 * @returns {Promise<TermxClient>}
 */
const clients = new Map();
export async function getClient(connection) {
  if (!connection?.endpoint || !connection?.token) {
    throw new Error("termx 连接缺少 endpoint/token");
  }
  const key = [
    connection.endpoint,
    connection.token,
    connection.rootSessionId || "",
    connection.admin ? "admin" : "user",
  ].join("\n");
  const hit = clients.get(key);
  if (hit && !hit.closed) return hit;

  const created = await connectWith(connection);
  clients.set(key, created);
  return created;
}

/** base64 解码为 Buffer。 */
export function fromB64(s) {
  return Buffer.from(s || "", "base64");
}

/** 构造 TerminalLocator。 */
export function locator({ terminal_id, name }, ctx) {
  const l = {};
  if (terminal_id) l.terminal_id = terminal_id;
  if (name) l.name = name;
  if (!l.terminal_id) l.root_session_id = ctx?.sessionID || process.env.TERMX_ROOT_SESSION_ID || "default";
  return l;
}

/** 轮询 status 直到终态（用于不依赖事件流的简单工具）。 */
export async function waitTerminal(client, terminalId, timeoutMs = 30000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const r = await client.request("terminal.status", { terminal_id: terminalId });
    const snap = r?.snapshot ?? r;
    if (["exited", "interrupted", "error"].includes(snap.state)) return snap;
    await new Promise((r2) => setTimeout(r2, 100));
  }
  throw new Error("等待终端终态超时");
}
