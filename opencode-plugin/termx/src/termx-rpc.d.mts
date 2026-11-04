// `termx-rpc.mjs` 的类型声明（供 TypeScript 插件源码使用）。
export type Locator = {
  terminal_id?: string;
  name?: string;
};

export type TermxEvent = {
  type: string;
  terminal_id?: string;
  seq?: number;
  [key: string]: unknown;
};

export type TermxConnection = {
  endpoint: string;
  token: string;
};

export type TermxClientOptions = {
  url: string;
  token: string;
  client?: string;
  timeoutMs?: number;
  /** 连接级调用者 opencode 会话 ID（hello.root_session_id）；普通连接必填。 */
  rootSessionId?: string;
  /** 是否为管理员连接（hello.admin）；仅 Web 代理/测试使用。 */
  admin?: boolean;
};

export type TermxConnectionOptions = {
  endpoint: string;
  token: string;
  client?: string;
  timeoutMs?: number;
  /** 连接级调用者 opencode 会话 ID（hello.root_session_id）。 */
  rootSessionId?: string;
  /** 是否为管理员连接（hello.admin）。 */
  admin?: boolean;
};

/** 从显式配置目录读取 endpoint/token（token 可由 TERMX_TOKEN 覆盖）。 */
export function discoverFrom(configDir: string, tokenFile?: string): TermxConnection;

/** 用显式 endpoint/token 建立连接并完成 hello。 */
export function connectWith(options: TermxConnectionOptions): Promise<TermxClient>;

/** 创建连接；`rootSessionId` 决定连接级访问身份。 */
export class TermxClient {
  constructor(ws: WebSocket, rootSessionId?: string);
  static connect(options: TermxClientOptions): Promise<TermxClient>;
  readonly closed: boolean;
  request<T = unknown>(method: string, params?: unknown, timeoutMs?: number): Promise<T>;
  /** 会话心跳：服务端据此决定终端是否仍可保活。 */
  heartbeat(rootSessionId: string): Promise<unknown>;
  /** 会话关闭：立即移除服务端存活记录。 */
  sessionClosed(rootSessionId: string): Promise<unknown>;
  onEvent(handler: (event: TermxEvent) => void): () => void;
  waitEvent(predicate: (event: TermxEvent) => boolean, timeoutMs?: number): Promise<TermxEvent>;
  close(): void;
}

/** 按 endpoint + token + rootSessionId(+admin) 复用进程内连接。 */
export function getClient(connection: TermxConnectionOptions): Promise<TermxClient>;
export function fromB64(data?: string): Buffer;
export function locator(input: Locator, ctx?: { sessionID?: string }): Record<string, unknown>;
export function waitTerminal(client: TermxClient, terminalId: string, timeoutMs?: number): Promise<unknown>;

export const HEARTBEAT_INTERVAL_MS: number;
