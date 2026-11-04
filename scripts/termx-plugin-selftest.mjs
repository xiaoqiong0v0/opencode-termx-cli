// termx 插件传输层自测：连接 service，跑 list/exec/status/terminate。
//
// 用法：node scripts/termx-plugin-selftest.mjs <config-dir>
//      或设置 TERMX_CONFIG_DIR；也可用 TERMX_ENDPOINT + TERMX_TOKEN 直连。
// 不再推导任何平台默认配置目录。

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { discoverFrom, connectWith, fromB64 } from "../opencode-plugin/termx/src/termx-rpc.mjs";

const configDir = process.argv[2] || process.env.TERMX_CONFIG_DIR;
const endpoint = (process.env.TERMX_ENDPOINT || "").trim();

let connection;
if (endpoint) {
  const tokenFile = configDir ? join(configDir, "token") : "";
  const token =
    (process.env.TERMX_TOKEN || "").trim() ||
    (tokenFile && existsSync(tokenFile) ? readFileSync(tokenFile, "utf8").trim() : "");
  connection = { endpoint, token };
} else if (configDir) {
  connection = discoverFrom(configDir);
} else {
  console.error("用法: node scripts/termx-plugin-selftest.mjs <config-dir>（或设置 TERMX_CONFIG_DIR / TERMX_ENDPOINT）");
  process.exit(2);
}

const c = await connectWith({ ...connection, client: "termx-selftest", rootSessionId: "selftest" });
console.log("connected", connection.endpoint);

const list0 = await c.request("terminal.list", {});
console.log("list0", JSON.stringify(list0));

const cmd = process.platform === "win32" ? "cmd.exe" : "/bin/sh";
const argv = process.platform === "win32" ? ["/C", "echo plugin_hi & exit 4"] : ["-c", "echo plugin_hi; exit 4"];
const created = await c.request("terminal.create", {
  mode: "stdio",
  cmd,
  args: argv,
  cwd: null,
  env: null,
  cols: null,
  rows: null,
  root_session_id: "selftest",
  name: "selftest",
  force: true,
  idle_timeout: null,
});
console.log("created", JSON.stringify(created));

let output = "";
const done = c.waitEvent(
  (ev) => ev.terminal_id === created.terminal_id && ["exited", "interrupted", "error"].includes(ev.type),
  15000,
);
await c.request("subscribe", { terminals: [{ terminal_id: created.terminal_id, cursor: created.cursor }] });
const off = c.onEvent((ev) => {
  if (ev.terminal_id !== created.terminal_id) return;
  if (ev.type === "output") output += fromB64(ev.data).toString("utf8");
});
const ev = await done;
off();
console.log("exit_event", JSON.stringify(ev));
console.log("output", JSON.stringify(output));

const st = await c.request("terminal.status", { terminal_id: created.terminal_id });
console.log("status", JSON.stringify(st));

const pruned = await c.request("terminal.prune", { states: ["exited", "interrupted", "error"], all: false });
console.log("prune", JSON.stringify(pruned));
c.close();
