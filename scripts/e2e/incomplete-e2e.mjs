// P1 incomplete e2e：对 termx-service 验证混合（host+native）/纯 O/纯 native
// 输出时 transcript 的 incomplete 标记语义。
//
// 用法：node scripts/e2e/incomplete-e2e.mjs <config-dir>
// 说明：<config-dir> 为 termx-service 的配置目录（含 port/token），由
// run-termx-e2e.ps1 驱动（启动 service → 传入 config 目录 → 收集退出码）。
import assert from "node:assert/strict";

import { handleCliCommand } from "../../opencode-plugin/termx/dist/cli.js";
import { createI18n } from "../../opencode-plugin/termx/dist/i18n.js";
import { discoverFrom, getClient } from "../../opencode-plugin/termx/dist/termx-rpc.mjs";

const configDir = process.argv[2];
if (!configDir) {
  console.error("用法: node scripts/e2e/incomplete-e2e.mjs <config-dir>");
  process.exit(2);
}
const ctx = { sessionID: `incomplete-e2e-${Date.now()}`, ask: async () => {} };
const t = createI18n("en");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** 等待 termx-pwsh 进入可交互状态（启动约 6-10s）。 */
async function waitInteractive(run, name, limitMs = 30_000) {
  const deadline = Date.now() + limitMs;
  while (Date.now() < deadline) {
    const status = JSON.parse(await run(`status -n ${name}`));
    if (status.snapshot?.interactive === "waiting_input") return true;
    await sleep(500);
  }
  return false;
}

/** 用单行命令预热，失败重试；返回是否成功。 */
async function warmup(run, name, attempts = 4) {
  for (let i = 0; i < attempts; i++) {
    const out = await run(`exec "Write-Output 'warmup-ok'" -n ${name} --timeout 15000`);
    if (out.trim() === "warmup-ok") return true;
    await sleep(2000);
  }
  return false;
}

/** 读取最后一条 transcript 命令。 */
async function lastCommand(run, name) {
  const result = JSON.parse(await run(`transcript -n ${name} --json`));
  const commands = result.commands || [];
  return commands[commands.length - 1];
}

const connection = discoverFrom(configDir);
const client = await getClient({
  ...connection,
  client: "incomplete-e2e",
  rootSessionId: ctx.sessionID,
});
const deps = {
  t,
  cursors: new Map(),
  exec: { timeoutMs: 15_000, pollMs: 200 },
  client: async () => client,
};
const run = (line) => handleCliCommand(line, ctx, deps);
const name = `incomplete-${Date.now()}`;
let failures = 0;

/** 运行一条断言并记录结果。 */
async function check(label, fn) {
  try {
    await fn();
    console.log(`ok   ${label}`);
  } catch (error) {
    failures += 1;
    console.error(`FAIL ${label}: ${error instanceof Error ? error.message : error}`);
  }
}

try {
  const created = await run(`local --runtime termx-pwsh -n ${name}`);
  assert.match(created, /^terminal_id=/, `local failed: ${created}`);
  assert.ok(await waitInteractive(run, name), "termx-pwsh did not reach waiting_input");
  assert.ok(await warmup(run, name), "termx-pwsh warmup exec failed");

  await check("mixed host+native marks incomplete and does not duplicate", async () => {
    const text = await run(`exec "Write-Output host; cmd /c echo native" -n ${name} --timeout 15000`);
    assert.ok(text.includes("incomplete=true"), `missing incomplete header: ${text}`);
    assert.ok(!text.includes("best_effort=true"), `must not fabricate best_effort: ${text}`);
    assert.equal((text.match(/host/g) || []).length, 1, `host duplicated: ${text}`);
    assert.ok(!text.includes("native"), `native fallback must not be appended: ${text}`);
    const last = await lastCommand(run, name);
    assert.equal(last.incomplete, true, "transcript incomplete flag missing");
    assert.notEqual(last.best_effort, true, "incomplete must not set best_effort");
    assert.equal(typeof last.exit_code, "number", "real exit code expected");
    assert.equal(last.exit_code, 0);
  });

  await check("pure host output is not incomplete", async () => {
    const token = `pure-host-${Date.now()}`;
    const text = await run(`exec "Write-Output '${token}'" -n ${name} --timeout 15000`);
    assert.equal(text.trim(), token, `unexpected output: ${text}`);
    assert.ok(!text.includes("[termx exec]"), `normal success must be output-only: ${text}`);
    const last = await lastCommand(run, name);
    assert.notEqual(last.incomplete, true, "pure O must not be incomplete");
    assert.equal(last.output.trim(), token);
  });

  await check("pure native output is not incomplete", async () => {
    const token = `pure-native-${Date.now()}`;
    const text = await run(`exec "cmd /c echo ${token}" -n ${name} --timeout 15000`);
    assert.ok(text.includes(token), `native output missing: ${text}`);
    assert.ok(!text.includes("[termx exec]"), `normal native success must be output-only: ${text}`);
    const last = await lastCommand(run, name);
    assert.notEqual(last.incomplete, true, "pure PTY fallback must not be incomplete");
    assert.ok(last.output.includes(token), `native transcript missing: ${last.output}`);
  });

  await check("read --last marks incomplete block", async () => {
    const text = await run(`read --last 3 -n ${name}`);
    assert.ok(text.includes("[incomplete]"), `missing [incomplete] marker: ${text}`);
    assert.ok(!text.includes("[termx read]"), "normal idle must not show header");
  });
} finally {
  try {
    await run(`terminate -n ${name}`);
  } catch {
    // 已终止
  }
  client.close();
}

if (failures > 0) {
  console.error(`FAILED: ${failures} check(s)`);
  process.exitCode = 1;
} else {
  console.log("ALL OK");
}
