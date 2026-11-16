// read --last N e2e：对 termx-service 验证多命令结果块（稳定 marker、
// 泄漏防护、参数校验与 busy 等待上界）。
//
// 用法：node scripts/e2e/read-last-e2e.mjs <config-dir>
// 说明：<config-dir> 为 termx-service 的配置目录（含 port/token），由
// run-termx-e2e.ps1 驱动（启动 service → 传入 config 目录 → 收集退出码）。
import assert from "node:assert/strict";

import { handleCliCommand } from "../../opencode-plugin/termx/dist/cli.js";
import { createI18n } from "../../opencode-plugin/termx/dist/i18n.js";
import { discoverFrom, getClient } from "../../opencode-plugin/termx/dist/termx-rpc.mjs";

const configDir = process.argv[2];
if (!configDir) {
  console.error("用法: node scripts/e2e/read-last-e2e.mjs <config-dir>");
  process.exit(2);
}
const ctx = { sessionID: `read-last-e2e-${Date.now()}`, ask: async () => {} };
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

const connection = discoverFrom(configDir);
const client = await getClient({
  ...connection,
  client: "read-last-e2e",
  rootSessionId: ctx.sessionID,
});
const deps = {
  t,
  cursors: new Map(),
  exec: { timeoutMs: 15_000, pollMs: 200 },
  client: async () => client,
};
const run = (line) => handleCliCommand(line, ctx, deps);
const name = `readlast-${Date.now()}`;
const token = `CMDTOKEN_${Date.now()}_${Math.random().toString(16).slice(2)}`;
const outputs = ["alpha-read-last-1", "beta-read-last-2", "gamma-read-last-3"];
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

/** 断言文本不含命令文本/游标/URL，也没有正常 idle 的首行。 */
function assertNoCommandLeak(text) {
  assert.ok(!text.includes(token), "command text leaked");
  assert.ok(!text.includes("$ "), "$ command leaked");
  assert.ok(!text.includes("[cursor="), "cursor leaked");
  assert.ok(!/https?:\/\//.test(text), "URL leaked");
}

try {
  const created = await run(`local --runtime termx-pwsh -n ${name}`);
  assert.match(created, /^terminal_id=/, `local failed: ${created}`);
  assert.ok(await waitInteractive(run, name), "termx-pwsh did not reach waiting_input");
  assert.ok(await warmup(run, name), "termx-pwsh warmup exec failed");

  for (let i = 0; i < outputs.length; i += 1) {
    const out = await run(
      `exec "Write-Output '${outputs[i]}' # ${token}-${i + 1}" -n ${name} --timeout 15000`,
    );
    assert.equal(out.trim(), outputs[i], `exec ${i + 1} output mismatch: ${out}`);
    assert.ok(!out.includes(token), "exec echoed command text");
  }

  await check("read --last 3 returns all outputs with stable markers", async () => {
    const text = await run(`read --last 3 -n ${name}`);
    assert.ok(!text.includes("[termx read]"), "normal idle must not show header");
    assertNoCommandLeak(text);
    for (const out of outputs) assert.ok(text.includes(out), `missing ${out}`);
    assert.equal((text.match(/--- termx result \d+\/3 ---/g) || []).length, 3);
    assert.ok(text.indexOf(outputs[0]) < text.indexOf(outputs[1]));
    assert.ok(text.indexOf(outputs[1]) < text.indexOf(outputs[2]));
  });

  await check("read --last 2 (-l alias) returns the last two only", async () => {
    const text = await run(`read -l 2 -n ${name}`);
    assertNoCommandLeak(text);
    assert.ok(!text.includes(outputs[0]), "older output must be excluded");
    assert.ok(text.includes(outputs[1]) && text.includes(outputs[2]));
    assert.equal((text.match(/--- termx result \d+\/2 ---/g) || []).length, 2);
  });

  await check("read --last rejects N < 1", async () => {
    const text = await run(`read --last 0 -n ${name}`);
    assert.ok(text.includes("--last must be >= 1"), text);
  });

  await check("read --last rejects --includeCommand", async () => {
    const text = await run(`read --last 3 --includeCommand -n ${name}`);
    assert.ok(text.includes("--last cannot be combined"), text);
  });

  await check("read --last rejects -s", async () => {
    const text = await run(`read --last 2 -s history -n ${name}`);
    assert.ok(text.includes("--last cannot be combined"), text);
  });

  await check("read --last rejects legacy flags", async () => {
    const text = await run(`read --last 2 --tail-lines 5 -n ${name}`);
    assert.ok(text.includes("--last cannot be combined"), text);
  });

  await check("read --last busy wait is bounded and completed-only", async () => {
    await run(`exec "Start-Sleep -Seconds 6" -n ${name} --async`);
    let running = false;
    for (let i = 0; i < 25; i += 1) {
      const result = JSON.parse(await run(`transcript -n ${name} --json`));
      const commands = result.commands || [];
      running = commands[commands.length - 1]?.status === "running";
      if (running) break;
      await sleep(150);
    }
    assert.ok(running, "sleep command did not become running");

    const started = Date.now();
    const text = await run(`read --last 1 --timeout 300 -n ${name}`);
    const elapsed = Date.now() - started;
    assert.ok(
      text.includes("[termx read]") &&
        text.includes("source=history state=busy note=completed-only"),
      text,
    );
    assert.ok(text.includes(outputs[2]), "last completed output missing");
    assertNoCommandLeak(text);
    assert.ok(!text.includes("Start-Sleep"), "running command text leaked");
    assert.ok(elapsed < 4000, `bounded wait took ${elapsed}ms`);
    console.log(`busy wait elapsed=${elapsed}ms`);
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
