// payload e2e：对 termx-service 验证 exec/send 的 --base64 / --file 载荷来源、
// 冲突与编码错误。只使用 termx-pwsh 终端。
//
// 用法：node scripts/e2e/payload-e2e.mjs <config-dir>
// 说明：<config-dir> 为 termx-service 的配置目录（含 port/token），由
// run-termx-e2e.ps1 驱动（启动 service → 传入 config 目录 → 收集退出码）。
// termx-pwsh 宿主只接受单行输入，多行文本会被拆成多条 transcript 记录，
// exec 无法匹配同一条完成记录；因此这里用「带引号 + 末尾 CRLF」的载荷验证
// CRLF→LF 与去外沿空白，多行保留由单元测试覆盖（test/payload.test.mjs）。
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { handleCliCommand } from "../../opencode-plugin/termx/dist/cli.js";
import { createI18n } from "../../opencode-plugin/termx/dist/i18n.js";
import { discoverFrom, getClient } from "../../opencode-plugin/termx/dist/termx-rpc.mjs";

const configDir = process.argv[2];
if (!configDir) {
  console.error("用法: node scripts/e2e/payload-e2e.mjs <config-dir>");
  process.exit(2);
}
const ctx = { sessionID: `payload-e2e-${Date.now()}`, ask: async () => {} };
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

/** 把文本编码为 base64 并放进引号参数（标准字母表无引号/反斜杠）。 */
function b64(text) {
  return Buffer.from(text, "utf8").toString("base64");
}

const connection = discoverFrom(configDir);
const client = await getClient({ ...connection, client: "payload-e2e", rootSessionId: ctx.sessionID });
const deps = {
  t,
  cursors: new Map(),
  exec: { timeoutMs: 15_000, pollMs: 200 },
  client: async () => client,
};
const run = (line) => handleCliCommand(line, ctx, deps);
const name = `payload-${Date.now()}`;
let workDir;
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
  workDir = await mkdtemp(join(tmpdir(), "termx-payload-e2e-"));
  const created = await run(`local --runtime termx-pwsh -n ${name}`);
  assert.match(created, /^terminal_id=/, `local failed: ${created}`);
  const interactive = await waitInteractive(run, name);
  assert.ok(interactive, "termx-pwsh did not reach waiting_input");
  assert.ok(await warmup(run, name), "termx-pwsh warmup exec failed");

  await check("exec --base64 keeps quotes and trims trailing CRLF", async () => {
    const command = "Write-Output 'alpha \"quoted\" beta'\r\n";
    const out = await run(`exec "${b64(command)}" --base64 -n ${name} --timeout 15000`);
    assert.equal(out.trim(), 'alpha "quoted" beta');
    assert.ok(!out.includes(b64(command)), "receipt echoed base64 text");
  });

  await check("exec --file reads UTF-8 (BOM + CRLF) on the plugin host", async () => {
    const file = join(workDir, "file-command.txt");
    await writeFile(file, "\uFEFFWrite-Output \"file 'quoted' ok\"\r\n", "utf8");
    const out = await run(`exec --file "${file}" -n ${name} --timeout 15000`);
    assert.equal(out.trim(), "file 'quoted' ok");
  });

  await check("send --base64 --enter writes the decoded text", async () => {
    const payload = `Write-Output "sent 'from' base64"`;
    const ack = await run(`send "${b64(payload)}" --base64 -n ${name} --enter`);
    assert.equal(ack, "ok");
    const read = await run(`read -n ${name} -s history`);
    assert.ok(read.includes(`sent 'from' base64`), `read did not show output: ${read}`);
  });

  await check("--base64 and --file conflict is reported", async () => {
    const out = await run(`exec "${b64("Write-Output 1")}" --base64 --file "${join(workDir, "x.txt")}" -n ${name}`);
    assert.match(out, /either --base64 or --file/);
  });

  await check("invalid base64 is rejected", async () => {
    const out = await run(`exec "abc" --base64 -n ${name}`);
    assert.match(out, /invalid base64 payload/);
  });

  await check("invalid UTF-8 file is rejected", async () => {
    const file = join(workDir, "invalid-utf8.txt");
    await writeFile(file, Buffer.from([0x57, 0x72, 0xc3, 0x28, 0x0a]));
    const out = await run(`exec --file "${file}" -n ${name}`);
    assert.match(out, /not valid UTF-8/);
  });

  await check("NUL-containing file is rejected", async () => {
    const file = join(workDir, "nul.txt");
    await writeFile(file, Buffer.from("Write-Output\u0000x", "utf8"));
    const out = await run(`exec --file "${file}" -n ${name}`);
    assert.match(out, /not valid UTF-8/);
  });

  await check("missing file reports the path without content", async () => {
    const file = join(workDir, "absent.txt");
    const out = await run(`exec --file "${file}" -n ${name}`);
    assert.match(out, /cannot read file/);
    assert.ok(out.includes(file));
  });
} finally {
  try {
    await run(`terminate -n ${name}`);
  } catch {
    // 终端可能已不存在
  }
  client.close();
  if (workDir) await rm(workDir, { recursive: true, force: true });
}

if (failures > 0) {
  console.error(`FAILED: ${failures} check(s)`);
  process.exit(1);
}
console.log("ALL OK");
