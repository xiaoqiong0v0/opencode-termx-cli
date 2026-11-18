// read --last 结果块纯函数测试：稳定标记、无命令文本、空输出与状态 flags。
// 运行：npm test（先 build，再用 node --test 跑本目录）。

import assert from "node:assert/strict";
import test from "node:test";

import {
  formatLastResultsBudgeted,
  formatReadHeader,
  formatResultBlock,
  formatResultMarker,
  joinResultBlocks,
  LAST_TOTAL_BUDGET_BYTES,
  truncateUtf8End,
  utf8ByteLength,
} from "../dist/read-output.js";

const EMPTY = "No output yet.";

test("plain marker uses stable ASCII numbering", () => {
  assert.equal(formatResultMarker(1, 3), "--- termx result 1/3 ---");
  assert.equal(formatResultMarker(3, 3), "--- termx result 3/3 ---");
});

test("block is marker + output and never includes command text", () => {
  const commandText = "Write-Output 'secret-command-token'";
  const block = formatResultBlock(1, 3, "alpha-output", EMPTY, { status: "done" });
  assert.equal(block, "--- termx result 1/3 ---\nalpha-output");
  assert.ok(!block.includes(commandText));
  assert.ok(!block.includes("$ "));
});

test("empty output falls back to the localized no-output text", () => {
  assert.equal(
    formatResultBlock(2, 2, "", EMPTY),
    "--- termx result 2/2 ---\nNo output yet.",
  );
  assert.equal(
    formatResultBlock(1, 1, "   \n", EMPTY),
    "--- termx result 1/1 ---\nNo output yet.",
  );
});

test("error, interrupted and best_effort flags live in the marker only", () => {
  assert.equal(
    formatResultBlock(1, 3, "boom", EMPTY, { status: "error", exitCode: 1 }),
    "--- termx result 1/3 [error exit_code=1] ---\nboom",
  );
  assert.equal(
    formatResultMarker(2, 3, { status: "interrupted" }),
    "--- termx result 2/3 [interrupted] ---",
  );
  assert.equal(
    formatResultMarker(3, 3, { bestEffort: true }),
    "--- termx result 3/3 [best_effort] ---",
  );
  assert.equal(
    formatResultMarker(1, 1, { status: "done", exitCode: 2 }),
    "--- termx result 1/1 [error exit_code=2] ---",
  );
  assert.equal(
    formatResultMarker(1, 1, { status: "done", exitCode: 0 }),
    "--- termx result 1/1 ---",
  );
});

test("incomplete flag is compact and lives in the marker", () => {
  assert.equal(
    formatResultMarker(1, 2, { status: "done", incomplete: true }),
    "--- termx result 1/2 [incomplete] ---",
  );
  assert.equal(
    formatResultMarker(2, 2, {
      status: "done",
      bestEffort: true,
      incomplete: true,
    }),
    "--- termx result 2/2 [best_effort incomplete] ---",
  );
  assert.equal(
    formatReadHeader("t", "history", "idle", "best_effort", true),
    "[termx read] terminal=t source=history state=idle incomplete=true note=best_effort",
  );
  assert.equal(
    formatReadHeader("t", "history", "idle"),
    "[termx read] terminal=t source=history state=idle",
  );
});

test("interactive flag is compact and lives in the marker", () => {
  assert.equal(
    formatResultMarker(1, 2, { status: "done", interactive: true }),
    "--- termx result 1/2 [interactive] ---",
  );
  assert.equal(
    formatResultMarker(2, 2, {
      status: "done",
      incomplete: true,
      interactive: true,
    }),
    "--- termx result 2/2 [incomplete interactive] ---",
  );
  assert.equal(
    formatReadHeader("t", "history", "idle", undefined, false, true),
    "[termx read] terminal=t source=history state=idle interactive=true",
  );
});

test("joinResultBlocks separates blocks with one blank line", () => {
  const blocks = [
    formatResultBlock(1, 2, "one", EMPTY),
    formatResultBlock(2, 2, "two", EMPTY),
  ];
  assert.equal(
    joinResultBlocks(blocks),
    "--- termx result 1/2 ---\none\n\n--- termx result 2/2 ---\ntwo",
  );
  assert.equal(joinResultBlocks([]), "");
});

// ---- read --last 总字节预算裁剪 ----

const DROP_NOTICE = "dropped {count}";

test("LAST_TOTAL_BUDGET_BYTES is 128 KiB", () => {
  assert.equal(LAST_TOTAL_BUDGET_BYTES, 128 * 1024);
});

test("budget within limit: no truncation, no dropped notice", () => {
  const items = [{ output: "alpha" }, { output: "beta" }];
  const result = formatLastResultsBudgeted(items, EMPTY, 1000, DROP_NOTICE);
  assert.equal(
    result,
    "--- termx result 1/2 ---\nalpha\n\n--- termx result 2/2 ---\nbeta",
  );
  assert.ok(!result.includes("dropped"));
  assert.ok(!result.includes("truncated"));
});

test("over budget: oldest blocks dropped whole + dropped notice", () => {
  // 最新块标记 "--- termx result 3/3 ---"（24 字节）+ 换行 + "ccc"（3 字节）= 28 字节。
  const budget = 28;
  const items = [{ output: "aaa" }, { output: "bbb" }, { output: "ccc" }];
  const result = formatLastResultsBudgeted(items, EMPTY, budget, DROP_NOTICE);
  assert.equal(result, "dropped 2\n\n--- termx result 3/3 ---\nccc");
});

test("newest single block exceeds budget: keep tail + truncated flag", () => {
  // 最新块无 flag 时 24+1+30=55 字节 > 50 → 截断。
  // 截断标记 "--- termx result 2/2 [truncated] ---"（36 字节）+ 换行 → 正文可留 13 字节。
  const budget = 50;
  const items = [{ output: "old" }, { output: "abcdefghijklmnopqrstuvwxyz0123" }];
  const result = formatLastResultsBudgeted(items, EMPTY, budget, DROP_NOTICE);
  assert.equal(result, "dropped 1\n\n--- termx result 2/2 [truncated] ---\nrstuvwxyz0123");
});

test("exactly equal to budget: no truncation", () => {
  // 标记 "--- termx result 1/1 ---"（24 字节）+ 换行 + 10 字节正文 = 35 字节。
  const budget = 35;
  const content = "0123456789";
  const result = formatLastResultsBudgeted([{ output: content }], EMPTY, budget, DROP_NOTICE);
  assert.equal(result, "--- termx result 1/1 ---\n0123456789");
  assert.ok(!result.includes("truncated"));
  assert.ok(!result.includes("dropped"));
});

test("utf8ByteLength counts multi-byte characters correctly", () => {
  assert.equal(utf8ByteLength(""), 0);
  assert.equal(utf8ByteLength("abc"), 3);
  assert.equal(utf8ByteLength("中"), 3);
  assert.equal(utf8ByteLength("中文"), 6);
  assert.equal(utf8ByteLength("😀"), 4);
  assert.equal(utf8ByteLength("a中😀b"), 1 + 3 + 4 + 1);
});

test("truncateUtf8End does not split multi-byte characters", () => {
  // 截断点落在 "你" 的中间字节：跳过 continuation byte 后应得到完整的 "好"。
  assert.equal(truncateUtf8End("你好", 4), "好");
  // 末尾落在 emoji 中间：尾部只含 emoji 的 continuation byte 时整体丢弃为空串。
  assert.equal(truncateUtf8End("a😀", 2), "");
  // 不截断时原样返回。
  assert.equal(truncateUtf8End("你好", 6), "你好");
});

test("truncateUtf8End result is always valid UTF-8 without replacement chars", () => {
  const text = "abc你好def😀xyz";
  for (const maxBytes of [1, 2, 3, 4, 5, 6, 7, 9, 12, 13, 17]) {
    const result = truncateUtf8End(text, maxBytes);
    // 合法 UTF-8 再编码后字节长度一致且无 U+FFFD，说明没有半个字符。
    assert.equal(utf8ByteLength(result), new TextEncoder().encode(result).length);
    assert.ok(!result.includes("\uFFFD"), `maxBytes=${maxBytes} produced U+FFFD`);
    // 结果必须是原文的末尾子串。
    assert.ok(text.endsWith(result) || result === "", `maxBytes=${maxBytes} not a suffix`);
  }
});

test("empty truncation body is replaced by the no-output text", () => {
  // 预算恰好只够 [truncated] 标记行 + 换行：正文可用字节为 0 → 截成空串 →
  // formatResultBlock 用 noOutput 提示替换（read-output.ts 的空正文回退路径）。
  // 标记 "--- termx result 1/1 [truncated] ---" 为 36 字节，预算 37 → contentBytes = 0。
  const budget = 37;
  const result = formatLastResultsBudgeted(
    [{ output: "a".repeat(50) }],
    EMPTY,
    budget,
    DROP_NOTICE,
  );
  assert.equal(result, "--- termx result 1/1 [truncated] ---\nNo output yet.");
});

test("empty dropNotice suppresses the notice line", () => {
  // 最新块标记 + 换行 + 3 字节 = 28 字节，旧块整体丢弃，但空 dropNotice 不加说明行。
  const budget = 28;
  const items = [{ output: "aaa" }, { output: "bbb" }, { output: "ccc" }];
  const result = formatLastResultsBudgeted(items, EMPTY, budget, "");
  assert.equal(result, "--- termx result 3/3 ---\nccc");
  assert.ok(!result.startsWith("\n"));
});
