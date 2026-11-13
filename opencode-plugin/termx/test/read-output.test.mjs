// read --last 结果块纯函数测试：稳定标记、无命令文本、空输出与状态 flags。
// 运行：npm test（先 build，再用 node --test 跑本目录）。

import assert from "node:assert/strict";
import test from "node:test";

import {
  formatResultBlock,
  formatResultMarker,
  joinResultBlocks,
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
