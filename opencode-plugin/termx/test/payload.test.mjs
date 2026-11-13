// payload 单元测试：载荷来源（文本 / base64 / 本地文件）与严格 UTF-8 解码。
// 运行：npm test（先 build，再用 node --test 跑本目录）。

import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  expandLocalHome,
  normalizeExecText,
  PayloadError,
  resolvePayload,
} from "../dist/payload.js";

/** 断言是指定 code 的 PayloadError。 */
function isPayloadError(code) {
  return (error) => {
    assert.ok(error instanceof PayloadError, `expected PayloadError, got ${error}`);
    assert.equal(error.code, code);
    return true;
  };
}

test("text source is passed through verbatim and not raw", async () => {
  const payload = await resolvePayload("echo 'hi there'", {});
  assert.deepEqual(payload, { text: "echo 'hi there'", raw: false, source: "text" });
});

test("text source may be empty (caller decides the missing-argument error)", async () => {
  const payload = await resolvePayload("", {});
  assert.equal(payload.text, "");
  assert.equal(payload.source, "text");
});

test("base64 decodes quoted/multiline content", async () => {
  const original = `Write-Output 'a "b" c'\nWrite-Output "第二行"`;
  const encoded = Buffer.from(original, "utf8").toString("base64");
  const payload = await resolvePayload(encoded, { base64: true });
  assert.deepEqual(payload, { text: original, raw: true, source: "base64" });
});

test("base64 tolerates whitespace and newlines in the argument", async () => {
  const encoded = Buffer.from("echo hi", "utf8").toString("base64");
  const wrapped = `${encoded.slice(0, 4)}\n  ${encoded.slice(4, 8)}\r\n${encoded.slice(8)}`;
  const payload = await resolvePayload(wrapped, { base64: true });
  assert.equal(payload.text, "echo hi");
  assert.equal(payload.source, "base64");
});

test("base64 accepts standard padding", async () => {
  const payload = await resolvePayload("aGk=", { base64: true });
  assert.equal(payload.text, "hi");
});

test("base64 rejects invalid alphabet", async () => {
  await assert.rejects(resolvePayload("!!!not-base64!!!", { base64: true }), isPayloadError("base64"));
});

test("base64 rejects a length that is not a multiple of 4", async () => {
  await assert.rejects(resolvePayload("abc", { base64: true }), isPayloadError("base64"));
  await assert.rejects(resolvePayload("aGk", { base64: true }), isPayloadError("base64"));
});

test("base64 and file are mutually exclusive", async () => {
  await assert.rejects(
    resolvePayload("aGk=", { base64: true, file: "whatever.txt" }),
    isPayloadError("conflict"),
  );
});

test("empty base64 decodes to empty text (caller reports the missing payload)", async () => {
  const payload = await resolvePayload("", { base64: true });
  assert.equal(payload.text, "");
  assert.equal(payload.source, "base64");
});

test("file source reads UTF-8 text and strips one leading BOM", async () => {
  const dir = await mkdtemp(join(tmpdir(), "termx-payload-"));
  try {
    const file = join(dir, "cmd.txt");
    await writeFile(file, "\uFEFFWrite-Output '你好'\nWrite-Output second\n", "utf8");
    const payload = await resolvePayload("", { file });
    assert.equal(payload.source, "file");
    assert.equal(payload.raw, true);
    assert.equal(payload.text, "Write-Output '你好'\nWrite-Output second\n");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("file source keeps only one BOM (a second U+FEFF stays in the payload)", async () => {
  const dir = await mkdtemp(join(tmpdir(), "termx-payload-"));
  try {
    const file = join(dir, "double-bom.txt");
    await writeFile(file, "\uFEFF\uFEFFecho hi", "utf8");
    const payload = await resolvePayload("", { file });
    assert.equal(payload.text, "\uFEFFecho hi");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("file source rejects invalid UTF-8 instead of sending it", async () => {
  const dir = await mkdtemp(join(tmpdir(), "termx-payload-"));
  try {
    const file = join(dir, "utf16.txt");
    // UTF-16LE "echo hi" 的字节序列，作为 UTF-8 非法
    await writeFile(file, Buffer.from([0x65, 0x00, 0x63, 0x00, 0x68, 0x00, 0x6f, 0x00]));
    await assert.rejects(resolvePayload("", { file }), isPayloadError("decode"));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("file source rejects NUL bytes", async () => {
  const dir = await mkdtemp(join(tmpdir(), "termx-payload-"));
  try {
    const file = join(dir, "nul.txt");
    await writeFile(file, Buffer.from("echo\u0000hi", "utf8"));
    await assert.rejects(resolvePayload("", { file }), isPayloadError("decode"));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("file read failure reports a file error", async () => {
  await assert.rejects(
    resolvePayload("", { file: join(tmpdir(), "termx-does-not-exist-9b7f.txt") }),
    isPayloadError("file"),
  );
});

test("expandLocalHome expands ~, ~/ and ~\\ only at the start", () => {
  assert.equal(expandLocalHome("~"), homedir());
  assert.equal(expandLocalHome(join("~", "a", "b.txt")), join(homedir(), "a", "b.txt"));
  assert.equal(expandLocalHome("~\\a\\b.txt"), join(homedir(), "a", "b.txt"));
  assert.equal(expandLocalHome("~other/a.txt"), "~other/a.txt");
  assert.equal(expandLocalHome("a/~/b.txt"), "a/~/b.txt");
});

test("normalizeExecText converts CRLF to LF and trims outer whitespace", () => {
  assert.equal(normalizeExecText("echo a\r\necho b\r\n"), "echo a\necho b");
  assert.equal(normalizeExecText("\n  echo a\r\n  echo b  \n"), "echo a\n  echo b");
  assert.equal(normalizeExecText("   \r\n  "), "");
});
