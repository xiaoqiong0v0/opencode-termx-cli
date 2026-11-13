// 载荷解析：termx exec / send 的输入来源（内联文本 / base64 / 本地文件）。
//
// 目的：复杂引号、目标 shell 转义以及以 "-" 开头的载荷，无法靠 CLI 外层分词
// 安全表达时，用 base64 或本地文件把 payload 原样带进来。
// 只读插件进程本机文件；不回显载荷内容（base64/文件可能含密码、密钥）。

import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";

/** 载荷来源：内联文本 / base64 / 本地文件。 */
export type PayloadSource = "text" | "base64" | "file";

/** 解析结果：最终文本 + 是否原样发送 + 来源（用于日志与回执，避免回显内容）。 */
export interface ResolvedPayload {
  /** 最终要发送或执行的文本。 */
  text: string;
  /** true = 原样使用（不解释转义）；text 模式为 false。 */
  raw: boolean;
  /** 载荷来源。 */
  source: PayloadSource;
}

/** 载荷解析错误码（调用方据此映射 i18n 文案）。 */
export type PayloadErrorCode = "conflict" | "base64" | "file" | "decode";

/**
 * 载荷解析错误：只携带机器可读 code，展示文案由调用方按语言生成。
 */
export class PayloadError extends Error {
  readonly code: PayloadErrorCode;

  /**
   * @param code 错误码（conflict / base64 / file / decode）
   * @param message 内部诊断信息（不直接展示给用户）
   */
  constructor(code: PayloadErrorCode, message: string) {
    super(message);
    this.name = "PayloadError";
    this.code = code;
  }
}

/** 严格 base64 字符集：仅标准字母表、末尾最多两个补位符（长度另校验 4 的倍数）。 */
const BASE64_RE = /^[A-Za-z0-9+/]*={0,2}$/;

/**
 * 展开开头的 ~（"~" / "~/" / "~\\"）为当前用户 home 目录；其他路径原样返回。
 * 仅用于插件进程本机路径；相对路径保持原样（由 readFile 按插件 cwd 解析）。
 * @param path 用户传入的文件路径
 * @returns 展开后的路径
 */
export function expandLocalHome(path: string): string {
  if (path === "~") return homedir();
  if (path.startsWith("~/") || path.startsWith("~\\")) return join(homedir(), path.slice(2));
  return path;
}

/**
 * 严格解码 UTF-8 载荷：拒绝非法字节序列与 NUL，剥掉开头的 UTF-8 BOM。
 * 目的：避免 UTF-16LE（PowerShell `Out-File` 默认）等非 UTF-8 文件被当成
 * “带 NUL 的乱码”静默发送，也避免 BOM 变成载荷首字符 U+FEFF。
 * @param bytes 原始字节
 * @param code 出错时归属的错误码（base64 / file）
 * @returns 解码后的文本（无 BOM）
 * @throws {PayloadError} 非法 UTF-8 或含 NUL
 */
function decodeUtf8(bytes: Buffer, code: "base64" | "file"): string {
  let text: string;
  try {
    // ignoreBOM=true：保留 BOM 由下面统一剥离，避免依赖 TextDecoder 的默认行为
    text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new PayloadError("decode", `invalid UTF-8 ${code} payload: ${reason}`);
  }
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  if (text.includes("\u0000")) {
    throw new PayloadError(
      "decode",
      `payload ${code} contains NUL (likely UTF-16 or binary); save it as UTF-8`,
    );
  }
  return text;
}

/**
 * 归一 exec 载荷：CRLF -> LF，并去掉外沿空白（含文件末尾换行）。
 * 只在 exec 的 base64/file 载荷上调用；内部换行保留，避免把多行脚本压成一行。
 * 去掉末尾换行是因为 exec 会另行发送一次 Enter，否则会多提交一条空命令。
 * send raw 必须保持原样，故不调用本函数。
 * @param text 载荷文本
 * @returns 归一后的命令文本
 */
export function normalizeExecText(text: string): string {
  return text.replace(/\r\n/g, "\n").trim();
}

/**
 * 解析 termx 的载荷来源。
 * - 默认：positional 原样作为文本，raw=false；
 * - --base64：忽略空白后严格校验并解码为 UTF-8 文本，raw=true；
 * - --file：读取插件进程本机的 UTF-8 文件，raw=true（不读远程文件）；
 * 两者都严格解码：剥开头的 U+FEFF，非法 UTF-8 或含 NUL 时报错（不静默发送）；
 * - --base64 与 --file 互斥。
 * 空载荷不在此报错（由调用方按缺失命令/文本文案处理）。
 * @param positional 位置参数拼接后的内联文本
 * @param opts base64 / file 选项
 * @returns 解析结果（文本、是否原样、来源）
 * @throws {PayloadError} 选项冲突、base64 非法、解码失败或文件读取失败
 */
export async function resolvePayload(
  positional: string,
  opts: { base64?: boolean; file?: string },
): Promise<ResolvedPayload> {
  if (opts.base64 && opts.file) {
    throw new PayloadError("conflict", "--base64 and --file are mutually exclusive");
  }

  if (opts.base64) {
    // 允许用户把 base64 按行/空格折行传入；去掉所有空白后再严格校验
    const compact = positional.replace(/\s+/g, "");
    if (!BASE64_RE.test(compact) || compact.length % 4 !== 0) {
      throw new PayloadError("base64", "invalid base64 payload");
    }
    return {
      text: decodeUtf8(Buffer.from(compact, "base64"), "base64"),
      raw: true,
      source: "base64",
    };
  }

  if (opts.file) {
    const resolved = expandLocalHome(opts.file);
    let bytes: Buffer;
    try {
      bytes = await readFile(resolved);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      throw new PayloadError("file", `cannot read ${resolved}: ${reason}`);
    }
    return { text: decodeUtf8(bytes, "file"), raw: true, source: "file" };
  }

  return { text: positional, raw: false, source: "text" };
}
