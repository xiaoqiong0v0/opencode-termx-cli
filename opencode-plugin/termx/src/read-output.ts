// read 输出布局与选择（纯函数，供 cli.ts 复用并便于单测）。
//
// 约定：
// - 正常 idle 结果只返回实际命令输出，不显示工具元数据；
// - busy/超时/error/best_effort 才在首行显示 `[termx read] ...`，空行后是输出/快照；
// - 首行字段是协议化标识（terminal/source/state/note），不做翻译。

/** read 的数据来源：buffer（当前命令实时快照）/ history（已完成命令历史）。 */
export type ReadSource = "buffer" | "history";

/** read 首行的来源：status 表示超时/错误等只有状态、没有输出。 */
export type ReadHeaderSource = ReadSource | "status";

/** read 首行的状态。 */
export type ReadHeaderState = "busy" | "idle" | "error";

/** read 首行的可选提示（部分快照 / 超时 / 静默完成 / 命令失败 / 仅已完成）。 */
export type ReadHeaderNote =
  | "partial"
  | "timeout"
  | "best_effort"
  | "failed"
  | "disconnected"
  | "completed-only";

/**
 * 生成 read 首行元数据。
 *
 * @param name 终端名
 * @param source 数据来源（buffer/history/status）
 * @param state 终端/结果状态
 * @param note 是否附加提示
 * @param incomplete 输出文本是否已知不完整（混合 host/native、丢弃或屏障失败）
 * @param interactive 命令执行期间是否曾等待输入（弱语义）
 * @returns `[termx read] terminal=... source=... state=...[ incomplete=true][ interactive=true][ note=...]`
 */
export function formatReadHeader(
  name: string,
  source: ReadHeaderSource,
  state: ReadHeaderState,
  note?: ReadHeaderNote,
  incomplete = false,
  interactive = false,
): string {
  const header = `[termx read] terminal=${name} source=${source} state=${state}${
    incomplete ? " incomplete=true" : ""
  }${interactive ? " interactive=true" : ""}`;
  return note ? `${header} note=${note}` : header;
}

/**
 * 统一 read 输出布局。
 *
 * @param header 元数据行（含 `[termx read]`）
 * @param output 实际命令输出/快照
 * @param emptyText 无输出时的提示
 * @param showHeader 是否显示工具元数据
 * @returns header + 空行 + output（或 emptyText）；showHeader=false 时只返回 output/emptyText
 */
export function formatReadResult(
  header: string,
  output: string,
  emptyText: string,
  showHeader = true,
): string {
  const actual = output.trim() ? output : emptyText;
  return showHeader ? `${header}\n\n${actual}` : actual;
}

/**
 * 多命令结果块的元数据（只来自 transcript，绝不包含命令文本）。
 */
export interface ResultBlockMeta {
  /** transcript 状态（done/error/interrupted/...）。 */
  status?: string;
  /** 退出码；非数字时忽略。 */
  exitCode?: number | null;
  /** 静默完成（完成判定不确定）。 */
  bestEffort?: boolean;
  /** 输出文本已知不完整（混合 host/native、丢弃或屏障失败）。 */
  incomplete?: boolean;
  /** 命令执行期间曾等待输入（弱语义，输出里可能含提示与输入回显）。 */
  interactive?: boolean;
  /** 输出文本已按 `--last` 总字节预算截断（保留末尾）。 */
  truncated?: boolean;
}

/**
 * 生成稳定的结果块标记：`--- termx result i/N [flags] ---`。
 *
 * flags 只含紧凑 ASCII 状态（error/interrupted/exit_code/best_effort/incomplete/
 * interactive），绝不包含命令文本。
 *
 * @param index 当前块序号（从 1 开始）
 * @param total 本次返回的块总数
 * @param meta 结果元数据
 * @returns 单行标记
 */
export function formatResultMarker(
  index: number,
  total: number,
  meta?: ResultBlockMeta,
): string {
  const flags = resultFlags(meta);
  const suffix = flags.length ? ` [${flags.join(" ")}]` : "";
  return `--- termx result ${index}/${total}${suffix} ---`;
}

/**
 * 生成一条命令的结果块：标记行 + 输出（无输出时用本地化提示）。
 *
 * @param index 当前块序号（从 1 开始）
 * @param total 本次返回的块总数
 * @param output 已清洗的命令输出（可含尾部换行）
 * @param emptyText 无输出提示
 * @param meta 结果元数据
 * @returns 标记 + 换行 + 输出正文
 */
export function formatResultBlock(
  index: number,
  total: number,
  output: string,
  emptyText: string,
  meta?: ResultBlockMeta,
): string {
  const body = output.trim() ? output : emptyText;
  return `${formatResultMarker(index, total, meta)}\n${body}`;
}

/**
 * 用空行拼接多个结果块（空数组返回空字符串）。
 *
 * @param blocks 结果块数组
 * @returns 拼接后的文本
 */
export function joinResultBlocks(blocks: string[]): string {
  return blocks.join("\n\n");
}

/** 把结果元数据转为紧凑 flags；顺序固定，便于稳定断言。 */
function resultFlags(meta?: ResultBlockMeta): string[] {
  if (!meta) return [];
  const flags: string[] = [];
  const exitCode = typeof meta.exitCode === "number" ? meta.exitCode : undefined;
  if (meta.status === "interrupted") {
    flags.push("interrupted");
  } else if (meta.status === "error" || (exitCode !== undefined && exitCode !== 0)) {
    flags.push("error");
    if (exitCode !== undefined) flags.push(`exit_code=${exitCode}`);
  }
  if (meta.bestEffort) flags.push("best_effort");
  if (meta.incomplete) flags.push("incomplete");
  if (meta.interactive) flags.push("interactive");
  if (meta.truncated) flags.push("truncated");
  return flags;
}

/**
 * 取最后一个元素；空数组返回 undefined。
 *
 * @param items 数组
 * @returns 最后元素或 undefined
 */
export function lastItem<T>(items: T[]): T | undefined {
  return items.length ? items[items.length - 1] : undefined;
}

/**
 * `read --last` 多命令结果的总字节预算（UTF-8 字节，含标记行与块内容，
 * 不含丢弃说明行）。
 */
export const LAST_TOTAL_BUDGET_BYTES = 128 * 1024;

/** `--last` 结果块裁剪的输入：已清洗的命令输出 + 元数据（旧→新顺序传入）。 */
export interface LastResultItem {
  /** 已清洗、无尾部换行的命令输出。 */
  output: string;
  /** 结果元数据（截断 flag 由裁剪函数按需追加）。 */
  meta?: ResultBlockMeta;
}

/** UTF-8 字节数。 */
export function utf8ByteLength(text: string): number {
  return new TextEncoder().encode(text).length;
}

/**
 * 保留字符串**末尾**不超过 `maxBytes` 字节的子串，不切断多字节字符。
 *
 * @param text 原文
 * @param maxBytes 字节上限（<=0 返回空串）
 * @returns 截断后的末尾子串
 */
export function truncateUtf8End(text: string, maxBytes: number): string {
  if (maxBytes <= 0) return "";
  const bytes = new TextEncoder().encode(text);
  if (bytes.length <= maxBytes) return text;
  const tail = bytes.subarray(bytes.length - maxBytes);
  // 开头可能落在多字节字符的中间字节：跳过连续 continuation byte（0b10xxxxxx）。
  let start = 0;
  while (start < tail.length && (tail[start] & 0xc0) === 0x80) start += 1;
  return new TextDecoder().decode(tail.subarray(start));
}

/**
 * 按总字节预算格式化并裁剪 `--last` 的结果块。
 *
 * 策略：从最新块往回累积保留；超预算的旧块整体丢弃；最新单块自身超预算时
 * 保留其末尾（按字节截断）并加 truncated flag；被丢弃块数 > 0 时在结果最前
 * 加一行说明（dropNotice 的 `{count}` 占位符替换为丢弃数，说明行不计入预算）。
 *
 * @param items 结果块输入（旧→新顺序）
 * @param noOutput 无输出提示
 * @param budget 总字节预算（UTF-8 字节，含标记行与块间空行，不含说明行）
 * @param dropNotice 丢弃说明行模板（含 `{count}` 占位符；传空字符串则不加）
 * @returns 最终文本
 */
export function formatLastResultsBudgeted(
  items: LastResultItem[],
  noOutput: string,
  budget: number,
  dropNotice: string,
): string {
  const total = items.length;
  if (total === 0) return "";

  const blocks = items.map((item, index) =>
    formatResultBlock(index + 1, total, item.output, noOutput, item.meta),
  );

  // 从最新块往回累积；块间用 "\n\n"（2 字节）分隔。
  const kept: number[] = [total - 1];
  let used = utf8ByteLength(blocks[total - 1]);
  for (let index = total - 2; index >= 0; index -= 1) {
    const add = 2 + utf8ByteLength(blocks[index]);
    if (used + add > budget) break;
    used += add;
    kept.push(index);
  }
  kept.sort((a, b) => a - b);

  // 最新单块自身超预算：保留末尾 + truncated flag，其余旧块保持整体丢弃。
  const dropped = total - kept.length;
  let latest = blocks[total - 1];
  // 防御性分支：单条命令输出上限（64 KiB）低于本总预算（128 KiB），当前数据约束下不会触发。
  if (utf8ByteLength(latest) > budget) {
    const meta: ResultBlockMeta = {
      ...items[total - 1].meta,
      truncated: true,
    };
    const markerBytes = utf8ByteLength(formatResultMarker(total, total, meta));
    // 块格式为 `${marker}\n${body}`：正文可用字节需再扣除换行 1 字节。
    const contentBytes = Math.max(0, budget - markerBytes - 1);
    latest = formatResultBlock(
      total,
      total,
      truncateUtf8End(items[total - 1].output, contentBytes),
      noOutput,
      meta,
    );
  }

  const keptBlocks = kept.map((index) => (index === total - 1 ? latest : blocks[index]));
  const body = joinResultBlocks(keptBlocks);
  // dropNotice 为空字符串时按约定不加说明行（避免产生前导空行）。
  if (dropped === 0 || dropNotice === "") return body;
  const notice = dropNotice.replace(/\{count\}/g, String(dropped));
  return `${notice}\n\n${body}`;
}
