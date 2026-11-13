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
 * @returns `[termx read] terminal=... source=... state=...[ incomplete=true][ note=...]`
 */
export function formatReadHeader(
  name: string,
  source: ReadHeaderSource,
  state: ReadHeaderState,
  note?: ReadHeaderNote,
  incomplete = false,
): string {
  const header = `[termx read] terminal=${name} source=${source} state=${state}${
    incomplete ? " incomplete=true" : ""
  }`;
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
}

/**
 * 生成稳定的结果块标记：`--- termx result i/N [flags] ---`。
 *
 * flags 只含紧凑 ASCII 状态（error/interrupted/exit_code/best_effort/incomplete），
 * 绝不包含命令文本。
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
