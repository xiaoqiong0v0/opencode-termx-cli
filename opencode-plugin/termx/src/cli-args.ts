// CLI 参数分词：把 termx_cli 的整条 args 字符串拆成 token 数组。
//
// 口径（与旧 term_cli 一致）：
// - 空白分隔 token；单/双引号包裹的内容作为同一 token（引号本身不进入结果）；
// - 双引号内遇到反斜杠时保留反斜杠与其后一个字符，且不把 \" 当作结束引号；
// - 空引号 "" / '' 产生一个空 token；
// - 引号未闭合时由调用方拒绝执行并提示。

/** 分词空白匹配（空格/制表/换行等；无 g 标志，test 无状态）。 */
const WHITESPACE_RE = /\s/

/** 分词结果：tokens + 是否存在未闭合引号。 */
export interface CliArgsParseResult {
  tokens: string[]
  unclosedQuote: boolean
}

/**
 * 把 CLI 原始命令行字符串分词为参数数组，并报告未闭合引号。
 *
 * @param raw 原始命令行字符串
 * @returns 分词结果（引号已剥离，空引号保留为空字符串）
 */
export function parseCliArgs(raw: string): CliArgsParseResult {
  const tokens: string[] = []
  let buf = ""
  let hasToken = false
  let inSingle = false
  let inDouble = false

  for (let i = 0; i < raw.length; i++) {
    const ch = raw[i]

    if (inSingle) {
      if (ch === "'") inSingle = false
      else {
        buf += ch
        hasToken = true
      }
      continue
    }

    if (inDouble) {
      if (ch === '"') {
        inDouble = false
        continue
      }
      if (ch === "\\") {
        // 保留反斜杠与下一个字符（含 \"），后者不结束引号。
        const next = raw[i + 1]
        if (next !== undefined) {
          buf += ch + next
          i++
        } else {
          buf += ch
        }
        hasToken = true
        continue
      }
      buf += ch
      hasToken = true
      continue
    }

    if (WHITESPACE_RE.test(ch)) {
      if (hasToken) {
        tokens.push(buf)
        buf = ""
        hasToken = false
      }
      continue
    }
    if (ch === "'") {
      inSingle = true
      hasToken = true
      continue
    }
    if (ch === '"') {
      inDouble = true
      hasToken = true
      continue
    }
    buf += ch
    hasToken = true
  }

  if (hasToken) tokens.push(buf)
  return { tokens, unclosedQuote: inSingle || inDouble }
}

/**
 * 仅返回分词结果（不检查未闭合引号），供二次分词调用。
 *
 * @param raw 原始命令行字符串
 * @returns token 数组
 */
export function splitCliArgs(raw: string): string[] {
  return parseCliArgs(raw).tokens
}
