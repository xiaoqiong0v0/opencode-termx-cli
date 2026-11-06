//! 本地 PTY 单行命令的 shell integration marker。
//!
//! 服务端在写入前给受支持的 shell 命令追加一段不可见的 OSC `133;D;<exit_code>`
//! 输出；`termx-core` 的 transcript 扫描该序列得到真实退出码与命令结束时间。
//! 只有明确的本地单行命令会被包装：任何不确定（多行、后台/管道结尾、未闭合
//! 引号、cmd/未知 shell）一律跳过，宁可退回静默判定也不伪造完成。

/// 受支持的 shell 类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellKind {
    /// PowerShell（`pwsh` / `powershell.exe`）。
    Pwsh,
    /// POSIX shell（`bash` / `zsh` / `sh`）。
    Posix,
    /// `cmd.exe`（不支持 OSC，降级）。
    Cmd,
    /// 其他程序或 SSH 占位命令（`shell`）。
    Other,
}

/// POSIX shell 追加的 marker：保留 `$?` 后用 printf 输出 OSC 133;D。
const POSIX_MARKER_SUFFIX: &str = "; __termx_ec=$?; printf '\\033]133;D;%s\\007' \"$__termx_ec\"";

/// PowerShell 追加的 marker：优先 `$?`，否则取 `$LASTEXITCODE`，最后回落 1。
const PWSH_MARKER_SUFFIX: &str = r#"; $__termx_ec = if ($?) {0} elseif ($LASTEXITCODE -is [int]) {$LASTEXITCODE} else {1}; [Console]::Write("$([char]27)]133;D;$__termx_ec$([char]7)")"#;

/// 按可执行程序识别 shell 类型。
///
/// 只取路径最后一段并忽略大小写与 `.exe` 后缀；未知程序返回
/// [`ShellKind::Other`]（包括 SSH 的 `shell` 占位命令）。
pub fn shell_kind(command: &str) -> ShellKind {
    let trimmed = command.trim();
    let name = trimmed.rsplit(['/', '\\']).next().unwrap_or(trimmed);
    let name = name.to_ascii_lowercase();
    match name.strip_suffix(".exe").unwrap_or(&name) {
        "pwsh" | "powershell" => ShellKind::Pwsh,
        "bash" | "zsh" | "sh" => ShellKind::Posix,
        "cmd" => ShellKind::Cmd,
        _ => ShellKind::Other,
    }
}

/// 为单行命令追加 shell marker。
///
/// 返回 `Some(包装后的文本)` 表示可安全追加；返回 `None` 表示不包装（调用方
/// 保持原文本发送）。原命令文本保持不变，只在其后拼接后缀。
pub fn wrap_command(kind: ShellKind, command: &str) -> Option<String> {
    if !is_safe_single_line(command) {
        return None;
    }
    match kind {
        ShellKind::Posix => Some(format!("{command}{POSIX_MARKER_SUFFIX}")),
        ShellKind::Pwsh => Some(format!("{command}{PWSH_MARKER_SUFFIX}")),
        ShellKind::Cmd | ShellKind::Other => None,
    }
}

/// 判断命令是否适合追加 marker：非空、单行、无悬空运算符、引号闭合。
///
/// 这是保守检查而非完整 shell 解析：无法确定时返回 `false`（跳过）。
fn is_safe_single_line(command: &str) -> bool {
    if command.trim().is_empty() {
        return false;
    }
    if command.contains('\n') || command.contains('\r') {
        return false;
    }
    let trimmed = command.trim_end();
    if matches!(trimmed.chars().last(), Some('\\' | '&' | '|' | ';')) {
        return false;
    }
    !has_unclosed_quote(command)
}

/// 简单引号配对检测；只处理 `'`、`"` 与反斜杠转义，无法判断时按未闭合处理。
fn has_unclosed_quote(command: &str) -> bool {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        Single,
        Double,
    }

    let mut quote = None;
    let mut escaped = false;
    for ch in command.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        match quote {
            Some(Quote::Single) => {
                if ch == '\'' {
                    quote = None;
                }
            }
            Some(Quote::Double) => match ch {
                '\\' => escaped = true,
                '"' => quote = None,
                _ => {}
            },
            None => match ch {
                '\\' => escaped = true,
                '\'' => quote = Some(Quote::Single),
                '"' => quote = Some(Quote::Double),
                _ => {}
            },
        }
    }
    quote.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_common_shells_by_basename() {
        assert_eq!(shell_kind("pwsh"), ShellKind::Pwsh);
        assert_eq!(shell_kind("pwsh.exe"), ShellKind::Pwsh);
        assert_eq!(
            shell_kind("C:\\Program Files\\PowerShell\\7\\pwsh.exe"),
            ShellKind::Pwsh
        );
        assert_eq!(shell_kind("powershell.exe"), ShellKind::Pwsh);
        assert_eq!(shell_kind("/bin/bash"), ShellKind::Posix);
        assert_eq!(shell_kind("/usr/bin/zsh"), ShellKind::Posix);
        assert_eq!(shell_kind("sh"), ShellKind::Posix);
        assert_eq!(shell_kind("C:\\Windows\\System32\\cmd.exe"), ShellKind::Cmd);
        assert_eq!(shell_kind("shell"), ShellKind::Other);
        assert_eq!(shell_kind("python"), ShellKind::Other);
    }

    #[test]
    fn wraps_bash_and_pwsh_without_changing_original() {
        let bash = wrap_command(ShellKind::Posix, "echo hi").expect("应包装 bash");
        assert!(bash.starts_with("echo hi;"));
        assert!(bash.contains("__termx_ec=$?"));
        assert!(bash.contains("]133;D;%s"));
        assert!(bash.ends_with("\"$__termx_ec\""));

        let pwsh = wrap_command(ShellKind::Pwsh, "Write-Output hi").expect("应包装 pwsh");
        assert!(pwsh.starts_with("Write-Output hi;"));
        assert!(pwsh.contains("$LASTEXITCODE"));
        assert!(pwsh.contains("]133;D;"));
    }

    #[test]
    fn skips_unsupported_shells() {
        assert_eq!(wrap_command(ShellKind::Cmd, "echo hi"), None);
        assert_eq!(wrap_command(ShellKind::Other, "echo hi"), None);
    }

    #[test]
    fn skips_unsafe_commands() {
        for command in [
            "",
            "   ",
            "echo one\necho two",
            "echo one\r\necho two",
            "echo trailing\\",
            "sleep 1 &",
            "sleep 1 &&",
            "cat file |",
            "false ||",
            "echo one;",
            "echo 'unclosed",
            "echo \"unclosed",
        ] {
            assert_eq!(
                wrap_command(ShellKind::Posix, command),
                None,
                "应跳过: {command:?}"
            );
        }
    }

    #[test]
    fn wraps_closed_quotes_and_trailing_pipe_inside_quotes() {
        assert!(wrap_command(ShellKind::Posix, "echo 'a | b'").is_some());
        assert!(wrap_command(ShellKind::Posix, "echo \"a; b\"").is_some());
        assert!(wrap_command(ShellKind::Posix, "echo \"it's\"").is_some());
    }
}
