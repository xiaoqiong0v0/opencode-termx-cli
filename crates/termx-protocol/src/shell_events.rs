//! shell 带外事件（prompt / command 生命周期）的共享类型。
//!
//! 这些类型同时供本地事件通道（named pipe / Unix domain socket）、远端
//! helper 转发与 service 的 transcript 判定使用；本模块不依赖 OS。

/// shell 事件类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellEventKind {
    /// prompt 开始（`A`）。
    PromptStart,
    /// prompt 结束（`B`）。
    PromptEnd,
    /// 命令开始（`C`）。
    CommandStart,
    /// 命令结束（`D`）。
    CommandEnd,
    /// 命令取消（`X`）：多行输入未执行，撤销 `terminal.send` 留下的 pending 记录。
    CommandAbort,
}

impl ShellEventKind {
    /// 线上单字母事件名。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PromptStart => "A",
            Self::PromptEnd => "B",
            Self::CommandStart => "C",
            Self::CommandEnd => "D",
            Self::CommandAbort => "X",
        }
    }

    /// 解析单字母事件名；未知事件返回 `None`。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "A" => Some(Self::PromptStart),
            "B" => Some(Self::PromptEnd),
            "C" => Some(Self::CommandStart),
            "D" => Some(Self::CommandEnd),
            "X" => Some(Self::CommandAbort),
            _ => None,
        }
    }
}

/// 一条 shell 事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellEvent {
    /// 事件类型。
    pub kind: ShellEventKind,
    /// `D` 的退出码；其他事件为 `None`。
    pub code: Option<i32>,
    /// `C`/`D` 的 command-id；其他事件为 `None`。
    pub id: Option<String>,
    /// `C` 实际执行的完整命令文本；旧探针不填。
    pub command: Option<String>,
    /// `C` 命令开始的 Unix 毫秒时间戳（UTC）；旧探针不填。
    pub started_at_ms: Option<i64>,
    /// `D` 命令结束的 Unix 毫秒时间戳（UTC）；旧探针不填。
    pub ended_at_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 事件名与解析必须一一对应。
    #[test]
    fn kind_names_round_trip() {
        for kind in [
            ShellEventKind::PromptStart,
            ShellEventKind::PromptEnd,
            ShellEventKind::CommandStart,
            ShellEventKind::CommandEnd,
            ShellEventKind::CommandAbort,
        ] {
            assert_eq!(ShellEventKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(ShellEventKind::parse("E"), None);
        assert_eq!(ShellEventKind::parse("a"), None);
    }
}
