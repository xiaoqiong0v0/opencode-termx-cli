//! 远端注入授权与路径规则（协议层）。
//!
//! 本模块只定义**跨进程传输的形状**与**无副作用的路径校验**：
//! - [`InjectionConsent`] 作为 `consent_required` 错误载荷，告诉客户端本次将写入什么；
//! - [`InjectionPlan`] 由 service 根据配置与本次请求组装后交给 `termx-ssh`，
//!   不进入 JSON（见 `SshConfig::injection` 的 `#[serde(skip)]`）；
//! - [`validate_path_spec`] / [`expand_home`] 约束注入路径只能是绝对路径或
//!   `~/...`，展开时使用**远端用户 home**。
//!
//! 路径校验刻意不依赖 OS：远端可能是 Unix，主控端可能运行在 Windows。

use serde::{Deserialize, Serialize};

/// 注入授权的配置模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum InjectionConsentMode {
    /// 需要写入时先向用户申请授权（默认）。
    #[default]
    Ask,
    /// 自动授权写入。
    Allow,
    /// 拒绝一切写入。
    Deny,
}

impl InjectionConsentMode {
    /// 解析配置/环境变量字符串；无法识别时返回 `None`。
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "ask" => Some(Self::Ask),
            "allow" => Some(Self::Allow),
            "deny" => Some(Self::Deny),
            _ => None,
        }
    }

    /// 配置/日志使用的规范名。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }
}

/// `consent_required` 错误载荷：本次创建准备写入的远端文件。
///
/// 不含密码/token；`host`/`user` 仅用于客户端展示确认文案。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InjectionConsent {
    /// 目标主机（不含端口与凭据）。
    #[serde(default)]
    pub host: String,
    /// 登录用户。
    #[serde(default)]
    pub user: String,
    /// 本次需要写入的文件列表。
    #[serde(default)]
    pub writes: Vec<InjectionWrite>,
}

/// 需要写入的单个远端文件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InjectionWrite {
    /// 文件类型。
    pub kind: InjectionWriteKind,
    /// 写入动作。
    pub action: InjectionWriteAction,
    /// 需要写入的原因。
    pub reason: InjectionWriteReason,
    /// 展开后的目标绝对路径。
    pub path: String,
    /// 文件大小（字节）；未知时缺省。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// 文件 sha256（小写十六进制）；未知时缺省。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// 注入文件类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectionWriteKind {
    /// helper sidecar。
    Helper,
    /// 远端 runtime（pwsh/bash/sh）。
    Runtime,
}

/// 写入动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectionWriteAction {
    /// 目标不存在，创建新文件。
    Install,
    /// 目标已存在但不可用，替换并备份旧文件。
    Replace,
}

/// 需要写入的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectionWriteReason {
    /// 目标不存在。
    Missing,
    /// 远端摘要与期望值不一致。
    ChecksumMismatch,
    /// 文件存在但不可执行。
    NotExecutable,
}

/// service 组装、`termx-ssh` 消费的注入计划。
///
/// 该类型不实现 `Serialize`，作为 `SshConfig` 的内部字段时使用
/// `#[serde(skip)]`，不会进入客户端可见的 JSON。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InjectionPlan {
    /// 配置的授权模式。
    pub consent: InjectionConsentMode,
    /// 本次请求是否已由用户确认（`allow_injection=true`）。
    pub allow: bool,
    /// 注入根目录（原样，可能含 `~/`）。
    pub root: Option<String>,
    /// helper 安装目标（原样，可能含 `~/`）。
    pub helper: Option<String>,
    /// pwsh runtime 目录（原样）。
    pub pwsh: Option<String>,
    /// bash runtime 目录（原样）。
    pub bash: Option<String>,
    /// sh runtime 目录（原样）。
    pub sh: Option<String>,
}

/// 路径规范错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InjectionPathError {
    /// 空字符串或全空白。
    #[error("注入路径不能为空")]
    Empty,
    /// 含空白字符（含空格、制表符、换行）。
    #[error("注入路径不能包含空白字符")]
    Whitespace,
    /// 含控制字符。
    #[error("注入路径不能包含控制字符")]
    Control,
    /// 不是绝对路径，也不是 `~/...`。
    #[error("注入路径必须是绝对路径或以 ~/ 开头")]
    NotAbsolute,
    /// 含 `..` 路径组件。
    #[error("注入路径不能包含 .. 组件")]
    Traversal,
    /// 远端 HOME 缺失或不是合法的绝对路径。
    #[error("远端 HOME 不可用或不是合法绝对路径")]
    InvalidHome,
}

impl InjectionPathError {
    /// 简短、可安全写入日志的原因。
    pub fn reason(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Whitespace => "whitespace",
            Self::Control => "control",
            Self::NotAbsolute => "not_absolute",
            Self::Traversal => "traversal",
            Self::InvalidHome => "invalid_home",
        }
    }
}

/// 校验注入路径的形状。
///
/// 只允许：
/// - Unix 绝对路径（以 `/` 开头）；
/// - 以 `~/` 开头的路径（`~` 代表远端用户 home）。
///
/// 拒绝空值、任何空白/控制字符、`..` 路径组件，以及相对路径。
pub fn validate_path_spec(spec: &str) -> Result<(), InjectionPathError> {
    if spec.trim().is_empty() {
        return Err(InjectionPathError::Empty);
    }
    if spec.chars().any(char::is_whitespace) {
        return Err(InjectionPathError::Whitespace);
    }
    if spec.chars().any(char::is_control) {
        return Err(InjectionPathError::Control);
    }
    let is_absolute = spec.starts_with('/');
    let is_home_relative = spec.starts_with("~/");
    if !is_absolute && !is_home_relative {
        return Err(InjectionPathError::NotAbsolute);
    }
    if spec.split('/').any(|component| component == "..") {
        return Err(InjectionPathError::Traversal);
    }
    Ok(())
}

/// 用远端用户 home 展开 `~/...`；绝对路径原样返回。
///
/// `home` 必须是合法的绝对路径且不含 `..`；展开结果不含 `~/`。
pub fn expand_home(spec: &str, home: &str) -> Result<String, InjectionPathError> {
    validate_path_spec(spec)?;
    let Some(rest) = spec.strip_prefix("~/") else {
        return Ok(spec.to_string());
    };
    if home.trim().is_empty()
        || !home.starts_with('/')
        || home.chars().any(|ch| ch.is_whitespace() || ch.is_control())
        || home.split('/').any(|component| component == "..")
    {
        return Err(InjectionPathError::InvalidHome);
    }
    let home = home.trim_end_matches('/');
    let rest = rest.trim_start_matches('/');
    if rest.is_empty() {
        return Ok(if home.is_empty() {
            "/".to_string()
        } else {
            home.to_string()
        });
    }
    Ok(format!("{home}/{rest}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consent_mode_parses_and_round_trips() {
        for (raw, expected) in [
            ("ask", InjectionConsentMode::Ask),
            ("ALLOW", InjectionConsentMode::Allow),
            (" deny ", InjectionConsentMode::Deny),
        ] {
            assert_eq!(InjectionConsentMode::parse(raw), Some(expected));
            assert_eq!(expected.as_str(), expected.as_str());
        }
        assert_eq!(InjectionConsentMode::parse("bogus"), None);
        assert_eq!(InjectionConsentMode::default(), InjectionConsentMode::Ask);

        let json = serde_json::to_value(InjectionConsentMode::Allow).expect("序列化失败");
        assert_eq!(json, "allow");
        let back: InjectionConsentMode = serde_json::from_value(json).expect("反序列化失败");
        assert_eq!(back, InjectionConsentMode::Allow);
    }

    #[test]
    fn validate_accepts_absolute_and_home_relative_paths() {
        for spec in [
            "/usr/local/bin/termx-helper",
            "~/.termx/bin/termx-helper",
            "~/.termx/runtimes/pwsh",
            "/home/user/a..b",
            "/",
            "~/",
        ] {
            assert!(validate_path_spec(spec).is_ok(), "{spec:?} 应合法");
        }
    }

    #[test]
    fn validate_rejects_unsafe_paths() {
        let cases = [
            ("", InjectionPathError::Empty),
            ("   ", InjectionPathError::Empty),
            (" /usr/bin/x", InjectionPathError::Whitespace),
            ("/usr bin/x", InjectionPathError::Whitespace),
            ("/usr/bin/x\t", InjectionPathError::Whitespace),
            ("/usr/bin/x\n", InjectionPathError::Whitespace),
            ("/usr/bin/\u{1}", InjectionPathError::Control),
            ("usr/bin/x", InjectionPathError::NotAbsolute),
            ("~", InjectionPathError::NotAbsolute),
            ("~user/bin/x", InjectionPathError::NotAbsolute),
            ("~/../x", InjectionPathError::Traversal),
            ("/home/user/../x", InjectionPathError::Traversal),
            ("/a/../../b", InjectionPathError::Traversal),
        ];
        for (spec, expected) in cases {
            assert_eq!(validate_path_spec(spec), Err(expected), "路径 {spec:?}");
        }
    }

    #[test]
    fn expand_home_joins_remote_home() {
        assert_eq!(
            expand_home("~/.termx/bin/termx-helper", "/home/alice").expect("应展开"),
            "/home/alice/.termx/bin/termx-helper"
        );
        assert_eq!(
            expand_home("~//.termx/bin/termx-helper", "/home/alice/").expect("应展开"),
            "/home/alice/.termx/bin/termx-helper"
        );
        assert_eq!(
            expand_home("~/", "/home/alice").expect("应展开"),
            "/home/alice"
        );
        assert_eq!(expand_home("~/", "/").expect("root home 应展开"), "/");
        assert_eq!(
            expand_home("/opt/termx-helper", "/home/alice").expect("绝对路径原样"),
            "/opt/termx-helper"
        );
    }

    #[test]
    fn expand_home_rejects_invalid_home() {
        for home in ["", "  ", "relative/home", "/home/../x", "/home/al ice"] {
            assert_eq!(
                expand_home("~/x", home),
                Err(InjectionPathError::InvalidHome),
                "HOME {home:?}"
            );
        }
    }

    #[test]
    fn injection_write_round_trips_without_optional_fields() {
        let write = InjectionWrite {
            kind: InjectionWriteKind::Helper,
            action: InjectionWriteAction::Install,
            reason: InjectionWriteReason::Missing,
            path: "/home/alice/.termx/bin/termx-helper".to_string(),
            size: None,
            sha256: None,
        };
        let json = serde_json::to_value(&write).expect("序列化失败");
        assert_eq!(json["kind"], "helper");
        assert_eq!(json["action"], "install");
        assert_eq!(json["reason"], "missing");
        assert!(json.get("size").is_none());
        assert!(json.get("sha256").is_none());
        let back: InjectionWrite = serde_json::from_value(json).expect("反序列化失败");
        assert_eq!(back, write);
    }
}
