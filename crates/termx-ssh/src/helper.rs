//! SSH helper 预检、持久安装、握手与协议驱动（远端注入阶段一）。
//!
//! 流程（全部在已认证的 SSH 连接上完成）：
//! 1. `upload_helper=false`：只读预检 `helper_path`（目录/文件/摘要/可执行），
//!    有效直接使用；缺失/不符/不可写一律报 `InjectionFailed` 直接失败，不回退；
//! 2. `upload_helper=true`：先只读预检安装目标（不 mkdir、不写），已存在且校验
//!    通过则直接复用；需要写入时按 [`InjectionPlan`] 决策（deny/ask/allow）；
//! 3. 持久安装：`mkdir -p -m 700` 父目录 → 同目录 `mktemp` → 上传 → `sha256sum`
//!    校验 → `chmod 700` → 旧文件 `mv` 为 `.old.<ts>` → `mv` 原子替换；
//! 4. 以 `exec` 启动 `<helper> serve`，完成 `hello`/`spawn` 握手；
//! 5. 路径/权限/安装/写入失败均报 `InjectionFailed`，由调用方直接失败；
//!    仅 helper 协议/版本/握手错误允许回退 fallback。
//!
//! 安全约定：
//! - 密码/口令只存在于内存，不进入命令行；上传/校验命令只拼接路径与十六进制摘要；
//! - 只写配置的安装目标与同目录临时文件；不删除已安装文件，旧文件改名为
//!   `.old.<ts>` 保留；`consent_required` 时不创建目录、不写任何文件；
//! - 注入失败消息只包含目标路径与原因，不含 token/密码。

use std::{
    collections::{BTreeMap, VecDeque},
    sync::mpsc::{Sender, channel},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use russh::{
    ChannelMsg, ChannelReadHalf, ChannelWriteHalf,
    client::{self, Msg},
};
use sha2::{Digest, Sha256};
use termx_proc::{OutputStream, ProcEvent};
use termx_protocol::{
    Capabilities, HELPER_PROTOCOL_VERSION, InjectionConsent, InjectionConsentMode,
    InjectionPathError, InjectionPlan, InjectionWrite, InjectionWriteAction, InjectionWriteKind,
    InjectionWriteReason, ProcNode, SHELL_EVENTS_CAPABILITY, SshConfig, Stream, expand_home,
    helper::{HelperEvent, HelperRequest},
    shell_events::ShellEventKind,
    to_json, validate_path_spec,
};
use tokio::sync::mpsc;

use crate::{
    BEST_EFFORT_EXIT_CODE, HostKeyPolicy, SshConnectRequest, SshControl, SshError, map_russh_error,
    shell_quote,
};

/// 单次上传/校验/握手步骤的超时。
pub(crate) const HELPER_STEP_TIMEOUT: Duration = Duration::from_secs(10);

/// 上传 + 握手的整体上限；必须小于建连等待（见 `SshSession::connect`），
/// 让内层超时先返回并完成清理，避免外层等待与其同时到期。
pub(crate) const HELPER_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(60);

/// `terminate` 后等待终态事件的上限。
pub(crate) const HELPER_TERMINATE_TIMEOUT: Duration = Duration::from_secs(5);

/// 同步 `tree()` 等待远端响应的上限。
pub(crate) const TREE_WAIT: Duration = Duration::from_millis(2000);

/// 同目录临时文件名模板；`mktemp` 会替换尾部 `X`。
const REMOTE_TEMP_TEMPLATE: &str = ".termx-helper.XXXXXXXXXX";

/// 同目录临时文件前缀；清理时据此再次校验路径归属。
const REMOTE_TEMP_PREFIX: &str = ".termx-helper.";

/// 未知信号编号的占位值；`termx-core` 会把它映射为 `"UNKNOWN"`。
const UNKNOWN_SIGNAL: i32 = 0;

/// 远端路径预检结果。
#[derive(Debug, Clone, PartialEq, Eq)]
struct RemotePrecheck {
    /// 目标类型。
    kind: RemotePathKind,
    /// 是否为可执行文件。
    executable: bool,
    /// 文件 sha256（小写）；非文件或读取失败时为 `None`。
    sha256: Option<String>,
    /// 最近存在的祖先目录是否可写。
    parent_writable: bool,
}

impl RemotePrecheck {
    /// 目标是否已存在（含目录/其他类型）。
    fn exists(&self) -> bool {
        !matches!(self.kind, RemotePathKind::Missing)
    }
}

/// 预检得到的远端路径类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemotePathKind {
    /// 不存在。
    Missing,
    /// 普通文件。
    File,
    /// 目录。
    Directory,
    /// 其他类型（符号链接指向目录、设备等）。
    Other,
}

/// 是否需要写入的决策结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InjectionDecision {
    /// 已存在且校验通过，直接复用。
    Reuse,
    /// 按计划持久安装。
    Install,
    /// 需要用户授权。
    ConsentRequired,
    /// 不允许/无法写入；直接失败，不回退 fallback。
    Unavailable,
}

/// helper 会话：已握手并完成 `spawn`。
pub(crate) struct HelperSession {
    /// 协议通道。
    pub(crate) channel: HelperChannel,
    /// 远端根进程 ID。
    pub(crate) pid: u32,
    /// 远端进程组 ID。
    pub(crate) pgid: u32,
    /// helper 协议版本（`hello_ok.proto`）。
    pub(crate) proto: String,
    /// helper 是否声明 `shell_events` 能力（`hello_ok.capabilities`）。
    pub(crate) events_supported: bool,
    /// 本次会话是否实际建立带外事件通道（`started.events`）。
    pub(crate) events_active: bool,
    /// 握手期间提前读到的会话事件（输出等）。
    pub(crate) pending: Vec<HelperEvent>,
}

impl HelperSession {
    /// helper 模式能力：无降级。
    pub(crate) fn capabilities(&self) -> Capabilities {
        Capabilities {
            helper: true,
            version: self.proto.clone(),
            best_effort: false,
            runtime_catalog: false,
        }
    }

    /// 带外事件通道是否真正生效：能力已声明且本次 `started.events=true`。
    pub(crate) fn shell_events_active(&self) -> bool {
        self.events_supported && self.events_active
    }
}

/// 构造注入失败错误：只带目标路径与原因，不含任何凭据。
fn injection_failed(path: &str, message: impl Into<String>) -> SshError {
    SshError::InjectionFailed {
        path: path.to_string(),
        message: message.into(),
    }
}

/// 把安装阶段的任意错误归并为指向目标的注入失败。
fn injection_error_at(target: &str, error: SshError) -> SshError {
    match error {
        SshError::InjectionFailed { .. } => error,
        other => injection_failed(target, other.to_string()),
    }
}

/// 在已认证连接上尝试建立 helper 会话。
///
/// 返回 `Ok((连接句柄, helper 会话))` 继续驱动；失败时把连接句柄一并返回。
/// `ConsentRequired` 与 `InjectionFailed` 表示不能回退 fallback：前者需要用户
/// 授权，后者是路径/权限/安装/写入失败，调用方都必须原样上报。
///
/// # 错误
/// 参数非法返回 `BadRequest`；需要授权返回 `ConsentRequired`；预检失败、
/// 不可写、拒绝写入、安装/写入失败返回 `InjectionFailed`；握手/版本/`spawn`
/// 失败返回对应的可回退错误。
pub(crate) async fn bootstrap(
    handle: client::Handle<HostKeyPolicy>,
    config: &SshConfig,
    request: &SshConnectRequest,
) -> Result<(client::Handle<HostKeyPolicy>, HelperSession), (client::Handle<HostKeyPolicy>, SshError)>
{
    let spec = match helper_target(config) {
        Ok(spec) => spec,
        Err(error) => return Err((handle, error)),
    };
    let home = if spec.starts_with("~/") {
        match remote_home(&handle, &spec).await {
            Ok(home) => Some(home),
            Err(error) => return Err((handle, error)),
        }
    } else {
        None
    };
    let target = match resolve_target(&spec, home.as_deref()) {
        Ok(target) => target,
        Err(error) => return Err((handle, error)),
    };

    // 上传模式：本地读取/摘要校验没有远端副作用，失败可直接返回。
    let (bytes, digest) = if config.upload_helper {
        let Some(local_path) = config.helper_local_path.as_deref() else {
            return Err((
                handle,
                SshError::BadRequest("upload_helper=true 但缺少 helper_local_path".to_string()),
            ));
        };
        let bytes = match tokio::fs::read(local_path).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err((
                    handle,
                    injection_failed(&target, format!("读取本地 helper 失败: {error}")),
                ));
            }
        };
        let digest = sha256_hex(&bytes);
        if let Some(expected) = configured_sha(config) {
            if expected != digest {
                return Err((
                    handle,
                    injection_failed(&target, "本地 helper sha256 与 helper_sha256 不一致"),
                ));
            }
        }
        (Some(bytes), Some(digest))
    } else {
        (None, configured_sha(config))
    };

    // 写前只读预检：目录/文件/摘要/可执行检查，绝不 mkdir/写。
    let precheck = match precheck_remote(&handle, &target).await {
        Ok(precheck) => precheck,
        Err(error) => return Err((handle, error)),
    };

    if helper_usable(&precheck, digest.as_deref()) {
        return match open_and_spawn(&handle, &target, request).await {
            Ok(spawned) => Ok((handle, helper_session(spawned))),
            Err(error) => Err((handle, error)),
        };
    }

    // 只读模式无法写入：不可用即直接失败，不回退 fallback。
    if !config.upload_helper {
        return Err((
            handle,
            injection_failed(&target, precheck_problem(&precheck, digest.as_deref())),
        ));
    }
    if matches!(
        precheck.kind,
        RemotePathKind::Directory | RemotePathKind::Other
    ) {
        return Err((
            handle,
            injection_failed(&target, precheck_problem(&precheck, digest.as_deref())),
        ));
    }
    if !precheck.parent_writable {
        return Err((handle, injection_failed(&target, "目标目录不可写")));
    }

    match injection_decision(&config.injection, true) {
        InjectionDecision::Install => {}
        InjectionDecision::ConsentRequired => {
            let write = injection_write(
                &target,
                &precheck,
                digest.as_deref(),
                bytes.as_ref().map(Vec::len),
            );
            return Err((
                handle,
                SshError::ConsentRequired(InjectionConsent {
                    host: config.host.clone(),
                    user: config.user.clone(),
                    writes: vec![write],
                }),
            ));
        }
        InjectionDecision::Unavailable | InjectionDecision::Reuse => {
            return Err((
                handle,
                injection_failed(&target, "注入计划拒绝写入（consent=deny）"),
            ));
        }
    }

    let (Some(bytes), Some(digest)) = (bytes.as_deref(), digest.as_deref()) else {
        return Err((
            handle,
            SshError::BadRequest("upload_helper=true 但未读取到本地 helper".to_string()),
        ));
    };
    if let Err(error) = install_helper(&handle, &target, bytes, digest).await {
        return Err((handle, error));
    }

    match open_and_spawn(&handle, &target, request).await {
        Ok(spawned) => Ok((handle, helper_session(spawned))),
        Err(error) => Err((handle, error)),
    }
}

/// 把 `spawn` 结果转成会话；不再持有任何远端临时文件。
fn helper_session(spawned: SpawnOutcome) -> HelperSession {
    HelperSession {
        channel: spawned.channel,
        pid: spawned.pid,
        pgid: spawned.pgid,
        proto: spawned.proto,
        events_supported: spawned.events_supported,
        events_active: spawned.events_active,
        pending: spawned.pending,
    }
}

/// 解析 helper 安装目标：显式 `helper_path` 优先，其次计划里的 `helper`。
fn helper_target(config: &SshConfig) -> Result<String, SshError> {
    let spec = config
        .helper_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .or_else(|| {
            config
                .injection
                .helper
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
        })
        .ok_or_else(|| {
            SshError::BadRequest(
                "未配置 helper 安装路径（helper_path 或注入计划的 helper）".to_string(),
            )
        })?;
    validate_path_spec(spec)
        .map_err(|error| SshError::BadRequest(format!("helper 路径非法: {error}")))?;
    if spec.ends_with('/') {
        return Err(SshError::BadRequest(
            "helper 安装路径必须是文件路径，不能以 / 结尾".to_string(),
        ));
    }
    Ok(spec.to_string())
}

/// 用远端 HOME 展开 `~/...`；绝对路径原样返回。
fn resolve_target(spec: &str, home: Option<&str>) -> Result<String, SshError> {
    if !spec.starts_with("~/") {
        return Ok(spec.to_string());
    }
    let Some(home) = home else {
        return Err(injection_failed(spec, "无法解析远端 HOME"));
    };
    expand_home(spec, home).map_err(|error| match error {
        InjectionPathError::InvalidHome => injection_failed(spec, "远端 HOME 非法"),
        other => SshError::BadRequest(format!("helper 路径非法: {other}")),
    })
}

/// 读取远端用户 HOME；优先 `$HOME`，缺失时用登录 shell 的 `~` 展开。
async fn remote_home(
    handle: &client::Handle<HostKeyPolicy>,
    spec: &str,
) -> Result<String, SshError> {
    let (code, stdout) = exec_step(handle, "printf '%s' \"$HOME\"", "读取远端 HOME").await?;
    let home = String::from_utf8_lossy(&stdout).trim().to_string();
    if code.unwrap_or(0) == 0 && !home.is_empty() {
        return Ok(home);
    }

    let (code, stdout) = exec_step(handle, "cd ~ && pwd", "解析远端 HOME").await?;
    let home = String::from_utf8_lossy(&stdout).trim().to_string();
    if code.unwrap_or(0) == 0 && !home.is_empty() {
        return Ok(home);
    }
    Err(injection_failed(spec, "无法解析远端 HOME"))
}

/// 只读预检远端目标；任何远程命令失败都视为注入失败（直接失败）。
async fn precheck_remote(
    handle: &client::Handle<HostKeyPolicy>,
    path: &str,
) -> Result<RemotePrecheck, SshError> {
    let script = precheck_script(path);
    let (code, stdout) = exec_script(handle, &script, "预检 helper").await?;
    if code.unwrap_or(0) != 0 {
        return Err(injection_failed(
            path,
            format!("预检失败（退出码 {}）", code.unwrap_or(-1)),
        ));
    }
    Ok(parse_precheck_output(&String::from_utf8_lossy(&stdout)))
}

/// 预检脚本：只读 `test` / `sha256sum`，不创建任何目录或文件。
fn precheck_script(path: &str) -> String {
    format!(
        r#"p={path}
if [ -d "$p" ]; then echo kind=dir
elif [ -f "$p" ]; then echo kind=file
elif [ -e "$p" ]; then echo kind=other
else echo kind=missing
fi
if [ -x "$p" ]; then echo exec=1; else echo exec=0; fi
if [ -f "$p" ]; then h=$(sha256sum "$p" 2>/dev/null | awk '{{print $1}}'); echo "sha=$h"; fi
d=$(dirname "$p")
while [ ! -e "$d" ] && [ "$d" != "/" ] && [ "$d" != "." ]; do d=$(dirname "$d"); done
if [ -d "$d" ] && [ -w "$d" ]; then echo writable=1; else echo writable=0; fi
"#,
        path = shell_quote(path)
    )
}

/// 解析预检输出；未知/缺失的键使用安全默认值。
fn parse_precheck_output(output: &str) -> RemotePrecheck {
    let mut kind = RemotePathKind::Missing;
    let mut executable = false;
    let mut sha256 = None;
    let mut parent_writable = false;
    for line in output.lines() {
        let Some((key, value)) = line.trim().split_once('=') else {
            continue;
        };
        match key {
            "kind" => {
                kind = match value {
                    "file" => RemotePathKind::File,
                    "dir" => RemotePathKind::Directory,
                    "other" => RemotePathKind::Other,
                    _ => RemotePathKind::Missing,
                };
            }
            "exec" => executable = value == "1",
            "writable" => parent_writable = value == "1",
            "sha" if !value.is_empty() => sha256 = Some(value.to_ascii_lowercase()),
            _ => {}
        }
    }
    RemotePrecheck {
        kind,
        executable,
        sha256,
        parent_writable,
    }
}

/// 目标是否已存在且满足使用条件。
fn helper_usable(precheck: &RemotePrecheck, expected_sha: Option<&str>) -> bool {
    precheck.kind == RemotePathKind::File
        && precheck.executable
        && expected_sha.is_none_or(|expected| precheck.sha256.as_deref() == Some(expected))
}

/// 生成预检失败的可读原因（只含路径相关事实，不含凭据）。
fn precheck_problem(precheck: &RemotePrecheck, expected_sha: Option<&str>) -> String {
    match precheck.kind {
        RemotePathKind::Missing => "目标路径不存在且当前不允许写入".to_string(),
        RemotePathKind::Directory => "目标路径是目录".to_string(),
        RemotePathKind::Other => "目标路径不是普通文件".to_string(),
        RemotePathKind::File if !precheck.executable => "目标文件不可执行".to_string(),
        RemotePathKind::File => match expected_sha {
            Some(expected) if precheck.sha256.as_deref() != Some(expected) => {
                "目标文件 sha256 与 helper_sha256 不一致".to_string()
            }
            _ if !precheck.parent_writable => "目标目录不可写".to_string(),
            _ => "目标文件不可复用".to_string(),
        },
    }
}

/// 按 plan 决策：复用/安装/需要授权/不可用。
fn injection_decision(plan: &InjectionPlan, needs_write: bool) -> InjectionDecision {
    if !needs_write {
        return InjectionDecision::Reuse;
    }
    match plan.consent {
        InjectionConsentMode::Deny => InjectionDecision::Unavailable,
        InjectionConsentMode::Ask if !plan.allow => InjectionDecision::ConsentRequired,
        InjectionConsentMode::Ask | InjectionConsentMode::Allow => InjectionDecision::Install,
    }
}

/// 构造 `consent_required` 的单条写入描述。
fn injection_write(
    target: &str,
    precheck: &RemotePrecheck,
    digest: Option<&str>,
    size: Option<usize>,
) -> InjectionWrite {
    let action = if precheck.exists() {
        InjectionWriteAction::Replace
    } else {
        InjectionWriteAction::Install
    };
    let reason = match precheck.kind {
        RemotePathKind::Missing => InjectionWriteReason::Missing,
        RemotePathKind::File if !precheck.executable => InjectionWriteReason::NotExecutable,
        _ => InjectionWriteReason::ChecksumMismatch,
    };
    InjectionWrite {
        kind: InjectionWriteKind::Helper,
        action,
        reason,
        path: target.to_string(),
        size: size.map(|size| size as u64),
        sha256: digest.map(str::to_string),
    }
}

/// 同目录临时文件是否由本模块创建（路径归属 + 前缀 + 无控制字符）。
fn is_safe_temp_file(temp: &str, parent: &str) -> bool {
    let parent = parent.trim_end_matches('/');
    let prefix = if parent.is_empty() {
        "/".to_string()
    } else {
        format!("{parent}/")
    };
    if !temp.starts_with(&prefix) {
        return false;
    }
    let name = &temp[prefix.len()..];
    !name.is_empty()
        && name.starts_with(REMOTE_TEMP_PREFIX)
        && name.len() > REMOTE_TEMP_PREFIX.len()
        && !temp.chars().any(|ch| ch.is_whitespace() || ch.is_control())
        && !temp.split('/').any(|component| component == "..")
}

/// 安装目标文件的父目录；目标必须是文件路径（由 `helper_target` 保证）。
fn parent_dir(path: &str) -> String {
    match path.rsplit_once('/') {
        Some(("", _)) => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
        None => "/".to_string(),
    }
}

/// 持久安装：mkdir 700 → 同目录 mktemp → 上传 → sha256 → chmod → 备份旧文件 → 原子替换。
async fn install_helper(
    handle: &client::Handle<HostKeyPolicy>,
    target: &str,
    bytes: &[u8],
    digest: &str,
) -> Result<(), SshError> {
    let parent = parent_dir(target);
    let mkdir = format!("mkdir -p -m 700 {}", shell_quote(&parent));
    let (code, _) = exec_step(handle, &mkdir, "创建注入目录")
        .await
        .map_err(|error| injection_error_at(target, error))?;
    if code.unwrap_or(0) != 0 {
        return Err(injection_failed(
            target,
            format!(
                "创建注入目录失败（退出码 {}）: {parent}",
                code.unwrap_or(-1)
            ),
        ));
    }

    let template = format!("{}/{}", parent.trim_end_matches('/'), REMOTE_TEMP_TEMPLATE);
    let command = format!("mktemp {}", shell_quote(&template));
    let (code, stdout) = exec_step(handle, &command, "创建临时文件")
        .await
        .map_err(|error| injection_error_at(target, error))?;
    if code.unwrap_or(0) != 0 {
        return Err(injection_failed(
            target,
            format!(
                "创建临时文件失败（退出码 {}）: {parent}",
                code.unwrap_or(-1)
            ),
        ));
    }
    let temp = String::from_utf8_lossy(&stdout).trim().to_string();
    if !is_safe_temp_file(&temp, &parent) {
        tracing::warn!(temp = %temp, parent = %parent, "mktemp 返回了非预期的临时路径，拒绝继续");
        return Err(injection_failed(target, "mktemp 返回了非预期的临时路径"));
    }

    let installed = async {
        upload(handle, &temp, bytes)
            .await
            .map_err(|error| injection_error_at(target, error))?;
        verify_remote(handle, &temp, digest)
            .await
            .map_err(|error| injection_error_at(target, error))?;
        chmod_exec(handle, &temp)
            .await
            .map_err(|error| injection_error_at(target, error))?;
        replace_target(handle, target, &temp).await
    }
    .await;
    if let Err(error) = installed {
        // 失败只清理本次同目录临时文件；绝不删除已安装目标或旧备份。
        remove_temp(handle, &temp, &parent).await;
        return Err(error);
    }
    Ok(())
}

/// 备份已存在的目标并以 `mv` 原子替换为新文件。
async fn replace_target(
    handle: &client::Handle<HostKeyPolicy>,
    target: &str,
    temp: &str,
) -> Result<(), SshError> {
    let script = format!(
        r#"set -e
p={target}
t={temp}
if [ -e "$p" ]; then
  base="$p.old.$(date -u +%Y%m%d%H%M%S)"
  dest="$base"
  i=1
  while [ -e "$dest" ]; do dest="$base.$i"; i=$((i+1)); done
  mv "$p" "$dest"
fi
mv "$t" "$p"
"#,
        target = shell_quote(target),
        temp = shell_quote(temp)
    );
    let (code, _) = exec_script(handle, &script, "安装 helper")
        .await
        .map_err(|error| injection_error_at(target, error))?;
    if code.unwrap_or(0) != 0 {
        return Err(injection_failed(
            target,
            format!("替换 helper 失败（退出码 {}）", code.unwrap_or(-1)),
        ));
    }
    Ok(())
}

/// 删除本次创建的临时文件；路径不匹配时只记日志。
async fn remove_temp(handle: &client::Handle<HostKeyPolicy>, temp: &str, parent: &str) {
    if !is_safe_temp_file(temp, parent) {
        tracing::warn!(temp = %temp, "拒绝清理非本次创建的临时路径");
        return;
    }
    let command = format!("rm -f {}", shell_quote(temp));
    let cleaned = tokio::time::timeout(HELPER_STEP_TIMEOUT, exec_collect(handle, &command)).await;
    if !matches!(cleaned, Ok(Ok((Some(0), _)))) {
        tracing::debug!(temp = %temp, "清理临时 helper 文件未确认成功");
    }
}

/// `open_and_spawn` 的结果：已握手的通道、进程信息与事件通道状态。
struct SpawnOutcome {
    /// 协议通道。
    channel: HelperChannel,
    /// helper 协议版本。
    proto: String,
    /// 远端根进程 ID。
    pid: u32,
    /// 远端进程组 ID。
    pgid: u32,
    /// `spawn` 之后、`started` 之前提前到达的事件。
    pending: Vec<HelperEvent>,
    /// helper 是否声明 `shell_events` 能力。
    events_supported: bool,
    /// 本次会话是否实际建立带外事件通道。
    events_active: bool,
}

/// 打开 helper 通道、`exec serve`、握手并 `spawn`；失败时关闭通道。
async fn open_and_spawn(
    handle: &client::Handle<HostKeyPolicy>,
    command: &str,
    request: &SshConnectRequest,
) -> Result<SpawnOutcome, SshError> {
    let channel = handle
        .channel_open_session()
        .await
        .map_err(map_russh_error)?;
    channel
        .exec(true, helper_invocation(command).as_bytes())
        .await
        .map_err(map_russh_error)?;
    let mut helper_channel = HelperChannel::new(channel);

    let handshake_result = async {
        let (proto, events_supported) = handshake(&mut helper_channel).await?;
        let (pid, pgid, pending, events_active) =
            spawn_session(&mut helper_channel, request, events_supported).await?;
        Ok::<_, SshError>((proto, events_supported, pid, pgid, pending, events_active))
    }
    .await;

    match handshake_result {
        Ok((proto, events_supported, pid, pgid, pending, events_active)) => Ok(SpawnOutcome {
            channel: helper_channel,
            proto,
            pid,
            pgid,
            pending,
            events_supported,
            events_active,
        }),
        Err(error) => {
            helper_channel.close().await;
            Err(error)
        }
    }
}

/// 归一化配置中的期望摘要（小写；空串视为未配置）。
fn configured_sha(config: &SshConfig) -> Option<String> {
    config
        .helper_sha256
        .as_deref()
        .map(|sha| sha.trim().to_ascii_lowercase())
        .filter(|sha| !sha.is_empty())
}

/// 带单步超时执行命令；超时与错误都转成可回退的 `Connect` 错误。
async fn exec_step(
    handle: &client::Handle<HostKeyPolicy>,
    command: &str,
    stage: &str,
) -> Result<(Option<i32>, Vec<u8>), SshError> {
    match tokio::time::timeout(HELPER_STEP_TIMEOUT, exec_collect(handle, command)).await {
        Ok(result) => result,
        Err(_) => Err(SshError::Connect {
            message: format!("{stage} 超过 {} 秒", HELPER_STEP_TIMEOUT.as_secs()),
        }),
    }
}

/// 以 `sh -c` 执行脚本；单引号包裹整个脚本，避免远端登录 shell 二次解释。
async fn exec_script(
    handle: &client::Handle<HostKeyPolicy>,
    script: &str,
    stage: &str,
) -> Result<(Option<i32>, Vec<u8>), SshError> {
    let command = format!("sh -c {}", shell_quote(script));
    exec_step(handle, &command, stage).await
}

/// 上传本地字节到同目录临时文件。
///
/// 目标由 `mktemp` 唯一创建，直接截断写入；`umask 077` 与随后的 `chmod 700`
/// 限制访问。
async fn upload(
    handle: &client::Handle<HostKeyPolicy>,
    remote: &str,
    bytes: &[u8],
) -> Result<(), SshError> {
    let inner = format!("umask 077; cat > {}", shell_quote(remote));
    let command = format!("sh -c {}", shell_quote(&inner));
    let channel = handle
        .channel_open_session()
        .await
        .map_err(map_russh_error)?;
    channel
        .exec(true, command.as_bytes())
        .await
        .map_err(map_russh_error)?;
    let (mut read, write) = channel.split();

    // 顺序写入并等待窗口；远端 cat 持续消费，不会因为不读而对端阻塞。
    write
        .data_bytes(bytes.to_vec())
        .await
        .map_err(map_russh_error)?;
    write.eof().await.map_err(map_russh_error)?;

    let code = drain_exit_status(&mut read).await?;
    if code.unwrap_or(0) != 0 {
        return Err(SshError::Connect {
            message: format!("上传 helper 失败（退出码 {}）", code.unwrap_or(-1)),
        });
    }
    Ok(())
}

/// 远端 `chmod 700`；失败即视为 helper 不可用。
async fn chmod_exec(handle: &client::Handle<HostKeyPolicy>, remote: &str) -> Result<(), SshError> {
    let command = format!("chmod 700 {}", shell_quote(remote));
    let (code, _) = exec_step(handle, &command, "chmod 700").await?;
    if code.unwrap_or(0) != 0 {
        return Err(SshError::Connect {
            message: format!("chmod helper 失败（退出码 {}）", code.unwrap_or(-1)),
        });
    }
    Ok(())
}

/// 用 `sha256sum` 校验远端文件摘要。
async fn verify_remote(
    handle: &client::Handle<HostKeyPolicy>,
    remote: &str,
    expected: &str,
) -> Result<(), SshError> {
    let command = format!("sha256sum {}", shell_quote(remote));
    let (code, stdout) = exec_step(handle, &command, "sha256sum").await?;
    if code.unwrap_or(0) != 0 {
        return Err(SshError::Connect {
            message: format!("校验 helper 失败（退出码 {}）", code.unwrap_or(-1)),
        });
    }
    let actual = String::from_utf8_lossy(&stdout)
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if actual != expected {
        return Err(SshError::Connect {
            message: "helper sha256 校验失败".to_string(),
        });
    }
    Ok(())
}

/// 执行命令并收集 stdout 与退出码。
async fn exec_collect(
    handle: &client::Handle<HostKeyPolicy>,
    command: &str,
) -> Result<(Option<i32>, Vec<u8>), SshError> {
    let channel = handle
        .channel_open_session()
        .await
        .map_err(map_russh_error)?;
    channel
        .exec(true, command.as_bytes())
        .await
        .map_err(map_russh_error)?;
    let (mut read, _write) = channel.split();
    let mut stdout = Vec::new();
    let mut exit = None;
    loop {
        match read.wait().await {
            None => break,
            Some(ChannelMsg::Data { data }) => stdout.extend_from_slice(&data),
            Some(ChannelMsg::ExitStatus { exit_status }) => {
                exit = Some(exit_status as i32);
            }
            Some(ChannelMsg::Close) => break,
            Some(_) => {}
        }
    }
    Ok((exit, stdout))
}

/// 读取直到通道关闭，返回退出码。
async fn drain_exit_status(read: &mut ChannelReadHalf) -> Result<Option<i32>, SshError> {
    let mut exit = None;
    loop {
        match read.wait().await {
            None => break,
            Some(ChannelMsg::ExitStatus { exit_status }) => exit = Some(exit_status as i32),
            Some(ChannelMsg::Close) => break,
            Some(_) => {}
        }
    }
    Ok(exit)
}

/// helper 启动命令：安装路径已在校验阶段保证为不含空白/控制字符，按单一路径引用。
fn helper_invocation(path: &str) -> String {
    format!("{} serve", shell_quote(path.trim()))
}

/// 完成 `hello` / `hello_ok` 握手，返回 helper 协议版本与 `shell_events` 能力。
async fn handshake(channel: &mut HelperChannel) -> Result<(String, bool), SshError> {
    channel
        .send(&HelperRequest::Hello {
            proto: HELPER_PROTOCOL_VERSION.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        })
        .await?;

    let deadline = tokio::time::Instant::now() + HELPER_STEP_TIMEOUT;
    loop {
        let event = channel
            .next_event_until(deadline, "waiting hello_ok")
            .await?;
        match event {
            HelperEvent::HelloOk {
                proto,
                capabilities,
            } => {
                if proto != HELPER_PROTOCOL_VERSION {
                    return Err(SshError::BadRequest(format!("helper 协议不匹配: {proto}")));
                }
                let supported = capabilities
                    .iter()
                    .any(|capability| capability == SHELL_EVENTS_CAPABILITY);
                return Ok((proto, supported));
            }
            HelperEvent::Error { code, message } => {
                return Err(SshError::BadRequest(format!(
                    "helper 握手失败: {code:?} {message}"
                )));
            }
            other => tracing::debug!(event = ?other, "握手期间忽略事件"),
        }
    }
}

/// 发送 `spawn` 并等待 `started`；返回 pid/pgid、提前到达的事件与事件通道状态。
async fn spawn_session(
    channel: &mut HelperChannel,
    request: &SshConnectRequest,
    events_supported: bool,
) -> Result<(u32, u32, Vec<HelperEvent>, bool), SshError> {
    channel
        .send(&spawn_request(request, events_supported))
        .await?;

    let deadline = tokio::time::Instant::now() + HELPER_STEP_TIMEOUT;
    let mut pending = Vec::new();
    loop {
        let event = channel
            .next_event_until(deadline, "waiting started")
            .await?;
        match event {
            HelperEvent::Started { pid, pgid, events } => {
                return Ok((pid, pgid, pending, events && events_supported));
            }
            HelperEvent::Error { code, message } => {
                return Err(SshError::BadRequest(format!(
                    "helper spawn 失败: {code:?} {message}"
                )));
            }
            other => pending.push(other),
        }
    }
}

/// 远端 PTY 缺失 `TERM` 时的默认值；与 xterm.js 前端声明的 256 色能力一致。
///
/// 远端 TERM 为空时，PSReadLine 等行编辑器会退化为逐键处理（输入 `ls`
/// 显示为 `lsls`），补全/历史/预测同时失效。
const DEFAULT_REMOTE_TERM: &str = "xterm-256color";

/// 在 helper `spawn` 的环境覆盖表中补齐 `TERM`。
///
/// - 调用方显式提供非空 `TERM`（含 `dumb`）时原样保留；
/// - 缺失或空值才注入 `term`，`term` 本身为空时回退
///   [`DEFAULT_REMOTE_TERM`]。
///
/// `TERM=dumb` 视为用户显式选择，不在本层覆盖；该值会让行编辑/补全等能力
/// 退化，兼容性风险由调用方承担。
///
/// 只在 helper 模式使用；fallback 由 SSH `pty-req` 的 term 字段携带。
fn with_default_term(
    env: Option<BTreeMap<String, String>>,
    term: &str,
) -> Option<BTreeMap<String, String>> {
    let mut env = env.unwrap_or_default();
    let explicit = env
        .get("TERM")
        .is_some_and(|value| !value.trim().is_empty());
    if !explicit {
        let term = if term.trim().is_empty() {
            DEFAULT_REMOTE_TERM
        } else {
            term.trim()
        };
        env.insert("TERM".to_string(), term.to_string());
    }
    Some(env)
}

/// 把 `terminal.create` 的请求翻译为 helper `spawn`。
///
/// 只有 helper 声明了 `shell_events` 能力且请求本身需要事件时才置
/// `events=true`；旧 helper 不下发该字段，保持原有行为。
fn spawn_request(request: &SshConnectRequest, events_supported: bool) -> HelperRequest {
    let (cmd, args) = match &request.program {
        Some(program) => (program.cmd.clone(), program.args.clone()),
        None => match &request.mode {
            crate::SshChannelMode::Shell => ("/bin/sh".to_string(), Vec::new()),
            crate::SshChannelMode::Exec(command) => (
                "/bin/sh".to_string(),
                vec!["-c".to_string(), command.clone()],
            ),
        },
    };
    HelperRequest::Spawn {
        cmd,
        args,
        cwd: request.cwd.clone(),
        env: with_default_term(request.env.clone(), &request.term),
        cols: Some(request.cols),
        rows: Some(request.rows),
        events: events_supported && request.events,
    }
}

/// helper 协议通道：按行读取事件、按行写入请求。
pub(crate) struct HelperChannel {
    /// 读取半端。
    read: ChannelReadHalf,
    /// 写入半端。
    write: ChannelWriteHalf<Msg>,
    /// 尚未按行切分的读缓冲。
    buffer: Vec<u8>,
    /// 是否已观察到通道关闭。
    closed: bool,
}

impl HelperChannel {
    /// 从已 `exec` 的通道创建。
    pub(crate) fn new(channel: russh::Channel<Msg>) -> Self {
        let (read, write) = channel.split();
        Self {
            read,
            write,
            buffer: Vec::new(),
            closed: false,
        }
    }

    /// 发送一条请求（一行 JSON）。
    async fn send(&self, request: &HelperRequest) -> Result<(), SshError> {
        let line = to_json(request).map_err(|error| SshError::Connect {
            message: format!("helper 请求序列化失败: {error}"),
        })?;
        let mut payload = line.into_bytes();
        payload.push(b'\n');
        self.write
            .data_bytes(payload)
            .await
            .map_err(map_russh_error)
    }

    /// 读取下一条事件；通道关闭时返回 `None`。
    pub(crate) async fn next_event(&mut self) -> Option<HelperEvent> {
        loop {
            if let Some(line) = self.take_line() {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<HelperEvent>(&line) {
                    Ok(event) => return Some(event),
                    Err(error) => {
                        tracing::warn!(%error, "helper 帧不是合法 JSON，已跳过");
                    }
                }
            }
            if self.closed {
                return None;
            }
            match self.read.wait().await {
                None => {
                    self.closed = true;
                    return None;
                }
                Some(ChannelMsg::Data { data }) => self.buffer.extend_from_slice(&data),
                Some(ChannelMsg::ExtendedData { .. }) => {
                    // helper 日志固定写 stderr，这里只丢弃，避免污染协议流。
                }
                Some(ChannelMsg::ExitStatus { .. }) | Some(ChannelMsg::Close) => {
                    self.closed = true;
                }
                Some(_) => {}
            }
        }
    }

    /// 带截止时间读取下一条事件。
    async fn next_event_until(
        &mut self,
        deadline: tokio::time::Instant,
        stage: &str,
    ) -> Result<HelperEvent, SshError> {
        match tokio::time::timeout_at(deadline, self.next_event()).await {
            Ok(Some(event)) => Ok(event),
            Ok(None) => Err(SshError::Connect {
                message: format!("helper 在 {stage} 期间关闭"),
            }),
            Err(_) => Err(SshError::Connect {
                message: format!("helper 在 {stage} 超时"),
            }),
        }
    }

    /// 关闭通道（幂等）。
    pub(crate) async fn close(&mut self) {
        let _ = self.write.eof().await;
        let _ = self.write.close().await;
    }

    /// 从缓冲中取出一行（不含换行）。
    fn take_line(&mut self) -> Option<String> {
        let position = self.buffer.iter().position(|byte| *byte == b'\n')?;
        let line: Vec<u8> = self.buffer.drain(..=position).collect();
        let body = &line[..line.len().saturating_sub(1)];
        Some(
            String::from_utf8_lossy(body)
                .trim_end_matches('\r')
                .to_string(),
        )
    }
}

/// 调用方已放弃时关闭 helper 通道并断开连接。
pub(crate) async fn abort(handle: &client::Handle<HostKeyPolicy>, mut session: HelperSession) {
    session.channel.close().await;
    let _ = handle
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await;
}

/// helper 驱动：转发事件、执行控制消息，并在终态后关闭通道。
pub(crate) async fn drive(
    handle: client::Handle<HostKeyPolicy>,
    mut helper: HelperSession,
    events: mpsc::Sender<ProcEvent>,
    mut control: mpsc::UnboundedReceiver<SshControl>,
) {
    let mut incoming: VecDeque<HelperEvent> = helper.pending.drain(..).collect();
    let mut tree_waiters: VecDeque<Sender<Vec<ProcNode>>> = VecDeque::new();
    let mut terminal_sent = false;
    let mut terminating = false;
    let mut deadline = tokio::time::Instant::now() + HELPER_TERMINATE_TIMEOUT;

    loop {
        if let Some(event) = incoming.pop_front() {
            if handle_event(event, &events, &mut tree_waiters).await {
                terminal_sent = true;
                break;
            }
            continue;
        }

        tokio::select! {
            message = helper.channel.next_event() => match message {
                None => break,
                Some(event) => {
                    if handle_event(event, &events, &mut tree_waiters).await {
                        terminal_sent = true;
                        break;
                    }
                }
            },
            command = control.recv() => match command {
                None => break,
                Some(SshControl::Write(data)) => {
                    let request = HelperRequest::Input { data: STANDARD.encode(data) };
                    if helper.channel.send(&request).await.is_err() {
                        break;
                    }
                }
                Some(SshControl::Resize { cols, rows }) => {
                    if helper.channel.send(&HelperRequest::Resize { cols, rows }).await.is_err() {
                        break;
                    }
                }
                Some(SshControl::Signal(signal)) => {
                    let request = HelperRequest::Signal { signal: signal_name(&signal) };
                    if helper.channel.send(&request).await.is_err() {
                        break;
                    }
                }
                Some(SshControl::Terminate) => {
                    if !terminating {
                        terminating = true;
                        deadline = tokio::time::Instant::now() + HELPER_TERMINATE_TIMEOUT;
                        if helper.channel.send(&HelperRequest::Terminate).await.is_err() {
                            break;
                        }
                    }
                }
                Some(SshControl::Tree(reply)) => {
                    tree_waiters.push_back(reply);
                    if helper.channel.send(&HelperRequest::Tree).await.is_err() {
                        break;
                    }
                }
            },
            _ = tokio::time::sleep_until(deadline), if terminating => break,
        }
    }

    // 未完成的 tree 请求统一回空，避免调用方等到通道关闭。
    for waiter in tree_waiters.drain(..) {
        let _ = waiter.send(Vec::new());
    }
    if !terminal_sent {
        let _ = events.send(ProcEvent::Exited(BEST_EFFORT_EXIT_CODE)).await;
    }

    helper.channel.close().await;
    let _ = handle
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await;
}

/// 处理一条 helper 事件；返回 `true` 表示已是终态、驱动应退出。
async fn handle_event(
    event: HelperEvent,
    events: &mpsc::Sender<ProcEvent>,
    tree_waiters: &mut VecDeque<Sender<Vec<ProcNode>>>,
) -> bool {
    match event {
        HelperEvent::Output { data, stream } => {
            let Ok(bytes) = STANDARD.decode(data.as_bytes()) else {
                tracing::warn!("helper output.data 不是合法 base64，已丢弃");
                return false;
            };
            events
                .send(ProcEvent::Output {
                    data: bytes,
                    stream: output_stream(stream),
                })
                .await
                .is_err()
        }
        HelperEvent::Exited { code } => {
            let _ = events.send(ProcEvent::Exited(code)).await;
            true
        }
        HelperEvent::Signaled { signal } => {
            // 未知信号名不猜测具体编号：统一映射为 UNKNOWN(0)，由 core 转成
            // 协议里的 `"UNKNOWN"`，与文档一致。
            let number = signal_number(&signal);
            if number.is_none() {
                tracing::warn!(%signal, "helper 返回未知信号名，按 UNKNOWN 处理");
            }
            let _ = events
                .send(ProcEvent::Signaled(number.unwrap_or(UNKNOWN_SIGNAL)))
                .await;
            true
        }
        HelperEvent::Tree { processes } => {
            if let Some(waiter) = tree_waiters.pop_front() {
                let _ = waiter.send(processes);
            }
            false
        }
        HelperEvent::Error { code, message } => {
            tracing::warn!(?code, %message, "helper 返回错误事件");
            false
        }
        HelperEvent::ShellEvent {
            event,
            code,
            id,
            command,
            started_at_ms,
            ended_at_ms,
        } => {
            let Some(kind) = ShellEventKind::parse(&event) else {
                tracing::warn!(%event, "helper 返回未知 shell 事件，已忽略");
                return false;
            };
            events
                .send(ProcEvent::ShellEvent(
                    termx_protocol::shell_events::ShellEvent {
                        kind,
                        code,
                        id,
                        command,
                        started_at_ms,
                        ended_at_ms,
                        // 远端 helper 协议尚未转发 `O` 的 data；远端 transcript
                        // 继续走 PTY 输出路径。
                        data: None,
                    },
                ))
                .await
                .is_err()
        }
        HelperEvent::HelloOk { .. } | HelperEvent::Started { .. } => false,
    }
}

/// 把 russh 信号映射为协议信号名；未知/自定义信号原样保留，不猜测。
fn signal_name(signal: &russh::Sig) -> String {
    let name = match signal {
        russh::Sig::ABRT => "ABRT",
        russh::Sig::ALRM => "ALRM",
        russh::Sig::FPE => "FPE",
        russh::Sig::HUP => "HUP",
        russh::Sig::ILL => "ILL",
        russh::Sig::INT => "INT",
        russh::Sig::KILL => "KILL",
        russh::Sig::PIPE => "PIPE",
        russh::Sig::QUIT => "QUIT",
        russh::Sig::SEGV => "SEGV",
        russh::Sig::TERM => "TERM",
        russh::Sig::USR1 => "USR1",
        russh::Sig::Custom(name) => {
            let upper = name.trim().to_ascii_uppercase();
            return if upper.starts_with("SIG") {
                upper
            } else {
                format!("SIG{upper}")
            };
        }
    };
    format!("SIG{name}")
}

/// 把协议流映射为本地输出流。
fn output_stream(stream: Stream) -> OutputStream {
    match stream {
        Stream::Pty => OutputStream::Pty,
        Stream::Stdout => OutputStream::Stdout,
        Stream::Stderr => OutputStream::Stderr,
    }
}

/// 把协议信号名映射为 Unix 信号编号；无法识别时返回 `None`。
fn signal_number(name: &str) -> Option<i32> {
    let upper = name.trim().to_ascii_uppercase();
    let bare = upper.strip_prefix("SIG").unwrap_or(upper.as_str());
    match bare {
        "HUP" => Some(1),
        "INT" => Some(2),
        "QUIT" => Some(3),
        "ILL" => Some(4),
        "ABRT" => Some(6),
        "FPE" => Some(8),
        "KILL" => Some(9),
        "USR1" => Some(10),
        "SEGV" => Some(11),
        "PIPE" => Some(13),
        "ALRM" => Some(14),
        "TERM" => Some(15),
        other => other.parse::<i32>().ok(),
    }
}

/// 计算小写十六进制 sha256。
fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

/// 创建给 `SshSession::tree()` 使用的回复通道。
pub(crate) fn tree_channel() -> (
    Sender<Vec<ProcNode>>,
    std::sync::mpsc::Receiver<Vec<ProcNode>>,
) {
    channel()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 注入失败必须带目标路径与原因，且不能混入凭据。
    #[test]
    fn injection_failed_carries_path_and_reason_only() {
        let error = injection_failed("/home/alice/.termx/bin/termx-helper", "目标目录不可写");
        match &error {
            SshError::InjectionFailed { path, message } => {
                assert_eq!(path, "/home/alice/.termx/bin/termx-helper");
                assert_eq!(message, "目标目录不可写");
            }
            other => panic!("应为 InjectionFailed，实际 {other:?}"),
        }
        let text = error.to_string();
        assert!(text.contains("/home/alice/.termx/bin/termx-helper"));
        assert!(!text.contains("password"));
    }

    /// deny 与不可写场景都归类为注入失败，不允许回退 fallback。
    #[test]
    fn unavailable_decision_is_injection_failure() {
        assert_eq!(
            injection_decision(
                &InjectionPlan {
                    consent: InjectionConsentMode::Deny,
                    allow: false,
                    ..InjectionPlan::default()
                },
                true,
            ),
            InjectionDecision::Unavailable
        );
        let precheck = RemotePrecheck {
            kind: RemotePathKind::Missing,
            executable: false,
            sha256: None,
            parent_writable: false,
        };
        assert!(precheck_problem(&precheck, None).contains("不存在"));
    }

    #[test]
    fn temp_file_shape_is_checked_before_cleanup() {
        assert!(is_safe_temp_file(
            "/home/alice/.termx/bin/.termx-helper.abc123",
            "/home/alice/.termx/bin"
        ));
        for (temp, parent) in [
            (
                "/home/alice/.termx/bin/.termx-helper.",
                "/home/alice/.termx/bin",
            ),
            ("/tmp/.termx-helper.abc123", "/home/alice/.termx/bin"),
            (
                "/home/alice/.termx/bin/termx-helper",
                "/home/alice/.termx/bin",
            ),
            (
                "/home/alice/.termx/bin/.termx-helper.abc 123",
                "/home/alice/.termx/bin",
            ),
            (
                "/home/alice/.termx/bin/../.termx-helper.x",
                "/home/alice/.termx/bin",
            ),
        ] {
            assert!(!is_safe_temp_file(temp, parent), "应拒绝 {temp:?}");
        }
    }

    #[test]
    fn parent_dir_handles_root_and_nested_paths() {
        assert_eq!(parent_dir("/termx-helper"), "/");
        assert_eq!(
            parent_dir("/home/alice/.termx/bin/termx-helper"),
            "/home/alice/.termx/bin"
        );
    }

    #[test]
    fn precheck_output_parses_safe_defaults() {
        let parsed = parse_precheck_output("kind=file\nexec=1\nsha=ABCDEF\nwritable=1\n");
        assert_eq!(parsed.kind, RemotePathKind::File);
        assert!(parsed.executable);
        assert_eq!(parsed.sha256.as_deref(), Some("abcdef"));
        assert!(parsed.parent_writable);

        let missing = parse_precheck_output("kind=missing\nexec=0\nwritable=0\n");
        assert_eq!(missing.kind, RemotePathKind::Missing);
        assert!(!missing.executable);
        assert!(missing.sha256.is_none());
        assert!(!missing.parent_writable);

        let garbage = parse_precheck_output("garbage\nkind=weird\n");
        assert_eq!(garbage.kind, RemotePathKind::Missing);
    }

    #[test]
    fn usable_helper_requires_file_exec_and_expected_sha() {
        let file = RemotePrecheck {
            kind: RemotePathKind::File,
            executable: true,
            sha256: Some("abc".to_string()),
            parent_writable: true,
        };
        assert!(helper_usable(&file, None));
        assert!(helper_usable(&file, Some("abc")));
        assert!(!helper_usable(&file, Some("def")));

        let not_exec = RemotePrecheck {
            executable: false,
            ..file.clone()
        };
        assert!(!helper_usable(&not_exec, None));

        let dir = RemotePrecheck {
            kind: RemotePathKind::Directory,
            ..file
        };
        assert!(!helper_usable(&dir, None));
    }

    #[test]
    fn injection_decision_follows_plan() {
        let mut plan = InjectionPlan::default();
        assert_eq!(injection_decision(&plan, false), InjectionDecision::Reuse);
        assert_eq!(
            injection_decision(&plan, true),
            InjectionDecision::ConsentRequired,
            "默认 ask + 未授权"
        );

        plan.consent = InjectionConsentMode::Allow;
        assert_eq!(injection_decision(&plan, true), InjectionDecision::Install);

        plan.consent = InjectionConsentMode::Ask;
        plan.allow = true;
        assert_eq!(injection_decision(&plan, true), InjectionDecision::Install);

        plan.allow = false;
        plan.consent = InjectionConsentMode::Deny;
        assert_eq!(
            injection_decision(&plan, true),
            InjectionDecision::Unavailable
        );
    }

    #[test]
    fn injection_write_describes_target_state() {
        let missing = RemotePrecheck {
            kind: RemotePathKind::Missing,
            executable: false,
            sha256: None,
            parent_writable: true,
        };
        let write = injection_write(
            "/home/alice/.termx/bin/termx-helper",
            &missing,
            Some("abc"),
            Some(3),
        );
        assert_eq!(write.kind, InjectionWriteKind::Helper);
        assert_eq!(write.action, InjectionWriteAction::Install);
        assert_eq!(write.reason, InjectionWriteReason::Missing);
        assert_eq!(write.size, Some(3));

        let stale = RemotePrecheck {
            kind: RemotePathKind::File,
            executable: true,
            sha256: Some("def".to_string()),
            parent_writable: true,
        };
        let write = injection_write("/x", &stale, Some("abc"), None);
        assert_eq!(write.action, InjectionWriteAction::Replace);
        assert_eq!(write.reason, InjectionWriteReason::ChecksumMismatch);

        let not_exec = RemotePrecheck {
            executable: false,
            ..stale
        };
        let write = injection_write("/x", &not_exec, Some("abc"), None);
        assert_eq!(write.reason, InjectionWriteReason::NotExecutable);
    }

    #[test]
    fn helper_target_and_resolve_prefer_explicit_path() {
        let mut config = SshConfig {
            host: "h".to_string(),
            port: 22,
            user: "u".to_string(),
            auth: termx_protocol::SshAuth::Password {
                password: "p".to_string(),
            },
            helper_path: Some("~/.termx/bin/termx-helper".to_string()),
            helper_local_path: None,
            helper_sha256: None,
            upload_helper: false,
            accept_any_host_key: true,
            host_key_policy: None,
            trusted_host_key: None,
            known_hosts_path: None,
            injection: InjectionPlan {
                helper: Some("/ignored/helper".to_string()),
                ..InjectionPlan::default()
            },
        };
        assert_eq!(
            helper_target(&config).expect("显式 helper_path 优先"),
            "~/.termx/bin/termx-helper"
        );
        assert_eq!(
            resolve_target("~/.termx/bin/termx-helper", Some("/home/alice")).expect("应展开"),
            "/home/alice/.termx/bin/termx-helper"
        );
        assert_eq!(
            resolve_target("/opt/helper", None).expect("绝对路径无需 HOME"),
            "/opt/helper"
        );
        assert!(matches!(
            resolve_target("~/x", None),
            Err(SshError::InjectionFailed { path, message })
                if path == "~/x" && message.contains("HOME")
        ));

        config.helper_path = None;
        assert_eq!(
            helper_target(&config).expect("回退 plan"),
            "/ignored/helper"
        );

        config.injection.helper = None;
        assert!(matches!(
            helper_target(&config),
            Err(SshError::BadRequest(message)) if message.contains("安装路径")
        ));

        config.helper_path = Some("relative/helper".to_string());
        assert!(matches!(
            helper_target(&config),
            Err(SshError::BadRequest(message)) if message.contains("路径非法")
        ));

        config.helper_path = Some("/opt/dir/".to_string());
        assert!(matches!(
            helper_target(&config),
            Err(SshError::BadRequest(message)) if message.contains("文件路径")
        ));
    }

    #[test]
    fn helper_invocation_quotes_path() {
        assert_eq!(
            helper_invocation("/usr/bin/termx-helper"),
            "'/usr/bin/termx-helper' serve"
        );
        assert_eq!(
            helper_invocation(" /usr/bin/it's-helper "),
            "'/usr/bin/it'\\''s-helper' serve"
        );
    }

    #[test]
    fn signal_mapping_never_guesses_sigterm() {
        assert_eq!(signal_name(&russh::Sig::INT), "SIGINT");
        assert_eq!(
            signal_name(&russh::Sig::Custom("HUP".to_string())),
            "SIGHUP"
        );
        assert_eq!(
            signal_name(&russh::Sig::Custom("WEIRD".to_string())),
            "SIGWEIRD"
        );
        assert_eq!(signal_number("BOGUS"), None);
        assert_eq!(signal_number("SIG31"), Some(31));
        assert_eq!(signal_number("15"), Some(15));
    }

    /// 只有 helper 声明能力且请求需要事件时，`spawn.events` 才为 `true`。
    #[test]
    fn spawn_requests_events_only_when_supported_and_requested() {
        let mut request = SshConnectRequest::from_create(crate::SHELL_EVENT_PROGRAM, &[], 80, 24);
        request.events = true;
        assert!(matches!(
            spawn_request(&request, true),
            HelperRequest::Spawn { events: true, .. }
        ));
        assert!(matches!(
            spawn_request(&request, false),
            HelperRequest::Spawn { events: false, .. }
        ));

        request.events = false;
        assert!(matches!(
            spawn_request(&request, true),
            HelperRequest::Spawn { events: false, .. }
        ));
    }

    /// 取出 `spawn` 的环境覆盖表，断言确实带上了 env。
    fn spawn_env(request: &SshConnectRequest) -> BTreeMap<String, String> {
        match spawn_request(request, false) {
            HelperRequest::Spawn { env: Some(env), .. } => env,
            other => panic!("应为带 env 的 spawn，实际 {other:?}"),
        }
    }

    /// 缺失的 TERM 用默认值补齐，不改变其他环境项。
    #[test]
    fn spawn_request_injects_default_term_when_absent() {
        let env = spawn_env(&SshConnectRequest::shell(80, 24));
        assert_eq!(
            env.get("TERM").map(String::as_str),
            Some(DEFAULT_REMOTE_TERM)
        );

        let mut overrides = BTreeMap::new();
        overrides.insert("LANG".to_string(), "C.UTF-8".to_string());
        let request = SshConnectRequest::shell(80, 24).with_working_dir(None, Some(overrides));
        let env = spawn_env(&request);
        assert_eq!(
            env.get("TERM").map(String::as_str),
            Some(DEFAULT_REMOTE_TERM)
        );
        assert_eq!(env.get("LANG").map(String::as_str), Some("C.UTF-8"));
    }

    /// 空 TERM 值视同未设置；用户显式 TERM 必须原样保留。
    #[test]
    fn spawn_request_keeps_explicit_term() {
        let mut empty = BTreeMap::new();
        empty.insert("TERM".to_string(), "  ".to_string());
        let request = SshConnectRequest::shell(80, 24).with_working_dir(None, Some(empty));
        assert_eq!(
            spawn_env(&request).get("TERM").map(String::as_str),
            Some(DEFAULT_REMOTE_TERM)
        );

        for term in ["screen-256color", "dumb"] {
            let mut explicit = BTreeMap::new();
            explicit.insert("TERM".to_string(), term.to_string());
            let request = SshConnectRequest::shell(80, 24).with_working_dir(None, Some(explicit));
            assert_eq!(
                spawn_env(&request).get("TERM").map(String::as_str),
                Some(term)
            );
        }

        // 未显式给 env 时，使用请求自身的 term（构造器默认即 xterm-256color）。
        let mut request = SshConnectRequest::shell(80, 24);
        request.term = "vt100".to_string();
        assert_eq!(
            spawn_env(&request).get("TERM").map(String::as_str),
            Some("vt100")
        );
    }

    /// helper 的 `shell_event` 帧转发为 `ProcEvent::ShellEvent`，未知事件被忽略。
    #[tokio::test]
    async fn shell_events_map_to_proc_events() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut tree_waiters = VecDeque::new();
        let terminal = handle_event(
            HelperEvent::ShellEvent {
                event: "D".to_string(),
                code: Some(7),
                id: Some("cmd-1".to_string()),
                command: Some("echo hi".to_string()),
                started_at_ms: Some(1_700_000_000_000),
                ended_at_ms: Some(1_700_000_001_000),
            },
            &tx,
            &mut tree_waiters,
        )
        .await;
        assert!(!terminal, "shell 事件不是终态");
        assert_eq!(
            rx.recv().await,
            Some(ProcEvent::ShellEvent(
                termx_protocol::shell_events::ShellEvent {
                    kind: ShellEventKind::CommandEnd,
                    code: Some(7),
                    id: Some("cmd-1".to_string()),
                    command: Some("echo hi".to_string()),
                    started_at_ms: Some(1_700_000_000_000),
                    ended_at_ms: Some(1_700_000_001_000),
                    data: None,
                }
            ))
        );

        assert!(
            !handle_event(
                HelperEvent::ShellEvent {
                    event: "Z".to_string(),
                    code: None,
                    id: None,
                    command: None,
                    started_at_ms: None,
                    ended_at_ms: None,
                },
                &tx,
                &mut tree_waiters,
            )
            .await
        );
        assert!(rx.try_recv().is_err(), "未知事件不应转发");
    }
}
