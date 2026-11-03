//! SSH helper 分发、握手与协议驱动（M6b-2）。
//!
//! 流程（全部在已认证的 SSH 连接上完成）：
//! 1. 需要上传时，远端 `mktemp -d` 创建唯一的私有目录（700），把本地 helper
//!    写入目录内的固定文件名，`chmod 700` 并用 `sha256sum` 校验；
//! 2. 以 `exec` 启动 `<helper> serve`，经 stdin/stdout 走 JSON Lines 协议；
//! 3. 完成 `hello` / `hello_ok` 与 `spawn` / `started` 握手，得到远端 pid/pgid；
//! 4. 成功则进入 helper 驱动；任一步失败返回错误，由调用方关闭通道并回退 fallback。
//!
//! 安全约定：
//! - 密码/口令只存在于内存，不进入命令行；上传/校验命令只拼接路径与十六进制摘要；
//! - 上传只写本次 `mktemp -d` 创建的目录，绝不覆盖/删除用户配置的 `helper_path`；
//! - 本次创建的目录在失败、握手前超时/取消、会话终态或断连时只 `rm -rf` 该目录。

use std::{
    collections::VecDeque,
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
    Capabilities, HELPER_PROTOCOL_VERSION, ProcNode, SshConfig, Stream,
    helper::{HelperEvent, HelperRequest},
    to_json,
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

/// 远端临时目录模板；`mktemp -d` 会替换尾部 `X`，目录名便于测试核对。
const REMOTE_TEMP_TEMPLATE: &str = "/tmp/termx-helper.XXXXXXXXXX";

/// 上传到临时目录内的固定文件名。
const REMOTE_HELPER_FILE: &str = "termx-helper";

/// 未知信号编号的占位值；`termx-core` 会把它映射为 `"UNKNOWN"`。
const UNKNOWN_SIGNAL: i32 = 0;

/// 本次上传创建的远端目录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UploadedHelper {
    /// 由 `mktemp -d` 创建并校验过格式的目录。
    dir: String,
    /// 目录内的 helper 文件绝对路径。
    file: String,
}

impl UploadedHelper {
    /// 从目录拼出固定文件名。
    fn new(dir: String) -> Self {
        let file = format!("{}/{REMOTE_HELPER_FILE}", dir.trim_end_matches('/'));
        Self { dir, file }
    }

    /// helper 可执行文件路径。
    fn command(&self) -> &str {
        &self.file
    }
}

/// 上传阶段的清理守卫：持有连接句柄与本次创建的目录。
///
/// 正常路径通过 [`UploadGuard::into_parts`] 交出句柄与目录；失败路径先显式
/// `cleanup()`，取消/超时导致 future 被丢弃时由 `Drop` 把句柄移入后台任务
/// 完成清理与断开，避免远端临时目录残留。
struct UploadGuard {
    /// 已认证连接；`None` 表示已交出或已转入清理任务。
    handle: Option<client::Handle<HostKeyPolicy>>,
    /// 本次创建的目录；`None` 表示未上传或已清理。
    uploaded: Option<UploadedHelper>,
}

impl UploadGuard {
    /// 用连接句柄与可选上传目录创建守卫。
    fn new(handle: client::Handle<HostKeyPolicy>, uploaded: Option<UploadedHelper>) -> Self {
        Self {
            handle: Some(handle),
            uploaded,
        }
    }

    /// 借用连接句柄；守卫存活期间始终可用。
    fn handle(&self) -> &client::Handle<HostKeyPolicy> {
        self.handle
            .as_ref()
            .expect("UploadGuard 持有连接句柄直到 into_parts")
    }

    /// 显式清理本次目录；失败只记日志。
    async fn cleanup(&mut self) {
        if let (Some(handle), Some(uploaded)) = (self.handle.as_ref(), self.uploaded.as_ref()) {
            cleanup_uploaded(handle, uploaded).await;
        }
        self.uploaded = None;
    }

    /// 交出连接句柄与仍待会话结束时清理的目录。
    fn into_parts(mut self) -> (client::Handle<HostKeyPolicy>, Option<UploadedHelper>) {
        let handle = self.handle.take().expect("UploadGuard 只能交出一次句柄");
        let uploaded = self.uploaded.take();
        (handle, uploaded)
    }
}

impl Drop for UploadGuard {
    fn drop(&mut self) {
        let (Some(handle), Some(uploaded)) = (self.handle.take(), self.uploaded.take()) else {
            return;
        };
        // 只有仍在 tokio 运行时内才可能派发清理；运行期退出时只能放弃。
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(dir = %uploaded.dir, "运行时已关闭，无法清理远端临时 helper 目录");
            return;
        };
        runtime.spawn(async move {
            cleanup_uploaded(&handle, &uploaded).await;
            let _ = handle
                .disconnect(russh::Disconnect::ByApplication, "", "")
                .await;
        });
    }
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
    /// 握手期间提前读到的会话事件（输出等）。
    pub(crate) pending: Vec<HelperEvent>,
    /// 本次创建的远端临时目录；仅用于结束/失败时清理。
    pub(crate) uploaded: Option<UploadedHelper>,
}

impl HelperSession {
    /// helper 模式能力：无降级。
    pub(crate) fn capabilities(&self) -> Capabilities {
        Capabilities {
            helper: true,
            version: self.proto.clone(),
            best_effort: false,
        }
    }
}

/// 在已认证连接上尝试建立 helper 会话。
///
/// 返回 `Ok((连接句柄, helper 会话))` 继续驱动；失败时把连接句柄一并返回，
/// 调用方可在同一连接上回退 fallback。上传/握手期间的临时目录由
/// [`UploadGuard`] 负责清理（失败显式清理，取消/超时由 `Drop` 兜底）。
///
/// # 错误
/// 上传/校验失败、exec 失败、握手超时或版本不符、`spawn` 失败时返回错误。
pub(crate) async fn bootstrap(
    handle: client::Handle<HostKeyPolicy>,
    config: &SshConfig,
    request: &SshConnectRequest,
) -> Result<(client::Handle<HostKeyPolicy>, HelperSession), (client::Handle<HostKeyPolicy>, SshError)>
{
    if !config.upload_helper {
        // 远端已存在模式：只校验/使用 helper_path，不写远端任何文件。
        let command = match config.helper_path.as_deref() {
            Some(path) if !path.trim().is_empty() => path,
            _ => {
                return Err((
                    handle,
                    SshError::BadRequest("未配置 helper_path，无法启用 helper 模式".to_string()),
                ));
            }
        };
        let expected = configured_sha(config);
        if let Some(expected) = expected.as_deref() {
            if let Err(error) = verify_remote(&handle, command, expected).await {
                return Err((handle, error));
            }
        }
        return match open_and_spawn(&handle, command, request).await {
            Ok((channel, proto, pid, pgid, pending)) => Ok((
                handle,
                HelperSession {
                    channel,
                    pid,
                    pgid,
                    proto,
                    pending,
                    uploaded: None,
                },
            )),
            Err(error) => Err((handle, error)),
        };
    }

    // 上传模式：本地读取/摘要校验没有远端副作用，失败可直接返回。
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
                SshError::Connect {
                    message: format!("读取本地 helper 失败: {error}"),
                },
            ));
        }
    };
    let digest = sha256_hex(&bytes);
    if let Some(expected) = configured_sha(config) {
        if expected != digest {
            return Err((
                handle,
                SshError::BadRequest("本地 helper sha256 与 helper_sha256 不一致".to_string()),
            ));
        }
    }

    // 先创建唯一私有目录，创建成功后的任何失败/取消都由守卫清理。
    let dir = match create_temp_dir(&handle).await {
        Ok(dir) => dir,
        Err(error) => return Err((handle, error)),
    };
    let mut guard = UploadGuard::new(handle, Some(UploadedHelper::new(dir)));
    let remote = guard
        .uploaded
        .as_ref()
        .expect("UploadGuard 刚由目录构造")
        .clone();
    let command = remote.command().to_string();
    let step = async {
        match tokio::time::timeout(
            HELPER_STEP_TIMEOUT,
            upload(guard.handle(), &remote.file, &bytes),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(SshError::Connect {
                    message: format!("上传 helper 超过 {} 秒", HELPER_STEP_TIMEOUT.as_secs()),
                });
            }
        }
        verify_remote(guard.handle(), &remote.file, &digest).await?;
        chmod_exec(guard.handle(), &remote.file).await
    }
    .await;
    if let Err(error) = step {
        guard.cleanup().await;
        let (handle, _) = guard.into_parts();
        return Err((handle, error));
    }

    let opened = open_and_spawn(guard.handle(), &command, request).await;
    match opened {
        Ok((channel, proto, pid, pgid, pending)) => {
            let (handle, uploaded) = guard.into_parts();
            Ok((
                handle,
                HelperSession {
                    channel,
                    pid,
                    pgid,
                    proto,
                    pending,
                    uploaded,
                },
            ))
        }
        Err(error) => {
            guard.cleanup().await;
            let (handle, _) = guard.into_parts();
            Err((handle, error))
        }
    }
}

/// 打开 helper 通道、`exec serve`、握手并 `spawn`；失败时关闭通道。
async fn open_and_spawn(
    handle: &client::Handle<HostKeyPolicy>,
    command: &str,
    request: &SshConnectRequest,
) -> Result<(HelperChannel, String, u32, u32, Vec<HelperEvent>), SshError> {
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
        let proto = handshake(&mut helper_channel).await?;
        let (pid, pgid, pending) = spawn_session(&mut helper_channel, request).await?;
        Ok::<_, SshError>((proto, pid, pgid, pending))
    }
    .await;

    match handshake_result {
        Ok((proto, pid, pgid, pending)) => Ok((helper_channel, proto, pid, pgid, pending)),
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

/// 远端 `mktemp -d` 创建唯一私有目录并返回路径。
///
/// 目录模板固定在 `/tmp`，返回后立即 `chmod 700`；输出必须是绝对路径且不含
/// 空白/控制字符，否则视为远端异常并拒绝继续。
async fn create_temp_dir(handle: &client::Handle<HostKeyPolicy>) -> Result<String, SshError> {
    let command = format!("mktemp -d {REMOTE_TEMP_TEMPLATE}");
    let (code, stdout) = exec_step(handle, &command, "mktemp -d").await?;
    if code.unwrap_or(0) != 0 {
        return Err(SshError::Connect {
            message: format!("创建远端临时目录失败（退出码 {}）", code.unwrap_or(-1)),
        });
    }
    let dir = String::from_utf8_lossy(&stdout).trim().to_string();
    if !is_safe_temp_dir(&dir) {
        return Err(SshError::Connect {
            message: format!("远端 mktemp 返回非法目录: {dir:?}"),
        });
    }
    let uploaded = UploadedHelper::new(dir);
    let command = format!("chmod 700 {}", shell_quote(&uploaded.dir));
    let (code, _) = exec_step(handle, &command, "chmod 700").await?;
    if code.unwrap_or(0) != 0 {
        // 目录已创建但权限设置失败：立即清理，避免返回错误后残留。
        cleanup_uploaded(handle, &uploaded).await;
        return Err(SshError::Connect {
            message: format!("设置临时目录权限失败（退出码 {}）", code.unwrap_or(-1)),
        });
    }
    Ok(uploaded.dir)
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

/// 校验 `mktemp -d` 返回目录的形状：`/tmp/termx-helper.` 前缀、无空白/控制字符。
fn is_safe_temp_dir(dir: &str) -> bool {
    dir.starts_with("/tmp/termx-helper.")
        && dir.len() > "/tmp/termx-helper.".len()
        && !dir.chars().any(|ch| ch.is_whitespace() || ch.is_control())
        && !dir.contains("..")
}

/// 上传本地字节到本次创建的私有目录内固定文件名。
///
/// `set -C`（noclobber）拒绝覆盖已存在路径；目录权限 700 且由 `mktemp -d`
/// 新建，配合 `umask 077` 与随后的 `chmod 700` 限制访问。
async fn upload(
    handle: &client::Handle<HostKeyPolicy>,
    remote: &str,
    bytes: &[u8],
) -> Result<(), SshError> {
    let inner = format!("set -C; umask 077; cat > {}", shell_quote(remote));
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

/// 只删除本次 `mktemp -d` 创建的目录；失败只记日志。
///
/// 清理前再次校验目录形状，避免把非本模块创建的路径交给 `rm -rf`；
/// 用户配置的 `helper_path` 永不在此路径上，也不会被删除。
async fn cleanup_uploaded(handle: &client::Handle<HostKeyPolicy>, uploaded: &UploadedHelper) {
    if !is_safe_temp_dir(&uploaded.dir) {
        tracing::warn!(dir = %uploaded.dir, "拒绝清理非本次创建的远端目录");
        return;
    }
    let command = format!("rm -rf -- {}", shell_quote(&uploaded.dir));
    let cleaned = tokio::time::timeout(HELPER_STEP_TIMEOUT, exec_collect(handle, &command)).await;
    if !matches!(cleaned, Ok(Ok((Some(0), _)))) {
        tracing::debug!(dir = %uploaded.dir, "清理远端临时 helper 目录未确认成功");
    }
}

/// helper 启动命令：`helper_path` 已在校验阶段保证为不含空白的路径，按单一路径引用。
fn helper_invocation(path: &str) -> String {
    format!("{} serve", shell_quote(path.trim()))
}

/// 完成 `hello` / `hello_ok` 握手，返回 helper 协议版本。
async fn handshake(channel: &mut HelperChannel) -> Result<String, SshError> {
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
            HelperEvent::HelloOk { proto, .. } => {
                if proto != HELPER_PROTOCOL_VERSION {
                    return Err(SshError::BadRequest(format!("helper 协议不匹配: {proto}")));
                }
                return Ok(proto);
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

/// 发送 `spawn` 并等待 `started`；返回 pid/pgid 与提前到达的事件。
async fn spawn_session(
    channel: &mut HelperChannel,
    request: &SshConnectRequest,
) -> Result<(u32, u32, Vec<HelperEvent>), SshError> {
    channel.send(&spawn_request(request)).await?;

    let deadline = tokio::time::Instant::now() + HELPER_STEP_TIMEOUT;
    let mut pending = Vec::new();
    loop {
        let event = channel
            .next_event_until(deadline, "waiting started")
            .await?;
        match event {
            HelperEvent::Started { pid, pgid } => return Ok((pid, pgid, pending)),
            HelperEvent::Error { code, message } => {
                return Err(SshError::BadRequest(format!(
                    "helper spawn 失败: {code:?} {message}"
                )));
            }
            other => pending.push(other),
        }
    }
}

/// 把 `terminal.create` 的请求翻译为 helper `spawn`。
fn spawn_request(request: &SshConnectRequest) -> HelperRequest {
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
        env: request.env.clone(),
        cols: Some(request.cols),
        rows: Some(request.rows),
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

/// 调用方已放弃时关闭 helper 通道、清理本次临时目录并断开连接。
pub(crate) async fn abort(handle: &client::Handle<HostKeyPolicy>, mut session: HelperSession) {
    session.channel.close().await;
    if let Some(uploaded) = session.uploaded.as_ref() {
        cleanup_uploaded(handle, uploaded).await;
    }
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
    if let Some(uploaded) = helper.uploaded.as_ref() {
        cleanup_uploaded(&handle, uploaded).await;
    }
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

    #[test]
    fn temp_dir_shape_is_checked_before_use_and_cleanup() {
        assert!(is_safe_temp_dir("/tmp/termx-helper.abc123"));
        for unsafe_dir in [
            "/tmp/termx-helper.",
            "/tmp/other.abc123",
            "/tmp/termx-helper.abc 123",
            "/tmp/termx-helper.../etc",
            "relative/termx-helper.abc",
        ] {
            assert!(!is_safe_temp_dir(unsafe_dir), "应拒绝 {unsafe_dir:?}");
        }
    }

    #[test]
    fn uploaded_helper_uses_fixed_file_name() {
        let uploaded = UploadedHelper::new("/tmp/termx-helper.abc123".to_string());
        assert_eq!(uploaded.command(), "/tmp/termx-helper.abc123/termx-helper");
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
}
