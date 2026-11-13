//! `termx-helper` 的 stdio sidecar 服务端实现。
//!
//! 协议见 `docs/specs/PROTOCOL.md` §11：
//! - stdin 每行一个 [`HelperRequest`]，首帧必须是 `hello`；
//! - stdout 每行一个 [`HelperEvent`]，**只承载协议 JSON**；
//! - 日志一律写 stderr（tracing）；
//! - 同一时刻只维护一个 PTY 会话，`spawn` 会先清理上一会话；
//! - stdin EOF 或进程退出前终止整棵进程树，确保无孤儿。
//!
//! 线程边界见 `docs/specs/ASYNC.md`：stdin 请求循环、PTY 事件转发任务、stdout 写任务
//! 三者相互独立；所有可能阻塞的 PTY 操作都经 `spawn_blocking` 执行。

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use termx_proc::{
    BarrierHandle, BarrierReport, OutputStream, ProcError, ProcEvent, PtyConfig, PtySession,
};
use termx_protocol::helper::{HELPER_CAPABILITIES, HelperEvent, HelperRequest};
use termx_protocol::shell_events::{ShellEvent, ShellEventKind};
use termx_protocol::{ErrorCode, HELPER_PROTOCOL_VERSION, ProcNode, Stream, to_json};
use termx_shell_events::ShellEventCancel;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, Stdin};
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::platform;

/// 正常结束（stdin EOF）。
pub const EXIT_OK: u8 = 0;
/// 未分类内部错误。
pub const EXIT_INTERNAL: u8 = 1;
/// 首帧缺失、proto 不匹配或首帧非法。
pub const EXIT_PROTOCOL: u8 = 2;

/// 未指定尺寸时的默认列数。
const DEFAULT_COLS: u16 = 80;
/// 未指定尺寸时的默认行数。
const DEFAULT_ROWS: u16 = 24;
/// 默认日志过滤：helper 自身输出 info，依赖库只保留告警。
const DEFAULT_LOG_FILTER: &str = "termx_helper=info,termx_proc=warn";
/// 命令结束事件前的输出屏障上限；超时则丢弃该 `D`，让 service 退化为静默判定。
const OUTPUT_BARRIER_TIMEOUT: Duration = termx_proc::DEFAULT_BARRIER_TIMEOUT;

/// PTY 输出转发进度：`forwarded` 为已写入 `out_tx` 的 `Output` 事件数。
#[derive(Debug, Clone, Copy, Default)]
struct OutputProgress {
    /// 已写入协议输出端的 `Output` 事件累计数。
    forwarded: u64,
    /// 转发任务是否已结束。
    closed: bool,
}

/// 命令结束前的输出屏障；生产实现委托给 [`BarrierHandle`]。
///
/// 抽成 trait 便于用假实现确定性测试“D 必须等 Output 先转发”的顺序约束。
trait OutputBarrier: Send + Sync {
    /// 请求排空当前可读输出。
    fn request(&self, timeout: Duration) -> Result<BarrierReport, ProcError>;
}

impl OutputBarrier for BarrierHandle {
    fn request(&self, timeout: Duration) -> Result<BarrierReport, ProcError> {
        self.barrier(timeout)
    }
}

/// 运行 `termx-helper serve`，返回进程退出码。
///
/// 该函数负责初始化 tracing（stderr）、启动 stdout 写任务并串行处理 stdin 请求；
/// 任何返回路径都会先清理 PTY 会话。
pub async fn serve() -> ExitCode {
    init_logging();
    info!(proto = HELPER_PROTOCOL_VERSION, "termx-helper 启动");

    let (out_tx, out_rx) = unbounded_channel::<HelperEvent>();
    let writer = tokio::spawn(write_loop(out_rx));

    let code = run_loop(&out_tx).await;

    // 先释放协议输出端，writer 才能在通道排空后退出。
    drop(out_tx);
    if let Err(err) = writer.await {
        warn!(error = %err, "stdout 写任务异常结束");
    }
    ExitCode::from(code)
}

/// 初始化 stderr 日志；`RUST_LOG` 可覆盖默认过滤。
fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(DEFAULT_LOG_FILTER));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .with_ansi(false)
        .try_init();
}

/// 手写首帧后进入请求循环；正常 EOF 返回 [`EXIT_OK`]。
async fn run_loop(out_tx: &UnboundedSender<HelperEvent>) -> u8 {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    if let Err(code) = handshake(&mut lines, out_tx).await {
        return code;
    }

    let mut state = HelperState::new();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => {
                debug!("stdin EOF，开始收尾");
                break;
            }
            Err(err) => {
                warn!(error = %err, "读取 stdin 失败，开始收尾");
                break;
            }
        };

        let request = match serde_json::from_str::<HelperRequest>(&line) {
            Ok(request) => request,
            Err(err) => {
                send_error(
                    out_tx,
                    ErrorCode::BadRequest,
                    format!("请求不是合法 JSON: {err}"),
                );
                continue;
            }
        };
        state.handle(request, out_tx).await;
    }

    state.cleanup().await;
    EXIT_OK
}

/// 校验首帧 `hello`：失败时发送 `error` 并返回退出码。
async fn handshake(
    lines: &mut Lines<BufReader<Stdin>>,
    out_tx: &UnboundedSender<HelperEvent>,
) -> Result<(), u8> {
    let first = match lines.next_line().await {
        Ok(Some(line)) => line,
        Ok(None) => {
            warn!("首帧缺失：stdin 提前 EOF");
            return Err(EXIT_PROTOCOL);
        }
        Err(err) => {
            warn!(error = %err, "读取首帧失败");
            return Err(EXIT_PROTOCOL);
        }
    };

    let request = match serde_json::from_str::<HelperRequest>(&first) {
        Ok(request) => request,
        Err(err) => {
            send_error(
                out_tx,
                ErrorCode::BadRequest,
                format!("首帧不是合法 JSON: {err}"),
            );
            return Err(EXIT_PROTOCOL);
        }
    };

    match request {
        HelperRequest::Hello { proto, version } => {
            if proto != HELPER_PROTOCOL_VERSION {
                send_error(
                    out_tx,
                    ErrorCode::VersionMismatch,
                    format!("不支持的 helper 协议 {proto}"),
                );
                return Err(EXIT_PROTOCOL);
            }
            info!(client_version = %version, "helper 握手完成");
            let _ = out_tx.send(HelperEvent::HelloOk {
                proto: HELPER_PROTOCOL_VERSION.to_string(),
                capabilities: HELPER_CAPABILITIES
                    .iter()
                    .map(|cap| (*cap).to_string())
                    .collect(),
            });
            Ok(())
        }
        _ => {
            send_error(
                out_tx,
                ErrorCode::BadRequest,
                "首帧必须是 hello".to_string(),
            );
            Err(EXIT_PROTOCOL)
        }
    }
}

/// `spawn` 请求的字段集合，避免处理函数参数过多。
struct SpawnSpec {
    /// 可执行程序。
    cmd: String,
    /// 程序参数。
    args: Vec<String>,
    /// 工作目录。
    cwd: Option<String>,
    /// 环境变量覆盖表。
    env: Option<BTreeMap<String, String>>,
    /// 列数。
    cols: Option<u16>,
    /// 行数。
    rows: Option<u16>,
    /// 是否请求带外 shell 事件通道。
    events: bool,
}

/// 单个 helper 会话及其后台转发任务。
struct HelperState {
    /// 当前 PTY 会话；`None` 表示尚未 spawn 或已清理。
    session: Option<PtySession>,
    /// PTY 事件转发任务；随会话创建，会话释放后结束。
    forwarder: Option<JoinHandle<()>>,
    /// shell 事件通道的取消句柄；drop 时关闭 listener 并清理 socket。
    shell_events_cancel: Option<ShellEventCancel>,
    /// 把 shell 事件转成 `HelperEvent::ShellEvent` 的转发任务。
    shell_events_forwarder: Option<JoinHandle<()>>,
    /// 会话是否仍在运行；由转发任务在事件流结束时置 `false`。
    running: Arc<AtomicBool>,
}

impl HelperState {
    /// 创建空会话状态。
    fn new() -> Self {
        Self {
            session: None,
            forwarder: None,
            shell_events_cancel: None,
            shell_events_forwarder: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 串行处理一条请求。
    async fn handle(&mut self, request: HelperRequest, out_tx: &UnboundedSender<HelperEvent>) {
        match request {
            HelperRequest::Hello { .. } => {
                send_error(out_tx, ErrorCode::BadRequest, "重复 hello".to_string())
            }
            HelperRequest::Spawn {
                cmd,
                args,
                cwd,
                env,
                cols,
                rows,
                events,
            } => {
                let spec = SpawnSpec {
                    cmd,
                    args,
                    cwd,
                    env,
                    cols,
                    rows,
                    events,
                };
                self.spawn(spec, out_tx).await;
            }
            HelperRequest::Input { data } => self.input(data, out_tx).await,
            HelperRequest::Resize { cols, rows } => self.resize(cols, rows, out_tx).await,
            HelperRequest::Signal { signal } => self.signal(signal, out_tx).await,
            HelperRequest::Terminate => self.terminate(out_tx).await,
            HelperRequest::Tree => self.tree(out_tx).await,
        }
    }

    /// 启动新 PTY；若已有活动会话则拒绝，否则先清理旧会话。
    async fn spawn(&mut self, spec: SpawnSpec, out_tx: &UnboundedSender<HelperEvent>) {
        if self.session.is_some() && self.running.load(Ordering::SeqCst) {
            send_error(
                out_tx,
                ErrorCode::BadRequest,
                "已有活动会话，先 terminate".to_string(),
            );
            return;
        }
        self.cleanup().await;

        let mut config = PtyConfig {
            program: spec.cmd,
            args: spec.args,
            cwd: spec.cwd,
            env: spec.env,
            cols: spec.cols.unwrap_or(DEFAULT_COLS),
            rows: spec.rows.unwrap_or(DEFAULT_ROWS),
        };
        let event_channel = platform::open_event_channel(spec.events, &mut config.env);
        let spawned = tokio::task::spawn_blocking(move || platform::spawn(config)).await;

        match spawned {
            Ok(Ok((session, events))) => {
                let (pid, pgid) = (session.pid(), session.pgid());
                let barrier: Arc<dyn OutputBarrier> = Arc::new(session.barrier_handle());
                self.session = Some(session);
                self.running.store(true, Ordering::SeqCst);
                let events_enabled = event_channel.is_some();
                let _ = out_tx.send(HelperEvent::Started {
                    pid,
                    pgid,
                    events: events_enabled,
                });
                // PTY 输出与 shell 事件是两条独立任务；用转发进度让 D 等待 Output。
                let (progress_tx, progress_rx) = watch::channel(OutputProgress::default());
                self.forwarder = Some(spawn_forwarder(
                    events,
                    out_tx.clone(),
                    Arc::clone(&self.running),
                    progress_tx,
                ));
                if let Some(channel) = event_channel {
                    let (receiver, cancel) = channel.split();
                    self.shell_events_cancel = Some(cancel);
                    self.shell_events_forwarder = Some(spawn_shell_event_forwarder(
                        receiver,
                        out_tx.clone(),
                        barrier,
                        progress_rx,
                    ));
                }
                debug!(pid, pgid, events = events_enabled, "PTY 会话已启动");
            }
            Ok(Err(err)) => {
                let (code, message) = map_proc_error(&err);
                send_error(out_tx, code, message);
            }
            Err(err) => send_error(
                out_tx,
                ErrorCode::Internal,
                format!("spawn 任务失败: {err}"),
            ),
        }
    }

    /// 写入 base64 解码后的输入字节。
    async fn input(&mut self, data: String, out_tx: &UnboundedSender<HelperEvent>) {
        let Ok(bytes) = STANDARD.decode(data.as_bytes()) else {
            send_error(
                out_tx,
                ErrorCode::BadRequest,
                "input.data 不是合法 base64".to_string(),
            );
            return;
        };

        if let Err(err) = session_op(&mut self.session, move |session| session.write(&bytes)).await
        {
            self.report(err, out_tx);
        }
    }

    /// 调整 PTY 尺寸。
    async fn resize(&mut self, cols: u16, rows: u16, out_tx: &UnboundedSender<HelperEvent>) {
        if let Err(err) =
            session_op(&mut self.session, move |session| session.resize(cols, rows)).await
        {
            self.report(err, out_tx);
        }
    }

    /// 转发信号到子进程组/进程树。
    async fn signal(&mut self, signal: String, out_tx: &UnboundedSender<HelperEvent>) {
        if let Err(err) =
            session_op(&mut self.session, move |session| session.signal(&signal)).await
        {
            self.report(err, out_tx);
        }
    }

    /// 终止整棵进程树；终态事件由转发任务发出。
    ///
    /// `terminate` 返回成功即代表进程树已回收，因此立刻清掉 `running` 标记，
    /// 避免紧随其后的 `spawn` 被尚未排空的旧转发任务误判为活动会话；会话
    /// 句柄本身保留，使重复 `terminate` 仍按幂等路径返回。
    async fn terminate(&mut self, out_tx: &UnboundedSender<HelperEvent>) {
        match session_op(&mut self.session, |session| session.terminate()).await {
            Ok(()) => {
                self.running.store(false, Ordering::SeqCst);
                self.stop_shell_events().await;
            }
            Err(err) => self.report(err, out_tx),
        }
    }

    /// 采集当前会话的进程树；无活动会话或采集失败时返回空树。
    ///
    /// 只有 Linux 走 `/proc` 采集；其它平台按协议返回空列表，避免把
    /// “平台不支持”伪装成会话错误。
    async fn tree(&mut self, out_tx: &UnboundedSender<HelperEvent>) {
        let processes = self.collect_tree().await;
        let _ = out_tx.send(HelperEvent::Tree { processes });
    }

    /// 在阻塞线程池里读取进程树；平台不支持或采集失败只记日志。
    async fn collect_tree(&mut self) -> Vec<ProcNode> {
        if !platform::tree_supported() {
            return Vec::new();
        }
        match session_op(&mut self.session, |session| session.tree()).await {
            Ok(processes) => processes,
            Err(err) => {
                debug!(error = %err, "采集进程树失败，返回空树");
                Vec::new()
            }
        }
    }

    /// 上报 PTY 操作失败，并按需把会话标记为不再运行。
    fn report(&self, err: ProcError, out_tx: &UnboundedSender<HelperEvent>) {
        if matches!(err, ProcError::NotRunning) {
            self.running.store(false, Ordering::SeqCst);
        }
        let (code, message) = map_proc_error(&err);
        send_error(out_tx, code, message);
    }

    /// 终止并释放当前会话，等待转发任务退出。
    async fn cleanup(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = tokio::task::spawn_blocking(move || {
                let mut session = session;
                let _ = session.terminate();
            })
            .await;
        }
        if let Some(forwarder) = self.forwarder.take() {
            let _ = forwarder.await;
        }
        self.stop_shell_events().await;
        self.running.store(false, Ordering::SeqCst);
    }

    /// 关闭 shell 事件 listener、等待转发任务退出；Unix socket 随之清理。
    async fn stop_shell_events(&mut self) {
        drop(self.shell_events_cancel.take());
        if let Some(forwarder) = self.shell_events_forwarder.take() {
            let _ = forwarder.await;
        }
    }
}

/// 在阻塞线程池中执行一次 PTY 操作，避免阻塞 async 运行时。
async fn session_op<T, F>(session: &mut Option<PtySession>, op: F) -> Result<T, ProcError>
where
    T: Send + 'static,
    F: FnOnce(&mut PtySession) -> Result<T, ProcError> + Send + 'static,
{
    let Some(mut current) = session.take() else {
        return Err(ProcError::NotRunning);
    };

    match tokio::task::spawn_blocking(move || {
        let result = op(&mut current);
        (current, result)
    })
    .await
    {
        Ok((current, result)) => {
            *session = Some(current);
            result
        }
        Err(err) => Err(ProcError::Internal {
            message: format!("PTY 操作任务失败: {err}"),
        }),
    }
}

/// 把通道事件转成 `HelperEvent::ShellEvent` 写入协议输出。
///
/// `D`（命令结束）必须先让 PTY 转发任务把所有先于 D 的 `Output` 写入
/// `out_tx`，再转发 D。A/B/C/X 不参与屏障。
fn spawn_shell_event_forwarder(
    mut events: Receiver<ShellEvent>,
    out_tx: UnboundedSender<HelperEvent>,
    barrier: Arc<dyn OutputBarrier>,
    mut progress: watch::Receiver<OutputProgress>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if event.kind == ShellEventKind::CommandEnd
                && !wait_for_output_before_command_end(&barrier, &mut progress).await
            {
                // 屏障超时：不转发该 D，让 service 侧退化为静默判定（不伪造退出码）。
                continue;
            }
            let _ = out_tx.send(translate_shell_event(event));
        }
    })
}

/// 在转发 D 之前排空 PTY 输出，并等待转发任务把这些输出写入协议输出端。
///
/// 返回 `false` 仅表示屏障超时；此时调用方丢弃 D。
async fn wait_for_output_before_command_end(
    barrier: &Arc<dyn OutputBarrier>,
    progress: &mut watch::Receiver<OutputProgress>,
) -> bool {
    let barrier = Arc::clone(barrier);
    let report = tokio::task::spawn_blocking(move || barrier.request(OUTPUT_BARRIER_TIMEOUT)).await;
    let output_seq = match report {
        Ok(Ok(BarrierReport::Drained { output_seq })) => output_seq,
        // 平台/后端没有屏障：保持既有行为。
        Ok(Ok(BarrierReport::Unsupported)) => return true,
        Ok(Err(ProcError::BarrierTimeout)) => {
            warn!("PTY 输出屏障超时，丢弃 D 让 transcript 退化为静默判定");
            return false;
        }
        Ok(Err(error)) => {
            warn!(%error, "PTY 输出屏障失败，继续转发 D");
            return true;
        }
        Err(error) => {
            warn!(%error, "PTY 输出屏障任务失败，继续转发 D");
            return true;
        }
    };

    if output_seq == 0 {
        return true;
    }
    if wait_forwarded(progress, output_seq, OUTPUT_BARRIER_TIMEOUT).await {
        return true;
    }
    // 转发任务已结束或超时：无法保证 D 在 Output 之后，丢弃 D，
    // 与 service 侧一致退化为静默判定，不伪造顺序。
    warn!(
        output_seq,
        forwarded = progress.borrow().forwarded,
        "等待 PTY 输出转发超时，丢弃 D"
    );
    false
}

/// 等待 `progress.forwarded >= target`；转发任务结束或超时返回 `false`。
async fn wait_forwarded(
    progress: &mut watch::Receiver<OutputProgress>,
    target: u64,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let current = *progress.borrow_and_update();
            if current.forwarded >= target {
                return true;
            }
            if current.closed {
                return false;
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        match tokio::time::timeout(remaining, progress.changed()).await {
            Ok(Ok(())) => continue,
            // 发送端已丢弃或超时：值不会再有意义地变化。
            _ => return false,
        }
    }
}

/// 共享事件类型到 helper 协议的映射。
fn translate_shell_event(event: ShellEvent) -> HelperEvent {
    HelperEvent::ShellEvent {
        event: event.kind.as_str().to_string(),
        code: event.code,
        id: event.id,
        command: event.command,
        started_at_ms: event.started_at_ms,
        ended_at_ms: event.ended_at_ms,
    }
}

/// 启动 PTY 事件转发任务；事件流结束（会话释放）后清空运行标记。
///
/// 每转发一条 `Output` 就递增 `progress.forwarded`，供 D 事件等待。
fn spawn_forwarder(
    mut events: Receiver<ProcEvent>,
    out_tx: UnboundedSender<HelperEvent>,
    running: Arc<AtomicBool>,
    progress: watch::Sender<OutputProgress>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            let is_output = matches!(event, ProcEvent::Output { .. });
            if out_tx.send(translate_event(event)).is_err() {
                break;
            }
            if is_output {
                progress.send_modify(|state| {
                    state.forwarded = state.forwarded.saturating_add(1);
                });
            }
        }
        progress.send_modify(|state| state.closed = true);
        running.store(false, Ordering::SeqCst);
    })
}

/// 把 `termx-proc` 事件翻译为 helper 协议事件。
fn translate_event(event: ProcEvent) -> HelperEvent {
    match event {
        ProcEvent::Output { data, stream } => HelperEvent::Output {
            data: STANDARD.encode(data),
            stream: translate_stream(stream),
        },
        ProcEvent::Exited(code) => HelperEvent::Exited { code },
        ProcEvent::Signaled(signal) => HelperEvent::Signaled {
            signal: signal_name(signal),
        },
        // 本地 PTY 不产生该事件；保留映射以覆盖穷举并让将来的本地通道可直接复用。
        ProcEvent::ShellEvent(event) => HelperEvent::ShellEvent {
            event: event.kind.as_str().to_string(),
            code: event.code,
            id: event.id,
            command: event.command,
            started_at_ms: event.started_at_ms,
            ended_at_ms: event.ended_at_ms,
        },
    }
}

/// 映射输出流来源；helper 的 PTY 固定使用 `pty`。
fn translate_stream(stream: OutputStream) -> Stream {
    match stream {
        OutputStream::Pty => Stream::Pty,
        OutputStream::Stdout => Stream::Stdout,
        OutputStream::Stderr => Stream::Stderr,
    }
}

/// 把 Unix 信号编号转成协议里的信号名。
fn signal_name(signal: i32) -> String {
    match signal {
        1 => "SIGHUP".to_string(),
        2 => "SIGINT".to_string(),
        3 => "SIGQUIT".to_string(),
        6 => "SIGABRT".to_string(),
        9 => "SIGKILL".to_string(),
        15 => "SIGTERM".to_string(),
        other => format!("SIG{other}"),
    }
}

/// `termx-proc` 错误到协议错误码的映射。
fn map_proc_error(err: &ProcError) -> (ErrorCode, String) {
    let code = match err {
        ProcError::SpawnFailed { .. } => ErrorCode::SpawnFailed,
        ProcError::PtyFailed { .. } | ProcError::Unsupported => ErrorCode::PtyFailed,
        ProcError::NotRunning => ErrorCode::ProtocolError,
        ProcError::UnsupportedSignal(_) => ErrorCode::BadRequest,
        ProcError::WriteQueueFull => ErrorCode::LimitExceeded,
        ProcError::BarrierTimeout | ProcError::Io(_) | ProcError::Internal { .. } => {
            ErrorCode::Internal
        }
    };
    (code, err.to_string())
}

/// 发送错误帧。
fn send_error(out_tx: &UnboundedSender<HelperEvent>, code: ErrorCode, message: String) {
    let _ = out_tx.send(HelperEvent::Error { code, message });
}

/// 单任务串行写 stdout，保证 JSON Lines 不交错。
async fn write_loop(mut events: UnboundedReceiver<HelperEvent>) {
    let mut stdout = tokio::io::stdout();
    while let Some(event) = events.recv().await {
        let line = match to_json(&event) {
            Ok(line) => line,
            Err(err) => {
                warn!(error = %err, "helper 事件序列化失败");
                continue;
            }
        };
        if stdout.write_all(line.as_bytes()).await.is_err()
            || stdout.write_all(b"\n").await.is_err()
            || stdout.flush().await.is_err()
        {
            warn!("写入 stdout 失败，停止输出");
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proc_errors_map_to_protocol_codes() {
        let cases = [
            (
                ProcError::SpawnFailed {
                    message: "x".to_string(),
                },
                ErrorCode::SpawnFailed,
            ),
            (
                ProcError::PtyFailed {
                    message: "x".to_string(),
                },
                ErrorCode::PtyFailed,
            ),
            (ProcError::Unsupported, ErrorCode::PtyFailed),
            (ProcError::NotRunning, ErrorCode::ProtocolError),
            (
                ProcError::UnsupportedSignal("SIGHUP".to_string()),
                ErrorCode::BadRequest,
            ),
            (ProcError::WriteQueueFull, ErrorCode::LimitExceeded),
        ];

        for (err, expected) in cases {
            assert_eq!(map_proc_error(&err).0, expected, "{err:?}");
        }
    }

    #[test]
    fn signal_names_cover_common_values() {
        assert_eq!(signal_name(2), "SIGINT");
        assert_eq!(signal_name(15), "SIGTERM");
        assert_eq!(signal_name(31), "SIG31");
    }

    #[test]
    fn shell_events_map_to_protocol_frames() {
        use termx_protocol::shell_events::ShellEventKind;

        let event = translate_shell_event(ShellEvent {
            kind: ShellEventKind::CommandEnd,
            code: Some(7),
            id: Some("cmd-1".to_string()),
            command: Some("echo hi".to_string()),
            started_at_ms: Some(1_700_000_000_000),
            ended_at_ms: Some(1_700_000_001_000),
            data: None,
        });
        assert_eq!(
            event,
            HelperEvent::ShellEvent {
                event: "D".to_string(),
                code: Some(7),
                id: Some("cmd-1".to_string()),
                command: Some("echo hi".to_string()),
                started_at_ms: Some(1_700_000_000_000),
                ended_at_ms: Some(1_700_000_001_000),
            }
        );
    }

    #[test]
    fn streams_keep_protocol_meaning() {
        assert_eq!(translate_stream(OutputStream::Pty), Stream::Pty);
        assert_eq!(translate_stream(OutputStream::Stdout), Stream::Stdout);
        assert_eq!(translate_stream(OutputStream::Stderr), Stream::Stderr);
    }

    #[tokio::test]
    async fn command_end_waits_until_barrier_output_is_forwarded() {
        use std::sync::Mutex;
        use std::sync::atomic::AtomicUsize;
        use tokio::sync::mpsc;

        /// 假屏障：被调用时把一条 Output 入队，并报告已排空 1 条输出。
        struct FakeBarrier {
            calls: AtomicUsize,
            output: Mutex<Option<mpsc::Sender<ProcEvent>>>,
        }

        impl OutputBarrier for FakeBarrier {
            fn request(&self, _timeout: Duration) -> Result<BarrierReport, ProcError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if let Some(tx) = self
                    .output
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    tx.blocking_send(ProcEvent::Output {
                        data: b"hello".to_vec(),
                        stream: OutputStream::Pty,
                    })
                    .expect("模拟屏障输出入队失败");
                }
                Ok(BarrierReport::Drained { output_seq: 1 })
            }
        }

        /// 构造一条带 command-id 的 shell 事件。
        fn shell(kind: ShellEventKind, code: Option<i32>) -> ShellEvent {
            ShellEvent {
                kind,
                code,
                id: Some("cmd-1".to_string()),
                command: None,
                started_at_ms: None,
                ended_at_ms: None,
                data: None,
            }
        }

        let (out_tx, mut out_rx) = unbounded_channel::<HelperEvent>();
        let (proc_tx, proc_rx) = mpsc::channel::<ProcEvent>(8);
        let (shell_tx, shell_rx) = mpsc::channel::<ShellEvent>(8);
        let running = Arc::new(AtomicBool::new(true));
        let (progress_tx, progress_rx) = watch::channel(OutputProgress::default());
        let forwarder = spawn_forwarder(proc_rx, out_tx.clone(), Arc::clone(&running), progress_tx);
        let barrier = Arc::new(FakeBarrier {
            calls: AtomicUsize::new(0),
            output: Mutex::new(Some(proc_tx)),
        });
        let shell_forwarder =
            spawn_shell_event_forwarder(shell_rx, out_tx.clone(), barrier.clone(), progress_rx);

        // C 不触发屏障，立即转发。
        shell_tx
            .send(shell(ShellEventKind::CommandStart, None))
            .await
            .expect("发送 C");
        match out_rx.recv().await.expect("应收到 C") {
            HelperEvent::ShellEvent { event, .. } => assert_eq!(event, "C"),
            other => panic!("期望 C，实际 {other:?}"),
        }
        assert_eq!(
            barrier.calls.load(Ordering::SeqCst),
            0,
            "A/B/C/X 不应触发屏障"
        );

        // D 到达：屏障先把 Output 入队，D 必须等它转发完成后才能出现。
        shell_tx
            .send(shell(ShellEventKind::CommandEnd, Some(7)))
            .await
            .expect("发送 D");
        let mut saw_output = false;
        loop {
            match out_rx.recv().await.expect("等待转发事件") {
                HelperEvent::Output { data, .. } => {
                    assert_eq!(
                        STANDARD.decode(data).expect("output.data 非 base64"),
                        b"hello"
                    );
                    saw_output = true;
                }
                HelperEvent::ShellEvent { event, code, .. } if event == "D" => {
                    assert!(saw_output, "D 必须先等 Output 转发");
                    assert_eq!(code, Some(7));
                    break;
                }
                other => panic!("不应出现其它事件: {other:?}"),
            }
        }
        assert_eq!(barrier.calls.load(Ordering::SeqCst), 1, "只有 D 触发屏障");

        drop(shell_tx);
        drop(out_tx);
        let _ = forwarder.await;
        let _ = shell_forwarder.await;
    }

    #[tokio::test]
    async fn barrier_timeout_drops_command_end() {
        /// 始终报告超时的假屏障。
        struct TimeoutBarrier;

        impl OutputBarrier for TimeoutBarrier {
            fn request(&self, _timeout: Duration) -> Result<BarrierReport, ProcError> {
                Err(ProcError::BarrierTimeout)
            }
        }

        let (_tx, mut rx) = watch::channel(OutputProgress::default());
        let barrier: Arc<dyn OutputBarrier> = Arc::new(TimeoutBarrier);
        assert!(
            !wait_for_output_before_command_end(&barrier, &mut rx).await,
            "屏障超时应返回 false，让调用方丢弃 D"
        );
    }
}
