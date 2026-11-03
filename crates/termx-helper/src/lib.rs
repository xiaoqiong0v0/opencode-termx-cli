//! `termx-helper` 的 stdio sidecar 服务端实现。
//!
//! 协议见 `docs/PROTOCOL.md` §11：
//! - stdin 每行一个 [`HelperRequest`]，首帧必须是 `hello`；
//! - stdout 每行一个 [`HelperEvent`]，**只承载协议 JSON**；
//! - 日志一律写 stderr（tracing）；
//! - 同一时刻只维护一个 PTY 会话，`spawn` 会先清理上一会话；
//! - stdin EOF 或进程退出前终止整棵进程树，确保无孤儿。
//!
//! 线程边界见 `docs/ASYNC.md`：stdin 请求循环、PTY 事件转发任务、stdout 写任务
//! 三者相互独立；所有可能阻塞的 PTY 操作都经 `spawn_blocking` 执行。

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use termx_proc::{OutputStream, ProcError, ProcEvent, PtyConfig, PtySession};
use termx_protocol::helper::{HELPER_CAPABILITIES, HelperEvent, HelperRequest};
use termx_protocol::{ErrorCode, HELPER_PROTOCOL_VERSION, Stream, to_json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, Stdin};
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

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
}

/// 单个 helper 会话及其后台转发任务。
struct HelperState {
    /// 当前 PTY 会话；`None` 表示尚未 spawn 或已清理。
    session: Option<PtySession>,
    /// PTY 事件转发任务；随会话创建，会话释放后结束。
    forwarder: Option<JoinHandle<()>>,
    /// 会话是否仍在运行；由转发任务在事件流结束时置 `false`。
    running: Arc<AtomicBool>,
}

impl HelperState {
    /// 创建空会话状态。
    fn new() -> Self {
        Self {
            session: None,
            forwarder: None,
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
            } => {
                let spec = SpawnSpec {
                    cmd,
                    args,
                    cwd,
                    env,
                    cols,
                    rows,
                };
                self.spawn(spec, out_tx).await;
            }
            HelperRequest::Input { data } => self.input(data, out_tx).await,
            HelperRequest::Resize { cols, rows } => self.resize(cols, rows, out_tx).await,
            HelperRequest::Signal { signal } => self.signal(signal, out_tx).await,
            HelperRequest::Terminate => self.terminate(out_tx).await,
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

        let config = PtyConfig {
            program: spec.cmd,
            args: spec.args,
            cwd: spec.cwd,
            env: spec.env,
            cols: spec.cols.unwrap_or(DEFAULT_COLS),
            rows: spec.rows.unwrap_or(DEFAULT_ROWS),
        };
        let spawned = tokio::task::spawn_blocking(move || PtySession::spawn(config)).await;

        match spawned {
            Ok(Ok((session, events))) => {
                let (pid, pgid) = (session.pid(), session.pgid());
                self.session = Some(session);
                self.running.store(true, Ordering::SeqCst);
                let _ = out_tx.send(HelperEvent::Started { pid, pgid });
                self.forwarder = Some(spawn_forwarder(
                    events,
                    out_tx.clone(),
                    Arc::clone(&self.running),
                ));
                debug!(pid, pgid, "PTY 会话已启动");
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
            Ok(()) => self.running.store(false, Ordering::SeqCst),
            Err(err) => self.report(err, out_tx),
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
        self.running.store(false, Ordering::SeqCst);
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

/// 启动 PTY 事件转发任务；事件流结束（会话释放）后清空运行标记。
fn spawn_forwarder(
    mut events: Receiver<ProcEvent>,
    out_tx: UnboundedSender<HelperEvent>,
    running: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            let _ = out_tx.send(translate_event(event));
        }
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
        ProcError::Io(_) | ProcError::Internal { .. } => ErrorCode::Internal,
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
    fn streams_keep_protocol_meaning() {
        assert_eq!(translate_stream(OutputStream::Pty), Stream::Pty);
        assert_eq!(translate_stream(OutputStream::Stdout), Stream::Stdout);
        assert_eq!(translate_stream(OutputStream::Stderr), Stream::Stderr);
    }
}
