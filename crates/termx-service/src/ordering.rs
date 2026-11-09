//! 本地带外 shell 事件与 PTY 输出的显式顺序屏障。
//!
//! `ingest_proc_events` 与 `ingest_shell_events` 是两条独立任务；D 事件可能
//! 先于 Output 处理。本模块用两步收敛这种情况，不依赖任何时间窗口：
//!
//! 1. 后端读侧屏障：排空当前已可读的 PTY 输出并把 `Output` 入队；
//! 2. 处理进度门：等 `ingest_proc_events` 处理到屏障返回的序号后，才应用 D。
//!
//! 超时/失败时返回 `false`，调用方应丢弃该 D，让 transcript 退化为静默判定，
//! 不得伪造退出码。

use std::sync::Arc;
use std::time::Duration;

use termx_core::TerminalBackend;
use termx_proc::BarrierReport;
use tokio::sync::watch;
use tracing::warn;

/// 单终端的 Output 处理进度快照。
#[derive(Debug, Clone, Copy, Default)]
pub struct OutputProgress {
    /// `ingest_proc_events` 已处理完成的 `Output` 事件累计数。
    processed: u64,
    /// `ingest_proc_events` 是否已结束。
    closed: bool,
}

/// 追踪 `ingest_proc_events` 已处理到的 Output 事件序号。
#[derive(Debug)]
pub struct OutputGate {
    /// 进度广播；`watch` 保留最新值，订阅方不会漏掉已达成状态。
    progress: watch::Sender<OutputProgress>,
}

impl OutputGate {
    /// 创建进度为 0 的门。
    pub fn new() -> Self {
        let (progress, _) = watch::channel(OutputProgress::default());
        Self { progress }
    }

    /// `ingest_proc_events` 处理完一条 Output 后调用。
    pub fn record_processed(&self) {
        self.progress.send_modify(|state| {
            state.processed = state.processed.saturating_add(1);
        });
    }

    /// `ingest_proc_events` 结束时调用；所有等待者应尽快收敛。
    pub fn close(&self) {
        self.progress.send_modify(|state| state.closed = true);
    }

    /// 已处理完成的 Output 数。
    pub fn processed(&self) -> u64 {
        self.progress.borrow().processed
    }

    /// 订阅进度变化。
    fn subscribe(&self) -> watch::Receiver<OutputProgress> {
        self.progress.subscribe()
    }
}

impl Default for OutputGate {
    fn default() -> Self {
        Self::new()
    }
}

/// 等待 `processed >= target`；进度门关闭或超时返回 `false`。
async fn wait_processed(
    progress: &mut watch::Receiver<OutputProgress>,
    target: u64,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let current = *progress.borrow_and_update();
            if current.processed >= target {
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

/// D 事件应用前的输出屏障。
///
/// 先让后端排空当前可读输出，再等 `ingest_proc_events` 处理到该输出序号。
/// 返回 `false` 表示屏障失败或超时，调用方应丢弃该 D 并退化为静默判定。
/// 后端不支持屏障（stdio/Windows/SSH）时返回 `true`，保持既有行为。
pub async fn barrier_before_command_end(
    backend: Arc<dyn TerminalBackend>,
    gate: &OutputGate,
    timeout: Duration,
) -> bool {
    let target = match tokio::task::spawn_blocking(move || backend.barrier(timeout)).await {
        Ok(Ok(BarrierReport::Drained { output_seq })) => output_seq,
        Ok(Ok(BarrierReport::Unsupported)) => return true,
        Ok(Err(error)) => {
            warn!(%error, "PTY 输出屏障失败，transcript 退化为静默判定");
            return false;
        }
        Err(error) => {
            warn!(%error, "PTY 输出屏障任务失败，transcript 退化为静默判定");
            return false;
        }
    };

    if target <= gate.processed() {
        return true;
    }
    let mut progress = gate.subscribe();
    if wait_processed(&mut progress, target, timeout).await {
        return true;
    }
    warn!(
        target,
        processed = gate.processed(),
        "等待 PTY 输出处理超时，transcript 退化为静默判定"
    );
    false
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use termx_core::{CoreError, MockFactory, Registry, TerminalBackend, TerminalId};
    use termx_proc::BarrierReport;
    use termx_protocol::shell_events::{ShellEvent, ShellEventKind};
    use termx_protocol::{CreateParams, Mode, ProcNode, TerminalLocator, TranscriptStatus};
    use time::OffsetDateTime;

    use super::{OutputGate, barrier_before_command_end};

    /// 固定屏障结果的后端；只用于验证顺序门。
    struct FixedBackend {
        report: BarrierReport,
    }

    impl TerminalBackend for FixedBackend {
        fn pid(&self) -> u32 {
            0
        }

        fn pgid(&self) -> u32 {
            0
        }

        fn foreground_pgrp(&self) -> Option<u32> {
            None
        }

        fn tree(&self) -> Vec<ProcNode> {
            Vec::new()
        }

        fn write(&self, _data: &[u8]) -> Result<(), CoreError> {
            Ok(())
        }

        fn resize(&self, _cols: u16, _rows: u16) -> Result<(), CoreError> {
            Ok(())
        }

        fn signal(&self, _name: &str) -> Result<(), CoreError> {
            Ok(())
        }

        fn terminate(&self) {}

        fn barrier(&self, _timeout: Duration) -> Result<BarrierReport, CoreError> {
            Ok(self.report)
        }
    }

    /// 构造使用 mock 后端的 registry。
    fn registry() -> Registry {
        Registry::with_factory(64, Arc::new(MockFactory))
    }

    /// 创建参数。
    fn params() -> CreateParams {
        CreateParams {
            mode: Mode::Pty,
            cmd: "mock".to_string(),
            args: Vec::new(),
            cwd: None,
            env: None,
            cols: None,
            rows: None,
            root_session_id: "ses-1".to_string(),
            name: "build".to_string(),
            force: false,
            idle_timeout: None,
            ssh: None,
            allow_injection: false,
            runtime_id: Some("termx-bash".to_string()),
        }
    }

    /// 按 terminal_id 定位。
    fn by_id(id: &str) -> TerminalLocator {
        TerminalLocator {
            terminal_id: Some(id.to_string()),
            root_session_id: None,
            name: None,
        }
    }

    /// 固定基准时间。
    fn base() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("合法时间戳")
    }

    /// 构造带 command-id 的 shell 事件。
    fn event(kind: ShellEventKind, code: Option<i32>) -> ShellEvent {
        ShellEvent {
            kind,
            code,
            id: Some("cmd-1".to_string()),
            command: None,
            started_at_ms: None,
            ended_at_ms: None,
        }
    }

    #[tokio::test]
    async fn command_end_waits_for_prior_output_before_finalizing() {
        let mut registry = registry();
        let created = registry.create(params()).expect("create");
        let id = TerminalId::new(created.terminal_id.clone());
        let t0 = base();
        let mut tracker = crate::shell_events::ShellEventTracker::new();

        registry
            .record_transcript_command(&id, "echo hi", t0)
            .expect("记录命令");
        assert!(
            !crate::shell_events::apply_event(
                &mut registry,
                &id,
                &mut tracker,
                &event(ShellEventKind::CommandStart, None),
                t0,
            )
            .expect("C")
        );

        // D 先到达：屏障目标为 1，但此时 Output 尚未被 ingest_proc_events 处理。
        let gate = Arc::new(OutputGate::new());
        let backend = Arc::new(FixedBackend {
            report: BarrierReport::Drained { output_seq: 1 },
        });
        let mut barrier = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                barrier_before_command_end(backend, &gate, Duration::from_secs(2)).await
            })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut barrier)
                .await
                .is_err(),
            "D 不应在 Output 处理前完成屏障"
        );

        // 现在才处理 Output；屏障应立即收敛。
        registry
            .record_transcript_output(&id, "hi\r\n", t0)
            .expect("记录输出");
        gate.record_processed();
        assert!(
            barrier.await.expect("屏障任务不应 panic"),
            "Output 处理后屏障应完成"
        );

        // 应用真实 D：输出必须仍在 transcript 中。
        assert!(
            crate::shell_events::apply_event(
                &mut registry,
                &id,
                &mut tracker,
                &event(ShellEventKind::CommandEnd, Some(7)),
                t0,
            )
            .expect("D")
        );
        let commands = registry
            .transcript(&by_id(&created.terminal_id), None)
            .expect("读取 transcript")
            .commands;
        assert_eq!(commands[0].status, TranscriptStatus::Done);
        assert_eq!(commands[0].exit_code, Some(7));
        assert!(
            commands[0].output.contains("hi"),
            "逆序到达的 Output 不应丢失: {:?}",
            commands[0].output
        );
    }

    #[tokio::test]
    async fn barrier_timeout_degrades_to_silent_determination() {
        let gate = OutputGate::new();
        let backend = Arc::new(FixedBackend {
            report: BarrierReport::Drained { output_seq: 1 },
        });
        let ready = barrier_before_command_end(backend, &gate, Duration::from_millis(50)).await;
        assert!(!ready, "超时应返回 false，让调用方丢弃 D");
    }

    #[tokio::test]
    async fn unsupported_backend_skips_barrier() {
        let gate = OutputGate::new();
        let backend = Arc::new(FixedBackend {
            report: BarrierReport::Unsupported,
        });
        assert!(
            barrier_before_command_end(backend, &gate, Duration::from_secs(1)).await,
            "不支持屏障的后端应保持既有行为"
        );
    }
}
