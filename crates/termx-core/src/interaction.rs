//! 交互识别状态机：组合 shell integration（OSC 133/633）、前台进程组与
//! 静默时长，产出 `waiting_input` / `interactive_changed` 更新。
//!
//! 设计约束（对齐 `docs/REFERENCE_AND_PITFALLS.md` §4 与 `docs/PROTOCOL.md` §7）：
//! - **不自动判定完成、不自动退出**；本模块只产生非终态提示事件。
//! - 置信度：OSC prompt/command 结束 = `high`；前台进程组进入/切换/退出 =
//!   `medium`；无输出启发式在前台子进程存在时为 `medium`，仅 shell 静默时为
//!   `low`。每条 `waiting_input` 都必须带 `reason`。
//! - 只在状态变化或达到节流间隔时产出，避免每个 tick 都刷事件。
//! - `stdin_blocked` 需要真实的阻塞态证据（`wchan`/fd 等），当前跨平台后端
//!   未提供，因此本模块**不伪造**该 reason；等具备阻塞态采集后再启用。

use std::time::Duration;

use termx_protocol::{Confidence, EventKind, InteractionState, ProcNode};
use termx_terminal::{OscEvent, OscScanner};
use time::OffsetDateTime;

/// `waiting_input` 的 reason：shell integration 提示符/命令结束。
pub const REASON_SHELL_INTEGRATION: &str = "shell_integration";
/// `waiting_input` 的 reason：输出静默超过阈值。
pub const REASON_NO_OUTPUT: &str = "no_output";

/// 默认无输出阈值。
pub const DEFAULT_NO_OUTPUT_THRESHOLD: Duration = Duration::from_secs(5);
/// 同一等待信号默认的最小重复间隔（节流）。
pub const DEFAULT_REPEAT_INTERVAL: Duration = Duration::from_secs(30);

/// 交互识别配置；由 registry 固化到每个新建终端的 analyzer。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InteractionConfig {
    /// 是否启用交互识别；关闭后 `observe_output` / `sample` 都不产生更新。
    pub enabled: bool,
    /// 连续无输出达到该时长后尝试启发式 `waiting_input`。
    pub no_output_threshold: Duration,
    /// 同一 `(confidence, reason)` 等待信号的最小重复间隔。
    pub repeat_interval: Duration,
}

impl Default for InteractionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            no_output_threshold: DEFAULT_NO_OUTPUT_THRESHOLD,
            repeat_interval: DEFAULT_REPEAT_INTERVAL,
        }
    }
}

/// 当前前台交互对象。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractiveTarget {
    /// 交互进程 PID。
    pub pid: u32,
    /// 交互进程组 ID。
    pub pgid: u32,
    /// 命令行（进程树中缺失时为空）。
    pub command: Option<String>,
}

/// 一次交互状态变化；registry 负责转成协议事件并广播。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InteractionUpdate {
    /// 前台进入/切换/退出交互对象。
    InteractiveChanged {
        /// 是否处于交互态。
        active: bool,
        /// 交互进程 PID；退出时缺省。
        pid: Option<u32>,
        /// 交互进程组 ID；退出时缺省。
        pgid: Option<u32>,
        /// 交互进程命令；退出或缺省时为空。
        command: Option<String>,
        /// 置信度。
        confidence: Confidence,
    },
    /// 疑似等待输入。
    WaitingInput {
        /// 置信度。
        confidence: Confidence,
        /// 推断依据。
        reason: String,
    },
}

impl InteractionUpdate {
    /// 转成协议事件。
    pub fn into_event(self) -> EventKind {
        match self {
            Self::InteractiveChanged {
                active,
                pid,
                pgid,
                command,
                confidence,
            } => EventKind::InteractiveChanged {
                active,
                pid,
                pgid,
                command,
                confidence,
            },
            Self::WaitingInput { confidence, reason } => {
                EventKind::WaitingInput { confidence, reason }
            }
        }
    }
}

/// 当前等待信号的来源；用于区分 OSC 直接证据与静默启发式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitingSource {
    /// shell integration（OSC 133/633）。
    ShellIntegration,
    /// 输出静默启发式。
    Heuristic,
}

/// 每终端一份的交互识别状态机。
#[derive(Debug)]
pub struct InteractionAnalyzer {
    /// 识别配置。
    config: InteractionConfig,
    /// OSC 增量扫描器，跨输出分块保持状态。
    scanner: OscScanner,
    /// 最近一次输出时间；创建时以注册时间作为静默计时起点。
    last_output_at: Option<OffsetDateTime>,
    /// 当前前台交互对象。
    interactive: Option<InteractiveTarget>,
    /// 当前等待信号来源；`None` 表示未处于等待输入候选。
    waiting_source: Option<WaitingSource>,
    /// 当前等待信号置信度（供快照读取）。
    waiting_confidence: Confidence,
    /// 最近一次实际发出的等待信号 `(confidence, reason)`。
    last_emitted: Option<(Confidence, String)>,
    /// 最近一次实际发出等待信号的时间。
    last_emitted_at: Option<OffsetDateTime>,
    /// 对外快照使用的交互状态。
    state: InteractionState,
    /// 对外快照使用的置信度。
    confidence: Confidence,
}

impl Default for InteractionAnalyzer {
    fn default() -> Self {
        Self::new(InteractionConfig::default())
    }
}

impl InteractionAnalyzer {
    /// 用给定配置创建分析器；静默计时从首次输出/首次 `sample` 开始。
    pub fn new(config: InteractionConfig) -> Self {
        Self {
            config,
            scanner: OscScanner::new(),
            last_output_at: None,
            interactive: None,
            waiting_source: None,
            waiting_confidence: Confidence::Low,
            last_emitted: None,
            last_emitted_at: None,
            state: InteractionState::None,
            confidence: Confidence::Low,
        }
    }

    /// 以指定注册时间作为静默计时起点创建分析器。
    ///
    /// 用于“进程启动后一直没有输出”的终端：从创建时刻起累计静默时长，而不是
    /// 等到第一条输出才建立基线。
    pub fn with_start_time(config: InteractionConfig, now: OffsetDateTime) -> Self {
        let mut analyzer = Self::new(config);
        analyzer.last_output_at = Some(now);
        analyzer
    }

    /// 当前交互状态，供快照同步。
    pub fn state(&self) -> InteractionState {
        self.state
    }

    /// 当前置信度，供快照同步。
    pub fn confidence(&self) -> Confidence {
        self.confidence
    }

    /// 喂入一段终端输出，更新静默计时并处理 OSC 133/633。
    ///
    /// - `PromptStart`（`A`）/ `PromptEnd`（`B`）/ `CommandEnd`（`D`）：候选
    ///   `waiting_input{high, shell_integration}`；
    /// - `CommandStart`（`C`）：命令开始执行，清除等待输入候选；
    /// - 任意新输出都会结束此前的静默启发式等待（prompt 等待由后续 OSC 维持）。
    pub fn observe_output(&mut self, bytes: &[u8], now: OffsetDateTime) -> Vec<InteractionUpdate> {
        if !self.config.enabled {
            return Vec::new();
        }
        if !bytes.is_empty() {
            self.last_output_at = Some(now);
        }
        if self.waiting_source == Some(WaitingSource::Heuristic) {
            self.clear_waiting();
        }

        let mut updates = Vec::new();
        for event in self.scanner.push(bytes) {
            match event {
                // `A`/`B`/`D` 都是 shell 已把提示符交还给输入的信号：
                // `B` 是 prompt 结束（输入行开始），不能按“命令开始”清除等待。
                OscEvent::PromptStart(_) | OscEvent::PromptEnd(_) | OscEvent::CommandEnd(_) => {
                    if let Some(update) = self.consider_waiting(
                        now,
                        WaitingSource::ShellIntegration,
                        Confidence::High,
                        REASON_SHELL_INTEGRATION,
                    ) {
                        updates.push(update);
                    }
                }
                // `C` 才是命令开始执行/输出开始，必须清除等待候选。
                OscEvent::CommandStart(_) => self.clear_waiting(),
            }
        }
        self.refresh_state();
        updates
    }

    /// 采样一次前台进程组与进程树，产出状态变化。
    ///
    /// - `fg` 从前台为根进程组变为子进程组（或子进程组之间切换）→
    ///   `interactive_changed{active:true, confidence:medium}`；
    /// - 回到根进程组/查询不到前台组 → `interactive_changed{active:false}`；
    /// - 静默超过阈值且前台子进程存在 → `waiting_input{medium, no_output}`，
    ///   否则 `waiting_input{low, no_output}`。
    ///
    /// 调用方必须在 registry 锁内调用；`fg`/`tree` 由调用方在锁外采集。
    pub fn sample(
        &mut self,
        now: OffsetDateTime,
        fg: Option<u32>,
        root_pgid: u32,
        tree: &[ProcNode],
    ) -> Vec<InteractionUpdate> {
        if !self.config.enabled {
            return Vec::new();
        }

        let mut updates = Vec::new();
        let target = resolve_target(fg, root_pgid, tree);
        if let Some(update) = self.apply_interactive_change(target) {
            updates.push(update);
        }
        if let Some(update) = self.poll_silence(now) {
            updates.push(update);
        }
        self.refresh_state();
        updates
    }

    /// 前台目标变化时更新内部状态，仅在变化时产出事件。
    fn apply_interactive_change(
        &mut self,
        target: Option<InteractiveTarget>,
    ) -> Option<InteractionUpdate> {
        let previous = self.interactive.clone();
        let changed = match (&previous, &target) {
            (None, None) => false,
            (None, Some(_)) => true,
            (Some(_), None) => true,
            (Some(old), Some(new)) => old.pgid != new.pgid,
        };
        if !changed {
            return None;
        }
        self.interactive = target.clone();

        let update = match target {
            Some(target) => InteractionUpdate::InteractiveChanged {
                active: true,
                pid: Some(target.pid),
                pgid: Some(target.pgid),
                command: target.command,
                confidence: Confidence::Medium,
            },
            None => InteractionUpdate::InteractiveChanged {
                active: false,
                pid: None,
                pgid: None,
                command: None,
                confidence: Confidence::Medium,
            },
        };
        Some(update)
    }

    /// 静默启发式：OSC 高置信等待已存在时不降级。
    ///
    /// 置信度只看前台是否真的是子进程：`fg=None`（stdio/无 TTY）或仍为根
    /// 进程组时保持 `low`，只有前台 pgrp 指向子进程时才升为 `medium`。
    fn poll_silence(&mut self, now: OffsetDateTime) -> Option<InteractionUpdate> {
        if self.waiting_source == Some(WaitingSource::ShellIntegration) {
            return None;
        }
        let last_output_at = self.last_output_at?;
        if now - last_output_at < to_time_duration(self.config.no_output_threshold) {
            return None;
        }
        let confidence = if self.interactive.is_some() {
            Confidence::Medium
        } else {
            Confidence::Low
        };
        self.consider_waiting(now, WaitingSource::Heuristic, confidence, REASON_NO_OUTPUT)
    }

    /// 记录等待候选并按“新状态 / 信号变化 / 节流到期”决定是否产出。
    fn consider_waiting(
        &mut self,
        now: OffsetDateTime,
        source: WaitingSource,
        confidence: Confidence,
        reason: &str,
    ) -> Option<InteractionUpdate> {
        let became_active = self.waiting_source.is_none();
        self.waiting_source = Some(source);
        self.waiting_confidence = confidence;

        let signature = (confidence, reason.to_string());
        let changed = self.last_emitted.as_ref() != Some(&signature);
        let due = self
            .last_emitted_at
            .is_none_or(|last| now - last >= to_time_duration(self.config.repeat_interval));
        if !became_active && !changed && !due {
            return None;
        }

        self.last_emitted = Some(signature);
        self.last_emitted_at = Some(now);
        Some(InteractionUpdate::WaitingInput {
            confidence,
            reason: reason.to_string(),
        })
    }

    /// 清除等待候选（命令开始执行或产生新输出）。
    fn clear_waiting(&mut self) {
        self.waiting_source = None;
        self.waiting_confidence = Confidence::Low;
    }

    /// 根据等待/交互状态刷新快照字段。
    fn refresh_state(&mut self) {
        if self.waiting_source.is_some() {
            self.state = InteractionState::WaitingInput;
            self.confidence = self.waiting_confidence;
        } else if self.interactive.is_some() {
            self.state = InteractionState::Interactive;
            self.confidence = Confidence::Medium;
        } else {
            self.state = InteractionState::None;
            self.confidence = Confidence::Low;
        }
    }
}

/// 前台进程组是否指向根进程组之外的子进程。
///
/// `stdio`/无 TTY 时没有前台进程组（`fg=None`）；尚未执行子进程时前台组仍
/// 等于根组。两种情况都不构成“前台存在子进程”的证据，不能据此提升置信度。
fn is_foreground_child(fg: Option<u32>, root_pgid: u32) -> bool {
    fg.is_some_and(|pgid| pgid != 0 && pgid != root_pgid)
}

/// 由前台进程组与进程树解析交互对象；前台为根进程组时视为无交互对象。
fn resolve_target(fg: Option<u32>, root_pgid: u32, tree: &[ProcNode]) -> Option<InteractiveTarget> {
    if !is_foreground_child(fg, root_pgid) {
        return None;
    }
    let pgid = fg?;
    // 优先取“自身即组长”的节点（shell 作业通常如此），否则取同组任一节点。
    let node = tree
        .iter()
        .find(|node| node.pgid == pgid && node.pid == pgid)
        .or_else(|| tree.iter().find(|node| node.pgid == pgid));
    Some(InteractiveTarget {
        pid: node.map_or(pgid, |node| node.pid),
        pgid,
        command: node
            .map(|node| node.cmd.clone())
            .filter(|cmd| !cmd.is_empty()),
    })
}

/// 把 std 时长转换为 time 时长；超出表示范围时按上限处理。
fn to_time_duration(duration: Duration) -> time::Duration {
    time::Duration::try_from(duration).unwrap_or(time::Duration::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 固定基准时间，避免真实时钟造成抖动。
    fn at(offset_ms: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("合法时间戳")
            + time::Duration::milliseconds(offset_ms)
    }

    /// 默认测试配置：静默阈值 1s、重复间隔 5s。
    fn analyzer() -> InteractionAnalyzer {
        InteractionAnalyzer::new(InteractionConfig {
            enabled: true,
            no_output_threshold: Duration::from_millis(1000),
            repeat_interval: Duration::from_millis(5000),
        })
    }

    /// 构造进程树节点。
    fn node(pid: u32, ppid: u32, pgid: u32, cmd: &str) -> ProcNode {
        ProcNode {
            pid,
            ppid,
            pgid,
            cmd: cmd.to_string(),
        }
    }

    #[test]
    fn prompt_start_emits_high_confidence_shell_integration() {
        let mut analyzer = analyzer();
        let updates = analyzer.observe_output(b"\x1b]133;A\x07", at(0));

        assert_eq!(
            updates,
            vec![InteractionUpdate::WaitingInput {
                confidence: Confidence::High,
                reason: REASON_SHELL_INTEGRATION.to_string(),
            }]
        );
        assert_eq!(analyzer.state(), InteractionState::WaitingInput);
        assert_eq!(analyzer.confidence(), Confidence::High);
    }

    #[test]
    fn command_start_clears_osc_waiting_candidate() {
        let mut analyzer = analyzer();
        analyzer.observe_output(b"\x1b]133;A\x07", at(0));
        let updates = analyzer.observe_output(b"\x1b]133;C\x07", at(10));

        assert!(updates.is_empty(), "清除等待不应产出额外事件");
        assert_eq!(analyzer.state(), InteractionState::None);
        assert_eq!(analyzer.confidence(), Confidence::Low);
    }

    #[test]
    fn prompt_end_keeps_waiting_input_within_same_chunk() {
        let mut analyzer = analyzer();
        // `A`（prompt 开始）与 `B`（prompt 结束）在同一输出块到达：
        // `B` 表示提示符已绘制、等待用户输入，不能把 `A` 的等待状态复位。
        let updates = analyzer.observe_output(b"\x1b]133;A\x07\x1b]133;B\x07", at(0));

        assert_eq!(
            updates,
            vec![InteractionUpdate::WaitingInput {
                confidence: Confidence::High,
                reason: REASON_SHELL_INTEGRATION.to_string(),
            }],
            "`B` 不应清除或重复投递等待事件"
        );
        assert_eq!(analyzer.state(), InteractionState::WaitingInput);
        assert_eq!(analyzer.confidence(), Confidence::High);
    }

    #[test]
    fn prompt_end_alone_emits_waiting_input() {
        let mut analyzer = analyzer();
        let updates = analyzer.observe_output(b"\x1b]633;B\x07", at(0));

        assert_eq!(
            updates,
            vec![InteractionUpdate::WaitingInput {
                confidence: Confidence::High,
                reason: REASON_SHELL_INTEGRATION.to_string(),
            }]
        );
        assert_eq!(analyzer.state(), InteractionState::WaitingInput);
    }

    #[test]
    fn command_end_emits_waiting_again() {
        let mut analyzer = analyzer();
        analyzer.observe_output(b"\x1b]133;C\x07", at(0));
        let updates = analyzer.observe_output(b"\x1b]133;D;0\x07", at(10));

        assert_eq!(
            updates,
            vec![InteractionUpdate::WaitingInput {
                confidence: Confidence::High,
                reason: REASON_SHELL_INTEGRATION.to_string(),
            }]
        );
        assert_eq!(analyzer.state(), InteractionState::WaitingInput);
    }

    #[test]
    fn foreground_change_emits_interactive_changed_and_return_to_root() {
        let mut analyzer = analyzer();
        let tree = vec![node(10, 1, 50, "sh"), node(20, 10, 20, "python3")];

        let updates = analyzer.sample(at(0), Some(20), 50, &tree);
        assert_eq!(
            updates,
            vec![InteractionUpdate::InteractiveChanged {
                active: true,
                pid: Some(20),
                pgid: Some(20),
                command: Some("python3".to_string()),
                confidence: Confidence::Medium,
            }]
        );
        assert_eq!(analyzer.state(), InteractionState::Interactive);
        assert_eq!(analyzer.confidence(), Confidence::Medium);

        let updates = analyzer.sample(at(10), Some(50), 50, &tree);
        assert_eq!(
            updates,
            vec![InteractionUpdate::InteractiveChanged {
                active: false,
                pid: None,
                pgid: None,
                command: None,
                confidence: Confidence::Medium,
            }]
        );
        assert_eq!(analyzer.state(), InteractionState::None);
    }

    #[test]
    fn same_foreground_group_does_not_repeat_interactive_changed() {
        let mut analyzer = analyzer();
        let tree = vec![node(20, 10, 20, "python3")];

        assert_eq!(analyzer.sample(at(0), Some(20), 50, &tree).len(), 1);
        assert!(analyzer.sample(at(10), Some(20), 50, &tree).is_empty());
        assert!(analyzer.sample(at(20), Some(20), 50, &tree).is_empty());
    }

    #[test]
    fn silence_heuristic_uses_low_without_foreground_child() {
        let mut analyzer = analyzer();
        analyzer.observe_output(b"hello", at(0));

        let updates = analyzer.sample(at(1000), Some(50), 50, &[]);
        assert_eq!(
            updates,
            vec![InteractionUpdate::WaitingInput {
                confidence: Confidence::Low,
                reason: REASON_NO_OUTPUT.to_string(),
            }]
        );
        assert_eq!(analyzer.state(), InteractionState::WaitingInput);
        assert_eq!(analyzer.confidence(), Confidence::Low);
    }

    #[test]
    fn silence_heuristic_stays_low_for_unknown_foreground() {
        // stdio/无 TTY：`fg=None` 不构成前台子进程证据，只能是 low，且不得
        // 伪造 `interactive_changed`。
        let mut analyzer = analyzer();
        analyzer.observe_output(b"hello", at(0));

        let updates = analyzer.sample(at(1000), None, 50, &[]);
        assert_eq!(
            updates,
            vec![InteractionUpdate::WaitingInput {
                confidence: Confidence::Low,
                reason: REASON_NO_OUTPUT.to_string(),
            }]
        );
        assert_eq!(analyzer.state(), InteractionState::WaitingInput);
        assert_eq!(analyzer.confidence(), Confidence::Low);
    }

    #[test]
    fn silence_heuristic_uses_medium_with_foreground_child() {
        let mut analyzer = analyzer();
        analyzer.observe_output(b"hello", at(0));
        let tree = vec![node(20, 10, 20, "python3")];

        let updates = analyzer.sample(at(1000), Some(20), 50, &tree);
        assert_eq!(
            updates,
            vec![
                InteractionUpdate::InteractiveChanged {
                    active: true,
                    pid: Some(20),
                    pgid: Some(20),
                    command: Some("python3".to_string()),
                    confidence: Confidence::Medium,
                },
                InteractionUpdate::WaitingInput {
                    confidence: Confidence::Medium,
                    reason: REASON_NO_OUTPUT.to_string(),
                },
            ]
        );
        assert_eq!(analyzer.state(), InteractionState::WaitingInput);
        assert_eq!(analyzer.confidence(), Confidence::Medium);
    }

    #[test]
    fn repeated_silence_is_throttled_but_reemitted_after_interval() {
        let mut analyzer = analyzer();
        analyzer.observe_output(b"hello", at(0));

        assert_eq!(analyzer.sample(at(1000), Some(50), 50, &[]).len(), 1);
        assert!(
            analyzer.sample(at(1200), Some(50), 50, &[]).is_empty(),
            "未到重复间隔不应再次投递"
        );
        assert!(analyzer.sample(at(5900), Some(50), 50, &[]).is_empty());
        assert_eq!(
            analyzer.sample(at(6000), Some(50), 50, &[]).len(),
            1,
            "达到重复间隔后应再次投递"
        );
    }

    #[test]
    fn new_output_clears_heuristic_waiting_but_keeps_osc_waiting() {
        let mut analyzer = analyzer();
        analyzer.observe_output(b"hello", at(0));
        analyzer.sample(at(1000), Some(50), 50, &[]);
        assert_eq!(analyzer.state(), InteractionState::WaitingInput);

        // 新输出结束静默等待。
        assert!(analyzer.observe_output(b"more", at(1100)).is_empty());
        assert_eq!(analyzer.state(), InteractionState::None);

        // OSC prompt 等待不会被普通输出清除。
        analyzer.observe_output(b"\x1b]133;A\x07", at(1200));
        analyzer.observe_output(b"prompt text", at(1210));
        assert_eq!(analyzer.state(), InteractionState::WaitingInput);
        assert_eq!(analyzer.confidence(), Confidence::High);
    }

    #[test]
    fn osc_waiting_is_not_downgraded_by_silence_heuristic() {
        let mut analyzer = analyzer();
        analyzer.observe_output(b"\x1b]133;A\x07", at(0));
        assert!(analyzer.sample(at(1000), Some(50), 50, &[]).is_empty());
        assert_eq!(analyzer.confidence(), Confidence::High);
    }

    #[test]
    fn disabled_analyzer_produces_no_updates() {
        let mut analyzer = InteractionAnalyzer::new(InteractionConfig {
            enabled: false,
            ..InteractionConfig::default()
        });
        assert!(analyzer.observe_output(b"\x1b]133;A\x07", at(0)).is_empty());
        assert!(analyzer.sample(at(1000), Some(20), 50, &[]).is_empty());
        assert_eq!(analyzer.state(), InteractionState::None);
    }
}
