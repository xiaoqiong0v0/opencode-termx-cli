//! 自研 VT/ANSI 终端模型与 shell integration 信号解析。
//!
//! 当前提供：
//! - [`OscScanner`]：OSC 133/633 增量扫描，识别 `A`/`B`/`C`/`D` 四个命令；
//! - [`TerminalModel`]：屏幕模型占位，后续里程碑补齐 VT/ANSI 解析与缓冲。
//!
//! [`OscScanner`] **不做完整 VT 解析**：只处理以 `ESC ]` 起始的 OSC 字符串，
//! 终止符仅支持 `BEL (0x07)` 与 `ST (ESC \)`（不支持 C1 `0x9C`），其他 OSC、
//! CSI 与普通文本全部忽略。未终止的 OSC 载荷有固定上限，超限后丢弃并恢复
//! 普通解析，避免缓冲区无限增长。

mod osc;

pub use osc::{OscEvent, OscScanner, OscSource};

/// 终端屏幕尺寸（列 × 行）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Size {
    /// 列数。
    pub cols: u16,
    /// 行数。
    pub rows: u16,
}

/// 终端模型占位：后续承载 VT/ANSI 解析状态与屏幕缓冲。
#[derive(Debug, Default)]
pub struct TerminalModel {
    /// 当前屏幕尺寸。
    size: Size,
}

impl TerminalModel {
    /// 按给定尺寸创建空模型。
    pub fn new(size: Size) -> Self {
        Self { size }
    }

    /// 返回当前屏幕尺寸。
    pub fn size(&self) -> Size {
        self.size
    }
}
