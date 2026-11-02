//! 自研 VT/ANSI 终端模型（M0 占位）。
//!
//! 计划在 M2+ 实现 VT/ANSI 解析、屏幕与滚动缓冲、光标/模式与键盘编码。
//! 本阶段只提供最小类型占位，不含解析实现，也不引入任何 OS 依赖。

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
