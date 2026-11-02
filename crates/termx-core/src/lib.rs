//! 会话状态机、registry、事件总线与背压（M0 占位）。
//!
//! 依赖方向：本 crate 位于 `protocol` 之上，`service` / `cli` 之下；
//! M1 起实现会话键 `(root_session_id, name)`、attach/detach 与订阅回放。

use termx_protocol::LifecycleState;

/// 会话注册表占位；M1 起承载 `(root_session_id, name)` → 终端映射。
#[derive(Debug, Default)]
pub struct SessionRegistry;

/// 判断当前生命周期状态是否接受写操作（终态拒绝）。
pub fn accepts_input(state: LifecycleState) -> bool {
    !state.is_terminal()
}
