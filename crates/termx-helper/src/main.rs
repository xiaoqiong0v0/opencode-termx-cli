//! termx-helper：远端 sidecar（M0 占位）。
//!
//! M6 起随 SSH 会话分发，提供进程树/退出状态增强与降级握手。

fn main() {
    println!(
        "termx-helper {} (M0 骨架，尚未实现 sidecar)",
        env!("CARGO_PKG_VERSION")
    );
}
