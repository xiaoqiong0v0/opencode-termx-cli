//! termx-service：本机 daemon（M0 占位）。
//!
//! M1 起提供环回 WebSocket + token 鉴权、`hello`、终端 RPC 与事件订阅。

fn main() {
    println!(
        "termx-service {} (M0 骨架，尚未实现 daemon)",
        env!("CARGO_PKG_VERSION")
    );
}
