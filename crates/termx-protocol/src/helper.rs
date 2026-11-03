//! `termx-helper` sidecar 的 JSON Lines 协议。
//!
//! 与主协议一致，本模块只做类型定义，不含任何 OS 依赖：
//! - 请求以 `method` 为标签（`hello` / `spawn` / `input` / `resize` / `signal` /
//!   `terminate` / `tree`）；
//! - 响应与事件以 `type` 为标签（`hello_ok` / `started` / `output` / `exited` /
//!   `signaled` / `tree` / `error`）。
//!
//! 复用主协议的 [`Stream`] 与 [`ErrorCode`]：helper 是主协议链路的延伸，
//! 客户端已经按同一套错误码与流语义做映射；额外定义一套只会增加两处漂移风险。
//! 字段统一 snake_case，一行一个 JSON 对象（见 `docs/PROTOCOL.md` §11）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ErrorCode, ProcNode, Stream};

/// helper 声明的能力列表，顺序固定，便于测试与文档核对。
pub const HELPER_CAPABILITIES: &[&str] = &["pty", "proc_tree", "exit_status"];

/// helper 请求（客户端 → helper），以 `method` 为标签。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum HelperRequest {
    /// 首帧握手；`proto` 必须等于 [`crate::HELPER_PROTOCOL_VERSION`]。
    Hello {
        /// helper 协议版本。
        proto: String,
        /// 客户端版本标识（仅记录，不参与校验）。
        version: String,
    },
    /// 在本地 PTY 中启动子进程；同一时刻只允许一个会话。
    Spawn {
        /// 可执行程序。
        cmd: String,
        /// 程序参数。
        #[serde(default)]
        args: Vec<String>,
        /// 工作目录；缺省继承 helper 当前目录。
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        /// 环境变量覆盖表；缺省完整继承父环境。
        #[serde(skip_serializing_if = "Option::is_none")]
        env: Option<BTreeMap<String, String>>,
        /// 列数；缺省使用 80。
        #[serde(skip_serializing_if = "Option::is_none")]
        cols: Option<u16>,
        /// 行数；缺省使用 24。
        #[serde(skip_serializing_if = "Option::is_none")]
        rows: Option<u16>,
    },
    /// 向 PTY 写入原始字节（base64）。
    Input {
        /// base64 编码的输入字节。
        data: String,
    },
    /// 调整 PTY 尺寸。
    Resize {
        /// 列数。
        cols: u16,
        /// 行数。
        rows: u16,
    },
    /// 向子进程发送信号（`SIGINT` / `SIGTERM` / `SIGKILL`）。
    Signal {
        /// 信号名；大小写与 `SIG` 前缀不敏感。
        signal: String,
    },
    /// 终止整棵进程树并释放 PTY（幂等）。
    Terminate,
    /// 查询当前会话的远端进程树；无活动会话时返回空列表。
    Tree,
}

/// helper 响应/事件（helper → 客户端），以 `type` 为标签。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HelperEvent {
    /// 握手成功。
    HelloOk {
        /// helper 协议版本。
        proto: String,
        /// 能力列表，取值见 [`HELPER_CAPABILITIES`]。
        capabilities: Vec<String>,
    },
    /// 进程已创建。
    Started {
        /// 根进程 ID。
        pid: u32,
        /// 进程组 ID（Windows 上等于 `pid`）。
        pgid: u32,
    },
    /// 一段 PTY 输出。
    Output {
        /// base64 编码的输出字节。
        data: String,
        /// 输出流来源；helper 固定使用 `pty`。
        stream: Stream,
    },
    /// 进程自然退出。
    Exited {
        /// 退出码。
        code: i32,
    },
    /// 进程被信号终止（仅 Unix）。
    Signaled {
        /// 信号名，如 `SIGTERM`；未知编号回退为 `SIG<num>`。
        signal: String,
    },
    /// 进程树快照，对应一次 `tree` 请求。
    ///
    /// 请求与响应按 helper 的串行处理顺序一一对应；客户端应用队列保存待回填
    /// 的请求方，收到本事件时弹出队首。
    Tree {
        /// 以会话根进程为起点的进程树；无活动会话时为空。
        processes: Vec<ProcNode>,
    },
    /// 请求失败或协议违例；不改变当前会话状态。
    Error {
        /// 错误码，复用主协议词表。
        code: ErrorCode,
        /// 人类可读说明（不含原始输入输出）。
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;

    /// 请求与事件都应能与文档中的 JSON 形状互转。
    #[test]
    fn requests_round_trip_and_use_method_tag() {
        let mut env = BTreeMap::new();
        env.insert("TERM".to_string(), "xterm-256color".to_string());

        let requests = vec![
            HelperRequest::Hello {
                proto: "termx-helper/1".to_string(),
                version: "0.1.0".to_string(),
            },
            HelperRequest::Spawn {
                cmd: "bash".to_string(),
                args: vec!["-l".to_string()],
                cwd: Some("/tmp".to_string()),
                env: Some(env),
                cols: Some(120),
                rows: Some(30),
            },
            HelperRequest::Input {
                data: "bHM=".to_string(),
            },
            HelperRequest::Resize {
                cols: 100,
                rows: 40,
            },
            HelperRequest::Signal {
                signal: "SIGINT".to_string(),
            },
            HelperRequest::Terminate,
            HelperRequest::Tree,
        ];
        let tags = [
            "hello",
            "spawn",
            "input",
            "resize",
            "signal",
            "terminate",
            "tree",
        ];

        for (request, tag) in requests.into_iter().zip(tags) {
            let json = serde_json::to_string(&request).expect("请求序列化失败");
            let value: JsonValue = serde_json::from_str(&json).expect("JSON 解析失败");
            assert_eq!(value["method"], tag);

            let back: HelperRequest = serde_json::from_str(&json).expect("请求反序列化失败");
            assert_eq!(request, back);
        }
    }

    /// 事件必须使用 `type` 标签，导出字段为 snake_case。
    #[test]
    fn events_round_trip_and_use_type_tag() {
        let events = vec![
            HelperEvent::HelloOk {
                proto: "termx-helper/1".to_string(),
                capabilities: HELPER_CAPABILITIES
                    .iter()
                    .map(|cap| cap.to_string())
                    .collect(),
            },
            HelperEvent::Started {
                pid: 123,
                pgid: 123,
            },
            HelperEvent::Output {
                data: "aGVsbG8=".to_string(),
                stream: Stream::Pty,
            },
            HelperEvent::Exited { code: 4 },
            HelperEvent::Signaled {
                signal: "SIGTERM".to_string(),
            },
            HelperEvent::Tree {
                processes: vec![crate::ProcNode {
                    pid: 123,
                    ppid: 100,
                    pgid: 123,
                    cmd: "sh".to_string(),
                }],
            },
            HelperEvent::Error {
                code: ErrorCode::SpawnFailed,
                message: "启动失败".to_string(),
            },
        ];
        let tags = [
            "hello_ok", "started", "output", "exited", "signaled", "tree", "error",
        ];

        for (event, tag) in events.into_iter().zip(tags) {
            let json = serde_json::to_string(&event).expect("事件序列化失败");
            let value: JsonValue = serde_json::from_str(&json).expect("JSON 解析失败");
            assert_eq!(value["type"], tag);

            let back: HelperEvent = serde_json::from_str(&json).expect("事件反序列化失败");
            assert_eq!(event, back);
        }
    }

    /// 缺省字段应被省略，输出流复用主协议的 `pty` 取值。
    #[test]
    fn optional_fields_are_omitted() {
        let request = HelperRequest::Spawn {
            cmd: "sh".to_string(),
            args: Vec::new(),
            cwd: None,
            env: None,
            cols: None,
            rows: None,
        };
        let value: JsonValue =
            serde_json::from_str(&serde_json::to_string(&request).expect("序列化失败"))
                .expect("JSON 解析失败");
        for key in ["cwd", "env", "cols", "rows"] {
            assert!(value.get(key).is_none(), "{key} 不应出现");
        }

        let event = HelperEvent::Output {
            data: "aGk=".to_string(),
            stream: Stream::Pty,
        };
        let value: JsonValue =
            serde_json::from_str(&serde_json::to_string(&event).expect("序列化失败"))
                .expect("JSON 解析失败");
        assert_eq!(value["stream"], "pty");
    }
}
