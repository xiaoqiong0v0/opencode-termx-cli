//! Windows ConPTY 集成测试：真实进程 + 事件流。
//!
//! 所有用例都带超时，结束时确保会话被丢弃（Drop 会终止 Job 并关闭伪控制台）。

#![cfg(windows)]

use std::time::Duration;

use termx_proc::{ProcError, ProcEvent, PtyConfig, PtySession};
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, timeout};

/// 单次等待的上限，避免测试挂死。
const TEST_TIMEOUT: Duration = Duration::from_secs(20);

/// 构造 `cmd.exe` 配置。
fn cmd_config(args: &[&str]) -> PtyConfig {
    PtyConfig {
        program: "cmd.exe".to_string(),
        args: args.iter().map(|arg| (*arg).to_string()).collect(),
        ..PtyConfig::default()
    }
}

/// 收集输出直到 `Exited`，并返回输出字节与退出码。
async fn wait_exit(rx: &mut Receiver<ProcEvent>) -> (Vec<u8>, i32) {
    let mut output = Vec::new();
    let exit_code = timeout(TEST_TIMEOUT, async {
        loop {
            match rx.recv().await {
                Some(ProcEvent::Output(chunk)) => output.extend_from_slice(&chunk),
                Some(ProcEvent::Exited(code)) => return code,
                Some(ProcEvent::Signaled(signal)) => {
                    panic!("Windows 不应收到 Signaled({signal})")
                }
                None => panic!("事件通道在收到 Exited 前关闭"),
            }
        }
    })
    .await
    .expect("等待进程退出超时");
    (output, exit_code)
}

#[tokio::test]
async fn echo_and_exit() {
    let (session, mut rx) =
        PtySession::spawn(cmd_config(&["/C", "echo hello_termx & exit 7"])).expect("启动 cmd 失败");

    assert!(session.pid() > 0, "pid 应为正数");
    assert_eq!(
        session.pgid(),
        session.pid(),
        "Windows 上 pgid 应等于根 pid"
    );

    let (output, exit_code) = wait_exit(&mut rx).await;
    let text = String::from_utf8_lossy(&output);
    assert!(text.contains("hello_termx"), "输出缺少标记: {text:?}");
    assert_eq!(exit_code, 7, "退出码应为 7");
}

/// 按 pid 查询进程表；`tree()` 为空不代表进程已消失，必须直接核对 pid。
fn pid_alive(pid: u32) -> bool {
    let filter = format!("PID eq {pid}");
    let Ok(output) = std::process::Command::new("tasklist")
        .args(["/FI", &filter, "/NH", "/FO", "CSV"])
        .output()
    else {
        return false;
    };
    String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
}

#[tokio::test]
async fn terminate_no_orphan() {
    let (mut session, _rx) = PtySession::spawn(cmd_config(&["/C", "ping -n 30 127.0.0.1 > nul"]))
        .expect("启动 ping 失败");

    // 先确认 cmd 已派生出 ping 子进程，并记录整棵树的 pid。
    let pids = timeout(Duration::from_secs(5), async {
        loop {
            let nodes = session.tree().expect("采集进程树失败");
            if nodes.len() > 1 {
                let mut pids: Vec<u32> = nodes.iter().map(|node| node.pid).collect();
                pids.sort_unstable();
                pids.dedup();
                return pids;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("等待 ping 子进程出现超时");

    session.terminate().expect("terminate 失败");

    // 终止后按 pid 查真实进程表，而不是只看 `tree()` 是否为空。
    timeout(Duration::from_secs(10), async {
        loop {
            let alive: Vec<u32> = pids.iter().copied().filter(|pid| pid_alive(*pid)).collect();
            if alive.is_empty() {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("terminate 后仍观察到存活进程");
}

#[tokio::test]
async fn resize_ok() {
    let (mut session, _rx) = PtySession::spawn(cmd_config(&["/C", "ping -n 30 127.0.0.1 > nul"]))
        .expect("启动 ping 失败");

    session.resize(100, 40).expect("resize 应成功");
    session.terminate().expect("terminate 失败");
}

#[tokio::test]
async fn write_stdin() {
    let (mut session, mut rx) = PtySession::spawn(cmd_config(&[])).expect("启动交互 cmd 失败");

    session
        .write(b"echo termx_write_ok\r\n")
        .expect("写入 echo 命令失败");
    session.write(b"exit 3\r\n").expect("写入 exit 命令失败");

    let (output, exit_code) = wait_exit(&mut rx).await;
    let text = String::from_utf8_lossy(&output);
    assert!(text.contains("termx_write_ok"), "输出缺少标记: {text:?}");
    assert_eq!(exit_code, 3, "exit 3 的退出码应为 3");
}

#[tokio::test]
async fn invalid_program() {
    let cfg = PtyConfig {
        program: "termx_missing_program_9f3a".to_string(),
        ..PtyConfig::default()
    };

    match PtySession::spawn(cfg) {
        Err(ProcError::SpawnFailed { message }) => {
            assert!(!message.is_empty(), "失败原因不应为空");
        }
        Err(other) => panic!("期望 SpawnFailed，实际为 {other:?}"),
        Ok(_) => panic!("不存在的程序不应创建成功"),
    }
}
