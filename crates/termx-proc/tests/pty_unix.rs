//! Unix/Linux PTY 集成测试：真实进程 + 事件流。
//!
//! 所有用例都带超时，结束时确保会话被丢弃（Drop 会终止进程组并关闭主端）。

#![cfg(unix)]

use std::time::Duration;

use termx_proc::{ProcError, ProcEvent, PtyConfig, PtySession};
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, timeout};

/// 单次等待的上限，避免测试挂死。
const TEST_TIMEOUT: Duration = Duration::from_secs(20);

/// 终态事件。
#[derive(Debug, PartialEq, Eq)]
enum Terminal {
    /// 正常退出码。
    Exited(i32),
    /// 终止信号编号。
    Signaled(i32),
}

/// 构造 `/bin/sh -c <script>` 配置。
fn sh_config(script: &str) -> PtyConfig {
    PtyConfig {
        program: "/bin/sh".to_string(),
        args: vec!["-c".to_string(), script.to_string()],
        ..PtyConfig::default()
    }
}

/// 构造交互式 `/bin/sh` 配置。
fn interactive_sh_config() -> PtyConfig {
    PtyConfig {
        program: "/bin/sh".to_string(),
        ..PtyConfig::default()
    }
}

/// 收集输出直到终态事件。
async fn wait_terminal(rx: &mut Receiver<ProcEvent>) -> (Vec<u8>, Terminal) {
    let mut output = Vec::new();
    let terminal = timeout(TEST_TIMEOUT, async {
        loop {
            match rx.recv().await {
                Some(ProcEvent::Output(chunk)) => output.extend_from_slice(&chunk),
                Some(ProcEvent::Exited(code)) => return Terminal::Exited(code),
                Some(ProcEvent::Signaled(signal)) => return Terminal::Signaled(signal),
                None => panic!("事件通道在收到终态事件前关闭"),
            }
        }
    })
    .await
    .expect("等待进程退出超时");
    (output, terminal)
}

/// 扫描 `/proc`，返回指定进程组内的非僵尸进程。
fn group_members(pgid: u32) -> Vec<u32> {
    let mut members = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return members;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(raw) = std::fs::read(format!("/proc/{pid}/stat")) else {
            continue;
        };
        let Some(close) = raw.iter().rposition(|&byte| byte == b')') else {
            continue;
        };
        let Ok(rest) = std::str::from_utf8(&raw[close + 1..]) else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        let state = fields.next().unwrap_or_default();
        let _ppid = fields.next();
        let Some(pgrp) = fields.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        if pgrp == pgid && state != "Z" {
            members.push(pid);
        }
    }
    members
}

#[tokio::test]
async fn echo_and_exit() {
    let (session, mut rx) =
        PtySession::spawn(sh_config("echo hello_termx; exit 7")).expect("启动 /bin/sh 失败");

    assert!(session.pid() > 0, "pid 应为正数");
    assert_eq!(session.pgid(), session.pid(), "setsid 后 pgid 应等于根 pid");

    let (output, terminal) = wait_terminal(&mut rx).await;
    let text = String::from_utf8_lossy(&output);
    assert!(text.contains("hello_termx"), "输出缺少标记: {text:?}");
    assert_eq!(terminal, Terminal::Exited(7), "退出码应为 7");
}

#[tokio::test]
async fn terminate_no_orphan() {
    let (mut session, _rx) = PtySession::spawn(sh_config("sleep 30")).expect("启动 sleep 失败");
    let pgid = session.pgid();

    // 先确认进程组内已有成员。
    timeout(Duration::from_secs(5), async {
        loop {
            if !group_members(pgid).is_empty() {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("等待子进程出现超时");

    session.terminate().expect("terminate 失败");

    // 终止后轮询：进程组成员应全部消失。
    timeout(Duration::from_secs(10), async {
        loop {
            if group_members(pgid).is_empty() {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("terminate 后仍观察到存活进程");
}

#[tokio::test]
async fn tree_finds_session_descendants_after_root_exit() {
    // 根 shell 退出后，后台 sleep 仍留在同一会话（session ID = 根 PID）。
    // 忽略 SIGHUP，避免会话首进程退出时控制终端把后台作业一并带走。
    let (mut session, mut rx) =
        PtySession::spawn(sh_config("trap '' HUP; sleep 30 & exit 0")).expect("启动 /bin/sh 失败");
    let (_, terminal) = wait_terminal(&mut rx).await;
    assert_eq!(terminal, Terminal::Exited(0), "根 shell 应正常退出");

    let tree = timeout(Duration::from_secs(5), async {
        loop {
            let nodes = session.tree().expect("采集进程树失败");
            if nodes.iter().any(|node| node.pid != session.pid()) {
                return nodes;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("根进程退出后应仍能看到同会话后代");
    assert!(
        tree.iter().all(|node| node.pid != session.pid()),
        "根进程已退出，不应再出现在树中: {tree:?}"
    );

    session.terminate().expect("terminate 失败");
}

#[tokio::test]
async fn resize_ok() {
    let (mut session, _rx) = PtySession::spawn(sh_config("sleep 30")).expect("启动 sleep 失败");

    session.resize(100, 40).expect("resize 应成功");
    assert!(
        session.foreground_pgrp().expect("读取前台进程组失败") > 0,
        "前台进程组应为正数"
    );
    session.terminate().expect("terminate 失败");
}

#[tokio::test]
async fn write_stdin() {
    let (mut session, mut rx) =
        PtySession::spawn(interactive_sh_config()).expect("启动交互式 /bin/sh 失败");

    session
        .write(b"printf 'TERMX_%s\\n' OUT\n")
        .expect("写入 printf 命令失败");
    session.write(b"exit 0\n").expect("写入 exit 命令失败");

    let (output, terminal) = wait_terminal(&mut rx).await;
    let text = String::from_utf8_lossy(&output);
    assert!(text.contains("TERMX_OUT"), "输出缺少标记: {text:?}");
    assert_eq!(terminal, Terminal::Exited(0), "exit 0 的退出码应为 0");
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

#[tokio::test]
async fn signaled() {
    let (session, mut rx) =
        PtySession::spawn(sh_config("kill -TERM $$")).expect("启动 /bin/sh 失败");

    let (_output, terminal) = wait_terminal(&mut rx).await;
    assert_eq!(terminal, Terminal::Signaled(15), "应报告 SIGTERM 终止");
    assert!(session.pid() > 0, "pid 应为正数");
}
