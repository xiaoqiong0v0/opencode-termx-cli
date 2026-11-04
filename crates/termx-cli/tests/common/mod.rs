//! termx-cli 集成测试脚手架：起真实 service（优先二进制，缺失时进程内）并跑 CLI。

#![allow(dead_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use termx_service::{Service, ServiceConfig, ServiceError};
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};

/// 单个 CLI 子进程的最长等待时间。
pub const CLI_TIMEOUT: Duration = Duration::from_secs(30);
/// service 写出 port/token 的最长等待时间。
pub const READY_TIMEOUT: Duration = Duration::from_secs(20);
/// 关闭 service 的最长等待时间。
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
/// service 二进制的文件名。
const SERVICE_BIN_NAME: &str = if cfg!(windows) {
    "termx-service.exe"
} else {
    "termx-service"
};

/// CLI 子进程输出。
pub struct CliOutput {
    /// 退出码；被信号终止时为 `None`。
    pub code: Option<i32>,
    /// 原始 stdout。
    pub stdout: Vec<u8>,
    /// 原始 stderr。
    pub stderr: Vec<u8>,
}

impl CliOutput {
    /// stdout 的 UTF-8 显示（非 UTF-8 用替换字符）。
    pub fn stdout_text(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }

    /// stderr 的 UTF-8 显示（非 UTF-8 用替换字符）。
    pub fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.stderr).into_owned()
    }

    /// 解析 stdout 的单行 JSON。
    pub fn stdout_json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.stdout).expect("stdout 不是合法 JSON")
    }
}

/// 测试服务守卫。
pub struct TestService {
    /// 临时配置目录；Drop 时删除。
    _temp: tempfile::TempDir,
    /// 传给 CLI 的配置目录。
    pub config_dir: PathBuf,
    /// 服务 token。
    pub token: String,
    backend: Backend,
}

/// 服务承载方式。
enum Backend {
    /// 外部 service 二进制。
    Process(Option<Child>),
    /// 进程内 service（跨包拿不到 `CARGO_BIN_EXE_termx-service` 时使用）。
    InProcess {
        shutdown: Option<oneshot::Sender<()>>,
        handle: Option<JoinHandle<Result<(), ServiceError>>>,
    },
}

impl TestService {
    /// 启动服务并等待 port/token 就绪。
    pub async fn start() -> Self {
        let temp = tempfile::tempdir().expect("创建临时配置目录失败");
        let config_dir = temp.path().to_path_buf();

        if let Some(bin) = service_bin() {
            if let Ok(mut child) = spawn_service(&bin, &config_dir) {
                return match wait_for_ready(&config_dir, Some(&mut child)).await {
                    Ok(token) => Self {
                        _temp: temp,
                        config_dir,
                        token,
                        backend: Backend::Process(Some(child)),
                    },
                    Err(error) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("service 未就绪: {error}");
                    }
                };
            }
        }

        start_in_process(temp, config_dir).await
    }

    /// 读取 service 写出的端口并组装本机 endpoint。
    pub fn endpoint(&self) -> String {
        let text = fs::read_to_string(self.config_dir.join("port")).expect("读取端口文件失败");
        format!("ws://127.0.0.1:{}", text.trim())
    }

    /// 优雅停机并等待后台任务退出。
    pub async fn shutdown(&mut self) {
        match &mut self.backend {
            Backend::Process(slot) => {
                if let Some(mut child) = slot.take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
            Backend::InProcess { shutdown, handle } => {
                if let Some(sender) = shutdown.take() {
                    let _ = sender.send(());
                }
                if let Some(handle) = handle.take() {
                    let _ = timeout(SHUTDOWN_TIMEOUT, handle).await;
                }
            }
        }
    }
}

impl Drop for TestService {
    fn drop(&mut self) {
        match &mut self.backend {
            Backend::Process(slot) => {
                if let Some(mut child) = slot.take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
            Backend::InProcess { shutdown, handle } => {
                if let Some(sender) = shutdown.take() {
                    let _ = sender.send(());
                }
                if let Some(handle) = handle.take() {
                    handle.abort();
                }
            }
        }
    }
}

/// 运行 CLI 子进程：显式传入 `--config-dir <临时目录>`，不依赖环境变量。
pub async fn run_cli(service: &TestService, args: &[&str]) -> CliOutput {
    run_cli_with_config(Some(&service.config_dir), args).await
}

/// 运行 CLI 子进程且不注入任何配置目录/endpoint，用于验证显式配置要求。
pub async fn run_cli_bare(args: &[&str]) -> CliOutput {
    run_cli_with_config(None, args).await
}

/// 运行 CLI 子进程；带超时并保证超时/失败时杀掉子进程。
async fn run_cli_with_config(config_dir: Option<&Path>, args: &[&str]) -> CliOutput {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_termx-cli"));
    if let Some(dir) = config_dir {
        command.arg("--config-dir").arg(dir);
    }
    command
        .args(args)
        .env_remove("TERMX_CONFIG_DIR")
        .env_remove("TERMX_TOKEN")
        .env_remove("TERMX_ROOT_SESSION_ID")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let child = command.spawn().expect("启动 termx-cli 失败");
    let output = timeout(CLI_TIMEOUT, child.wait_with_output())
        .await
        .expect("termx-cli 执行超时")
        .expect("等待 termx-cli 失败");
    CliOutput {
        code: output.status.code(),
        stdout: output.stdout,
        stderr: output.stderr,
    }
}

/// 找到 service 二进制：优先编译期 env，其次与 CLI 可执行文件同目录。
fn service_bin() -> Option<PathBuf> {
    if let Some(path) = option_env!("CARGO_BIN_EXE_termx-service") {
        return Some(PathBuf::from(path));
    }
    let current = std::env::current_exe().ok()?;
    let target_dir = current.parent()?.parent()?;
    let candidate = target_dir.join(SERVICE_BIN_NAME);
    candidate.is_file().then_some(candidate)
}

/// 启动外部 service 二进制。
fn spawn_service(bin: &Path, config_dir: &Path) -> std::io::Result<Child> {
    Command::new(bin)
        .arg("--token-file")
        .arg(config_dir.join("token"))
        .env("TERMX_CONFIG_DIR", config_dir)
        .env_remove("TERMX_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

/// 在测试进程内启动 service。
async fn start_in_process(temp: tempfile::TempDir, config_dir: PathBuf) -> TestService {
    let config = ServiceConfig::new(config_dir.clone()).with_token_file(config_dir.join("token"));
    let service = Service::bind(config)
        .await
        .expect("进程内 service 绑定失败");
    let token = read_token(&config_dir).expect("读取 token 失败");
    let (shutdown, receiver) = oneshot::channel();
    let handle = tokio::spawn(service.run_until(async move {
        let _ = receiver.await;
    }));
    TestService {
        _temp: temp,
        config_dir,
        token,
        backend: Backend::InProcess {
            shutdown: Some(shutdown),
            handle: Some(handle),
        },
    }
}

/// 等待 port/token 文件出现并返回 token。
async fn wait_for_ready(
    config_dir: &Path,
    mut child: Option<&mut Child>,
) -> Result<String, String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if let Some(child) = child.as_deref_mut() {
            if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
                return Err(format!("service 提前退出: {status}"));
            }
        }
        if port_ready(config_dir) {
            if let Ok(token) = read_token(config_dir) {
                return Ok(token);
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("等待 {} 超时", config_dir.join("port").display()));
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// 端口文件存在且内容为有效非零端口。
fn port_ready(config_dir: &Path) -> bool {
    let Ok(text) = fs::read_to_string(config_dir.join("port")) else {
        return false;
    };
    text.trim().parse::<u16>().is_ok_and(|port| port > 0)
}

/// 读取并 trim token 文件。
fn read_token(config_dir: &Path) -> std::io::Result<String> {
    let text = fs::read_to_string(config_dir.join("token"))?;
    let token = text.trim();
    if token.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "token 文件为空",
        ));
    }
    Ok(token.to_string())
}
