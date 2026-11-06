//! 服务端 `known_hosts` 存储：host:port + 算法 + SHA256 指纹。
//!
//! 设计约束（对应 `.tmp/requirements/20261106-known-hosts.md`）：
//! - 路径由 service 配置目录注入，不读写用户 `~/.ssh/known_hosts`；
//! - 只记录主机密钥指纹，不保存密码/私钥；
//! - 写入使用跨进程文件锁 + 同目录临时文件 rename，避免并发首次信任互相覆盖；
//! - 目标行损坏时返回 [`KnownHostsError::Corrupt`]，绝不降级成“未知可信任”；
//! - 已记录指纹不一致时返回 [`KnownHostsError::KeyChanged`]，不自动覆盖。

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use thiserror::Error;

/// 文件头的稳定标识；解析时按注释跳过。
const FILE_HEADER: &str = "# termx known_hosts v1";
/// SHA256 指纹前缀（OpenSSH 风格）。
const SHA256_PREFIX: &str = "SHA256:";

/// 同进程内串行化 known_hosts 写入，避免同进程多线程在文件锁上的平台差异。
static LOCAL_WRITE_LOCK: Mutex<()> = Mutex::new(());
/// 临时文件名序号；与 pid/时间一起保证唯一。
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// 主机标识：host + port。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostKeyTarget {
    /// 主机名或 IP。
    pub host: String,
    /// 端口。
    pub port: u16,
}

impl HostKeyTarget {
    /// 构造目标；host 只做去空白与去方括号处理。
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let host = normalize_host(&host.into());
        Self { host, port }
    }

    /// 存储格式里的 `host:port`；IPv6 使用 `[addr]:port`。
    pub fn record(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// 单条主机密钥信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostKeyInfo {
    /// 密钥算法（如 `ssh-ed25519`）。
    pub algorithm: String,
    /// SHA256 指纹（含 `SHA256:` 前缀）。
    pub fingerprint: String,
}

impl HostKeyInfo {
    /// 构造主机密钥信息；调用方应传入 russh 已经格式化好的值。
    pub fn new(algorithm: impl Into<String>, fingerprint: impl Into<String>) -> Self {
        Self {
            algorithm: algorithm.into(),
            fingerprint: fingerprint.into(),
        }
    }
}

/// 查询结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnownHostVerdict {
    /// 算法与指纹都匹配。
    Match,
    /// 目标没有任何可解析记录。
    Unknown,
    /// 目标有记录，但指纹/算法不同。
    Changed {
        /// 已记录的旧密钥信息。
        recorded: HostKeyInfo,
    },
}

/// known_hosts 读写错误。
#[derive(Debug, Error)]
pub enum KnownHostsError {
    /// 文件读写失败。
    #[error("known_hosts 读写失败: {0}")]
    Io(String),
    /// 目标行损坏；不得据此建立信任。
    #[error("known_hosts 记录损坏: {0}")]
    Corrupt(String),
    /// 加锁失败。
    #[error("known_hosts 加锁失败: {0}")]
    Lock(String),
    /// 记录与当前密钥不一致；可能被中间人替换。
    #[error("known_hosts 记录与当前主机密钥不一致（可能中间人）")]
    KeyChanged {
        /// 已记录的旧密钥信息。
        recorded: HostKeyInfo,
    },
}

/// 服务端 known_hosts 文件。
#[derive(Debug, Clone)]
pub struct KnownHosts {
    /// 文件路径。
    path: PathBuf,
}

impl KnownHosts {
    /// 绑定一个 known_hosts 路径；文件不存在视为空库。
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// 文件路径（测试与诊断用）。
    #[cfg(test)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 查询目标当前密钥是否可信。
    pub fn check(
        &self,
        target: &HostKeyTarget,
        info: &HostKeyInfo,
    ) -> Result<KnownHostVerdict, KnownHostsError> {
        let content = match fs::read_to_string(&self.path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(KnownHostVerdict::Unknown);
            }
            Err(error) => return Err(KnownHostsError::Io(error.to_string())),
        };
        parse_verdict(&content, target, info)
    }

    /// 首次信任：在锁内重新检查后追加写入；已存在同指纹时幂等返回。
    ///
    /// # 错误
    /// - 目标已有不同密钥：[`KnownHostsError::KeyChanged`]（不覆盖）；
    /// - 目标行损坏：[`KnownHostsError::Corrupt`]（不写入）；
    /// - 加锁或写入失败：[`KnownHostsError::Lock`] / [`KnownHostsError::Io`]。
    pub fn trust(&self, target: &HostKeyTarget, info: &HostKeyInfo) -> Result<(), KnownHostsError> {
        validate_info(info)?;
        let _local = LOCAL_WRITE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| KnownHostsError::Io(error.to_string()))?;
        }

        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.lock_path())
            .map_err(|error| KnownHostsError::Lock(error.to_string()))?;
        lock.lock_exclusive()
            .map_err(|error| KnownHostsError::Lock(error.to_string()))?;

        let result = self.trust_locked(target, info);
        let _ = FileExt::unlock(&lock);
        result
    }

    /// 持锁后的读-查-写；必须由 [`Self::trust`] 调用。
    fn trust_locked(
        &self,
        target: &HostKeyTarget,
        info: &HostKeyInfo,
    ) -> Result<(), KnownHostsError> {
        let content = match fs::read_to_string(&self.path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(KnownHostsError::Io(error.to_string())),
        };
        match parse_verdict(&content, target, info)? {
            KnownHostVerdict::Match => return Ok(()),
            KnownHostVerdict::Changed { recorded } => {
                return Err(KnownHostsError::KeyChanged { recorded });
            }
            KnownHostVerdict::Unknown => {}
        }

        let mut next = content;
        if next.is_empty() {
            next.push_str(FILE_HEADER);
            next.push('\n');
        } else if !next.ends_with('\n') {
            next.push('\n');
        }
        next.push_str(&format!(
            "{} {} {}\n",
            target.record(),
            info.algorithm,
            info.fingerprint
        ));
        self.write_atomic(&next)
    }

    /// 同目录临时文件 + rename，避免半写文件被后续连接读到。
    fn write_atomic(&self, content: &str) -> Result<(), KnownHostsError> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let temp = parent.join(temp_file_name());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp)
            .map_err(|error| KnownHostsError::Io(error.to_string()))?;
        let write_result = file
            .write_all(content.as_bytes())
            .and_then(|()| file.sync_all());
        drop(file);
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temp);
            return Err(KnownHostsError::Io(error.to_string()));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))
                .map_err(|error| KnownHostsError::Io(error.to_string()))?;
        }
        if let Err(error) = fs::rename(&temp, &self.path) {
            let _ = fs::remove_file(&temp);
            return Err(KnownHostsError::Io(error.to_string()));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&self.path, fs::Permissions::from_mode(0o600))
                .map_err(|error| KnownHostsError::Io(error.to_string()))?;
        }
        Ok(())
    }

    /// 锁文件路径；与 known_hosts 同目录。
    fn lock_path(&self) -> PathBuf {
        let name = self
            .path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "known_hosts".to_string());
        self.path.with_file_name(format!("{name}.lock"))
    }
}

/// 解析文件并给出查询结论。
fn parse_verdict(
    content: &str,
    target: &HostKeyTarget,
    info: &HostKeyInfo,
) -> Result<KnownHostVerdict, KnownHostsError> {
    validate_info(info)?;
    let expected_target = target.record();
    let mut changed = None;
    let mut corrupt = None;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(recorded_target) = parts.next() else {
            continue;
        };
        if normalize_record_target(recorded_target) != expected_target {
            continue;
        }
        let Some(algorithm) = parts.next() else {
            corrupt = Some(trimmed.to_string());
            continue;
        };
        let Some(fingerprint) = parts.next() else {
            corrupt = Some(trimmed.to_string());
            continue;
        };
        if !is_valid_fingerprint(fingerprint) {
            corrupt = Some(trimmed.to_string());
            continue;
        }
        if algorithm == info.algorithm && fingerprint == info.fingerprint {
            return Ok(KnownHostVerdict::Match);
        }
        changed.get_or_insert_with(|| HostKeyInfo::new(algorithm, fingerprint));
    }

    if let Some(recorded) = changed {
        return Ok(KnownHostVerdict::Changed { recorded });
    }
    if let Some(line) = corrupt {
        return Err(KnownHostsError::Corrupt(line));
    }
    Ok(KnownHostVerdict::Unknown)
}

/// 校验主机密钥信息，避免写入无法再匹配的空值。
fn validate_info(info: &HostKeyInfo) -> Result<(), KnownHostsError> {
    if info.algorithm.trim().is_empty() {
        return Err(KnownHostsError::Corrupt("算法为空".to_string()));
    }
    if !is_valid_fingerprint(&info.fingerprint) {
        return Err(KnownHostsError::Corrupt(format!(
            "指纹格式非法: {}",
            info.fingerprint
        )));
    }
    Ok(())
}

/// 只接受 `SHA256:` 前缀且后缀非空。
fn is_valid_fingerprint(value: &str) -> bool {
    value
        .strip_prefix(SHA256_PREFIX)
        .is_some_and(|rest| !rest.trim().is_empty())
}

/// 归一化查询目标：小写、去方括号。
fn normalize_host(host: &str) -> String {
    host.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase()
}

/// 把文件里的目标归一化后再比较；非法目标保留原值（不会误匹配）。
fn normalize_record_target(value: &str) -> String {
    if let Some((host, port)) = split_target(value) {
        if let Ok(port) = port.parse::<u16>() {
            return HostKeyTarget::new(host, port).record();
        }
    }
    value.to_ascii_lowercase()
}

/// 拆分 `host:port` 或 `[v6]:port`。
fn split_target(value: &str) -> Option<(&str, &str)> {
    if let Some(rest) = value.strip_prefix('[') {
        let (host, port) = rest.split_once("]:")?;
        return Some((host, port));
    }
    value.rsplit_once(':')
}

/// 临时文件名：pid + 时间 + 序号 + 线程 id。
fn temp_file_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        ".known_hosts.tmp.{}.{nanos}.{counter}.{:?}",
        std::process::id(),
        std::thread::current().id()
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Barrier},
        thread,
    };

    use tempfile::tempdir;

    use super::*;

    /// 测试用目标与密钥。
    fn target(host: &str, port: u16) -> HostKeyTarget {
        HostKeyTarget::new(host, port)
    }

    /// 测试用指纹。
    fn info(fingerprint: &str) -> HostKeyInfo {
        HostKeyInfo::new("ssh-ed25519", format!("SHA256:{fingerprint}"))
    }

    #[test]
    fn missing_file_is_unknown_and_first_trust_matches() {
        let dir = tempdir().expect("临时目录");
        let store = KnownHosts::new(dir.path().join("known_hosts"));
        let host = target("example.com", 2222);
        let key = info("abc123");

        assert_eq!(
            store.check(&host, &key).expect("查询"),
            KnownHostVerdict::Unknown
        );
        store.trust(&host, &key).expect("首次信任");
        assert_eq!(
            store.check(&host, &key).expect("查询"),
            KnownHostVerdict::Match
        );

        let text = fs::read_to_string(store.path()).expect("读取");
        assert!(text.contains("example.com:2222 ssh-ed25519 SHA256:abc123"));
    }

    #[test]
    fn changed_fingerprint_is_hard_failure_and_never_overwritten() {
        let dir = tempdir().expect("临时目录");
        let store = KnownHosts::new(dir.path().join("known_hosts"));
        let host = target("example.com", 22);
        store.trust(&host, &info("old")).expect("首次信任");

        let error = store
            .trust(&host, &info("new"))
            .expect_err("不同指纹必须拒绝");
        assert!(matches!(error, KnownHostsError::KeyChanged { .. }));

        assert_eq!(
            store.check(&host, &info("old")).expect("查询"),
            KnownHostVerdict::Match
        );
        let text = fs::read_to_string(store.path()).expect("读取");
        assert!(!text.contains("SHA256:new"), "不得覆盖旧记录: {text}");
    }

    #[test]
    fn corrupt_target_line_is_never_trusted() {
        let dir = tempdir().expect("临时目录");
        let path = dir.path().join("known_hosts");
        fs::write(&path, "example.com:22 ssh-ed25519 not-a-fingerprint\n").expect("写入");
        let store = KnownHosts::new(&path);
        let host = target("example.com", 22);

        let error = store
            .check(&host, &info("abc"))
            .expect_err("损坏记录应失败");
        assert!(matches!(error, KnownHostsError::Corrupt(_)));
        assert!(
            store.trust(&host, &info("abc")).is_err(),
            "损坏文件不得被追加信任"
        );
    }

    #[test]
    fn unrelated_corruption_does_not_block_new_hosts() {
        let dir = tempdir().expect("临时目录");
        let path = dir.path().join("known_hosts");
        fs::write(&path, "broken-line\n").expect("写入");
        let store = KnownHosts::new(&path);
        let host = target("new.example", 22);

        assert_eq!(
            store.check(&host, &info("abc")).expect("无关损坏可忽略"),
            KnownHostVerdict::Unknown
        );
        store.trust(&host, &info("abc")).expect("可写入新主机");
    }

    #[test]
    fn concurrent_first_trust_keeps_every_entry() {
        let dir = tempdir().expect("临时目录");
        let path = dir.path().join("known_hosts");
        let workers = 8;
        let barrier = Arc::new(Barrier::new(workers));
        let mut handles = Vec::new();
        for index in 0..workers {
            let store = KnownHosts::new(path.clone());
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let host = HostKeyTarget::new(format!("host{index}.example"), 22);
                let key = HostKeyInfo::new("ssh-ed25519", format!("SHA256:fp{index}"));
                barrier.wait();
                store.trust(&host, &key).expect("并发首次信任");
            }));
        }
        for handle in handles {
            handle.join().expect("线程结束");
        }

        let store = KnownHosts::new(&path);
        for index in 0..workers {
            let host = HostKeyTarget::new(format!("host{index}.example"), 22);
            let key = HostKeyInfo::new("ssh-ed25519", format!("SHA256:fp{index}"));
            assert_eq!(
                store.check(&host, &key).expect("查询"),
                KnownHostVerdict::Match,
                "并发写入不得丢失 host{index}"
            );
        }
        let text = fs::read_to_string(&path).expect("读取");
        assert_eq!(
            text.lines()
                .filter(|line| !line.starts_with('#') && line.contains(".example"))
                .count(),
            workers
        );
    }

    #[test]
    fn ipv6_targets_use_bracketed_form() {
        let dir = tempdir().expect("临时目录");
        let store = KnownHosts::new(dir.path().join("known_hosts"));
        let host = target("::1", 2222);
        store.trust(&host, &info("v6")).expect("写入 IPv6");

        assert_eq!(
            store.check(&host, &info("v6")).expect("查询"),
            KnownHostVerdict::Match
        );
        let text = fs::read_to_string(store.path()).expect("读取");
        assert!(text.contains("[::1]:2222 ssh-ed25519 SHA256:v6"));
    }

    #[cfg(unix)]
    #[test]
    fn written_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().expect("临时目录");
        let store = KnownHosts::new(dir.path().join("known_hosts"));
        store
            .trust(&target("example.com", 22), &info("abc"))
            .expect("写入");
        let mode = fs::metadata(store.path())
            .expect("元数据")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}
