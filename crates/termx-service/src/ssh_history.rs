//! SSH 连接历史存储。
//!
//! 设计约束（见 `docs/specs/CONFIG.md` / `PROTOCOL.md`）：
//! - 只保存 host/port/user/auth_type/use_count/last_used，**绝不**保存密码、
//!   私钥口令、私钥内容或私钥路径；
//! - 路径固定为 `<config>/ssh_history.json`；Unix 写入 `0600`；
//! - 同目录临时文件 + rename 原子替换，读取到损坏 JSON 时按空历史处理；
//! - 同进程互斥 + 跨进程文件锁，读-改-写串行化；
//! - 超限按 use_count 最小淘汰，count 相同时淘汰 last_used 最旧。

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write as _},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use termx_protocol::SshHistoryEntry;

/// 历史文件名。
pub const SSH_HISTORY_FILE_NAME: &str = "ssh_history.json";
/// 历史条数默认上限。
pub const DEFAULT_SSH_HISTORY_LIMIT: usize = 64;

/// 同进程内串行化读-改-写，避免同进程多线程在文件锁上的平台差异。
static LOCAL_LOCK: Mutex<()> = Mutex::new(());
/// 临时文件名序号；与 pid/时间一起保证唯一。
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// 文件根对象；保持可扩展，未知字段忽略。
#[derive(Debug, Default, Serialize, Deserialize)]
struct HistoryFile {
    /// 历史条目。
    #[serde(default)]
    entries: Vec<SshHistoryEntry>,
}

/// SSH 连接历史存储句柄。
///
/// 无内部可变状态：每次操作都在文件锁内从磁盘读取并原子写回，便于多进程共享。
#[derive(Debug, Clone)]
pub struct SshHistory {
    /// `ssh_history.json` 路径。
    path: PathBuf,
    /// 条数上限；`0` 表示禁用历史。
    limit: usize,
}

impl SshHistory {
    /// 绑定配置目录与条数上限。
    pub fn new(config_dir: &Path, limit: usize) -> Self {
        Self {
            path: config_dir.join(SSH_HISTORY_FILE_NAME),
            limit,
        }
    }

    /// 按 `last_used` 降序返回历史（最多 `limit` 条）。
    ///
    /// 文件缺失视为空；JSON 损坏时记录警告并返回空，不影响调用方。
    pub fn list(&self) -> io::Result<Vec<SshHistoryEntry>> {
        if self.limit == 0 {
            return Ok(Vec::new());
        }
        let _local = local_lock();
        let lock = self.open_lock()?;
        let result = self.load();
        let _ = FileExt::unlock(&lock);
        let mut entries = result?;
        entries.sort_by(|left, right| {
            right
                .last_used
                .cmp(&left.last_used)
                .then_with(|| right.use_count.cmp(&left.use_count))
        });
        entries.truncate(self.limit);
        Ok(entries)
    }

    /// 记录一次成功连接：同 host + port + user 去重、`use_count += 1`、
    /// 刷新 `last_used`，然后按 LFU + LRU 淘汰到上限。
    ///
    /// `limit == 0`、host/user 为空或 auth_type 非 `password`/`key` 时静默忽略。
    pub fn record(&self, host: &str, port: u16, user: &str, auth_type: &str) -> io::Result<()> {
        let now = termx_core::now_rfc3339();
        self.record_at(host, port, user, auth_type, &now)
    }

    /// 带显式时间的 [`Self::record`]，便于测试 LRU 淘汰顺序。
    fn record_at(
        &self,
        host: &str,
        port: u16,
        user: &str,
        auth_type: &str,
        last_used: &str,
    ) -> io::Result<()> {
        if self.limit == 0 {
            return Ok(());
        }
        let host = host.trim();
        let user = user.trim();
        let auth_type = auth_type.trim();
        if host.is_empty() || user.is_empty() || !is_known_auth_type(auth_type) {
            return Ok(());
        }
        let _local = local_lock();
        let lock = self.open_lock()?;
        let result = self.record_locked(host, port, user, auth_type, last_used);
        let _ = FileExt::unlock(&lock);
        result
    }

    /// 删除单条历史；返回是否真的删除。
    pub fn remove(&self, host: &str, port: u16, user: &str) -> io::Result<bool> {
        let host = host.trim();
        let user = user.trim();
        if host.is_empty() || user.is_empty() {
            return Ok(false);
        }
        let _local = local_lock();
        let lock = self.open_lock()?;
        let result = self.remove_locked(host, port, user);
        let _ = FileExt::unlock(&lock);
        result
    }

    /// 清空历史；返回删除条数。
    pub fn clear(&self) -> io::Result<usize> {
        let _local = local_lock();
        let lock = self.open_lock()?;
        let result = self.clear_locked();
        let _ = FileExt::unlock(&lock);
        result
    }

    /// 持锁后的记录逻辑。
    fn record_locked(
        &self,
        host: &str,
        port: u16,
        user: &str,
        auth_type: &str,
        last_used: &str,
    ) -> io::Result<()> {
        let mut entries = self.load()?;
        if let Some(entry) = entries
            .iter_mut()
            .find(|entry| entry.host == host && entry.port == port && entry.user == user)
        {
            entry.use_count = entry.use_count.saturating_add(1);
            entry.last_used = last_used.to_string();
            entry.auth_type = auth_type.to_string();
        } else {
            entries.push(SshHistoryEntry {
                host: host.to_string(),
                port,
                user: user.to_string(),
                auth_type: auth_type.to_string(),
                use_count: 1,
                last_used: last_used.to_string(),
            });
        }
        evict(&mut entries, self.limit);
        self.save(&entries)
    }

    /// 持锁后的删除逻辑。
    fn remove_locked(&self, host: &str, port: u16, user: &str) -> io::Result<bool> {
        let mut entries = self.load()?;
        let before = entries.len();
        entries.retain(|entry| !(entry.host == host && entry.port == port && entry.user == user));
        if entries.len() == before {
            return Ok(false);
        }
        self.save(&entries)?;
        Ok(true)
    }

    /// 持锁后的清空逻辑。
    fn clear_locked(&self) -> io::Result<usize> {
        let entries = self.load()?;
        if entries.is_empty() {
            return Ok(0);
        }
        let removed = entries.len();
        self.save(&[])?;
        Ok(removed)
    }

    /// 读取历史；缺失视为空，JSON 损坏时记录警告并返回空。
    fn load(&self) -> io::Result<Vec<SshHistoryEntry>> {
        match fs::read_to_string(&self.path) {
            Ok(content) => match serde_json::from_str::<HistoryFile>(&content) {
                Ok(file) => Ok(file.entries),
                Err(error) => {
                    tracing::warn!(
                        path = %self.path.display(),
                        %error,
                        "SSH 历史 JSON 损坏，按空历史处理"
                    );
                    Ok(Vec::new())
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    /// 同目录临时文件 + rename 原子写入；Unix 权限 `0600`。
    fn save(&self, entries: &[SshHistoryEntry]) -> io::Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let content = serde_json::to_vec_pretty(&HistoryFile {
            entries: entries.to_vec(),
        })
        .map_err(io::Error::other)?;

        let temp = parent.join(temp_file_name());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        let write_result = file.write_all(&content).and_then(|()| file.sync_all());
        drop(file);
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temp);
            return Err(error);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
        }
        if let Err(error) = fs::rename(&temp, &self.path) {
            let _ = fs::remove_file(&temp);
            return Err(error);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&self.path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    /// 打开并获取跨进程排他锁；父目录不存在时先创建。
    fn open_lock(&self) -> io::Result<File> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(self.lock_path())?;
        lock.lock_exclusive()?;
        Ok(lock)
    }

    /// 锁文件路径；与历史文件同目录。
    fn lock_path(&self) -> PathBuf {
        let name = self
            .path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| SSH_HISTORY_FILE_NAME.to_string());
        self.path.with_file_name(format!("{name}.lock"))
    }
}

/// 获取同进程互斥锁；中毒时继续使用内部值。
fn local_lock() -> std::sync::MutexGuard<'static, ()> {
    LOCAL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 是否为受支持的认证方式名。
fn is_known_auth_type(value: &str) -> bool {
    matches!(value, "password" | "key")
}

/// 按 LFU + LRU 淘汰到 `limit` 条。
fn evict(entries: &mut Vec<SshHistoryEntry>, limit: usize) {
    while entries.len() > limit {
        let victim = entries
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                left.use_count
                    .cmp(&right.use_count)
                    .then_with(|| left.last_used.cmp(&right.last_used))
            })
            .map(|(index, _)| index);
        match victim {
            Some(index) => {
                entries.remove(index);
            }
            None => break,
        }
    }
}

/// 临时文件名：pid + 时间 + 序号 + 线程 id。
fn temp_file_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        ".ssh_history.tmp.{}.{nanos}.{counter}.{:?}",
        std::process::id(),
        std::thread::current().id()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(dir: &tempfile::TempDir, limit: usize) -> SshHistory {
        SshHistory::new(dir.path(), limit)
    }

    fn read_file(dir: &tempfile::TempDir) -> serde_json::Value {
        let path = dir.path().join(SSH_HISTORY_FILE_NAME);
        serde_json::from_str(&fs::read_to_string(path).expect("读取历史文件失败"))
            .expect("历史文件 JSON 非法")
    }

    #[test]
    fn record_dedups_and_increments_use_count() {
        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, DEFAULT_SSH_HISTORY_LIMIT);
        history
            .record_at(
                "example.com",
                22,
                "alice",
                "password",
                "2026-11-09T10:00:00Z",
            )
            .expect("首次记录");
        history
            .record_at("example.com", 22, "alice", "key", "2026-11-09T10:05:00Z")
            .expect("重复记录");

        let entries = history.list().expect("读取历史");
        assert_eq!(entries.len(), 1, "host+port+user 应去重");
        assert_eq!(entries[0].use_count, 2);
        assert_eq!(entries[0].auth_type, "key", "认证方式应刷新为最近一次");
        assert_eq!(entries[0].last_used, "2026-11-09T10:05:00Z");
    }

    #[test]
    fn list_is_sorted_by_last_used_desc() {
        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, 4);
        history
            .record_at("old", 22, "u", "password", "2026-11-09T10:00:00Z")
            .expect("记录 old");
        history
            .record_at("new", 22, "u", "password", "2026-11-09T12:00:00Z")
            .expect("记录 new");
        history
            .record_at("mid", 22, "u", "key", "2026-11-09T11:00:00Z")
            .expect("记录 mid");

        let hosts: Vec<String> = history
            .list()
            .expect("读取历史")
            .into_iter()
            .map(|entry| entry.host)
            .collect();
        assert_eq!(hosts, vec!["new", "mid", "old"]);
    }

    #[test]
    fn eviction_is_lfu_then_lru() {
        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, 2);
        // A 两次、B/C 各一次；超限先淘汰 count 最小的最旧者 B。
        history
            .record_at("a", 22, "u", "password", "2026-11-09T10:00:00Z")
            .unwrap();
        history
            .record_at("a", 22, "u", "password", "2026-11-09T10:01:00Z")
            .unwrap();
        history
            .record_at("b", 22, "u", "password", "2026-11-09T10:02:00Z")
            .unwrap();
        history
            .record_at("c", 22, "u", "password", "2026-11-09T10:03:00Z")
            .unwrap();

        let hosts: Vec<String> = history
            .list()
            .unwrap()
            .into_iter()
            .map(|entry| entry.host)
            .collect();
        assert_eq!(hosts, vec!["c", "a"], "count 相同淘汰 last_used 最旧者 B");

        // D 进来后，count=1 的 C 比 D 旧，应淘汰 C，A 因 use_count=2 保留。
        history
            .record_at("d", 22, "u", "key", "2026-11-09T10:04:00Z")
            .unwrap();
        let hosts: Vec<String> = history
            .list()
            .unwrap()
            .into_iter()
            .map(|entry| entry.host)
            .collect();
        assert_eq!(hosts, vec!["d", "a"]);
    }

    #[test]
    fn limit_boundaries_four_and_default_sixty_four() {
        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, 4);
        for index in 0..5 {
            history
                .record_at(
                    &format!("h{index}"),
                    22,
                    "u",
                    "password",
                    &format!("2026-11-09T10:0{index}:00Z"),
                )
                .unwrap();
        }
        let entries = history.list().unwrap();
        assert_eq!(entries.len(), 4, "limit=4 应只保留 4 条");
        assert_eq!(entries.last().unwrap().host, "h1", "最旧的 h0 应被淘汰");

        let big = tempfile::tempdir().expect("临时目录");
        let history = SshHistory::new(big.path(), DEFAULT_SSH_HISTORY_LIMIT);
        assert_eq!(history.limit, 64);
        for index in 0..64 {
            history
                .record_at(
                    &format!("h{index}"),
                    22,
                    "u",
                    "password",
                    &format!("2026-11-09T10:{index:02}:00Z"),
                )
                .unwrap();
        }
        assert_eq!(history.list().unwrap().len(), 64, "默认上限应为 64");
    }

    #[test]
    fn zero_limit_disables_history() {
        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, 0);
        history
            .record("example.com", 22, "alice", "password")
            .unwrap();
        assert!(history.list().unwrap().is_empty());
        assert!(!dir.path().join(SSH_HISTORY_FILE_NAME).exists());
    }

    #[test]
    fn corrupt_json_is_ignored_and_create_can_overwrite() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join(SSH_HISTORY_FILE_NAME);
        fs::write(&path, "{not valid json").expect("写入损坏文件");

        let history = store(&dir, DEFAULT_SSH_HISTORY_LIMIT);
        assert!(
            history.list().unwrap().is_empty(),
            "损坏 JSON 应按空历史处理"
        );

        history
            .record("example.com", 22, "alice", "password")
            .expect("损坏后记录不应失败");
        let entries = history.list().unwrap();
        assert_eq!(entries.len(), 1);
        let file = read_file(&dir);
        assert_eq!(file["entries"][0]["host"], "example.com");
    }

    #[test]
    fn only_non_sensitive_fields_are_persisted() {
        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, DEFAULT_SSH_HISTORY_LIMIT);
        history
            .record("example.com", 22, "alice", "password")
            .expect("记录密码认证");
        history
            .record("key.example.com", 2222, "bob", "key")
            .expect("记录私钥认证");

        let raw = fs::read_to_string(dir.path().join(SSH_HISTORY_FILE_NAME)).unwrap();
        for secret in [
            "secret-password",
            "passphrase",
            "PRIVATE KEY",
            "id_ed25519",
            "id_rsa",
        ] {
            assert!(!raw.contains(secret), "历史文件不得包含敏感值 {secret}");
        }

        let file = read_file(&dir);
        for entry in file["entries"].as_array().unwrap() {
            let mut keys: Vec<&str> = entry
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                vec![
                    "auth_type",
                    "host",
                    "last_used",
                    "port",
                    "use_count",
                    "user"
                ],
                "历史条目只能包含非敏感字段"
            );
        }
    }

    #[test]
    fn remove_and_clear_are_idempotent() {
        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, DEFAULT_SSH_HISTORY_LIMIT);
        history.record("a", 22, "u", "password").unwrap();
        history.record("b", 23, "u", "password").unwrap();

        assert!(history.remove("a", 22, "u").unwrap());
        assert!(!history.remove("a", 22, "u").unwrap(), "重复删除应幂等");
        assert_eq!(history.list().unwrap().len(), 1);
        assert_eq!(history.clear().unwrap(), 1);
        assert_eq!(history.clear().unwrap(), 0);
        assert!(history.list().unwrap().is_empty());
    }

    #[test]
    fn json_file_has_no_temp_leftovers() {
        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, DEFAULT_SSH_HISTORY_LIMIT);
        history.record("a", 22, "u", "password").unwrap();
        let leftovers: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".tmp") || name.contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "不应残留临时文件: {leftovers:?}");
    }

    #[test]
    fn concurrent_records_are_serialized() {
        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, DEFAULT_SSH_HISTORY_LIMIT);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let history = history.clone();
                scope.spawn(move || {
                    for _ in 0..10 {
                        history
                            .record("example.com", 22, "alice", "password")
                            .unwrap();
                    }
                });
            }
        });
        let entries = history.list().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].use_count, 80, "并发记录不能丢更新");
    }

    #[cfg(unix)]
    #[test]
    fn file_permissions_are_0600() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("临时目录");
        let history = store(&dir, DEFAULT_SSH_HISTORY_LIMIT);
        history.record("a", 22, "u", "password").unwrap();
        let mode = fs::metadata(dir.path().join(SSH_HISTORY_FILE_NAME))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}
