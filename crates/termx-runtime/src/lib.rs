//! 本地 runtime catalog。
//!
//! service 只读 `<config_dir>/runtimes/index.json`，绝不从网络下载或搬运二进制。
//! manifest 中的 `bin` 必须是 runtime 目录内的相对路径；解析时同时做词法越界
//! 检查、存在性检查与可执行检查，避免把任意绝对路径交给 PTY 启动。
//!
//! # 示例
//!
//! ```json
//! {
//!   "runtimes": [
//!     {
//!       "id": "termx-pwsh",
//!       "name": "termx-pwsh",
//!       "version": "0.1.0",
//!       "platform": "windows",
//!       "arch": "x86_64",
//!       "bin": "bin/termx-pwsh.exe",
//!       "args": []
//!     }
//!   ]
//! }
//! ```

use std::{
    env::consts::{ARCH, OS},
    fs,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};

/// manifest 文件名。
pub const INDEX_FILE_NAME: &str = "index.json";
/// `runtimes` 子目录名。
pub const RUNTIMES_DIR_NAME: &str = "runtimes";
/// 使用带外结构化 shell 事件通道的本地 runtime ID。
///
/// 普通程序（含直接启动的 `bash`/`sh`）不在列表内，保持 raw-only。
pub const STRUCTURED_RUNTIME_IDS: &[&str] = &["termx-pwsh", "termx-bash"];

/// 判断 runtime ID 是否使用结构化 shell 事件通道。
pub fn is_structured_runtime(id: &str) -> bool {
    STRUCTURED_RUNTIME_IDS.contains(&id)
}

/// manifest 根结构。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeManifest {
    /// 条目列表；缺省为空。
    #[serde(default)]
    pub runtimes: Vec<RuntimeEntry>,
}

/// 单个 runtime 条目。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEntry {
    /// 稳定 ID，例如 `termx-pwsh`。
    pub id: String,
    /// 展示名。
    pub name: String,
    /// 版本号（自由格式）。
    pub version: String,
    /// 平台：`windows` / `linux` / `macos`。
    pub platform: String,
    /// 架构：`x86_64` / `aarch64` 等。
    pub arch: String,
    /// runtime 目录内的相对路径。
    pub bin: String,
    /// 启动参数；缺省为空。
    #[serde(default)]
    pub args: Vec<String>,
}

/// 面向 RPC/UI 的只读摘要（不暴露绝对路径）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeSummary {
    /// runtime ID。
    pub id: String,
    /// 展示名。
    pub name: String,
    /// 版本。
    pub version: String,
    /// 平台。
    pub platform: String,
    /// 架构。
    pub arch: String,
    /// 当前机器上 bin 是否存在且可执行。
    pub available: bool,
}

/// 已解析、可直接交给进程启动的 runtime。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRuntime {
    /// runtime ID。
    pub id: String,
    /// 绝对（或相对 runtime_dir 的完整）bin 路径。
    pub bin_path: PathBuf,
    /// manifest 中的启动参数。
    pub args: Vec<String>,
}

/// catalog 加载/解析错误。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeError {
    /// manifest 文件不存在。
    #[error("runtime manifest 不存在: {0}")]
    MissingManifest(PathBuf),
    /// manifest 读取失败。
    #[error("读取 runtime manifest 失败: {path}: {message}")]
    Io {
        /// 文件路径。
        path: PathBuf,
        /// 错误说明。
        message: String,
    },
    /// manifest JSON 非法或字段缺失。
    #[error("runtime manifest 非法: {0}")]
    InvalidManifest(String),
    /// 指定 ID 不存在。
    #[error("runtime 不存在: {0}")]
    NotFound(String),
    /// 条目与当前平台/架构不兼容。
    #[error("runtime {id} 不兼容当前平台（需要 {platform}/{arch}）")]
    Incompatible {
        /// runtime ID。
        id: String,
        /// 条目要求的平台。
        platform: String,
        /// 条目要求的架构。
        arch: String,
    },
    /// `bin` 不是 runtime 目录内的安全相对路径。
    #[error("runtime {id} 的 bin 路径非法: {bin}")]
    UnsafeBinary {
        /// runtime ID。
        id: String,
        /// 原始 bin。
        bin: String,
    },
    /// 可执行文件不存在。
    #[error("runtime {id} 的可执行文件不存在: {path}")]
    MissingBinary {
        /// runtime ID。
        id: String,
        /// 解析后的路径。
        path: PathBuf,
    },
    /// 可执行文件没有执行权限（Unix）或扩展名不可执行（Windows）。
    #[error("runtime {id} 的可执行文件不可执行: {path}")]
    NotExecutable {
        /// runtime ID。
        id: String,
        /// 解析后的路径。
        path: PathBuf,
    },
}

/// 本地 runtime catalog；加载失败时保持为空，不 panic。
#[derive(Debug, Clone)]
pub struct RuntimeCatalog {
    /// runtime 根目录。
    dir: PathBuf,
    /// manifest 条目。
    entries: Vec<RuntimeEntry>,
}

impl RuntimeCatalog {
    /// 在 `dir` 下读取 `index.json`。
    ///
    /// 无论成功与否都返回 catalog；失败时 catalog 为空并通过第二个返回值给出
    /// 明确原因。调用方应记录错误日志，但继续以空 catalog 运行（旧 cmd/args
    /// 路径不受影响）。
    pub fn load(dir: impl AsRef<Path>) -> (Self, Option<RuntimeError>) {
        let dir = dir.as_ref().to_path_buf();
        let empty = Self {
            dir: dir.clone(),
            entries: Vec::new(),
        };
        let path = dir.join(INDEX_FILE_NAME);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return (empty, Some(RuntimeError::MissingManifest(path)));
            }
            Err(error) => {
                return (
                    empty,
                    Some(RuntimeError::Io {
                        path,
                        message: error.to_string(),
                    }),
                );
            }
        };
        let manifest: RuntimeManifest = match serde_json::from_slice(&bytes) {
            Ok(manifest) => manifest,
            Err(error) => {
                return (
                    empty,
                    Some(RuntimeError::InvalidManifest(error.to_string())),
                );
            }
        };
        if let Err(error) = validate_manifest(&manifest) {
            return (empty, Some(error));
        }
        (
            Self {
                dir,
                entries: manifest.runtimes,
            },
            None,
        )
    }

    /// runtime 根目录。
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 是否没有条目。
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 条目数量。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 全部条目（不按平台过滤），供诊断使用。
    pub fn entries(&self) -> &[RuntimeEntry] {
        &self.entries
    }

    /// 按 ID 查找条目，不做平台/路径校验。
    pub fn get(&self, id: &str) -> Option<&RuntimeEntry> {
        self.entries.iter().find(|entry| entry.id == id)
    }

    /// 只返回与给定平台/架构兼容的条目摘要。
    ///
    /// `available` 表示 bin 在当前文件系统上存在且可执行；路径不安全或缺失的
    /// 条目仍会返回（`available = false`），便于 UI 区分“平台不兼容”和“文件缺失”。
    pub fn list(&self, platform: &str, arch: &str) -> Vec<RuntimeSummary> {
        self.entries
            .iter()
            .filter(|entry| entry.platform == platform && entry.arch == arch)
            .map(|entry| RuntimeSummary {
                id: entry.id.clone(),
                name: entry.name.clone(),
                version: entry.version.clone(),
                platform: entry.platform.clone(),
                arch: entry.arch.clone(),
                available: self.check_binary(entry).is_ok(),
            })
            .collect()
    }

    /// 解析指定 ID：检查平台、路径安全、存在性与可执行性。
    pub fn resolve(&self, id: &str) -> Result<ResolvedRuntime, RuntimeError> {
        let entry = self
            .get(id)
            .ok_or_else(|| RuntimeError::NotFound(id.to_string()))?;
        if entry.platform != current_platform() || entry.arch != current_arch() {
            return Err(RuntimeError::Incompatible {
                id: entry.id.clone(),
                platform: entry.platform.clone(),
                arch: entry.arch.clone(),
            });
        }
        let bin_path = self.check_binary(entry)?;
        Ok(ResolvedRuntime {
            id: entry.id.clone(),
            bin_path,
            args: entry.args.clone(),
        })
    }

    /// 校验并返回 bin 的完整路径。
    fn check_binary(&self, entry: &RuntimeEntry) -> Result<PathBuf, RuntimeError> {
        let relative = safe_relative_bin(&entry.id, &entry.bin)?;
        let path = self.dir.join(relative);
        let metadata = fs::metadata(&path).map_err(|_| RuntimeError::MissingBinary {
            id: entry.id.clone(),
            path: path.clone(),
        })?;
        if !metadata.is_file() {
            return Err(RuntimeError::MissingBinary {
                id: entry.id.clone(),
                path,
            });
        }
        // 存在时再做一次 canonicalize，挡住通过 symlink 跳出 runtime 目录。
        if let (Ok(root), Ok(canonical)) = (fs::canonicalize(&self.dir), fs::canonicalize(&path)) {
            if !canonical.starts_with(&root) {
                return Err(RuntimeError::UnsafeBinary {
                    id: entry.id.clone(),
                    bin: entry.bin.clone(),
                });
            }
        }
        if !is_executable(&path, &metadata) {
            return Err(RuntimeError::NotExecutable {
                id: entry.id.clone(),
                path,
            });
        }
        Ok(path)
    }
}

/// 当前平台标识（与 manifest 的 `platform` 对齐）。
pub fn current_platform() -> String {
    match OS {
        "windows" => "windows".to_string(),
        "linux" => "linux".to_string(),
        "macos" => "macos".to_string(),
        other => other.to_string(),
    }
}

/// 当前架构标识（与 manifest 的 `arch` 对齐）。
pub fn current_arch() -> String {
    match ARCH {
        "x86_64" => "x86_64".to_string(),
        "aarch64" => "aarch64".to_string(),
        other => other.to_string(),
    }
}

/// 校验 manifest 的必填字段与 ID 唯一性。
fn validate_manifest(manifest: &RuntimeManifest) -> Result<(), RuntimeError> {
    let mut seen = std::collections::BTreeSet::new();
    for entry in &manifest.runtimes {
        if entry.id.trim().is_empty() {
            return Err(RuntimeError::InvalidManifest("id 不能为空".to_string()));
        }
        if entry.name.trim().is_empty() {
            return Err(RuntimeError::InvalidManifest(format!(
                "runtime {} 的 name 不能为空",
                entry.id
            )));
        }
        if entry.version.trim().is_empty() {
            return Err(RuntimeError::InvalidManifest(format!(
                "runtime {} 的 version 不能为空",
                entry.id
            )));
        }
        if entry.platform.trim().is_empty() || entry.arch.trim().is_empty() {
            return Err(RuntimeError::InvalidManifest(format!(
                "runtime {} 的 platform/arch 不能为空",
                entry.id
            )));
        }
        if !seen.insert(entry.id.as_str()) {
            return Err(RuntimeError::InvalidManifest(format!(
                "runtime id 重复: {}",
                entry.id
            )));
        }
    }
    Ok(())
}

/// 把 `bin` 校验为纯相对路径，拒绝绝对路径、前缀、父目录与空路径。
fn safe_relative_bin<'a>(id: &str, bin: &'a str) -> Result<&'a Path, RuntimeError> {
    let path = Path::new(bin);
    let unsafe_bin = || RuntimeError::UnsafeBinary {
        id: id.to_string(),
        bin: bin.to_string(),
    };
    let bytes = bin.as_bytes();
    let has_drive_prefix =
        bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes.get(1) == Some(&b':');
    let has_unc_prefix = bin.starts_with(r"\\") || bin.starts_with("//");
    if bin.trim().is_empty() || path.is_absolute() || has_drive_prefix || has_unc_prefix {
        return Err(unsafe_bin());
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(unsafe_bin());
            }
        }
    }
    Ok(path)
}

/// 判断文件是否可执行。
#[cfg(unix)]
fn is_executable(_path: &Path, metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

/// Windows 上以扩展名判断可执行；service 的 PTY 只会直接启动可执行文件。
#[cfg(windows)]
fn is_executable(path: &Path, _metadata: &fs::Metadata) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "exe" | "cmd" | "bat" | "com"
            )
        })
}

/// 其他平台保持“存在即视为可执行”。
#[cfg(not(any(unix, windows)))]
fn is_executable(_path: &Path, _metadata: &fs::Metadata) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 在临时目录写 manifest 与假 bin。
    fn fixture(manifest: &str, bin: Option<&str>) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("创建临时目录失败");
        std::fs::create_dir_all(dir.path().join("bin")).expect("创建 bin 目录失败");
        std::fs::write(dir.path().join(INDEX_FILE_NAME), manifest).expect("写 manifest 失败");
        if let Some(bin) = bin {
            let path = dir.path().join(bin);
            std::fs::write(&path, b"stub").expect("写假 bin 失败");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                    .expect("设置执行权限失败");
            }
        }
        let dir_path = dir.path().to_path_buf();
        (dir, dir_path)
    }

    /// 当前平台 manifest。
    fn manifest(id: &str, bin: &str) -> String {
        format!(
            r#"{{"runtimes":[{{"id":"{id}","name":"{id}","version":"0.1.0","platform":"{}","arch":"{}","bin":"{bin}","args":["--no-profile"]}}]}}"#,
            current_platform(),
            current_arch()
        )
    }

    #[test]
    fn loads_valid_manifest_and_resolves_binary() {
        let (_guard, dir) = fixture(
            &manifest("termx-pwsh", "bin/fake-runtime.exe"),
            Some("bin/fake-runtime.exe"),
        );
        let (catalog, error) = RuntimeCatalog::load(&dir);
        assert!(error.is_none(), "不应有错误: {error:?}");
        assert_eq!(catalog.len(), 1);
        let list = catalog.list(&current_platform(), &current_arch());
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "termx-pwsh");
        assert!(list[0].available);
        assert!(
            !serde_json::to_string(&list[0])
                .expect("摘要应可序列化")
                .contains("bin")
        );

        let resolved = catalog.resolve("termx-pwsh").expect("解析应成功");
        assert_eq!(resolved.bin_path, dir.join("bin/fake-runtime.exe"));
        assert_eq!(resolved.args, vec!["--no-profile".to_string()]);
    }

    #[test]
    fn structured_runtime_ids_cover_both_managed_shells() {
        assert!(is_structured_runtime("termx-pwsh"));
        assert!(is_structured_runtime("termx-bash"));
        for plain in ["bash", "sh", "pwsh", "termx-bash-old", ""] {
            assert!(!is_structured_runtime(plain), "{plain:?} 不应结构化");
        }
    }

    #[test]
    fn missing_manifest_returns_empty_catalog_and_error() {
        let dir = tempfile::tempdir().expect("创建临时目录失败");
        let (catalog, error) = RuntimeCatalog::load(dir.path());
        assert!(catalog.is_empty());
        assert!(matches!(error, Some(RuntimeError::MissingManifest(_))));
    }

    #[test]
    fn invalid_json_returns_empty_catalog_and_error() {
        let (_guard, dir) = fixture("{ not json", None);
        let (catalog, error) = RuntimeCatalog::load(&dir);
        assert!(catalog.is_empty());
        assert!(matches!(error, Some(RuntimeError::InvalidManifest(_))));
    }

    #[test]
    fn escapes_and_absolute_bins_are_rejected() {
        for bin in [
            "../outside.exe",
            "bin/../../outside.exe",
            "C:\\Windows\\System32\\cmd.exe",
            "/bin/sh",
        ] {
            let json = serde_json::json!({
                "runtimes": [{
                    "id": "bad",
                    "name": "bad",
                    "version": "1",
                    "platform": current_platform(),
                    "arch": current_arch(),
                    "bin": bin,
                    "args": []
                }]
            })
            .to_string();
            let (_guard, dir) = fixture(&json, None);
            let (catalog, error) = RuntimeCatalog::load(&dir);
            assert!(error.is_none(), "manifest 本身合法: {error:?}");
            let error = catalog.resolve("bad").expect_err("不安全 bin 应拒绝");
            match error {
                RuntimeError::UnsafeBinary { .. } => {}
                other => panic!("应返回 UnsafeBinary，实际 {other:?}"),
            }
        }
    }

    #[test]
    fn filters_incompatible_platforms() {
        let other_platform = if current_platform() == "windows" {
            "linux"
        } else {
            "windows"
        };
        let json = format!(
            r#"{{"runtimes":[{{"id":"other","name":"other","version":"1","platform":"{other_platform}","arch":"{}","bin":"bin/x"}}]}}"#,
            current_arch()
        );
        let (_guard, dir) = fixture(&json, Some("bin/x"));
        let (catalog, error) = RuntimeCatalog::load(&dir);
        assert!(error.is_none());
        assert!(
            catalog
                .list(&current_platform(), &current_arch())
                .is_empty()
        );
        assert!(matches!(
            catalog.resolve("other"),
            Err(RuntimeError::Incompatible { .. })
        ));
    }

    #[test]
    fn missing_binary_is_listed_unavailable_and_resolve_fails() {
        let (_guard, dir) = fixture(&manifest("termx-pwsh", "bin/missing"), None);
        let (catalog, error) = RuntimeCatalog::load(&dir);
        assert!(error.is_none());
        let list = catalog.list(&current_platform(), &current_arch());
        assert_eq!(list.len(), 1);
        assert!(!list[0].available);
        assert!(matches!(
            catalog.resolve("termx-pwsh"),
            Err(RuntimeError::MissingBinary { .. })
        ));
    }

    #[test]
    fn duplicate_ids_are_rejected() {
        let entry = format!(
            r#"{{"id":"dup","name":"dup","version":"1","platform":"{}","arch":"{}","bin":"bin/x"}}"#,
            current_platform(),
            current_arch()
        );
        let json = format!(r#"{{"runtimes":[{entry},{entry}]}}"#);
        let (_guard, dir) = fixture(&json, Some("bin/x"));
        let (catalog, error) = RuntimeCatalog::load(&dir);
        assert!(catalog.is_empty());
        assert!(matches!(error, Some(RuntimeError::InvalidManifest(_))));
    }
}
