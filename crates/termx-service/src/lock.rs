//! 单实例锁：对 `<config_dir>/service.lock` 持有进程级排他文件锁。
//!
//! 锁由 [`ServiceLock`] 的文件句柄持有；进程正常退出或崩溃时由操作系统自动释放，
//! 因此无需清理「陈旧锁」。锁文件本身保留在磁盘上，下次启动直接复用。
//!
//! 崩溃恢复与多进程并发由内核保证：只有真正持有锁的实例才能继续绑定端口、
//! 写入 token/port 文件。

use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

use fs2::FileExt as _;

/// 获取排他锁失败的原因。
#[derive(Debug)]
pub(crate) enum LockError {
    /// 锁已被其它实例持有（另一个 termx-service 正在运行）。
    Contended,
    /// 打开锁文件或加锁时的 I/O 失败。
    Io(io::Error),
}

/// 持有的单实例锁。
///
/// 字段保存文件句柄：句柄存活即锁存活；`Drop` 关闭句柄时内核释放锁。
pub(crate) struct ServiceLock {
    /// 仅用于保持文件句柄（锁）存活，不直接读取。
    _file: File,
}

impl ServiceLock {
    /// 在指定路径上**非阻塞**尝试获取排他锁。
    ///
    /// 目录必须已存在；调用方负责在获取锁之前准备好配置目录。
    pub(crate) fn acquire(path: &Path) -> Result<Self, LockError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(LockError::Io)?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { _file: file }),
            Err(error) if is_contended(&error) => Err(LockError::Contended),
            Err(error) => Err(LockError::Io(error)),
        }
    }
}

/// 判断错误是否表示「锁已被占用」。
///
/// Windows 的 `ERROR_LOCK_VIOLATION` 与 Unix 的 `EWOULDBLOCK` 由 fs2 暴露为
/// [`fs2::lock_contended_error`]；优先比较原始错误码，拿不到时回退比较 kind。
fn is_contended(error: &io::Error) -> bool {
    let contended = fs2::lock_contended_error();
    match (error.raw_os_error(), contended.raw_os_error()) {
        (Some(actual), Some(expected)) => actual == expected,
        _ => error.kind() == contended.kind(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_is_exclusive_and_released_on_drop() {
        let dir = tempfile::tempdir().expect("创建临时目录失败");
        let path = dir.path().join("service.lock");

        let first = ServiceLock::acquire(&path).expect("首次加锁应成功");
        assert!(
            matches!(ServiceLock::acquire(&path), Err(LockError::Contended)),
            "同一锁文件的第二次获取应判定为已占用"
        );

        drop(first);
        ServiceLock::acquire(&path).expect("锁释放后应可重新获取");
    }
}
