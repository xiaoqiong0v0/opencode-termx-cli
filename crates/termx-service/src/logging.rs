//! tracing 初始化、按日文件日志与过期日志清理。
//!
//! 日志写入 `<log-dir>/service-YYYY-MM-DD.log`（UTC 日期，与
//! `tracing-appender` 的按日滚动口径一致）。文件写入走
//! `tracing_appender::non_blocking`，避免阻塞同步 runtime 线程；返回的
//! [`WorkerGuard`] 必须持有到进程退出，否则尾部日志可能丢失。
//!
//! `log_enabled=false` 时不创建、不写任何文件，只保留 stderr 输出
//! （与之前的行为一致）。清理失败一律不致命，不影响服务启动。

use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use time::{Date, Month, OffsetDateTime};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::ServiceConfig;

/// 未设置 `RUST_LOG` 时的默认日志级别。
const DEFAULT_LOG_FILTER: &str = "info";
/// 日志文件名前缀。
const LOG_FILE_PREFIX: &str = "service-";
/// 日志文件名后缀。
const LOG_FILE_SUFFIX: &str = ".log";

/// 初始化 tracing；启用文件日志时返回必须持有到进程退出的 guard。
///
/// 任何文件日志初始化失败（目录不可创建、文件不可写）都会回退为仅
/// stderr，并以 `eprintln!` 说明原因，不阻止服务启动。
pub fn init_logging(config: &ServiceConfig) -> Option<WorkerGuard> {
    if config.log_enabled {
        if let Some(dir) = config.log_dir.as_deref() {
            match start_file_logging(dir, config.log_max_days) {
                Ok((writer, guard, pruned)) => {
                    init_subscriber_with_file(writer, new_filter());
                    tracing::info!(
                        dir = %dir.display(),
                        max_days = config.log_max_days,
                        pruned = pruned,
                        "文件日志已启用"
                    );
                    return Some(guard);
                }
                Err(error) => {
                    eprintln!(
                        "termx-service: 启用文件日志失败（{}）：{error}；回退为仅 stderr",
                        dir.display()
                    );
                }
            }
        } else {
            // 参数解析阶段已拦截该组合；这里兜底，避免库调用方漏校验。
            eprintln!("termx-service: 已启用文件日志但未提供日志目录；回退为仅 stderr");
        }
    }

    init_subscriber_stderr(new_filter());
    None
}

/// 构造日志过滤器；`RUST_LOG` 非法时回退默认级别。
fn new_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER))
}

/// 初始化「stderr + 文件」双输出。
fn init_subscriber_with_file(writer: NonBlocking, filter: EnvFilter) {
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(io::stderr))
        .with(fmt::layer().with_writer(writer).with_ansi(false))
        .init();
}

/// 仅初始化 stderr 输出。
fn init_subscriber_stderr(filter: EnvFilter) {
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(io::stderr))
        .init();
}

/// 创建日志目录、清理过期日志并启动非阻塞写入。
///
/// 返回 `(writer, guard, 清理数量)`。清理失败只记录到 stderr，不影响
/// 文件日志本身。
fn start_file_logging(dir: &Path, max_days: u32) -> io::Result<(NonBlocking, WorkerGuard, usize)> {
    std::fs::create_dir_all(dir)?;
    let pruned = match prune_old_logs(dir, max_days, utc_today()) {
        Ok(paths) => paths.len(),
        Err(error) => {
            eprintln!(
                "termx-service: 清理过期日志失败（{}）：{error}；继续启动",
                dir.display()
            );
            0
        }
    };
    let (writer, guard) = tracing_appender::non_blocking(DailyFile::new(dir.to_path_buf())?);
    Ok((writer, guard, pruned))
}

/// 当前 UTC 日期；与按日文件命名保持一致。
fn utc_today() -> Date {
    OffsetDateTime::now_utc().date()
}

/// 指定日期的日志文件名。
fn log_file_name(date: Date) -> String {
    format!("{LOG_FILE_PREFIX}{date}{LOG_FILE_SUFFIX}")
}

/// 按日切换的文件写入器。
///
/// 只在非阻塞 worker 线程内被调用；每次写入前检查日期，跨天时换到新文件。
struct DailyFile {
    dir: PathBuf,
    current: Option<(Date, File)>,
}

impl DailyFile {
    /// 打开当天文件；目录不存在时创建。
    fn new(dir: PathBuf) -> io::Result<Self> {
        let today = utc_today();
        let file = open_log_file(&dir, today)?;
        Ok(Self {
            dir,
            current: Some((today, file)),
        })
    }

    /// 日期变化时切换到新文件。
    fn rotate_if_needed(&mut self, today: Date) -> io::Result<()> {
        if matches!(self.current.as_ref(), Some((date, _)) if *date == today) {
            return Ok(());
        }
        let file = open_log_file(&self.dir, today)?;
        self.current = Some((today, file));
        Ok(())
    }
}

impl Write for DailyFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.rotate_if_needed(utc_today())?;
        match self.current.as_mut() {
            Some((_, file)) => file.write(buf),
            None => Err(io::Error::other("日志文件未打开")),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.current.as_mut() {
            Some((_, file)) => file.flush(),
            None => Ok(()),
        }
    }
}

/// 以追加方式打开指定日期的日志文件。
fn open_log_file(dir: &Path, date: Date) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(log_file_name(date)))
}

/// 删除 `dir` 下超过 `max_days`（含当天）的 `service-YYYY-MM-DD.log`。
///
/// - 保留最近 `max_days` 天：年龄大于等于 `max_days` 的文件删除。
/// - 只处理文件名匹配前缀/后缀的文件；其它文件与无法解析日期的文件跳过。
/// - 单个文件删除失败会被忽略，整体返回已删除路径。
fn prune_old_logs(dir: &Path, max_days: u32, today: Date) -> io::Result<Vec<PathBuf>> {
    let mut deleted = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let Ok(entry) = entry else { continue };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(date) = parse_log_date(name) else {
            continue;
        };
        if (today - date).whole_days() >= i64::from(max_days) {
            match std::fs::remove_file(&path) {
                Ok(()) => deleted.push(path),
                Err(error) => {
                    tracing::debug!(path = %path.display(), %error, "删除过期日志失败，已跳过");
                }
            }
        }
    }
    Ok(deleted)
}

/// 从 `service-YYYY-MM-DD.log` 解析日期；不匹配时返回 `None`。
fn parse_log_date(name: &str) -> Option<Date> {
    let middle = name
        .strip_prefix(LOG_FILE_PREFIX)?
        .strip_suffix(LOG_FILE_SUFFIX)?;
    let mut parts = middle.split('-');
    let year = parts.next()?.parse::<i32>().ok()?;
    let month = Month::try_from(parts.next()?.parse::<u8>().ok()?).ok()?;
    let day = parts.next()?.parse::<u8>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Date::from_calendar_date(year, month, day).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造日期，便于固定测试时钟。
    fn date(year: i32, month: u8, day: u8) -> Date {
        Date::from_calendar_date(year, Month::try_from(month).expect("月份合法"), day)
            .expect("日期合法")
    }

    #[test]
    fn log_file_name_uses_dashed_daily_format() {
        assert_eq!(
            log_file_name(date(2026, 11, 3)),
            "service-2026-11-03.log".to_string()
        );
    }

    #[test]
    fn parse_log_date_accepts_only_matching_names() {
        assert_eq!(
            parse_log_date("service-2026-11-03.log"),
            Some(date(2026, 11, 3))
        );
        assert_eq!(parse_log_date("service-2026-02-30.log"), None);
        assert_eq!(parse_log_date("service-bad.log"), None);
        assert_eq!(parse_log_date("other-2026-11-03.log"), None);
        assert_eq!(parse_log_date("service-2026-11-03.txt"), None);
    }

    #[test]
    fn prune_removes_only_expired_daily_logs() {
        let dir = tempfile::tempdir().expect("创建临时目录失败");
        let today = date(2026, 11, 3);

        let expired = dir.path().join("service-2026-11-01.log");
        let yesterday = dir.path().join("service-2026-11-02.log");
        let current = dir.path().join("service-2026-11-03.log");
        let unrelated = dir.path().join("other-2026-11-01.log");
        let wrong_suffix = dir.path().join("service-2026-11-01.txt");
        let unparsable = dir.path().join("service-bad.log");
        for path in [
            &expired,
            &yesterday,
            &current,
            &unrelated,
            &wrong_suffix,
            &unparsable,
        ] {
            std::fs::write(path, b"x").expect("写入测试文件失败");
        }

        // max_days=2：保留 11-02、11-03；11-01 年龄为 2 天，删除。
        let deleted = prune_old_logs(dir.path(), 2, today).expect("清理应成功");
        assert_eq!(deleted, vec![expired.clone()]);

        assert!(!expired.exists(), "过期日志应被删除");
        for kept in [&yesterday, &current, &unrelated, &wrong_suffix, &unparsable] {
            assert!(kept.exists(), "不应删除 {}", kept.display());
        }
    }

    #[test]
    fn daily_file_writes_into_dated_file() {
        let dir = tempfile::tempdir().expect("创建临时目录失败");
        let mut file = DailyFile::new(dir.path().to_path_buf()).expect("打开日志文件失败");
        file.write_all(b"hello").expect("写入失败");
        file.flush().expect("flush 失败");

        let path = dir.path().join(log_file_name(utc_today()));
        assert_eq!(
            std::fs::read_to_string(path).expect("读取日志失败"),
            "hello"
        );
    }
}
