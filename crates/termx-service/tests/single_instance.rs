//! 单实例锁：同一 `config_dir` 只允许一个 `Service` 绑定并写 port 文件。

use std::{fs, path::Path};

use termx_service::{PORT_FILE_NAME, SERVICE_LOCK_FILE_NAME, Service, ServiceConfig, ServiceError};

/// 读取 port 文件内容；文件缺失或非整数时 panic（测试断言用）。
fn read_port(config_dir: &Path) -> u16 {
    let path = config_dir.join(PORT_FILE_NAME);
    let raw = fs::read_to_string(&path).expect("读取 port 文件失败");
    raw.trim().parse::<u16>().expect("port 文件内容非法")
}

#[tokio::test]
async fn second_bind_same_config_dir_is_rejected_and_keeps_port_file() {
    let dir = tempfile::tempdir().expect("创建临时目录失败");
    let config = ServiceConfig::new(dir.path().to_path_buf());

    let first = Service::bind(config.clone()).await.expect("首次绑定应成功");
    let first_port = read_port(dir.path());
    assert_eq!(
        first_port,
        first.local_addr().port(),
        "port 文件应对应首次绑定"
    );

    // 第二次绑定必须被单实例锁拒绝，且不得覆盖首次写入的 port 文件。
    match Service::bind(config.clone()).await {
        Err(ServiceError::AlreadyRunning(path)) => {
            assert_eq!(path, dir.path().join(SERVICE_LOCK_FILE_NAME));
        }
        Err(error) => panic!("第二次绑定应为 AlreadyRunning，实际错误: {error}"),
        Ok(_) => panic!("第二次绑定不应成功"),
    }
    assert_eq!(
        read_port(dir.path()),
        first_port,
        "失败的绑定不得覆盖 port 文件"
    );

    // 丢弃第一个实例等价于进程退出：锁由 OS 释放，之后可以重新绑定。
    drop(first);
    let third = Service::bind(config).await.expect("锁释放后应可重新绑定");
    assert_eq!(
        read_port(dir.path()),
        third.local_addr().port(),
        "重新绑定后 port 文件应更新为新端口"
    );
}

#[tokio::test]
async fn web_bind_failure_keeps_previous_port_file() {
    let dir = tempfile::tempdir().expect("创建临时目录失败");
    let config = ServiceConfig::new(dir.path().to_path_buf());

    let first = Service::bind(config.clone()).await.expect("首次绑定应成功");
    let previous_port = read_port(dir.path());
    drop(first);

    // 预占 Web 端口，让第二次绑定在写 port 之前失败。
    let occupied = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("预占 Web 端口失败");
    let web_port = occupied.local_addr().expect("读取预占端口失败").port();

    let error = Service::bind(config.with_web_port(web_port))
        .await
        .err()
        .expect("Web 端口冲突应导致绑定失败");
    assert!(matches!(error, ServiceError::Io(_)), "实际错误: {error}");
    assert_eq!(
        read_port(dir.path()),
        previous_port,
        "失败绑定不得覆盖上一次写入的 port 文件"
    );

    drop(occupied);
}
