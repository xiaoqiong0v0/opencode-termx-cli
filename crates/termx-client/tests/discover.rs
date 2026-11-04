//! `ClientConfig` 的端口/token 发现规则：配置目录必须显式。

use std::collections::HashMap;

use termx_client::{ClientConfig, ClientError, TERMX_CONFIG_DIR_ENV, TERMX_TOKEN_ENV};

#[test]
fn discover_reads_port_and_token_files() {
    let dir = tempfile::tempdir().expect("临时目录");
    std::fs::write(dir.path().join("port"), "45678\n").expect("写端口文件");
    std::fs::write(dir.path().join("token"), " file-token \n").expect("写 token 文件");
    let config_dir = dir.path().to_path_buf();

    let config = ClientConfig::discover_with(|key| {
        (key == TERMX_CONFIG_DIR_ENV).then(|| config_dir.as_os_str().to_owned())
    })
    .expect("发现配置");

    assert_eq!(config.endpoint, "ws://127.0.0.1:45678");
    assert_eq!(config.token, "file-token");
    assert_eq!(config.client, termx_client::DEFAULT_CLIENT_NAME);
}

#[test]
fn discover_token_env_overrides_token_file() {
    let dir = tempfile::tempdir().expect("临时目录");
    std::fs::write(dir.path().join("port"), "45679").expect("写端口文件");
    std::fs::write(dir.path().join("token"), "file-token").expect("写 token 文件");

    let mut env = HashMap::new();
    env.insert(TERMX_CONFIG_DIR_ENV, dir.path().as_os_str().to_owned());
    env.insert(TERMX_TOKEN_ENV, "env-token".into());

    let config = ClientConfig::discover_with(|key| env.get(key).cloned()).expect("发现配置");
    assert_eq!(config.endpoint, "ws://127.0.0.1:45679");
    assert_eq!(config.token, "env-token");
}

#[test]
fn discover_from_dir_reads_files_without_env() {
    let dir = tempfile::tempdir().expect("临时目录");
    std::fs::write(dir.path().join("port"), "45700").expect("写端口文件");
    std::fs::write(dir.path().join("token"), "file-token").expect("写 token 文件");

    let config = ClientConfig::discover_from_dir(dir.path(), |_| None).expect("显式目录发现配置");
    assert_eq!(config.endpoint, "ws://127.0.0.1:45700");
    assert_eq!(config.token, "file-token");
}

#[test]
fn discover_without_explicit_dir_is_rejected() {
    // 即使存在平台默认目录相关变量，也不再被采用。
    let mut env: HashMap<&str, std::ffi::OsString> = HashMap::new();
    env.insert("APPDATA", "C:\\Users\\test\\AppData\\Roaming".into());
    env.insert("HOME", "/home/test".into());
    env.insert("XDG_CONFIG_HOME", "/home/test/.config".into());

    let error =
        ClientConfig::discover_with(|key| env.get(key).cloned()).expect_err("缺少显式目录应失败");
    match error {
        ClientError::Connect(message) => {
            assert!(
                message.contains(TERMX_CONFIG_DIR_ENV),
                "应提示显式目录: {message}"
            );
        }
        other => panic!("应返回 Connect 错误，实际: {other:?}"),
    }
}

#[test]
fn discover_token_without_explicit_dir_is_rejected() {
    let error = ClientConfig::discover_token_with(|_| None).expect_err("缺少显式目录应失败");
    assert!(matches!(error, ClientError::Connect(_)));
}
