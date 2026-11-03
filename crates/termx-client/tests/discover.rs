//! `ClientConfig::discover` 的端口/token 发现规则。

use std::collections::HashMap;

use termx_client::{ClientConfig, TERMX_CONFIG_DIR_ENV, TERMX_TOKEN_ENV};

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
