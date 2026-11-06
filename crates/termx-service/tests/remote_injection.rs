//! 远端注入授权接线测试：mock 工厂校验 service 组装的注入计划，
//! 不依赖真实 SSH / Docker，也不会写入任何远端文件。

mod common;

use std::sync::{Arc, Mutex};

use common::{TestService, call, connect, handshake_root};
use tempfile::tempdir;
use termx_core::{BackendFactory, CoreError, MockFactory, SpawnedBackend};
use termx_protocol::{
    CreateParams, ErrorCode, InjectionConsent, InjectionConsentMode, InjectionPlan, InjectionWrite,
    InjectionWriteAction, InjectionWriteKind, InjectionWriteReason,
};
use termx_service::{RemoteInjectionConfig, ServiceConfig};

/// 记录收到的创建参数，并在需要写入且未授权时返回 `ConsentRequired`。
#[derive(Default)]
struct ProbeFactory {
    /// service 传给后端的最终参数（含内部注入计划）。
    seen: Mutex<Vec<CreateParams>>,
}

impl BackendFactory for ProbeFactory {
    fn spawn(&self, params: &CreateParams) -> Result<SpawnedBackend, CoreError> {
        self.seen.lock().expect("probe 锁中毒").push(params.clone());

        if let Some(ssh) = params.ssh.as_ref() {
            let needs_write = ssh.upload_helper
                && ssh.injection.consent == InjectionConsentMode::Ask
                && !ssh.injection.allow;
            if needs_write {
                let path = ssh.helper_path.clone().unwrap_or_default();
                return Err(CoreError::ConsentRequired {
                    details: InjectionConsent {
                        host: ssh.host.clone(),
                        user: ssh.user.clone(),
                        writes: vec![InjectionWrite {
                            kind: InjectionWriteKind::Helper,
                            action: InjectionWriteAction::Install,
                            reason: InjectionWriteReason::Missing,
                            path,
                            size: Some(7),
                            sha256: Some("f".repeat(64)),
                        }],
                    },
                });
            }
        }

        <MockFactory as BackendFactory>::spawn(&MockFactory, params)
    }
}

/// 构造一个只到 service 为止的 SSH 创建参数。
///
/// Web/新客户端不再传 helper 字段；`termx-pwsh` 由 service 自动补齐。
fn remote_params(name: &str, cmd: &str, allow_injection: bool) -> serde_json::Value {
    serde_json::json!({
        "mode": "pty",
        "cmd": cmd,
        "root_session_id": "ses-inject",
        "name": name,
        "allow_injection": allow_injection,
        "ssh": {
            "host": "example.com",
            "port": 22,
            "user": "alice",
            "auth": { "type": "password", "password": "secret" },
            "accept_any_host_key": false
        }
    })
}

#[tokio::test]
async fn consent_required_carries_writes_and_allow_retry_succeeds() {
    let dir = tempdir().expect("创建临时目录失败");
    let config = ServiceConfig::new(dir.path().to_path_buf())
        .with_remote_injection(RemoteInjectionConfig::new().with_helper_local("./termx-helper"));
    let probe = Arc::new(ProbeFactory::default());
    let service =
        TestService::start_with_config_and_factory(config, Some(Arc::clone(&probe) as Arc<_>))
            .await;
    let mut client = connect(&service).await;
    handshake_root(&mut client, &service.token, "ses-inject").await;

    let denied = call(
        &mut client,
        "inject-deny",
        "terminal.create",
        remote_params("remote-deny", "termx-pwsh", false),
    )
    .await;
    assert!(!denied.ok, "未授权不应创建终端: {denied:?}");
    let error = denied.error.expect("缺少错误载荷");
    assert_eq!(error.code, ErrorCode::ConsentRequired);
    assert!(!error.retryable);
    let injection = error.injection.expect("consent_required 必须带写入计划");
    assert_eq!(injection.host, "example.com");
    assert_eq!(injection.user, "alice");
    assert_eq!(injection.writes.len(), 1);
    assert_eq!(injection.writes[0].kind, InjectionWriteKind::Helper);
    assert_eq!(
        injection.writes[0].path, "~/.termx/bin/termx-helper",
        "应使用服务端配置的默认安装路径"
    );

    let allowed = call(
        &mut client,
        "inject-allow",
        "terminal.create",
        remote_params("remote-allow", "termx-pwsh", true),
    )
    .await;
    assert!(allowed.ok, "带 allow_injection 的请求应被接受: {allowed:?}");

    let seen = probe.seen.lock().expect("probe 锁中毒");
    assert_eq!(seen.len(), 2);
    assert!(!seen[0].allow_injection);
    assert_eq!(
        seen[0].ssh.as_ref().expect("ssh").injection.consent,
        InjectionConsentMode::Ask
    );
    assert!(!seen[0].ssh.as_ref().expect("ssh").injection.allow);

    let first = seen[0].ssh.as_ref().expect("ssh");
    assert!(first.upload_helper, "termx-pwsh 应自动开启上传");
    assert_eq!(first.helper_local_path.as_deref(), Some("./termx-helper"));

    let retried = seen[1].ssh.as_ref().expect("ssh");
    assert!(seen[1].allow_injection);
    assert!(retried.injection.allow, "allow 只来自本次请求");
    assert_eq!(
        retried.helper_path.as_deref(),
        Some("~/.termx/bin/termx-helper")
    );
    assert_eq!(retried.helper_local_path.as_deref(), Some("./termx-helper"));
}

#[tokio::test]
async fn read_only_ssh_create_gets_deny_plan_and_is_not_blocked() {
    let dir = tempdir().expect("创建临时目录失败");
    let config = ServiceConfig::new(dir.path().to_path_buf()).with_remote_injection(
        RemoteInjectionConfig::new().with_consent(InjectionConsentMode::Allow),
    );
    let probe = Arc::new(ProbeFactory::default());
    let service =
        TestService::start_with_config_and_factory(config, Some(Arc::clone(&probe) as Arc<_>))
            .await;
    let mut client = connect(&service).await;
    handshake_root(&mut client, &service.token, "ses-inject").await;

    let created = call(
        &mut client,
        "inject-readonly",
        "terminal.create",
        remote_params("remote-readonly", "sh", true),
    )
    .await;
    assert!(created.ok, "只读模式不应阻止创建: {created:?}");

    let seen = probe.seen.lock().expect("probe 锁中毒");
    let ssh = seen[0].ssh.as_ref().expect("ssh");
    assert_eq!(
        ssh.injection.consent,
        InjectionConsentMode::Deny,
        "只读模式计划固定 deny"
    );
    assert!(!ssh.injection.allow);
    assert_eq!(ssh.helper_path, None, "只读模式不得覆盖请求路径");
}

#[tokio::test]
async fn missing_helper_local_uses_preinstalled_path_without_upload() {
    let dir = tempdir().expect("创建临时目录失败");
    let config = ServiceConfig::new(dir.path().to_path_buf()).with_remote_injection(
        RemoteInjectionConfig::new().with_consent(InjectionConsentMode::Allow),
    );
    let probe = Arc::new(ProbeFactory::default());
    let service =
        TestService::start_with_config_and_factory(config, Some(Arc::clone(&probe) as Arc<_>))
            .await;
    let mut client = connect(&service).await;
    handshake_root(&mut client, &service.token, "ses-inject").await;

    let created = call(
        &mut client,
        "inject-preinstalled",
        "terminal.create",
        remote_params("remote-preinstalled", "termx-pwsh", true),
    )
    .await;
    assert!(created.ok, "预装 helper 模式不应阻止创建: {created:?}");

    let seen = probe.seen.lock().expect("probe 锁中毒");
    let ssh = seen[0].ssh.as_ref().expect("ssh");
    assert!(!ssh.upload_helper, "没有 helper_local 时不得自动上传");
    assert!(ssh.helper_local_path.is_none());
    assert_eq!(
        ssh.helper_path.as_deref(),
        Some("~/.termx/bin/termx-helper"),
        "应回退到配置的远端预装路径"
    );
    assert_eq!(ssh.injection.consent, InjectionConsentMode::Deny);
    assert!(!ssh.injection.allow);
}

#[tokio::test]
async fn invalid_injection_path_returns_bad_request_without_spawning() {
    let dir = tempdir().expect("创建临时目录失败");
    let config = ServiceConfig::new(dir.path().to_path_buf()).with_remote_injection(
        RemoteInjectionConfig::new()
            .with_helper_path("relative/helper")
            .with_helper_local("./termx-helper"),
    );
    let probe = Arc::new(ProbeFactory::default());
    let service =
        TestService::start_with_config_and_factory(config, Some(Arc::clone(&probe) as Arc<_>))
            .await;
    let mut client = connect(&service).await;
    handshake_root(&mut client, &service.token, "ses-inject").await;

    let response = call(
        &mut client,
        "inject-invalid",
        "terminal.create",
        remote_params("remote-invalid", "termx-pwsh", true),
    )
    .await;
    assert!(!response.ok);
    assert_eq!(
        response.error.expect("缺少错误载荷").code,
        ErrorCode::BadRequest
    );
    assert!(
        probe.seen.lock().expect("probe 锁中毒").is_empty(),
        "路径非法时不得调用后端工厂"
    );
}

#[test]
fn probe_plan_default_matches_protocol_default() {
    // 防止协议默认值变化后测试悄悄失去意义。
    assert_eq!(InjectionPlan::default().consent, InjectionConsentMode::Ask);
    assert!(!InjectionPlan::default().allow);
}
