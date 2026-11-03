//! 并发配对、事件与响应互不阻塞、断开后 pending 收尾。

mod common;

use std::{sync::Arc, time::Duration};

use common::{TestService, locator, next_event_matching, pty_params};
use futures_util::future::join_all;
use termx_client::{Client, ClientError};
use termx_protocol::{ListParams, StatusParams, SubscribeParams, TerminateParams};
use tokio::time::{sleep, timeout};

/// 多个请求并发时按 `id` 正确配对；事件广播不被请求处理阻塞。
#[tokio::test]
async fn concurrent_requests_pair_by_id_and_events_flow() {
    let dir = tempfile::tempdir().expect("临时目录");
    let service = TestService::start(dir.path()).await;
    let client = Client::connect(service.client_config())
        .await
        .expect("连接并握手应成功");

    let mut events = client.events();
    client
        .subscribe(SubscribeParams {
            terminals: None,
            all: Some(true),
        })
        .await
        .expect("订阅全部终端应成功");

    let names = ["alpha", "beta", "gamma", "delta"];
    let creates = names
        .iter()
        .map(|name| client.create(pty_params("ses-concurrent", name)))
        .collect::<Vec<_>>();

    // 请求进行期间同时读事件：两者由 reader 任务分别投递，互不阻塞。
    let (created, first_event) = tokio::join!(
        join_all(creates),
        next_event_matching(&mut events, |_| true)
    );
    let created = created
        .into_iter()
        .map(|result| result.expect("并发 create 应成功"))
        .collect::<Vec<_>>();
    assert!(
        created
            .iter()
            .any(|item| item.terminal_id == first_event.terminal_id),
        "事件应来自本次创建的终端，实际 terminal_id={}",
        first_event.terminal_id
    );

    // 结果与请求一一配对：按 name 查 list，terminal_id 必须与对应 create 结果一致。
    let terminals = client
        .list(ListParams::default())
        .await
        .expect("list 应成功");
    for (name, created) in names.iter().zip(&created) {
        let matched = terminals
            .iter()
            .find(|item| &item.name == name)
            .unwrap_or_else(|| panic!("list 缺少终端 {name}"));
        assert_eq!(
            matched.terminal_id, created.terminal_id,
            "create 响应与 name 配对错误"
        );
    }

    // 每个 status 返回自身终端 ID，验证乱序响应仍按 id 配对。
    let ids = created
        .iter()
        .map(|item| item.terminal_id.clone())
        .collect::<Vec<_>>();
    let statuses = join_all(ids.iter().map(|id| {
        client.status(StatusParams {
            locator: locator(id),
        })
    }))
    .await;
    for (id, status) in ids.iter().zip(statuses) {
        let status = status.expect("status 应成功");
        assert_eq!(status.snapshot.terminal_id, *id, "status 响应配对错误");
    }

    for id in &ids {
        client
            .terminate(TerminateParams {
                locator: locator(id),
                force: false,
            })
            .await
            .expect("terminate 应成功");
    }
    client.close().await;
}

/// 连接收尾时，尚未完成的请求返回 `Closed`。
#[tokio::test]
async fn pending_request_returns_closed_on_disconnect() {
    let dir = tempfile::tempdir().expect("临时目录");
    let mut service = TestService::start(dir.path()).await;
    let client = Client::connect(service.client_config())
        .await
        .expect("连接并握手应成功");

    let created = client
        .create(pty_params("ses-closed", "pending"))
        .await
        .expect("create 应成功");

    // 用一个后台线程持住 registry 锁，让 status 请求停在服务端；
    // 避免在 async 测试里跨 await 持有 std MutexGuard。
    let registry = Arc::clone(&service.registry);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _guard = registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ready_tx.send(()).expect("发送锁就绪");
        let _ = release_rx.recv();
    });
    ready_rx
        .recv_timeout(common::WAIT)
        .expect("等待 registry 锁就绪");

    let pending = client.status(StatusParams {
        locator: locator(&created.terminal_id),
    });
    let disconnect = async {
        sleep(Duration::from_millis(150)).await;
        client.close().await;
    };
    let joined = timeout(common::WAIT, async { tokio::join!(pending, disconnect) }).await;

    // 先释放服务端阻塞，再断言，避免断言失败时锁持有线程悬挂。
    release_tx.send(()).expect("释放 registry 锁");
    holder.join().expect("锁持有线程不应 panic");
    let (pending, ()) = joined.expect("等待客户端收尾超时");

    assert!(
        matches!(pending, Err(ClientError::Closed)),
        "断开后 pending 应返回 Closed，实际: {pending:?}"
    );

    service.shutdown().await;
}

/// 服务端不响应时，`request_timeout` 到期返回 `Timeout` 并清理 pending。
#[tokio::test]
async fn request_returns_timeout_when_server_stalls() {
    let dir = tempfile::tempdir().expect("临时目录");
    let mut service = TestService::start(dir.path()).await;
    let client = Client::connect(
        service
            .client_config()
            .with_request_timeout(Duration::from_millis(150)),
    )
    .await
    .expect("连接并握手应成功");

    let created = client
        .create(pty_params("ses-timeout", "timeout"))
        .await
        .expect("create 应成功");

    // 持住 registry 锁，让 status 停在服务端无法返回。
    let registry = Arc::clone(&service.registry);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _guard = registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ready_tx.send(()).expect("发送锁就绪");
        let _ = release_rx.recv();
    });
    ready_rx
        .recv_timeout(common::WAIT)
        .expect("等待 registry 锁就绪");

    let status = client.status(StatusParams {
        locator: locator(&created.terminal_id),
    });
    let outcome = timeout(common::WAIT, status).await;

    // 先释放服务端阻塞，再断言，避免失败时锁持有线程悬挂。
    release_tx.send(()).expect("释放 registry 锁");
    holder.join().expect("锁持有线程不应 panic");

    let error = outcome
        .expect("等待客户端超时不应超过测试上限")
        .expect_err("服务端阻塞时请求应超时");
    assert!(
        matches!(error, ClientError::Timeout),
        "应返回 Timeout，实际: {error:?}"
    );

    // 超时后连接仍可继续使用（pending 已清理）。
    let terminals = client
        .list(ListParams::default())
        .await
        .expect("超时不应破坏连接");
    assert!(
        terminals
            .iter()
            .any(|item| item.terminal_id == created.terminal_id)
    );

    client.close().await;
    service.shutdown().await;
}
