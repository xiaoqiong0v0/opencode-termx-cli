//! W1 Web 服务：HTTP 静态页、登录会话与 `/ws` 代理。
//!
//! 绑定策略由 [`crate::ServiceConfig`] 决定：
//! - `web_allow_remote = false`：只绑定 `127.0.0.1`，回环请求免登录；
//! - `web_allow_remote = true`：绑定 `0.0.0.0`，非回环请求必须携带有效会话。
//!
//! 浏览器只与本模块通信；内部 token 由代理在 `hello` 中注入，
//! 不会出现在返回给浏览器的任何帧里。

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Form, Router,
    body::Body,
    extract::{
        ConnectInfo, Path, State,
        ws::{Message as BrowserMessage, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use subtle::ConstantTimeEq;
use termx_protocol::{HelloParams, PROTOCOL_VERSION, Request};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Message as UpstreamMessage,
        protocol::{CloseFrame as UpstreamCloseFrame, frame::coding::CloseCode},
    },
};
use tracing::{debug, info};

use crate::config::{ServiceConfig, generate_token};

/// 浏览器会话 Cookie 名。
const SESSION_COOKIE: &str = "termx_web";
/// 代理发给内部服务的 `hello` 请求 ID。
const WEB_HELLO_ID: &str = "web-hello";
/// 代理 `hello` 使用的客户端标识。
const WEB_CLIENT_LABEL: &str = "termx-web";
/// 等待内部服务 `hello` 响应的超时。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// 默认关闭码（正常关闭）。
const NORMAL_CLOSE_CODE: u16 = 1000;
/// Web 登录会话有效期；过期后需重新登录。
const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);
/// 内存中最多保留的 Web 登录会话数；超出时淘汰最早创建的会话。
const MAX_WEB_SESSIONS: usize = 256;

/// 内嵌静态资源。
const INDEX_HTML: &str = include_str!("web_assets/index.html");
const LOGIN_HTML: &str = include_str!("web_assets/login.html");
const APP_JS: &str = include_str!("web_assets/app.js");
const APP_CSS: &str = include_str!("web_assets/app.css");
const XTERM_JS: &str = include_str!("web_assets/xterm.js");
const XTERM_CSS: &str = include_str!("web_assets/xterm.css");

/// Web 服务运行期错误。
#[derive(Debug, thiserror::Error)]
enum WebError {
    /// 与内部服务之间的 WebSocket 错误。
    #[error("上游 WebSocket 错误: {0}")]
    Upstream(#[from] tokio_tungstenite::tungstenite::Error),
    /// 与浏览器之间的 WebSocket 错误。
    #[error("浏览器 WebSocket 错误: {0}")]
    Browser(#[from] axum::Error),
    /// 协议 JSON 编解码失败。
    #[error("JSON 编解码失败: {0}")]
    Json(#[from] serde_json::Error),
    /// 等待内部服务 `hello` 响应超时。
    #[error("等待上游 hello 响应超时")]
    HandshakeTimeout,
    /// 内部服务在 `hello` 响应前关闭连接。
    #[error("上游连接在 hello 响应前关闭")]
    UpstreamClosed,
}

/// Web 请求共享状态。
#[derive(Clone)]
struct WebState {
    /// 内部 service 的环回地址。
    internal_addr: SocketAddr,
    /// 内部 service 的鉴权 token。
    internal_token: Arc<str>,
    /// 是否允许远程访问。
    allow_remote: bool,
    /// 登录密码；未配置时远程登录必失败。
    password: Option<Arc<str>>,
    /// 内存会话 token -> 创建时间；不持久化。
    sessions: Arc<Mutex<HashMap<String, Instant>>>,
}

impl WebState {
    /// 构造 Web 状态。
    fn new(
        internal_addr: SocketAddr,
        internal_token: Arc<str>,
        allow_remote: bool,
        password: Option<String>,
    ) -> Self {
        Self {
            internal_addr,
            internal_token,
            allow_remote,
            password: password.map(Arc::from),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// 已绑定端口的 Web 服务。
pub(crate) struct WebServer {
    listener: TcpListener,
    state: WebState,
    local_addr: SocketAddr,
}

impl WebServer {
    /// 按配置绑定 Web 监听地址。
    ///
    /// `allow_remote = false` 只绑定 `127.0.0.1`；否则绑定 `0.0.0.0`。
    pub(crate) async fn bind(
        config: &ServiceConfig,
        internal_addr: SocketAddr,
        internal_token: Arc<str>,
    ) -> std::io::Result<Self> {
        let addr = web_bind_addr(config.web_allow_remote, config.web_port);
        let state = WebState::new(
            internal_addr,
            internal_token,
            config.web_allow_remote,
            config.web_password.clone(),
        );
        Self::bind_on(addr, state).await
    }

    /// 在指定地址绑定；测试用 `127.0.0.1:0` 获取临时端口。
    async fn bind_on(addr: SocketAddr, state: WebState) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(Self {
            listener,
            state,
            local_addr,
        })
    }

    /// 实际监听地址（含 OS 分配的端口）。
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 运行 HTTP/WebSocket 服务，直到监听器出错或被任务 abort。
    pub(crate) async fn serve(self) -> std::io::Result<()> {
        let app = router(self.state);
        info!(addr = %self.local_addr, "termx web 服务启动");
        axum::serve(
            self.listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
    }
}

/// Web 监听地址：默认仅回环；允许远程时绑定 `0.0.0.0`。
fn web_bind_addr(allow_remote: bool, port: u16) -> SocketAddr {
    let ip = if allow_remote {
        Ipv4Addr::UNSPECIFIED
    } else {
        Ipv4Addr::LOCALHOST
    };
    SocketAddr::new(IpAddr::V4(ip), port)
}

/// 组装路由。
fn router(state: WebState) -> Router {
    Router::new()
        .route("/", get(handle_index))
        .route("/login", get(handle_login_page).post(handle_login))
        .route("/static/{*path}", get(handle_static))
        .route("/ws", get(handle_ws))
        .with_state(state)
}

/// `GET /`：回环直接返回首页；未授权访问跳转登录页。
async fn handle_index(
    State(state): State<WebState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if is_authorized(&state, peer, &headers) {
        static_response("text/html; charset=utf-8", INDEX_HTML)
    } else {
        redirect_to_login()
    }
}

/// `GET /login`：登录页。
async fn handle_login_page() -> Response {
    static_response("text/html; charset=utf-8", LOGIN_HTML)
}

/// 登录表单。
#[derive(Debug, Deserialize)]
struct LoginForm {
    /// 提交的密码。
    password: String,
}

/// 登录结果。
enum LoginOutcome {
    /// 登录成功，携带新会话 token。
    Success(String),
    /// 登录失败，携带可直接回显的说明。
    Failure(&'static str),
}

/// `POST /login`：校验密码并写入会话 Cookie。
async fn handle_login(
    State(state): State<WebState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Form(form): Form<LoginForm>,
) -> Response {
    match attempt_login(&state, peer, &form.password) {
        LoginOutcome::Success(token) => login_success_response(&token),
        LoginOutcome::Failure(message) => text_response(StatusCode::UNAUTHORIZED, message),
    }
}

/// `GET /static/*`：内嵌静态资源。
async fn handle_static(Path(path): Path<String>) -> Response {
    let asset = match path.as_str() {
        "index.html" => Some(("text/html; charset=utf-8", INDEX_HTML)),
        "login.html" => Some(("text/html; charset=utf-8", LOGIN_HTML)),
        "app.js" => Some(("text/javascript; charset=utf-8", APP_JS)),
        "app.css" => Some(("text/css; charset=utf-8", APP_CSS)),
        "xterm.js" => Some(("text/javascript; charset=utf-8", XTERM_JS)),
        "xterm.css" => Some(("text/css; charset=utf-8", XTERM_CSS)),
        _ => None,
    };

    match asset {
        Some((content_type, body)) => static_response(content_type, body),
        None => text_response(StatusCode::NOT_FOUND, "静态资源不存在"),
    }
}

/// `GET /ws`：校验同源后鉴权、升级并代理到内部 service。
async fn handle_ws(
    State(state): State<WebState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if !validate_ws_request(state.allow_remote, &headers) {
        return text_response(
            StatusCode::FORBIDDEN,
            "WebSocket 请求的 Host/Origin 不被允许",
        );
    }
    if !is_authorized(&state, peer, &headers) {
        return text_response(StatusCode::UNAUTHORIZED, "未登录或无权访问 WebSocket");
    }

    upgrade
        .on_upgrade(move |socket| async move {
            if let Err(error) = proxy_websocket(socket, state).await {
                debug!(peer = %peer, %error, "Web WebSocket 代理结束");
            }
        })
        .into_response()
}

/// 尝试登录；密码比较为常量时间。
fn attempt_login(state: &WebState, peer: SocketAddr, provided: &str) -> LoginOutcome {
    if !is_loopback(peer) && !state.allow_remote {
        return LoginOutcome::Failure("远程访问未开启，请通过回环地址访问");
    }

    let Some(expected) = state.password.as_deref() else {
        return LoginOutcome::Failure("未配置 Web 密码（环境变量 TERMX_WEB_PASSWORD）");
    };

    if !secrets_match(expected, provided) {
        return LoginOutcome::Failure("密码错误");
    }

    let token = generate_token();
    let now = Instant::now();
    let mut sessions = state
        .sessions
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    sessions.retain(|_, created| now.saturating_duration_since(*created) < SESSION_TTL);
    if sessions.len() >= MAX_WEB_SESSIONS {
        if let Some(oldest) = sessions
            .iter()
            .min_by_key(|(_, created)| **created)
            .map(|(token, _)| token.clone())
        {
            sessions.remove(&oldest);
        }
    }
    sessions.insert(token.clone(), now);
    drop(sessions);
    info!(peer = %peer, "Web 登录成功");
    LoginOutcome::Success(token)
}

/// 常量时间比较两个字符串；长度不同直接失败。
fn secrets_match(expected: &str, provided: &str) -> bool {
    let expected = expected.as_bytes();
    let provided = provided.as_bytes();
    if expected.len() != provided.len() {
        return false;
    }
    expected.ct_eq(provided).into()
}

/// 判断请求是否已授权。
///
/// 回环请求一律放行；非回环必须开启远程访问、配置密码并携带有效会话。
fn is_authorized(state: &WebState, peer: SocketAddr, headers: &HeaderMap) -> bool {
    if is_loopback(peer) {
        return true;
    }
    if !state.allow_remote || state.password.is_none() {
        return false;
    }
    let Some(token) = session_token(headers) else {
        return false;
    };
    let now = Instant::now();
    let mut sessions = state
        .sessions
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    sessions.retain(|_, created| now.saturating_duration_since(*created) < SESSION_TTL);
    sessions.contains_key(&token)
}

/// 从 `Cookie` 头读取会话 token。
fn session_token(headers: &HeaderMap) -> Option<String> {
    let prefix = format!("{SESSION_COOKIE}=");
    for value in headers.get_all(header::COOKIE) {
        let Ok(text) = value.to_str() else {
            continue;
        };
        for part in text.split(';') {
            let part = part.trim();
            if let Some(token) = part.strip_prefix(&prefix) {
                if !token.is_empty() {
                    return Some(token.to_string());
                }
            }
        }
    }
    None
}

/// 构造登录成功响应：写入 HttpOnly 会话 Cookie 并跳转首页。
fn login_success_response(token: &str) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::SEE_OTHER;
    let cookie = format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/");
    if let Ok(value) = HeaderValue::from_str(&cookie) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
        .headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("/"));
    response
}

/// 302 跳转到登录页。
fn redirect_to_login() -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::FOUND;
    response
        .headers_mut()
        .insert(header::LOCATION, HeaderValue::from_static("/login"));
    response
}

/// 带 Content-Type 的静态文本响应。
fn static_response(content_type: &'static str, body: &'static str) -> Response {
    let mut response = Response::new(Body::from(body));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    // 内嵌资源随 service 二进制更新；禁止浏览器缓存，避免升级后仍用旧 UI。
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// 纯文本响应（错误说明）。
fn text_response(status: StatusCode, message: &'static str) -> Response {
    let mut response = Response::new(Body::from(message));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// 是否为环回地址。
fn is_loopback(addr: SocketAddr) -> bool {
    addr.ip().is_loopback()
}

/// 校验 `/ws` 升级请求的 Host/Origin，防止浏览器跨站 WebSocket 劫持（CSWSH）。
///
/// - `allow_remote = false`：Host 必须是回环主机；带 Origin 时其 host 必须与
///   Host 一致，无 Origin 的非浏览器请求可放行；
/// - `allow_remote = true`：Origin 必须存在且 host 与 Host 一致。
fn validate_ws_request(allow_remote: bool, headers: &HeaderMap) -> bool {
    let Some(host) = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_authority_host)
    else {
        return false;
    };
    if !allow_remote && !is_loopback_host(&host) {
        return false;
    }
    match headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    {
        Some(origin) => parse_origin_host(origin)
            .is_some_and(|origin_host| origin_host.eq_ignore_ascii_case(&host)),
        None => !allow_remote,
    }
}

/// 解析 `Host` 头中的主机名（去掉端口与 IPv6 方括号）；非法输入返回 `None`。
fn parse_authority_host(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty()
        || value
            .chars()
            .any(|ch| matches!(ch, '/' | '?' | '#' | '@' | ','))
    {
        return None;
    }

    let (host, is_ipv6) = if let Some(rest) = value.strip_prefix('[') {
        // IPv6 字面量：`[::1]` 或 `[::1]:8123`。
        let end = rest.find(']')?;
        let host = &rest[..end];
        let suffix = &rest[end + 1..];
        if !suffix.is_empty() {
            let port = suffix.strip_prefix(':')?;
            if !is_valid_port(port) {
                return None;
            }
        }
        (host, true)
    } else if let Some((host, port)) = value.rsplit_once(':') {
        if !is_valid_port(port) {
            return None;
        }
        (host, false)
    } else {
        (value, false)
    };

    if host.is_empty() || (!is_ipv6 && host.contains(':')) {
        return None;
    }
    Some(host.to_string())
}

/// 是否为合法端口（非空纯数字）。
fn is_valid_port(port: &str) -> bool {
    !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit())
}

/// 解析 `Origin` 头中的主机名；`null`、多来源或非法格式返回 `None`。
fn parse_origin_host(value: &str) -> Option<String> {
    let value = value.trim();
    let (scheme, rest) = value.split_once("://")?;
    if scheme.is_empty()
        || !scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
    {
        return None;
    }
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    parse_authority_host(&rest[..end])
}

/// 是否为本机回环主机（与 `host` 的 Host 白名单保持一致）。
fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1" || host == "::1"
}

/// 代理浏览器与内部 service 之间的 WebSocket。
async fn proxy_websocket(socket: WebSocket, state: WebState) -> Result<(), WebError> {
    let url = internal_ws_url(state.internal_addr);
    let (upstream, _) = connect_async(url.as_str()).await?;
    let (mut upstream_tx, mut upstream_rx) = upstream.split();

    upstream_tx
        .send(UpstreamMessage::Text(build_hello(&state)?.into()))
        .await?;

    // 先转发内部服务的 hello 响应，浏览器随后才能看到 termx/1 握手结果。
    let first = tokio::time::timeout(HANDSHAKE_TIMEOUT, upstream_rx.next())
        .await
        .map_err(|_| WebError::HandshakeTimeout)?
        .ok_or(WebError::UpstreamClosed)??;

    let (mut browser_tx, browser_rx) = socket.split();
    let (first, close) = upstream_to_browser(first);
    browser_tx.send(first).await?;
    if close {
        let _ = browser_tx.send(BrowserMessage::Close(None)).await;
        return Ok(());
    }

    let to_browser = pump_upstream(upstream_rx, browser_tx);
    let to_upstream = pump_browser(browser_rx, upstream_tx);
    tokio::select! {
        result = to_browser => result,
        result = to_upstream => result,
    }
}

/// 构造代理使用的 `hello` 请求文本；token 只存在于服务端内存。
fn build_hello(state: &WebState) -> Result<String, WebError> {
    let request = Request {
        id: WEB_HELLO_ID.to_string(),
        method: "hello".to_string(),
        params: serde_json::to_value(HelloParams {
            proto: PROTOCOL_VERSION.to_string(),
            token: state.internal_token.to_string(),
            client: WEB_CLIENT_LABEL.to_string(),
            root_session_id: None,
            admin: true,
        })?,
    };
    Ok(serde_json::to_string(&request)?)
}

/// 浏览器连接拆出的读端。
type BrowserStream = futures_util::stream::SplitStream<WebSocket>;
/// 浏览器连接拆出的写端。
type BrowserSink = futures_util::stream::SplitSink<WebSocket, BrowserMessage>;
/// 上游连接拆出的读端。
type UpstreamStream = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
/// 上游连接拆出的写端。
type UpstreamSink =
    futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, UpstreamMessage>;

/// 上游 → 浏览器：转发文本/二进制/控制帧；关闭时关闭浏览器侧。
async fn pump_upstream(mut stream: UpstreamStream, mut sink: BrowserSink) -> Result<(), WebError> {
    while let Some(message) = stream.next().await {
        let (message, close) = upstream_to_browser(message?);
        sink.send(message).await?;
        if close {
            return Ok(());
        }
    }
    let _ = sink.send(BrowserMessage::Close(None)).await;
    Ok(())
}

/// 浏览器 → 上游：转发文本/二进制/控制帧；关闭时关闭上游侧。
async fn pump_browser(mut stream: BrowserStream, mut sink: UpstreamSink) -> Result<(), WebError> {
    while let Some(message) = stream.next().await {
        let (message, close) = browser_to_upstream(message?);
        sink.send(message).await?;
        if close {
            return Ok(());
        }
    }
    let _ = sink.send(UpstreamMessage::Close(None)).await;
    Ok(())
}

/// 把内部服务消息转换为浏览器消息；返回是否需要结束代理。
fn upstream_to_browser(message: UpstreamMessage) -> (BrowserMessage, bool) {
    match message {
        UpstreamMessage::Text(text) => (BrowserMessage::Text(text.to_string().into()), false),
        UpstreamMessage::Binary(bytes) => (BrowserMessage::Binary(bytes.to_vec().into()), false),
        UpstreamMessage::Ping(bytes) => (BrowserMessage::Ping(bytes.to_vec().into()), false),
        UpstreamMessage::Pong(bytes) => (BrowserMessage::Pong(bytes.to_vec().into()), false),
        UpstreamMessage::Close(frame) => {
            let (code, reason) = frame.map_or((NORMAL_CLOSE_CODE, String::new()), |frame| {
                (u16::from(frame.code), frame.reason.to_string())
            });
            (close_message(code, reason), true)
        }
        // Frame 是底层原始帧，协议层不使用。
        UpstreamMessage::Frame(_) => (BrowserMessage::Close(None), true),
    }
}

/// 把浏览器消息转换为内部服务消息；返回是否需要结束代理。
fn browser_to_upstream(message: BrowserMessage) -> (UpstreamMessage, bool) {
    match message {
        BrowserMessage::Text(text) => (UpstreamMessage::Text(text.to_string().into()), false),
        BrowserMessage::Binary(bytes) => (UpstreamMessage::Binary(bytes.to_vec().into()), false),
        BrowserMessage::Ping(bytes) => (UpstreamMessage::Ping(bytes.to_vec().into()), false),
        BrowserMessage::Pong(bytes) => (UpstreamMessage::Pong(bytes.to_vec().into()), false),
        BrowserMessage::Close(frame) => {
            let (code, reason) = frame.map_or((NORMAL_CLOSE_CODE, String::new()), |frame| {
                (frame.code, frame.reason.to_string())
            });
            (upstream_close_message(code, reason), true)
        }
    }
}

/// 构造发往浏览器的关闭帧。
fn close_message(code: u16, reason: String) -> BrowserMessage {
    BrowserMessage::Close(Some(axum::extract::ws::CloseFrame {
        code,
        reason: reason.into(),
    }))
}

/// 构造发往内部服务的关闭帧。
fn upstream_close_message(code: u16, reason: String) -> UpstreamMessage {
    UpstreamMessage::Close(Some(UpstreamCloseFrame {
        code: CloseCode::from(code),
        reason: reason.into(),
    }))
}

/// 内部 service 的环回 WebSocket URL。
fn internal_ws_url(addr: SocketAddr) -> String {
    match addr {
        SocketAddr::V4(addr) => format!("ws://{addr}"),
        SocketAddr::V6(addr) => format!("ws://[{addr}]"),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read as _, Write as _},
        net::IpAddr,
        sync::Arc,
        time::Duration,
    };

    use futures_util::{SinkExt, StreamExt};
    use tempfile::tempdir;
    use termx_core::MockFactory;
    use termx_protocol::{PROTOCOL_VERSION, Response as ProtocolResponse, WEB_ROOT_SESSION_ID};
    use tokio::{
        net::TcpStream,
        task::JoinHandle,
        time::{sleep, timeout},
    };
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    use super::*;
    use crate::server::Service;

    /// 远程测试地址（非回环）。
    fn remote_peer() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 45000)
    }

    /// 环回测试地址。
    fn loopback_peer() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45001)
    }

    /// 构造带会话 Cookie 的请求头。
    fn headers_with_session(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("{SESSION_COOKIE}={token}")).expect("测试 Cookie 合法"),
        );
        headers
    }

    /// 等待 Web 服务可连接；避免测试在 accept 就绪前发起请求。
    async fn wait_ready(addr: SocketAddr) {
        for _ in 0..50 {
            if TcpStream::connect(addr).await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("Web 服务未在预期时间内就绪: {addr}");
    }

    /// 发送原始 HTTP 请求并读取完整响应。
    fn http_request(addr: SocketAddr, request: &str) -> (u16, String) {
        let mut stream = std::net::TcpStream::connect(addr).expect("连接 Web 服务失败");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("设置读超时失败");
        stream
            .write_all(request.as_bytes())
            .expect("写入 HTTP 请求失败");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("读取 HTTP 响应失败");
        let response = String::from_utf8_lossy(&raw).into_owned();
        let status = response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .expect("响应缺少状态码");
        (status, response)
    }

    /// 启动一个仅回环的 Web 服务（无内部 service）。
    async fn start_loopback_web() -> (SocketAddr, JoinHandle<()>) {
        let state = WebState::new(loopback_peer(), Arc::from("unused-token"), false, None);
        let server = WebServer::bind_on(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0), state)
            .await
            .expect("绑定 Web 测试端口失败");
        let addr = server.local_addr();
        let handle = tokio::spawn(async move {
            server.serve().await.expect("Web 服务异常退出");
        });
        wait_ready(addr).await;
        (addr, handle)
    }

    #[test]
    fn web_bind_address_follows_allow_remote() {
        assert_eq!(
            web_bind_addr(false, 8138),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8138)
        );
        assert_eq!(
            web_bind_addr(true, 8138),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8138)
        );
    }

    #[test]
    fn secrets_match_only_when_equal() {
        assert!(secrets_match("correct", "correct"));
        assert!(!secrets_match("correct", "incorrect"));
        assert!(!secrets_match("correct", ""));
        assert!(!secrets_match("", "x"));
    }

    #[test]
    fn loopback_is_always_authorized() {
        let state = WebState::new(loopback_peer(), Arc::from("t"), false, None);
        assert!(is_authorized(&state, loopback_peer(), &HeaderMap::new()));
    }

    /// 构造带 Host/Origin 的请求头。
    fn ws_headers(host: Option<&str>, origin: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(host) = host {
            headers.insert(
                header::HOST,
                HeaderValue::from_str(host).expect("测试 Host 合法"),
            );
        }
        if let Some(origin) = origin {
            headers.insert(
                header::ORIGIN,
                HeaderValue::from_str(origin).expect("测试 Origin 合法"),
            );
        }
        headers
    }

    #[test]
    fn ws_rejects_cross_origin() {
        let headers = ws_headers(Some("127.0.0.1:8138"), Some("http://evil.example"));
        assert!(!validate_ws_request(false, &headers));
    }

    #[test]
    fn ws_rejects_non_loopback_host_locally() {
        let headers = ws_headers(Some("192.168.1.10:8138"), None);
        assert!(!validate_ws_request(false, &headers));
        let headers = ws_headers(Some("192.168.1.10:8138"), Some("http://192.168.1.10:8138"));
        assert!(!validate_ws_request(false, &headers));
    }

    #[test]
    fn ws_allows_same_origin_case_insensitively() {
        let headers = ws_headers(Some("localhost:8138"), Some("http://LOCALHOST:8138"));
        assert!(validate_ws_request(false, &headers));
        let headers = ws_headers(Some("127.0.0.1:8138"), Some("http://127.0.0.1:8138/"));
        assert!(validate_ws_request(false, &headers));
    }

    #[test]
    fn ws_allows_missing_origin_on_loopback() {
        let headers = ws_headers(Some("127.0.0.1:8138"), None);
        assert!(validate_ws_request(false, &headers));
        let headers = ws_headers(Some("localhost"), None);
        assert!(validate_ws_request(false, &headers));
    }

    #[test]
    fn ws_rejects_missing_origin_when_remote() {
        let headers = ws_headers(Some("192.168.1.10:8138"), None);
        assert!(!validate_ws_request(true, &headers));
        let headers = ws_headers(Some("192.168.1.10:8138"), Some("http://192.168.1.10:8138"));
        assert!(validate_ws_request(true, &headers));
    }

    #[test]
    fn ws_handles_ipv6_loopback_and_malformed_headers() {
        let headers = ws_headers(Some("[::1]:8138"), Some("http://[::1]:8138"));
        assert!(validate_ws_request(false, &headers));

        // 非法 / 缺失 Host 不应 panic，且一律拒绝。
        for headers in [
            ws_headers(Some("[::1"), None),
            ws_headers(Some(""), None),
            ws_headers(Some("127.0.0.1:not-a-port"), None),
            ws_headers(None, None),
        ] {
            assert!(!validate_ws_request(false, &headers));
        }
        // `Origin: null` 无 host，必须拒绝。
        let headers = ws_headers(Some("127.0.0.1:8138"), Some("null"));
        assert!(!validate_ws_request(false, &headers));
    }

    #[test]
    fn remote_without_cookie_is_rejected() {
        let state = WebState::new(
            loopback_peer(),
            Arc::from("t"),
            true,
            Some("pw".to_string()),
        );
        assert!(!is_authorized(&state, remote_peer(), &HeaderMap::new()));
        // 有 cookie 但不在会话集合中同样拒绝。
        assert!(!is_authorized(
            &state,
            remote_peer(),
            &headers_with_session("unknown")
        ));
    }

    #[test]
    fn remote_with_valid_session_is_authorized() {
        let state = WebState::new(
            loopback_peer(),
            Arc::from("t"),
            true,
            Some("pw".to_string()),
        );
        let token = match attempt_login(&state, remote_peer(), "pw") {
            LoginOutcome::Success(token) => token,
            LoginOutcome::Failure(message) => panic!("登录本应成功: {message}"),
        };
        assert!(is_authorized(
            &state,
            remote_peer(),
            &headers_with_session(&token)
        ));
        assert!(!is_authorized(
            &state,
            remote_peer(),
            &headers_with_session("other")
        ));
    }

    #[test]
    fn expired_session_is_rejected() {
        let state = WebState::new(
            loopback_peer(),
            Arc::from("t"),
            true,
            Some("pw".to_string()),
        );
        let token = "expired-token";
        state.sessions.lock().unwrap().insert(
            token.to_string(),
            Instant::now() - SESSION_TTL - Duration::from_secs(1),
        );
        assert!(!is_authorized(
            &state,
            remote_peer(),
            &headers_with_session(token)
        ));
    }

    #[test]
    fn remote_login_without_password_fails() {
        let state = WebState::new(loopback_peer(), Arc::from("t"), true, None);
        match attempt_login(&state, remote_peer(), "anything") {
            LoginOutcome::Failure(message) => {
                assert!(
                    message.contains("TERMX_WEB_PASSWORD"),
                    "应给出明确提示: {message}"
                );
            }
            LoginOutcome::Success(_) => panic!("未配置密码时登录必须失败"),
        }
        // 未开启远程访问时，远程登录同样失败。
        let local_only = WebState::new(
            loopback_peer(),
            Arc::from("t"),
            false,
            Some("pw".to_string()),
        );
        assert!(matches!(
            attempt_login(&local_only, remote_peer(), "pw"),
            LoginOutcome::Failure(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn loopback_http_serves_index_static_and_404() {
        let (addr, handle) = start_loopback_web().await;

        let (status, response) = http_request(
            addr,
            &format!("GET / HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert_eq!(status, 200);
        assert!(response.contains("termx"));

        // W2 页面：header 控件 + 全高终端容器，并引用 xterm 与 app 资源。
        let (status, index) = http_request(
            addr,
            &format!("GET / HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert_eq!(status, 200);
        for marker in [
            "termx-session",
            "termx-terminal",
            "termx-info-meta",
            "termx-create-dialog",
            "termx-create-root",
            "termx-create-history",
            "termx-refresh",
            "termx-terminate",
            "termx-term",
        ] {
            assert!(index.contains(marker), "首页缺少 {marker}");
        }
        assert!(index.contains("/static/xterm.js"), "首页应引入 xterm.js");

        for (path, marker) in [
            ("/static/app.css", "termx-header"),
            ("/static/app.css", "termx-login"),
            ("/static/app.js", "terminal.list"),
            ("/static/app.js", "terminal.create"),
            ("/static/app.js", "ssh.history.list"),
            ("/static/app.js", "session.list"),
            ("/static/app.js", WEB_ROOT_SESSION_ID),
            ("/static/app.js", "terminal.send"),
            ("/static/app.js", "\"subscribe\""),
            ("/static/app.js", WEB_HELLO_ID),
            ("/static/xterm.css", ".xterm"),
        ] {
            let (status, response) = http_request(
                addr,
                &format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
            );
            assert_eq!(status, 200, "静态资源 {path} 应返回 200");
            assert!(response.contains(marker), "静态资源 {path} 内容不符合预期");
        }

        // 真实 xterm.js 构建（UMD，浏览器全局 Terminal）而非占位脚本。
        let (status, xterm) = http_request(
            addr,
            &format!("GET /static/xterm.js HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert_eq!(status, 200);
        assert!(xterm.contains("Terminal"), "xterm.js 应暴露 Terminal");
        assert!(
            xterm.len() > 200 * 1024,
            "xterm.js 应为完整构建，实际 {} 字节",
            xterm.len()
        );

        let (status, _) = http_request(
            addr,
            &format!(
                "GET /static/unknown.js HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert_eq!(status, 404);

        let (status, _) = http_request(
            addr,
            &format!("GET /nope HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        );
        assert_eq!(status, 404);

        handle.abort();
    }

    #[tokio::test]
    async fn websocket_proxy_forwards_hello_and_rpc() {
        let dir = tempdir().expect("创建临时目录失败");
        let service = Service::bind_with_factory(
            ServiceConfig::new(dir.path().to_path_buf()).with_web_port(0),
            Arc::new(MockFactory),
        )
        .await
        .expect("内部 service 绑定失败");
        let internal_addr = service.local_addr();
        let internal_token: Arc<str> = Arc::from(
            fs::read_to_string(service.config().token_path())
                .expect("token 文件缺失")
                .trim(),
        );
        let service_task = tokio::spawn(service.run());

        let state = WebState::new(internal_addr, internal_token, false, None);
        let server = WebServer::bind_on(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0), state)
            .await
            .expect("绑定 Web 测试端口失败");
        let web_addr = server.local_addr();
        let web_task = tokio::spawn(async move {
            server.serve().await.expect("Web 服务异常退出");
        });
        wait_ready(web_addr).await;

        let (mut client, _) = connect_async(format!("ws://{web_addr}/ws"))
            .await
            .expect("连接 WebSocket 代理失败");

        let hello = next_text(&mut client).await;
        let hello: ProtocolResponse = termx_protocol::from_json(&hello).expect("hello 响应非法");
        assert!(hello.ok);
        assert_eq!(hello.id, WEB_HELLO_ID);
        assert!(
            !format!("{hello:?}").contains("token"),
            "hello 响应不得包含内部 token"
        );
        let result = hello.result.as_ref().expect("hello 应带结果");
        assert_eq!(result["proto"], PROTOCOL_VERSION);

        client
            .send(Message::Text(
                r#"{"id":"list-1","method":"terminal.list","params":{}}"#.into(),
            ))
            .await
            .expect("发送 terminal.list 失败");
        let list = next_text(&mut client).await;
        let list: ProtocolResponse =
            termx_protocol::from_json(&list).expect("terminal.list 响应非法");
        assert!(list.ok);
        assert_eq!(list.id, "list-1");

        client.close(None).await.expect("关闭客户端失败");
        timeout(Duration::from_secs(2), async {
            while let Some(message) = client.next().await {
                if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                    return;
                }
            }
        })
        .await
        .expect("代理未在预期时间内关闭连接");

        web_task.abort();
        service_task.abort();
    }

    /// 读取下一条文本帧（忽略控制帧）。
    async fn next_text(client: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) -> String {
        loop {
            let message = timeout(Duration::from_secs(5), client.next())
                .await
                .expect("等待文本帧超时")
                .expect("连接已关闭")
                .expect("读取帧失败");
            match message {
                Message::Text(text) => return text.to_string(),
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("期望文本帧，实际为 {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn web_port_zero_does_not_bind() {
        let dir = tempdir().expect("创建临时目录失败");
        let service = Service::bind_with_factory(
            ServiceConfig::new(dir.path().to_path_buf()).with_web_port(0),
            Arc::new(MockFactory),
        )
        .await
        .expect("服务绑定失败");
        assert!(
            service.web_local_addr().is_none(),
            "web_port=0 不应绑定 Web 端口"
        );
    }
}
