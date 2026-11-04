// W2: xterm.js Web 终端。浏览器只与本页面的 /ws 代理通信，内部 token 不下发。
(function () {
  "use strict";

  var RECONNECT_MIN_MS = 500;
  var RECONNECT_MAX_MS = 10000;
  var RPC_TIMEOUT_MS = 15000;
  var RESIZE_DEBOUNCE_MS = 120;
  var FALLBACK_FONT_SIZE = 14;
  var WEB_HELLO_ID = "web-hello";
  // Web 未绑定终端的保留会话 ID；服务端对其豁免无会话超时。
  var WEB_ROOT_SESSION_ID = "@web";
  var WEB_ROOT_LABEL = "其他/未绑定";
  // 新建终端表单的默认尺寸与服务端默认值一致。
  var DEFAULT_COLS = 120;
  var DEFAULT_ROWS = 30;
  // 历史必须用 read 分段拉取；subscribe 只回放 read 末尾到订阅时刻之间的小段事件。
  var READ_MAX_BYTES = 64 * 1024;
  var READ_MAX_ROUNDS = 50;
  // 信息页轮询间隔与单次返回的命令条数上限（与服务端默认容量一致）。
  var TRANSCRIPT_POLL_MS = 500;
  var TRANSCRIPT_PAGE_SIZE = 200;
  // 信息页 DOM 只保留最近的文本，避免长时间运行无限增长（服务端仍有界）。
  var TRANSCRIPT_UI_MAX_CHARS = 200000;

  var elements = {};
  var term = null;
  var socket = null;
  var connected = false;
  var pending = Object.create(null);
  var nextRequestId = 1;
  var currentTerminalId = null;
  // 每终端已渲染/已消费的最后事件 seq；重连复用它避免从 0 重放。
  var cursors = Object.create(null);
  // 订阅窗口内先缓存事件，等 subscribe 响应确认后再按 seq 顺序渲染。
  var liveBuffer = null;
  var liveBufferToken = 0;
  // 历史读取/回放被重置时保留提示，避免被随后的“已订阅终端”覆盖。
  var historyNotice = null;
  // 每次选中递增，用于丢弃前一次异步读取的迟到回调。
  var selectToken = 0;
  var claimed = false;
  var claimInFlight = null;
  var lastSize = null;
  var resizeTimer = null;
  var reconnectTimer = null;
  var reconnectDelay = RECONNECT_MIN_MS;
  // 最近一次 terminal.list 结果，用于标记当前终端所属 root。
  var lastTerminals = [];
  // 最近一次 session.list 结果（已含 @web），用于两级选择与新建表单。
  var lastSessions = [];
  // 当前选中的 opencode 会话（两级选择的第一级）。
  var selectedSession = null;
  // 迁移请求在飞时禁用弹窗提交/取消。
  var rebindBusy = false;
  // 新建请求在飞时禁用弹窗提交/取消。
  var createBusy = false;
  // 当前视图：默认信息页；raw 保持现有 xterm。
  var viewMode = "info";
  // 最近一次渲染的 transcript 修订号；null 表示尚未渲染。
  var transcriptRevision = null;
  var transcriptTimer = null;
  var transcriptInFlight = false;
  var transcriptNotice = "";
  // 终端进入终态后信息页输入禁用并显示提示。
  var terminalEnded = false;

  /** 初始化页面：创建终端、绑定控件、连接代理。 */
  function init() {
    elements.term = document.getElementById("termx-term");
    elements.sessionSelect = document.getElementById("termx-session");
    elements.select = document.getElementById("termx-terminal");
    elements.refresh = document.getElementById("termx-refresh");
    elements.createOpen = document.getElementById("termx-create-open");
    elements.createDialog = document.getElementById("termx-create-dialog");
    elements.createForm = document.getElementById("termx-create-form");
    elements.createRoot = document.getElementById("termx-create-root");
    elements.createName = document.getElementById("termx-create-name");
    elements.createType = document.getElementById("termx-create-type");
    elements.createLocal = document.getElementById("termx-create-local");
    elements.createSsh = document.getElementById("termx-create-ssh");
    elements.createCmd = document.getElementById("termx-create-cmd");
    elements.createArgs = document.getElementById("termx-create-args");
    elements.createCwd = document.getElementById("termx-create-cwd");
    elements.createEnv = document.getElementById("termx-create-env");
    elements.createHost = document.getElementById("termx-create-host");
    elements.createPort = document.getElementById("termx-create-port");
    elements.createUser = document.getElementById("termx-create-user");
    elements.createAuth = document.getElementById("termx-create-auth");
    elements.createPasswordGroup = document.getElementById("termx-create-password-group");
    elements.createPassword = document.getElementById("termx-create-password");
    elements.createKeyGroup = document.getElementById("termx-create-key-group");
    elements.createKeyPath = document.getElementById("termx-create-key-path");
    elements.createKeyPassphrase = document.getElementById("termx-create-key-passphrase");
    elements.createSshCmd = document.getElementById("termx-create-ssh-cmd");
    elements.createHelperPath = document.getElementById("termx-create-helper-path");
    elements.createUploadHelper = document.getElementById("termx-create-upload-helper");
    elements.createUploadGroup = document.getElementById("termx-create-upload-group");
    elements.createHelperLocal = document.getElementById("termx-create-helper-local");
    elements.createHelperSha = document.getElementById("termx-create-helper-sha");
    elements.createAnyHostKey = document.getElementById("termx-create-any-host-key");
    elements.createCols = document.getElementById("termx-create-cols");
    elements.createRows = document.getElementById("termx-create-rows");
    elements.createError = document.getElementById("termx-create-error");
    elements.createCancel = document.getElementById("termx-create-cancel");
    elements.createSubmit = document.getElementById("termx-create-submit");
    elements.infoMeta = document.getElementById("termx-info-meta");
    elements.terminate = document.getElementById("termx-terminate");
    elements.rebindOpen = document.getElementById("termx-rebind-open");
    elements.rebindDialog = document.getElementById("termx-rebind-dialog");
    elements.rebindForm = document.getElementById("termx-rebind-form");
    elements.rebindRoot = document.getElementById("termx-rebind-root");
    elements.rebindName = document.getElementById("termx-rebind-name");
    elements.rebindForce = document.getElementById("termx-rebind-force");
    elements.rebindCurrent = document.getElementById("termx-rebind-current");
    elements.rebindError = document.getElementById("termx-rebind-error");
    elements.rebindSubmit = document.getElementById("termx-rebind-submit");
    elements.rebindCancel = document.getElementById("termx-rebind-cancel");
    elements.status = document.getElementById("termx-status");
    elements.tabInfo = document.getElementById("termx-tab-info");
    elements.tabRaw = document.getElementById("termx-tab-raw");
    elements.viewInfo = document.getElementById("termx-view-info");
    elements.viewRaw = document.getElementById("termx-view-raw");
    elements.transcript = document.getElementById("termx-transcript");
    elements.infoNotice = document.getElementById("termx-info-notice");
    elements.infoForm = document.getElementById("termx-info-form");
    elements.infoInput = document.getElementById("termx-info-input");
    elements.infoSend = document.getElementById("termx-info-send");
    if (!elements.term || !elements.select || !elements.sessionSelect) {
      return;
    }
    if (typeof window.Terminal !== "function") {
      setStatus("xterm.js 加载失败");
      return;
    }

    createTerminal();
    bindControls();
    observeResize();
    setView("info");
    updateControls();
    connect();
  }

  /** 创建 xterm 实例并挂载到容器。 */
  function createTerminal() {
    term = new window.Terminal({
      cursorBlink: true,
      fontFamily: '"Cascadia Mono", "Consolas", "DejaVu Sans Mono", monospace',
      fontSize: FALLBACK_FONT_SIZE,
      scrollback: 5000,
      theme: {
        background: "#05070a",
        foreground: "#e6e6e6",
        cursor: "#4da3ff",
        selectionBackground: "#2b4a6f"
      }
    });
    term.open(elements.term);
    term.onData(handleInput);
    elements.term.addEventListener("mousedown", function () {
      if (currentTerminalId) {
        term.focus();
      }
    });
  }

  /** 绑定 header 控件。 */
  function bindControls() {
    elements.refresh.addEventListener("click", function () {
      refreshTerminals().then(announceList);
    });
    elements.terminate.addEventListener("click", terminateCurrent);
    if (elements.sessionSelect) {
      elements.sessionSelect.addEventListener("change", function () {
        onSessionChange(elements.sessionSelect.value || null);
      });
    }
    if (elements.createOpen) {
      elements.createOpen.addEventListener("click", openCreateDialog);
    }
    if (elements.createForm) {
      elements.createForm.addEventListener("submit", submitCreate);
    }
    if (elements.createCancel) {
      elements.createCancel.addEventListener("click", closeCreateDialog);
    }
    if (elements.createType) {
      elements.createType.addEventListener("change", updateCreateType);
    }
    if (elements.createAuth) {
      elements.createAuth.addEventListener("change", updateCreateAuth);
    }
    if (elements.createUploadHelper) {
      elements.createUploadHelper.addEventListener("change", updateCreateHelper);
    }
    if (elements.createDialog) {
      // 原生 Esc 关闭不经过 cancel 按钮：同样清掉只在内存中的密码/口令。
      elements.createDialog.addEventListener("close", clearCreateSecrets);
    }
    if (elements.rebindOpen) {
      elements.rebindOpen.addEventListener("click", openRebindDialog);
    }
    if (elements.rebindForm) {
      elements.rebindForm.addEventListener("submit", submitRebind);
    }
    if (elements.rebindCancel) {
      elements.rebindCancel.addEventListener("click", closeRebindDialog);
    }
    if (elements.rebindRoot) {
      elements.rebindRoot.addEventListener("change", updateRebindSubmit);
    }
    elements.select.addEventListener("change", function () {
      selectTerminal(elements.select.value || null, false);
    });
    if (elements.tabInfo) {
      elements.tabInfo.addEventListener("click", function () {
        setView("info");
      });
    }
    if (elements.tabRaw) {
      elements.tabRaw.addEventListener("click", function () {
        setView("raw");
      });
    }
    if (elements.infoForm) {
      elements.infoForm.addEventListener("submit", submitInfoCommand);
    }
  }

  /** 建立到 /ws 的 WebSocket 连接。 */
  function connect() {
    var scheme = window.location.protocol === "https:" ? "wss://" : "ws://";
    var ws;
    try {
      ws = new WebSocket(scheme + window.location.host + "/ws");
    } catch (error) {
      setConnected(false);
      setStatus("无法创建 WebSocket 连接");
      scheduleReconnect();
      return;
    }
    socket = ws;
    setStatus("正在连接 WebSocket…");

    ws.addEventListener("open", function () {
      if (socket !== ws) {
        return;
      }
      reconnectDelay = RECONNECT_MIN_MS;
      setConnected(true);
      setStatus("已连接，正在读取终端列表…");
      refreshTerminals().then(function (items) {
        if (!connected) {
          return;
        }
        if (currentTerminalId) {
          selectTerminal(currentTerminalId, true);
        } else {
          announceList(items);
        }
      });
    });
    ws.addEventListener("message", function (event) {
      if (socket === ws) {
        handleMessage(event.data);
      }
    });
    ws.addEventListener("close", function () {
      if (socket !== ws) {
        return;
      }
      socket = null;
      setConnected(false);
      stopTranscriptPolling();
      rejectPending("连接已断开");
      claimed = false;
      claimInFlight = null;
      setStatus("连接已断开，正在重连…");
      scheduleReconnect();
    });
    ws.addEventListener("error", function () {
      if (socket === ws) {
        setStatus("WebSocket 连接错误");
      }
    });
  }

  /** 按退避策略安排重连。 */
  function scheduleReconnect() {
    if (reconnectTimer) {
      return;
    }
    reconnectTimer = window.setTimeout(function () {
      reconnectTimer = null;
      connect();
    }, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
  }

  /** 发送一次 RPC 请求并按 id 配对响应。 */
  function rpc(method, params) {
    return new Promise(function (resolve, reject) {
      if (!connected || !socket || socket.readyState !== WebSocket.OPEN) {
        reject(new Error("WebSocket 未连接"));
        return;
      }
      var id = "r" + nextRequestId;
      nextRequestId += 1;
      var entry = { resolve: resolve, reject: reject, timer: null };
      entry.timer = window.setTimeout(function () {
        delete pending[id];
        reject(new Error("请求超时: " + method));
      }, RPC_TIMEOUT_MS);
      pending[id] = entry;
      try {
        socket.send(JSON.stringify({ id: id, method: method, params: params || {} }));
      } catch (error) {
        window.clearTimeout(entry.timer);
        delete pending[id];
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  /** 处理代理转发的响应帧与事件帧。 */
  function handleMessage(raw) {
    if (typeof raw !== "string") {
      return;
    }
    var message;
    try {
      message = JSON.parse(raw);
    } catch (error) {
      return;
    }
    if (!message || typeof message !== "object") {
      return;
    }
    if (typeof message.id === "string") {
      if (message.id === WEB_HELLO_ID) {
        return;
      }
      settleRequest(message);
      return;
    }
    if (typeof message.type === "string") {
      handleEvent(message);
    }
  }

  /** 结算一条 RPC 响应。 */
  function settleRequest(message) {
    var entry = pending[message.id];
    if (!entry) {
      if (message.id === "" && !message.ok) {
        setStatus("连接错误：" + describeError(message.error));
      }
      return;
    }
    delete pending[message.id];
    window.clearTimeout(entry.timer);
    if (message.ok) {
      entry.resolve(message.result);
    } else {
      entry.reject(makeRpcError(message.error));
    }
  }

  /** 把协议错误转成携带 code / terminal_id 的 Error。 */
  function makeRpcError(error) {
    var result = new Error(describeError(error));
    result.code = error && error.code;
    // session_exists 会携带冲突的旧 terminal_id，供界面直接展示。
    result.terminalId = error && error.terminal_id;
    return result;
  }

  /** 读取错误码与说明。 */
  function describeError(error) {
    if (!error) {
      return "未知错误";
    }
    var message = error.message || "未知错误";
    return error.code ? error.code + ": " + message : message;
  }

  /** 拒绝所有等待中的请求，避免重连后旧回调串线。 */
  function rejectPending(reason) {
    var ids = Object.keys(pending);
    for (var i = 0; i < ids.length; i += 1) {
      var entry = pending[ids[i]];
      window.clearTimeout(entry.timer);
      entry.reject(new Error(reason));
    }
    pending = Object.create(null);
  }

  /** 处理当前终端的事件；订阅窗口内先入缓冲，避免与历史回放乱序。 */
  function handleEvent(event) {
    if (!event.terminal_id || event.terminal_id !== currentTerminalId) {
      return;
    }
    if (liveBuffer) {
      liveBuffer.push(event);
      return;
    }
    applyEvent(event);
  }

  /** 按类型渲染一条事件并推进该终端的游标。 */
  function applyEvent(event) {
    if (typeof event.seq === "number") {
      advanceCursor(event.terminal_id, event.seq);
    }
    switch (event.type) {
      case "started":
        setStatus("终端运行中（pid " + event.pid + "）");
        break;
      case "output":
        if (event.data) {
          term.write(base64ToBytes(event.data));
        }
        break;
      case "output_dropped":
        setStatus(
          "输出因背压被丢弃（seq " + event.from_seq + "–" + event.to_seq + "）"
        );
        break;
      case "waiting_input":
        setStatus("等待输入（" + event.confidence + " / " + event.reason + "）");
        break;
      case "interactive_changed":
        if (event.active) {
          setStatus("交互中：" + (event.command || "pid " + event.pid));
        }
        break;
      case "lease_revoked":
        claimed = false;
        setStatus("输入控制已被释放或抢占");
        break;
      case "exited":
        claimed = false;
        terminalEnded = true;
        setInfoNotice("终端已退出（退出码 " + event.exit_code + "）");
        setStatus("终端已退出（退出码 " + event.exit_code + "）");
        updateControls();
        refreshTerminals();
        break;
      case "interrupted":
        claimed = false;
        terminalEnded = true;
        setInfoNotice("终端已中断（" + event.signal + "）");
        setStatus("终端已中断（" + event.signal + "）");
        updateControls();
        refreshTerminals();
        break;
      case "rebound":
        setStatus(
          "终端已迁移到 " + event.new_root_session_id + "/" + event.new_name
        );
        closeRebindDialog();
        refreshTerminals();
        break;
      case "error":
        claimed = false;
        terminalEnded = true;
        setInfoNotice("终端错误：" + event.message);
        setStatus("终端错误：" + event.message);
        updateControls();
        refreshTerminals();
        break;
      default:
        break;
    }
  }

  /** 只前进不回退地记录某终端的最后事件 seq。 */
  function advanceCursor(terminalId, seq) {
    if (!terminalId || typeof seq !== "number") {
      return;
    }
    if (seq > (cursors[terminalId] || 0)) {
      cursors[terminalId] = seq;
    }
  }

  /** 渲染完订阅窗口内缓存的事件；只处理属于当前选择代次的事件。 */
  function flushLiveBuffer(token) {
    if (liveBufferToken !== token) {
      return;
    }
    var buffered = liveBuffer || [];
    liveBuffer = null;
    buffered.sort(function (left, right) {
      return (left.seq || 0) - (right.seq || 0);
    });
    for (var i = 0; i < buffered.length; i += 1) {
      applyEvent(buffered[i]);
    }
  }

  /**
   * 拉取会话与终端列表，刷新两级下拉并返回终端条目。
   *
   * `focusTerminalId` 用于新建成功后强制把第一级切到新终端所属会话。
   */
  function refreshTerminals(focusTerminalId) {
    if (!connected) {
      return Promise.resolve([]);
    }
    var sessionsPromise = rpc("session.list", {}).then(normalizeSessions, function () {
      // 读取会话失败不应阻断终端列表；@web 仍会由 mergeSessions 补入。
      return [];
    });
    return Promise.all([rpc("terminal.list", {}), sessionsPromise]).then(
      function (results) {
        var items = normalizeList(results[0]);
        lastTerminals = items;
        lastSessions = mergeSessions(results[1], items);
        syncSelectedSession(items, focusTerminalId);
        renderSessionList();
        renderTerminalList(items);
        var exists = currentTerminalId && hasTerminal(items, currentTerminalId);
        if (currentTerminalId && !exists) {
          delete cursors[currentTerminalId];
          currentTerminalId = null;
          claimed = false;
          term.clear();
          stopTranscriptPolling();
          resetTranscriptView();
          closeRebindDialog();
          renderInfoMeta(null);
          setInfoNotice("终端已不存在");
          setStatus("终端已不存在");
        } else {
          renderInfoMeta(findTerminal(currentTerminalId));
        }
        updateControls();
        return items;
      },
      function (error) {
        if (connected) {
          setStatus("读取终端列表失败：" + error.message);
        }
        return [];
      }
    );
  }

  /**
   * 合并会话：去掉服务端可能返回的 @web，再在末尾补一条未绑定项。
   *
   * 这样下拉固定为「已知 opencode 会话 + 其他/未绑定」，且 @web 的终端数
   * 仍来自 session.list。
   */
  function mergeSessions(sessions, items) {
    var known = [];
    var web = null;
    for (var i = 0; i < sessions.length; i += 1) {
      var session = sessions[i];
      if (!session || !session.root_session_id) {
        continue;
      }
      if (session.root_session_id === WEB_ROOT_SESSION_ID) {
        web = session;
      } else {
        known.push(session);
      }
    }
    if (!web) {
      web = {
        root_session_id: WEB_ROOT_SESSION_ID,
        live: false,
        terminals: countTerminals(items, WEB_ROOT_SESSION_ID)
      };
    }
    known.push(web);
    return known;
  }

  /** 统计某 root 下的终端数量。 */
  function countTerminals(items, root) {
    var count = 0;
    for (var i = 0; i < items.length; i += 1) {
      if (items[i] && items[i].root_session_id === root) {
        count += 1;
      }
    }
    return count;
  }

  /** 依据当前终端或上次选择确定第一级会话；优先跟随焦点终端。 */
  function syncSelectedSession(items, focusTerminalId) {
    var focus = focusTerminalId ? findTerminalIn(items, focusTerminalId) : null;
    if (focus) {
      selectedSession = focus.root_session_id;
      return;
    }
    var current = findTerminalIn(items, currentTerminalId);
    if (current) {
      selectedSession = current.root_session_id;
      return;
    }
    if (selectedSession && sessionExists(selectedSession)) {
      return;
    }
    // 默认选第一个有终端的非 @web 会话，其次第一个已知会话，最后 @web。
    var fallback = null;
    for (var i = 0; i < lastSessions.length; i += 1) {
      var session = lastSessions[i];
      if (!session || !session.root_session_id) {
        continue;
      }
      if (!fallback) {
        fallback = session.root_session_id;
      }
      if (session.root_session_id !== WEB_ROOT_SESSION_ID && session.terminals > 0) {
        fallback = session.root_session_id;
        break;
      }
    }
    selectedSession = fallback || WEB_ROOT_SESSION_ID;
  }

  /** 会话是否仍在最近的 session.list 中。 */
  function sessionExists(root) {
    for (var i = 0; i < lastSessions.length; i += 1) {
      if (lastSessions[i] && lastSessions[i].root_session_id === root) {
        return true;
      }
    }
    return false;
  }

  /** 第一级下拉：已知会话 + 其他/未绑定。 */
  function renderSessionList() {
    var select = elements.sessionSelect;
    if (!select) {
      return;
    }
    while (select.firstChild) {
      select.removeChild(select.firstChild);
    }
    if (!lastSessions.length) {
      appendOptionTo(select, WEB_ROOT_SESSION_ID, WEB_ROOT_LABEL, WEB_ROOT_SESSION_ID);
      selectedSession = WEB_ROOT_SESSION_ID;
      return;
    }
    for (var i = 0; i < lastSessions.length; i += 1) {
      var session = lastSessions[i];
      if (!session || !session.root_session_id) {
        continue;
      }
      var label =
        session.root_session_id === WEB_ROOT_SESSION_ID
          ? webSessionLabel(session)
          : sessionLabel(session, false);
      appendOptionTo(select, session.root_session_id, label, session.root_session_id);
    }
    select.value = sessionExists(selectedSession)
      ? selectedSession
      : lastSessions[0].root_session_id;
    selectedSession = select.value || WEB_ROOT_SESSION_ID;
  }

  /** `@web` 会话的显示名：其他/未绑定（带终端数）。 */
  function webSessionLabel(session) {
    var label = WEB_ROOT_LABEL;
    if (typeof session.terminals === "number" && session.terminals > 0) {
      label += " · 终端 " + session.terminals;
    }
    return label;
  }

  /** 统一列表结果形状。 */
  function normalizeList(result) {
    if (Array.isArray(result)) {
      return result;
    }
    if (result && Array.isArray(result.terminals)) {
      return result.terminals;
    }
    return [];
  }

  /** 在最近一次终端列表中查找指定终端。 */
  function findTerminal(terminalId) {
    return findTerminalIn(lastTerminals, terminalId);
  }

  /** 在给定终端列表中查找指定终端。 */
  function findTerminalIn(items, terminalId) {
    if (!terminalId) {
      return null;
    }
    for (var i = 0; i < items.length; i += 1) {
      if (items[i] && items[i].terminal_id === terminalId) {
        return items[i];
      }
    }
    return null;
  }

  /** 判断列表中是否仍有指定终端。 */
  function hasTerminal(items, terminalId) {
    return !!findTerminalIn(items, terminalId);
  }

  /** 终端是否属于当前选中的会话。 */
  function terminalInSelectedSession(item) {
    return !!item && (!selectedSession || item.root_session_id === selectedSession);
  }

  /** 渲染第二级终端下拉：只显示当前会话下的终端，保留当前选择。 */
  function renderTerminalList(items) {
    var selected = currentTerminalId;
    var found = false;
    var visible = [];
    for (var i = 0; i < items.length; i += 1) {
      if (terminalInSelectedSession(items[i]) && items[i].terminal_id) {
        visible.push(items[i]);
      }
    }
    while (elements.select.firstChild) {
      elements.select.removeChild(elements.select.firstChild);
    }
    appendOption("", visible.length ? "选择终端" : "（该会话没有终端）");
    for (var j = 0; j < visible.length; j += 1) {
      var item = visible[j];
      if (item.terminal_id === selected) {
        found = true;
      }
      appendOption(item.terminal_id, terminalLabel(item));
    }
    elements.select.value = found ? selected : "";
  }

  /** 第一级会话切换：清空不属于该会话的当前终端并刷新第二级。 */
  function onSessionChange(root) {
    selectedSession = root || null;
    if (currentTerminalId) {
      var item = findTerminal(currentTerminalId);
      if (!terminalInSelectedSession(item)) {
        selectTerminal(null, false);
      }
    }
    renderTerminalList(lastTerminals);
    renderInfoMeta(findTerminal(currentTerminalId));
    updateControls();
  }

  /** 追加一个 option 到终端下拉框。 */
  function appendOption(value, label) {
    appendOptionTo(elements.select, value, label, null);
  }

  /** 追加一个 option 到指定 select；`title` 用于查看完整值。 */
  function appendOptionTo(select, value, label, title) {
    var option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    if (title) {
      option.title = title;
    }
    select.appendChild(option);
  }

  /** 下拉框显示名：名称 · 状态 · pid。 */
  function terminalLabel(item) {
    var label = item.name || item.terminal_id;
    if (item.state) {
      label += " · " + item.state;
    }
    if (typeof item.pid === "number") {
      label += " · pid " + item.pid;
    }
    return label;
  }

  /** 列表刷新后无选中终端时提示。 */
  function announceList(items) {
    if (!currentTerminalId) {
      var count = 0;
      for (var i = 0; i < items.length; i += 1) {
        if (terminalInSelectedSession(items[i])) {
          count += 1;
        }
      }
      if (!selectedSession) {
        setStatus("请选择会话");
      } else {
        setStatus(count ? "请选择终端" : "该会话没有终端");
      }
    }
  }

  /** 切换/重连终端：先 read 历史，再从游标订阅实时事件，最后 claim 并聚焦。 */
  function selectTerminal(terminalId, force) {
    terminalId = terminalId || null;
    if (!force && terminalId === currentTerminalId) {
      return;
    }
    var previous = currentTerminalId;
    // 重连同名终端时保留 xterm 内容与游标，只补齐断线期间的历史。
    var reconnect = !!(force && terminalId && terminalId === previous);
    currentTerminalId = terminalId;
    var selectedItem = findTerminal(terminalId);
    if (selectedItem && selectedItem.root_session_id !== selectedSession) {
      selectedSession = selectedItem.root_session_id;
      renderSessionList();
      renderTerminalList(lastTerminals);
    }
    renderInfoMeta(selectedItem);
    claimed = false;
    claimInFlight = null;
    lastSize = null;
    selectToken += 1;
    var token = selectToken;
    liveBuffer = null;
    liveBufferToken = 0;
    historyNotice = null;

    if (!reconnect) {
      term.clear();
      resetTranscriptView();
      terminalEnded = false;
      if (terminalId) {
        cursors[terminalId] = 0;
      }
    }
    if (previous && connected && previous !== terminalId) {
      rpc("unsubscribe", { terminals: [{ terminal_id: previous }] }).catch(function () {
        // 旧终端可能已消失，退订失败不影响新选择。
      });
    }
    updateControls();

    if (!terminalId) {
      stopTranscriptPolling();
      setStatus("未选择终端");
      return;
    }
    if (!connected) {
      setStatus("未连接");
      return;
    }
    elements.select.value = terminalId;
    if (viewMode === "info") {
      startTranscriptPolling();
    }
    var startCursor = cursors[terminalId] || 0;
    setStatus(reconnect ? "正在补齐断线期间的历史…" : "正在读取终端历史…");
    readHistory(terminalId, startCursor, token)
      .then(function (cursor) {
        if (token !== selectToken || currentTerminalId !== terminalId) {
          return null;
        }
        return subscribeTerminal(terminalId, cursor, token, true);
      })
      .then(function () {
        if (token !== selectToken || currentTerminalId !== terminalId) {
          return;
        }
        refreshStatusMeta(terminalId, token);
        if (viewMode === "raw") {
          applyResize();
          term.focus();
        } else {
          startTranscriptPolling();
        }
      });
  }

  /**
   * 用 `terminal.read` 分段拉取历史并渲染，直到游标不再前进或达到轮次上限。
   * 返回最终游标；不可回放（reset）时清屏并从服务端基线继续。
   */
  function readHistory(terminalId, startCursor, token) {
    var cursor = startCursor || 0;
    var round = 0;

    function step() {
      if (token !== selectToken || currentTerminalId !== terminalId || !connected) {
        return Promise.resolve(cursor);
      }
      var before = cursor;
      return rpc("terminal.read", {
        terminal_id: terminalId,
        cursor: before,
        max_bytes: READ_MAX_BYTES
      }).then(
        function (result) {
          if (token !== selectToken || currentTerminalId !== terminalId) {
            return cursor;
          }
          if (result && result.reset) {
            // 该游标之后的历史已不可用：清屏，从服务端基线继续实时流。
            term.clear();
            if (typeof result.cursor === "number") {
              cursor = result.cursor;
            }
            advanceCursor(terminalId, cursor);
            historyNotice = "历史已不可回放（" + result.reset + "），从最新位置继续";
            setStatus(historyNotice);
            return cursor;
          }
          var chunks = (result && result.chunks) || [];
          for (var i = 0; i < chunks.length; i += 1) {
            if (chunks[i] && chunks[i].data) {
              term.write(base64ToBytes(chunks[i].data));
            }
            if (chunks[i] && typeof chunks[i].seq === "number") {
              advanceCursor(terminalId, chunks[i].seq);
              cursor = Math.max(cursor, chunks[i].seq);
            }
          }
          if (result && typeof result.cursor === "number") {
            cursor = Math.max(cursor, result.cursor);
            advanceCursor(terminalId, cursor);
          }
          round += 1;
          if (round >= READ_MAX_ROUNDS) {
            setStatus("历史读取达到上限，后续输出从实时流继续");
            return cursor;
          }
          if (cursor === before) {
            return cursor;
          }
          return step();
        },
        function (error) {
          if (token === selectToken && currentTerminalId === terminalId) {
            setStatus("读取终端历史失败：" + error.message);
          }
          return cursor;
        }
      );
    }

    return step();
  }

  /**
   * 订阅实时事件：`cursor` 为 read 返回的最终游标，服务端只回放该点之后的小段
   * 事件。回放过大时服务端返回 reset，此时丢弃缓冲、退订并重新 read 后再订阅。
   */
  function subscribeTerminal(terminalId, cursor, token, allowResetRetry) {
    liveBuffer = [];
    liveBufferToken = token;
    return rpc("subscribe", {
      terminals: [{ terminal_id: terminalId, cursor: cursor }],
      all: false
    }).then(
      function (result) {
        if (token !== selectToken || currentTerminalId !== terminalId) {
          if (liveBufferToken === token) {
            liveBuffer = null;
          }
          return false;
        }
        var entry = firstCursor(result);
        var reset = entry && entry.reset;
        var baseline =
          entry && typeof entry.cursor === "number" ? entry.cursor : cursor;

        if (reset) {
          // 已渲染的历史保持不变；缓冲丢弃，退订后用 read 补齐缺口。
          if (liveBufferToken === token) {
            liveBuffer = [];
          }
          if (!allowResetRetry) {
            liveBuffer = null;
            advanceCursor(terminalId, baseline);
            historyNotice = "历史回放过大（" + reset + "），已从最新位置继续";
            setStatus(historyNotice);
            return claimTerminal();
          }
          historyNotice = "历史回放过大（" + reset + "），已改用分段读取";
          setStatus(historyNotice);
          return rpc("unsubscribe", { terminals: [{ terminal_id: terminalId }] })
            .catch(function () {
              // 退订失败不阻塞恢复流程。
            })
            .then(function () {
              if (token !== selectToken || currentTerminalId !== terminalId) {
                return false;
              }
              return readHistory(terminalId, cursor, token).then(function (next) {
                if (token !== selectToken || currentTerminalId !== terminalId) {
                  return false;
                }
                return subscribeTerminal(terminalId, next, token, false);
              });
            });
        }

        flushLiveBuffer(token);
        advanceCursor(terminalId, baseline);
        setStatus(historyNotice || "已订阅终端");
        historyNotice = null;
        return claimTerminal();
      },
      function (error) {
        if (liveBufferToken === token) {
          liveBuffer = null;
        }
        if (connected && currentTerminalId === terminalId) {
          setStatus("订阅失败：" + error.message);
        }
        return false;
      }
    );
  }

  /** 取 subscribe 返回的第一条游标记录。 */
  function firstCursor(result) {
    var items = result && result.cursors;
    if (!items || !items.length) {
      return null;
    }
    return items[0];
  }

  /** 取得当前终端的输入 lease；失败时显示状态并允许下次重试。 */
  function claimTerminal() {
    if (!currentTerminalId || !connected) {
      return Promise.resolve(false);
    }
    if (claimed) {
      return Promise.resolve(true);
    }
    if (claimInFlight) {
      return claimInFlight;
    }
    var terminalId = currentTerminalId;
    claimInFlight = rpc("terminal.claim", { terminal_id: terminalId }).then(
      function () {
        claimInFlight = null;
        if (currentTerminalId === terminalId) {
          claimed = true;
          return true;
        }
        return false;
      },
      function (error) {
        claimInFlight = null;
        if (connected && currentTerminalId === terminalId) {
          setStatus("输入控制获取失败：" + error.message);
        }
        return false;
      }
    );
    return claimInFlight;
  }

  /** xterm 输入 → terminal.send（base64 UTF-8，不追加换行）。 */
  function handleInput(data) {
    if (!currentTerminalId || !connected) {
      return;
    }
    var terminalId = currentTerminalId;
    claimTerminal().then(function (ok) {
      if (!ok || currentTerminalId !== terminalId || !connected) {
        return;
      }
      rpc("terminal.send", {
        terminal_id: terminalId,
        data: base64FromUtf8(data),
        enter: false
      }).catch(function (error) {
        if (connected && currentTerminalId === terminalId) {
          setStatus("发送失败：" + error.message);
        }
      });
    });
  }

  /** UTF-8 文本转 base64。 */
  function base64FromUtf8(text) {
    var bytes = new TextEncoder().encode(text);
    var binary = "";
    var chunkSize = 0x8000;
    for (var i = 0; i < bytes.length; i += chunkSize) {
      var chunk = bytes.subarray(i, i + chunkSize);
      binary += String.fromCharCode.apply(null, chunk);
    }
    return window.btoa(binary);
  }

  /** base64 转 Uint8Array，交给 xterm 按字节渲染。 */
  function base64ToBytes(data) {
    var binary = window.atob(data);
    var bytes = new Uint8Array(binary.length);
    for (var i = 0; i < binary.length; i += 1) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }

  /** 打开迁移弹窗并读取已知会话。 */
  function openRebindDialog() {
    if (!currentTerminalId || !connected || !elements.rebindDialog) {
      return;
    }
    rebindBusy = false;
    if (elements.rebindName) {
      elements.rebindName.value = "";
    }
    if (elements.rebindForce) {
      elements.rebindForce.checked = false;
    }
    hideRebindError();
    updateRebindCurrentHint();
    resetRebindSelect("正在加载…");
    showRebindDialog();
    setStatus("正在读取已知会话…");
    rpc("session.list", {}).then(
      function (result) {
        if (!isRebindDialogOpen()) {
          return;
        }
        renderSessionOptions(normalizeSessions(result));
        setStatus("请选择目标会话");
      },
      function (error) {
        if (!isRebindDialogOpen()) {
          return;
        }
        resetRebindSelect("（读取会话失败）");
        showRebindError("读取会话列表失败：" + error.message);
        setStatus("读取会话列表失败：" + error.message);
      }
    );
  }

  /** 展开原生 dialog（不支持 showModal 时退回 open 属性）。 */
  function showRebindDialog() {
    var dialog = elements.rebindDialog;
    if (!dialog) {
      return;
    }
    if (typeof dialog.showModal === "function") {
      if (!dialog.open) {
        dialog.showModal();
      }
    } else {
      dialog.setAttribute("open", "");
    }
  }

  /** 关闭迁移弹窗。 */
  function closeRebindDialog() {
    var dialog = elements.rebindDialog;
    if (!dialog) {
      return;
    }
    if (typeof dialog.close === "function") {
      if (dialog.open) {
        dialog.close();
      }
    } else {
      dialog.removeAttribute("open");
    }
  }

  /** 弹窗是否处于打开状态。 */
  function isRebindDialogOpen() {
    var dialog = elements.rebindDialog;
    return !!dialog && (dialog.open || dialog.hasAttribute("open"));
  }

  /** 更新弹窗内的当前终端提示。 */
  function updateRebindCurrentHint() {
    if (!elements.rebindCurrent || !currentTerminalId) {
      return;
    }
    var root = currentRootSessionId();
    elements.rebindCurrent.textContent =
      "当前终端：" + currentTerminalId + (root ? "（" + root + "）" : "");
  }

  /** 清空目标会话下拉并显示占位/错误项。 */
  function resetRebindSelect(message) {
    var select = elements.rebindRoot;
    if (!select) {
      return;
    }
    while (select.firstChild) {
      select.removeChild(select.firstChild);
    }
    appendOptionTo(select, "", message);
    select.disabled = true;
    updateRebindSubmit();
  }

  /** `session.list` 结果统一成数组。 */
  function normalizeSessions(result) {
    if (Array.isArray(result)) {
      return result;
    }
    if (result && Array.isArray(result.sessions)) {
      return result.sessions;
    }
    return [];
  }

  /**
   * 填充目标会话下拉；标记当前终端的 root，并固定追加 `其他/未绑定`。
   *
   * 选择 `@web` 即把终端迁移到未绑定（等同解绑，仅管理员可用）。
   */
  function renderSessionOptions(sessions) {
    var select = elements.rebindRoot;
    if (!select) {
      return;
    }
    var currentRoot = currentRootSessionId();
    while (select.firstChild) {
      select.removeChild(select.firstChild);
    }
    var values = [];
    var web = null;
    for (var i = 0; i < sessions.length; i += 1) {
      var session = sessions[i];
      if (!session || !session.root_session_id) {
        continue;
      }
      if (session.root_session_id === WEB_ROOT_SESSION_ID) {
        web = session;
        continue;
      }
      var isCurrent = session.root_session_id === currentRoot;
      appendOptionTo(
        select,
        session.root_session_id,
        sessionLabel(session, isCurrent),
        session.root_session_id
      );
      values.push(session.root_session_id);
    }
    appendOptionTo(
      select,
      WEB_ROOT_SESSION_ID,
      webSessionLabel(web || { terminals: 0 }),
      WEB_ROOT_SESSION_ID
    );
    values.push(WEB_ROOT_SESSION_ID);
    select.disabled = false;
    select.value = preferredSession(values, currentRoot);
    updateRebindSubmit();
  }

  /**
   * 默认选第一个非当前、非 @web 的会话；没有别的会话时保留当前会话，
   * 最后才退回 @web（避免误把终端解绑）。
   */
  function preferredSession(values, currentRoot) {
    var web = null;
    for (var i = 0; i < values.length; i += 1) {
      if (values[i] === WEB_ROOT_SESSION_ID) {
        web = values[i];
        continue;
      }
      if (values[i] !== currentRoot) {
        return values[i];
      }
    }
    if (currentRoot && values.indexOf(currentRoot) >= 0) {
      return currentRoot;
    }
    return web || values[0];
  }

  /** 下拉项文本：截断 root + live 状态 + 终端数 + 当前标记。 */
  function sessionLabel(session, isCurrent) {
    var label = shortSessionId(session.root_session_id);
    label += " · " + (session.live ? "在线" : "无心跳");
    if (typeof session.terminals === "number" && session.terminals > 0) {
      label += " · 终端 " + session.terminals;
    }
    if (isCurrent) {
      label += "（当前）";
    }
    return label;
  }

  /** 截断过长的 session id，完整值放在 option.title。 */
  function shortSessionId(root) {
    if (root.length <= 20) {
      return root;
    }
    return root.slice(0, 12) + "…" + root.slice(-6);
  }

  /** 当前终端所属的 root_session_id。 */
  function currentRootSessionId() {
    if (!currentTerminalId) {
      return null;
    }
    for (var i = 0; i < lastTerminals.length; i += 1) {
      var item = lastTerminals[i];
      if (item && item.terminal_id === currentTerminalId) {
        return item.root_session_id || null;
      }
    }
    return null;
  }

  /** 按当前状态启用/禁用弹窗提交按钮。 */
  function updateRebindSubmit() {
    if (!elements.rebindSubmit) {
      return;
    }
    elements.rebindSubmit.disabled =
      rebindBusy ||
      !connected ||
      !currentTerminalId ||
      !elements.rebindRoot ||
      !elements.rebindRoot.value;
  }

  /** 迁移进行中禁用提交与取消。 */
  function setRebindBusy(value) {
    rebindBusy = !!value;
    if (elements.rebindCancel) {
      elements.rebindCancel.disabled = rebindBusy;
    }
    updateRebindSubmit();
  }

  /** 在弹窗内显示错误。 */
  function showRebindError(message) {
    if (!elements.rebindError) {
      return;
    }
    elements.rebindError.textContent = message;
    elements.rebindError.hidden = false;
  }

  /** 清空弹窗错误。 */
  function hideRebindError() {
    if (!elements.rebindError) {
      return;
    }
    elements.rebindError.textContent = "";
    elements.rebindError.hidden = true;
  }

  /** 提交 `terminal.rebind`；成功后关闭弹窗并刷新列表。 */
  function submitRebind(event) {
    if (event) {
      event.preventDefault();
    }
    if (!currentTerminalId || !connected) {
      showRebindError("终端未选择或连接已断开");
      return;
    }
    var targetRoot = (elements.rebindRoot && elements.rebindRoot.value || "").trim();
    if (!targetRoot) {
      showRebindError("请选择目标会话");
      return;
    }
    var newName = (elements.rebindName && elements.rebindName.value || "").trim();
    var params = buildRebindParams(targetRoot, newName);
    hideRebindError();
    setRebindBusy(true);
    setStatus("正在迁移终端…");
    rpc("terminal.rebind", params).then(
      function (result) {
        setRebindBusy(false);
        var rebound = result || {};
        setStatus(
          "已迁移终端到 " +
            (rebound.root_session_id || targetRoot) +
            "/" +
            (rebound.name || newName || "(原名)")
        );
        closeRebindDialog();
        refreshTerminals();
      },
      function (error) {
        setRebindBusy(false);
        var detail = describeRebindError(error);
        showRebindError("迁移失败：" + detail);
        setStatus("迁移失败：" + detail);
      }
    );
  }

  /** 组装 `terminal.rebind` 参数：name 留空时不发送。 */
  function buildRebindParams(targetRoot, newName) {
    var params = {
      terminal_id: currentTerminalId,
      root_session_id: targetRoot,
      force: !!(elements.rebindForce && elements.rebindForce.checked)
    };
    if (newName) {
      params.name = newName;
    }
    return params;
  }

  /** 迁移失败详情：附带 session_exists 返回的冲突 terminal_id。 */
  function describeRebindError(error) {
    if (!connected) {
      return "连接已断开，迁移结果未知";
    }
    var detail = describeError(error);
    if (error && error.terminalId) {
      detail += "（冲突的旧 terminal_id=" + error.terminalId + "）";
    }
    return detail;
  }

  /** 打开新建终端弹窗；会话下拉来自 `session.list`（含 @web）。 */
  function openCreateDialog() {
    if (!connected || !elements.createDialog) {
      return;
    }
    createBusy = false;
    hideCreateError();
    resetCreateForm();
    renderCreateSessions(lastSessions, selectedSession || WEB_ROOT_SESSION_ID);
    showCreateDialog();
    setStatus("正在读取会话列表…");
    rpc("session.list", {}).then(
      function (result) {
        if (!isCreateDialogOpen()) {
          return;
        }
        var sessions = mergeSessions(normalizeSessions(result), lastTerminals);
        lastSessions = sessions;
        renderCreateSessions(sessions, selectedSession || WEB_ROOT_SESSION_ID);
        setStatus("填写终端参数");
      },
      function (error) {
        if (!isCreateDialogOpen()) {
          return;
        }
        showCreateError("读取会话列表失败：" + error.message);
      }
    );
  }

  /** 重置新建表单为默认值（不保留密码等敏感输入）。 */
  function resetCreateForm() {
    if (elements.createName) {
      elements.createName.value = "default";
    }
    if (elements.createType) {
      elements.createType.value = "local";
    }
    setInputValue(elements.createCmd, "");
    setInputValue(elements.createArgs, "");
    setInputValue(elements.createCwd, "");
    setInputValue(elements.createEnv, "");
    setInputValue(elements.createHost, "");
    setInputValue(elements.createPort, "22");
    setInputValue(elements.createUser, "");
    if (elements.createAuth) {
      elements.createAuth.value = "password";
    }
    setInputValue(elements.createPassword, "");
    setInputValue(elements.createKeyPath, "");
    setInputValue(elements.createKeyPassphrase, "");
    setInputValue(elements.createSshCmd, "");
    setInputValue(elements.createHelperPath, "");
    if (elements.createUploadHelper) {
      elements.createUploadHelper.checked = false;
    }
    setInputValue(elements.createHelperLocal, "");
    setInputValue(elements.createHelperSha, "");
    if (elements.createAnyHostKey) {
      elements.createAnyHostKey.checked = false;
    }
    setInputValue(elements.createCols, String(DEFAULT_COLS));
    setInputValue(elements.createRows, String(DEFAULT_ROWS));
    updateCreateType();
    updateCreateAuth();
    updateCreateHelper();
    updateCreateSubmit();
  }

  /** 设置输入框值；元素缺失时忽略。 */
  function setInputValue(element, value) {
    if (element) {
      element.value = value;
    }
  }

  /** 填充新建表单的绑定会话下拉；`preferred` 不存在时回退到 @web。 */
  function renderCreateSessions(sessions, preferred) {
    var select = elements.createRoot;
    if (!select) {
      return;
    }
    while (select.firstChild) {
      select.removeChild(select.firstChild);
    }
    var values = [];
    for (var i = 0; i < sessions.length; i += 1) {
      var session = sessions[i];
      if (!session || !session.root_session_id) {
        continue;
      }
      var label =
        session.root_session_id === WEB_ROOT_SESSION_ID
          ? webSessionLabel(session)
          : sessionLabel(session, false);
      appendOptionTo(select, session.root_session_id, label, session.root_session_id);
      values.push(session.root_session_id);
    }
    if (!values.length) {
      appendOptionTo(select, WEB_ROOT_SESSION_ID, WEB_ROOT_LABEL, WEB_ROOT_SESSION_ID);
      values.push(WEB_ROOT_SESSION_ID);
    }
    select.value = values.indexOf(preferred) >= 0 ? preferred : WEB_ROOT_SESSION_ID;
    select.disabled = false;
    updateCreateSubmit();
  }

  /** 展开新建弹窗。 */
  function showCreateDialog() {
    var dialog = elements.createDialog;
    if (!dialog) {
      return;
    }
    if (typeof dialog.showModal === "function") {
      if (!dialog.open) {
        dialog.showModal();
      }
    } else {
      dialog.setAttribute("open", "");
    }
  }

  /** 清空只用于创建请求的密码/口令输入。 */
  function clearCreateSecrets() {
    setInputValue(elements.createPassword, "");
    setInputValue(elements.createKeyPassphrase, "");
  }

  /** 关闭新建弹窗并清空密码/口令。 */
  function closeCreateDialog() {
    var dialog = elements.createDialog;
    clearCreateSecrets();
    if (!dialog) {
      return;
    }
    if (typeof dialog.close === "function") {
      if (dialog.open) {
        dialog.close();
      }
    } else {
      dialog.removeAttribute("open");
    }
  }

  /** 弹窗是否打开。 */
  function isCreateDialogOpen() {
    var dialog = elements.createDialog;
    return !!dialog && (dialog.open || dialog.hasAttribute("open"));
  }

  /** 切换 local / ssh 表单区块。 */
  function updateCreateType() {
    var ssh = !!elements.createType && elements.createType.value === "ssh";
    if (elements.createLocal) {
      elements.createLocal.classList.toggle("termx-form-hidden", ssh);
    }
    if (elements.createSsh) {
      elements.createSsh.classList.toggle("termx-form-hidden", !ssh);
    }
    updateCreateSubmit();
  }

  /** 切换密码 / 私钥认证区块。 */
  function updateCreateAuth() {
    var key = !!elements.createAuth && elements.createAuth.value === "key";
    if (elements.createPasswordGroup) {
      elements.createPasswordGroup.classList.toggle("termx-form-hidden", key);
    }
    if (elements.createKeyGroup) {
      elements.createKeyGroup.classList.toggle("termx-form-hidden", !key);
    }
  }

  /** 上传 helper 时显示本地路径组，并禁用互斥的远端 helper 路径。 */
  function updateCreateHelper() {
    var upload = !!elements.createUploadHelper && elements.createUploadHelper.checked;
    if (elements.createUploadGroup) {
      elements.createUploadGroup.classList.toggle("termx-form-hidden", !upload);
    }
    if (elements.createHelperPath) {
      elements.createHelperPath.disabled = upload;
    }
  }

  /** 按连接状态启用/禁用新建提交。 */
  function updateCreateSubmit() {
    if (!elements.createSubmit) {
      return;
    }
    elements.createSubmit.disabled = createBusy || !connected;
    if (elements.createCancel) {
      elements.createCancel.disabled = createBusy;
    }
  }

  /** 新建进行中禁用提交与取消。 */
  function setCreateBusy(value) {
    createBusy = !!value;
    updateCreateSubmit();
  }

  /** 弹窗内错误。 */
  function showCreateError(message) {
    if (!elements.createError) {
      return;
    }
    elements.createError.textContent = message;
    elements.createError.hidden = false;
  }

  /** 清空弹窗错误。 */
  function hideCreateError() {
    if (!elements.createError) {
      return;
    }
    elements.createError.textContent = "";
    elements.createError.hidden = true;
  }

  /** 提交 `terminal.create`；成功后刷新两级选择并选中新终端。 */
  function submitCreate(event) {
    if (event) {
      event.preventDefault();
    }
    if (!connected) {
      showCreateError("连接已断开");
      return;
    }
    var params;
    try {
      params = buildCreateParams();
    } catch (error) {
      showCreateError(error.message);
      return;
    }
    hideCreateError();
    setCreateBusy(true);
    setStatus("正在创建终端…");
    rpc("terminal.create", params).then(
      function (result) {
        setCreateBusy(false);
        var created = result || {};
        setInputValue(elements.createPassword, "");
        setInputValue(elements.createKeyPassphrase, "");
        closeCreateDialog();
        setStatus("已创建终端 " + (created.terminal_id || ""));
        refreshTerminals(created.terminal_id).then(function () {
          if (created.terminal_id) {
            selectTerminal(created.terminal_id, false);
          }
          announceList(lastTerminals);
        });
      },
      function (error) {
        setCreateBusy(false);
        showCreateError("创建失败：" + describeError(error));
        setStatus("创建失败：" + describeError(error));
      }
    );
  }

  /** 组装 `terminal.create` 参数；校验失败抛出可展示的错误。 */
  function buildCreateParams() {
    var name = (elements.createName && elements.createName.value || "").trim();
    if (!name) {
      throw new Error("名称不能为空");
    }
    var root = (elements.createRoot && elements.createRoot.value || "").trim();
    if (!root) {
      throw new Error("请选择绑定会话");
    }
    var cols = parseInteger(elements.createCols, 2, 1000, "列");
    var rows = parseInteger(elements.createRows, 1, 1000, "行");
    var type = elements.createType && elements.createType.value === "ssh" ? "ssh" : "local";
    var params = {
      mode: "pty",
      cols: cols,
      rows: rows,
      root_session_id: root,
      name: name
    };

    if (type === "ssh") {
      var host = (elements.createHost && elements.createHost.value || "").trim();
      var user = (elements.createUser && elements.createUser.value || "").trim();
      if (!host) {
        throw new Error("SSH 主机不能为空");
      }
      if (!user) {
        throw new Error("SSH 用户不能为空");
      }
      var port = parseInteger(elements.createPort, 1, 65535, "端口");
      params.cmd = (elements.createSshCmd && elements.createSshCmd.value || "").trim() || "shell";
      params.args = [];
      params.ssh = buildSshConfig(host, port, user);
      return params;
    }

    var cmd = (elements.createCmd && elements.createCmd.value || "").trim();
    if (!cmd) {
      throw new Error("本地终端需要程序（cmd）");
    }
    params.cmd = cmd;
    params.args = parseArgs(elements.createArgs && elements.createArgs.value || "");
    var cwd = (elements.createCwd && elements.createCwd.value || "").trim();
    if (cwd) {
      params.cwd = cwd;
    }
    var env = parseEnvLines(elements.createEnv && elements.createEnv.value || "");
    if (env) {
      params.env = env;
    }
    return params;
  }

  /** 组装 SSH 连接参数；密码/口令只随本次请求发送。 */
  function buildSshConfig(host, port, user) {
    var authMode = elements.createAuth && elements.createAuth.value === "key" ? "key" : "password";
    var auth;
    if (authMode === "key") {
      var path = (elements.createKeyPath && elements.createKeyPath.value || "").trim();
      if (!path) {
        throw new Error("私钥路径不能为空");
      }
      auth = { type: "key", path: path };
      var passphrase = elements.createKeyPassphrase && elements.createKeyPassphrase.value;
      if (passphrase) {
        auth.passphrase = passphrase;
      }
    } else {
      var password = elements.createPassword && elements.createPassword.value;
      if (!password) {
        throw new Error("密码不能为空");
      }
      auth = { type: "password", password: password };
    }

    var ssh = {
      host: host,
      port: port,
      user: user,
      auth: auth,
      upload_helper: !!(elements.createUploadHelper && elements.createUploadHelper.checked),
      accept_any_host_key: !!(elements.createAnyHostKey && elements.createAnyHostKey.checked)
    };
    var helperPath = (elements.createHelperPath && elements.createHelperPath.value || "").trim();
    var helperLocal = (elements.createHelperLocal && elements.createHelperLocal.value || "").trim();
    var helperSha = (elements.createHelperSha && elements.createHelperSha.value || "").trim();
    if (ssh.upload_helper) {
      if (!helperLocal) {
        throw new Error("上传 helper 时必须填写本地 helper 路径");
      }
      if (helperPath) {
        throw new Error("远端 helper 路径与上传 helper 不能同时使用");
      }
      ssh.helper_local_path = helperLocal;
    } else if (helperPath) {
      ssh.helper_path = helperPath;
    }
    if (helperSha) {
      if (!ssh.upload_helper && !ssh.helper_path) {
        throw new Error("helper sha256 需要远端 helper 路径或上传 helper");
      }
      ssh.helper_sha256 = helperSha;
    }
    return ssh;
  }

  /** 读取并校验整数输入。 */
  function parseInteger(element, min, max, label) {
    var raw = element ? String(element.value || "").trim() : "";
    var value = Number(raw);
    if (!raw || !isFinite(value) || Math.floor(value) !== value) {
      throw new Error(label + "必须是整数");
    }
    if (value < min || value > max) {
      throw new Error(label + "需要在 " + min + "–" + max + " 之间");
    }
    return value;
  }

  /** 解析空格分隔参数，支持单/双引号与空引号；未闭合引号报错。 */
  function parseArgs(text) {
    var args = [];
    var current = "";
    var quote = null;
    var hasToken = false;
    for (var i = 0; i < text.length; i += 1) {
      var ch = text.charAt(i);
      if (quote) {
        if (ch === quote) {
          quote = null;
        } else if (ch === "\\" && i + 1 < text.length && (text.charAt(i + 1) === quote || text.charAt(i + 1) === "\\")) {
          i += 1;
          current += text.charAt(i);
        } else {
          current += ch;
        }
        hasToken = true;
        continue;
      }
      if (ch === '"' || ch === "'") {
        quote = ch;
        hasToken = true;
        continue;
      }
      if (/\s/.test(ch)) {
        if (hasToken) {
          args.push(current);
          current = "";
          hasToken = false;
        }
        continue;
      }
      current += ch;
      hasToken = true;
    }
    if (quote) {
      throw new Error("参数中存在未闭合的引号");
    }
    if (hasToken) {
      args.push(current);
    }
    return args;
  }

  /** 解析 `KEY=VALUE` 行；空行与 `#` 注释忽略，无内容返回 null。 */
  function parseEnvLines(text) {
    var lines = String(text || "").split(/\r?\n/);
    var env = {};
    var count = 0;
    for (var i = 0; i < lines.length; i += 1) {
      var line = lines[i].trim();
      if (!line || line.charAt(0) === "#") {
        continue;
      }
      var index = line.indexOf("=");
      if (index <= 0) {
        throw new Error("环境变量行需要 KEY=VALUE：" + line);
      }
      env[line.slice(0, index).trim()] = line.slice(index + 1);
      count += 1;
    }
    return count ? env : null;
  }

  /** 终止当前终端。 */
  function terminateCurrent() {
    if (!currentTerminalId || !connected) {
      return;
    }
    var terminalId = currentTerminalId;
    if (!window.confirm("确定终止终端 " + terminalId + "？")) {
      return;
    }
    setStatus("正在终止终端…");
    rpc("terminal.terminate", { terminal_id: terminalId, force: true }).then(
      function () {
        setStatus("已请求终止终端");
        refreshTerminals();
      },
      function (error) {
        if (connected) {
          setStatus("终止失败：" + error.message);
        }
      }
    );
  }

  /**
   * 信息页顶部的终端元信息：state、pid、mode、启动命令与 cwd。
   *
   * 数据来自 `terminal.list` 条目或 `terminal.status` 快照（字段同名）。
   */
  function renderInfoMeta(item) {
    var container = elements.infoMeta;
    if (!container) {
      return;
    }
    while (container.firstChild) {
      container.removeChild(container.firstChild);
    }
    if (!item) {
      container.hidden = true;
      return;
    }
    container.hidden = false;
    appendMetaRow(container, "状态", item.state || "-");
    appendMetaRow(container, "PID", typeof item.pid === "number" ? String(item.pid) : "-");
    appendMetaRow(container, "模式", item.mode || "-");
    appendMetaRow(container, "启动", launchCommand(item));
    appendMetaRow(container, "目录", item.cwd || "-");
  }

  /** 追加一行元信息（label + value），文本走 textContent 避免注入。 */
  function appendMetaRow(container, label, value) {
    var row = document.createElement("div");
    row.className = "termx-meta-row";
    var name = document.createElement("span");
    name.className = "termx-meta-label";
    name.textContent = label + ":";
    var text = document.createElement("span");
    text.className = "termx-meta-value";
    text.textContent = value;
    row.appendChild(name);
    row.appendChild(text);
    container.appendChild(row);
  }

  /** 启动命令展示：cmd + args（含空白时加引号）。 */
  function launchCommand(item) {
    if (!item || !item.command) {
      return "-";
    }
    var parts = [quoteCommandPart(item.command)];
    var args = Array.isArray(item.args) ? item.args : [];
    for (var i = 0; i < args.length; i += 1) {
      parts.push(quoteCommandPart(args[i]));
    }
    return parts.join(" ");
  }

  /** 含空白的参数用双引号包裹（仅用于展示）。 */
  function quoteCommandPart(value) {
    var text = String(value == null ? "" : value);
    if (!text) {
      return '""';
    }
    return /\s/.test(text) ? '"' + text.replace(/"/g, '\\"') + '"' : text;
  }

  /** 读取一次 `terminal.status` 并更新信息页头部；过期选择直接丢弃。 */
  function refreshStatusMeta(terminalId, token) {
    if (!connected || !terminalId) {
      return;
    }
    rpc("terminal.status", { terminal_id: terminalId }).then(
      function (result) {
        if (token !== selectToken || currentTerminalId !== terminalId) {
          return;
        }
        if (result && result.snapshot) {
          renderInfoMeta(result.snapshot);
        }
      },
      function () {
        // 状态读取失败不覆盖列表已有的展示。
      }
    );
  }

  /** 监听容器尺寸变化。 */
  function observeResize() {
    if (typeof window.ResizeObserver === "function") {
      var observer = new window.ResizeObserver(function () {
        scheduleResize();
      });
      observer.observe(elements.term);
      return;
    }
    window.addEventListener("resize", scheduleResize);
  }

  /** 防抖后按容器尺寸同步 cols/rows。 */
  function scheduleResize() {
    if (resizeTimer) {
      window.clearTimeout(resizeTimer);
    }
    resizeTimer = window.setTimeout(applyResize, RESIZE_DEBOUNCE_MS);
  }

  /** 计算并提交终端尺寸。 */
  function applyResize() {
    resizeTimer = null;
    if (!term || !currentTerminalId) {
      return;
    }
    var rect = elements.term.getBoundingClientRect();
    if (rect.width <= 0 || rect.height <= 0) {
      return;
    }
    var cell = cellSize();
    var style = window.getComputedStyle(elements.term);
    var padX = pixelValue(style.paddingLeft) + pixelValue(style.paddingRight);
    var padY = pixelValue(style.paddingTop) + pixelValue(style.paddingBottom);
    var cols = Math.max(2, Math.floor((rect.width - padX) / cell.width));
    var rows = Math.max(1, Math.floor((rect.height - padY) / cell.height));
    if (!isFinite(cols) || !isFinite(rows)) {
      return;
    }
    if (lastSize && lastSize.cols === cols && lastSize.rows === rows) {
      return;
    }
    if (term.cols !== cols || term.rows !== rows) {
      term.resize(cols, rows);
    }
    lastSize = { cols: cols, rows: rows };
    rpc("terminal.resize", {
      terminal_id: currentTerminalId,
      cols: cols,
      rows: rows
    }).catch(function () {
      // 断线时由重连流程重新订阅并补发尺寸。
    });
  }

  /** 读取 xterm 字符尺寸；内部字段不可用时按字号估算。 */
  function cellSize() {
    try {
      var dimensions = term._core && term._core._renderService && term._core._renderService.dimensions;
      var cell = dimensions && dimensions.css && dimensions.css.cell;
      if (cell && cell.width > 0 && cell.height > 0) {
        return cell;
      }
    } catch (error) {
      // 回退到字号估算。
    }
    var fontSize = (term.options && term.options.fontSize) || FALLBACK_FONT_SIZE;
    return {
      width: Math.max(1, fontSize * 0.62),
      height: Math.max(1, Math.ceil(fontSize * 1.2))
    };
  }

  /** CSS 长度转像素。 */
  function pixelValue(value) {
    var parsed = parseFloat(value);
    return isFinite(parsed) ? parsed : 0;
  }

  /** 切换信息页/原始控制台；只切可见性，不销毁 xterm。 */
  function setView(mode) {
    viewMode = mode === "raw" ? "raw" : "info";
    var info = viewMode === "info";
    setTabState(elements.tabInfo, info);
    setTabState(elements.tabRaw, !info);
    if (elements.viewInfo) {
      elements.viewInfo.classList.toggle("termx-view-hidden", !info);
    }
    if (elements.viewRaw) {
      elements.viewRaw.classList.toggle("termx-view-hidden", info);
    }
    if (info) {
      startTranscriptPolling();
    } else {
      stopTranscriptPolling();
      applyResize();
      if (term) {
        term.focus();
      }
    }
  }

  /** 同步单个 Tab 的选中态。 */
  function setTabState(tab, active) {
    if (!tab) {
      return;
    }
    tab.classList.toggle("is-active", active);
    tab.setAttribute("aria-selected", active ? "true" : "false");
  }

  /** 清空信息页本地视图与游标（切换终端时调用）。 */
  function resetTranscriptView() {
    stopTranscriptPolling();
    transcriptRevision = null;
    transcriptInFlight = false;
    if (elements.transcript) {
      elements.transcript.textContent = "";
    }
    setInfoNotice("");
  }

  /** 信息页轮询：仅在已连接、选中终端且处于信息页时运行。 */
  function startTranscriptPolling() {
    if (transcriptTimer || viewMode !== "info" || !connected || !currentTerminalId) {
      return;
    }
    refreshTranscript();
    transcriptTimer = window.setInterval(refreshTranscript, TRANSCRIPT_POLL_MS);
  }

  /** 停止信息页轮询。 */
  function stopTranscriptPolling() {
    if (transcriptTimer) {
      window.clearInterval(transcriptTimer);
      transcriptTimer = null;
    }
  }

  /** 轮询命令记录并按修订号重渲染信息页。 */
  function refreshTranscript() {
    if (
      transcriptInFlight ||
      viewMode !== "info" ||
      !connected ||
      !currentTerminalId
    ) {
      return;
    }
    var terminalId = currentTerminalId;
    var token = selectToken;
    transcriptInFlight = true;
    rpc("terminal.transcript", {
      terminal_id: terminalId,
      limit: TRANSCRIPT_PAGE_SIZE
    }).then(
      function (result) {
        transcriptInFlight = false;
        if (token !== selectToken || currentTerminalId !== terminalId || !connected) {
          return;
        }
        if (!result || result.revision === transcriptRevision) {
          return;
        }
        transcriptRevision = result.revision;
        renderTranscriptCommands(result.commands || []);
      },
      function (error) {
        transcriptInFlight = false;
        if (token === selectToken && currentTerminalId === terminalId && connected) {
          setStatus("读取信息页失败：" + error.message);
        }
      }
    );
  }

  /**
   * 重渲染命令列表。
   *
   * 每条显示 `HH:MM:SS → HH:MM:SS [status]`、命令文本与清洗后输出；
   * 不显示 shell 提示符，也不重复回显命令。服务端已清洗，这里只排版。
   */
  function renderTranscriptCommands(commands) {
    var pre = elements.transcript;
    if (!pre) {
      return;
    }
    var atBottom =
      pre.scrollHeight - pre.scrollTop - pre.clientHeight < 24;
    var blocks = [];
    for (var i = 0; i < commands.length; i += 1) {
      var command = commands[i];
      if (!command) {
        continue;
      }
      var started = clockOf(command.started_at);
      var ended = command.ended_at ? clockOf(command.ended_at) : "…";
      var block =
        started +
        " → " +
        ended +
        " [" +
        (command.status || "unknown") +
        "]\n" +
        (command.command || "");
      if (typeof command.output === "string" && command.output) {
        block += "\n" + command.output;
      }
      blocks.push(block);
    }
    var text = blocks.join("\n\n");
    if (text.length > TRANSCRIPT_UI_MAX_CHARS) {
      text = text.slice(-TRANSCRIPT_UI_MAX_CHARS);
    }
    pre.textContent = text;
    if (atBottom) {
      pre.scrollTop = pre.scrollHeight;
    }
  }

  /** 从 RFC3339 时间取 `HH:MM:SS`；格式异常时原样返回。 */
  function clockOf(timestamp) {
    var match = /T(\d\d:\d\d:\d\d)/.exec(String(timestamp || ""));
    return match ? match[1] : String(timestamp || "");
  }

  /** 信息页输入框提交：claim 后 `terminal.send(text, enter=true)`。 */
  function submitInfoCommand(event) {
    if (event) {
      event.preventDefault();
    }
    if (!currentTerminalId || !connected) {
      setStatus("未连接或未选择终端");
      return;
    }
    if (terminalEnded) {
      setInfoNotice("终端已结束，不能再发送命令");
      return;
    }
    var input = elements.infoInput;
    var text = input ? input.value : "";
    if (!text) {
      return;
    }
    var terminalId = currentTerminalId;
    claimTerminal()
      .then(function (ok) {
        if (!ok || currentTerminalId !== terminalId || !connected) {
          return false;
        }
        return rpc("terminal.send", {
          terminal_id: terminalId,
          text: text,
          enter: true
        });
      })
      .then(function (sent) {
        if (sent === false) {
          return;
        }
        if (currentTerminalId === terminalId && input) {
          input.value = "";
          input.focus();
          window.setTimeout(refreshTranscript, 120);
        }
      })
      .catch(function (error) {
        if (connected && currentTerminalId === terminalId) {
          setStatus("发送失败：" + error.message);
        }
      });
  }

  /** 信息页提示条（终态、错误、transcript reset）。 */
  function setInfoNotice(text) {
    transcriptNotice = text || "";
    if (elements.infoNotice) {
      elements.infoNotice.textContent = transcriptNotice;
      elements.infoNotice.hidden = !transcriptNotice;
    }
  }
  /** 更新状态文本。 */
  function setStatus(text) {
    if (elements.status) {
      elements.status.textContent = text;
    }
  }

  /** 记录连接状态并同步控件可用性。 */
  function setConnected(value) {
    connected = value;
    if (!value) {
      stopTranscriptPolling();
    }
    updateControls();
  }

  /** 按连接与选中状态启用/禁用控件。 */
  function updateControls() {
    if (elements.refresh) {
      elements.refresh.disabled = !connected;
    }
    if (elements.sessionSelect) {
      elements.sessionSelect.disabled = !connected || !lastSessions.length;
    }
    if (elements.select) {
      elements.select.disabled = !connected || !selectedSession;
    }
    if (elements.createOpen) {
      elements.createOpen.disabled = !connected;
    }
    if (elements.terminate) {
      elements.terminate.disabled = !connected || !currentTerminalId;
    }
    if (elements.rebindOpen) {
      elements.rebindOpen.disabled = !connected || !currentTerminalId;
    }
    if (elements.infoInput) {
      elements.infoInput.disabled = !connected || !currentTerminalId || terminalEnded;
    }
    if (elements.infoSend) {
      elements.infoSend.disabled = !connected || !currentTerminalId || terminalEnded;
    }
    if (!connected) {
      closeCreateDialog();
    }
    if (!connected || !currentTerminalId) {
      closeRebindDialog();
    }
    updateRebindSubmit();
    updateCreateSubmit();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();




