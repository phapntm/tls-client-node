// Smoke test for Session.websocket()/TLSWebSocket against a local stub bridge
// server that stands in for tls-client-api's /api/ws (no Socket.IO involved).
import assert from "node:assert/strict";
import http from "node:http";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const scriptFilePath = fileURLToPath(import.meta.url);
const rootDir = path.resolve(path.dirname(scriptFilePath), "..");

const { TLSClient, TLSWebSocket, ClientIdentifier } = require(
  path.join(rootDir, "dist", "index.js"),
);
const binaryModule = require(path.join(rootDir, "dist", "binary.js"));
const nativeModule = require(path.join(rootDir, "dist", "native.js"));
const { WebSocketServer } = require("ws");

const TEST_API_KEY = "unit-test-bridge-key";

const unhandledRejections = [];
process.on("unhandledRejection", (reason) => {
  unhandledRejections.push(reason);
});

function decodeBridgeConfig(headerValue) {
  return JSON.parse(Buffer.from(headerValue, "base64url").toString("utf8"));
}

// Only an expected open rejects on error, so connection failures fail fast.
// Waiting for close still permits scenarios that emit both error and close.
function once(emitter, event) {
  return new Promise((resolve, reject) => {
    const onError = (error) => {
      emitter.off(event, onEvent);
      reject(error);
    };
    const onEvent = (...args) => {
      if (event === "open") emitter.off("error", onError);
      resolve(args);
    };
    emitter.once(event, onEvent);
    if (event === "open") emitter.once("error", onError);
  });
}

// Minimal stand-in for the tls-client-api /api/ws bridge: behavior branches
// off the decoded config's requestUrl path so one server covers every case.
function createBridgeStub() {
  const connections = [];
  const server = http.createServer((_req, res) => {
    res.writeHead(404).end();
  });

  const wss = new WebSocketServer({
    noServer: true,
    handleProtocols: (protocols) =>
      protocols.size ? protocols.values().next().value : false,
  });

  server.on("upgrade", (req, socket, head) => {
    if (req.url !== "/api/ws") {
      socket.destroy();
      return;
    }

    const apiKey = req.headers["x-api-key"];
    const configHeader = req.headers["x-tls-client-ws-config"];

    if (apiKey !== TEST_API_KEY || !configHeader) {
      socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
      socket.destroy();
      return;
    }

    const config = decodeBridgeConfig(configHeader);
    connections.push({ config, localHeaders: { ...req.headers } });

    const pathname = new URL(config.requestUrl).pathname;

    if (pathname === "/remote-fail") {
      socket.write("HTTP/1.1 502 Bad Gateway\r\n\r\n");
      socket.destroy();
      return;
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
      if (pathname === "/abnormal") {
        ws.terminate();
        return;
      }

      if (pathname === "/remote-close") {
        setImmediate(() => ws.close(4001, "remote closed"));
        return;
      }

      ws.on("message", (data, isBinary) => {
        ws.send(data, { binary: isBinary });
      });
    });
  });

  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      resolve({
        connections,
        baseUrl: `http://127.0.0.1:${port}`,
        close: () => new Promise((r) => server.close(r)),
      });
    });
  });
}

const bridge = await createBridgeStub();

function makeClient(overrides = {}) {
  return new TLSClient({
    baseUrl: bridge.baseUrl,
    apiKey: TEST_API_KEY,
    ...overrides,
  });
}

function findConnection(requestUrl) {
  return bridge.connections.find(
    (entry) => entry.config.requestUrl === requestUrl,
  );
}

// 1, 2, 3, 4, 6, 7, 9, 24: returns immediately in CONNECTING, handlers
// registered before bootstrap survive, OPEN fires, text/binary echo works,
// normal close transitions to CLOSED and Session stops tracking the socket.
{
  const client = makeClient();
  const session = client.session({
    clientIdentifier: ClientIdentifier.chrome_136,
  });
  const ws = session.websocket("wss://example.test/basic-flow", {
    enableCompression: true,
  });

  assert.ok(ws instanceof TLSWebSocket);
  assert.equal(ws.readyState, TLSWebSocket.CONNECTING);

  let openFiredBeforeAwait = false;
  ws.on("open", () => {
    openFiredBeforeAwait = true;
  });

  await once(ws, "open");
  assert.equal(ws.readyState, TLSWebSocket.OPEN);
  assert.equal(openFiredBeforeAwait, true);

  const textEcho = once(ws, "message");
  ws.send("hello-text");
  const [textReceived] = await textEcho;
  assert.equal(textReceived, "hello-text");

  const binaryEcho = once(ws, "message");
  ws.send(Buffer.from([1, 2, 3, 4]));
  const [binaryReceived] = await binaryEcho;
  assert.ok(Buffer.isBuffer(binaryReceived));
  assert.deepEqual([...binaryReceived], [1, 2, 3, 4]);

  const closeEvent = once(ws, "close");
  ws.close(1000, "done");
  const [code, reason] = await closeEvent;
  assert.equal(code, 1000);
  assert.equal(reason, "done");
  assert.equal(ws.readyState, TLSWebSocket.CLOSED);
  assert.equal(session.activeWebSockets.size, 0);

  await client.stop();
}

// Compression config forwarding only: this stub never negotiates compression
// with a target server. The internal upgrade must stay uncompressed in all cases.
for (const [name, sessionOptions, socketOptions, expected] of [
  ["default-off", {}, {}, false],
  ["socket-on", {}, { enableCompression: true }, true],
  ["socket-off", {}, { enableCompression: false }, false],
  ["session-on", { enableCompression: true }, {}, true],
  [
    "socket-off-overrides-session-on",
    { enableCompression: true },
    { enableCompression: false },
    false,
  ],
  [
    "socket-on-overrides-session-off",
    { enableCompression: false },
    { enableCompression: true },
    true,
  ],
]) {
  const client = makeClient();
  const session = client.session(sessionOptions);
  const url = `wss://example.test/compression-${name}`;
  const ws = session.websocket(url, socketOptions);
  assert.equal(ws.readyState, TLSWebSocket.CONNECTING, name);
  await once(ws, "open");

  const conn = findConnection(url);
  assert.equal(conn.config.version, 1, name);
  assert.equal(conn.config.enableCompression, expected, name);
  assert.match(conn.localHeaders["x-tls-client-ws-config"], /^[A-Za-z0-9_-]+$/);
  assert.equal(conn.localHeaders["sec-websocket-extensions"], undefined, name);
  assert.equal(conn.localHeaders.enablecompression, undefined, name);
  assert.equal(conn.config.headers["sec-websocket-extensions"], undefined, name);
  assert.equal(conn.config.headers.enablecompression, undefined, name);
  assert.equal(ws.extensions, "", name);

  const closed = once(ws, "close");
  ws.close();
  await closed;
  await client.stop();
}

// 5: remote failure (bridge upgrade rejected) never emits OPEN.
{
  const client = makeClient();
  const session = client.session();
  const ws = session.websocket("wss://example.test/remote-fail");
  let openFired = false;
  ws.on("open", () => {
    openFired = true;
  });
  ws.on("error", () => {});

  const [code] = await once(ws, "close");
  assert.equal(openFired, false);
  assert.equal(code, 1006);
  assert.equal(ws.readyState, TLSWebSocket.CLOSED);

  await client.stop();
}

// 10: server-initiated close becomes a normal TLSWebSocket close.
{
  const client = makeClient();
  const session = client.session();
  const ws = session.websocket("wss://example.test/remote-close");
  ws.on("error", () => {});

  await once(ws, "open");
  const [code, reason] = await once(ws, "close");
  assert.equal(code, 4001);
  assert.equal(reason, "remote closed");

  await client.stop();
}

// 11: abnormal termination surfaces as an unclean close (code 1006).
{
  const client = makeClient();
  const session = client.session();
  const ws = session.websocket("wss://example.test/abnormal");
  ws.on("error", () => {});

  await once(ws, "open");
  const [code] = await once(ws, "close");
  assert.equal(code, 1006);

  await client.stop();
}

// 12: close() before bootstrap finishes cancels setup; no bridge connection
// is ever created and no unhandled rejection is produced.
{
  const client = makeClient();
  const session = client.session();
  const ws = session.websocket("wss://example.test/close-before-bootstrap");
  ws.close();
  assert.equal(ws.readyState, TLSWebSocket.CLOSING);

  await once(ws, "close");
  assert.equal(ws.readyState, TLSWebSocket.CLOSED);

  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(
    findConnection("wss://example.test/close-before-bootstrap"),
    undefined,
  );

  await client.stop();
}

// 13: cookie jar integration - cookies already in the Session jar are
// forwarded as a Cookie header without a separate cookie store.
{
  const client = makeClient();
  const session = client.session();
  await session.cookieJar.setCookie(
    "session=abc123; Domain=example.test; Path=/",
    "https://example.test/",
  );

  const ws = session.websocket("wss://example.test/cookie-echo");
  await once(ws, "open");

  const conn = findConnection("wss://example.test/cookie-echo");
  assert.equal(conn.config.headers.cookie, "session=abc123");

  ws.close();
  await once(ws, "close");
  await client.stop();
}

// 14, 15, 16, 17, 18: Session headers/proxy/profile inheritance, per-socket
// header override, and canonical bridge config shape (no remote header leaks
// onto the local bridge upgrade request).
{
  const client = makeClient();
  const session = client.session({
    clientIdentifier: ClientIdentifier.chrome_136,
    proxyUrl: "http://127.0.0.1:40001",
    headers: {
      Origin: "https://example.test",
      "X-Session-Header": "session-value",
    },
  });

  const ws = session.websocket("wss://example.test/header-inherit", {
    headers: { "X-Session-Header": "override-value" },
  });
  await once(ws, "open");

  const conn = findConnection("wss://example.test/header-inherit");
  assert.equal(conn.config.version, 1);
  assert.equal(conn.config.requestUrl, "wss://example.test/header-inherit");
  assert.equal(conn.config.tlsClientIdentifier, "chrome_136");
  assert.equal(conn.config.proxyUrl, "http://127.0.0.1:40001");
  assert.equal(conn.config.headers["x-session-header"], "override-value");
  assert.equal(conn.config.headers.origin, "https://example.test");
  assert.equal(conn.localHeaders.origin, undefined);
  assert.equal(conn.localHeaders["x-session-header"], undefined);
  assert.ok(conn.localHeaders["x-api-key"]);
  assert.ok(conn.localHeaders["x-tls-client-ws-config"]);

  ws.close();
  await once(ws, "close");

  const wsOverride = session.websocket(
    "wss://example.test/header-inherit-override",
    {
      proxyUrl: "http://127.0.0.1:50002",
      tlsClientIdentifier: ClientIdentifier.firefox_135,
    },
  );
  await once(wsOverride, "open");

  const overrideConn = findConnection(
    "wss://example.test/header-inherit-override",
  );
  assert.equal(overrideConn.config.proxyUrl, "http://127.0.0.1:50002");
  assert.equal(overrideConn.config.tlsClientIdentifier, "firefox_135");

  wsOverride.close();
  await once(wsOverride, "close");
  await client.stop();
}

// 8: subprotocols flow through the bridge config and the negotiated protocol
// is exposed on TLSWebSocket.protocol.
{
  const client = makeClient();
  const session = client.session();
  const ws = session.websocket("wss://example.test/protocol-echo", {
    protocols: ["graphql-transport-ws", "other-proto"],
  });
  await once(ws, "open");

  assert.equal(ws.protocol, "graphql-transport-ws");
  const conn = findConnection("wss://example.test/protocol-echo");
  assert.deepEqual(conn.config.protocols, [
    "graphql-transport-ws",
    "other-proto",
  ]);

  ws.close();
  await once(ws, "close");
  await client.stop();
}

// 19: managed runtime sharing - concurrent websockets share a single lazily
// started runtime instead of spawning one process per socket.
{
  const originalEnsureBinary = binaryModule.ensureBinary;
  const originalSupportsNativeRuntime = binaryModule.supportsNativeRuntime;
  let ensureBinaryCalls = 0;
  const sentinel = new Error("managed-runtime-sentinel");

  binaryModule.supportsNativeRuntime = () => false;
  binaryModule.ensureBinary = async () => {
    ensureBinaryCalls += 1;
    throw sentinel;
  };

  try {
    const managedClient = new TLSClient();
    const managedSession = managedClient.session();
    const wsA = managedSession.websocket("wss://example.test/managed-a");
    const wsB = managedSession.websocket("wss://example.test/managed-b");
    wsA.on("error", () => {});
    wsB.on("error", () => {});

    await Promise.all([once(wsA, "close"), once(wsB, "close")]);

    assert.equal(ensureBinaryCalls, 1);
    assert.equal(managedSession.activeWebSockets.size, 0);
  } finally {
    binaryModule.ensureBinary = originalEnsureBinary;
    binaryModule.supportsNativeRuntime = originalSupportsNativeRuntime;
  }
}

// 21: native-library mode returns a clear unsupported error and never opens.
{
  const originalEnsureNativeBinding = nativeModule.ensureNativeBinding;
  nativeModule.ensureNativeBinding = async () => ({
    libraryPath: "stub-native",
    async request() {
      throw new Error("not used in this test");
    },
    async getCookiesFromSession() {
      return { id: "native-cookies", cookies: [] };
    },
    async destroySession() {
      return { id: "native-session", success: true };
    },
    async destroyAll() {
      return { id: "native-destroy-all", success: true };
    },
  });

  try {
    const nativeClient = new TLSClient({ runtimeMode: "native" });
    const nativeSession = nativeClient.session();
    const ws = nativeSession.websocket("wss://example.test/native-unsupported");

    let openFired = false;
    let capturedError;
    ws.on("open", () => {
      openFired = true;
    });
    ws.on("error", (error) => {
      capturedError = error;
    });

    const [code] = await once(ws, "close");

    assert.equal(openFired, false);
    assert.equal(code, 1006);
    assert.equal(capturedError?.code, "ERR_UNSUPPORTED_RUNTIME_OPERATION");
    assert.equal(ws.readyState, TLSWebSocket.CLOSED);

    await nativeClient.stop();
  } finally {
    nativeModule.ensureNativeBinding = originalEnsureNativeBinding;
  }
}

// 20: remote runtime mode - every case above already runs against a
// TLSClient configured with baseUrl/apiKey, proving Session.websocket()
// targets the configured remote runtime's /api/ws endpoint.
assert.ok(bridge.connections.length > 0);

// 22, 24: multiple concurrent generic WebSockets on one Session, independent
// send/receive, and no stale entries left in Session tracking after close.
{
  const client = makeClient();
  const session = client.session();
  const sockets = Array.from({ length: 5 }, (_, i) =>
    session.websocket(`wss://example.test/concurrent-${i}`),
  );

  await Promise.all(sockets.map((ws) => once(ws, "open")));
  assert.equal(session.activeWebSockets.size, 5);

  await Promise.all(
    sockets.map(async (ws, i) => {
      const messageEvent = once(ws, "message");
      ws.send(`payload-${i}`);
      const [data] = await messageEvent;
      assert.equal(data, `payload-${i}`);
    }),
  );

  await Promise.all(
    sockets.map(async (ws) => {
      const closeEvent = once(ws, "close");
      ws.close(1000);
      await closeEvent;
    }),
  );

  assert.equal(session.activeWebSockets.size, 0);
  await client.stop();
}

await bridge.close();

// 23: no unhandled promise rejections across the whole run.
await new Promise((resolve) => setImmediate(resolve));
assert.deepEqual(unhandledRejections, []);

console.log(
  JSON.stringify({
    ok: true,
    scenariosChecked: [
      "immediate CONNECTING return",
      "handlers registered before bootstrap",
      "successful OPEN",
      "remote failure never opens",
      "text send/receive",
      "binary send/receive",
      "subprotocol negotiation",
      "normal close",
      "remote-initiated close",
      "abnormal failure",
      "close before bootstrap completes",
      "cookie jar integration",
      "session header inheritance + per-socket override",
      "proxy inheritance + override",
      "profile inheritance + override",
      "canonical bridge config encoding",
      "remote compression defaults off",
      "per-socket compression true and false",
      "session compression inheritance and per-socket overrides",
      "no local compression offer or fabricated remote extensions",
      "managed runtime sharing",
      "remote runtime mode",
      "unsupported native mode",
      "multiple concurrent sockets",
      "no stale session tracking",
      "no unhandled promise rejections",
    ],
  }),
);
