// Smoke test for the OPTIONAL Engine.IO/Socket.IO adapter (TLSWebSocketTransport).
// Kept separate from scripts/validate-websocket.mjs per the architecture: this
// file is the only place engine.io-client is touched, and it must not affect
// core WebSocket behavior. Skips gracefully if engine.io-client isn't installed.
import assert from "node:assert/strict";
import http from "node:http";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const scriptFilePath = fileURLToPath(import.meta.url);
const rootDir = path.resolve(path.dirname(scriptFilePath), "..");

let engineIoClientAvailable = true;
try {
  require.resolve("engine.io-client");
} catch {
  engineIoClientAvailable = false;
}

if (!engineIoClientAvailable) {
  console.log(
    JSON.stringify({
      ok: true,
      skipped: true,
      reason:
        "engine.io-client is not installed; optional adapter smoke test skipped",
    }),
  );
  process.exit(0);
}

const { TLSClient } = require(path.join(rootDir, "dist", "index.js"));
const { TLSWebSocketTransport } = require(
  path.join(rootDir, "dist", "socket-io.js"),
);
const { WebSocketServer } = require("ws");

const TEST_API_KEY = "unit-test-bridge-key";

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

function decodeBridgeConfig(headerValue) {
  return JSON.parse(Buffer.from(headerValue, "base64url").toString("utf8"));
}

// Same minimal /api/ws stand-in used by the core smoke test, duplicated here
// (rather than imported) so adapter and core WebSocket tests stay independent.
function createBridgeStub() {
  const connections = [];
  const server = http.createServer((_req, res) => {
    res.writeHead(404).end();
  });

  const wss = new WebSocketServer({ noServer: true });

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

    const target = new URL(config.requestUrl);
    if (target.searchParams.get("simulateFailure") === "1") {
      socket.write("HTTP/1.1 502 Bad Gateway\r\n\r\n");
      socket.destroy();
      return;
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
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
        port,
        baseUrl: `http://127.0.0.1:${port}`,
        close: () => new Promise((r) => server.close(r)),
      });
    });
  });
}

// Constructs a bare Engine.IO Transport (bypassing the higher-level Socket
// class, which speaks the full Engine.IO protocol) so these tests can assert
// on transport-level open/close/packet behavior against a plain WS stub.
function createTransport(
  session,
  { port, extraQuery = {}, extraHeaders, enableCompression, perMessageDeflate } = {},
) {
  return new TLSWebSocketTransport({
    query: { EIO: 4, transport: "websocket", ...extraQuery },
    socket: { binaryType: "nodebuffer" },
    hostname: "127.0.0.1",
    port,
    secure: false,
    path: "/engine.io/",
    forceBase64: false,
    tlsSession: session,
    extraHeaders,
    enableCompression,
    perMessageDeflate,
  });
}

const bridge = await createBridgeStub();
const client = new TLSClient({ baseUrl: bridge.baseUrl, apiKey: TEST_API_KEY });
const session = client.session();

// 1: custom Engine.IO transport creation.
const transport = createTransport(session, {
  port: bridge.port,
  extraHeaders: { Origin: "https://example.test" },
});
assert.equal(transport.name, "websocket");

// 8: reconnection remains Socket.IO's responsibility, never implemented here.
assert.equal(typeof transport.reconnect, "undefined");

// 5: connect.
transport.open();
await once(transport, "open");

// 2, 3, 4: target Engine.IO URL preserved, with EIO=4 and transport=websocket.
const conn = bridge.connections.at(-1);
assert.match(
  conn.config.requestUrl,
  /^ws:\/\/127\.0\.0\.1:\d+\/engine\.io\/\?EIO=4&transport=websocket$/,
);
assert.equal(conn.config.headers.origin, "https://example.test");

// 7: binary payload round-trips through real engine.io-parser encode/decode.
const binaryPacket = once(transport, "packet");
transport.send([{ type: "message", data: Buffer.from([9, 8, 7]) }]);
const [received] = await binaryPacket;
assert.equal(received.type, "message");
assert.ok(Buffer.isBuffer(received.data));
assert.deepEqual([...received.data], [9, 8, 7]);

// 6: disconnect.
transport.close();
await once(transport, "close");

// 9: remote failure never produces a false open/connect.
const failingTransport = createTransport(session, {
  port: bridge.port,
  extraQuery: { simulateFailure: "1" },
});
let openFired = false;
failingTransport.on("open", () => {
  openFired = true;
});
failingTransport.on("error", () => {});

failingTransport.open();
await once(failingTransport, "close");
assert.equal(openFired, false);

// Assert both the adapter's raw option forwarding and the final encoded bridge
// config. This proves config propagation, not target compression negotiation.
for (const [name, sessionOptions, transportOptions, expected] of [
  ["default-off", {}, {}, false],
  ["transport-on", {}, { enableCompression: true }, true],
  ["transport-off", {}, { enableCompression: false }, false],
  ["session-on", { enableCompression: true }, {}, true],
  [
    "transport-off-overrides-session-on",
    { enableCompression: true },
    { enableCompression: false },
    false,
  ],
  [
    "transport-on-overrides-session-off",
    { enableCompression: false },
    { enableCompression: true },
    true,
  ],
  [
    "engine-io-deflate-does-not-enable-remote",
    {},
    { perMessageDeflate: { threshold: 1024 } },
    false,
  ],
  [
    "engine-io-no-deflate-does-not-disable-remote",
    { enableCompression: true },
    { perMessageDeflate: false },
    true,
  ],
]) {
  const compressionSession = client.session(sessionOptions);
  const originalWebsocket = compressionSession.websocket.bind(compressionSession);
  let forwardedOptions;
  compressionSession.websocket = (url, options) => {
    forwardedOptions = options;
    return originalWebsocket(url, options);
  };

  const compressionTransport = createTransport(compressionSession, {
    port: bridge.port,
    extraQuery: { compressionCase: name },
    ...transportOptions,
  });
  compressionTransport.open();
  await once(compressionTransport, "open");

  assert.equal(
    forwardedOptions.enableCompression,
    transportOptions.enableCompression,
    name,
  );
  const compressionConn = bridge.connections.at(-1);
  assert.equal(
    new URL(compressionConn.config.requestUrl).searchParams.get("compressionCase"),
    name,
  );
  assert.equal(compressionConn.config.version, 1, name);
  assert.equal(compressionConn.config.enableCompression, expected, name);
  assert.equal(compressionConn.localHeaders["sec-websocket-extensions"], undefined, name);
  assert.equal(compressionConn.localHeaders.enablecompression, undefined, name);
  assert.equal(compressionConn.config.headers["sec-websocket-extensions"], undefined, name);
  assert.equal(compressionTransport.ws.extensions, "", name);

  const closed = once(compressionTransport, "close");
  compressionTransport.close();
  await closed;
}

await client.stop();
await bridge.close();

console.log(
  JSON.stringify({
    ok: true,
    scenariosChecked: [
      "custom Engine.IO transport creation",
      "no built-in reconnect",
      "connect",
      "target Engine.IO URL preserved (EIO=4, transport=websocket)",
      "binary payload round-trip",
      "disconnect",
      "remote failure never opens",
      "compression true, false, and undefined forwarded unchanged",
      "compression inheritance and transport overrides in encoded bridge config",
      "Engine.IO perMessageDeflate independent of remote compression",
      "no local compression offer or fabricated remote extensions",
    ],
  }),
);
