import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { Readable } from "node:stream";

const require = createRequire(import.meta.url);
const axios = require("axios");
const native = require("../dist/native.js");
const binary = require("../dist/binary.js");
let starts = 0;
const calls = [];
const closed = [];
let handler;
const unhandled = [];
process.on("unhandledRejection", error => unhandled.push(error));
binary.ensureBinary = async () => { throw new Error("Native adapter must not use an EXE"); };
native.ensureNativeBinding = async options => {
  assert.equal(options.runtimeMode, "native");
  starts++;
  return {
    request: async payload => {
      calls.push(payload);
      return handler(payload);
    },
    destroySession: async payload => { closed.push(payload.sessionId); return { success: true }; },
    destroyAll: async () => { throw new Error("Must not destroy another adapter's sessions"); },
  };
};
const { createTLSAdapter } = require("tls-client-node");
const identity = { proxy: "http://user:pass@127.0.0.1:8080", userAgent: "Adapter-Test-UA", clientIdentifier: "chrome_136" };
for (const invalid of [undefined, {}, { ...identity, proxy: undefined }, { ...identity, userAgent: "" }, { ...identity, clientIdentifier: "" }]) {
  assert.throws(() => createTLSAdapter(invalid), TypeError);
}
const first = createTLSAdapter(identity);
const second = createTLSAdapter({ ...identity, proxy: "", userAgent: "Second-UA", clientIdentifier: "firefox_135" });
assert.notEqual(first.session.id, second.session.id);
assert.equal(starts, 0);
const http = axios.create({ adapter: first.adapter, baseURL: "https://example.test/api" });
const other = axios.create({ adapter: second.adapter });

function response(payload, { status = 200, body = Buffer.from('{"ok":true}'), cookies = {}, headers = {} } = {}) {
  return {
    id: "test", status, body: "data:text/plain; charset=utf-8;base64," + body.toString("base64"),
    target: payload.requestUrl, sessionId: payload.sessionId, cookies, headers,
  };
}

handler = async p => response(p, { cookies: { token: "first" }, headers: { "Set-Cookie": ["token=first; Path=/", "extra=1; Path=/"] } });
const result = await http.post("/echo", { hello: "world" }, {
  params: { query: "a b" }, headers: { "uSeR-aGeNt": "wrong" }, auth: { username: "user", password: "pass" },
});
assert.deepEqual(result.data, { ok: true });
assert.equal(result.statusText, "OK");
assert.deepEqual(result.headers["set-cookie"], ["token=first; Path=/", "extra=1; Path=/"]);
assert.equal(result.request, first.session);
let last = calls.at(-1);
assert.equal(last.requestUrl, "https://example.test/api/echo?query=a+b");
assert.equal(last.requestBody, '{"hello":"world"}');
assert.equal(last.headers["user-agent"], identity.userAgent);
assert.equal(last.headers.authorization, "Basic dXNlcjpwYXNz");
assert.equal(last.proxyUrl, identity.proxy);
assert.equal(last.tlsClientIdentifier, identity.clientIdentifier);
assert.equal(last.isByteResponse, true);
assert.equal(starts, 1);

await http.get("/cookies", { params: { ignored: true }, paramsSerializer: () => "custom=yes", maxRedirects: 0 });
last = calls.at(-1);
assert.equal(last.requestUrl, "https://example.test/api/cookies?custom=yes");
assert.equal(last.followRedirects, false);
assert.ok(last.requestCookies.some(c => c.name === "token" && c.value === "first"));
await other.get("https://example.test/");
last = calls.at(-1);
assert.equal(last.proxyUrl, "");
assert.equal(last.headers["user-agent"], "Second-UA");
assert.equal(last.tlsClientIdentifier, "firefox_135");
assert.equal(last.requestCookies.length, 0);
assert.equal(starts, 1);

handler = async p => response(p, { status: 404, body: Buffer.from('{"missing":true}') });
await assert.rejects(http.get("/missing"), error =>
  axios.isAxiosError(error) && error.code === "ERR_BAD_REQUEST" &&
  error.response.status === 404 && error.response.data.missing === true);
assert.equal((await http.get("/missing", { validateStatus: () => true })).status, 404);
assert.equal((await http.get("/missing", { validateStatus: null })).status, 404);
handler = async p => response(p, { status: 302 });
await assert.rejects(http.get("/redirect", { maxRedirects: 0 }), error =>
  error.code === "ERR_BAD_RESPONSE" && error.response.status === 302);
handler = async p => response(p, { status: 503 });
await assert.rejects(http.get("/unavailable"), error => error.code === "ERR_BAD_RESPONSE");

const raw = Buffer.from([0, 255, 128, 65]);
handler = async p => response(p, { body: raw });
assert.deepEqual((await http.get("/binary", { responseType: "arraybuffer" })).data, raw);
const stream = (await http.get("/binary", { responseType: "stream" })).data;
const chunks = [];
for await (const chunk of stream) chunks.push(chunk);
assert.deepEqual(Buffer.concat(chunks), raw);
await assert.rejects(http.get("/large", { maxContentLength: 1 }), error => error.code === "ERR_BAD_RESPONSE");

handler = async p => response(p);
await http.post("/binary-upload", raw);
assert.equal(calls.at(-1).requestBody, raw.toString("base64"));
assert.equal(calls.at(-1).isByteRequest, true);
await http.post("/urlencoded", new URLSearchParams({ key: "value" }));
assert.equal(calls.at(-1).requestBody, "key=value");
const multipart = new FormData();
multipart.append("field", "value");
await http.post("/multipart", multipart, { headers: { "Content-Type": "multipart/form-data; boundary=wrong", "Content-Length": "1" } });
last = calls.at(-1);
assert.match(last.headers["content-type"], /^multipart\/form-data; boundary=/);
assert.ok(!last.headers["content-type"].endsWith("wrong"));
assert.ok(!("content-length" in last.headers));
assert.match(Buffer.from(last.requestBody, "base64").toString(), /name="field"/);
await http.post("/blob", new Blob(["blob-data"], { type: "text/plain" }));
assert.equal(calls.at(-1).headers["content-type"], "text/plain");
await assert.rejects(http.post("/stream-upload", Readable.from(["x"])), error => error.code === "ERR_NOT_SUPPORT" && error.config.url === "/stream-upload");
await assert.rejects(http.get("/blob-response", { responseType: "blob" }), error => error.code === "ERR_NOT_SUPPORT");
await assert.rejects(http.request({ url: "/", method: "TRACE" }), error => error.code === "ERR_NOT_SUPPORT");

const already = new AbortController();
already.abort();
let count = calls.length;
await assert.rejects(http.get("/abort-before", { signal: already.signal }), axios.isCancel);
assert.equal(calls.length, count);
let complete;
handler = p => new Promise(resolve => { complete = () => resolve(response(p)); });
const abort = new AbortController();
const pending = http.get("/abort-during", { signal: abort.signal });
await new Promise(resolve => setImmediate(resolve));
abort.abort();
await assert.rejects(pending, error => axios.isCancel(error) && error.code === "ERR_CANCELED");
complete();
const token = axios.CancelToken.source();
const canceled = http.get("/cancel-token", { cancelToken: token.token });
await new Promise(resolve => setImmediate(resolve));
token.cancel("stop here");
await assert.rejects(canceled, error => axios.isCancel(error) && error.message === "stop here");
complete();
const timeout = http.get("/timeout", { timeout: 10, transitional: { clarifyTimeoutError: true } });
await assert.rejects(timeout, error => error.code === "ETIMEDOUT");
complete();

// A timeout while Blob is being prepared must not send the request later.
handler = async p => response(p);
const slowBlob = new Blob(["later"]);
slowBlob.arrayBuffer = () => new Promise(resolve => setTimeout(() => resolve(new ArrayBuffer(0)), 30));
count = calls.length;
await assert.rejects(http.post("/slow-body", slowBlob, { timeout: 5 }), error => error.code === "ECONNABORTED");
await new Promise(resolve => setTimeout(resolve, 40));
assert.equal(calls.length, count);

handler = async () => { throw new Error("connection failed"); };
await assert.rejects(http.get("/network"), error => axios.isAxiosError(error) && error.code === "ERR_NETWORK" && error.cause.message === "connection failed");
await first.session.close();
assert.deepEqual(closed, [first.session.id]);
handler = async p => response(p);
await other.get("https://example.test/still-open");
await second.session.close();
await new Promise(resolve => setImmediate(resolve));
assert.deepEqual(unhandled, []);

console.log(JSON.stringify({
  ok: true, axios: axios.VERSION, nativeStarts: starts,
  scenarios: ["native-only runtime", "identity/session isolation", "URL/params", "transformed bodies", "Basic auth", "cookies", "validateStatus", "binary/stream responses", "multipart/Blob", "errors", "cancellation", "timeout", "session cleanup"],
}));
