import path from "path";
import { createRequire } from "module";
import { fileURLToPath, pathToFileURL } from "url";

const require = createRequire(import.meta.url);
const scriptFilePath = fileURLToPath(import.meta.url);
const rootDir = path.resolve(path.dirname(scriptFilePath), "..");
const cjsModule = require(path.join(rootDir, "dist", "index.js"));
const esmModule = await import(
  pathToFileURL(path.join(rootDir, "index.mjs")).href
);

const requiredExports = [
  "ClientIdentifier",
  "Emulation",
  "CookieJar",
  "MultipartForm",
  "createMultipartForm",
  "TLSClient",
  "Session",
  "fetch",
  "TLSClientError",
  "TLSResponse",
  "TLSWebSocket",
];

for (const exportName of requiredExports) {
  if (!(exportName in cjsModule)) {
    throw new Error(`Missing CommonJS export: ${exportName}`);
  }

  if (!(exportName in esmModule)) {
    throw new Error(`Missing ESM export: ${exportName}`);
  }
}

if (
  typeof cjsModule.TLSClient !== "function" ||
  typeof esmModule.TLSClient !== "function"
) {
  throw new Error("TLSClient export is not a constructor.");
}

if (cjsModule.Emulation.chrome_136 !== cjsModule.ClientIdentifier.chrome_136) {
  throw new Error("Emulation export does not mirror ClientIdentifier.");
}

if (
  typeof cjsModule.MultipartForm !== "function" ||
  typeof cjsModule.createMultipartForm !== "function"
) {
  throw new Error("Multipart helper exports are invalid.");
}

if (
  typeof cjsModule.TLSWebSocket !== "function" ||
  typeof esmModule.TLSWebSocket !== "function"
) {
  throw new Error("TLSWebSocket export is not a constructor.");
}

const cjsSocketIoModule = require(path.join(rootDir, "dist", "socket-io.js"));
const esmSocketIoModule = await import(
  pathToFileURL(path.join(rootDir, "socket-io.mjs")).href
);

if (typeof cjsSocketIoModule.TLSWebSocketTransport !== "function") {
  throw new Error("Missing CommonJS export: TLSWebSocketTransport");
}

if (typeof esmSocketIoModule.TLSWebSocketTransport !== "function") {
  throw new Error("Missing ESM export: TLSWebSocketTransport");
}

console.log(
  JSON.stringify({ ok: true, exportsChecked: requiredExports.length + 1 }),
);
