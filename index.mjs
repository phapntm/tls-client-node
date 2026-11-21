import pkg from "./dist/index.js";

export const {
  ClientIdentifier,
  Emulation,
  CookieJar,
  MultipartForm,
  createMultipartForm,
  createTLSAdapter,
  TLSClient,
  Session,
  fetch,
  TLSClientError,
  TLSResponse,
  TLSWebSocket,
} = pkg;

export default pkg;
