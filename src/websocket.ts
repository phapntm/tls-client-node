import { EventEmitter } from "node:events";
import WebSocket from "ws";
import { TLSClientError } from "./errors";
import { WebSocketBridgeConfigV1, WebSocketData } from "./types";

export interface TLSWebSocketBootstrapContext {
  isCancelled(): boolean;
}

export type TLSWebSocketBootstrapResult = {
  socket: WebSocket;
};

export type TLSWebSocketBootstrap = (
  context: TLSWebSocketBootstrapContext,
) => Promise<TLSWebSocketBootstrapResult>;

export type TLSWebSocketBinaryType = "nodebuffer" | "arraybuffer" | "fragments";

export interface TLSWebSocketOpenEvent {
  type: "open";
}

export interface TLSWebSocketMessageEvent {
  type: "message";
  data: WebSocketData;
  target: TLSWebSocket;
}

export interface TLSWebSocketCloseEvent {
  type: "close";
  code: number;
  reason: string;
  wasClean: boolean;
  target: TLSWebSocket;
}

export interface TLSWebSocketErrorEvent {
  type: "error";
  error: Error;
  message: string;
  target: TLSWebSocket;
}

// Base64URL(UTF8(JSON.stringify(config))) per the tls-client-api bridge contract.
export function encodeWebSocketBridgeConfig(
  config: WebSocketBridgeConfigV1,
): string {
  return Buffer.from(JSON.stringify(config), "utf8").toString("base64url");
}

// Rewrites the runtime's http(s) base URL into the ws(s) /api/ws bridge endpoint.
export function buildBridgeWebSocketUrl(httpBaseUrl: string): string {
  const parsed = new URL(httpBaseUrl);
  parsed.protocol = parsed.protocol === "https:" ? "wss:" : "ws:";
  parsed.pathname = `${parsed.pathname.replace(/\/+$/, "")}/api/ws`;
  parsed.search = "";
  parsed.hash = "";
  return parsed.toString();
}

function toMessagePayload(
  data: WebSocket.RawData,
  isBinary: boolean,
): WebSocketData {
  if (isBinary) {
    return data as WebSocketData;
  }

  if (typeof data === "string") {
    return data;
  }

  if (Buffer.isBuffer(data)) {
    return data.toString("utf8");
  }

  if (Array.isArray(data)) {
    return Buffer.concat(data).toString("utf8");
  }

  return Buffer.from(data as ArrayBuffer).toString("utf8");
}

/**
 * Generic WebSocket client that composes a local `ws` connection to the
 * tls-client-api `/api/ws` bridge. It never connects directly to the remote
 * target; the bridge owns the remote connection.
 */
export class TLSWebSocket extends EventEmitter {
  public static readonly CONNECTING = 0;
  public static readonly OPEN = 1;
  public static readonly CLOSING = 2;
  public static readonly CLOSED = 3;

  public readonly CONNECTING = TLSWebSocket.CONNECTING;
  public readonly OPEN = TLSWebSocket.OPEN;
  public readonly CLOSING = TLSWebSocket.CLOSING;
  public readonly CLOSED = TLSWebSocket.CLOSED;

  public onopen: ((event: TLSWebSocketOpenEvent) => void) | null = null;
  public onmessage: ((event: TLSWebSocketMessageEvent) => void) | null = null;
  public onclose: ((event: TLSWebSocketCloseEvent) => void) | null = null;
  public onerror: ((event: TLSWebSocketErrorEvent) => void) | null = null;

  private readonly _url: string;
  private _socket?: WebSocket;
  private _readyState = TLSWebSocket.CONNECTING;
  private _protocol = "";
  private _binaryType: TLSWebSocketBinaryType = "nodebuffer";
  private _bootstrapCancelled = false;
  private _closeEmitted = false;
  private readonly _domListeners = new Map<
    string,
    WeakMap<(event: unknown) => void, (...args: unknown[]) => void>
  >();

  constructor(url: string, bootstrap: TLSWebSocketBootstrap) {
    super();
    this._url = url;
    // EventEmitter throws synchronously on an 'error' emit with zero listeners.
    // Consumers using only the DOM-style `onerror` property (no `.on('error')`)
    // must not crash the process, so this permanent no-op keeps emit() safe.
    this.on("error", () => {});
    // Bootstrap runs asynchronously; every code path inside is caught so this
    // never produces an unhandled rejection.
    void this._runBootstrap(bootstrap);
  }

  public get readyState(): number {
    return this._readyState;
  }

  public get url(): string {
    return this._url;
  }

  public get protocol(): string {
    return this._protocol;
  }

  /** Extensions negotiated with the local bridge, not with the remote target. */
  public get extensions(): string {
    return this._socket?.extensions ?? "";
  }

  public get bufferedAmount(): number {
    return this._socket?.bufferedAmount ?? 0;
  }

  public get binaryType(): TLSWebSocketBinaryType {
    return this._binaryType;
  }

  public set binaryType(value: TLSWebSocketBinaryType) {
    this._binaryType = value;
    if (this._socket) {
      this._socket.binaryType = value;
    }
  }

  public send(data: WebSocketData, callback?: (error?: Error) => void): void {
    if (!this._socket || this._readyState !== TLSWebSocket.OPEN) {
      const error = new TLSClientError(
        `WebSocket is not open: readyState ${this._readyState}`,
        { code: "ERR_WS_NOT_OPEN" },
      );

      if (callback) {
        callback(error);
        return;
      }

      throw error;
    }

    this._socket.send(data as never, callback);
  }

  public close(code?: number, reason?: string | Buffer): void {
    if (
      this._readyState === TLSWebSocket.CLOSING ||
      this._readyState === TLSWebSocket.CLOSED
    ) {
      return;
    }

    if (this._socket) {
      this._readyState = TLSWebSocket.CLOSING;
      this._socket.close(code, reason);
      return;
    }

    // Bootstrap has not produced a socket yet: cancel it so no bridge
    // connection is ever created, then transition straight to CLOSED.
    this._bootstrapCancelled = true;
    this._readyState = TLSWebSocket.CLOSING;
    const reasonText = Buffer.isBuffer(reason)
      ? reason.toString("utf8")
      : (reason ?? "");
    queueMicrotask(() => this._emitClose(code ?? 1000, reasonText));
  }

  public terminate(): void {
    if (this._readyState === TLSWebSocket.CLOSED) {
      return;
    }

    if (this._socket) {
      this._socket.terminate();
      return;
    }

    this._bootstrapCancelled = true;
    this._readyState = TLSWebSocket.CLOSING;
    queueMicrotask(() => this._emitClose(1006, ""));
  }

  public addEventListener(
    type: string,
    listener: (event: unknown) => void,
  ): void {
    let listenersForType = this._domListeners.get(type);
    if (!listenersForType) {
      listenersForType = new WeakMap();
      this._domListeners.set(type, listenersForType);
    }

    if (listenersForType.has(listener)) {
      return;
    }

    const wrapped = this._wrapDomListener(type, listener);
    listenersForType.set(listener, wrapped);
    this.on(type, wrapped);
  }

  public removeEventListener(
    type: string,
    listener: (event: unknown) => void,
  ): void {
    const wrapped = this._domListeners.get(type)?.get(listener);
    if (wrapped) {
      this.off(type, wrapped);
      this._domListeners.get(type)?.delete(listener);
    }
  }

  private _wrapDomListener(
    type: string,
    listener: (event: unknown) => void,
  ): (...args: unknown[]) => void {
    switch (type) {
      case "message":
        return (data: unknown) => listener({ type, data, target: this });
      case "close":
        return (code: unknown, reason: unknown) =>
          listener({
            type,
            code,
            reason,
            wasClean: code === 1000,
            target: this,
          });
      case "error":
        return (error: unknown) =>
          listener({
            type,
            error,
            message: error instanceof Error ? error.message : String(error),
            target: this,
          });
      default:
        return () => listener({ type, target: this });
    }
  }

  private async _runBootstrap(bootstrap: TLSWebSocketBootstrap): Promise<void> {
    let socket: WebSocket;

    try {
      const result = await bootstrap({
        isCancelled: () => this._bootstrapCancelled,
      });
      socket = result.socket;
    } catch (error) {
      if (this._bootstrapCancelled) {
        // close()/terminate() already drove the state transition; a setup
        // error after cancellation is expected and must stay silent.
        return;
      }

      this._handleError(TLSClientError.fromUnknown(error));
      this._emitClose(1006, "");
      return;
    }

    if (this._bootstrapCancelled) {
      try {
        socket.terminate();
      } catch {
        // Socket was never fully established; ignore teardown failures.
      }
      return;
    }

    this._attachSocket(socket);
  }

  private _attachSocket(socket: WebSocket): void {
    this._socket = socket;
    socket.binaryType = this._binaryType;

    if (socket.readyState === WebSocket.OPEN) {
      this._handleOpen(socket);
    }

    socket.on("open", () => this._handleOpen(socket));
    socket.on("message", (data, isBinary) =>
      this._handleMessage(data, isBinary),
    );
    socket.on("close", (code, reason) =>
      this._emitClose(code, reason.toString("utf8")),
    );
    socket.on("error", (error) => this._handleError(error));
    socket.on("unexpected-response", (_request, response) => {
      const error = new TLSClientError(
        `tls-client-api bridge returned an unexpected response: ${response.statusCode}`,
        {
          code: "ERR_WS_BRIDGE_UNEXPECTED_RESPONSE",
          status: response.statusCode,
        },
      );
      this._handleError(error);
      try {
        socket.terminate();
      } catch {
        // Best-effort teardown; the connection never completed anyway.
      }
      this._emitClose(1006, "");
    });
  }

  private _handleOpen(socket: WebSocket): void {
    if (this._readyState !== TLSWebSocket.CONNECTING) {
      return;
    }

    this._readyState = TLSWebSocket.OPEN;
    this._protocol = socket.protocol ?? "";
    this.emit("open");
    this.onopen?.({ type: "open" });
  }

  private _handleMessage(data: WebSocket.RawData, isBinary: boolean): void {
    const payload = toMessagePayload(data, isBinary);
    this.emit("message", payload, isBinary);
    this.onmessage?.({ type: "message", data: payload, target: this });
  }

  private _handleError(error: Error): void {
    this.emit("error", error);
    this.onerror?.({
      type: "error",
      error,
      message: error.message,
      target: this,
    });
  }

  private _emitClose(code: number, reason: string): void {
    if (this._closeEmitted) {
      return;
    }

    this._closeEmitted = true;
    this._readyState = TLSWebSocket.CLOSED;
    this.emit("close", code, reason);
    this.onclose?.({
      type: "close",
      code,
      reason,
      wasClean: code === 1000,
      target: this,
    });
  }
}
