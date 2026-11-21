// Optional Engine.IO/Socket.IO adapter. Thin bridge over Session.websocket();
// tls-client-node does not implement Socket.IO itself. Kept out of the core
// entry point so engine.io-client is never required for ordinary usage.
import { NodeWebSocket } from "engine.io-client";
import type { Session } from "./client";
import type { TLSWebSocket } from "./websocket";
import type { HeadersShape } from "./types";

export interface TLSWebSocketTransportOptions {
  tlsSession: Session;
  extraHeaders?: HeadersShape;
  headerOrder?: string[];
  /** Bridge-to-target compression offer; undefined inherits the Session default. */
  enableCompression?: boolean;
}

function normalizeRemoteHeaders(opts: Record<string, unknown>): HeadersShape {
  const headers = opts.headers;
  return (headers ?? {}) as HeadersShape;
}

function normalizeProtocols(
  protocols: string | string[] | undefined,
): string[] | undefined {
  if (protocols === undefined) {
    return undefined;
  }

  return Array.isArray(protocols) ? protocols : [protocols];
}

/**
 * Adapts Engine.IO's WebSocket transport creation to Session.websocket().
 * Holds no proxy, TLS profile, cookie jar, or runtime management logic of its
 * own; all of that belongs to Session. See TLSWebSocketTransportOptions for
 * the extra fields expected on this transport's options (passed through
 * Engine.IO's `transportOptions.websocket`).
 */
export class TLSWebSocketTransport extends NodeWebSocket {
  public createSocket(
    uri: string,
    protocols: string | string[] | undefined,
    opts: Record<string, any>,
  ): TLSWebSocket {
    const { tlsSession, headerOrder, enableCompression } = this
      .opts as unknown as TLSWebSocketTransportOptions;

    return tlsSession.websocket(uri, {
      protocols: normalizeProtocols(protocols),
      headers: normalizeRemoteHeaders(opts),
      headerOrder,
      enableCompression,
    });
  }
}
