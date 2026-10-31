import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import type { ServerMessageV1 } from "@famashines/ai-systems-contracts";
import { waitFor } from "@famashines/ai-systems-kit/testing";
import { exportJWK, generateKeyPair, SignJWT, type CryptoKey } from "jose";
import { WebSocket } from "ws";

export const audience = "ai-systems-app";

export interface TokenOptions {
  sub?: string;
  roles?: string[];
  expInSec?: number;
}

export interface Jwks {
  issuer: string;
  token(options?: TokenOptions): Promise<string>;
  close(): Promise<void>;
}

export const listen = async (server: Server): Promise<string> => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return `http://127.0.0.1:${String((server.address() as AddressInfo).port)}`;
};

export async function startJwks(): Promise<Jwks> {
  const { privateKey, publicKey } = await generateKeyPair("RS256", { extractable: true });
  const jwk = { ...(await exportJWK(publicKey)), kid: "k1", alg: "RS256", use: "sig" };
  const server = createServer((_request, response) => {
    response
      .writeHead(200, { "content-type": "application/json" })
      .end(JSON.stringify({ keys: [jwk] }));
  });
  const issuer = `${await listen(server)}/realms/ai-systems`;
  return {
    issuer,
    token: (options = {}) => sign(privateKey, issuer, options),
    close: () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(() => {
          resolve();
        });
      }),
  };
}

const sign = (key: CryptoKey, issuer: string, options: TokenOptions): Promise<string> =>
  new SignJWT({
    typ: "Bearer",
    azp: audience,
    realm_access: { roles: options.roles ?? ["owner"] },
  })
    .setProtectedHeader({ alg: "RS256", kid: "k1" })
    .setIssuer(issuer)
    .setAudience(audience)
    .setSubject(options.sub ?? "owner-1")
    .setIssuedAt()
    .setExpirationTime(Math.floor(Date.now() / 1000) + (options.expInSec ?? 300))
    .sign(key);

export interface Closed {
  code: number;
  reason: string;
}

export interface Client {
  ws: WebSocket;
  frames: ServerMessageV1[];
  closed: Promise<Closed>;
  send(message: unknown): void;
  next<T extends ServerMessageV1["type"]>(
    type: T,
    timeoutMs?: number,
  ): Promise<Extract<ServerMessageV1, { type: T }>>;
  notices(): Extract<ServerMessageV1, { type: "notice" }>[];
  close(): Promise<void>;
}

export interface ConnectOptions {
  origin?: string;
  autoPong?: boolean;
  path?: string;
}

export async function connect(base: string, options: ConnectOptions = {}): Promise<Client> {
  const ws = new WebSocket(`${base.replace(/^http/, "ws")}${options.path ?? "/ws"}`, {
    ...(options.origin === undefined ? {} : { origin: options.origin }),
    autoPong: options.autoPong ?? true,
  });
  const frames: ServerMessageV1[] = [];
  let read = 0;
  ws.on("message", (data: Buffer) => {
    frames.push(JSON.parse(data.toString("utf8")) as ServerMessageV1);
  });
  const closed = new Promise<Closed>((resolve) => {
    ws.once("close", (code: number, reason: Buffer) => {
      resolve({ code, reason: reason.toString("utf8") });
    });
  });
  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => {
      resolve();
    });
    ws.once("error", reject);
    ws.once("unexpected-response", (_request, response) => {
      reject(new Error(`upgrade refused: ${String(response.statusCode)}`));
    });
  });
  ws.on("error", () => undefined);
  return {
    ws,
    frames,
    closed,
    send: (message) => {
      ws.send(typeof message === "string" ? message : JSON.stringify(message));
    },
    async next(type, timeoutMs = 5000) {
      let found: ServerMessageV1 | undefined;
      await waitFor(
        () => {
          const index = frames.findIndex((frame, i) => i >= read && frame.type === type);
          if (index === -1) return false;
          read = index + 1;
          found = frames[index];
          return true;
        },
        timeoutMs,
        10,
      );
      return found as Extract<ServerMessageV1, { type: typeof type }>;
    },
    notices: () =>
      frames.filter(
        (frame): frame is Extract<ServerMessageV1, { type: "notice" }> => frame.type === "notice",
      ),
    async close() {
      if (ws.readyState === WebSocket.CLOSED) return;
      ws.close();
      const timer = setTimeout(() => {
        ws.terminate();
      }, 1000);
      await closed;
      clearTimeout(timer);
    },
  };
}

export const taskTopic = (): `task:${string}` => `task:${randomUUID()}`;
