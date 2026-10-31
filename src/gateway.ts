import { randomUUID } from "node:crypto";
import { STATUS_CODES, type IncomingMessage, type Server } from "node:http";
import type { Duplex } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";
import {
  viewSubject,
  WsCloseCode,
  WsLimits,
  type ClientMessageV1,
  type ServerMessageV1,
  type Topic,
} from "@famashines/ai-systems-contracts";
import type { JwtVerifier } from "@famashines/ai-systems-kit/auth";
import { UnavailableError } from "@famashines/ai-systems-kit/http";
import { metrics } from "@opentelemetry/api";
import type { Logger } from "pino";
import { WebSocketServer, type WebSocket } from "ws";
import {
  isTooOld,
  parseClientFrame,
  parseDelta,
  parseViewMessage,
  rateWindow,
  subscriptions,
  taskOf,
  untilExpiry,
  type StreamHead,
  type Subscriptions,
} from "./protocol.ts";

export interface ViewMessage {
  seq: number;
  subject: string;
  data: string;
}

export interface Views {
  head(): Promise<StreamHead>;
  consume(
    subjects: string[],
    startSeq: number,
    onMessage: (message: ViewMessage) => void,
    onError: (error: unknown) => void,
    onGone: () => void,
  ): Promise<() => Promise<void>>;
  deltas(taskId: string, onMessage: (subject: string, data: string) => void): () => void;
}

export interface Timers {
  helloTimeoutMs: number;
  pingIntervalMs: number;
  pongTimeoutMs: number;
  closeGraceMs: number;
  sessionsEndMs: number;
}

export interface Limits {
  maxConnections: number;
  maxPerSubject: number;
}

export interface GatewayOptions {
  server: Server;
  views: Views;
  verify: JwtVerifier;
  allowedOrigins: readonly string[];
  logger: Logger;
  timers?: Partial<Timers>;
  limits?: Partial<Limits>;
}

export interface Gateway {
  connections(): number;
  close(drainDelayMs?: number): Promise<void>;
}

const unavailable = 1013;

function refuse(socket: Duplex, status: number): void {
  socket.on("error", () => socket.destroy());
  socket.end(
    `HTTP/1.1 ${String(status)} ${STATUS_CODES[status] ?? ""}\r\nconnection: close\r\ncontent-length: 0\r\n\r\n`,
  );
}

export function attachGateway(options: GatewayOptions): Gateway {
  const { server, views, verify, allowedOrigins, logger } = options;
  const timers: Timers = {
    helloTimeoutMs: WsLimits.helloTimeoutMs,
    pingIntervalMs: WsLimits.pingIntervalMs,
    pongTimeoutMs: WsLimits.pongTimeoutMs,
    closeGraceMs: 1000,
    sessionsEndMs: 5000,
    ...options.timers,
  };
  const limits: Limits = { maxConnections: 48, maxPerSubject: 8, ...options.limits };
  const perSubject = new Map<string, number>();
  const meter = metrics.getMeter("ai-systems-socket-gateway");
  const opened = meter.createCounter("ws_connections_opened", {
    description: "WebSocket connections that passed the upgrade",
  });
  const closed = meter.createCounter("ws_connections_closed", {
    description: "WebSocket connections that closed, by close code",
  });
  const framesIn = meter.createCounter("ws_frames_received", {
    description: "Frames from clients",
  });
  const framesOut = meter.createCounter("ws_frames_sent", {
    description: "Frames to clients, by message type",
  });
  const droppedDeltas = meter.createCounter("ws_deltas_dropped", {
    description: "Delta frames not sent because the send buffer was full",
  });
  const badViews = meter.createCounter("ws_view_messages_invalid", {
    description: "VIEWS messages that are not a valid ViewMessageV1",
  });
  const refused = meter.createCounter("ws_upgrades_refused", {
    description: "Upgrade requests refused, by HTTP status",
  });
  const stopFailures = meter.createCounter("ws_consumer_stop_failures", {
    description: "VIEWS consumers that did not stop or were not deleted",
  });
  const live = meter.createObservableGauge("ws_connections", {
    description: "Open WebSocket connections",
  });
  const wss = new WebSocketServer({ noServer: true, maxPayload: WsLimits.clientFrameBytes });
  const sessions = new Set<Promise<void>>();
  let stopping = false;
  live.addCallback((result) => {
    result.observe(wss.clients.size);
  });

  server.on("upgrade", (request: IncomingMessage, socket: Duplex, head: Buffer) => {
    const path = (request.url ?? "/").split("?")[0];
    const origin = request.headers.origin;
    const status =
      path !== "/ws"
        ? 404
        : stopping
          ? 503
          : origin !== undefined && !allowedOrigins.includes(origin)
            ? 403
            : undefined;
    if (status !== undefined) {
      refused.add(1, { status });
      refuse(socket, status);
      return;
    }
    wss.handleUpgrade(request, socket, head, (ws) => {
      opened.add(1);
      const ended = session(ws);
      sessions.add(ended);
      void ended.finally(() => sessions.delete(ended));
    });
  });

  function session(ws: WebSocket): Promise<void> {
    const sessionId = randomUUID();
    const log = logger.child({ sessionId });
    const allow = rateWindow();
    let state: "hello" | "open" | "closed" = "hello";
    let sub = "";
    let subs: Subscriptions | undefined;
    let stopConsumer: (() => Promise<void>) | undefined;
    const deltaSubs = new Map<string, () => void>();
    let queue = Promise.resolve();
    let closeCode: number | undefined;
    let expiry: NodeJS.Timeout | undefined;
    let pongTimer: NodeJS.Timeout | undefined;
    let graceTimer: NodeJS.Timeout | undefined;
    let counted = false;

    const send = (message: ServerMessageV1): void => {
      if (state === "closed") return;
      ws.send(JSON.stringify(message));
      framesOut.add(1, { type: message.type });
      if (ws.bufferedAmount > WsLimits.closeOverBytes)
        end(WsCloseCode.slowConsumer, "slow consumer");
    };

    const end = (code: number, reason: string): void => {
      if (state === "closed") return;
      state = "closed";
      closeCode = code;
      ws.close(code, reason);
      graceTimer = setTimeout(() => {
        ws.terminate();
      }, timers.closeGraceMs);
    };

    const stopQuietly = async (stop: (() => Promise<void>) | undefined): Promise<void> => {
      try {
        await stop?.();
      } catch (error) {
        stopFailures.add(1);
        log.error({ err: error }, "the VIEWS consumer did not stop cleanly");
      }
    };

    const helloTimer = setTimeout(() => {
      end(WsCloseCode.auth, "no hello");
    }, timers.helloTimeoutMs);

    const heartbeat = setInterval(() => {
      if (pongTimer !== undefined) return;
      pongTimer = setTimeout(() => {
        log.info("no pong, closing");
        closeCode ??= 1006;
        state = "closed";
        ws.terminate();
      }, timers.pongTimeoutMs);
      ws.ping();
    }, timers.pingIntervalMs);

    if (wss.clients.size > limits.maxConnections) end(unavailable, "too many connections");

    ws.on("pong", () => {
      clearTimeout(pongTimer);
      pongTimer = undefined;
    });

    const watchExpiry = (exp: number): void => {
      clearTimeout(expiry);
      expiry = setTimeout(
        () => {
          end(WsCloseCode.auth, "token expired");
        },
        untilExpiry(exp, Date.now()),
      );
    };

    const restart = async (): Promise<void> => {
      const stop = stopConsumer;
      stopConsumer = undefined;
      await stopQuietly(stop);
      if (!subs || state === "closed") return;
      const topics = subs.topics();
      if (topics.length === 0) return;
      const current = subs;
      stopConsumer = await views.consume(
        topics.map(viewSubject),
        current.start(),
        (message) => {
          deliver(current, message);
        },
        (error: unknown) => {
          log.error({ err: error }, "the VIEWS consumer failed");
          end(unavailable, "consumer failed");
        },
        () => {
          log.warn("the VIEWS stream or the consumer is gone");
          end(WsCloseCode.serverRestart, "stream changed");
        },
      );
    };

    const deliver = (current: Subscriptions, message: ViewMessage): void => {
      const notice = parseViewMessage(message.seq, message.subject, message.data);
      if (!notice) badViews.add(1);
      if (current.accept(message.seq, notice?.topic) && notice) send(notice);
    };

    const watchDeltas = (topics: readonly Topic[], on: boolean): void => {
      for (const topic of topics) {
        const taskId = taskOf(topic);
        if (taskId === undefined) continue;
        deltaSubs.get(taskId)?.();
        deltaSubs.delete(taskId);
        if (!on || state === "closed") continue;
        deltaSubs.set(
          taskId,
          views.deltas(taskId, (subject, data) => {
            if (ws.bufferedAmount > WsLimits.dropDeltasOverBytes) {
              droppedDeltas.add(1);
              return;
            }
            const delta = parseDelta(subject, data);
            if (delta) send(delta);
          }),
        );
      }
    };

    const authenticate = async (token: string): Promise<{ sub: string; exp: number } | number> => {
      try {
        const subject = await verify(token);
        return subject.exp * 1000 > Date.now() ? subject : WsCloseCode.auth;
      } catch (error) {
        return error instanceof UnavailableError ? unavailable : WsCloseCode.auth;
      }
    };

    const hello = async (frame: Extract<ClientMessageV1, { type: "hello" }>): Promise<void> => {
      clearTimeout(helloTimer);
      const subject = await authenticate(frame.token);
      if (typeof subject === "number") {
        end(subject, "authentication failed");
        return;
      }
      const head = await views.head();
      if (state === "closed") return;
      const count = perSubject.get(subject.sub) ?? 0;
      if (count >= limits.maxPerSubject) {
        end(WsCloseCode.rateLimit, "too many connections");
        return;
      }
      perSubject.set(subject.sub, count + 1);
      counted = true;
      sub = subject.sub;
      state = "open";
      watchExpiry(subject.exp);
      const tooOld = frame.lastSeq !== undefined && isTooOld(frame.lastSeq, head);
      subs = subscriptions(tooOld ? undefined : frame.lastSeq);
      send({ type: "welcome", sessionId, headSeq: head.last, heartbeatMs: timers.pingIntervalMs });
      if (tooOld) send({ type: "resync", reason: "too_old" });
    };

    const reauth = async (token: string): Promise<void> => {
      const subject = await authenticate(token);
      if (subject === unavailable) {
        send({ type: "error", code: "unavailable", message: "The token cannot be checked now" });
      } else if (typeof subject === "number" || subject.sub !== sub) {
        end(WsCloseCode.auth, "reauth failed");
      } else {
        watchExpiry(subject.exp);
      }
    };

    const subscribe = async (current: Subscriptions, topics: Topic[]): Promise<void> => {
      const last = current.needsHead() ? (await views.head()).last : 0;
      const result = current.add(topics, last);
      if (!result) {
        send({
          type: "error",
          code: "validation_failed",
          message: `At most ${String(WsLimits.maxTopics)} topics for each connection`,
        });
        return;
      }
      send({ type: "subscribed", topics, fromSeq: result.fromSeq });
      if (result.added.length > 0) {
        watchDeltas(result.added, true);
        await restart();
      }
    };

    const unsubscribe = async (current: Subscriptions, topics: Topic[]): Promise<void> => {
      const removed = current.remove(topics);
      watchDeltas(removed, false);
      if (removed.length > 0) await restart();
    };

    const handle = async (frame: ClientMessageV1): Promise<void> => {
      if (state === "closed") return;
      if (state === "hello" || !subs) {
        if (frame.type === "hello") await hello(frame);
        else end(WsCloseCode.auth, "hello first");
        return;
      }
      if (frame.type === "hello") end(WsCloseCode.badMessage, "hello again");
      else if (frame.type === "ping") send({ type: "pong", t: frame.t });
      else if (frame.type === "reauth") await reauth(frame.token);
      else if (frame.type === "subscribe") await subscribe(subs, frame.topics);
      else if (frame.type === "unsubscribe") await unsubscribe(subs, frame.topics);
    };

    ws.on("message", (data: Buffer, isBinary: boolean) => {
      framesIn.add(1);
      if (state === "closed") return;
      if (!allow(Date.now())) {
        end(WsCloseCode.rateLimit, "rate limit");
        return;
      }
      const frame = isBinary ? undefined : parseClientFrame(data.toString("utf8"));
      if (!frame) {
        end(WsCloseCode.badMessage, "bad message");
        return;
      }
      queue = queue
        .then(() => handle(frame))
        .catch((error: unknown) => {
          log.error({ err: error }, "frame handling failed");
          end(unavailable, "try again later");
        });
    });

    ws.on("error", (error) => {
      log.debug({ err: error }, "websocket error");
    });

    return new Promise<void>((resolve) => {
      ws.once("close", (code: number) => {
        state = "closed";
        clearTimeout(helloTimer);
        clearTimeout(expiry);
        clearTimeout(pongTimer);
        clearTimeout(graceTimer);
        clearInterval(heartbeat);
        if (counted) {
          const left = (perSubject.get(sub) ?? 1) - 1;
          if (left > 0) perSubject.set(sub, left);
          else perSubject.delete(sub);
        }
        closed.add(1, { code: closeCode ?? code });
        for (const stop of deltaSubs.values()) stop();
        deltaSubs.clear();
        void queue.then(() => stopQuietly(stopConsumer)).finally(resolve);
      });
    });
  }

  const within = async (promise: Promise<unknown>, ms: number): Promise<boolean> => {
    const timer = new AbortController();
    const expired = sleep(ms, false, { signal: timer.signal }).catch(() => false);
    const result = await Promise.race([promise.then(() => true), expired]);
    timer.abort();
    return result;
  };

  return {
    connections: () => wss.clients.size,
    async close(drainDelayMs = 0) {
      stopping = true;
      await sleep(drainDelayMs);
      for (const ws of wss.clients) ws.close(WsCloseCode.serverRestart, "server restart");
      const all = Promise.all(sessions);
      if (await within(all, timers.closeGraceMs)) return;
      for (const ws of wss.clients) ws.terminate();
      if (!(await within(all, timers.sessionsEndMs))) {
        logger.error({ sessions: sessions.size }, "sessions did not end in time");
      }
    },
  };
}
