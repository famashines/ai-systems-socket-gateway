import { EventEmitter } from "node:events";
import { createJwtVerifier } from "@famashines/ai-systems-kit/auth";
import { createService } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { attachGateway, type ViewMessage, type Views } from "../../src/gateway.ts";
import { protocolCases, type Backend } from "../cases.ts";
import { audience } from "../support.ts";

const logger = createLogger({ service: "test", release: "test", level: "silent" });

function memoryViews() {
  const messages: ViewMessage[] = [];
  let first = 0;
  const live = new EventEmitter();
  const deltas = new EventEmitter();
  const views: Views = {
    head: () => Promise.resolve({ first, last: messages.length }),
    consume(subjects, startSeq, onMessage) {
      let next = Math.max(startSeq, first, 1);
      let stopped = false;
      const pump = (): void => {
        while (!stopped && next <= messages.length) {
          const message = messages[next - 1];
          next += 1;
          if (message && subjects.includes(message.subject)) onMessage(message);
        }
      };
      live.on("message", pump);
      setImmediate(pump);
      return Promise.resolve(() => {
        stopped = true;
        live.off("message", pump);
        return Promise.resolve();
      });
    },
    deltas(taskId, onMessage) {
      const listener = (subject: string, data: string): void => {
        if (subject.split(".")[1] === taskId) onMessage(subject, data);
      };
      deltas.on("delta", listener);
      return () => {
        deltas.off("delta", listener);
      };
    },
  };
  const backend: Omit<Backend, "start"> = {
    publish(subject, data) {
      messages.push({ seq: messages.length + 1, subject, data });
      live.emit("message");
      return Promise.resolve(messages.length);
    },
    publishDelta(subject, data) {
      deltas.emit("delta", subject, data);
      return Promise.resolve();
    },
    head: () => views.head(),
    trimBefore(seq) {
      first = seq;
      return Promise.resolve();
    },
    close: () => Promise.resolve(),
  };
  return { views, backend };
}

protocolCases("memory", () => {
  const { views, backend } = memoryViews();
  return Promise.resolve({
    ...backend,
    async start({ issuer, timers, allowedOrigins = [] }) {
      const app = createService({ logger, routes: [], drainDelayMs: 0 });
      const gateway = attachGateway({
        server: app.server,
        views,
        verify: createJwtVerifier({ issuer, audience, requiredRole: "owner" }),
        allowedOrigins,
        logger,
        ...(timers ? { timers } : {}),
      });
      const address = await app.listen(0, "127.0.0.1");
      return {
        url: `http://127.0.0.1:${String(address.port)}`,
        async stop() {
          const closing = app.close();
          await gateway.close();
          return closing;
        },
      };
    },
  });
});
