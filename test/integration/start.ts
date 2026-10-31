import { loadConfig } from "@famashines/ai-systems-kit/config";
import type { StartOptions } from "../cases.ts";
import { envSchema, start } from "../../src/main.ts";

export async function startGateway(natsUrl: string, options: StartOptions) {
  const { issuer, allowedOrigins = [], timers = {}, limits = {} } = options;
  const config = loadConfig(envSchema, {
    NATS_SERVERS: natsUrl,
    NATS_USER: "ai-systems-socket-gateway",
    NATS_PASSWORD: "gateway",
    OIDC_ISSUER: issuer,
    ALLOWED_ORIGINS: allowedOrigins.join(","),
    LOG_LEVEL: "silent",
    ...(limits.maxConnections === undefined
      ? {}
      : { MAX_CONNECTIONS: String(limits.maxConnections) }),
    ...(limits.maxPerSubject === undefined
      ? {}
      : { MAX_CONNECTIONS_PER_SUBJECT: String(limits.maxPerSubject) }),
  });
  const running = await start({ ...config, PORT: 0 }, { drainDelayMs: 0, timers });
  return {
    ...running,
    url: `http://127.0.0.1:${String(running.address.port)}`,
    connections: () => running.gateway.connections(),
  };
}
