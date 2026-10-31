import { loadConfig } from "@famashines/ai-systems-kit/config";
import type { Timers } from "../../src/gateway.ts";
import { envSchema, start } from "../../src/main.ts";

export async function startGateway(
  natsUrl: string,
  issuer: string,
  allowedOrigins: string[] = [],
  timers: Partial<Timers> = {},
) {
  const config = loadConfig(envSchema, {
    NATS_SERVERS: natsUrl,
    NATS_USER: "ai-systems-socket-gateway",
    NATS_PASSWORD: "gateway",
    OIDC_ISSUER: issuer,
    ALLOWED_ORIGINS: allowedOrigins.join(","),
    LOG_LEVEL: "silent",
  });
  const running = await start({ ...config, PORT: 0 }, { drainDelayMs: 0, timers });
  return { ...running, url: `http://127.0.0.1:${String(running.address.port)}` };
}
