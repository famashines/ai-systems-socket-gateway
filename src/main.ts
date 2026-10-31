import { createJwtVerifier } from "@famashines/ai-systems-kit/auth";
import { commonEnv, loadConfig } from "@famashines/ai-systems-kit/config";
import { createService } from "@famashines/ai-systems-kit/http";
import { createLogger, startTelemetry } from "@famashines/ai-systems-kit/observability";
import { z } from "zod";
import { attachGateway, type Timers } from "./gateway.ts";
import { connectNats, natsViews } from "./views.io.ts";

export const service = "socket-gateway";

const list = (value: string): string[] =>
  value
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item !== "");

const httpUrl = z.url({ protocol: /^https?$/ });

export const envSchema = commonEnv.omit({ SERVICE_NAME: true }).extend({
  NATS_SERVERS: z.string().min(1).transform(list).pipe(z.array(z.string()).min(1)),
  NATS_USER: z.string().min(1),
  NATS_PASSWORD: z.string().min(1),
  OIDC_ISSUER: httpUrl.default("https://auth.famashines.com/realms/ai-systems"),
  OIDC_AUDIENCE: z.string().min(1).default("ai-systems-app"),
  REQUIRED_ROLE: z.string().min(1).default("owner"),
  ALLOWED_ORIGINS: z
    .string()
    .default("")
    .transform(list)
    .pipe(
      z.array(
        z.string().refine((value) => URL.canParse(value) && new URL(value).origin === value, {
          message: "An origin is scheme, host and port, for example https://host:8443",
        }),
      ),
    ),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.url().optional(),
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: z.url().optional(),
});

export type Config = z.output<typeof envSchema>;

export interface StartOptions {
  drainDelayMs?: number;
  timers?: Partial<Timers>;
}

export async function start(config: Readonly<Config>, options: StartOptions = {}) {
  const { drainDelayMs = 5000 } = options;
  const release = config.RELEASE;
  const telemetry = startTelemetry({ serviceName: service, release });
  const logger = createLogger({ service, release, level: config.LOG_LEVEL });
  logger.info("connecting to NATS");
  const nats = await connectNats({
    servers: config.NATS_SERVERS,
    user: config.NATS_USER,
    pass: config.NATS_PASSWORD,
    service,
  });
  const views = await natsViews(nats);
  const app = createService({
    logger,
    routes: [],
    readiness: { views: () => views.ready() },
    metrics: telemetry.metricsHandler,
    closeHooks: [telemetry.shutdown, () => nats.drain()],
    drainDelayMs,
  });
  const gateway = attachGateway({
    server: app.server,
    views,
    verify: createJwtVerifier({
      issuer: config.OIDC_ISSUER,
      audience: config.OIDC_AUDIENCE,
      requiredRole: config.REQUIRED_ROLE,
      logger,
    }),
    allowedOrigins: config.ALLOWED_ORIGINS,
    logger,
    ...(options.timers ? { timers: options.timers } : {}),
  });
  const address = await app.listen(config.PORT);
  logger.info({ port: address.port }, "socket-gateway started");
  let stopping: Promise<boolean> | undefined;
  const stop = (): Promise<boolean> =>
    (stopping ??= (async () => {
      const closing = app.close();
      await gateway.close(drainDelayMs);
      return closing;
    })());
  return { app, gateway, address, stop };
}

if (import.meta.main) {
  const exitEarly = (): never => process.exit(143);
  process.once("SIGTERM", exitEarly);
  process.once("SIGINT", exitEarly);
  const { stop } = await start(loadConfig(envSchema));
  process.off("SIGTERM", exitEarly);
  process.off("SIGINT", exitEarly);
  const onSignal = (): void => {
    void stop().then((clean) => process.exit(clean ? 0 : 1));
  };
  process.on("SIGTERM", onSignal);
  process.on("SIGINT", onSignal);
}
