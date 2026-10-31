import { protocolCases } from "../cases.ts";
import { startViewsNats } from "../nats.ts";
import { startGateway } from "./start.ts";

protocolCases("nats", async () => {
  const nats = await startViewsNats();
  return {
    ...nats,
    async start({ issuer, timers, allowedOrigins }) {
      const running = await startGateway(nats.url, issuer, allowedOrigins, timers);
      return { url: running.url, stop: running.stop };
    },
  };
});
