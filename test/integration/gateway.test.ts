import { protocolCases } from "../cases.ts";
import { startViewsNats } from "../nats.ts";
import { startGateway } from "./start.ts";

protocolCases("nats", async () => {
  const nats = await startViewsNats();
  return { ...nats, start: (options) => startGateway(nats.url, options) };
});
