import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { waitFor } from "@famashines/ai-systems-kit/testing";
import { startViewsNats, type ViewsNats } from "../nats.ts";
import { connect, startJwks, type Jwks } from "../support.ts";
import { startGateway } from "./start.ts";

let nats: ViewsNats;
let jwks: Jwks;
let running: Awaited<ReturnType<typeof startGateway>>;

before(async () => {
  [nats, jwks] = await Promise.all([startViewsNats(), startJwks()]);
  running = await startGateway(nats.url, { issuer: jwks.issuer });
});

after(async () => {
  assert.equal(await running.stop(), true);
  await jwks.close();
  await nats.close();
});

const metrics = async (): Promise<string> => (await fetch(`${running.url}/metrics`)).text();

test("/ready is 200 when VIEWS answers", async () => {
  assert.equal((await fetch(`${running.url}/ready`)).status, 200);
  assert.equal((await fetch(`${running.url}/health`)).status, 200);
});

test("metrics count connections, frames and closes; no consumer is left after a close", async () => {
  const c = await connect(running.url);
  c.send({ type: "hello", v: 1, token: await jwks.token() });
  await c.next("welcome");
  c.send({ type: "subscribe", topics: ["tasks"] });
  await c.next("subscribed");
  await waitFor(async () => (await nats.consumers()) === 1, 10_000);
  const open = await metrics();
  assert.match(open, /^ws_connections\{[^}]*\} 1$/m);
  assert.match(open, /^ws_frames_sent_total\{[^}]*type="welcome"[^}]*\} 1$/m);
  assert.match(open, /^ws_frames_received_total\{[^}]*\} 2$/m);
  await c.close();
  await waitFor(async () => (await nats.consumers()) === 0, 10_000);
  const closed = await metrics();
  assert.match(closed, /^ws_connections\{[^}]*\} 0$/m);
  assert.match(closed, /^ws_connections_closed_total\{[^}]*code="1005"[^}]*\} 1$/m);
  await assert.rejects(connect(running.url, { origin: "http://evil.test" }), /403/);
  assert.match(await metrics(), /^ws_upgrades_refused_total\{[^}]*status="403"[^}]*\} 1$/m);
});
