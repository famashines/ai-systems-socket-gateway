import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { randomUUID } from "node:crypto";
import { waitFor } from "@famashines/ai-systems-kit/testing";
import { viewMessage } from "../cases.ts";
import { startViewsNats, type ViewsNats } from "../nats.ts";
import { connect, startJwks, taskTopic, type Client, type Jwks } from "../support.ts";
import { startGateway } from "./start.ts";

let nats: ViewsNats;
let jwks: Jwks;

before(async () => {
  [nats, jwks] = await Promise.all([startViewsNats(), startJwks()]);
});

after(async () => {
  await jwks.close();
  await nats.close();
});

const session = async (url: string, lastSeq?: number): Promise<Client> => {
  const c = await connect(url);
  c.send({
    type: "hello",
    v: 1,
    token: await jwks.token(),
    ...(lastSeq === undefined ? {} : { lastSeq }),
  });
  await c.next("welcome");
  return c;
};

const publish = () =>
  nats.publish("view.tasks", JSON.stringify(viewMessage("tasks", randomUUID(), 1)));

test("shutdown with open sessions leaves no consumer on VIEWS", async () => {
  const running = await startGateway(nats.url, { issuer: jwks.issuer });
  const clients = await Promise.all(Array.from({ length: 6 }, () => session(running.url)));
  for (const c of clients) {
    c.send({ type: "subscribe", topics: ["tasks", taskTopic()] });
    await c.next("subscribed");
  }
  await waitFor(async () => (await nats.consumers()) === clients.length, 10_000);
  assert.equal(await running.stop(), true);
  for (const c of clients) assert.equal((await c.closed).code, 1012);
  assert.equal(await nats.consumers(), 0);
});

test("a VIEWS remake closes live sessions with 1012, and the resume gives resync", async () => {
  const running = await startGateway(nats.url, { issuer: jwks.issuer });
  try {
    const c = await session(running.url);
    c.send({ type: "subscribe", topics: ["tasks"] });
    await c.next("subscribed");
    const seq = await publish();
    assert.equal((await c.next("notice")).seq, seq);
    await nats.remake();
    assert.equal((await c.closed).code, 1012);
    const again = await session(running.url, seq);
    assert.equal((await again.next("resync")).reason, "too_old");
    await again.close();
    await waitFor(() => running.connections() === 0, 5000);
  } finally {
    assert.equal(await running.stop(), true);
  }
});
