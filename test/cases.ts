import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { viewSubject, type Topic } from "@famashines/ai-systems-contracts";
import { waitFor } from "@famashines/ai-systems-kit/testing";
import type { Limits, Timers } from "../src/gateway.ts";
import type { StreamHead } from "../src/protocol.ts";
import { connect, startJwks, taskTopic, type Client, type Jwks } from "./support.ts";

export interface Running {
  url: string;
  connections(): number;
  stop(): Promise<boolean>;
}

export interface StartOptions {
  issuer: string;
  timers?: Partial<Timers>;
  limits?: Partial<Limits>;
  allowedOrigins?: string[];
}

export interface Backend {
  start(options: StartOptions): Promise<Running>;
  publish(subject: string, data: string): Promise<number>;
  publishDelta(subject: string, data: string): Promise<void>;
  head(): Promise<StreamHead>;
  trimBefore(seq: number): Promise<void>;
  close(): Promise<void>;
}

const paused = (c: Client) => {
  const socket = (c.ws as unknown as { _socket: { pause(): void; resume(): void } })._socket;
  socket.pause();
  return socket;
};

export const viewMessage = (topic: Topic, docId: string, rev: number, data: unknown = {}) => ({
  eventId: randomUUID(),
  type: viewSubject(topic),
  schemaVersion: 1,
  streamId: `tasks-${docId}`,
  version: rev,
  occurredAt: new Date().toISOString(),
  correlationId: randomUUID(),
  causationId: randomUUID(),
  actor: { kind: "system", id: "projections" },
  payload: { topic, docId, rev, data, commandId: randomUUID() },
});

export function protocolCases(name: string, makeBackend: () => Promise<Backend>): void {
  let backend: Backend;
  let jwks: Jwks;
  let main: Running;
  const open: Client[] = [];
  const extra: Running[] = [];

  const notice = (topic: Topic, docId = randomUUID(), rev = 1, data: unknown = { docId }) =>
    backend.publish(viewSubject(topic), JSON.stringify(viewMessage(topic, docId, rev, data)));

  const client = async (options: Parameters<typeof connect>[1] = {}, running = main) => {
    const c = await connect(running.url, options);
    open.push(c);
    return c;
  };

  const ready = async (lastSeq?: number, running = main) => {
    const c = await client({}, running);
    c.send({
      type: "hello",
      v: 1,
      token: await jwks.token(),
      ...(lastSeq === undefined ? {} : { lastSeq }),
    });
    const welcome = await c.next("welcome");
    return { c, welcome };
  };

  const subscribed = async (c: Client, topics: Topic[]) => {
    c.send({ type: "subscribe", topics });
    return c.next("subscribed");
  };

  const gateway = async (options: Omit<StartOptions, "issuer">) => {
    const running = await backend.start({ issuer: jwks.issuer, ...options });
    extra.push(running);
    return running;
  };

  before(async () => {
    backend = await makeBackend();
    jwks = await startJwks();
    main = await backend.start({
      issuer: jwks.issuer,
      allowedOrigins: ["http://allowed.test"],
      limits: { maxConnections: 1000, maxPerSubject: 1000 },
    });
  });

  after(async () => {
    await Promise.all(open.map((c) => c.close()));
    await Promise.all([main, ...extra].map((running) => running.stop()));
    await jwks.close();
    await backend.close();
  });

  test(`${name}: hello gives welcome with the head sequence`, async () => {
    await notice("tasks");
    const head = await backend.head();
    const { welcome } = await ready();
    assert.match(welcome.sessionId, /^[0-9a-f-]{36}$/);
    assert.equal(welcome.headSeq, head.last);
    assert.equal(welcome.heartbeatMs, 20_000);
  });

  test(`${name}: a bad token, a wrong role or a frame before hello closes 4401`, async () => {
    const bad = await client();
    bad.send({ type: "hello", v: 1, token: "not.a.token" });
    assert.equal((await bad.closed).code, 4401);

    const role = await client();
    role.send({ type: "hello", v: 1, token: await jwks.token({ roles: ["guest"] }) });
    assert.equal((await role.closed).code, 4401);

    const expired = await client();
    expired.send({ type: "hello", v: 1, token: await jwks.token({ expInSec: -5 }) });
    assert.equal((await expired.closed).code, 4401);

    const early = await client();
    early.send({ type: "subscribe", topics: ["tasks"] });
    assert.equal((await early.closed).code, 4401);
  });

  test(`${name}: no hello in time closes 4401`, async () => {
    const running = await gateway({ timers: { helloTimeoutMs: 200 } });
    const c = await client({}, running);
    assert.equal((await c.closed).code, 4401);
  });

  test(`${name}: subscribe gives fromSeq = head + 1 and then notices of that topic only`, async () => {
    const task = taskTopic();
    const { c, welcome } = await ready();
    const answer = await subscribed(c, ["tasks", task]);
    assert.deepEqual(answer.topics, ["tasks", task]);
    assert.equal(answer.fromSeq, welcome.headSeq + 1);
    await notice("budget");
    const docId = randomUUID();
    const seq = await notice("tasks", docId, 3, { title: "Write a poem" });
    const got = await c.next("notice");
    assert.equal(got.seq, seq);
    assert.equal(got.topic, "tasks");
    assert.equal(got.docId, docId);
    assert.equal(got.rev, 3);
    assert.deepEqual(got.data, { title: "Write a poem" });
    assert.match(got.commandId ?? "", /^[0-9a-f-]{36}$/);
    const taskSeq = await notice(task);
    assert.equal((await c.next("notice")).seq, taskSeq);
    assert.equal(
      c.notices().some((n) => n.topic === "budget"),
      false,
    );
  });

  test(`${name}: reconnect with lastSeq gives the missed notices once and in order`, async () => {
    const first = await ready();
    await subscribed(first.c, ["tasks"]);
    const seen = await notice("tasks");
    await first.c.next("notice");
    await first.c.close();
    const missed = [await notice("tasks"), await notice("tasks"), await notice("tasks")];
    await notice("agents");
    const second = await ready(seen);
    assert.equal((await subscribed(second.c, ["tasks"])).fromSeq, seen + 1);
    const live = await notice("tasks");
    await waitFor(() => second.c.notices().length >= 4, 5000, 10);
    await sleep(100);
    assert.deepEqual(
      second.c.notices().map((n) => n.seq),
      [...missed, live],
    );
    assert.equal(
      second.c.frames.some((f) => f.type === "resync"),
      false,
    );
  });

  test(`${name}: all topics in the first subscribe resume exactly`, async () => {
    const task = taskTopic();
    const first = await ready();
    await subscribed(first.c, ["tasks", task]);
    const seen = await notice(task);
    await first.c.next("notice");
    await first.c.close();
    const missed = [await notice("tasks"), await notice(task), await notice("tasks")];
    const second = await ready(seen);
    assert.equal((await subscribed(second.c, ["tasks", task])).fromSeq, seen + 1);
    const live = await notice(task);
    await waitFor(() => second.c.notices().length >= 4, 5000, 10);
    await sleep(100);
    assert.deepEqual(
      second.c.notices().map((n) => n.seq),
      [...missed, live],
    );
  });

  test(`${name}: a topic added after the first subscribe starts at the head`, async () => {
    const task = taskTopic();
    const first = await ready();
    await subscribed(first.c, ["tasks"]);
    const seen = await notice("tasks");
    await first.c.next("notice");
    await first.c.close();
    await notice(task);
    await notice("tasks");
    const second = await ready(seen);
    assert.equal((await subscribed(second.c, ["tasks"])).fromSeq, seen + 1);
    const head = await backend.head();
    assert.equal((await subscribed(second.c, [task])).fromSeq, head.last + 1);
    const live = await notice(task);
    await waitFor(() => second.c.notices().some((n) => n.seq === live), 5000, 10);
    assert.deepEqual(
      second.c
        .notices()
        .filter((n) => n.topic === task)
        .map((n) => n.seq),
      [live],
    );
  });

  test(`${name}: subscribe changes lose nothing and double nothing`, async () => {
    const task = taskTopic();
    const { c } = await ready();
    await subscribed(c, ["tasks"]);
    const published: number[] = [];
    const done = new AbortController();
    const writer = (async () => {
      while (!done.signal.aborted) {
        published.push(await notice("tasks"));
        await notice(task);
        await sleep(2);
      }
    })();
    for (let i = 0; i < 5; i += 1) {
      await subscribed(c, [task]);
      await sleep(20);
      c.send({ type: "unsubscribe", topics: [task] });
      await sleep(20);
    }
    done.abort();
    await writer;
    const last = published.at(-1) ?? 0;
    await waitFor(() => c.notices().some((n) => n.seq === last), 5000, 10);
    const seqs = c.notices().map((n) => n.seq);
    assert.deepEqual(
      seqs,
      [...seqs].sort((a, b) => a - b),
    );
    assert.equal(new Set(seqs).size, seqs.length);
    assert.deepEqual(
      c
        .notices()
        .filter((n) => n.topic === "tasks")
        .map((n) => n.seq),
      published,
    );
  });

  test(`${name}: a lastSeq older than the stream gives resync too_old`, async () => {
    await notice("tasks");
    await notice("tasks");
    const head = await backend.head();
    await backend.trimBefore(head.last);
    const old = await ready(head.last - 2);
    assert.equal((await old.c.next("resync")).reason, "too_old");
    assert.equal((await subscribed(old.c, ["tasks"])).fromSeq, head.last + 1);
    const seq = await notice("tasks");
    assert.equal((await old.c.next("notice")).seq, seq);
    assert.equal(old.c.notices().length, 1);

    const ahead = await ready(head.last + 1000);
    assert.equal((await ahead.c.next("resync")).reason, "too_old");
  });

  test(`${name}: an invalid VIEWS message is skipped`, async () => {
    const { c } = await ready();
    await subscribed(c, ["tasks"]);
    await backend.publish("view.tasks", "not json");
    await backend.publish("view.tasks", JSON.stringify(viewMessage("budget", randomUUID(), 1)));
    const seq = await notice("tasks");
    assert.equal((await c.next("notice")).seq, seq);
    assert.equal(c.notices().length, 1);
  });

  test(`${name}: deltas of subscribed tasks arrive, others do not`, async () => {
    const task = taskTopic();
    const other = taskTopic();
    const taskId = task.slice(5);
    const callId = randomUUID();
    const { c } = await ready();
    await subscribed(c, [task]);
    await backend.publishDelta(
      `delta.${other.slice(5)}.${callId}`,
      JSON.stringify({ i: 0, text: "x" }),
    );
    await backend.publishDelta(`delta.${taskId}.${callId}`, "not json");
    await backend.publishDelta(`delta.${taskId}.${callId}`, JSON.stringify({ i: 1, text: "Hi" }));
    assert.deepEqual(await c.next("delta"), { type: "delta", taskId, callId, i: 1, text: "Hi" });
    c.send({ type: "unsubscribe", topics: [task] });
    c.send({ type: "ping", t: 1 });
    await c.next("pong");
    await backend.publishDelta(`delta.${taskId}.${callId}`, JSON.stringify({ i: 2, text: "!" }));
    c.send({ type: "ping", t: 2 });
    await c.next("pong");
    await sleep(100);
    assert.equal(c.frames.filter((f) => f.type === "delta").length, 1);
  });

  test(`${name}: at most 50 topics for each connection`, async () => {
    const { c } = await ready();
    const fifty = Array.from({ length: 50 }, taskTopic);
    assert.deepEqual((await subscribed(c, fifty)).topics, fifty);
    assert.equal((await subscribed(c, [fifty[0] ?? "tasks"])).topics.length, 1);
    c.send({ type: "subscribe", topics: ["tasks"] });
    const error = await c.next("error");
    assert.equal(error.code, "validation_failed");
    c.send({ type: "ping", t: 7 });
    assert.equal((await c.next("pong")).t, 7);

    const many = await ready();
    many.c.send({ type: "subscribe", topics: [...fifty, "tasks"] });
    assert.equal((await many.c.closed).code, 4400);
  });

  for (const [label, frame] of [
    ["not JSON", "{"],
    ["an unknown type", { type: "nope" }],
    ["an unknown topic", { type: "subscribe", topics: ["conversation:x"] }],
    ["an extra field", { type: "ping", t: 1, extra: true }],
    ["a second hello", { type: "hello", v: 1, token: "x" }],
  ] as const) {
    test(`${name}: ${label} closes 4400`, async () => {
      const { c } = await ready();
      c.send(frame);
      assert.equal((await c.closed).code, 4400);
    });
  }

  test(`${name}: a binary frame closes 4400, a frame over 64 KiB closes 1009`, async () => {
    const binary = await ready();
    binary.c.ws.send(Buffer.from("{}"));
    assert.equal((await binary.c.closed).code, 4400);
    const large = await ready();
    large.c.send({ type: "ping", t: 1, pad: "x".repeat(70_000) });
    assert.equal((await large.c.closed).code, 1009);
  });

  test(`${name}: ping gives pong, more than 20 frames in a second closes 4429`, async () => {
    const { c } = await ready();
    c.send({ type: "ping", t: 42 });
    assert.deepEqual(await c.next("pong"), { type: "pong", t: 42 });
    for (let i = 0; i < 25; i += 1) c.send({ type: "pong", t: i });
    assert.equal((await c.closed).code, 4429);
  });

  test(`${name}: a client with no pong is closed, a client with pong stays`, async () => {
    const running = await gateway({ timers: { pingIntervalMs: 100, pongTimeoutMs: 100 } });
    const silent = await client({ autoPong: false }, running);
    const alive = await client({}, running);
    const token = await jwks.token();
    silent.send({ type: "hello", v: 1, token });
    alive.send({ type: "hello", v: 1, token });
    assert.equal((await alive.next("welcome")).heartbeatMs, 100);
    assert.equal((await silent.closed).code, 1006);
    await sleep(500);
    assert.equal(alive.ws.readyState, alive.ws.OPEN);
  });

  test(`${name}: the token expiry closes 4401 unless reauth came first`, async () => {
    const expiring = await client();
    expiring.send({ type: "hello", v: 1, token: await jwks.token({ expInSec: 2 }) });
    await expiring.next("welcome");
    const renewed = await client();
    renewed.send({ type: "hello", v: 1, token: await jwks.token({ expInSec: 2 }) });
    await renewed.next("welcome");
    renewed.send({ type: "reauth", token: await jwks.token({ expInSec: 60 }) });
    assert.equal((await expiring.closed).code, 4401);
    await sleep(500);
    assert.equal(renewed.ws.readyState, renewed.ws.OPEN);

    renewed.send({ type: "reauth", token: await jwks.token({ sub: "someone-else" }) });
    assert.equal((await renewed.closed).code, 4401);

    const invalid = await ready();
    invalid.c.send({ type: "reauth", token: "bad" });
    assert.equal((await invalid.c.closed).code, 4401);
  });

  test(`${name}: over 1 MiB buffered drops deltas, over 4 MiB closes 4408`, async () => {
    const task = taskTopic();
    const taskId = task.slice(5);
    const callId = randomUUID();
    const running = await gateway({ timers: { closeGraceMs: 60_000 } });
    const { c } = await ready(undefined, running);
    await subscribed(c, [task]);
    c.send({ type: "ping", t: 0 });
    await c.next("pong");
    const socket = paused(c);
    const big = "x".repeat(60_000);
    let deltas = 0;
    for (let i = 0; i < 600; i += 1) {
      await notice(task, randomUUID(), 1, big);
      await backend.publishDelta(`delta.${taskId}.${callId}`, JSON.stringify({ i, text: "t" }));
      deltas += 1;
    }
    socket.resume();
    assert.equal((await c.closed).code, 4408);
    const received = c.frames.filter((f) => f.type === "delta").length;
    assert.ok(received < deltas, `${String(received)} of ${String(deltas)} deltas arrived`);
    assert.ok(
      c.frames.slice(-20).every((f) => f.type === "notice"),
      "the last frames before the close are notices",
    );
  });

  test(`${name}: a slow consumer that does not read is terminated after the close grace`, async () => {
    const task = taskTopic();
    const running = await gateway({ timers: { closeGraceMs: 200 } });
    const { c } = await ready(undefined, running);
    await subscribed(c, [task]);
    c.send({ type: "ping", t: 0 });
    await c.next("pong");
    paused(c);
    const big = "x".repeat(60_000);
    for (let i = 0; i < 2000 && running.connections() > 0; i += 1) {
      await notice(task, randomUUID(), 1, big);
    }
    await waitFor(() => running.connections() === 0, 5000, 20);
  });

  test(`${name}: a client with no hello that does not read is terminated after the grace`, async () => {
    const running = await gateway({ timers: { helloTimeoutMs: 100, closeGraceMs: 100 } });
    const c = await client({}, running);
    paused(c);
    assert.equal(running.connections(), 1);
    await waitFor(() => running.connections() === 0, 2000, 20);
  });

  test(`${name}: more than maxPerSubject sockets of one sub close 4429`, async () => {
    const running = await gateway({ limits: { maxPerSubject: 2 } });
    await ready(undefined, running);
    await ready(undefined, running);
    const third = await client({}, running);
    third.send({ type: "hello", v: 1, token: await jwks.token() });
    assert.equal((await third.closed).code, 4429);
    const other = await client({}, running);
    other.send({ type: "hello", v: 1, token: await jwks.token({ sub: "owner-2" }) });
    await other.next("welcome");
  });

  test(`${name}: sockets with no hello do not count against maxConnections`, async () => {
    const running = await gateway({
      timers: { helloTimeoutMs: 500 },
      limits: { maxConnections: 1, maxPendingHello: 16 },
    });
    const anonymous = await Promise.all(Array.from({ length: 15 }, () => client({}, running)));
    const owner = await client({}, running);
    await assert.rejects(connect(running.url), /503/);
    owner.send({ type: "hello", v: 1, token: await jwks.token() });
    await owner.next("welcome");
    const second = await client({}, running);
    second.send({ type: "hello", v: 1, token: await jwks.token() });
    assert.equal((await second.closed).code, 1013);
    for (const c of anonymous) assert.equal((await c.closed).code, 4401);
    const later = await client({}, running);
    assert.equal(later.ws.readyState, later.ws.OPEN);
  });

  test(`${name}: an Origin that is not allowed gets 403, other paths 404`, async () => {
    await assert.rejects(connect(main.url, { origin: "http://evil.test" }), /403/);
    await assert.rejects(connect(main.url, { path: "/other" }), /404/);
    const allowed = await client({ origin: "http://allowed.test" });
    allowed.send({ type: "hello", v: 1, token: await jwks.token() });
    await allowed.next("welcome");
  });

  test(`${name}: shutdown closes each socket with 1012`, async () => {
    const running = await backend.start({ issuer: jwks.issuer });
    const { c } = await ready(undefined, running);
    await subscribed(c, ["tasks"]);
    const quiet = await client({}, running);
    const stopped = running.stop();
    assert.equal((await c.closed).code, 1012);
    assert.equal((await quiet.closed).code, 1012);
    assert.equal(await stopped, true);
  });
}
