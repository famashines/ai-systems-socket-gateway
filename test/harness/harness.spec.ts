import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { createInterface } from "node:readline";
import type { ServerMessageV1 } from "@famashines/ai-systems-contracts";
import { expect, test, type Page } from "@playwright/test";
import { viewMessage } from "../cases.ts";
import { startViewsNats, type ViewsNats } from "../nats.ts";
import { listen, startJwks, type Jwks } from "../support.ts";

interface Harness {
  frames: ServerMessageV1[];
  closed: { code: number; reason: string } | null;
  connect(url: string): Promise<boolean>;
  send(message: unknown): void;
  close(): void;
}

const root = new URL("../../", import.meta.url);
let nats: ViewsNats;
let jwks: Jwks;
let gateway: ChildProcess;
let wsUrl = "";
let pageUrl = "";
let otherUrl = "";
const servers: Server[] = [];

const pageServer = async (): Promise<string> => {
  const html = await readFile(new URL("test/harness/index.html", root), "utf8");
  const server = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/html; charset=utf-8" }).end(html);
  });
  servers.push(server);
  return listen(server);
};

const freePort = async (): Promise<string> => {
  const server = createServer();
  const url = await listen(server);
  await new Promise((resolve) => server.close(resolve));
  return new URL(url).port;
};

const startGateway = async (allowedOrigin: string): Promise<string> => {
  const port = await freePort();
  gateway = spawn(process.execPath, ["src/main.ts"], {
    cwd: root,
    env: {
      PATH: process.env.PATH,
      NATS_SERVERS: nats.url,
      NATS_USER: "ai-systems-socket-gateway",
      NATS_PASSWORD: "gateway",
      OIDC_ISSUER: jwks.issuer,
      ALLOWED_ORIGINS: allowedOrigin,
      PORT: port,
      LOG_LEVEL: "info",
      RELEASE: "harness",
    },
    stdio: ["ignore", "pipe", "inherit"],
  });
  const lines = createInterface({ input: gateway.stdout ?? process.stdin });
  for await (const line of lines) {
    const entry = JSON.parse(line) as { msg?: string };
    if (entry.msg === "socket-gateway started") return `ws://127.0.0.1:${port}/ws`;
  }
  throw new Error("socket-gateway did not start");
};

test.beforeAll(async () => {
  [nats, jwks] = await Promise.all([startViewsNats(), startJwks()]);
  pageUrl = await pageServer();
  otherUrl = await pageServer();
  wsUrl = await startGateway(pageUrl);
});

test.afterAll(async () => {
  const exited = new Promise<number | null>((resolve) => gateway.once("exit", resolve));
  gateway.kill("SIGTERM");
  expect(await exited).toBe(0);
  for (const server of servers) server.close();
  await jwks.close();
  await nats.close();
});

declare global {
  interface Window {
    harness: Harness;
  }
}

const open = async (page: Page, url = pageUrl): Promise<boolean> => {
  await page.goto(url);
  return page.evaluate((target) => window.harness.connect(target), wsUrl);
};

const send = (page: Page, message: unknown) =>
  page.evaluate((value) => {
    window.harness.send(value);
  }, message);

const frames = (page: Page) => page.evaluate(() => window.harness.frames);

const closedCode = async (page: Page): Promise<number> => {
  await expect.poll(() => page.evaluate(() => window.harness.closed?.code ?? 0)).not.toBe(0);
  return page.evaluate(() => window.harness.closed?.code ?? 0);
};

const all = async <T extends ServerMessageV1["type"]>(page: Page, type: T, count = 1) => {
  await expect
    .poll(async () => (await frames(page)).filter((f) => f.type === type).length)
    .toBeGreaterThanOrEqual(count);
  return (await frames(page)).filter(
    (f): f is Extract<ServerMessageV1, { type: T }> => f.type === type,
  );
};

const notice = (rev = 1): Promise<number> => {
  const docId = randomUUID();
  return nats.publish("view.tasks", JSON.stringify(viewMessage("tasks", docId, rev, { docId })));
};

const hello = async (page: Page, lastSeq?: number) => {
  await send(page, {
    type: "hello",
    v: 1,
    token: await jwks.token(),
    ...(lastSeq === undefined ? {} : { lastSeq }),
  });
  return (await all(page, "welcome"))[0];
};

test("connect gives welcome", async ({ page }) => {
  expect(await open(page)).toBe(true);
  const welcome = await hello(page);
  expect(welcome?.heartbeatMs).toBe(20_000);
  expect(welcome?.headSeq).toBe((await nats.head()).last);
  await page.screenshot({ path: test.info().outputPath("welcome.png") });
});

test("a bad token closes 4401", async ({ page }) => {
  await open(page);
  await send(page, { type: "hello", v: 1, token: "not-a-token" });
  expect(await closedCode(page)).toBe(4401);
  await page.screenshot({ path: test.info().outputPath("bad-auth.png") });
});

test("an Origin that is not allowed is refused at the upgrade", async ({ page }) => {
  expect(await open(page, otherUrl)).toBe(false);
  expect(await closedCode(page)).toBe(1006);
});

test("subscribe, a notice arrives, reconnect with lastSeq gives the missed notices once and in order", async ({
  page,
}) => {
  await open(page);
  const welcome = await hello(page);
  await send(page, { type: "subscribe", topics: ["tasks"] });
  const [answer] = await all(page, "subscribed");
  expect(answer?.fromSeq).toBe((welcome?.headSeq ?? 0) + 1);
  const seen = await notice();
  const [first] = await all(page, "notice");
  expect(first?.seq).toBe(seen);
  await page.screenshot({ path: test.info().outputPath("notice.png") });

  await page.evaluate(() => {
    window.harness.close();
  });
  await closedCode(page);
  const missed = [await notice(), await notice(), await notice()];

  await open(page);
  await hello(page, seen);
  await send(page, { type: "subscribe", topics: ["tasks"] });
  expect((await all(page, "subscribed"))[0]?.fromSeq).toBe(seen + 1);
  const live = await notice();
  await all(page, "notice", 4);
  await page.waitForTimeout(300);
  const seqs = (await all(page, "notice")).map((n) => n.seq);
  expect(seqs).toEqual([...missed, live]);
  expect(new Set(seqs).size).toBe(seqs.length);
  await page.screenshot({ path: test.info().outputPath("resume.png") });
});

test("a lastSeq older than the stream gives resync too_old", async ({ page }) => {
  await notice();
  const head = await nats.head();
  await nats.trimBefore(head.last + 1);
  await open(page);
  await hello(page, head.last - 1);
  expect((await all(page, "resync"))[0]?.reason).toBe("too_old");
  await page.screenshot({ path: test.info().outputPath("resync.png") });
});
