import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigError, loadConfig } from "@famashines/ai-systems-kit/config";
import { createJwtVerifier } from "@famashines/ai-systems-kit/auth";
import { envSchema, verifierOptions } from "../../src/main.ts";
import { startJwks } from "../support.ts";

const base = { NATS_SERVERS: "nats://a:4222, nats://b:4222", NATS_USER: "u", NATS_PASSWORD: "p" };

test("defaults allow the app origin and cap the connections", () => {
  const config = loadConfig(envSchema, base);
  assert.deepEqual(config.NATS_SERVERS, ["nats://a:4222", "nats://b:4222"]);
  assert.deepEqual(config.ALLOWED_ORIGINS, ["https://ai.famashines.com"]);
  assert.equal(config.MAX_CONNECTIONS, 48);
  assert.equal(config.MAX_CONNECTIONS_PER_SUBJECT, 8);
  assert.equal(config.MAX_PENDING_HELLO, 16);
  assert.equal(config.OIDC_AUDIENCE, "ai-systems-app");
});

test("ALLOWED_ORIGINS takes exact origins only", () => {
  const config = loadConfig(envSchema, {
    ...base,
    ALLOWED_ORIGINS: "http://127.0.0.1:3000, https://ai.famashines.com",
  });
  assert.deepEqual(config.ALLOWED_ORIGINS, ["http://127.0.0.1:3000", "https://ai.famashines.com"]);
  assert.deepEqual(loadConfig(envSchema, { ...base, ALLOWED_ORIGINS: "" }).ALLOWED_ORIGINS, []);
  assert.throws(
    () => loadConfig(envSchema, { ...base, ALLOWED_ORIGINS: "https://ai.famashines.com/" }),
    ConfigError,
  );
});

test("OIDC_JWKS_URL is the key source, the issuer check stays", async () => {
  const jwks = await startJwks();
  try {
    const token = await jwks.token();
    const plain = verifierOptions(loadConfig(envSchema, { ...base, OIDC_ISSUER: jwks.issuer }));
    assert.equal(plain.jwksUrl, undefined);
    assert.equal((await createJwtVerifier(plain)(token)).sub, "owner-1");
    const dead = "http://127.0.0.1:9/realms/ai-systems/protocol/openid-connect/certs";
    const options = verifierOptions(
      loadConfig(envSchema, { ...base, OIDC_ISSUER: jwks.issuer, OIDC_JWKS_URL: dead }),
    );
    assert.equal(options.issuer, jwks.issuer);
    assert.equal(options.jwksUrl, dead);
    await assert.rejects(createJwtVerifier(options)(token));
    assert.throws(
      () => loadConfig(envSchema, { ...base, OIDC_JWKS_URL: "file:///etc/passwd" }),
      ConfigError,
    );
  } finally {
    await jwks.close();
  }
});
