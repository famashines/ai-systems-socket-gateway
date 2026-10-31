# Changelog

## Unreleased

- Optional `OIDC_JWKS_URL`: the verifier reads the keys from it; the `iss`
  check stays `OIDC_ISSUER`.
- Release workflow: image, CycloneDX SBOM, grype scan, push to GHCR
  by tag and digest, and a release pull request in `famashines/gitops`.

## 0.1.0

- `/ws` WebSocket protocol v1 (ARCHITECTURE 10): `hello` with the access
  token (kit/auth), `welcome`, `subscribe` and `unsubscribe` for the M1
  topics, `subscribed` with `fromSeq`, `notice` with the `VIEWS` sequence,
  `delta` from core NATS, `resync` (`too_old`), `reauth`, `ping` and
  `pong`, `error`.
- One ordered consumer on `VIEWS` for each connection. Resume from
  `lastSeq + 1` for the topics of the first `subscribe`, later topics from
  the head, first load from the last sequence plus 1, no loss and no duplicate on a
  subscribe change. A `VIEWS` remake closes the sessions with `1012`.
- Close codes `4400`, `4401`, `4408`, `4429`, `1009`, `1012`, `1013`. A
  socket that does not answer a close is terminated after 1 s.
- Origin allow-list at the upgrade (403), default
  `https://ai.famashines.com`. At most 8 sockets for each `sub` (`4429`),
  48 authenticated sessions for each pod (`1013`) and 16 sockets that wait
  for `hello` (HTTP 503). Heartbeat with protocol pings.
- `/health`, `/ready` (`VIEWS`), `/metrics` with the `ws_*` metrics.
- Graceful shutdown: new upgrades 503, each socket closes `1012` after the
  drain delay, NATS drains after every session deleted its consumer.
- Container image on Node 26 slim, uid 1000, read-only root file system.
- Tests: one protocol suite on an in-memory `VIEWS` (unit) and on NATS
  with the gitops grants (integration); the Playwright WebSocket harness.
