# Changelog

## 0.1.0

- `/ws` WebSocket protocol v1 (ARCHITECTURE 10): `hello` with the access
  token (kit/auth), `welcome`, `subscribe` and `unsubscribe` for the M1
  topics, `subscribed` with `fromSeq`, `notice` with the `VIEWS` sequence,
  `delta` from core NATS, `resync` (`too_old`), `reauth`, `ping` and
  `pong`, `error`.
- One ordered consumer on `VIEWS` for each connection. Resume from
  `lastSeq + 1`, first load from the last sequence plus 1, no loss and no
  duplicate on a subscribe change.
- Close codes `4400`, `4401`, `4408`, `4429`, `1009`, `1012`, `1013`.
  Origin allow-list at the upgrade (403). Heartbeat with protocol pings.
- `/health`, `/ready` (`VIEWS`), `/metrics` with the `ws_*` metrics.
- Graceful shutdown: new upgrades 503, each socket closes `1012` after the
  drain delay.
- Container image on Node 26 slim, uid 1000, read-only root file system.
- Tests: one protocol suite on an in-memory `VIEWS` (unit) and on NATS
  with the gitops grants (integration); the Playwright WebSocket harness.
