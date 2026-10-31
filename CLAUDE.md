# CLAUDE.md for ai-systems-socket-gateway

Rules for all ai-systems repos: `ai-systems-docs/CLAUDE.md`
(local: `../../docs/CLAUDE.md`). What the service does: `README.md`.

## Commands

| Command                    | Does                                                             |
| -------------------------- | ---------------------------------------------------------------- |
| `npm ci`                   | install from the lock file (builds kit and contracts on install) |
| `npm run check`            | `tsc --noEmit` (TypeScript 7)                                    |
| `npm run lint`             | ESLint (strict type-checked) and Prettier check                  |
| `npm test`                 | unit tests, 70% lines of `src` without `main.ts` and `*.io.ts`   |
| `npm run verify`           | check, lint and test                                             |
| `npm run test:integration` | `test/integration` with NATS in Testcontainers (needs Docker)    |
| `npm run test:harness`     | the Playwright WebSocket harness (needs Docker and Chromium)     |
| `npm start`                | `node src/main.ts`                                               |

Chromium for the harness:
`node node_modules/@playwright/test/cli.js install chromium`.

## Layout

```
src
├── protocol.ts   frame parsing, sequences and topics, rate window; no I/O
├── gateway.ts    upgrade checks and one session for each socket; uses the Views port
├── views.io.ts   NATS: connect, STREAM.INFO, ordered consumers, delta subscriptions
└── main.ts       config, wiring, start, shutdown
test
├── cases.ts      the protocol suite, run on two backends
├── support.ts    local JWKS and token signer, WebSocket test client
├── nats.ts       NATS container with the gitops grants, VIEWS, publishers
├── unit          the suite on an in-memory VIEWS (no Docker)
├── integration   the suite on NATS; readiness, metrics, shutdown, VIEWS remake
└── harness       dev page and Playwright test (not in the image)
```

## Tests

- `test/cases.ts` has each protocol case one time. `test/unit` runs it with
  an in-memory `Views`, `test/integration` with real NATS. A new rule gets
  a case there.
- The NATS config in `test/nats.ts` has the grants of
  `ai-systems-socket-gateway` and `ai-systems-admin` from gitops
  (`kubernetes/infrastructure/nats/values.yaml`). Change them together with
  gitops. The publishers are a part of the `ai-systems-projections` grants
  (`view.>`) and of `ai-systems-ai-proxy` (`delta.>`).
- Wait with `waitFor`, never with a fixed sleep, except to show that a
  frame does not come.
- The harness starts NATS, a local JWKS and a page server, and runs
  `node src/main.ts` as a child process with the page origin in
  `ALLOWED_ORIGINS`. It stops the gateway with SIGTERM and expects exit 0.
  Screenshots and traces go to `test-results/`, the report to
  `playwright-report/`. ADR 0017 lists Playwright for this harness.

## Dependencies

- Kit and contracts come from git tags. A new tag needs a new
  `allowScripts` entry with the commit of the tag
  (`npm install-scripts approve <package>`).
- `@nats-io/jetstream`, `@nats-io/transport-node` and `@opentelemetry/api`
  have the exact versions of the kit, so there is one copy of each. Check
  with `npm ls`. `kit/messaging` is not used: it loads `mongodb`.
- TypeScript 6 (`typescript`, for ESLint) and 7 (`typescript-7`, for
  `check`): ADR 0022. Never run `npx tsc`.
- The Docker build stage uses the full Node image, because npm needs `git`
  to install the kit and contracts. The runtime stage is slim.
