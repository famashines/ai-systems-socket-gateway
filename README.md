# ai-systems-socket-gateway

The WebSocket gateway of ai-systems. The app connects to `/ws`. The gateway
checks the access token, reads the change notices of the read models from
the JetStream stream `VIEWS`, and sends them to the app with the stream
sequence. On a reconnect the app sends its last sequence, and the gateway
sends the notices that the app did not get, once and in order. It also
forwards live token deltas from core NATS. It has no database and keeps no
state of its own, so it can restart at any time.

```
app --WSS /ws--> Traefik --> socket-gateway --JWKS--> Keycloak (realm ai-systems)
                                  |   ^
           ordered consumer, one  |   | notices (seq = VIEWS sequence)
           for each connection    v   |
                        NATS VIEWS (view.>)  <-- projections (outbox relay)
                        core NATS delta.<taskId>.<callId>  <-- ai-proxy (from M2)
```

Architecture and reasons: `ai-systems-docs` (`ARCHITECTURE.md` 2, 10, 11
and 12; ADR 0004, 0009, 0010, 0019, 0021, 0023 and 0025).

## Endpoints

| Path       | Does                                                              |
| ---------- | ----------------------------------------------------------------- |
| `/ws`      | WebSocket upgrade. Every other path gives 404 at the upgrade.     |
| `/health`  | Liveness: 200 while the process runs                              |
| `/ready`   | Readiness: 200 when `$JS.API.STREAM.INFO.VIEWS` answers, else 503 |
| `/metrics` | Prometheus metrics                                                |

## Protocol

JSON text frames, checked with `ClientMessageV1` and `ServerMessageV1` of
`@famashines/ai-systems-contracts` v0.2.0. The message list is in
`ARCHITECTURE.md` 10.1.

```
app                                   socket-gateway                       VIEWS
 | hello { v, token, lastSeq? } ------> | verify token (kit/auth)             |
 |                                      | STREAM.INFO ----------------------> |
 | <---------- welcome { sessionId, headSeq, heartbeatMs }                    |
 | <---------- resync { too_old }       (only if lastSeq is not in VIEWS)     |
 | subscribe { topics } --------------> |                                     |
 | <---------- subscribed { topics, fromSeq }                                 |
 |                                      | ordered consumer from fromSeq ----> |
 | <---------- notice { seq, topic, docId, rev, data, commandId? } <--------- |
 | GET /api/... snapshot (api-gateway), after subscribed                      |
```

### Rules

| Rule                                                                                                                       | Result                                                                                                                                                                                                                                              |
| -------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| No `hello` in 5 s, or another frame before it                                                                              | close `4401`                                                                                                                                                                                                                                        |
| Token not valid, wrong role, expired                                                                                       | close `4401`                                                                                                                                                                                                                                        |
| JWKS cannot be read at `hello`                                                                                             | close `1013` (try again later); the app does not sign out                                                                                                                                                                                           |
| Token expiry (`exp`) with no `reauth` before it                                                                            | close `4401`                                                                                                                                                                                                                                        |
| `reauth` with a valid token for the same `sub`                                                                             | the new `exp` is the limit                                                                                                                                                                                                                          |
| `reauth` with another `sub` or a bad token                                                                                 | close `4401`                                                                                                                                                                                                                                        |
| `reauth` while the JWKS cannot be read                                                                                     | `error { code: "unavailable" }`; the old token stays valid until its `exp`                                                                                                                                                                          |
| A frame that is not valid JSON or not `ClientMessageV1` (strict), a binary frame, a second `hello`                         | close `4400`                                                                                                                                                                                                                                        |
| A frame over 64 KiB                                                                                                        | close `1009` (the `ws` library, "message too big")                                                                                                                                                                                                  |
| More than 20 frames in one second                                                                                          | close `4429`                                                                                                                                                                                                                                        |
| More than `MAX_CONNECTIONS_PER_SUBJECT` sockets for one `sub`                                                              | the new socket closes `4429` after its `hello`                                                                                                                                                                                                      |
| `MAX_PENDING_HELLO` sockets wait for their `hello`                                                                         | a new upgrade gets HTTP 503. Sockets with no `hello` do not count against `MAX_CONNECTIONS`, and the hello timeout frees their slots.                                                                                                               |
| No pong in 10 s after a protocol ping (each 20 s)                                                                          | the socket is terminated (the client sees `1006`)                                                                                                                                                                                                   |
| App `ping { t }`                                                                                                           | `pong { t }`                                                                                                                                                                                                                                        |
| Send buffer over 1 MiB                                                                                                     | `delta` frames are dropped (counted)                                                                                                                                                                                                                |
| Send buffer over 4 MiB                                                                                                     | close `4408`; the app resumes with `lastSeq`, so no notice is lost                                                                                                                                                                                  |
| The gateway closes a socket (`4401`, `4408`, `4429`, `1013` ...)                                                           | if the client does not answer the close in 1 s, the socket is terminated                                                                                                                                                                            |
| `VIEWS` is deleted or made again while a socket is open (the consumer reports `consumer_deleted` or `stream_not_found`)    | close `1012`; the resume then gives `resync { too_old }`                                                                                                                                                                                            |
| An `Origin` header that is not in `ALLOWED_ORIGINS` (exact match)                                                          | 403 at the upgrade. A request with no `Origin` is allowed. React Native on Android sends `Origin: https://ai.famashines.com`, the default of `ALLOWED_ORIGINS`. Auth is the `hello` token, not a cookie, so the Origin check is a second line only. |
| SIGTERM                                                                                                                    | `/ready` 503 and new upgrades 503 at once; after the drain delay each socket closes `1012`; NATS drains only after every session stopped its consumer                                                                                               |
| NATS or the JWKS does not answer, the consumer fails, or the pod has `MAX_CONNECTIONS` authenticated sessions at a `hello` | close `1013` (try again later): the app reconnects with backoff and `lastSeq`                                                                                                                                                                       |

### Close codes

`4400`, `4401`, `4408`, `4429` and `1012` as in ARCHITECTURE 10.2, plus:

- `1009`: a frame over 64 KiB (the `ws` library).
- `1013` (try again later): NATS or the JWKS does not answer, the `VIEWS`
  consumer failed, or the pod has `MAX_CONNECTIONS` authenticated sessions. The app reconnects with backoff and
  `lastSeq`. Contracts `WsCloseCode` gets it in M2-02.
- `1006` on the client: the gateway terminated the socket (no pong, or no
  answer to a close in 1 s).

### Topics

M1 topics: `tasks`, `task:<id>`, `budget`, `agents`, `notifications` (the
`Topic` schema of contracts v0.2.0).

- An unknown topic (also `conversation:<id>` before contracts has it) fails
  the strict `ClientMessageV1` schema, so it closes `4400`. The app and the
  gateway use the same contracts version.
- More than 50 topics in one frame fails the schema (`4400`). More than 50
  topics in total over several frames gives
  `error { code: "validation_failed" }`, no change and no close.
- No topic is forbidden in M1 (one owner), so `4403` is not used yet.
- `unsubscribe` has no answer: contracts has no `unsubscribed` message, and
  `subscribed.topics` must have at least one topic.

### Sequences

- `welcome.headSeq` is the last sequence of `VIEWS` at `hello`.
- On the first connect the app omits `hello.lastSeq`. `lastSeq: 0` is valid
  in contracts v0.2.0, but it replays the whole stream (contracts M2-02 makes
  it positive).
- `hello.lastSeq` given: only the first `subscribe` resumes. Its topics
  start at `lastSeq + 1`, and `fromSeq` is `lastSeq + 1`. If `lastSeq + 1` is below the first sequence
  of `VIEWS` (older than the 7 days of the stream), or `lastSeq` is above
  the last sequence (the stream was made again), the gateway sends
  `resync { reason: "too_old" }` after `welcome` and continues as if no
  `lastSeq` came.
- No `lastSeq`: the first `subscribe` starts at the last sequence of `VIEWS`
  (read at that `subscribe`) plus 1. So a change between `subscribed` and the
  snapshot read is not lost, and the app drops a notice by its `rev`.
- A topic in a later `subscribe` starts at the last sequence at that
  `subscribe` plus 1, also after a resume. `fromSeq` is the lowest start of
  the topics in the frame.
- Client rule: on a reconnect, the app sends all its topics in the first
  `subscribe`. For a topic that it subscribes later, or when `fromSeq` is
  higher than `lastSeq + 1`, it loads the snapshot of that topic again.
- A subscribe change stops the consumer and starts a new one at the last
  sequence that the connection got plus 1, with the new filter subjects.
  The gateway drops each message with a sequence that is not higher than
  the last one, so nothing is lost and nothing is sent two times.
- A `VIEWS` message that is not a valid `ViewMessageV1` (or whose subject is
  not its `type`) is skipped and counted.

### Delivery

One ordered consumer for each connection, with the filter subjects of its
topics (`viewSubject(topic)`). It is ephemeral, in memory, with no ack, and
the gateway deletes it when the socket closes. This needs only the ADR 0023
grants of `ai-systems-socket-gateway` and no shared state. A shared consumer
for each pod with fan-out in the process is not necessary for one owner.

### Deltas

For each `task:<id>` topic the gateway subscribes core NATS
`delta.<id>.*`. Contracts v0.2.0 has no delta message for NATS yet (task
M2-02). The gateway takes `taskId` and `callId` from the subject and reads
the body as JSON `{ "i": <int>, "text": "<string>" }`, then checks the
frame with `ServerMessageV1`. A body that does not fit is dropped. Deltas
have no `seq` and are best effort (ADR 0019).

## Configuration

| Variable                                                            | Default                                             | Rule                                                                                                      |
| ------------------------------------------------------------------- | --------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `NATS_SERVERS`                                                      | none                                                | comma list                                                                                                |
| `NATS_USER`, `NATS_PASSWORD`                                        | none                                                | the user `ai-systems-socket-gateway`                                                                      |
| `OIDC_ISSUER`                                                       | `https://auth.famashines.com/realms/ai-systems`     | the token `iss` must be this value                                                                        |
| `OIDC_JWKS_URL`                                                     | none: `<OIDC_ISSUER>/protocol/openid-connect/certs` | the key source; on the cluster the in-cluster Keycloak URL, so the pod does not go out through Traefik    |
| `OIDC_AUDIENCE`                                                     | `ai-systems-app`                                    |                                                                                                           |
| `REQUIRED_ROLE`                                                     | `owner`                                             |                                                                                                           |
| `ALLOWED_ORIGINS`                                                   | `https://ai.famashines.com`                         | comma list of exact origins (`scheme://host:port`). An empty value allows only requests with no `Origin`. |
| `MAX_PENDING_HELLO`                                                 | `16`                                                | sockets for each pod that wait for `hello`                                                                |
| `MAX_CONNECTIONS`                                                   | `48`                                                | authenticated sessions for each pod; below the `max_consumers` 64 of `VIEWS`                              |
| `MAX_CONNECTIONS_PER_SUBJECT`                                       | `8`                                                 | sockets for each `sub`                                                                                    |
| `PORT`                                                              | `8080`                                              |                                                                                                           |
| `LOG_LEVEL`                                                         | `info`                                              |                                                                                                           |
| `RELEASE`                                                           | `local`                                             | the image sets it                                                                                         |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | none                                                | traces go out only when one is set                                                                        |

`.env.example` lists them with no values. The service has no database.

## Metrics

| Metric                            | Labels   |
| --------------------------------- | -------- |
| `ws_connections` (gauge)          |          |
| `ws_connections_opened_total`     |          |
| `ws_connections_closed_total`     | `code`   |
| `ws_upgrades_refused_total`       | `status` |
| `ws_frames_received_total`        |          |
| `ws_frames_sent_total`            | `type`   |
| `ws_deltas_dropped_total`         |          |
| `ws_view_messages_invalid_total`  |          |
| `ws_consumer_stop_failures_total` |          |

## Release

When `ci` passes on a push to `main`, `.github/workflows/release.yml`
builds the image of that commit, makes a CycloneDX SBOM (a workflow
artifact), and scans the image with grype. A HIGH or CRITICAL finding that
has a fix stops the release. Findings with no fix do not stop it; they are
in the job log. Then it pushes `ghcr.io/famashines/ai-systems-socket-gateway:sha-<commit>`
and opens or updates the pull request from the branch
`release/ai-systems-socket-gateway-prod` in `famashines/gitops`. That pull request sets the
tag and the digest in `kubernetes/ai-systems/ai-systems-socket-gateway/prod`. The owner
merges it. The job needs the org secret `GITOPS_TOKEN` with access to this
repo. A manual run (`workflow_dispatch`) works only on `main`.

## Known limits

- One ordered consumer for each connection. gitops sets `max_consumers` 64
  on `VIEWS` (ADR 0025), so `MAX_CONNECTIONS` is 48 for each pod. Two pods can
  together pass 64; then a consumer create fails and that socket closes
  `1013`.
- After a crash of the pod, its consumers are not deleted. NATS removes
  them after 5 minutes with no activity (`inactive_threshold` of the
  ordered consumer). Until then they count against that limit.
- A shutdown waits at most 1 s for the close handshakes, terminates the
  rest, then waits at most 5 s for the sessions to stop their consumers.
- The gateway connects to NATS with `kit/nats` (name, inbox prefix
  `_INBOX_socket-gateway`, no reconnect limit, wait on the first connect).
  It does not use `kit/messaging`, because that export loads `mongodb`,
  and a gateway does not install it.

## Commands and tests

See `CLAUDE.md`.
