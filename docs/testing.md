# Testing

How this package is verified, layer by layer. Normal tests never need live Discord — deterministic fakes
cover process, clock, Discord, WebSocket, SQLite, and persistence boundaries.

## Harness layers

| Harness | Real parts | Faked parts | Used by |
| --- | --- | --- | --- |
| `tests/hub/client-harness.ts` | HubClient logic | `Bun.serve` (monkey-patched), sockets, persistence | `tests/hub/client-*.test.ts` |
| `tests/bridge/client-harness.ts` | BridgeClient logic | Hub WebSocket, shard process factory | `tests/bridge/*.test.ts` |
| `tests/utilities/runtime-stack.ts` | Hub HTTP/WS server, BridgeClient, ShardClient, routing | Discord client, shard processes (in-process), persistence | `tests/integration/runtime-stack.test.ts`, fault injection, performance |
| `tests/fixtures/shard-process.ts` | Everything incl. Bun subprocesses + SQLite | Discord client | `tests/integration/shard-process*.test.ts` |
| `tests/utilities/live-gateway-client.ts` | Everything incl. the real Discord gateway | nothing | `tests/integration/live-*.test.ts` (TOKEN-gated) |

## Live verification (TOKEN-gated)

Tests in `tests/integration/live-gateway.test.ts` and `tests/integration/live-stack.test.ts` run only when
`process.env.TOKEN` holds a bot token; otherwise they skip. The live client
(`tests/utilities/live-gateway-client.ts`) is a minimal raw-WebSocket gateway client: zero intents, no
command registration, no REST calls, no dependencies. The live stack test boots a real Hub (live
gateway-bot metadata), a real Bridge, and two real shard subprocesses, then verifies identify scheduling,
READY, cross-shard routing, the identity snapshot, and Hub events.

Never commit or print the token; `.env` is git-ignored and loaded by Bun automatically.

## What is verified where

| Concern | Coverage |
| --- | --- |
| Reconnection: bridge ↔ hub | backoff loop (`tests/bridge/client-lifecycle.test.ts`), auth-reject then reconnect (`runtime-stack.test.ts`), Hub restart with shard processes surviving (`shard-process.test.ts`), Hub restart with 3 persisted bridges issuing zero unnecessary commands (`tests/hub/recovery.test.ts`) |
| Reconnection: shard ↔ bridge | IPC disconnect fails the shard fast; the Bridge reports it and the Hub restarts under policy (`tests/shard/client.test.ts`, `tests/bridge/process.test.ts`, `tests/integration/shard-process-crash.test.ts`) |
| Liveness deadlines | silent ready shard terminated (`tests/bridge/liveness.test.ts`), hub-silence deadline math |
| Stale generation rejection | `tests/bridge/topology-failures.test.ts`, `tests/hub/client-protocol.test.ts` |
| Shard identity | `tests/shard/identity.test.ts` (frozen, stale-version guarded), end-to-end in `live-stack.test.ts` |
| Hub events | `tests/hub/client-events.test.ts` |
| Shard-to-shard routing | same-bridge and cross-bridge through real sockets (`runtime-stack.test.ts`), load budgets (`tests/performance/routing.performance.test.ts`) |
| Error containment / allocation | `tests/integration/fault-injection.test.ts` (mid-handshake kills, throwing handlers, double-allocation provocation), `tests/hub/recovery.test.ts` (fail-closed) |
| Real subprocess crash + restart | `tests/integration/shard-process-crash.test.ts` |

## Commands

```bash
bun run test             # everything except tests/performance (live tests skip without TOKEN)
bun run test:performance # routing + planning + codec budgets
bun run test:coverage    # package-wide 80% minimum
bun run verify           # full CI pipeline
```

Single file or name:

```bash
bun test tests/integration/fault-injection.test.ts
bun test --test-name-pattern "same-bridge"
```
