# Shard Internals

`ShardClient` (`src/shard/ShardClient.ts`) extends `ShardInbound` extends `ShardCore`. The application owns
the Discord client; the shard runtime wraps it.

## Identity sources

Resolved from options first, then environment (set by the Bridge at spawn):

| Option | Environment variable | Default |
| --- | --- | --- |
| `shardId` | `SHARDING_SHARD_ID` | `0` |
| `totalShards` | `SHARDING_TOTAL_SHARDS` | `1` |
| `assignmentEpoch` | `SHARDING_ASSIGNMENT_EPOCH` | `1` |
| `processGeneration` | `SHARDING_PROCESS_GENERATION` | `1` |
| `bridgeId` | `SHARDING_BRIDGE_ID` | `null` |

The live cluster view (bridge shard counts, total bridges) arrives over the wire after synchronization —
see [shard-identity.md](../shard-identity.md).

## Lifecycle

1. `start()` registers the IPC transport, sends `shard.booted`, begins 10 s heartbeats, ready polling, and
   optional analytics.
2. `login()` sends `shard.identify.request` and waits for the Hub-granted
   `shard.control.identify.response` before calling the application client's `login()`. Grants are
   scheduled globally by the Hub's identify scheduler (Discord bucket-aware).
3. `shard.ready` is sent once the Discord client first reports ready.
4. `shard.control.shutdown` runs the app's `onShutdown`, destroys the Discord client, acks, and exits.

## Failure containment

- Inbound IPC is serialized through one queue with a capacity bound; overflow fails the transport.
- Any transport failure: pending requests reject, maintenance latches `true`, the Discord client is
  destroyed, and the process exits (code 1) when the transport owns the process. The Bridge reports the
  exit; the Hub restarts under policy.
- Application handler errors are serialized back to the caller as `REMOTE` errors; they never kill the
  shard process.

## Application surface

- `send(targetShardId, payload)` — one-way message.
- `request(targetShardId, payload, timeoutMs?)` — correlated request to another shard's `onRequest`
  handler.
- `broadcastEval(fn, context?, timeoutMs?)` — two-phase (prepare/commit) evaluation on every
  Discord-ready shard; results as `ReadonlyMap<shardId, result>`.
- `onMessage` listeners (capped at 256) and a single `onRequest` handler; contexts carry `sourceShardId`
  and an abort signal.
- `bridge.isInMaintenance` + `bridge.onMaintenanceChange(listener)` for maintenance windows.
- `identity` — frozen self-description snapshot (see [shard-identity.md](../shard-identity.md)).
