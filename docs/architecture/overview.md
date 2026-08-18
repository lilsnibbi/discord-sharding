# Architecture Overview

Deep-dive companion to the [architecture contract](../architecture.md). This directory documents how the
implemented flow actually moves through the code, with file references.

- [Hub internals](hub.md)
- [Bridge internals](bridge.md)
- [Shard internals](shard.md)

## The three processes

```text
HubClient  --(authenticated WebSocket)-->  BridgeClient  --(Bun IPC)-->  ShardClient + Discord client
```

| Process | One per | Owns |
| --- | --- | --- |
| Hub | bot | assignments, identify scheduling, routing, admin HTTP, SQLite state |
| Bridge | deployment host | shard subprocesses, reconnect loop, local analytics SQLite |
| Shard | Discord shard | one application Discord client |

## How a Bridge registers with the Hub

1. Bridge opens a WebSocket to the Hub's `/bridge` path with an `Authorization` bearer token plus
   `X-Sharding-Bridge-Id`, `X-Sharding-Bridge-Generation`, and `X-Sharding-Connection-Generation` headers
   (`src/bridge/runtime/BridgeConnection.ts`).
2. The Hub validates the token in constant time and opens a session in phase `awaiting-hello` with a hello
   deadline (`src/hub/client/HubServerController.ts`).
3. The first message must be `bridge.hello`, echoing the upgrade headers exactly and declaring `maxShards`,
   the restart policy, and any retained running shards (`src/hub/client/HubProtocolController.ts`).
4. The Hub reconciles retained shards against persisted state, persists the Bridge as connected, and pushes
   `hub.sync` with that Bridge's assignments and the topology version.
5. The Bridge synchronizes its local shards (maintenance-clear handshake), then answers `bridge.sync.ready`.
   Only then is the session phase `ready` and routable.

## How a shard is created

1. Hub reconciliation (`src/hub/client/HubAssignmentController.ts`) assigns a shard to a Bridge and sends
   `hub.shard.start { shardId, totalShards, assignmentEpoch }`.
2. The Bridge validates the command against its synchronized topology, bumps a per-shard
   `processGeneration`, and spawns `bun <script>` with JSON IPC
   (`src/bridge/runtime/BridgeTopology.ts`, `src/bridge/shards/ManagedShardProcess.ts`).
3. Identity reaches the child through environment variables: `SHARDING_SHARD_ID`, `SHARDING_TOTAL_SHARDS`,
   `SHARDING_ASSIGNMENT_EPOCH`, `SHARDING_PROCESS_GENERATION`, `SHARDING_BRIDGE_ID`.
4. The child constructs a `ShardClient`, which sends `shard.booted`, heartbeats every 10 seconds, and waits
   for a Hub identify grant before logging in to Discord.

Crash handling is split: the Bridge only reports a dead process (`bridge.shard.state failed`); the **Hub**
owns restarts, applying the restart policy the Bridge declared in `bridge.hello`
(`src/hub/client/HubRestartController.ts`).

## How messages flow

Every targeted message, request, and broadcast evaluation traverses the full path, even when source and
destination live on the same Bridge:

```text
shard --IPC--> source Bridge --WS--> Hub --WS--> destination Bridge --IPC--> shard
```

This keeps authentication, generation checks, capacity limits, expiry, and failure semantics identical for
every operation. See [Bridge internals](bridge.md#routing-legs) for the per-leg message types.

## Trust boundaries

- Every process, network, database, environment, and JSON boundary is validated at runtime
  (`src/protocol/codec.ts`, `src/internal/schemas.ts`).
- Bridge generations reject stale deployment sockets; process generations and assignment epochs reject
  stale shard messages.
- Limits for payload depth, size, node count, queues, and evaluator source live in
  `src/internal/limits.ts`.

## Verified assumptions (orientation notes)

- `docs/architecture.md` matches the implementation as of this audit.
- The Bridge never restarts its own shard processes; a Bridge cut off from the Hub keeps existing
  processes alive but cannot replace crashed ones until the Hub reconnects.
- "Connected" is two different facts: persistence writes `connected: true` at hello-accept, while
  topology snapshots report `connected` only once the session phase is `ready` (post-sync).
- A shard that exhausts its restart budget is parked silently until the policy window slides; the
  `shardRestartsExhausted` Hub event now surfaces this (see [hub-events.md](../hub-events.md)).
