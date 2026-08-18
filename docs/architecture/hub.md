# Hub Internals

`HubClient` (`src/hub/HubClient.ts`) is a thin facade over composed controllers. Add behavior to the
controller that owns the lifecycle stage:

| Controller | Owns |
| --- | --- |
| `HubCore` | shared state maps (assignments, bridges, shards, sessions), topology snapshots, error/event funnels |
| `HubServerController` | HTTP server, WebSocket upgrade/auth, admin endpoints, socket close handling |
| `HubProtocolController` | `bridge.hello` acceptance, shard state/stop handling, identify relay |
| `HubRoutingController` | route/eval correlation, session-closed cleanup |
| `HubAssignmentController` | reconciliation loop, assignment steps, topology sync sends |
| `HubRestartController` | `hub.shard.start` issuance, restart backoff, restart budget |
| `HubLifecycle` | start/stop, release, rollback |

## Session lifecycle

```text
upgrade (/bridge, token + identity headers)
  -> awaiting-hello (deadline)
  -> bridge.hello accepted (generation checks, retained-shard reconciliation)
  -> hub.sync sent
  -> bridge.sync.ready received
  -> phase ready (routable)
```

Rejections close the socket with code `1002`: duplicate active connection, new bridge generation holding
assignments without an operator release, stale connection generation, retained shards that contradict
persisted state.

## Assignment authority

- `reconcile()` serializes every mutation through one queue (`HubLifecycle.ts`); steps are
  `assign`, `transfer`, `unassign`, one shard at a time.
- Transfers stop the source first (`hub.shard.stop` and its `bridge.shard.stopped` ack) before the new
  owner is committed.
- Every step persists, bumps `topologyVersion`, and re-sends `hub.sync` to affected sessions.
- A disconnected Bridge keeps its assignments until an operator releases it
  (`DELETE /bridges/:id` admin route, `HubLifecycle.releaseBridgeAssignments`).

## Restart ownership

`bridge.shard.state failed` schedules a restart with exponential backoff under the restart policy the
Bridge declared in `bridge.hello`. When the budget within `windowMs` is exhausted the shard is parked until
the window slides; the Hub emits `shardRestartsExhausted` when this happens.

## Observability

- `getTopology()` returns a frozen `$HubTopology` snapshot; also served on the admin API (`GET /topology`).
- `events` exposes typed lifecycle events (see [hub-events.md](../hub-events.md)).
- `onError(error, context)` receives background failures; listener exceptions are swallowed.

## Admin HTTP surface

| Route | Auth | Purpose |
| --- | --- | --- |
| `GET /health` | none | state + total shards |
| `GET /topology` | admin token | full topology snapshot |
| `POST /reconcile` | admin token | force a reconciliation pass |
| `DELETE /bridges/:id` | admin token | release a lost Bridge's assignments |
