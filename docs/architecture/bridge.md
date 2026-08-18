# Bridge Internals

`BridgeClient` (`src/bridge/BridgeClient.ts`) delegates to `BridgeRuntime`, composed as a class chain:
`BridgeCore -> BridgeRequests -> BridgeShards -> BridgeTopology -> BridgeConnection -> BridgeRuntime`.

## Connection loop

- `start()` launches an endless reconnect loop; each attempt opens a WebSocket to the Hub with bearer
  token plus bridge identity headers (`BridgeConnection.ts`).
- Backoff is exponential with jitter (defaults: 500 ms initial, x2, 30 s cap, 0.2 jitter); the attempt
  counter resets only after a fully synchronized connection.
- Liveness: heartbeats every 10 s; a Hub silent for 45 s gets its socket closed (`1011`) and the loop
  reconnects. Outbound sends are backpressure-capped; exceeding `maxBufferedBytes` closes the socket
  (`1013`).

## Maintenance state machine

Maintenance starts `true` and re-enters on any socket loss or at the top of every non-repeat `hub.sync`.
Entering fails all pending routed work. Clearing requires the full handshake:

```text
hub.sync -> apply assignments -> shard.control.maintenance {maintenance:false, acknowledge:true}
        -> shard.sync.ack from every retained shard -> bridge.sync.ready -> connectionReady
```

A shard that fails to ack within the deadline closes the Hub socket (`1002`) and the loop retries.

## Shard process supervision

- Spawn only on `hub.shard.start`; validated against synchronized `totalShards` and epoch.
- Child env: parent env + option overrides + forced `SHARDING_*` identity variables
  (`src/bridge/runtime/configuration.ts`).
- A still-`starting` process that exceeds `startupTimeoutMs` is marked failed and killed (soft, then
  SIGKILL).
- A Discord-ready shard silent for 45 s is terminated by a 10 s watchdog sweep (`BridgeShards.ts`).
- Crashes are **reported, not restarted** — the Hub owns restarts.
- Per-process inbound IPC queues are serialized and capacity-bounded; overflow or a protocol error
  terminates that shard only, never the Bridge connection.

## Routing legs

| Leg | Message |
| --- | --- |
| shard -> Bridge | `shard.route.request { kind, payload, targetShardId }` |
| Bridge -> Hub | `bridge.route.request` (+ source shard identity) |
| Hub -> dest Bridge | `hub.route.request { kind, payload, sourceShardId, target identity }` |
| dest Bridge -> shard | `shard.control.route.request` |
| response path | `shard.route.response -> bridge.route.response -> hub.route.response -> shard.control.route.response` |

Outbound slots are reserved per request with the request timeout and capacity bounds; disconnects or
resyncs fail every in-flight route on both sides.
