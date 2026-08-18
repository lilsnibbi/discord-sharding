# Hub Events

`HubClient.events` is a bounded, EventEmitter-style surface for Hub lifecycle events (type `$HubEvents`).
Payloads are frozen objects; listener failures are reported through `onError` and never interrupt Hub
processing or other listeners.

```ts
import { HubClient } from "@lilsnibbi/discord-sharding";

declare const hub: HubClient;

const stop = hub.events.on("bridgeConnected", (payload) => {
	console.log(`Bridge ${payload.bridgeId} connected.`);
});
hub.events.once("shardReady", (payload) => {
	console.log(`Shard ${payload.shardId} is serving Discord traffic on ${payload.bridgeId}.`);
});
stop(); // every registration returns an unsubscribe callback
```

## API

| Method | Behavior |
| --- | --- |
| `on(event, listener)` | Registers; returns an unsubscribe callback. Max 256 listeners per event. |
| `once(event, listener)` | Removed after its first invocation. |
| `off(event, listener)` | Removes by reference. |
| `listenerCount(event)` | Current listener count for one event. |

All listeners are removed automatically when the Hub stops.

## Events

| Event | Payload | Emitted when |
| --- | --- | --- |
| `bridgeConnected` | `{ bridgeId, generation, connectionGeneration }` | a Bridge completed its hello handshake and holds the active connection |
| `bridgeSynchronized` | `{ bridgeId, topologyVersion }` | the Bridge and its retained shards acknowledged one current topology (maintenance cleared) |
| `bridgeDisconnected` | `{ bridgeId, generation }` | the Bridge socket closed; assignments are retained |
| `shardAssigned` | `{ shardId, bridgeId, epoch }` | a shard was committed to a Bridge (first assignment or transfer) |
| `shardDeallocated` | `{ shardId, bridgeId, reason: "capacity" \| "released" }` | a shard lost its owner through capacity planning or operator release |
| `shardReady` | `{ shardId, bridgeId }` | a shard process became Discord-ready |
| `shardStopped` | `{ shardId, bridgeId }` | a shard process stopped (Hub command or Bridge report) |
| `shardFailed` | `{ shardId, bridgeId }` | a shard process reported failure |
| `shardRestartScheduled` | `{ shardId, bridgeId, attempt, delayMs }` | a policy-bounded restart was scheduled for a failed shard |
| `shardRestartsExhausted` | `{ shardId, bridgeId, windowMs }` | the restart budget is spent; the shard is parked until the window slides |
| `error` | `{ error, context }` | any background failure; mirrors the `onError` option |

## Ordering and delivery notes

- Events are emitted synchronously at each state transition, in commit order. `shardAssigned` fires after
  the assignment is persisted, before the owning Bridge is synchronized.
- `bridgeConnected` fires at hello acceptance; routing only works after `bridgeSynchronized`.
- A `shardRestartsExhausted` shard is not abandoned: when the policy window slides, restarts resume
  automatically and a new `shardRestartScheduled` fires.
- Do not perform long synchronous work inside a listener — it runs on the Hub's processing path. Async
  listener rejections are caught and reported.

Payload types are exported: `$HubEventMap`, `$HubEventName`, `$HubEventListener`, plus one `$Hub*Event`
interface per payload (see [api-reference.md](api-reference.md)).
