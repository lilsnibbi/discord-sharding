# API reference

Import the public surface from the package root:

```ts
import {
  BridgeClient,
  HubClient,
  ShardClient,
  type $BridgeClientOptions,
} from "@lilsnibbi/discord-sharding";

const bridgeOptions = {
  id: "worker-a",
  hubUrl: "https://hub.example.com",
  token: "replace-with-bridge-token",
  maxShards: 4,
  shardScript: "./src/shard-entry.ts",
} satisfies $BridgeClientOptions;

void new BridgeClient(bridgeOptions);
void HubClient;
void ShardClient;
```

The package exposes raw TypeScript through `src/index.ts`. Internal protocol handlers, registries, schedulers, stores,
validators, timers, and managed-process records are not public entrypoints.

## `HubClient`

`HubClient` owns the HTTP and WebSocket control plane, global Discord identify admission, shard assignment, routing,
Bun SQLite persistence, and administration.

### Lifecycle and administration

| Member | Behaviour |
| --- | --- |
| `new HubClient(options)` | Validates immutable runtime configuration without opening resources |
| `state` | Current Hub lifecycle state |
| `url` | Bound server URL after startup, or `null` before listening |
| `totalShards` | Validated global shard count after startup |
| `start()` | Migrates storage, restores state, fetches Gateway Bot metadata, starts scheduling, and then listens |
| `getTopology()` | Returns an immutable snapshot of Bridges, assignments, ready shards, and unassigned shards |
| `reconcile()` | Applies capacity-aware changes one at a time until stable, then returns updated topology |
| `releaseBridge(bridgeId)` | Explicitly releases a confirmed-dead Bridge and its sticky assignments |
| `clearAnalytics(options?)` | Deletes retained Hub analytics in bounded batches and returns the total removed |
| `stop()` | Idempotently stops admission, sockets, pending work, server, scheduler, and persistence |
| `[Symbol.asyncDispose]()` | Calls `stop()` for `await using` ownership |

Hub startup does not accept traffic until migrations and durable state restoration succeed. Startup failure rolls back
every opened resource.

### `$HubClientOptions`

| Option | Required | Purpose |
| --- | --- | --- |
| `botToken` | yes | Discord token used only for Gateway Bot metadata |
| `bridgeToken` | yes | Token accepted only by Bridge WebSocket connections |
| `adminToken` | yes | Token accepted only by administration requests |
| `databasePath` | no | Built-in SQLite file path (default `./sharding-hub.sqlite`; explicitly allow `":memory:"`) |
| `persistence` | conditional | Custom `$HubPersistence`, mainly for deterministic tests and integrations |
| `totalShards` | no | Explicit global count; otherwise Discord's recommendation |
| `hostname` | no | Listener hostname; default `0.0.0.0` |
| `port` | no | Listener port; default `3000` |
| `gatewayEndpoint` | no | Gateway Bot endpoint override |
| `fetch` | no | Gateway metadata fetch implementation |
| `request` | no | Partial correlated-request policy |
| `payload` | no | Partial transport payload policy |
| `maxBufferedBytes` | no | Per-Bridge WebSocket byte limit; default 4 MiB |
| `maxQueuedMessages` | no | Per-Bridge queued-message limit; default `1024` |
| `maxEvaluations` | no | Concurrent broadcast limit; default `32` |
| `evaluationCommitLeadMs` | no | Delay between prepare and commit; default `25` ms |
| `now`, `wallClock`, `sleep` | no | Deterministic scheduler and persistence hooks |
| `onError` | no | Observer for background failures not returned by a caller promise |

Supply either databasePath (or use default `./sharding-hub.sqlite`) or custom persistence, but not both. `bridgeToken` and
`adminToken` must be distinct.

### HTTP administration

`GET /health` returns lifecycle state and shard count without administration authentication. All other management
requests require `Authorization: Bearer <adminToken>`.

| Request | Operation |
| --- | --- |
| `GET /topology` | Read the current topology |
| `POST /reconcile` | Run serialized reconciliation |
| `DELETE /bridges/:id` | Release one confirmed-dead, disconnected Bridge |
| `DELETE /analytics?before=<ms>&batchSize=<count>` | Clear global analytics in bounded batches |

`/bridge` is reserved for authenticated Bridge WebSocket upgrades and is not an administration route.

## `BridgeClient`

`BridgeClient` runs in each bot deployment. It owns the Hub socket, local analytics, assigned Bun subprocesses,
reconnect loop, routing correlation, and cleanup.

### Properties

| Member | Type | Meaning |
| --- | --- | --- |
| `id` | `string` | Stable deployment identity |
| `maxShards` | `number` | Declared process capacity |
| `generation` | `string` | Unique generation for this client instance |
| `state` | `$BridgeState` | Lifecycle state |
| `connected` | `boolean` | Hub and retained shards share one acknowledged topology |
| `isInMaintenance` | `boolean` | Hub-dependent shard work is unavailable |
| `shards` | `ReadonlyMap<number, $BridgeShardSnapshot>` | Immutable local process snapshot |

### Methods

| Method | Behaviour |
| --- | --- |
| `start()` | Opens SQLite and starts the indefinite Hub reconnect loop; returns this client |
| `waitUntilConnected(timeoutMs?)` | Waits for topology synchronization, with a default bounded deadline |
| `onMaintenanceChange(listener)` | Registers an observer and returns an idempotent cleanup callback |
| `getAnalytics(query?)` | Reads newest-first local SQLite analytics, optionally filtered by shard |
| `clearAnalytics(before?, batchSize?)` | Deletes local samples in bounded batches and returns the total |
| `stop()` | Idempotently closes reconnect work, socket, requests, subprocesses, listeners, timers, and SQLite |
| `[Symbol.asyncDispose]()` | Calls `stop()` |

### `$BridgeClientOptions`

Required options are `id`, `hubUrl`, `token`, `maxShards`, and `shardScript`. `hubUrl` is the Hub HTTP origin; the
Bridge derives its WebSocket endpoint.

Optional settings cover shard arguments, working directory, environment, SQLite path, reconnect and restart policy,
request and payload policy, startup and shutdown deadlines, buffered bytes, deterministic process and socket
factories, sleep and jitter hooks, and background error reporting.

The default SQLite path is `./sharding-bridge.sqlite`. `start()` does not wait for the Hub, so use
`waitUntilConnected()` only when synchronization is a real readiness requirement.

## `ShardClient<Client>`

`ShardClient` runs inside one Bridge-owned subprocess. `Client` extends the structural `$DiscordClient` contract; a
`discord.js` v14 `Client` satisfies it without being imported by package source.

### Properties

| Member | Type | Meaning |
| --- | --- | --- |
| `id` | `number` | Zero-based shard identity |
| `totalShards` | `number` | Global Discord shard count |
| `assignmentEpoch` | `number` | Current assignment fence |
| `processGeneration` | `number` | Current subprocess fence |
| `botClient` | `Client` | Application-owned Discord client |
| `bridge` | `$ShardBridge` | Local maintenance state and observer |
| `state` | `$ShardClientState` | Lifecycle state |
| `isReady` | `boolean` | Whether the Discord client reports ready |
| `pendingRequests` | `number` | Current correlated work retained locally |
| `activeHandlers` | `number` | Targeted request handlers currently executing |

### Methods

| Method | Behaviour |
| --- | --- |
| `start()` | Starts IPC, maintenance handling, readiness observation, and analytics |
| `login(token?)` | Waits for Hub identify admission, then calls the local Discord client |
| `onMessage(listener)` | Adds a targeted message listener and returns its cleanup callback |
| `onRequest(handler)` | Installs the single targeted request handler and returns its cleanup callback |
| `send(targetShardId, payload)` | Sends a JSON-compatible one-way message through the Hub |
| `request<Result>(targetShardId, payload, timeoutMs?)` | Sends a correlated request through the Hub |
| `broadcastEval<Context, Result>(evaluator, context?, timeoutMs?)` | Evaluates on the current ready-shard snapshot |
| `close()` | Idempotently stops IPC, timers, pending requests, handlers, and prepared evaluations |
| `[Symbol.asyncDispose]()` | Calls `close()` |

By default, identity comes from `SHARDING_SHARD_ID`, `SHARDING_TOTAL_SHARDS`,
`SHARDING_ASSIGNMENT_EPOCH`, and `SHARDING_PROCESS_GENERATION`. Explicit options are useful for deterministic tests.

Custom `$ShardTransport` implementations may provide `onDisconnect(listener)`. `ShardClient` uses it to stop pending
work and clean up the Discord client when the transport can no longer reach the Bridge. The registration must return
an idempotent cleanup callback. Set `ownsProcess` only when Sharding is allowed to exit that child process after
transport or authorized-shutdown cleanup.

`broadcastEval` returns `Promise<ReadonlyMap<number, Awaited<Result>>>`. The evaluator source and context are copied
across process boundaries, so the function must not depend on closures.

## Shared policies

`$PayloadPolicy` bounds encoded bytes, nesting depth, and total nodes. Supported values are finite JSON primitives,
plain records, and dense arrays without cycles or accessors.

`$RequestPolicy` sets a default timeout and maximum pending count. Timeout removes correlation state; it does not
terminate a destination handler.

`$ReconnectPolicy` defines bounded exponential delay and jitter. Bridge retry attempts continue until shutdown.

`$RestartPolicy` defines attempts within a rolling window and bounded exponential delay. Restart authorization remains
Hub-controlled.

## Public type groups

| Area | Types |
| --- | --- |
| Common | `$JsonPrimitive`, `$JsonObject`, `$JsonValue`, `$PayloadPolicy`, `$RequestPolicy`, `$ReconnectPolicy`, `$RestartPolicy`, `$Sleep`, `$ErrorListener` |
| Discord | `$DiscordCache`, `$DiscordManager`, `$DiscordWebSocketManager`, `$DiscordClient` |
| Hub | `$HubState`, `$GatewayFetch`, `$PersistedAssignment`, `$PersistedBridge`, `$PersistedShardState`, `$PersistedShard`, `$AnalyticsRecord`, `$PersistedHubState`, `$HubPersistence`, `$HubAssignment`, `$HubBridgeTopology`, `$HubTopology`, `$ClearAnalyticsOptions`, `$HubClientOptions` |
| Hub events | `$HubEvents`, `$HubEventMap`, `$HubEventName`, `$HubEventListener`, `$HubBridgeConnectedEvent`, `$HubBridgeSynchronizedEvent`, `$HubBridgeDisconnectedEvent`, `$HubShardAssignedEvent`, `$HubShardDeallocatedEvent`, `$HubShardLifecycleEvent`, `$HubShardRestartScheduledEvent`, `$HubShardRestartsExhaustedEvent`, `$HubErrorEvent` |
| Bridge | `$BridgeState`, `$BridgeShardState`, `$BridgeShardSnapshot`, `$ShardProcessExit`, `$ShardProcessCallbacks`, `$ShardProcessContext`, `$ShardProcess`, `$ShardProcessFactory`, `$BridgeSocketFactory`, `$BridgeAnalyticsQuery`, `$BridgeClientOptions` |
| Shard | `$ShardClientState`, `$ShardClientOptions`, `$ShardBridge`, `$ShardBridgeSummary`, `$ShardIdentity`, `$ShardTransport`, `$ShardMessageContext`, `$ShardMessageListener`, `$ShardRequestHandler`, `$BroadcastEvaluator` |

Only types re-exported by `src/index.ts` are supported as public API.
