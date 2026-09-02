# Operations

## Deployment shape

Run one authoritative Hub for a bot deployment and one Bridge on every shard host. Point the Hub at a durable Redis
server so assignments survive Hub restarts. Give each Bridge a stable identity and realistic `maxShards`;
capacity is a safety boundary, not a placement hint.

Bridge and Hub startup order is unrestricted. A Bridge reconnects until stopped, while the Hub verifies its stored
schema version and restores persisted state before serving traffic.

Keep Hub and shard-host clocks synchronized with NTP. Broadcast commits use a Hub-issued Unix timestamp, so clock skew
directly widens the execution spread between shards.

## Credentials and network policy

- Use separate random secrets for `bridgeToken` and `adminToken`.
- Rotate tokens through a staged restart and do not place them in URLs.
- Terminate TLS before traffic leaves a trusted network.
- Restrict Bridge WebSocket access and administration access independently.
- Keep Redis on a private network, require `AUTH`, and prefer `rediss://` when it leaves a trusted boundary.
- Restrict the Bridge SQLite database to the deployment account.

The package authenticates its transport. Application messages still need domain authorization.

## Redis storage

The Hub needs Redis or Valkey 7.2 or newer. There is no migration step: `HubClient.start()` stamps a
`<keyPrefix>:schema` version key, refuses to run against an unsupported version, and caches its Lua scripts.

**Enable AOF persistence for this data.** With snapshot-only durability a crash can lose the most recent assignment
writes, and a restarted Hub would then load a stale ownership epoch and could hand a shard to the wrong Bridge:

```conf
appendonly yes
appendfsync everysec
```

Every key the Hub owns is namespaced by `keyPrefix` (default `sharding`), so several bots can share one server. Give
each bot its own prefix; two Hubs sharing a prefix will fight over the same assignments.

| Key | Type | Contents |
| --- | --- | --- |
| `<prefix>:assignments` | hash | Sticky `shardId` to Bridge ownership with an epoch |
| `<prefix>:bridges` | hash | Latest Bridge generation, capacity, and connection state |
| `<prefix>:shards` | hash | Latest shard process state |
| `<prefix>:analytics:index` | sorted set | Record identifiers scored by collection time |
| `<prefix>:analytics:records` | hash | Analytics record payloads |
| `<prefix>:schema` | string | Stored schema version |

Conditional writes run as server-side Lua, so a stale epoch or timestamp is rejected atomically rather than
last-write-wins.

## Readiness and maintenance

Hub process readiness means the schema check passed, Gateway Bot metadata was accepted, durable state loaded, and the
server started. Bridge process readiness may be defined at two levels:

- `start()` confirms local storage and reconnect ownership.
- `waitUntilConnected()` confirms Hub and retained-shard topology synchronization.

On Hub loss, alert on maintenance but do not restart healthy Discord sessions. New starts, routing, and evaluations
remain unavailable until synchronization returns. Identify requests wait for synchronization instead of failing, so
brief maintenance during a rollout does not restart shard processes.

Maintenance that persists beyond a Bridge's reconnect backoff means the Hub is unreachable rather than busy. Both ends
replace a connection whose peer stops answering, so a Bridge that reports itself connected is exchanging traffic.

## Capacity and reassignment

Monitor assigned, ready, and unassigned shard counts against each Bridge capacity. Reconciliation preserves valid
sticky ownership and moves at most one shard at a time.

Never release a disconnected Bridge only because its socket disappeared. Confirm that the host and Discord sessions
are dead, then use the authenticated Hub operation to release it. Starting a replacement first can create duplicate
Discord sessions.

For a planned Bridge replacement:

1. Stop the old `BridgeClient` and await its cleanup.
2. Independently verify that its shard processes and Discord sessions ended.
3. Release the now-disconnected Bridge through the authenticated Hub operation.
4. Start the replacement with the same stable `id`.

The replacement has a new process generation. The Hub rejects it while unreleased assignments could still belong to
the old generation.

## Analytics

Bridge SQLite keeps local process and Discord samples. Hub SQLite keeps aggregated samples. Neither store applies
automatic retention.

Read or export records before calling the clear operations. Choose a batch size that limits each transaction; the
public methods continue bounded batches until the selected cutoff is clear.

## Shutdown

1. Stop external traffic that creates new administrative work.
2. Stop Bridges and await completion so shard processes receive graceful shutdown before forced termination.
3. Stop the Hub after Bridges are gone.
4. Verify process, socket, request, timer, and database handles are released.

`stop()` and `close()` methods are idempotent. Still log and investigate cleanup failures surfaced through `onError`.

## Monitoring

Record at least:

- Hub lifecycle, schema validation, Gateway metadata, identify queue, session-start exhaustion, and request failures;
- Bridge connection and maintenance changes, reconnect delay, buffered bytes, shard restarts, and shutdown deadlines;
- Hub and shard liveness deadline failures, which indicate a wedged peer rather than a closed connection;
- assigned, unassigned, starting, and Discord-ready shard counts;
- targeted request and evaluation latency, expiry, rejection, and capacity;
- Hub Redis latency, keyspace growth, AOF rewrite status, and evicted-key counters;
- Bridge SQLite size, filesystem errors, process memory, event-loop delay, and Discord WebSocket latency.

Do not publish throughput or capacity claims without measuring the target Bun version, host, shard count, payload
distribution, Discord client behaviour, database, and network.
