# Architecture

Sharding has one mandatory control path:

```text
                         +------------------------------+
                         | HubClient                    |
                         | assignment | identify |      |
                         | routing | administration     |
                         +---------------+--------------+
                                         | Bun SQLite (bun:sqlite)
                 authenticated WebSocket |
                         +---------------+--------------+
                         | BridgeClient                  |
                         | capacity | processes | SQLite |
                         +---------------+--------------+
                                         | Bun IPC
                         +---------------+--------------+
                         | ShardClient + Discord client  |
                         +------------------------------+
```

Multiple Bridges connect to the same Hub. Each Bridge starts a separate Bun process for every assigned shard.

## Ownership and assignment

The Hub is the only assignment authority. It persists a sticky `shardId` to Bridge mapping with an ownership epoch and
never assigns more than a Bridge's declared `maxShards`.

Planning keeps valid assignments when possible, fills available capacity evenly, and leaves excess shards unassigned.
Rebalancing transfers one shard at a time. The source process must stop before the Hub commits a new owner and
authorizes the destination process.

A disconnected Bridge retains its assignments. The Hub cannot assume its Discord sessions are dead, so an operator
must explicitly release a permanently lost Bridge before those shards can move.

## Identify scheduling

The Hub fetches Discord Gateway Bot metadata and owns one global identify scheduler for all Bridges. It observes
Discord's concurrency buckets, five-second windows, remaining session starts, and reset time. `ShardClient.login()`
waits for this admission before it calls the application Discord client.

This controls starts made through Sharding. Other processes using the same bot token must not bypass the same admission
boundary.

## Routing

Targeted messages, correlated requests, and broadcast evaluation always use:

```text
source Shard -> source Bridge -> Hub -> destination Bridge -> destination Shard
```

The Hub remains in the path even when both shards belong to one Bridge. This gives every operation the same
authentication, generation checks, capacity limits, expiry, and failure semantics.

Broadcast evaluation snapshots Discord-ready shards, prepares the evaluator on every destination, and commits with a
short shared lead time. Context and results must be JSON-compatible. A caller timeout removes correlation state but
cannot forcibly stop application code already running inside another process.

## Maintenance and reconnects

A Bridge enters maintenance as soon as its Hub socket is unavailable. It keeps existing shard processes and Discord
sessions alive, reconnects with bounded backoff until shutdown, and blocks Hub-dependent work. Maintenance clears only
after the Bridge and its retained shards acknowledge one current topology.

Bridge startup prepares local SQLite and begins reconnecting without waiting for Hub availability. Hub startup applies
SQLite migrations and restores durable state before accepting traffic.

## Protocol boundaries

All WebSocket and IPC envelopes are versioned and validated as untrusted data. Payload depth, size, keys, arrays,
requests, queues, listeners, evaluator source, prepared work, and buffered bytes have explicit limits.

Bridge generations reject stale deployment sockets. Process generations and assignment epochs reject stale shard
messages. Correlation identifiers expire, and shutdown rejects retained work.

Protocol validation protects framing; application code must still validate its own payload schemas and authorization.

## Storage

- Hub SQLite stores assignments, latest Bridge and shard state, and global analytics.
- Bridge SQLite stores local process and Discord analytics.
- Analytics have no automatic retention. Operators clear them explicitly in bounded batches.
- Database handles are owned by their client and close during rollback or shutdown.

Runtime package source uses Bun-native SQLite, HTTP, WebSocket, subprocess, and IPC capabilities. Directory-format
SQL migrations remain authoritative; exact `arktype@2.2.3` supports schema and runtime validation. `discord.js` remains an optional application-owned peer.
