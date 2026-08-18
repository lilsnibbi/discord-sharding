# Architecture

For implementation-level detail with file references, see the
[architecture deep dive](architecture/overview.md).

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

Hub startup retries the Gateway Bot request with bounded exponential backoff, because Discord is the Hub's only
external dependency and a single unavailable response would otherwise stop the whole deployment from starting. HTTP
401 and 403 are configuration failures and are not retried.

Every topology change briefly returns a Bridge to maintenance, so identify requests wait for synchronization to
finish rather than failing. Failing them would kill the shard process, which advances topology again and repeats.

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

Both ends of every link enforce a liveness deadline, because a wedged peer holds a socket open without answering. The
Hub closes idle Bridge sockets and answers each Bridge heartbeat, and a Bridge replaces a Hub connection that sends
nothing within its deadline. A Bridge also terminates a Discord-ready shard process that stops heartbeating; that exit
is reported as a failure so the Hub restarts it under the Bridge's restart policy.

A shard process that exits between a Hub decision and the local delivery is an expected local condition. The Bridge
answers the waiting Hub with a failure for that shard and keeps its connection, so one dead process never drops the
remaining shards on that host into maintenance.

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
