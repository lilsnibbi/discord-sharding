# Getting started

## 1. Install

Use Bun 1.3.14 or newer:

```bash
bun add @snibbilabs/sharding
```

Add `discord.js` v14 in the bot application if it is not already present. Sharding uses a structural client contract
and does not import or bundle it.

## 2. Prepare SQLite

Specify a file path for Hub persistence (or default to `./sharding-hub.sqlite`):

```powershell
$env:SHARDING_DATABASE_PATH = "./sharding-hub.sqlite"
```

`HubClient.start()` applies the migrations packaged with the installed library before it accepts traffic. Installed
consumers do not run this repository's `db:*` scripts. A deployment that checks out this repository may use
`bun run db:check` to validate migration consistency or `bun run db:migrate` to apply the same migrations ahead of
startup.

## 3. Start the Hub

Use the [Hub entrypoint](../examples/hub.ts). It requires:

- the Discord bot token for Gateway Bot metadata;
- a high-entropy Bridge token;
- a different high-entropy administration token;
- the SQLite database path (`databasePath`).

Expose the Hub through TLS in production. Restrict its administrative HTTP surface separately from Bridge WebSocket
traffic.

## 4. Start each Bridge

Use the [Bridge entrypoint](../examples/bridge.ts). Give every deployment a stable `id`, the same Bridge token, its
actual process capacity, and the shared shard script.

`start()` returns after local SQLite is ready and the reconnect loop has begun. Use `waitUntilConnected()` only when an
external readiness gate needs confirmed Hub and shard synchronization.

A reconnect by the same running `BridgeClient` keeps its process generation and sticky ownership. A replacement
instance has a new generation. Stop the old instance, independently confirm its shard processes and Discord sessions
ended, release the disconnected Bridge through the Hub, and only then start the replacement with the same stable
`id`.

## 5. Start each shard

The Bridge supplies shard identity and fencing values as environment variables. The
[shard integration example](../examples/shard.ts) shows the required `ShardClient` startup and login order for an
application-owned Discord client. Call that helper, or use the same sequence, from the actual file launched by the
Bridge. Use its `configure` callback to register application handlers before IPC and Discord login begin.

Construct the Discord client for exactly the injected shard: pass `SHARDING_SHARD_ID` as its only shard ID and
`SHARDING_TOTAL_SHARDS` as its shard count. `ShardClient` validates those values for its own protocol but cannot change
an already-created discord.js client.

Do not call the Discord client login method directly. `ShardClient.login()` first obtains the Hub's global identify
grant.

## 6. Validate staging

Before production, verify:

- every expected shard is assigned and becomes Discord-ready;
- declared Bridge capacity matches host limits;
- stopping the Hub enters maintenance without ending healthy Discord sessions;
- Hub recovery clears maintenance only after topology synchronization;
- targeted messages and requests cross between two Bridges;
- graceful Bridge shutdown releases subprocesses, sockets, timers, requests, and SQLite;
- the explicit dead-Bridge release procedure is understood and access-controlled.

Continue with [operations](operations.md) for deployment policy or [troubleshooting](troubleshooting.md) for failures.
