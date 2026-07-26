# Examples

The repository ships three concise raw TypeScript deployment examples:

- [`examples/hub.ts`](../examples/hub.ts) starts the control plane and owns graceful signal shutdown.
- [`examples/bridge.ts`](../examples/bridge.ts) connects one deployment and supervises its shard entrypoint.
- [`examples/shard.ts`](../examples/shard.ts) exports a typed bootstrap function for an application-owned Discord
  client; it is not itself a subprocess entrypoint.

## Environment

| Entrypoint | Required values |
| --- | --- |
| Hub | `DISCORD_BOT_TOKEN`, `SHARDING_BRIDGE_TOKEN`, `SHARDING_ADMIN_TOKEN`, `SHARDING_DATABASE_URL` |
| Bridge | `SHARDING_BRIDGE_ID`, `SHARDING_HUB_URL`, `SHARDING_BRIDGE_TOKEN`, `SHARDING_MAX_SHARDS`, `SHARDING_SHARD_SCRIPT` |
| Shard application | Optional bot token passed in `startShard` options; identity values are injected by the Bridge |

Run entrypoints with Bun:

```bash
bun run examples/hub.ts
bun run examples/bridge.ts
```

The shard file is a library-style integration example because the package does not depend on `discord.js`. In an
application, construct its Discord client, call `startShard`, and keep that code in the file launched by the Bridge.
Point `shardScript` at the application file, not at this helper or a compiled artifact.

Configure that application-owned client with the Bridge-injected `SHARDING_SHARD_ID` as its only shard and
`SHARDING_TOTAL_SHARDS` as its total shard count. The helper deliberately cannot reconfigure an existing discord.js
client. Treat missing or invalid identity values as startup failures.

## Targeted communication

Inside a running shard:

```text
await shard.send(4, { kind: "cache.invalidate", guildId });

const status = await shard.request<{ online: boolean }>(
  4,
  { kind: "status" },
  5_000,
);
```

Install one request handler with `onRequest`; install any number of bounded message listeners with `onMessage`. Payload
framing is validated by the package, but handlers must validate application fields and authorization.

## Broadcast evaluation

```text
const counts = await shard.broadcastEval(
  (client, context: { includeUsers: boolean }) => ({
    guilds: client.guilds?.cache?.size ?? 0,
    users: context.includeUsers ? (client.users?.cache?.size ?? 0) : 0,
  }),
  { includeUsers: true },
  10_000,
);
```

Evaluators must be self-contained functions. They cannot close over local variables, and their context and result must
be JSON-compatible. Only Discord-ready shards in the Hub snapshot participate.
