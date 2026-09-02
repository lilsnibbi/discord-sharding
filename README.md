<div align="center">

# Sharding

**Bun-native Discord shard orchestration across Hub, Bridge, and shard processes.**

Orchestrate, assign, schedule, route, and monitor Discord shards through one focused Bun and TypeScript stack.

[![CI](https://github.com/lilsnibbi/discord-sharding/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/lilsnibbi/discord-sharding/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@lilsnibbi/discord-sharding?logo=npm)](https://www.npmjs.com/package/@lilsnibbi/discord-sharding)
[![Bun](https://img.shields.io/badge/bun-%3E%3D1.3.14-000000?logo=bun)](https://bun.com)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

[`Documentation`](docs/README.md) · [`API reference`](docs/api-reference.md) · [`Getting started`](docs/getting-started.md) · [`Architecture`](docs/architecture.md)

<br />

<code>Bun</code> <code>TypeScript</code> <code>Redis</code> <code>ArkType</code> <code>Discord.js</code>

</div>

> [!NOTE]
> Sharding is Bun-first and raw TypeScript. You control the Hub server, its Redis database, Bridge workers, and application Discord clients.

> [!WARNING]
> Sharding is built natively for Bun (>= 1.3.14). Do not run with Node.js, npm, or npx.

## What is Sharding?

Sharding is a distributed Discord shard orchestration system with one Hub control plane, host Bridges, and application Shard workers.

It provides centralized Discord Gateway identify scheduling, sticky shard assignment rebalancing, inter-process communication (IPC) routing, and durable Bun Redis persistence.

<table>
<tr>
<td width="33%" valign="top">

### [HubClient](docs/api-reference.md#hubclient)

Central WebSocket control plane, Gateway identify scheduler, sticky assignments, Redis-backed state, and HTTP administration API.

</td>
<td width="33%" valign="top">

### [BridgeClient](docs/api-reference.md#bridgeclient)

Deployment host supervisor managing Bun shard subprocesses, reconnect loops, backpressure, and local SQLite analytics.

</td>
<td width="33%" valign="top">

### [ShardClient](docs/api-reference.md#shardclient)

Application process client providing correlated IPC requests, broadcast evaluation, maintenance isolation, and Discord login gating.

</td>
</tr>
</table>

### Core Architecture

| Component | Responsibility | Persistence / Protocol |
| --- | --- | --- |
| `HubClient` | Global assignment, identify scheduling, administration | Bun Redis (`RedisClient`) |
| `BridgeClient` | Subprocess supervision, capacity monitoring, analytics | Local SQLite & WebSockets |
| `ShardClient` | Application logic, IPC handling, Discord client integration | Bun IPC |

## Quick start

> [!IMPORTANT]
> Sharding requires **Bun** (>= 1.3.14) and a **Redis** (or Valkey) server 7.2 or newer.

### 1. Install package

```bash
bun add @lilsnibbi/discord-sharding
```

### 2. Start the Hub

```typescript
import { HubClient } from "@lilsnibbi/discord-sharding";

const hub = new HubClient({
	adminToken: process.env.HUB_ADMIN_TOKEN ?? "admin-secret-key-12345",
	botToken: process.env.DISCORD_BOT_TOKEN ?? "",
	bridgeToken: process.env.HUB_BRIDGE_TOKEN ?? "bridge-secret-key-12345",
	redisUrl: process.env.SHARDING_REDIS_URL ?? "redis://127.0.0.1:6379",
});

await hub.start();
```

### 3. Start a Bridge

```typescript
import { BridgeClient } from "@lilsnibbi/discord-sharding";

const bridge = new BridgeClient({
	hubUrl: "http://127.0.0.1:3000",
	id: "bridge-host-01",
	maxShards: 4,
	shardScript: "./src/shard.ts",
	token: process.env.HUB_BRIDGE_TOKEN ?? "bridge-secret-key-12345",
});

await bridge.start();
```

### 4. Integrate the Shard Client

```typescript
import { ShardClient, type $DiscordClient } from "@lilsnibbi/discord-sharding";

declare const discordClient: $DiscordClient;

const shard = new ShardClient(discordClient);
await shard.start();
await shard.login();
```

Continue with the [getting started guide](docs/getting-started.md) for full production deployment workflows.

## Documentation

<table>
<tr>
<td valign="top">

**Learn**

- [Getting started](docs/getting-started.md)
- [Architecture](docs/architecture.md)
- [Examples](docs/examples.md)

</td>
<td valign="top">

**Reference**

- [API reference](docs/api-reference.md)
- [Hub options](docs/api-reference.md#hubclientoptions)
- [Bridge options](docs/api-reference.md#bridgeclientoptions)
- [Shard Client](docs/api-reference.md#shardclient)

</td>
<td valign="top">

**Operate**

- [Operations](docs/operations.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Performance](docs/performance.md)

</td>
</tr>
</table>

Browse everything from the **[documentation hub](docs/README.md)**.

## Commands

<details>
<summary><strong>Development and validation</strong></summary>

| Command | Purpose |
| --- | --- |
| `bun install` | Install dependencies |
| `bun run typecheck` | Strict TypeScript diagnostics |
| `bun run check` | Run all formatting, line, import, doc, and JSDoc checks |
| `bun run test` | Run unit and integration tests |
| `bun run verify` | Full verification and package artifact check |

</details>

<details>
<summary><strong>Database</strong></summary>

| Command | Purpose |
| --- | --- |
| `bun run test:redis` | Run the live Redis integration suite against `SHARDING_REDIS_URL` |

</details>

<details>
<summary><strong>Testing and verification</strong></summary>

| Command | Purpose |
| --- | --- |
| `bun run test:redis` | Run live Redis persistence integration tests |
| `bun run test:coverage` | Run test coverage report |
| `bun run test:performance` | Run core performance budget checks |
| `bun run pack:check` | Validate raw TypeScript package archive |

</details>

## Project status

Sharding provides a production-ready Hub, Bridge, and Shard client stack for Bun runtimes. Before deploying to production, review the [operations](docs/operations.md) and [troubleshooting](docs/troubleshooting.md) guides.

## Community, security, and license

[Contributing](CONTRIBUTING.md) · [Security policy](SECURITY.md) · [Issues](https://github.com/lilsnibbi/discord-sharding/issues)

Licensed under the [Apache License 2.0](LICENSE).
