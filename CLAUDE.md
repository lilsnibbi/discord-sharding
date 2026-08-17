# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`@lilsnibbi/discord-sharding` is a Bun-native Discord shard orchestration library shipped as **raw TypeScript** (`src` is the published entry point — no build step, no `dist`, no declarations). Bun >= 1.3.14 only; never run Node, npm, npx, or `bun link`.

## Commands

```bash
bun install
bun run typecheck        # bunx tsc --noEmit, strict
bun run check            # maintained-file line limit + biome check (format + lint)
bun run check:fix        # biome check --write
bun run check:docs       # local markdown link/anchor validation for *.md and docs/**
bun run check:examples   # type-checks every ts fence in markdown and JSDoc @example
bun run check:imports    # import/type policy (see "Enforced policies")
bun run check:jsdoc      # public API JSDoc + $-prefix + src/types placement
bun run test             # bun test, excludes tests/performance
bun run test:sqlite      # native file-backed SQLite integration test
bun run test:coverage    # LCOV, package-wide 80% minimum enforced by scripts/coverage.ts
bun run test:performance # tests/performance budgets
bun run pack:check       # packs, installs, type-checks and runs the raw-source archive
bun run verify           # everything above, in CI order
```

Single test file or test name:

```bash
bun test tests/hub/client-routing.test.ts
bun test --test-name-pattern "rebalance"
```

Migrations (directory-format SQL under `migrations/`, authoritative over any code):

```bash
bun run db:check                      # applies all migrations to :memory: and asserts tables
bun run db:new -- short_name          # scaffolds migrations/<timestamp>_<slug>/migration.sql
SHARDING_DATABASE_PATH=./x.sqlite bun run db:migrate
```

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs the same steps as `bun run verify`. Never publish, push, deploy, tag, or release unless explicitly asked.

## Architecture

Three processes, one mandatory control path. See [docs/architecture.md](docs/architecture.md) for the full contract.

```text
HubClient  --(authenticated WebSocket)-->  BridgeClient  --(Bun IPC)-->  ShardClient + Discord client
```

- **`src/hub`** ([HubClient.ts](src/hub/HubClient.ts)) — one Hub per bot. Sole assignment authority (sticky `shardId` to Bridge mapping with an ownership epoch), owner of the single global Discord identify scheduler, WebSocket routing, HTTP admin API, and Bun SQLite persistence. `HubClient` is a thin facade over a chain of composed controllers: `HubCore` (shared state maps) → `HubServerController` → `HubProtocolController` → `HubRoutingController` → `HubAssignmentController` → `HubRestartController` → `HubLifecycle`. Add behavior to the controller that owns that lifecycle stage, not to `HubClient`.
- **`src/bridge`** ([BridgeClient.ts](src/bridge/BridgeClient.ts)) — one Bridge per deployment host. `BridgeClient` delegates to `BridgeRuntime`, itself composed from `BridgeCore`/`BridgeConnection`/`BridgeShards`/`BridgeTopology`/`BridgeRequests`. Supervises Bun subprocesses (`ManagedShardProcess`), reconnects with bounded backoff, and keeps local SQLite analytics.
- **`src/shard`** ([ShardClient.ts](src/shard/ShardClient.ts)) — application-facing. `ShardClient` extends `ShardInbound`; `login()` waits for a Hub identify grant before touching the application's Discord client.
- **`src/protocol`** — versioned wire envelopes ([types.ts](src/protocol/types.ts)), [codec.ts](src/protocol/codec.ts) key validation, [readers.ts](src/protocol/readers.ts) typed field readers.
- **`src/internal`** — limits, policies, payload normalization, [RequestRegistry.ts](src/internal/RequestRegistry.ts), [schemas.ts](src/internal/schemas.ts). ArkType imports stay confined here and in validation modules.
- **`src/types`** — every public exported type, all `$`-prefixed.

Invariants worth knowing before changing routing or assignment code:

- Routing always traverses `shard → source Bridge → Hub → destination Bridge → shard`, even when both shards live on one Bridge, so every operation shares one authentication, generation, capacity, expiry, and failure path.
- A disconnected Bridge keeps its assignments; only an explicit operator release lets those shards move. Rebalancing transfers one shard at a time and the source must stop before the Hub commits a new owner.
- Bridge generations reject stale deployment sockets; process generations and assignment epochs reject stale shard messages.
- A Bridge enters maintenance the moment its Hub socket drops, keeps existing Discord sessions alive, and clears maintenance only after Bridge and retained shards acknowledge one current topology.
- Every process, network, database, environment, and JSON boundary is untrusted and validated at runtime. Payload depth, size, node count, request counts, queues, and evaluator source are all bounded in [src/internal/limits.ts](src/internal/limits.ts).

## Enforced policies

These are validated by scripts and will fail CI, so follow them while writing rather than after:

- **No file over 500 lines**, anywhere in the repo except `.git`, `.tmp`, `coverage`, `node_modules`, `release-assets`. Split by lifecycle or domain responsibility into composed controllers/stores/helpers — never into arbitrary numbered chunks.
- **No Node built-ins** (`node:*` or bare builtins). Bun APIs and Web standards only; the `process` surface is allowed solely for Bun IPC and signal handling.
- **Import allowlist by directory**: `src/**` may import only `bun`, `bun:*`, and `arktype`; `tests/**` adds `discord.js`; `examples/**` adds `discord.js` and `@lilsnibbi/discord-sharding`; `scripts/**` adds `typescript/unstable/ast` and `typescript/unstable/sync`. `arktype@2.2.3` is pinned exactly and is the only production dependency — do not add another. `discord.js` is an optional peer and must never be imported by `src`.
- **Relative imports and re-exports must be extensionless.**
- **No `any`, no double-casts through `unknown`, no `@ts-ignore` / `@ts-nocheck` / `@ts-expect-error`.** Fix diagnostics instead of suppressing them. `noNonNullAssertion` is a Biome error.
- **Public API surface**: exported types and interfaces must start with `$` and live under `src/types`. Every public class, member, function, option, and exported type needs meaningful JSDoc (>= 12 chars of real summary, `@param` for each parameter). Keep [src/index.ts](src/index.ts) narrow — no internal protocol records, registries, schedulers, stores, or validators.
- **Errors**: throw the domain classes in [src/errors/ShardingError.ts](src/errors/ShardingError.ts) (`CAPACITY`, `CONFIGURATION`, `PERSISTENCE`, `PROTOCOL`, `REMOTE`, `STATE`, `TIMEOUT`, `TRANSPORT`) and preserve the underlying failure via `cause`.
- **Formatting** is Biome: tabs, width 120, double quotes, semicolons, trailing commas, organized imports.
- Any `ts`/`typescript` fence added to a root or `docs/` markdown file is type-checked by `check:examples`, and every local markdown link is resolved by `check:docs`.

## Tests

`bun:test`, rooted at `tests/` (see [bunfig.toml](bunfig.toml)). Use deterministic fakes for process, clock, Discord, WebSocket, SQLite, and persistence — normal tests must never need live Discord or external services. Shared harnesses (`tests/hub/client-harness.ts`, `tests/bridge/client-harness.ts`) stay separate from focused suites. Cover both success and failed-cleanup paths for lifecycle and protocol changes; assignment transfers, identify buckets, stale generations, backpressure, request expiry, and broadcast prepare/commit deserve extra scrutiny.

## Scope

This package is Discord shard orchestration only. Do not add Discord clients, REST wrappers, builders, voice, media, or general utilities.
