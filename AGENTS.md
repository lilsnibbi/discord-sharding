# Sharding contributor guide

## Purpose

This repository contains one Bun-first raw TypeScript package for Discord shard orchestration:

- `src/hub`: global assignment, identify scheduling, routing, administration, and Bun SQLite persistence.
- `src/bridge`: Hub connectivity, Bun subprocess supervision, and local SQLite analytics.
- `src/shard`: application-facing Discord client integration and Bun IPC.
- `src/protocol`, `src/internal`, `src/errors`, and `src/types`: shared, narrowly exported support code.

Do not add unrelated Discord clients, REST wrappers, builders, voice, media, or utility packages.

## Runtime and dependencies

- Use Bun APIs or Web standards first.
- Use the compatible `process` surface only for Bun IPC and signal handling.
- Use `bun` and `bunx`; never use npm, npx, Node, or `bun link`.
- The only production dependency is exact `arktype@2.2.3` for schema and runtime validation. Do not add another production dependency.
- `discord.js` is an optional peer and must never be imported by package source.
- Hub persistence, runtime queries, and Bridge analytics use `bun:sqlite`. ArkType imports stay in internal schemas and validation modules, and directory-format SQL migrations are authoritative.
- Ship `src` directly. Never emit or package JavaScript, declarations, maps, or `dist`.

## Types and public API

- Keep TypeScript strict and fix diagnostics without suppression.
- Never use `any`, `as any`, double-casts through `unknown`, `@ts-ignore`, or `@ts-nocheck`.
- Prefix every public, user-facing exported type and interface with `$`.
- Keep public exported types in dedicated files under `src/types`.
- Keep `src/index.ts` narrow; do not expose internal protocol records, registries, schedulers, stores, or validators.
- Use extensionless relative imports and re-exports.
- Validate every process, network, database, environment, and JSON boundary at runtime.
- Preserve underlying failures with domain errors and `cause`.
- Give every public class, method, function, option, property, and exported type concise production JSDoc.

## Lifecycle and performance

- Make ownership of listeners, timers, subprocesses, sockets, database clients, requests, and retained callbacks clear.
- Roll back partial startup and make shutdown idempotent.
- Bound requests, queues, payloads, evaluator source, deduplication, caches, and retained references.
- Bound retries with backoff while allowing Bridge reconnect attempts to continue until shutdown.
- Preserve existing Discord sessions during Hub maintenance.
- Measure meaningful optimizations and avoid unsupported performance claims.

## Modularity

- No maintained repository file may exceed 500 lines.
- Split by lifecycle or domain responsibility before a file approaches the limit; split again when one extracted
  module still exceeds it.
- Prefer small composed controllers, stores, protocol handlers, and typed helpers over large client classes or
  arbitrary numbered file chunks.
- Keep shared test harnesses separate from focused test suites.

## Tests

All tests use `bun:test`. Prefer deterministic process, clock, Discord, WebSocket, SQLite, and persistence fakes. Normal
tests must not require live Discord or external services.

Cover successful and failed cleanup for lifecycle and protocol defects. Review assignment transfers, Discord identify
buckets, stale generations, backpressure, request expiry, and broadcast prepare/commit especially carefully.

## Commands

```bash
bun install
bun run typecheck
bun run check:docs
bun run check:examples
bun run check:imports
bun run check:jsdoc
bun run check
bun run test
bun run test:coverage
bun run test:performance
bun run test:sqlite
bun run pack:check
bun run verify
```

Migration tooling:

```bash
bun run db:check
bun run db:migrate
bun run db:new -- short_name
```

The SQLite file persistence integration test runs natively:

```powershell
bun run test:sqlite
```

Never publish, push, deploy, tag, or release unless the user explicitly asks.

