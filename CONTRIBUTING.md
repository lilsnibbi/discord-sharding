# Contributing

Thanks for helping improve Sharding. This repository is Bun-native and ships raw TypeScript, so the workflow below is
short but strict: every rule here is enforced by a script that runs in CI.

## Requirements

- [Bun](https://bun.com) 1.3.14 or newer. Node.js, npm, npx, and `bun link` are never used in this repository.
- Git, and a POSIX shell or PowerShell.

```bash
git clone https://github.com/lilsnibbi/discord-sharding.git
cd discord-sharding
bun install
```

## The one command that matters

```bash
bun run verify
```

`verify` runs exactly what CI runs, in the same order: type-check, documentation links, documentation examples, import
policy, public JSDoc, migrations, formatting and lint, tests, coverage, performance budgets, release metadata, and the
packed-archive consumer check. Run it before opening a pull request.

Useful subsets while iterating:

| Command | Purpose |
| --- | --- |
| `bun run typecheck` | Strict TypeScript diagnostics |
| `bun test tests/hub/client-routing.test.ts` | A single test file |
| `bun test --test-name-pattern "rebalance"` | A single test by name |
| `bun run check:fix` | Apply Biome formatting and safe lint fixes |
| `bun run check` | Line limits, formatting, and lint without fixes |

The full command list lives in the [README](README.md) and the [documentation hub](docs/README.md).

## Rules the scripts enforce

- **No file exceeds 500 lines.** Split by lifecycle or domain responsibility into composed controllers, stores, or
  helpers — never into arbitrary numbered chunks.
- **No Node built-ins.** Bun APIs and web standards only. The `process` surface is allowed solely for Bun IPC and
  signal handling.
- **Import allowlist by directory.** `src/**` may import only `bun`, `bun:*`, and `arktype`; `tests/**` adds
  `discord.js`; `examples/**` adds `discord.js` and `@lilsnibbi/discord-sharding`. `arktype` is pinned exactly and is
  the only production dependency. Relative imports and re-exports are extensionless.
- **No `any`, no double casts through `unknown`, and no `@ts-ignore`, `@ts-nocheck`, or `@ts-expect-error`.** Fix the
  diagnostic instead of suppressing it.
- **Public API surface.** Exported types and interfaces start with `$` and live in `src/types`. Every public class,
  member, function, option, and exported type needs meaningful JSDoc with a `@param` for each parameter.
- **Errors.** Throw the domain classes in `src/errors/ShardingError.ts` and preserve the underlying failure through
  `cause`.
- Every `ts` fence in a root or `docs/` markdown file is type-checked, and every local markdown link is resolved.

## Tests

Tests use `bun:test` and live under `tests/`. Use the deterministic fakes for process, clock, Discord, WebSocket,
SQLite, and persistence — a normal test must never require live Discord or any external service.

Cover both the success path and the failed-cleanup path for lifecycle and protocol changes. Assignment transfers,
identify buckets, stale generations, backpressure, request expiry, and broadcast prepare/commit deserve extra
scrutiny. Package-wide coverage must stay at or above 80%.

## Database migrations

Migrations are directory-format SQL under `migrations/` and are authoritative over any code.

```bash
bun run db:new -- short_name   # scaffold migrations/<timestamp>_<slug>/migration.sql
bun run db:check               # apply every migration to an in-memory database and assert tables
```

Never edit a migration that has already been released. Add a new one.

## Pull requests

- Use [conventional commits](https://www.conventionalcommits.org), for example `fix(hub): reject stale assignment
  epochs`.
- Keep one logical change per pull request, and update documentation in the same change as the behaviour.
- Fill in the pull request template, including the verification checklist.
- Do not bump the version or publish. Releases are automated (see below).

## Releases

Maintainers only. The [release workflow](.github/workflows/release.yml) triggers on a push to `main` whose commit
subject is exactly a version tag, for example `v0.1.5`:

1. Bump `version` in `package.json` and commit it with the subject `v0.1.5`.
2. Push to `main`.
3. The workflow runs `bun run verify`, packs a verified archive with a SHA-256 checksum, creates the Git tag and the
   GitHub release, and publishes to npm with `bun publish` when the `NPM_TOKEN` secret is set.

Release metadata is validated by `bun run release:verify`, which requires the tag to match `package.json` exactly.

## Reporting security issues

Do not open a public issue. Follow the [security policy](SECURITY.md).
