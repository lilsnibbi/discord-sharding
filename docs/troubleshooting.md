# Troubleshooting

Start at the first failing boundary: configuration, SQLite, Hub server, Bridge WebSocket, Bun subprocess, IPC,
Discord identify admission, routing, or application handling.

Capture the Bun and package versions, lifecycle state, Bridge and shard identifiers, assignment epoch, process
generation, error code, `cause`, and timestamp. Never include tokens, database paths, complete payloads,
or guild data.

## Hub does not start

1. Confirm `bridgeToken` and `adminToken` are distinct non-empty values, and that `botToken` is non-empty.
2. Confirm `databasePath` or a custom persistence implementation is supplied, but not an invalid combination.
3. Check SQLite file path, permissions, directory existence, disk space, and migration logs.
4. Confirm Discord's Gateway Bot endpoint is reachable and returned a valid shard recommendation and session limit.
   Transport failures are retried with backoff, so a reported failure means Discord stayed unavailable. HTTP 401 and
   403 are raised immediately and mean the bot token was rejected.
5. Confirm the configured host and port are available.

Startup rolls back partial resources. Preserve the first error and its `cause`.

## Bridge remains in maintenance

1. Confirm the Hub URL points to the HTTP origin; the Bridge derives its WebSocket endpoint.
2. Confirm the Bridge token matches and the reverse proxy supports WebSocket upgrades.
3. Check TLS trust, DNS, firewall, proxy idle timeouts, and reconnect errors.
4. Confirm the Bridge `id`, generation, `maxShards`, and retained shard acknowledgements were accepted.
5. Use `waitUntilConnected()` with a bounded timeout only for diagnostics or readiness.

If the Hub says a new process generation cannot inherit assignments, do not retry around the fence. Stop and verify
the old deployment, release the disconnected Bridge through the authenticated Hub operation, then start the
replacement.

Do not kill healthy shard processes merely because maintenance is active.

## Shards remain unassigned

Compare `totalShards` with the sum of connected Bridge capacity. The Hub intentionally leaves excess shards waiting.
If capacity is sufficient, inspect sticky assignments, a pending one-at-a-time transfer, and disconnected Bridges
whose ownership has not been explicitly released.

Release a Bridge only after independently confirming its processes and Discord sessions are dead.

## Shard process exits during startup

1. Confirm `shardScript` is a readable raw TypeScript entrypoint and the Bridge working directory is correct.
2. Inspect application startup before `ShardClient.start()`.
3. Confirm the Bridge supplied `SHARDING_SHARD_ID`, `SHARDING_TOTAL_SHARDS`, `SHARDING_ASSIGNMENT_EPOCH`, and
   `SHARDING_PROCESS_GENERATION`.
4. Confirm the Discord client satisfies the structural client contract.
5. Inspect startup deadline, restart backoff, exit code, signal, and the earliest reported error.

A shard terminated for silence is reported through the Bridge error listener before it exits. That means the process
stayed alive without heartbeating, so look for a blocked event loop, synchronous work, or memory pressure rather than
a crash.

## Discord login waits or fails

`ShardClient.login()` waits for a Hub identify grant, including through a Bridge maintenance window. Check Discord's
remaining session starts and reset time, identify bucket occupancy, and other processes using the same bot token.

A login that fails rather than waits means maintenance outlasted `request.timeoutMs`, so investigate the Hub
connection rather than the shard.

Do not bypass the scheduler by calling the Discord client login method directly. An invalid Discord token or Gateway
rejection remains an application or Discord failure after admission.

## Targeted request times out

Check that both shards are Discord-ready, both Bridges are synchronized, and the destination installed one
`onRequest` handler. Inspect payload limits, request capacity, timeout, stale generation rejection, and handler errors.

A timeout removes correlation state; it cannot terminate destination application code. The handler signal aborts when
the destination `ShardClient` closes, not when one caller times out, so handlers must remain bounded independently.

## Broadcast evaluation fails

- Evaluators must be self-contained and must not close over local variables.
- Context and results must be JSON-compatible and within configured limits.
- Only the ready-shard snapshot participates.
- Every destination must prepare before commit.
- Hung application code can outlive correlation expiry.

Prefer ordinary targeted requests for complex or privileged application behaviour.

## Analytics or database growth

Analytics are retained by design. Export what is required, then clear with a cutoff and bounded batch size. Monitor
Hub and Bridge SQLite paths separately.

If migration validation or Hub startup reports a checksum mismatch or a missing applied migration, stop and restore
the exact released migration history. Never edit an applied migration.

## Package imports fail

- Run with the supported Bun version.
- Import only from `@lilsnibbi/discord-sharding` or its documented `package.json` export.
- Do not add file extensions to imports.
- Do not expect `dist`, compiled JavaScript, or generated declarations.
- Run `bun run pack:check` to reproduce the clean-consumer validation.

For a minimal sanitized reproduction, use [GitHub Issues](https://github.com/lilsnibbi/discord-sharding/issues) and state
which boundaries were tested with fakes, locally, or against live services.
