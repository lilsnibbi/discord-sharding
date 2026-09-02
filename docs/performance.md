# Performance

Performance tests are regression guards, not production throughput claims.

Run deterministic checks with:

```bash
bun run test:performance
```

The suite should cover work whose complexity or retention can regress without requiring Discord or external services, such
as assignment planning, bounded queues, request cleanup, payload validation, and routing bookkeeping.

## Production measurement

Measure with the intended:

- Bun and operating-system versions;
- Hub and Bridge host sizes;
- shard and Bridge counts and declared capacity;
- request, message, analytics, and evaluation payload distributions;
- Redis round-trip latency, connection settings, and persistence mode;
- Discord client cache settings and handler behaviour;
- network latency and reconnect failure injection.

Record warm-up, sample count, percentiles, memory growth, CPU, event-loop delay, buffered bytes, and database effects.
Separate application handler time from package routing time.

Do not infer live Discord identify behaviour, safe capacity, tail latency, or database throughput from fake-backed
tests. Treat any optimization that adds queues, caches, batching, or retained state as a lifecycle change and add
cleanup plus hard-bound coverage.
