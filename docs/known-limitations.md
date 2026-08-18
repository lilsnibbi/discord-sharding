# Known Limitations — Cross-Machine Hub ↔ Bridge

The Hub ↔ Bridge link is a WebSocket and carries no same-machine assumptions in its data path: every
message is versioned JSON with explicit size/depth/node limits, generation checks reject stale peers, and
both ends enforce liveness deadlines. It has, however, only been **executed** on one machine. This page
records the review findings for multi-host deployments (task: cross-machine review — documented, not
live-verified).

## Reviewed and sound for cross-machine use

| Area | Finding |
| --- | --- |
| Serialization | Pure JSON wire format, validated as untrusted on both ends; no shared memory anywhere. |
| Liveness | Bridge heartbeats every 10 s (also keeps NAT mappings alive); Hub closes sockets idle 45 s; Bridge replaces a Hub connection silent for 45 s. |
| Reconnect | Exponential backoff with jitter (500 ms → 30 s cap) prevents thundering herd; attempt counter resets only after a full synchronization. |
| Timeouts | Default `request.timeoutMs` is 15 000 ms — generous for WAN round-trips (sync acks, identify grants, routed requests). |
| Auth | Bearer token compared in constant time; bridge/connection generations reject replayed or stale sockets. |
| Binding | Hub binds `0.0.0.0:3000` by default, so remote Bridges can reach it without config changes. |

## Flagged risks (work today only because everything shares one machine or one clock)

### 1. `broadcastEval` commit scheduling assumes synchronized wall clocks — highest impact

The Hub computes `executeAt = wallClock() + evaluationCommitLeadMs` (default lead: **25 ms**) and every
destination shard sleeps until `executeAt` **on its own host clock** (`src/hub/client/HubRoutingController.ts`,
`src/shard/ShardInbound.ts`). On one machine the clocks are identical; across machines:

- Host clock skew larger than ~25 ms silently destroys the "all shards execute together" property.
- A Hub clock ahead of a shard host by minutes delays execution by that skew; the caller's 15 s timeout
  expires first and every broadcastEval fails.
- A Hub clock behind the shard host makes evaluations run immediately (harmless but unsynchronized).

**Mitigation today:** run NTP/chrony on every host and raise `evaluationCommitLeadMs` above your worst
expected skew. **Better fix:** make the commit delay relative (`executeInMs`) instead of an absolute
wall-clock time.

### 2. No native TLS on the Hub server

`hubUrl` accepts `https://` (dialed as `wss://`), but the Hub itself serves plain HTTP/WS — there is no
TLS option in `$HubClientOptions`. Cross-machine, the bridge token and admin token cross the network in
cleartext unless you terminate TLS in front of the Hub (reverse proxy / load balancer). Same applies to
the admin HTTP API.

### 3. Proxies and load balancers can break the upgrade

The Bridge authenticates during the WebSocket upgrade with custom headers (`Authorization`,
`X-Sharding-Bridge-Id`, `X-Sharding-Bridge-Generation`, `X-Sharding-Connection-Generation`). Any
intermediary that strips custom headers or does not forward upgrades verbatim will make handshakes fail
with `1002`/HTTP 401 responses.

### 4. Backpressure limit tuned for loopback

Outbound sends close the socket (`1013`) when `bufferedAmount` would exceed `maxBufferedBytes`
(default 4 MiB). A slow WAN link buffers far more than loopback ever does, so heavy analytics or bulk
routing across a thin pipe can cycle connect → buffer-full → close → reconnect. Raise
`maxBufferedBytes` and/or lower analytics frequency for constrained links.

### 5. Restart clock is per-Hub, wall-clock persistence is per-Hub

Restart budgets and persistence timestamps use the Hub's own clocks only — safe cross-machine (single
writer), listed here for completeness because moving the Hub between machines mid-life discards the
monotonic restart clock (by design: it is in-memory only).

### 6. Operational assumptions

- Bridge `id` values must be unique **across machines**; a duplicate id is rejected as a duplicate
  connection, which looks like a "cannot connect" failure on the second host.
- The Bridge copies its whole environment into every shard subprocess (`buildEnvironment`), so a Bridge
  host's secrets are visible to shard processes on that host. Fine same-host; keep per-host environments
  minimal.
- `bridge.hello` can carry up to `maxShards` retained shard descriptors; with very large per-Bridge shard
  counts, confirm the payload stays under `payload.maxBytes` (default 16 MiB — thousands of shards fit
  comfortably).

## Not tested

No second machine was available. Everything above is code review; the same-machine behavior (including a
Hub outage with surviving remote shard processes) **is** covered by automated tests
(`tests/integration/shard-process.test.ts`, `tests/integration/runtime-stack.test.ts`).
