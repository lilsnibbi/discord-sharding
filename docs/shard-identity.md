# Shard Identity

Every `ShardClient` exposes a **frozen, readonly** self-description at `shard.identity` (type
`$ShardIdentity`). Mutating it or any nested value throws in strict mode — the object and its arrays are
`Object.freeze`-deep.

## Shape

```ts
import type { $ShardIdentity } from "@lilsnibbi/discord-sharding";

declare const identity: $ShardIdentity;
identity.instanceId; // unique per process incarnation, e.g. "bridge-a:3:2:5"
identity.shardId; // this shard's zero-based Discord shard id
identity.totalShards; // global shard count
identity.assignmentEpoch; // Hub ownership version
identity.processGeneration; // local process version
identity.bridgeId; // owning Bridge id, or null before it is known
identity.bridgeShardCount; // shards assigned to this shard's Bridge
identity.totalBridges; // Bridges known to the Hub
identity.bridges; // readonly [{ bridgeId, shardCount }] for every Bridge
```

## Where the data comes from

| Field | Source | Available |
| --- | --- | --- |
| `shardId`, `totalShards`, `assignmentEpoch`, `processGeneration` | options or `SHARDING_*` env vars set at spawn | immediately |
| `bridgeId` | `SHARDING_BRIDGE_ID` env var (spawn) or first topology report | immediately when Bridge-spawned |
| `bridgeShardCount`, `totalBridges`, `bridges` | `shard.control.topology` message | after first Hub synchronization |

The Hub includes a cluster summary (every Bridge and its assigned shard count) in each `hub.sync`. The
Bridge forwards it to every retained shard as `shard.control.topology` right before each maintenance-clear
handshake, and to newly booted shards after `shard.booted`.

## Snapshot semantics

`identity` is a **snapshot getter**: each topology change replaces the frozen object. Hold no long-lived
reference across maintenance windows — read the getter again instead:

```ts
import { ShardClient } from "@lilsnibbi/discord-sharding";

declare const shard: ShardClient<never>;
shard.bridge.onMaintenanceChange((maintenance) => {
	if (!maintenance) console.log("topology now", shard.identity.bridges);
});
```

Before the first synchronization the cluster fields are empty (`bridges: []`, `totalBridges: 0`,
`bridgeShardCount: 0`) and `bridgeId` falls back to the spawn-time environment value (`null` for
standalone processes and custom transports that do not set it).

## Version guarding

Topology messages carry the Hub `topologyVersion`; the shard ignores reports older than the newest it has
applied, so out-of-order delivery cannot roll the identity back.
