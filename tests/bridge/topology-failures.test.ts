import { describe, expect, test } from "bun:test";
import { BridgeClient } from "../../src/bridge/BridgeClient";
import type { WireDataMap } from "../../src/protocol/types";
import {
	type $HubHarness,
	type $ShardHarness,
	createBridgeOptions,
	createHubHarness,
	createShardHarness,
	waitFor,
	waitForHubMessage,
	waitForShardMessage,
} from "./client-harness";

interface Deployment {
	readonly bridge: BridgeClient;
	readonly hub: $HubHarness;
	readonly shards: $ShardHarness;
}

function createDeployment(): Deployment {
	const hub = createHubHarness();
	const shards = createShardHarness();
	const bridge = new BridgeClient(
		createBridgeOptions(hub.url, {
			processFactory: shards.factory,
			sleep: () => Promise.resolve(),
			socketFactory: hub.factory,
		}),
	);
	return { bridge, hub, shards };
}

function synchronization(
	bridge: BridgeClient,
	overrides: Partial<WireDataMap["hub.sync"]> = {},
): WireDataMap["hub.sync"] {
	return {
		assignments: [{ epoch: 3, shardId: 0 }],
		bridgeGeneration: bridge.generation,
		cluster: [],
		connectionGeneration: 1,
		topologyVersion: 1,
		totalShards: 2,
		...overrides,
	};
}

describe("BridgeClient topology rejections", () => {
	test.each([
		["a stale Bridge generation", { bridgeGeneration: "generation-other" }],
		["a stale connection generation", { connectionGeneration: 2 }],
		["an assignment outside the global topology", { assignments: [{ epoch: 3, shardId: 5 }] }],
		[
			"duplicate shard assignments",
			{
				assignments: [
					{ epoch: 3, shardId: 0 },
					{ epoch: 4, shardId: 0 },
				],
			},
		],
	])("closes the Hub socket for %s", async (_description, overrides) => {
		const { bridge, hub } = createDeployment();
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:1", synchronization(bridge, overrides));

			await waitFor(() => hub.closeCode() !== null, "topology rejection close");
			expect(hub.closeCode()).toBe(1002);
			expect(bridge.connected).toBeFalse();
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("rejects a topology version that moves backwards", async () => {
		const { bridge, hub } = createDeployment();
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:2", synchronization(bridge, { topologyVersion: 2 }));
			await bridge.waitUntilConnected(1_000);
			hub.send("hub.sync", "sync:1", synchronization(bridge, { topologyVersion: 1 }));

			await waitFor(() => hub.closeCode() !== null, "backwards topology close");
			expect(hub.closeCode()).toBe(1002);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("rejects changed topology content that reuses its version", async () => {
		const { bridge, hub } = createDeployment();
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:1", synchronization(bridge));
			await bridge.waitUntilConnected(1_000);
			hub.send("hub.sync", "sync:1-again", synchronization(bridge, { assignments: [{ epoch: 4, shardId: 1 }] }));

			await waitFor(() => hub.closeCode() !== null, "silent topology change close");
			expect(hub.closeCode()).toBe(1002);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("acknowledges a repeated synchronization without restarting shards", async () => {
		const { bridge, hub, shards } = createDeployment();
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:1", synchronization(bridge));
			await bridge.waitUntilConnected(1_000);
			hub.send("hub.shard.start", "start:1", { assignmentEpoch: 3, shardId: 0, totalShards: 2 });
			await waitFor(() => shards.contexts.length === 1, "shard process");

			const startIndex = hub.received.length;
			hub.send("hub.sync", "sync:repeat", synchronization(bridge));
			const acknowledgement = await waitForHubMessage(hub, "bridge.sync.ready", startIndex);

			expect(acknowledgement.id).toBe("sync:repeat");
			expect(shards.contexts).toHaveLength(1);
			expect(hub.closeCode()).toBeNull();
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test.each([
		["before topology synchronization", false, { assignmentEpoch: 3, shardId: 0, totalShards: 2 }],
		["with a mismatched global shard count", true, { assignmentEpoch: 3, shardId: 0, totalShards: 4 }],
		["with a mismatched assignment epoch", true, { assignmentEpoch: 9, shardId: 0, totalShards: 2 }],
	])("refuses a shard start %s", async (_description, synchronize, start) => {
		const { bridge, hub, shards } = createDeployment();
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			if (synchronize) {
				hub.send("hub.sync", "sync:1", synchronization(bridge));
				await bridge.waitUntilConnected(1_000);
			}
			hub.send("hub.shard.start", "start:invalid", start);

			await waitFor(() => hub.closeCode() !== null, "shard start rejection close");
			expect(shards.contexts).toHaveLength(0);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("stops an owned shard and still acknowledges an unknown stop command", async () => {
		const { bridge, hub, shards } = createDeployment();
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:1", synchronization(bridge));
			await bridge.waitUntilConnected(1_000);
			hub.send("hub.shard.start", "start:1", { assignmentEpoch: 3, shardId: 0, totalShards: 2 });
			await waitFor(() => shards.contexts.length === 1, "shard process");

			const stopIndex = hub.received.length;
			hub.send("hub.shard.stop", "stop:1", {
				assignmentEpoch: 3,
				processGeneration: 1,
				reason: "Operator release",
				shardId: 0,
			});
			const shutdown = await waitForShardMessage(shards, "shard.control.shutdown");
			const stopped = await waitForHubMessage(hub, "bridge.shard.stopped", stopIndex);

			expect(shutdown.data).toMatchObject({ commandId: "stop:1", reason: "Operator release" });
			expect(stopped.data).toMatchObject({ commandId: "stop:1", shardId: 0 });
			expect(bridge.shards.has(0)).toBeFalse();

			const unknownIndex = hub.received.length;
			hub.send("hub.shard.stop", "stop:unknown", {
				assignmentEpoch: 3,
				processGeneration: 1,
				reason: "Already gone",
				shardId: 1,
			});
			const acknowledged = await waitForHubMessage(hub, "bridge.shard.stopped", unknownIndex);

			expect(acknowledged.id).toBe("stop:unknown");
			expect(hub.closeCode()).toBeNull();
		} finally {
			await bridge.stop();
			hub.close();
		}
	});
});
