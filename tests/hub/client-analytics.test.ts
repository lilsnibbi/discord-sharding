import { describe, expect, test } from "bun:test";
import {
	BRIDGE_GENERATION,
	BRIDGE_ID,
	createHub,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	persistedBridge,
	readErrorField,
	synchronizeBridge,
	waitFor,
} from "./client-harness";

const RUNNING_SHARD = { assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 } as const;

function singleShardPersistence(): MemoryHubPersistence {
	return new MemoryHubPersistence({
		assignments: [{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 }],
		bridges: [persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 1)],
	});
}

describe("HubClient analytics and identify admission", () => {
	test("persists shard analytics reported by a synchronized Bridge", async () => {
		const installed = installFakeServe();
		const persistence = singleShardPersistence();
		const hub = createHub(persistence, { totalShards: 1 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, { runningShards: [RUNNING_SHARD] });
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 1, "ready shard topology");

			bridge.send("bridge.analytics", "analytics:1", {
				assignmentEpoch: 1,
				collectedAt: 1_700_000_000_000,
				payload: { discord: { ready: true }, process: { heapSizeBytes: 1_024 } },
				processGeneration: 1,
				shardId: 0,
			});
			await waitFor(() => persistence.analytics.length === 1, "persisted analytics");

			const record = persistence.analytics[0];
			if (record === undefined) throw new Error("Analytics record is missing.");
			expect(record).toMatchObject({
				bridgeId: BRIDGE_ID,
				collectedAt: 1_700_000_000_000,
				id: "analytics:1",
				shardId: 0,
			});
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("closes a Bridge that reports analytics for a stale shard identity", async () => {
		const installed = installFakeServe();
		const hub = createHub(singleShardPersistence(), { totalShards: 1 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, { runningShards: [RUNNING_SHARD] });
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 1, "ready shard topology");

			bridge.send("bridge.analytics", "analytics:stale", {
				assignmentEpoch: 2,
				collectedAt: 1_700_000_000_000,
				payload: { ready: true },
				processGeneration: 1,
				shardId: 0,
			});

			await waitFor(() => bridge.socket.closeCode !== undefined, "stale analytics close");
			expect(bridge.socket.closeCode).toBe(1002);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("grants one identify at a time for the same shard process", async () => {
		const installed = installFakeServe();
		const hub = createHub(singleShardPersistence(), { totalShards: 1 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: false, shardId: 0 }],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.connected === true, "Bridge synchronization");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.identify.request", "identify:1", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
			});
			bridge.send("bridge.identify.request", "identify:2", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
			});

			const responses = await bridge.waitForMessages("hub.identify.response", 2, startIndex);
			const rejected = responses.find((response) => response.data.granted === false);
			const granted = responses.find((response) => response.data.granted === true);

			expect(granted?.data).toMatchObject({ granted: true, shardId: 0 });
			if (rejected === undefined) throw new Error("A duplicate identify was not rejected.");
			expect(readErrorField(rejected, "code")).toBe("CAPACITY");
			expect(readErrorField(rejected, "message")).toContain("already pending");
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("persists shard state transitions reported after synchronization", async () => {
		const installed = installFakeServe();
		const persistence = singleShardPersistence();
		const hub = createHub(persistence, { totalShards: 1 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: false, shardId: 0 }],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.connected === true, "Bridge synchronization");

			bridge.send("bridge.shard.state", "state:ready", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
				state: "ready",
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 1, "ready shard state");

			bridge.send("bridge.shard.state", "state:duplicate", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
				state: "ready",
			});
			await Bun.sleep(5);

			expect(persistence.events.filter((event) => event === "shard:ready")).toHaveLength(1);
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});
});
