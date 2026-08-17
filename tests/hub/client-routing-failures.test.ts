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

const SECOND_BRIDGE_ID = "bridge-b";
const SECOND_BRIDGE_GENERATION = "generation-b";

describe("HubClient routing failures", () => {
	test("refuses routes to unassigned and not-ready shards", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 1 },
			],
			bridges: [persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 2)],
		});
		const hub = createHub(persistence, { totalShards: 3 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				maxShards: 2,
				runningShards: [
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: false, shardId: 1 },
				],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 1, "one ready shard");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.route.request", "route:starting", {
				assignmentEpoch: 1,
				kind: "request",
				payload: { ping: true },
				processGeneration: 1,
				shardId: 0,
				targetShardId: 1,
			});
			const startingFailure = await bridge.waitForMessage("hub.route.response", startIndex);
			expect(startingFailure.data.ok).toBe(false);
			expect(readErrorField(startingFailure, "message")).toContain("Shard 1 is not Discord-ready.");

			const unassignedIndex = bridge.socket.sent.length;
			bridge.send("bridge.route.request", "route:unassigned", {
				assignmentEpoch: 1,
				kind: "request",
				payload: { ping: true },
				processGeneration: 1,
				shardId: 0,
				targetShardId: 2,
			});
			const unassignedFailure = await bridge.waitForMessage("hub.route.response", unassignedIndex);
			expect(unassignedFailure.data.ok).toBe(false);
			expect(readErrorField(unassignedFailure, "code")).toBe("STATE");
			expect(readErrorField(unassignedFailure, "message")).toContain("Shard 2 is not assigned.");
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("expires a route whose destination never answers", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 1 },
			],
			bridges: [persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 2)],
		});
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				maxShards: 2,
				runningShards: [
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 },
				],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.route.request", "route:expired", {
				assignmentEpoch: 1,
				kind: "message",
				payload: { ping: true },
				processGeneration: 1,
				shardId: 0,
				targetShardId: 1,
			});
			await bridge.waitForMessage("hub.route.request", startIndex);
			const expired = await bridge.waitForMessage("hub.route.response", startIndex);

			expect(expired.data.ok).toBe(false);
			expect(readErrorField(expired, "code")).toBe("TIMEOUT");
			expect(readErrorField(expired, "message")).toContain("route:expired");
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("closes a Bridge whose route response identity does not match its request", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 1 },
			],
			bridges: [persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 2)],
		});
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				maxShards: 2,
				runningShards: [
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 },
				],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.route.request", "route:mismatch", {
				assignmentEpoch: 1,
				kind: "request",
				payload: { ping: true },
				processGeneration: 1,
				shardId: 0,
				targetShardId: 1,
			});
			await bridge.waitForMessage("hub.route.request", startIndex);
			bridge.send("bridge.route.response", "route:mismatch", {
				assignmentEpoch: 1,
				ok: true,
				processGeneration: 1,
				shardId: 1,
				sourceShardId: 1,
				value: { pong: true },
			});

			await waitFor(() => bridge.socket.closeCode !== undefined, "route identity protocol close");
			expect(bridge.socket.closeCode).toBe(1002);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("fails pending routes when the destination Bridge disconnects", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: SECOND_BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 1 },
			],
			bridges: [
				persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 1),
				persistedBridge(SECOND_BRIDGE_ID, SECOND_BRIDGE_GENERATION, 1),
			],
		});
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			const source = await openBridge(installed.server);
			await synchronizeBridge(source, {
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 }],
			});
			const destination = await openBridge(installed.server, 1, false, SECOND_BRIDGE_GENERATION, SECOND_BRIDGE_ID);
			await synchronizeBridge(destination, {
				bridgeGeneration: SECOND_BRIDGE_GENERATION,
				bridgeId: SECOND_BRIDGE_ID,
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 }],
			});
			await waitFor(
				() => hub.getTopology().bridges.filter((bridge) => bridge.readyShardIds.length === 1).length === 2,
				"both Bridges ready",
			);

			const startIndex = source.socket.sent.length;
			source.send("bridge.route.request", "route:disconnect", {
				assignmentEpoch: 1,
				kind: "request",
				payload: { ping: true },
				processGeneration: 1,
				shardId: 0,
				targetShardId: 1,
			});
			await destination.waitForMessage("hub.route.request");
			destination.socket.close(1006, "Destination lost");

			const failure = await source.waitForMessage("hub.route.response", startIndex);
			expect(failure.data.ok).toBe(false);
			expect(readErrorField(failure, "code")).toBe("TRANSPORT");
			expect(readErrorField(failure, "message")).toContain(`Destination Bridge ${SECOND_BRIDGE_ID} disconnected.`);
			expect(source.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});
});
