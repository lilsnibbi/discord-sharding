import { describe, expect, test } from "bun:test";
import {
	BRIDGE_GENERATION,
	BRIDGE_ID,
	createHub,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	persistedBridge,
	requireNumberProperty,
	sendHello,
	sentMessages,
	waitFor,
} from "./client-harness";

describe("HubClient assignment rebalancing", () => {
	test("waits for a matching stop acknowledgement before transferring ownership", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 2 },
			],
			bridges: [
				{
					connected: false,
					generation: BRIDGE_GENERATION,
					id: BRIDGE_ID,
					maxShards: 2,
					updatedAt: 1,
				},
			],
		});
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			const source = await openBridge(installed.server, 1, true);
			sendHello(source, {
				maxShards: 2,
				runningShards: [
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 },
				],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "source Bridge readiness");

			const target = await openBridge(installed.server, 1, true, "generation-b", "bridge-b");
			const targetStartIndex = target.socket.sent.length;
			sendHello(target, {
				bridgeGeneration: "generation-b",
				bridgeId: "bridge-b",
				maxShards: 2,
			});
			const stop = await source.waitForMessage("hub.shard.stop");
			await Bun.sleep(10);
			expect(
				sentMessages(target.socket, targetStartIndex).filter((message) => message.type === "hub.shard.start"),
			).toHaveLength(0);
			expect(persistence.assignments.get(1)?.bridgeId).toBe(BRIDGE_ID);

			source.send("bridge.shard.stopped", stop.id, {
				assignmentEpoch: requireNumberProperty(stop.data, "assignmentEpoch"),
				commandId: stop.id,
				processGeneration: requireNumberProperty(stop.data, "processGeneration"),
				shardId: requireNumberProperty(stop.data, "shardId"),
			});
			const start = await target.waitForMessage("hub.shard.start", targetStartIndex);
			expect(start.data).toMatchObject({ assignmentEpoch: 2, shardId: 1 });
			expect(persistence.assignments.get(1)).toMatchObject({
				bridgeId: "bridge-b",
				epoch: 2,
			});
			const stopped = persistence.events.indexOf("shard:stopped");
			const assignment = persistence.events.indexOf("assignment:1:bridge-b:start");
			expect(stopped).toBeGreaterThanOrEqual(0);
			expect(assignment).toBeGreaterThan(stopped);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("does not begin a second transfer before the first target reports startup", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 2 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 2, updatedAt: 3 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 3, updatedAt: 4 },
			],
			bridges: [persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 4)],
		});
		const hub = createHub(persistence, { totalShards: 4 });
		try {
			await hub.start();
			const source = await openBridge(installed.server, 1, true);
			sendHello(source, {
				maxShards: 4,
				runningShards: [
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 2 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 3 },
				],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 4, "source Bridge readiness");

			const target = await openBridge(installed.server, 1, true, "generation-b", "bridge-b");
			sendHello(target, {
				bridgeGeneration: "generation-b",
				bridgeId: "bridge-b",
				maxShards: 4,
			});
			const firstStop = await source.waitForMessage("hub.shard.stop");
			expect(firstStop.data.shardId).toBe(3);
			source.send("bridge.shard.stopped", firstStop.id, {
				assignmentEpoch: requireNumberProperty(firstStop.data, "assignmentEpoch"),
				commandId: firstStop.id,
				processGeneration: requireNumberProperty(firstStop.data, "processGeneration"),
				shardId: requireNumberProperty(firstStop.data, "shardId"),
			});
			const firstStart = await target.waitForMessage("hub.shard.start");
			expect(firstStart.data).toMatchObject({ assignmentEpoch: 2, shardId: 3 });
			await Bun.sleep(20);
			expect(sentMessages(source.socket).filter((message) => message.type === "hub.shard.stop")).toHaveLength(1);

			target.send("bridge.shard.state", "state:target-starting", {
				assignmentEpoch: 2,
				processGeneration: 1,
				shardId: 3,
				state: "starting",
			});
			const stops = await source.waitForMessages("hub.shard.stop", 2);
			expect(stops[1]?.data.shardId).toBe(2);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("restores source ownership when the transfer target disconnects", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 2 },
			],
			bridges: [
				{
					connected: false,
					generation: BRIDGE_GENERATION,
					id: BRIDGE_ID,
					maxShards: 2,
					updatedAt: 1,
				},
			],
		});
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			const source = await openBridge(installed.server, 1, true);
			sendHello(source, {
				maxShards: 2,
				runningShards: [
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 },
				],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "source Bridge readiness");

			const target = await openBridge(installed.server, 1, true, "generation-b", "bridge-b");
			sendHello(target, {
				bridgeGeneration: "generation-b",
				bridgeId: "bridge-b",
				maxShards: 2,
			});
			const stop = await source.waitForMessage("hub.shard.stop");
			const recoveryStartIndex = source.socket.sent.length;
			target.socket.close(1000, "Target unavailable");
			source.send("bridge.shard.stopped", stop.id, {
				assignmentEpoch: requireNumberProperty(stop.data, "assignmentEpoch"),
				commandId: stop.id,
				processGeneration: requireNumberProperty(stop.data, "processGeneration"),
				shardId: requireNumberProperty(stop.data, "shardId"),
			});

			const recovery = await source.waitForMessage("hub.shard.start", recoveryStartIndex);
			expect(recovery.data).toMatchObject({ assignmentEpoch: 1, shardId: 1 });
			expect(persistence.assignments.get(1)).toMatchObject({
				bridgeId: BRIDGE_ID,
				epoch: 1,
			});
			expect(hub.getTopology().assignments).toContainEqual({ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1 });
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("does not transfer ownership while the source startup outcome is unknown", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 2 },
			],
			bridges: [persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 2)],
		});
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			const source = await openBridge(installed.server, 1, true);
			sendHello(source, {
				maxShards: 2,
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 }],
			});
			await source.waitForMessage("hub.shard.start");

			const target = await openBridge(installed.server, 1, true, "generation-b", "bridge-b");
			sendHello(target, {
				bridgeGeneration: "generation-b",
				bridgeId: "bridge-b",
				maxShards: 2,
			});
			await Bun.sleep(20);
			expect(sentMessages(source.socket).filter((message) => message.type === "hub.shard.stop")).toHaveLength(0);
			expect(sentMessages(target.socket).filter((message) => message.type === "hub.shard.start")).toHaveLength(0);
			expect(persistence.assignments.get(1)?.bridgeId).toBe(BRIDGE_ID);

			await waitFor(() => source.socket.closeCode !== undefined, "unreported source startup close");
			expect(source.socket.closeCode).toBe(1011);
			expect(persistence.assignments.get(1)).toMatchObject({
				bridgeId: BRIDGE_ID,
				epoch: 1,
			});
		} finally {
			await hub.stop();
			installed.restore();
		}
	});
});
