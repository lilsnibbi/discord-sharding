import { describe, expect, test } from "bun:test";
import {
	attemptUpgrade,
	BRIDGE_GENERATION,
	BRIDGE_ID,
	createDeferred,
	createHub,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	sendHello,
} from "./client-harness";

describe("HubClient authentication and management", () => {
	test("rejects unauthenticated upgrades before allocating a session", async () => {
		const installed = installFakeServe();
		const hub = createHub(new MemoryHubPersistence());
		try {
			await hub.start();
			const result = await attemptUpgrade(installed.server, {
				authorization: "Bearer wrong-token-01",
				bridgeGeneration: BRIDGE_GENERATION,
				bridgeId: BRIDGE_ID,
				connectionGeneration: 1,
			});
			expect(result.response?.status).toBe(401);
			expect(result.socket).toBeUndefined();
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("serves authenticated topology, reconciliation, and analytics cleanup", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence();
		let cleanupCall = 0;
		persistence.clearAnalyticsOperation = () => Promise.resolve(cleanupCall++ === 0 ? 2 : 0);
		const hub = createHub(persistence);
		try {
			await hub.start();
			const unauthorized = await installed.server.fetch(new Request("http://hub.test/topology"));
			expect(unauthorized?.status).toBe(401);
			const headers = { authorization: "Bearer admin-token-0001" };
			const topology = await installed.server.fetch(new Request("http://hub.test/topology", { headers }));
			expect(topology?.status).toBe(200);
			expect(await topology?.json()).toEqual({
				assignments: [],
				bridges: [],
				generatedAt: 1_001,
				totalShards: 1,
				unassignedShardIds: [0],
			});
			const reconciliation = await installed.server.fetch(
				new Request("http://hub.test/reconcile", { headers, method: "POST" }),
			);
			expect(reconciliation?.status).toBe(200);
			const analytics = await installed.server.fetch(
				new Request("http://hub.test/analytics?before=25&batchSize=2", {
					headers,
					method: "DELETE",
				}),
			);
			expect(analytics?.status).toBe(200);
			expect(await analytics?.json()).toEqual({ removed: 2 });
			expect(persistence.clearAnalyticsCalls).toEqual([
				{ batchSize: 2, before: 25 },
				{ batchSize: 2, before: 25 },
			]);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("retains a fencing tombstone when releasing a disconnected Bridge", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 7, shardId: 0, updatedAt: 1 }],
			bridges: [
				{
					connected: false,
					generation: BRIDGE_GENERATION,
					id: BRIDGE_ID,
					maxShards: 1,
					updatedAt: 1,
				},
			],
		});
		const hub = createHub(persistence);
		try {
			await hub.start();
			expect(await hub.releaseBridge(BRIDGE_ID)).toEqual([0]);
			expect(persistence.assignments.get(0)).toMatchObject({
				bridgeId: "__released__:0",
				epoch: 7,
				shardId: 0,
			});
			expect(hub.getTopology().unassignedShardIds).toEqual([0]);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("advances persisted timestamps beyond loaded state when the clock stalls", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 7, shardId: 0, updatedAt: 80 }],
			bridges: [
				{
					connected: true,
					generation: BRIDGE_GENERATION,
					id: BRIDGE_ID,
					maxShards: 1,
					updatedAt: 50,
				},
			],
		});
		const hub = createHub(persistence, { wallClock: () => 10 });
		try {
			await hub.start();
			const disconnectedAt = persistence.bridges.get(BRIDGE_ID)?.updatedAt;
			expect(disconnectedAt).toBe(81);
			await hub.releaseBridge(BRIDGE_ID);
			expect(persistence.assignments.get(0)?.updatedAt).toBe(82);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("serializes reconciliation and explicit release assignment writes", async () => {
		const installed = installFakeServe();
		const assignmentSaveStarted = createDeferred<void>();
		const releaseAssignmentSave = createDeferred<void>();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: "bridge-old", epoch: 4, shardId: 0, updatedAt: 1 }],
			bridges: [
				{
					connected: false,
					generation: "generation-old",
					id: "bridge-old",
					maxShards: 1,
					updatedAt: 1,
				},
			],
		});
		persistence.saveAssignmentOperation = async (assignment) => {
			if (assignment.shardId !== 1 || assignment.bridgeId !== BRIDGE_ID) return;
			assignmentSaveStarted.resolve();
			await releaseAssignmentSave.promise;
		};
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server, 1, true);
			sendHello(bridge);
			await assignmentSaveStarted.promise;

			const releasing = hub.releaseBridge("bridge-old");
			await Bun.sleep(10);
			expect(persistence.events).not.toContain("assignment:0:__released__:0:start");

			releaseAssignmentSave.resolve();
			await releasing;
			const assignmentEnd = persistence.events.indexOf(`assignment:1:${BRIDGE_ID}:end`);
			const releaseStart = persistence.events.indexOf("assignment:0:__released__:0:start");
			expect(assignmentEnd).toBeGreaterThanOrEqual(0);
			expect(releaseStart).toBeGreaterThan(assignmentEnd);
		} finally {
			releaseAssignmentSave.resolve();
			await hub.stop();
			installed.restore();
		}
	});

	test("keeps a partially persisted Bridge release visible and fenced", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: "bridge-old", epoch: 4, shardId: 0, updatedAt: 1 },
				{ bridgeId: "bridge-old", epoch: 6, shardId: 1, updatedAt: 2 },
			],
			bridges: [
				{
					connected: false,
					generation: "generation-old",
					id: "bridge-old",
					maxShards: 2,
					updatedAt: 1,
				},
			],
		});
		persistence.saveAssignmentOperation = (assignment) =>
			assignment.shardId === 1 ? Promise.reject(new Error("Second tombstone failed.")) : Promise.resolve();
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			await expect(hub.releaseBridge("bridge-old")).rejects.toThrow("Second tombstone failed.");
			expect(persistence.assignments.get(0)?.bridgeId).toBe("__released__:0");
			expect(persistence.assignments.get(1)?.bridgeId).toBe("bridge-old");
			expect(hub.getTopology().assignments).toEqual([{ bridgeId: "bridge-old", epoch: 6, shardId: 1 }]);
			expect(hub.getTopology().unassignedShardIds).toEqual([0]);

			const bridge = await openBridge(installed.server, 1, false, "generation-new", "bridge-new");
			sendHello(bridge, { bridgeGeneration: "generation-new", bridgeId: "bridge-new" });
			const synchronization = await bridge.waitForMessage("hub.sync");
			expect(synchronization.data.topologyVersion).toBe(2);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("does not accept a reconnect in the middle of an explicit release", async () => {
		const installed = installFakeServe();
		const releaseSaveStarted = createDeferred<void>();
		const releaseSave = createDeferred<void>();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 3, shardId: 0, updatedAt: 1 }],
			bridges: [
				{
					connected: false,
					generation: BRIDGE_GENERATION,
					id: BRIDGE_ID,
					maxShards: 1,
					updatedAt: 1,
				},
			],
		});
		persistence.saveAssignmentOperation = async () => {
			releaseSaveStarted.resolve();
			await releaseSave.promise;
		};
		const hub = createHub(persistence);
		try {
			await hub.start();
			const releasing = hub.releaseBridge(BRIDGE_ID);
			await releaseSaveStarted.promise;
			const reconnect = await openBridge(installed.server);
			sendHello(reconnect);
			await Bun.sleep(10);
			expect(reconnect.socket.sent).toHaveLength(0);

			releaseSave.resolve();
			await releasing;
			const synchronization = await reconnect.waitForMessage("hub.sync");
			expect(synchronization.data.assignments).toEqual([]);
		} finally {
			releaseSave.resolve();
			await hub.stop();
			installed.restore();
		}
	});
});
