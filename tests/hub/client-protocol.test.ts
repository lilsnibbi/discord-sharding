import { describe, expect, test } from "bun:test";
import { DEFAULT_RESTART_POLICY } from "../../src/internal/policies";
import type { WireDataMap } from "../../src/protocol/types";
import {
	BRIDGE_GENERATION,
	BRIDGE_ID,
	createHub,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	persistedBridge,
	sendHello,
	synchronizeBridge,
	waitFor,
} from "./client-harness";

describe("HubClient Bridge protocol", () => {
	test("authenticates hello, synchronizes, rejects a duplicate, and accepts a newer session", async () => {
		const installed = installFakeServe();
		const hub = createHub(new MemoryHubPersistence());
		try {
			await hub.start();
			const first = await openBridge(installed.server);
			await synchronizeBridge(first);
			await waitFor(() => hub.getTopology().bridges[0]?.connected === true, "first Bridge synchronization");

			const duplicate = await openBridge(installed.server);
			sendHello(duplicate);
			await waitFor(() => duplicate.socket.closeCode !== undefined, "duplicate Bridge close");
			expect(duplicate.socket.closeCode).toBe(1002);

			first.socket.close(1000, "Reconnect");
			const replacement = await openBridge(installed.server, 2);
			replacement.send("bridge.hello", "hello:2", {
				bridgeGeneration: BRIDGE_GENERATION,
				bridgeId: BRIDGE_ID,
				connectionGeneration: 2,
				maxShards: 1,
				restartPolicy: DEFAULT_RESTART_POLICY,
				runningShards: [],
			});
			const sync = await replacement.waitForMessage("hub.sync");
			const topologyVersion = sync.data.topologyVersion;
			if (typeof topologyVersion !== "number") throw new Error("Hub sync omitted its topology version.");
			replacement.send("bridge.sync.ready", sync.id, { topologyVersion });
			await waitFor(() => hub.getTopology().bridges[0]?.connected === true, "replacement synchronization");
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("does not retain a shard state whose reconnect persistence failed", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 }],
			bridges: [persistedBridge()],
		});
		let saveShardCalls = 0;
		persistence.saveShardOperation = () => {
			saveShardCalls += 1;
			return saveShardCalls === 1
				? Promise.reject(new Error("Transient shard persistence failure."))
				: Promise.resolve();
		};
		const hub = createHub(persistence);
		const runningShards = [{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 }] as const;
		try {
			await hub.start();
			const failed = await openBridge(installed.server);
			sendHello(failed, { runningShards });
			await waitFor(() => failed.socket.closeCode !== undefined, "failed shard persistence close");
			expect(failed.socket.closeCode).toBe(1011);
			expect(persistence.savedShards).toHaveLength(0);

			const replacement = await openBridge(installed.server, 2);
			await synchronizeBridge(replacement, { connectionGeneration: 2, runningShards });
			expect(saveShardCalls).toBe(2);
			expect(persistence.savedShards).toHaveLength(1);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("requires explicit release before a different Bridge generation inherits assignments", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 7, shardId: 0, updatedAt: 10 }],
			bridges: [
				{
					connected: false,
					generation: BRIDGE_GENERATION,
					id: BRIDGE_ID,
					maxShards: 1,
					updatedAt: 10,
				},
			],
		});
		const hub = createHub(persistence);
		try {
			await hub.start();
			const unsafe = await openBridge(installed.server, 1, false, "generation-b");
			sendHello(unsafe, { bridgeGeneration: "generation-b" });
			await waitFor(() => unsafe.socket.closeCode !== undefined, "unsafe generation close");
			expect(unsafe.socket.closeCode).toBe(1002);
			expect(hub.getTopology().assignments).toEqual([{ bridgeId: BRIDGE_ID, epoch: 7, shardId: 0 }]);

			expect(await hub.releaseBridge(BRIDGE_ID)).toEqual([0]);
			const replacement = await openBridge(installed.server, 1, true, "generation-b");
			sendHello(replacement, { bridgeGeneration: "generation-b" });
			await replacement.waitForMessage("hub.shard.start");
			expect(hub.getTopology().assignments).toEqual([{ bridgeId: BRIDGE_ID, epoch: 8, shardId: 0 }]);

			replacement.socket.close(1000, "Generation restart");
			const staleGeneration = await openBridge(installed.server, 1, false, BRIDGE_GENERATION);
			sendHello(staleGeneration);
			await waitFor(() => staleGeneration.socket.closeCode !== undefined, "reintroduced generation close");
			expect(staleGeneration.socket.closeCode).toBe(1002);
			expect(hub.getTopology().assignments).toEqual([{ bridgeId: BRIDGE_ID, epoch: 8, shardId: 0 }]);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("ignores duplicate and stale shard states while waking a capped restart window", async () => {
		const installed = installFakeServe();
		let now = 0;
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 }],
			bridges: [persistedBridge()],
		});
		const hub = createHub(persistence, { now: () => now });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				restartPolicy: {
					initialDelayMs: 100,
					maxAttempts: 1,
					maxDelayMs: 100,
					windowMs: 5,
				},
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 }],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 1, "ready shard topology");

			const firstStartIndex = bridge.socket.sent.length;
			const failed: WireDataMap["bridge.shard.state"] = {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
				state: "failed",
			};
			bridge.send("bridge.shard.state", "state:failed:1", failed);
			bridge.send("bridge.shard.state", "state:failed:duplicate", failed);
			await bridge.waitForMessage("hub.shard.start", firstStartIndex);
			expect(
				persistence.savedShards.filter((shard) => shard.processGeneration === 1 && shard.state === "failed"),
			).toHaveLength(1);

			const secondStartIndex = bridge.socket.sent.length;
			bridge.send("bridge.shard.state", "state:failed:2", {
				...failed,
				processGeneration: 2,
			});
			await waitFor(
				() => persistence.savedShards.some((shard) => shard.processGeneration === 2 && shard.state === "failed"),
				"second failed process persistence",
			);
			now = 6;
			await bridge.waitForMessage("hub.shard.start", secondStartIndex);
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("ignores stale process generations and rejects state regression", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 }],
			bridges: [persistedBridge()],
		});
		const hub = createHub(persistence);
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				runningShards: [{ assignmentEpoch: 1, processGeneration: 2, ready: true, shardId: 0 }],
			});
			await waitFor(() => persistence.savedShards.length === 1, "initial ready state persistence");
			bridge.send("bridge.shard.state", "state:stale", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
				state: "failed",
			});
			await Bun.sleep(10);
			expect(persistence.savedShards).toHaveLength(1);
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);

			bridge.send("bridge.shard.state", "state:regression", {
				assignmentEpoch: 1,
				processGeneration: 2,
				shardId: 0,
				state: "starting",
			});
			await waitFor(() => bridge.socket.closeCode !== undefined, "state regression close");
			expect(bridge.socket.closeCode).toBe(1002);
			expect(persistence.savedShards).toHaveLength(1);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("rejects a stale shard process reported during reconnect", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 }],
			bridges: [persistedBridge()],
			shards: [
				{
					assignmentEpoch: 1,
					bridgeId: BRIDGE_ID,
					processGeneration: 2,
					shardId: 0,
					state: "ready",
					updatedAt: 2,
				},
			],
		});
		const hub = createHub(persistence);
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			sendHello(bridge, {
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 }],
			});
			await waitFor(() => bridge.socket.closeCode !== undefined, "stale reconnect process close");
			expect(bridge.socket.closeCode).toBe(1002);
			expect(persistence.savedShards).toHaveLength(1);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("accepts a repeated active state as completion of a pending start", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 }],
			bridges: [persistedBridge()],
			shards: [
				{
					assignmentEpoch: 1,
					bridgeId: BRIDGE_ID,
					processGeneration: 1,
					shardId: 0,
					state: "starting",
					updatedAt: 2,
				},
			],
		});
		const hub = createHub(persistence);
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: false, shardId: 0 }],
			});
			bridge.send("bridge.shard.state", "state:replayed-ready", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
				state: "ready",
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 1, "replayed active state");
			await Bun.sleep(110);
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
			expect(persistence.savedShards.at(-1)?.state).toBe("ready");
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("shares identify pacing across Bridge sessions", async () => {
		const installed = installFakeServe();
		let now = 0;
		const waits: number[] = [];
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: "bridge-b", epoch: 1, shardId: 1, updatedAt: 2 },
			],
			bridges: [persistedBridge(), persistedBridge("bridge-b", "generation-b")],
		});
		const hub = createHub(persistence, {
			now: () => now,
			sleep: (milliseconds) => {
				waits.push(milliseconds);
				now += milliseconds;
				return Promise.resolve();
			},
			totalShards: 2,
		});
		try {
			await hub.start();
			const first = await openBridge(installed.server);
			await synchronizeBridge(first, {
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 }],
			});
			const second = await openBridge(installed.server, 1, false, "generation-b", "bridge-b");
			await synchronizeBridge(second, {
				bridgeGeneration: "generation-b",
				bridgeId: "bridge-b",
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 }],
			});
			await waitFor(
				() => hub.getTopology().bridges.every((bridge) => bridge.readyShardIds.length === 1),
				"both Bridge shard states",
			);

			first.send("bridge.identify.request", "identify:first", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
			});
			const firstGrant = await first.waitForMessage("hub.identify.response");
			expect(firstGrant.data.granted).toBe(true);
			second.send("bridge.identify.request", "identify:second", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 1,
			});
			const secondGrant = await second.waitForMessage("hub.identify.response");
			expect(secondGrant.data.granted).toBe(true);
			expect(waits).toContain(5_000);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});
});
