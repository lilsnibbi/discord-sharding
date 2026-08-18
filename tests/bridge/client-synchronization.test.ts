import { describe, expect, test } from "bun:test";
import { BridgeClient } from "../../src/bridge/BridgeClient";
import { ShardingCapacityError, ShardingTimeoutError } from "../../src/errors/ShardingError";
import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { createWireMessage } from "../../src/protocol/codec";
import type { $Sleep } from "../../src/types/common";
import {
	createBridgeOptions,
	createHubHarness,
	createShardHarness,
	messageType,
	waitFor,
	waitForHubMessage,
	waitForShardMessage,
} from "./client-harness";

describe("BridgeClient synchronization and waiters", () => {
	test("authenticates, synchronizes maintenance, and closes cleanly", async () => {
		const hub = createHubHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				socketFactory: hub.factory,
			}),
		);
		const maintenance: boolean[] = [];
		bridge.onMaintenanceChange((value) => {
			maintenance.push(value);
		});
		try {
			await bridge.start();
			const hello = await waitForHubMessage(hub, "bridge.hello");
			expect(hub.authorization()).toBe("Bearer 0123456789abcdef");
			expect(hello.data.bridgeId).toBe("bridge:test");
			expect(hello.data.connectionGeneration).toBe(1);

			hub.send("hub.sync", "sync:1", {
				assignments: [],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 1,
				totalShards: 2,
			});
			await bridge.waitUntilConnected(1_000);
			expect(bridge.connected).toBe(true);
			expect(bridge.isInMaintenance).toBe(false);
			expect(maintenance).toEqual([false]);
		} finally {
			await bridge.stop();
			hub.close();
		}
		expect(bridge.state).toBe("stopped");
		expect(bridge.connected).toBe(false);
		expect(bridge.isInMaintenance).toBe(true);
	});

	test("times out connection waiters and removes them during shutdown", async () => {
		const sleep: $Sleep = async (_milliseconds, signal) =>
			new Promise<void>((_resolve, reject) => {
				signal.addEventListener("abort", () => reject(signal.reason), { once: true });
			});
		const bridge = new BridgeClient(
			createBridgeOptions("http://127.0.0.1:1", {
				requestMaxPending: 2,
				sleep,
				socketFactory: () => {
					throw new Error("Hub unavailable");
				},
			}),
		);
		await bridge.start();

		await expect(bridge.waitUntilConnected(5)).rejects.toBeInstanceOf(ShardingTimeoutError);
		const pendingWaiter = bridge.waitUntilConnected(1_000);
		const pendingAssertion = pendingWaiter.catch((error: unknown) => error);
		const firstStop = bridge.stop();
		const secondStop = bridge.stop();
		expect(firstStop).toBe(secondStop);
		expect(await pendingAssertion).toBeInstanceOf(Error);
		await firstStop;
		expect(bridge.state).toBe("stopped");
	});

	test("bounds connection waiter admission deterministically", async () => {
		const sleep: $Sleep = async (_milliseconds, signal) =>
			new Promise<void>((_resolve, reject) => {
				signal.addEventListener("abort", () => reject(signal.reason), { once: true });
			});
		const bridge = new BridgeClient(
			createBridgeOptions("http://127.0.0.1:1", {
				requestMaxPending: 1,
				sleep,
				socketFactory: () => {
					throw new Error("Hub unavailable");
				},
			}),
		);
		await bridge.start();
		const firstWaiter = bridge.waitUntilConnected(10_000);
		const observedFirstWaiter = firstWaiter.catch((error: unknown) => error);

		await expect(bridge.waitUntilConnected(10_000)).rejects.toBeInstanceOf(ShardingCapacityError);
		await bridge.stop();
		expect(await observedFirstWaiter).toBeInstanceOf(Error);
	});

	test("serializes Hub messages and removes stale processes before acknowledging topology", async () => {
		const hub = createHubHarness();
		const shards = createShardHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				processFactory: shards.factory,
				socketFactory: hub.factory,
			}),
		);
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:1", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 1,
				totalShards: 2,
			});
			hub.send("hub.shard.start", "start:1", {
				assignmentEpoch: 3,
				shardId: 0,
				totalShards: 2,
			});
			await bridge.waitUntilConnected(1_000);
			await waitFor(() => shards.contexts.length === 1, "serialized shard start");

			const beforeSync = hub.received.length;
			hub.send("hub.sync", "sync:2", {
				assignments: [],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 2,
				totalShards: 2,
			});
			const ready = await waitForHubMessage(hub, "bridge.sync.ready", beforeSync);
			expect(ready.id).toBe("sync:2");
			expect(bridge.shards.size).toBe(0);
			expect(shards.sent.some((entry) => messageType(entry) === "shard.control.shutdown")).toBe(true);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("ignores an expired shard acknowledgement after a newer sync replaces it", async () => {
		const hub = createHubHarness();
		const shards = createShardHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				processFactory: shards.factory,
				socketFactory: hub.factory,
			}),
		);
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:1", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 1,
				totalShards: 2,
			});
			await bridge.waitUntilConnected(1_000);
			hub.send("hub.shard.start", "start:1", {
				assignmentEpoch: 3,
				shardId: 0,
				totalShards: 2,
			});
			await waitFor(() => shards.contexts.length === 1, "shard process");
			const context = shards.contexts[0];
			if (context === undefined) throw new Error("Shard context was not created.");

			const beforeSecondSync = shards.sent.length;
			hub.send("hub.sync", "sync:2", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 2,
				totalShards: 2,
			});
			const oldMaintenance = await waitForShardMessage(shards, "shard.control.maintenance", beforeSecondSync);
			expect(bridge.connected).toBe(false);

			const beforeThirdSync = shards.sent.length;
			hub.send("hub.sync", "sync:3", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 3,
				totalShards: 2,
			});
			const currentMaintenance = await waitForShardMessage(shards, "shard.control.maintenance", beforeThirdSync);
			const beforeAcknowledgement = hub.received.length;
			context.callbacks.onMessage(
				createWireMessage("shard.sync.ack", oldMaintenance.id, { topologyVersion: 2 }, DEFAULT_PAYLOAD_POLICY),
			);
			context.callbacks.onMessage(
				createWireMessage("shard.sync.ack", currentMaintenance.id, { topologyVersion: 3 }, DEFAULT_PAYLOAD_POLICY),
			);
			const ready = await waitForHubMessage(hub, "bridge.sync.ready", beforeAcknowledgement);
			await waitFor(() => bridge.connected, "replacement topology acknowledgement");
			expect(ready.data.topologyVersion).toBe(3);
			expect(bridge.shards.size).toBe(1);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("holds a shard identify through a maintenance window instead of failing it", async () => {
		const hub = createHubHarness();
		const shards = createShardHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				processFactory: shards.factory,
				socketFactory: hub.factory,
			}),
		);
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:1", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 1,
				totalShards: 2,
			});
			await bridge.waitUntilConnected(1_000);
			hub.send("hub.shard.start", "start:1", {
				assignmentEpoch: 3,
				shardId: 0,
				totalShards: 2,
			});
			await waitFor(() => shards.contexts.length === 1, "shard process");
			const context = shards.contexts[0];
			if (context === undefined) throw new Error("Shard context was not created.");

			const beforeSync = shards.sent.length;
			hub.send("hub.sync", "sync:2", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 2,
				totalShards: 2,
			});
			const maintenance = await waitForShardMessage(shards, "shard.control.maintenance", beforeSync);
			expect(bridge.isInMaintenance).toBe(true);

			const beforeIdentify = hub.received.length;
			const heldSendCount = shards.sent.length;
			context.callbacks.onMessage(
				createWireMessage("shard.identify.request", "identify:1", {}, DEFAULT_PAYLOAD_POLICY),
			);
			await Bun.sleep(20);
			expect(shards.sent.length).toBe(heldSendCount);

			context.callbacks.onMessage(
				createWireMessage("shard.sync.ack", maintenance.id, { topologyVersion: 2 }, DEFAULT_PAYLOAD_POLICY),
			);
			await waitFor(() => bridge.connected, "topology acknowledgement");
			const forwarded = await waitForHubMessage(hub, "bridge.identify.request", beforeIdentify);
			expect(forwarded.id).toBe("identify:1");
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("stops a spawned process when its required startup notification fails", async () => {
		const hub = createHubHarness();
		const shards = createShardHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				processFactory: shards.factory,
				socketFactory: hub.factory,
			}),
		);
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:1", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 1,
				totalShards: 2,
			});
			await bridge.waitUntilConnected(1_000);
			hub.failNextBridgeMessage("bridge.shard.state");
			hub.send("hub.shard.start", "start:1", {
				assignmentEpoch: 3,
				shardId: 0,
				totalShards: 2,
			});

			await waitFor(
				() => shards.sent.some((entry) => messageType(entry) === "shard.control.shutdown"),
				"startup rollback",
			);
			await waitFor(() => bridge.shards.size === 0, "rolled-back shard process");
			await waitFor(() => hub.closeCode() === 1011, "transport close code");
		} finally {
			await bridge.stop();
			hub.close();
		}
	});
});
