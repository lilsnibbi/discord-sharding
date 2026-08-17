import { describe, expect, test } from "bun:test";
import { BridgeClient } from "../../src/bridge/BridgeClient";
import { ShardingCapacityError, ShardingConfigurationError, ShardingStateError } from "../../src/errors/ShardingError";
import { createBridgeOptions, createHubHarness, createShardHarness, waitForHubMessage } from "./client-harness";

function createBridge(): BridgeClient {
	const hub = createHubHarness();
	const shards = createShardHarness();
	return new BridgeClient(createBridgeOptions(hub.url, { processFactory: shards.factory, socketFactory: hub.factory }));
}

describe("BridgeClient facade", () => {
	test("exposes runtime identity and lifecycle state", async () => {
		const hub = createHubHarness();
		const shards = createShardHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, { processFactory: shards.factory, socketFactory: hub.factory }),
		);
		try {
			expect(bridge.id).toBe("bridge:test");
			expect(bridge.maxShards).toBe(2);
			expect(bridge.generation.length).toBeGreaterThan(0);
			expect(bridge.state).toBe("idle");
			expect(bridge.connected).toBeFalse();
			expect(bridge.isInMaintenance).toBeTrue();
			expect(bridge.shards.size).toBe(0);

			expect(await bridge.start()).toBe(bridge);
			expect(bridge.state).toBe("running");
			await waitForHubMessage(hub, "bridge.hello");
			expect(hub.authorization()).toBe("Bearer 0123456789abcdef");
		} finally {
			await bridge.stop();
			expect(bridge.state).toBe("stopped");
		}
	});

	test("reads and clears local analytics through the facade", async () => {
		const bridge = createBridge();
		await bridge.start();
		try {
			expect(await bridge.getAnalytics()).toEqual([]);
			expect(await bridge.getAnalytics({ limit: 5, shardId: 0 })).toEqual([]);
			expect(await bridge.clearAnalytics(Date.now(), 10)).toBe(0);
			expect(await bridge.clearAnalytics()).toBe(0);
		} finally {
			await bridge.stop();
		}
	});

	test("rejects invalid analytics arguments and access outside the running state", async () => {
		const bridge = createBridge();

		await expect(bridge.getAnalytics()).rejects.toBeInstanceOf(ShardingStateError);
		await bridge.start();
		try {
			await expect(bridge.clearAnalytics(-1)).rejects.toBeInstanceOf(ShardingConfigurationError);
			await expect(bridge.clearAnalytics(Number.NaN)).rejects.toBeInstanceOf(ShardingConfigurationError);
			await expect(bridge.clearAnalytics(Date.now(), 0)).rejects.toBeInstanceOf(ShardingConfigurationError);
			await expect(bridge.clearAnalytics(Date.now(), 10_001)).rejects.toBeInstanceOf(ShardingConfigurationError);
		} finally {
			await bridge.stop();
		}
		await expect(bridge.getAnalytics()).rejects.toBeInstanceOf(ShardingStateError);
	});

	test("bounds maintenance listeners and removes them idempotently", async () => {
		const bridge = createBridge();
		await bridge.start();
		try {
			const removals = Array.from({ length: 256 }, () => bridge.onMaintenanceChange(() => undefined));

			expect(() => bridge.onMaintenanceChange(() => undefined)).toThrow(ShardingCapacityError);
			const first = removals[0];
			if (first === undefined) throw new Error("Listener removal callback is missing.");
			first();
			first();
			expect(() => bridge.onMaintenanceChange(() => undefined)).not.toThrow();
			expect(() => bridge.onMaintenanceChange(Reflect.get({}, "missing"))).toThrow(ShardingConfigurationError);
		} finally {
			await bridge.stop();
		}
	});

	test("shares one shutdown operation and disposes with await using", async () => {
		const bridge = createBridge();
		await bridge.start();
		const first = bridge.stop();
		const second = bridge.stop();

		expect(second).toBe(first);
		await first;
		expect(bridge.state).toBe("stopped");
		await bridge.stop();

		const hub = createHubHarness();
		const shards = createShardHarness();
		{
			await using disposable = await new BridgeClient(
				createBridgeOptions(hub.url, { processFactory: shards.factory, socketFactory: hub.factory }),
			).start();
			expect(disposable.state).toBe("running");
		}
	});

	test("rejects waiting for a connection before the Bridge is running", async () => {
		const bridge = createBridge();

		await expect(bridge.waitUntilConnected(50)).rejects.toBeInstanceOf(ShardingStateError);
		await bridge.start();
		try {
			await expect(bridge.waitUntilConnected(-1)).rejects.toBeInstanceOf(ShardingConfigurationError);
		} finally {
			await bridge.stop();
		}
	});
});
