import { describe, expect, test } from "bun:test";
import { HUB_SILENCE_TIMEOUT_MS, hubSilenceExceeded } from "../../src/bridge/runtime/BridgeConnection";
import { BridgeRuntime } from "../../src/bridge/runtime/BridgeRuntime";
import { SHARD_SILENCE_TIMEOUT_MS, SHARD_WATCHDOG_INTERVAL_MS } from "../../src/bridge/runtime/BridgeShards";
import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { createWireMessage } from "../../src/protocol/codec";
import {
	createBridgeOptions,
	createHubHarness,
	createShardHarness,
	waitFor,
	waitForHubMessage,
} from "./client-harness";

class ObservableBridgeRuntime extends BridgeRuntime {
	public sweep(now: number): void {
		this.sweepUnresponsiveShards(now);
	}
}

describe("Bridge liveness deadlines", () => {
	test("treats a Hub connection as wedged only after several missed heartbeats", () => {
		expect(hubSilenceExceeded(1_000, 1_000 + HUB_SILENCE_TIMEOUT_MS)).toBe(false);
		expect(hubSilenceExceeded(1_000, 1_000 + HUB_SILENCE_TIMEOUT_MS + 1)).toBe(true);
		expect(HUB_SILENCE_TIMEOUT_MS).toBeGreaterThan(SHARD_WATCHDOG_INTERVAL_MS * 2);
		expect(SHARD_SILENCE_TIMEOUT_MS).toBeGreaterThan(SHARD_WATCHDOG_INTERVAL_MS * 2);
	});

	test("fails a silent Discord-ready shard so the Hub restarts it", async () => {
		const hub = createHubHarness();
		const shards = createShardHarness();
		const bridge = new ObservableBridgeRuntime(
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
			const identity = {
				assignmentEpoch: context.assignmentEpoch,
				processGeneration: context.processGeneration,
				shardId: context.shardId,
			};
			context.callbacks.onMessage(createWireMessage("shard.ready", "ready:1", identity, DEFAULT_PAYLOAD_POLICY));
			await waitFor(() => bridge.shards.get(0)?.state === "ready", "Discord-ready shard");

			const beforeSweep = hub.received.length;
			bridge.sweep(Date.now());
			expect(bridge.shards.get(0)?.state).toBe("ready");

			bridge.sweep(Date.now() + SHARD_SILENCE_TIMEOUT_MS + 1);
			const failure = await waitForHubMessage(hub, "bridge.shard.state", beforeSweep);
			expect(failure.data.state).toBe("failed");
			expect(failure.data.shardId).toBe(0);
			await waitFor(() => bridge.shards.size === 0, "terminated shard process");
		} finally {
			await bridge.stop();
			hub.close();
		}
	});
});
