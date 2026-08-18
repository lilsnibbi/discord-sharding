import { describe, expect, test } from "bun:test";
import { BridgeClient } from "../../src/bridge/BridgeClient";
import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { createWireMessage, parseWireMessage } from "../../src/protocol/codec";
import { BRIDGE_TO_HUB_TYPES } from "../../src/protocol/types";
import {
	createBridgeOptions,
	createHubHarness,
	createShardHarness,
	messageType,
	waitFor,
	waitForHubMessage,
	waitForShardMessage,
} from "./client-harness";

describe("BridgeClient routing and analytics", () => {
	test("answers the Hub instead of failing the connection when a shard process is gone", async () => {
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

			const staleTarget = { assignmentEpoch: 3, processGeneration: 99, shardId: 0 };
			const beforeRoute = hub.received.length;
			hub.send("hub.route.request", "route:stale", {
				kind: "request",
				payload: null,
				sourceShardId: 1,
				target: staleTarget,
			});
			const failure = await waitForHubMessage(hub, "bridge.route.response", beforeRoute);
			expect(failure.id).toBe("route:stale");
			expect(failure.data.ok).toBe(false);
			expect(failure.data.sourceShardId).toBe(1);

			const beforePrepare = hub.received.length;
			hub.send("hub.eval.prepare", "eval:stale", {
				context: null,
				evaluator: "() => 1",
				sourceShardId: 1,
				target: staleTarget,
			});
			const prepared = await waitForHubMessage(hub, "bridge.eval.prepared", beforePrepare);
			expect(prepared.data.ok).toBe(false);

			expect(hub.closeCode()).toBeNull();
			expect(bridge.connected).toBe(true);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("ignores late Hub route replies and returns local admission failures to the shard", async () => {
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

			context.callbacks.onMessage(
				createWireMessage(
					"shard.route.request",
					"route:late",
					{ kind: "request", payload: null, targetShardId: 1 },
					DEFAULT_PAYLOAD_POLICY,
				),
			);
			await waitForHubMessage(hub, "bridge.route.request");
			const responseData = {
				assignmentEpoch: 9,
				ok: true,
				processGeneration: 2,
				shardId: 1,
				sourceShardId: 0,
				value: "accepted",
			} as const;
			const beforeResponse = shards.sent.length;
			hub.send("hub.route.response", "route:late", responseData);
			await waitForShardMessage(shards, "shard.control.route.response", beforeResponse);
			hub.send("hub.route.response", "route:late", responseData);
			await Bun.sleep(2);
			expect(bridge.connected).toBe(true);

			hub.close();
			await waitFor(() => !bridge.connected, "Bridge maintenance");
			const beforeRejectedRequest = shards.sent.length;
			context.callbacks.onMessage(
				createWireMessage(
					"shard.route.request",
					"route:maintenance",
					{ kind: "request", payload: null, targetShardId: 1 },
					DEFAULT_PAYLOAD_POLICY,
				),
			);
			const rejected = await waitForShardMessage(shards, "shard.control.route.response", beforeRejectedRequest);
			expect(rejected.data.ok).toBe(false);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("rejects duplicate inbound correlations", async () => {
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
				connectionGeneration: 1,
				topologyVersion: 2,
				totalShards: 2,
			});
			await bridge.waitUntilConnected(1_000);
			hub.send("hub.shard.start", "start:1", {
				assignmentEpoch: 3,
				shardId: 0,
				totalShards: 2,
			});
			await waitFor(() => shards.contexts.length === 1, "shard process");

			const routed = {
				kind: "request" as const,
				payload: null,
				sourceShardId: null,
				target: {
					assignmentEpoch: 3,
					processGeneration: 1,
					shardId: 0,
				},
			};
			hub.send("hub.route.request", "inbound:duplicate", routed);
			await waitForShardMessage(shards, "shard.control.route.request");
			hub.send("hub.route.request", "inbound:duplicate", routed);
			await waitFor(() => hub.closeCode() === 1002, "duplicate correlation close");
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("forwards identify grants and shard route requests through the Hub", async () => {
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
			context.callbacks.onMessage(
				createWireMessage(
					"shard.booted",
					"boot:1",
					{
						assignmentEpoch: 3,
						processGeneration: 1,
						shardId: 0,
						totalShards: 2,
					},
					DEFAULT_PAYLOAD_POLICY,
				),
			);
			await waitForShardMessage(shards, "shard.control.maintenance");

			const hubMessagesBeforeIdentify = hub.received.length;
			context.callbacks.onMessage(
				createWireMessage("shard.identify.request", "identify:1", {}, DEFAULT_PAYLOAD_POLICY),
			);
			const identify = await waitForHubMessage(hub, "bridge.identify.request", hubMessagesBeforeIdentify);
			expect(identify.data).toEqual({
				assignmentEpoch: 3,
				processGeneration: 1,
				shardId: 0,
			});
			const shardMessagesBeforeIdentify = shards.sent.length;
			hub.send("hub.identify.response", "identify:1", {
				granted: true,
				shardId: 0,
			});
			const identifyResponse = await waitForShardMessage(
				shards,
				"shard.control.identify.response",
				shardMessagesBeforeIdentify,
			);
			expect(identifyResponse.data).toEqual({ granted: true });

			const hubMessagesBeforeRoute = hub.received.length;
			context.callbacks.onMessage(
				createWireMessage(
					"shard.route.request",
					"route:1",
					{
						kind: "request",
						payload: { ping: true },
						targetShardId: 1,
					},
					DEFAULT_PAYLOAD_POLICY,
				),
			);
			const route = await waitForHubMessage(hub, "bridge.route.request", hubMessagesBeforeRoute);
			expect(route.data).toEqual({
				assignmentEpoch: 3,
				kind: "request",
				payload: { ping: true },
				processGeneration: 1,
				shardId: 0,
				targetShardId: 1,
			});
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("keeps a healthy shard alive when analytics forwarding loses the Hub", async () => {
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
			context.callbacks.onMessage(
				createWireMessage(
					"shard.ready",
					"ready:1",
					{ assignmentEpoch: 3, processGeneration: 1, shardId: 0 },
					DEFAULT_PAYLOAD_POLICY,
				),
			);
			await waitFor(() => bridge.shards.get(0)?.state === "ready", "ready shard");

			const beforeReconnect = hub.received.length;
			hub.failNextBridgeMessage("bridge.analytics");
			context.callbacks.onMessage(
				createWireMessage(
					"shard.analytics",
					"analytics:loss",
					{ collectedAt: 10, payload: { guilds: 1 } },
					DEFAULT_PAYLOAD_POLICY,
				),
			);
			await waitFor(() => bridge.isInMaintenance, "Bridge maintenance");
			await waitForShardMessage(shards, "shard.control.maintenance");
			expect(bridge.shards.get(0)?.state).toBe("ready");
			expect(shards.sent.some((message) => messageType(message) === "shard.control.shutdown")).toBe(false);
			expect(await bridge.getAnalytics({ shardId: 0 })).toHaveLength(1);

			const hello = await waitForHubMessage(hub, "bridge.hello", beforeReconnect);
			expect(hello.data.connectionGeneration).toBe(2);
			const beforeResynchronization = shards.sent.length;
			hub.send("hub.sync", "sync:reconnected", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				connectionGeneration: 2,
				topologyVersion: 1,
				totalShards: 2,
			});
			const maintenance = await waitForShardMessage(shards, "shard.control.maintenance", beforeResynchronization);
			context.callbacks.onMessage(
				createWireMessage("shard.sync.ack", maintenance.id, { topologyVersion: 1 }, DEFAULT_PAYLOAD_POLICY),
			);
			await bridge.waitUntilConnected(1_000);
			expect(bridge.shards.get(0)?.state).toBe("ready");
			expect(shards.contexts).toHaveLength(1);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("reports one failed state for one unexpected shard exit", async () => {
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
			const beforeExit = hub.received.length;
			context.callbacks.onExit({ code: 1, signal: null });
			await waitFor(() => bridge.shards.size === 0, "failed shard release");
			const failedNotifications = hub.received
				.slice(beforeExit)
				.map((value) => parseWireMessage(value, BRIDGE_TO_HUB_TYPES, DEFAULT_PAYLOAD_POLICY))
				.filter((message) => message.type === "bridge.shard.state" && message.data.state === "failed");
			expect(failedNotifications).toHaveLength(1);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});
});
