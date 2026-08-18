import { describe, expect, test } from "bun:test";
import { BridgeClient } from "../../src/bridge/BridgeClient";
import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { createWireMessage } from "../../src/protocol/codec";
import type { $ShardProcessFactory } from "../../src/types/bridge";
import {
	createBridgeOptions,
	createHubHarness,
	createShardHarness,
	messageType,
	waitFor,
	waitForHubMessage,
	waitForShardMessage,
} from "./client-harness";

describe("BridgeClient topology and process control", () => {
	test("waits for active inbound Hub work before reconnecting", async () => {
		const hub = createHubHarness();
		const shards = createShardHarness();
		let releaseRouteSend = (): void => undefined;
		const routeSend = new Promise<void>((resolve) => {
			releaseRouteSend = resolve;
		});
		const processFactory: $ShardProcessFactory = (context) => {
			const process = shards.factory(context);
			return {
				exited: process.exited,
				get killed(): boolean {
					return process.killed;
				},
				kill(signal?: number): void {
					process.kill(signal);
				},
				pid: process.pid,
				send(message: object): void | Promise<void> {
					const operation = process.send(message);
					if (messageType(message) !== "shard.control.route.request") return operation;
					return Promise.resolve(operation).then(() => routeSend);
				},
			};
		};
		let connections = 0;
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				processFactory,
				sleep: () => Promise.resolve(),
				socketFactory: (url, headers) => {
					connections += 1;
					return hub.factory(url, headers);
				},
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
			hub.send("hub.route.request", "route:blocked", {
				kind: "message",
				payload: null,
				sourceShardId: null,
				target: {
					assignmentEpoch: 3,
					processGeneration: 1,
					shardId: 0,
				},
			});
			await waitForShardMessage(shards, "shard.control.route.request");
			hub.close();
			await Bun.sleep(10);
			expect(connections).toBe(1);

			releaseRouteSend();
			await waitFor(() => connections === 2, "reconnect after inbound work");
		} finally {
			releaseRouteSend();
			await bridge.stop();
			hub.close();
		}
	});

	test("bounds and terminates a shard process that floods its IPC queue", async () => {
		const hub = createHubHarness();
		const shards = createShardHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				processFactory: shards.factory,
				requestMaxPending: 2,
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
			const heartbeat = createWireMessage(
				"shard.heartbeat",
				"heartbeat:flood",
				{ assignmentEpoch: 3, processGeneration: 1, shardId: 0 },
				DEFAULT_PAYLOAD_POLICY,
			);
			context.callbacks.onMessage(heartbeat);
			context.callbacks.onMessage(heartbeat);
			context.callbacks.onMessage(heartbeat);

			await waitFor(
				() => shards.sent.some((entry) => messageType(entry) === "shard.control.shutdown"),
				"misbehaving shard termination",
			);
			await waitFor(() => bridge.shards.size === 0, "misbehaving shard release");
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("resets topology version comparison for each authenticated connection", async () => {
		const hub = createHubHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				socketFactory: hub.factory,
			}),
		);
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:first", {
				assignments: [],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 5,
				totalShards: 2,
			});
			await bridge.waitUntilConnected(1_000);
			const beforeReconnect = hub.received.length;
			hub.close();
			const hello = await waitForHubMessage(hub, "bridge.hello", beforeReconnect);
			expect(hello.data.connectionGeneration).toBe(2);
			hub.send("hub.sync", "sync:replacement", {
				assignments: [],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 2,
				topologyVersion: 1,
				totalShards: 3,
			});
			await bridge.waitUntilConnected(1_000);
			expect(bridge.connected).toBe(true);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("restarts retained shard processes when the global shard count changes", async () => {
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
			hub.send("hub.sync", "sync:first", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 1,
				totalShards: 2,
			});
			await bridge.waitUntilConnected(1_000);
			hub.send("hub.shard.start", "start:first", {
				assignmentEpoch: 3,
				shardId: 0,
				totalShards: 2,
			});
			await waitFor(() => shards.contexts.length === 1, "first shard process");

			const beforeReconnect = hub.received.length;
			const beforeRestart = shards.sent.length;
			hub.close();
			const hello = await waitForHubMessage(hub, "bridge.hello", beforeReconnect);
			expect(hello.data.connectionGeneration).toBe(2);
			hub.send("hub.sync", "sync:replacement", {
				assignments: [{ epoch: 3, shardId: 0 }],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 2,
				topologyVersion: 1,
				totalShards: 3,
			});
			await waitForShardMessage(shards, "shard.control.shutdown", beforeRestart);
			await bridge.waitUntilConnected(1_000);
			expect(bridge.shards.size).toBe(0);

			hub.send("hub.shard.start", "start:replacement", {
				assignmentEpoch: 3,
				shardId: 0,
				totalShards: 3,
			});
			await waitFor(() => shards.contexts.length === 2, "replacement shard process");
			expect(shards.contexts[1]?.totalShards).toBe(3);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("re-emits current state when a matching shard start is repeated", async () => {
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
			hub.send("hub.shard.start", "start:first", {
				assignmentEpoch: 3,
				shardId: 0,
				totalShards: 2,
			});
			const firstState = await waitForHubMessage(hub, "bridge.shard.state");
			const replayStart = hub.received.length;
			hub.send("hub.shard.start", "start:duplicate", {
				assignmentEpoch: 3,
				shardId: 0,
				totalShards: 2,
			});
			const replayedState = await waitForHubMessage(hub, "bridge.shard.state", replayStart);
			expect(replayedState.data).toEqual(firstState.data);
			expect(shards.contexts).toHaveLength(1);
		} finally {
			await bridge.stop();
			hub.close();
		}
	});

	test("rejects topology regression within one connection", async () => {
		const hub = createHubHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				socketFactory: hub.factory,
			}),
		);
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			hub.send("hub.sync", "sync:2", {
				assignments: [],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 2,
				totalShards: 2,
			});
			await bridge.waitUntilConnected(1_000);
			hub.send("hub.sync", "sync:1", {
				assignments: [],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 1,
				totalShards: 2,
			});
			await waitFor(() => hub.closeCode() === 1002, "topology regression close");
		} finally {
			await bridge.stop();
			hub.close();
		}
	});
});
