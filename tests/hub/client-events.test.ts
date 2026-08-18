import { describe, expect, test } from "bun:test";
import type { $HubEventName } from "../../src/types/hub";
import {
	BRIDGE_GENERATION,
	BRIDGE_ID,
	createHub,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	sendHello,
	synchronizeBridge,
	waitFor,
} from "./client-harness";

type RecordedEvent = { readonly event: $HubEventName; readonly payload: unknown };

function recordAll(hub: ReturnType<typeof createHub>): RecordedEvent[] {
	const recorded: RecordedEvent[] = [];
	const names: readonly $HubEventName[] = [
		"bridgeConnected",
		"bridgeDisconnected",
		"bridgeSynchronized",
		"error",
		"shardAssigned",
		"shardDeallocated",
		"shardFailed",
		"shardReady",
		"shardRestartScheduled",
		"shardRestartsExhausted",
		"shardStopped",
	];
	for (const event of names) {
		hub.events.on(event, (payload) => {
			recorded.push({ event, payload });
		});
	}
	return recorded;
}

function eventsOf(recorded: readonly RecordedEvent[], event: $HubEventName): readonly unknown[] {
	return recorded.filter((entry) => entry.event === event).map((entry) => entry.payload);
}

describe("HubClient events", () => {
	test("emits connect, synchronize, assign, ready, stop, and disconnect through the full lifecycle", async () => {
		const installed = installFakeServe();
		const hub = createHub(new MemoryHubPersistence());
		const recorded = recordAll(hub);
		try {
			await hub.start();
			const bridge = await openBridge(installed.server, 1, true);
			sendHello(bridge);
			await waitFor(() => eventsOf(recorded, "shardAssigned").length > 0, "shard assignment event");
			const start = await bridge.waitForMessage("hub.shard.start");
			const epoch = start.data.assignmentEpoch;
			if (typeof epoch !== "number") throw new Error("Shard start omitted its epoch.");
			bridge.send("bridge.shard.state", "state:starting", {
				assignmentEpoch: epoch,
				processGeneration: 1,
				shardId: 0,
				state: "starting",
			});
			bridge.send("bridge.shard.state", "state:ready", {
				assignmentEpoch: epoch,
				processGeneration: 1,
				shardId: 0,
				state: "ready",
			});
			await waitFor(() => eventsOf(recorded, "shardReady").length > 0, "shard ready event");
			bridge.send("bridge.shard.state", "state:stopped", {
				assignmentEpoch: epoch,
				processGeneration: 1,
				shardId: 0,
				state: "stopped",
			});
			await waitFor(() => eventsOf(recorded, "shardStopped").length > 0, "shard stopped event");
			bridge.socket.close(1000, "test disconnect");
			await waitFor(() => eventsOf(recorded, "bridgeDisconnected").length > 0, "bridge disconnected event");

			expect(eventsOf(recorded, "bridgeConnected")).toEqual([
				{ bridgeId: BRIDGE_ID, connectionGeneration: 1, generation: BRIDGE_GENERATION },
			]);
			expect(eventsOf(recorded, "bridgeSynchronized").length).toBeGreaterThanOrEqual(1);
			expect(eventsOf(recorded, "shardAssigned")).toEqual([{ bridgeId: BRIDGE_ID, epoch, shardId: 0 }]);
			expect(eventsOf(recorded, "shardReady")).toEqual([{ bridgeId: BRIDGE_ID, shardId: 0 }]);
			expect(eventsOf(recorded, "bridgeDisconnected")).toEqual([
				{ bridgeId: BRIDGE_ID, generation: BRIDGE_GENERATION },
			]);
			const payload = eventsOf(recorded, "shardAssigned")[0];
			expect(Object.isFrozen(payload)).toBe(true);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("emits shardFailed and shardRestartScheduled when a shard process dies", async () => {
		const installed = installFakeServe();
		const hub = createHub(new MemoryHubPersistence());
		const recorded = recordAll(hub);
		try {
			await hub.start();
			const bridge = await openBridge(installed.server, 1, true);
			sendHello(bridge);
			const start = await bridge.waitForMessage("hub.shard.start");
			const epoch = start.data.assignmentEpoch;
			if (typeof epoch !== "number") throw new Error("Shard start omitted its epoch.");
			bridge.send("bridge.shard.state", "state:starting", {
				assignmentEpoch: epoch,
				processGeneration: 1,
				shardId: 0,
				state: "starting",
			});
			bridge.send("bridge.shard.state", "state:failed", {
				assignmentEpoch: epoch,
				processGeneration: 1,
				shardId: 0,
				state: "failed",
			});
			await waitFor(() => eventsOf(recorded, "shardRestartScheduled").length > 0, "restart scheduled event");
			expect(eventsOf(recorded, "shardFailed")).toEqual([{ bridgeId: BRIDGE_ID, shardId: 0 }]);
			expect(eventsOf(recorded, "shardRestartScheduled")).toEqual([
				{ attempt: 1, bridgeId: BRIDGE_ID, delayMs: 1_000, shardId: 0 },
			]);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("emits shardDeallocated with reason released when an operator releases a Bridge", async () => {
		const installed = installFakeServe();
		const hub = createHub(new MemoryHubPersistence());
		const recorded = recordAll(hub);
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge);
			await waitFor(() => eventsOf(recorded, "shardAssigned").length > 0, "assignment");
			bridge.socket.close(1000, "gone");
			await waitFor(() => eventsOf(recorded, "bridgeDisconnected").length > 0, "disconnect");
			await hub.releaseBridge(BRIDGE_ID);
			expect(eventsOf(recorded, "shardDeallocated")).toEqual([{ bridgeId: BRIDGE_ID, reason: "released", shardId: 0 }]);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("mirrors reported background failures as error events", async () => {
		const installed = installFakeServe();
		const hub = createHub(new MemoryHubPersistence());
		const errors: { readonly context: string; readonly error: Error }[] = [];
		hub.events.on("error", (payload) => {
			errors.push({ context: payload.context, error: payload.error });
		});
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge);
			bridge.send("bridge.heartbeat", "heartbeat:bad", {
				bridgeGeneration: "stale-generation",
				connectionGeneration: 1,
				sentAt: 1,
			});
			await waitFor(() => errors.length > 0, "error event");
			const payload = errors[0];
			if (payload === undefined) throw new Error("No error event was recorded.");
			expect(payload.error).toBeInstanceOf(Error);
			expect(payload.context.length).toBeGreaterThan(0);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("isolates a throwing listener and keeps delivering to later listeners", async () => {
		const installed = installFakeServe();
		const hub = createHub(new MemoryHubPersistence());
		const delivered: string[] = [];
		hub.events.on("bridgeConnected", () => {
			throw new Error("listener exploded");
		});
		hub.events.on("bridgeConnected", (payload) => {
			delivered.push(payload.bridgeId);
		});
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge);
			await waitFor(() => delivered.length > 0, "second listener delivery");
			expect(delivered).toEqual([BRIDGE_ID]);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("supports once, off, unsubscribe closures, and listenerCount", () => {
		const hub = createHub(new MemoryHubPersistence());
		const seen: number[] = [];
		const onceRemove = hub.events.once("shardReady", () => {
			seen.push(1);
		});
		void onceRemove;
		const listener = (): void => {
			seen.push(2);
		};
		hub.events.on("shardReady", listener);
		const removeThird = hub.events.on("shardReady", () => {
			seen.push(3);
		});
		expect(hub.events.listenerCount("shardReady")).toBe(3);
		hub.events.off("shardReady", listener);
		removeThird();
		expect(hub.events.listenerCount("shardReady")).toBe(1);
	});
});
