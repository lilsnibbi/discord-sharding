import { describe, expect, test } from "bun:test";
import {
	BRIDGE_ID,
	createDeferred,
	createHub,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	persistedBridge,
	sendHello,
	synchronizeBridge,
	waitFor,
} from "./client-harness";

describe("HubClient session lifecycle", () => {
	test("processes each session's inbound messages in arrival order", async () => {
		const installed = installFakeServe();
		const bridgeSaveStarted = createDeferred<void>();
		const releaseBridgeSave = createDeferred<void>();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 }],
			bridges: [persistedBridge()],
		});
		persistence.saveBridgeOperation = async (bridge) => {
			if (!bridge.connected) return;
			bridgeSaveStarted.resolve();
			await releaseBridgeSave.promise;
		};
		const hub = createHub(persistence);
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			sendHello(bridge, {
				runningShards: [
					{
						assignmentEpoch: 1,
						processGeneration: 1,
						ready: false,
						shardId: 0,
					},
				],
			});
			await bridgeSaveStarted.promise;
			bridge.send("bridge.shard.state", "state:1", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
				state: "ready",
			});
			await Bun.sleep(10);
			expect(persistence.savedShards).toHaveLength(0);

			releaseBridgeSave.resolve();
			await waitFor(() => persistence.savedShards.length === 2, "serialized shard persistence");
			expect(persistence.savedShards.map((shard) => shard.state)).toEqual(["starting", "ready"]);
		} finally {
			releaseBridgeSave.resolve();
			await hub.stop();
			installed.restore();
		}
	});

	test("drops queued inbound work and waits for the active operation during shutdown", async () => {
		const installed = installFakeServe();
		const bridgeSaveStarted = createDeferred<void>();
		const releaseBridgeSave = createDeferred<void>();
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 }],
			bridges: [persistedBridge()],
		});
		persistence.saveBridgeOperation = async (bridge) => {
			if (!bridge.connected) return;
			bridgeSaveStarted.resolve();
			await releaseBridgeSave.promise;
		};
		const hub = createHub(persistence);
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			sendHello(bridge, {
				runningShards: [
					{
						assignmentEpoch: 1,
						processGeneration: 1,
						ready: false,
						shardId: 0,
					},
				],
			});
			await bridgeSaveStarted.promise;
			bridge.send("bridge.shard.state", "state:1", {
				assignmentEpoch: 1,
				processGeneration: 1,
				shardId: 0,
				state: "ready",
			});

			const stopping = hub.stop();
			await Bun.sleep(10);
			expect(hub.state).toBe("stopping");
			expect(persistence.closeCalls).toBe(0);
			releaseBridgeSave.resolve();
			await stopping;
			expect(persistence.savedShards).toHaveLength(0);
			expect(persistence.closeCalls).toBe(1);
		} finally {
			releaseBridgeSave.resolve();
			await hub.stop();
			installed.restore();
		}
	});

	test("runs reconciliation again after an earlier request settled", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence();
		const hub = createHub(persistence);
		try {
			await hub.start();
			await hub.reconcile();
			const bridge = await openBridge(installed.server, 1, true);
			sendHello(bridge);
			await bridge.waitForMessage("hub.shard.start");
			expect(hub.getTopology().assignments).toEqual([{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0 }]);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("waits for in-progress startup and keeps shutdown idempotent", async () => {
		const installed = installFakeServe();
		const migrationStarted = createDeferred<void>();
		const releaseMigration = createDeferred<void>();
		const persistence = new MemoryHubPersistence();
		persistence.migrateOperation = async () => {
			migrationStarted.resolve();
			await releaseMigration.promise;
		};
		const hub = createHub(persistence);
		try {
			const starting = hub.start();
			await migrationStarted.promise;
			const firstStop = hub.stop();
			const secondStop = hub.stop();
			expect(firstStop).toBe(secondStop);
			expect(hub.state).toBe("starting");

			releaseMigration.resolve();
			await starting;
			await firstStop;
			expect(hub.state).toBe("stopped");
			expect(hub.url).toBeNull();
			expect(persistence.closeCalls).toBe(1);
		} finally {
			releaseMigration.resolve();
			await hub.stop();
			installed.restore();
		}
	});

	test("rolls startup back once and permits idempotent shutdown after failure", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence();
		persistence.migrateOperation = () => Promise.reject(new Error("Migration failed."));
		const hub = createHub(persistence);
		try {
			await expect(hub.start()).rejects.toThrow("Migration failed.");
			expect(hub.state).toBe("failed");
			expect(persistence.closeCalls).toBe(1);
			const firstStop = hub.stop();
			const secondStop = hub.stop();
			expect(firstStop).toBe(secondStop);
			await firstStop;
			expect(hub.state).toBe("stopped");
			expect(persistence.closeCalls).toBe(1);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("waits for every startup branch before rolling persistence back", async () => {
		const installed = installFakeServe();
		const loadStarted = createDeferred<void>();
		const releaseLoad = createDeferred<void>();
		const persistence = new MemoryHubPersistence();
		persistence.loadStateOperation = async () => {
			loadStarted.resolve();
			await releaseLoad.promise;
		};
		const hub = createHub(persistence, {
			fetch: () => Promise.resolve(new Response("Invalid token", { status: 401 })),
		});
		try {
			const starting = hub.start();
			await loadStarted.promise;
			await Bun.sleep(10);
			expect(persistence.closeCalls).toBe(0);
			releaseLoad.resolve();
			await expect(starting).rejects.toThrow();
			expect(persistence.closeCalls).toBe(1);
			expect(hub.state).toBe("failed");
		} finally {
			releaseLoad.resolve();
			await hub.stop();
			installed.restore();
		}
	});

	test("preserves failures from both concurrent startup branches", async () => {
		const installed = installFakeServe();
		const releaseLoad = createDeferred<void>();
		const persistence = new MemoryHubPersistence();
		persistence.loadStateOperation = async () => {
			await releaseLoad.promise;
			throw new Error("State load failed.");
		};
		const hub = createHub(persistence, {
			fetch: () => Promise.resolve(new Response("Invalid token", { status: 401 })),
		});
		try {
			const starting = hub.start();
			releaseLoad.resolve();
			const failure: unknown = await starting.catch((cause: unknown) => cause);
			expect(failure).toBeInstanceOf(AggregateError);
			if (!(failure instanceof AggregateError)) throw new Error("Startup did not preserve both failures.");
			expect(failure.errors).toHaveLength(2);
			expect(failure.errors.map((error) => (error instanceof Error ? error.message : String(error)))).toContain(
				"State load failed.",
			);
		} finally {
			releaseLoad.resolve();
			await hub.stop();
			installed.restore();
		}
	});

	test("captures custom persistence methods without evaluating accessors", async () => {
		const persistence = new MemoryHubPersistence();
		let accessorReads = 0;
		Object.defineProperty(persistence, "migrate", {
			configurable: true,
			get: () => {
				accessorReads += 1;
				return () => Promise.resolve();
			},
		});
		expect(() => createHub(persistence)).toThrow("persistence.migrate must be a getter-free function.");
		expect(accessorReads).toBe(0);
	});

	test("keeps captured persistence methods stable after construction", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence();
		const hub = createHub(persistence);
		expect(Reflect.set(persistence, "migrate", () => Promise.reject(new Error("Mutated migration ran.")))).toBe(true);
		try {
			await hub.start();
			expect(hub.state).toBe("running");
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("rejects a custom persistence method that does not return a Promise", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence();
		expect(Reflect.set(persistence, "migrate", () => undefined)).toBe(true);
		const hub = createHub(persistence);
		try {
			await expect(hub.start()).rejects.toThrow("persistence.migrate must return a Promise.");
			expect(persistence.closeCalls).toBe(1);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("rejects a persisted assignment whose owning Bridge is missing", async () => {
		const persistence = new MemoryHubPersistence({
			assignments: [{ bridgeId: "orphaned-bridge", epoch: 1, shardId: 0, updatedAt: 1 }],
		});
		const hub = createHub(persistence);
		try {
			await expect(hub.start()).rejects.toThrow(
				"Persisted assignment shard 0 references unknown Bridge orphaned-bridge.",
			);
			expect(persistence.closeCalls).toBe(1);
		} finally {
			await hub.stop();
		}
	});

	test("serializes custom Bridge status writes across reconnects", async () => {
		const installed = installFakeServe();
		const disconnectSaveStarted = createDeferred<void>();
		const releaseDisconnectSave = createDeferred<void>();
		const persistence = new MemoryHubPersistence();
		let blockDisconnect = false;
		persistence.saveBridgeOperation = async (bridge) => {
			if (bridge.connected || !blockDisconnect) return;
			disconnectSaveStarted.resolve();
			await releaseDisconnectSave.promise;
		};
		const hub = createHub(persistence);
		try {
			await hub.start();
			const first = await openBridge(installed.server);
			await synchronizeBridge(first);
			await waitFor(() => persistence.bridges.get(BRIDGE_ID)?.connected === true, "first connected status");
			const eventStart = persistence.events.length;
			blockDisconnect = true;
			first.socket.close(1000, "Reconnect");
			await disconnectSaveStarted.promise;

			const replacement = await openBridge(installed.server, 2);
			sendHello(replacement, { connectionGeneration: 2 });
			await Bun.sleep(10);
			expect(replacement.socket.sent).toHaveLength(0);
			releaseDisconnectSave.resolve();
			await replacement.waitForMessage("hub.sync");
			expect(
				persistence.events
					.slice(eventStart)
					.filter((event) => event.startsWith("bridge:"))
					.slice(0, 4),
			).toEqual([
				"bridge:disconnected:start",
				"bridge:disconnected:end",
				"bridge:connected:start",
				"bridge:connected:end",
			]);
		} finally {
			releaseDisconnectSave.resolve();
			await hub.stop();
			installed.restore();
		}
	});
});
