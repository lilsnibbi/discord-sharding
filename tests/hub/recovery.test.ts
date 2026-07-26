import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { ShardingConfigurationError, ShardingPersistenceError } from "../../src/errors/ShardingError";
import { SQLiteHubPersistence } from "../../src/hub/database/SQLiteHubPersistence";
import {
	mapAssignmentRows,
	mapBridgeRows,
	mapShardRows,
	normalizeAssignment,
	normalizeBridge,
	normalizeShard,
} from "../../src/hub/database/SQLiteHubRecords";
import { runSQLiteMigrations } from "../../src/hub/database/SQLiteMigrationRunner";
import { createHub, installFakeServe, openBridge, synchronizeBridge, waitFor } from "./client-harness";

describe("Hub Recovery & Persistence Diagnostics", () => {
	test("restarts Hub with 3 persisted Bridges cleanly without unnecessary stop/start commands", async () => {
		const installed = installFakeServe();
		const path = `${import.meta.dir}/recovery-3bridges-${Bun.randomUUIDv7()}.sqlite`;
		const setupStore = new SQLiteHubPersistence(path);
		try {
			await setupStore.migrate();
			await setupStore.saveBridge({
				connected: false,
				generation: "gen-a",
				id: "bridge-a",
				maxShards: 2,
				updatedAt: 100,
			});
			await setupStore.saveBridge({
				connected: false,
				generation: "gen-b",
				id: "bridge-b",
				maxShards: 2,
				updatedAt: 100,
			});
			await setupStore.saveBridge({
				connected: false,
				generation: "gen-c",
				id: "bridge-c",
				maxShards: 2,
				updatedAt: 100,
			});

			await setupStore.saveAssignment({ bridgeId: "bridge-a", epoch: 10, shardId: 0, updatedAt: 100 });
			await setupStore.saveAssignment({ bridgeId: "bridge-b", epoch: 10, shardId: 1, updatedAt: 100 });
			await setupStore.saveAssignment({ bridgeId: "bridge-c", epoch: 10, shardId: 2, updatedAt: 100 });

			await setupStore.saveShard({
				assignmentEpoch: 10,
				bridgeId: "bridge-a",
				processGeneration: 1,
				shardId: 0,
				state: "ready",
				updatedAt: 100,
			});
			await setupStore.saveShard({
				assignmentEpoch: 10,
				bridgeId: "bridge-b",
				processGeneration: 1,
				shardId: 1,
				state: "ready",
				updatedAt: 100,
			});
			await setupStore.saveShard({
				assignmentEpoch: 10,
				bridgeId: "bridge-c",
				processGeneration: 1,
				shardId: 2,
				state: "ready",
				updatedAt: 100,
			});
		} finally {
			await setupStore.close();
		}

		const hub = createHub(new SQLiteHubPersistence(path), { totalShards: 3 });
		try {
			await hub.start();
			expect(hub.getTopology().assignments.length).toBe(3);

			const bridgeA = await openBridge(installed.server, 1, false, "gen-a", "bridge-a");
			const bridgeB = await openBridge(installed.server, 1, false, "gen-b", "bridge-b");
			const bridgeC = await openBridge(installed.server, 1, false, "gen-c", "bridge-c");

			await synchronizeBridge(bridgeA, {
				bridgeGeneration: "gen-a",
				bridgeId: "bridge-a",
				runningShards: [{ assignmentEpoch: 10, processGeneration: 1, ready: true, shardId: 0 }],
			});
			await synchronizeBridge(bridgeB, {
				bridgeGeneration: "gen-b",
				bridgeId: "bridge-b",
				runningShards: [{ assignmentEpoch: 10, processGeneration: 1, ready: true, shardId: 1 }],
			});
			await synchronizeBridge(bridgeC, {
				bridgeGeneration: "gen-c",
				bridgeId: "bridge-c",
				runningShards: [{ assignmentEpoch: 10, processGeneration: 1, ready: true, shardId: 2 }],
			});

			await waitFor(() => hub.getTopology().bridges.every((b) => b.readyShardIds.length === 1), "all 3 bridges ready");

			const startCommandsA = bridgeA.socket.sent.filter((msg) => msg.includes("hub.shard.start"));
			const stopCommandsA = bridgeA.socket.sent.filter((msg) => msg.includes("hub.shard.stop"));
			expect(startCommandsA).toHaveLength(0);
			expect(stopCommandsA).toHaveLength(0);
		} finally {
			await hub.stop();
			installed.restore();
			for (const f of [path, `${path}-wal`, `${path}-shm`]) {
				const file = Bun.file(f);
				if (await file.exists()) await file.delete();
			}
		}
	});

	test("fails closed if database is empty but bridge reports retained processes", async () => {
		const installed = installFakeServe();
		const hub = createHub(new SQLiteHubPersistence(":memory:"), { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server, 1, false, "gen-a", "bridge-a");
			bridge.send("bridge.hello", "hello:fail-closed", {
				bridgeGeneration: "gen-a",
				bridgeId: "bridge-a",
				connectionGeneration: 1,
				maxShards: 2,
				restartPolicy: { initialDelayMs: 1000, maxAttempts: 5, maxDelayMs: 30000, windowMs: 60000 },
				runningShards: [{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 }],
			});
			await waitFor(() => bridge.socket.closeCode !== undefined, "socket closed on empty db with retained shards");
			expect(bridge.socket.closeCode).toBe(1002);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("detects migration checksum drift and missing migration errors", async () => {
		const db = new Database(":memory:", { strict: true });
		try {
			await runSQLiteMigrations(db);
			db.run(
				"UPDATE __sharding_migrations SET checksum = '0000000000000000000000000000000000000000000000000000000000000000' WHERE name = '20260726054837_initial'",
			);
			await expect(runSQLiteMigrations(db)).rejects.toBeInstanceOf(ShardingPersistenceError);
		} finally {
			db.close();
		}
	});

	test("validates unexpected keys and accessor properties on record objects", () => {
		expect(() => normalizeAssignment({ bridgeId: "b-1", epoch: 1, shardId: 0, updatedAt: 1, extraKey: 123 })).toThrow(
			ShardingConfigurationError,
		);
		expect(() =>
			normalizeBridge({ connected: true, generation: "g-1", id: "b-1", maxShards: 2, updatedAt: 1, extra: "foo" }),
		).toThrow(ShardingConfigurationError);
		expect(() =>
			normalizeShard({
				assignmentEpoch: 1,
				bridgeId: "b-1",
				processGeneration: 1,
				shardId: 0,
				state: "ready",
				updatedAt: 1,
				badProp: true,
			}),
		).toThrow(ShardingConfigurationError);

		const getterObj = {
			get bridgeId() {
				return "b-1";
			},
			epoch: 1,
			shardId: 0,
			updatedAt: 1,
		};
		expect(() => normalizeAssignment(getterObj)).toThrow(ShardingConfigurationError);
	});

	test("rejects malformed row data in query mappers", () => {
		expect(() => mapAssignmentRows("not-an-array")).toThrow(ShardingPersistenceError);
		expect(() => mapBridgeRows([{ id: "b-1" }])).toThrow(ShardingPersistenceError);
		expect(() => mapShardRows([{ shard_id: "not-a-number" }])).toThrow(ShardingPersistenceError);
	});

	test("validates released assignment tombstones with leading underscores", () => {
		const tombstone = normalizeAssignment({
			bridgeId: "__released__:0",
			epoch: 1,
			shardId: 0,
			updatedAt: 10,
		});
		expect(tombstone.bridgeId).toBe("__released__:0");
	});

	test("releases disconnected Bridge assignments and persists tombstones cleanly to SQLite", async () => {
		const persistence = new SQLiteHubPersistence(":memory:");
		await persistence.migrate();
		await persistence.saveBridge({
			connected: false,
			generation: "gen-old",
			id: "bridge-stale",
			maxShards: 2,
			updatedAt: 100,
		});
		await persistence.saveAssignment({
			bridgeId: "bridge-stale",
			epoch: 1,
			shardId: 0,
			updatedAt: 100,
		});
		const hub = createHub(persistence, { totalShards: 1 });
		try {
			await hub.start();
			const released = await hub.releaseBridge("bridge-stale");
			expect(released).toEqual([0]);
			const state = await persistence.loadState();
			expect(state.assignments[0]?.bridgeId).toBe("__released__:0");
		} finally {
			await hub.stop();
		}
	});
});
