import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { ShardingPersistenceError } from "../../src/errors/ShardingError";
import { SQLiteHubPersistence } from "../../src/hub/database/SQLiteHubPersistence";

async function verifyPersistence(persistence: SQLiteHubPersistence, suffix: string): Promise<void> {
	const bridgeId = `bridge-${suffix}`;
	await persistence.migrate();
	await persistence.saveBridge({
		connected: true,
		generation: `generation-${suffix}`,
		id: bridgeId,
		maxShards: 2,
		updatedAt: 1_700_000_000_001,
	});
	await persistence.saveAssignment({
		bridgeId,
		epoch: 7,
		shardId: 1,
		updatedAt: 1_700_000_000_002,
	});
	await persistence.saveShard({
		assignmentEpoch: 7,
		bridgeId,
		processGeneration: 3,
		shardId: 1,
		state: "ready",
		updatedAt: 1_700_000_000_003,
	});
	for (let index = 0; index < 3; index += 1) {
		await persistence.appendAnalytics({
			bridgeId,
			collectedAt: index,
			data: { index, ready: true },
			id: `analytics-${suffix}-${index}`,
			shardId: 1,
		});
	}
	const state = await persistence.loadState();
	expect(state.assignments.find((assignment) => assignment.bridgeId === bridgeId)?.epoch).toBe(7);
	expect(state.bridges.find((bridge) => bridge.id === bridgeId)?.connected).toBeTrue();
	expect(state.shards.find((shard) => shard.bridgeId === bridgeId)?.state).toBe("ready");
	expect(await persistence.clearAnalyticsBatch(10, 2)).toBe(2);
	expect(await persistence.clearAnalyticsBatch(10, 2)).toBe(1);
}

describe("SQLiteHubPersistence", () => {
	test("supports deterministic in-memory persistence and repeated migrations", async () => {
		const persistence = new SQLiteHubPersistence(":memory:");
		try {
			const first = persistence.migrate();
			expect(persistence.migrate()).toBe(first);
			await first;
			await verifyPersistence(persistence, "unit");
		} finally {
			await persistence.close();
		}
	});

	test("reports migration failures and closes idempotently", async () => {
		const database = new Database(":memory:", { strict: true });
		const persistence = new SQLiteHubPersistence(":memory:", database, async () => {
			throw new Error("migration failed");
		});
		await expect(persistence.migrate()).rejects.toBeInstanceOf(ShardingPersistenceError);
		const firstClose = persistence.close();
		expect(persistence.close()).toBe(firstClose);
		await firstClose;
	});

	test("prevents stale topology writes from replacing newer fenced state", async () => {
		const persistence = new SQLiteHubPersistence(":memory:");
		try {
			await persistence.migrate();
			await persistence.saveBridge({
				connected: false,
				generation: "generation-new",
				id: "bridge-ordering",
				maxShards: 2,
				updatedAt: 20,
			});
			await persistence.saveBridge({
				connected: true,
				generation: "generation-old",
				id: "bridge-ordering",
				maxShards: 1,
				updatedAt: 10,
			});
			await persistence.saveAssignment({
				bridgeId: "bridge-ordering",
				epoch: 5,
				shardId: 0,
				updatedAt: 20,
			});
			await persistence.saveAssignment({
				bridgeId: "bridge-stale",
				epoch: 4,
				shardId: 0,
				updatedAt: 100,
			});
			await persistence.saveAssignment({
				bridgeId: "bridge-next",
				epoch: 6,
				shardId: 0,
				updatedAt: 1,
			});
			await persistence.saveShard({
				assignmentEpoch: 6,
				bridgeId: "bridge-next",
				processGeneration: 10,
				shardId: 0,
				state: "ready",
				updatedAt: 30,
			});
			await persistence.saveShard({
				assignmentEpoch: 6,
				bridgeId: "bridge-next",
				processGeneration: 1,
				shardId: 0,
				state: "starting",
				updatedAt: 40,
			});
			await persistence.saveShard({
				assignmentEpoch: 7,
				bridgeId: "bridge-new-owner",
				processGeneration: 1,
				shardId: 0,
				state: "starting",
				updatedAt: 1,
			});
			const state = await persistence.loadState();
			expect(state.bridges[0]?.generation).toBe("generation-new");
			expect(state.assignments[0]).toMatchObject({ bridgeId: "bridge-next", epoch: 6 });
			expect(state.shards[0]).toMatchObject({
				assignmentEpoch: 7,
				bridgeId: "bridge-new-owner",
				state: "starting",
			});
		} finally {
			await persistence.close();
		}
	});
});
