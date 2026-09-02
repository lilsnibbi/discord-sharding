import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import { ShardingPersistenceError } from "../../src/errors/ShardingError";
import { RedisHubPersistence } from "../../src/hub/redis/RedisHubPersistence";

const url = process.env.SHARDING_REDIS_URL;
const PREFIX = `sharding-test:${Bun.randomUUIDv7()}`;

describe.skipIf(url === undefined || url.length === 0)("RedisHubPersistence (live Redis)", () => {
	let store: RedisHubPersistence;

	beforeAll(async () => {
		if (url === undefined) throw new Error("SHARDING_REDIS_URL is required.");
		store = new RedisHubPersistence(url, PREFIX);
		await store.migrate();
	});

	afterAll(async () => {
		if (url === undefined) return;
		await store.close();
		const cleanup = new RedisClient(url);
		try {
			for (const suffix of ["assignments", "bridges", "shards", "schema", "analytics:index", "analytics:records"]) {
				await cleanup.send("DEL", [`${PREFIX}:${suffix}`]);
			}
		} finally {
			cleanup.close();
		}
	});

	test("runs the real Lua guards for assignments, Bridges, and shards", async () => {
		await store.saveAssignment({ bridgeId: "bridge-a", epoch: 5, shardId: 0, updatedAt: 200 });
		await store.saveAssignment({ bridgeId: "bridge-stale", epoch: 4, shardId: 0, updatedAt: 900 });
		await store.saveAssignment({ bridgeId: "bridge-equal", epoch: 5, shardId: 0, updatedAt: 200 });
		await store.saveBridge({ connected: true, generation: "gen-a", id: "bridge-a", maxShards: 2, updatedAt: 200 });
		await store.saveBridge({ connected: false, generation: "gen-old", id: "bridge-a", maxShards: 9, updatedAt: 199 });
		await store.saveShard({
			assignmentEpoch: 5,
			bridgeId: "bridge-a",
			processGeneration: 1,
			shardId: 0,
			state: "ready",
			updatedAt: 200,
		});
		await store.saveShard({
			assignmentEpoch: 4,
			bridgeId: "bridge-a",
			processGeneration: 1,
			shardId: 0,
			state: "failed",
			updatedAt: 999,
		});

		const state = await store.loadState();
		expect(state.assignments).toEqual([{ bridgeId: "bridge-a", epoch: 5, shardId: 0, updatedAt: 200 }]);
		expect(state.bridges[0]?.generation).toBe("gen-a");
		expect(state.shards[0]?.state).toBe("ready");
	});

	test("appends analytics, rejects duplicates, and clears oldest-first in batches", async () => {
		for (let index = 0; index < 5; index += 1) {
			await store.appendAnalytics({
				bridgeId: "bridge-a",
				collectedAt: index * 10,
				data: { index },
				id: `${PREFIX}-rec-${index}`,
				shardId: index % 2 === 0 ? 0 : null,
			});
		}
		await expect(
			store.appendAnalytics({
				bridgeId: "bridge-a",
				collectedAt: 0,
				data: { index: 0 },
				id: `${PREFIX}-rec-0`,
				shardId: 0,
			}),
		).rejects.toThrow(ShardingPersistenceError);
		await expect(store.clearAnalyticsBatch(25, 2)).resolves.toBe(2);
		await expect(store.clearAnalyticsBatch(25, 10)).resolves.toBe(1);
		await expect(store.clearAnalyticsBatch(25, 10)).resolves.toBe(0);
		await expect(store.clearAnalyticsBatch(100, 10)).resolves.toBe(2);
	});

	test("recovers from a flushed server-side script cache", async () => {
		if (url === undefined) throw new Error("SHARDING_REDIS_URL is required.");
		const admin = new RedisClient(url);
		try {
			await admin.send("SCRIPT", ["FLUSH"]);
		} finally {
			admin.close();
		}
		await store.saveAssignment({ bridgeId: "bridge-a", epoch: 6, shardId: 1, updatedAt: 300 });
		const state = await store.loadState();
		expect(state.assignments.some((entry) => entry.shardId === 1 && entry.epoch === 6)).toBe(true);
	});
});
