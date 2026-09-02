import { describe, expect, test } from "bun:test";
import {
	ShardingConfigurationError,
	ShardingPersistenceError,
	ShardingProtocolError,
} from "../../src/errors/ShardingError";
import { RedisHubPersistence } from "../../src/hub/redis/RedisHubPersistence";
import { encodeAnalyticsRecord } from "../../src/hub/redis/RedisHubRecords";
import { FakeRedisClient } from "../utilities/fake-redis";

const PREFIX = "test";

async function openPersistence(client = new FakeRedisClient()): Promise<{
	readonly client: FakeRedisClient;
	readonly store: RedisHubPersistence;
}> {
	const store = new RedisHubPersistence("redis://127.0.0.1:6379", PREFIX, client);
	await store.migrate();
	return { client, store };
}

describe("RedisHubPersistence", () => {
	test("rejects malformed connection URLs and key prefixes", () => {
		expect(() => new RedisHubPersistence("", PREFIX)).toThrow(ShardingConfigurationError);
		expect(() => new RedisHubPersistence("http://127.0.0.1:6379", PREFIX)).toThrow(ShardingConfigurationError);
		expect(() => new RedisHubPersistence("not a url", PREFIX)).toThrow(ShardingConfigurationError);
		expect(() => new RedisHubPersistence("redis://127.0.0.1:6379", "")).toThrow(ShardingConfigurationError);
		expect(() => new RedisHubPersistence("redis://127.0.0.1:6379", "bad prefix")).toThrow(ShardingConfigurationError);
		expect(() => new RedisHubPersistence("redis://127.0.0.1:6379", "-leading")).toThrow(ShardingConfigurationError);
	});

	test("stamps the schema version once and tolerates a matching stamp", async () => {
		const { client, store } = await openPersistence();
		expect(client.strings.get(`${PREFIX}:schema`)).toBe("1");
		await store.migrate();
		const reopened = new RedisHubPersistence("redis://127.0.0.1:6379", PREFIX, client);
		await reopened.migrate();
		expect(client.strings.get(`${PREFIX}:schema`)).toBe("1");
	});

	test("refuses to start against an unsupported stored schema version", async () => {
		const client = new FakeRedisClient();
		client.strings.set(`${PREFIX}:schema`, "99");
		const store = new RedisHubPersistence("redis://127.0.0.1:6379", PREFIX, client);
		await expect(store.migrate()).rejects.toThrow(ShardingPersistenceError);
	});

	test("returns an empty snapshot before anything is written", async () => {
		const { store } = await openPersistence();
		await expect(store.loadState()).resolves.toEqual({ assignments: [], bridges: [], shards: [] });
	});

	test("round-trips assignments, Bridges, and shards in stable order", async () => {
		const { store } = await openPersistence();
		await store.saveAssignment({ bridgeId: "bridge-b", epoch: 2, shardId: 1, updatedAt: 100 });
		await store.saveAssignment({ bridgeId: "bridge-a", epoch: 1, shardId: 0, updatedAt: 100 });
		await store.saveBridge({ connected: true, generation: "gen-b", id: "bridge-b", maxShards: 2, updatedAt: 100 });
		await store.saveBridge({ connected: false, generation: "gen-a", id: "bridge-a", maxShards: 4, updatedAt: 100 });
		await store.saveShard({
			assignmentEpoch: 2,
			bridgeId: "bridge-b",
			processGeneration: 1,
			shardId: 1,
			state: "ready",
			updatedAt: 100,
		});
		const state = await store.loadState();
		expect(state.assignments.map((entry) => entry.shardId)).toEqual([0, 1]);
		expect(state.bridges.map((entry) => entry.id)).toEqual(["bridge-a", "bridge-b"]);
		expect(state.bridges[0]?.connected).toBe(false);
		expect(state.bridges[0]?.maxShards).toBe(4);
		expect(state.shards).toEqual([
			{
				assignmentEpoch: 2,
				bridgeId: "bridge-b",
				processGeneration: 1,
				shardId: 1,
				state: "ready",
				updatedAt: 100,
			},
		]);
	});

	test("keeps the newest assignment and discards stale epochs and timestamps", async () => {
		const { store } = await openPersistence();
		await store.saveAssignment({ bridgeId: "bridge-a", epoch: 5, shardId: 0, updatedAt: 200 });
		await store.saveAssignment({ bridgeId: "bridge-stale", epoch: 4, shardId: 0, updatedAt: 900 });
		await store.saveAssignment({ bridgeId: "bridge-same-epoch", epoch: 5, shardId: 0, updatedAt: 200 });
		await store.saveAssignment({ bridgeId: "bridge-older", epoch: 5, shardId: 0, updatedAt: 150 });
		let state = await store.loadState();
		expect(state.assignments[0]?.bridgeId).toBe("bridge-a");
		await store.saveAssignment({ bridgeId: "bridge-newer", epoch: 5, shardId: 0, updatedAt: 201 });
		state = await store.loadState();
		expect(state.assignments[0]?.bridgeId).toBe("bridge-newer");
		await store.saveAssignment({ bridgeId: "bridge-next-epoch", epoch: 6, shardId: 0, updatedAt: 1 });
		state = await store.loadState();
		expect(state.assignments[0]).toEqual({ bridgeId: "bridge-next-epoch", epoch: 6, shardId: 0, updatedAt: 1 });
	});

	test("keeps the newest Bridge record by timestamp", async () => {
		const { store } = await openPersistence();
		await store.saveBridge({ connected: true, generation: "gen-a", id: "bridge-a", maxShards: 2, updatedAt: 200 });
		await store.saveBridge({ connected: false, generation: "gen-old", id: "bridge-a", maxShards: 8, updatedAt: 200 });
		await store.saveBridge({ connected: false, generation: "gen-older", id: "bridge-a", maxShards: 8, updatedAt: 10 });
		let state = await store.loadState();
		expect(state.bridges[0]?.generation).toBe("gen-a");
		await store.saveBridge({ connected: false, generation: "gen-new", id: "bridge-a", maxShards: 8, updatedAt: 201 });
		state = await store.loadState();
		expect(state.bridges[0]?.generation).toBe("gen-new");
	});

	test("keeps the newest shard record by assignment epoch then timestamp", async () => {
		const { store } = await openPersistence();
		const base = { bridgeId: "bridge-a", processGeneration: 1, shardId: 0 } as const;
		await store.saveShard({ ...base, assignmentEpoch: 5, state: "ready", updatedAt: 200 });
		await store.saveShard({ ...base, assignmentEpoch: 4, state: "failed", updatedAt: 900 });
		await store.saveShard({ ...base, assignmentEpoch: 5, state: "failed", updatedAt: 200 });
		let state = await store.loadState();
		expect(state.shards[0]?.state).toBe("ready");
		await store.saveShard({ ...base, assignmentEpoch: 5, state: "stopping", updatedAt: 201 });
		state = await store.loadState();
		expect(state.shards[0]?.state).toBe("stopping");
	});

	test("rejects records that fail validation before touching Redis", async () => {
		const { client, store } = await openPersistence();
		const before = client.commands.length;
		await expect(store.saveAssignment({ bridgeId: "", epoch: 1, shardId: 0, updatedAt: 1 })).rejects.toThrow(
			ShardingConfigurationError,
		);
		await expect(store.saveAssignment({ bridgeId: "b", epoch: 0, shardId: 0, updatedAt: 1 })).rejects.toThrow(
			ShardingConfigurationError,
		);
		await expect(
			store.saveBridge({ connected: true, generation: "g", id: "b", maxShards: 0, updatedAt: 1 }),
		).rejects.toThrow(ShardingConfigurationError);
		await expect(
			store.saveShard({
				assignmentEpoch: 1,
				bridgeId: "b",
				processGeneration: 1,
				shardId: -1,
				state: "ready",
				updatedAt: 1,
			}),
		).rejects.toThrow(ShardingConfigurationError);
		expect(client.commands.length).toBe(before);
	});

	test("appends analytics and rejects a duplicate identifier", async () => {
		const { client, store } = await openPersistence();
		const record = { bridgeId: "bridge-a", collectedAt: 10, data: { cpu: 1 }, id: "rec-1", shardId: 0 };
		await store.appendAnalytics(record);
		expect(client.zsets.get(`${PREFIX}:analytics:index`)?.get("rec-1")).toBe(10);
		expect(client.hashes.get(`${PREFIX}:analytics:records`)?.size).toBe(1);
		await expect(store.appendAnalytics(record)).rejects.toThrow(ShardingPersistenceError);
	});

	test("rejects analytics payloads that are not JSON-compatible", () => {
		expect(() =>
			encodeAnalyticsRecord({ bridgeId: "b", collectedAt: 1, data: { at: () => 1 }, id: "rec-x", shardId: null }),
		).toThrow(ShardingProtocolError);
		expect(() =>
			encodeAnalyticsRecord({ bridgeId: "", collectedAt: 1, data: null, id: "rec-x", shardId: null }),
		).toThrow(ShardingConfigurationError);
	});

	test("clears the oldest analytics first within the requested batch size", async () => {
		const { client, store } = await openPersistence();
		for (let index = 0; index < 5; index += 1) {
			await store.appendAnalytics({
				bridgeId: "bridge-a",
				collectedAt: index * 10,
				data: { index },
				id: `rec-${index}`,
				shardId: null,
			});
		}
		await expect(store.clearAnalyticsBatch(25, 2)).resolves.toBe(2);
		expect([...(client.hashes.get(`${PREFIX}:analytics:records`)?.keys() ?? [])]).toEqual(["rec-2", "rec-3", "rec-4"]);
		await expect(store.clearAnalyticsBatch(25, 10)).resolves.toBe(1);
		await expect(store.clearAnalyticsBatch(25, 10)).resolves.toBe(0);
		expect(client.zsets.get(`${PREFIX}:analytics:index`)?.size).toBe(2);
	});

	test("validates the analytics clear bounds", async () => {
		const { store } = await openPersistence();
		await expect(store.clearAnalyticsBatch(-1, 10)).rejects.toThrow(ShardingPersistenceError);
		await expect(store.clearAnalyticsBatch(10, 0)).rejects.toThrow(ShardingPersistenceError);
		await expect(store.clearAnalyticsBatch(10, 10_001)).rejects.toThrow(ShardingPersistenceError);
	});

	test("falls back to EVAL and re-caches the digest after NOSCRIPT", async () => {
		const { client, store } = await openPersistence();
		client.flushScripts();
		await store.saveAssignment({ bridgeId: "bridge-a", epoch: 1, shardId: 0, updatedAt: 1 });
		expect(client.commands).toContain("EVAL");
		const evalCalls = client.commands.filter((command) => command === "EVAL").length;
		await store.saveAssignment({ bridgeId: "bridge-a", epoch: 2, shardId: 0, updatedAt: 2 });
		expect(client.commands.filter((command) => command === "EVAL").length).toBe(evalCalls);
		const state = await store.loadState();
		expect(state.assignments[0]?.epoch).toBe(2);
	});

	test("surfaces corrupt stored records as persistence failures", async () => {
		const { client, store } = await openPersistence();
		client.hashes.set(`${PREFIX}:assignments`, new Map([["0", "{not json"]]));
		await expect(store.loadState()).rejects.toThrow(ShardingPersistenceError);
		client.hashes.set(`${PREFIX}:assignments`, new Map([["0", JSON.stringify({ shardId: 0 })]]));
		await expect(store.loadState()).rejects.toThrow(ShardingPersistenceError);
	});

	test("closes once and refuses later operations", async () => {
		const { client, store } = await openPersistence();
		await store.close();
		await store.close();
		expect(client.closed).toBe(true);
		expect(() => store.migrate()).toThrow(ShardingPersistenceError);
		await expect(store.loadState()).rejects.toThrow(ShardingPersistenceError);
		await expect(store.saveAssignment({ bridgeId: "b", epoch: 1, shardId: 0, updatedAt: 1 })).rejects.toThrow(
			ShardingPersistenceError,
		);
	});
});
