import { describe, expect, test } from "bun:test";
import { BridgeAnalyticsStore } from "../../src/bridge/database/BridgeAnalyticsStore";
import { ShardingPersistenceError } from "../../src/errors/ShardingError";
import type { $AnalyticsRecord } from "../../src/types/hub";

function sample(id: string, collectedAt: number, shardId: number | null): $AnalyticsRecord {
	return {
		bridgeId: "bridge-test",
		collectedAt,
		data: { eventCount: collectedAt, ready: true },
		id,
		shardId,
	};
}

describe("BridgeAnalyticsStore", () => {
	test("retains, orders, and filters samples until explicitly cleared", async () => {
		const store = new BridgeAnalyticsStore(":memory:");
		try {
			await store.append(sample("sample-old", 1, null));
			await store.append(sample("sample-a", 10, 0));
			await store.append(sample("sample-b", 20, 1));
			await store.append(sample("sample-c", 30, 0));

			expect((await store.read()).map((record) => record.id)).toEqual([
				"sample-c",
				"sample-b",
				"sample-a",
				"sample-old",
			]);
			expect((await store.read({ limit: 1, shardId: 0 })).map((record) => record.id)).toEqual(["sample-c"]);
			expect((await store.read()).some((record) => record.id === "sample-old")).toBeTrue();
		} finally {
			await store.close();
		}
	});

	test("clears no more than the requested batch", async () => {
		const store = new BridgeAnalyticsStore(":memory:");
		try {
			for (let index = 0; index < 5; index += 1) {
				await store.append(sample(`sample-${index}`, index, 0));
			}

			expect(await store.clear(10, 2)).toBe(2);
			expect((await store.read()).map((record) => record.id)).toEqual(["sample-4", "sample-3", "sample-2"]);
			expect(await store.clear(10, 2)).toBe(2);
			expect(await store.clear(10, 2)).toBe(1);
			expect(await store.clear(10, 2)).toBe(0);
		} finally {
			await store.close();
		}
	});

	test("closes idempotently and rejects later operations", async () => {
		const store = new BridgeAnalyticsStore(":memory:");
		const firstClose = store.close();
		const secondClose = store.close();
		expect(secondClose).toBe(firstClose);
		await firstClose;
		await expect(store.read()).rejects.toBeInstanceOf(ShardingPersistenceError);
	});
});
