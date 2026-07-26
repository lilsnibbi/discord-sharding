import { describe, expect, test } from "bun:test";
import { ShardingCapacityError, ShardingStateError, ShardingTimeoutError } from "../../src/errors/ShardingError";
import { RequestRegistry } from "../../src/internal/RequestRegistry";

describe("RequestRegistry", () => {
	test("settles and rejects registered requests while releasing capacity", async () => {
		const registry = new RequestRegistry<number>(1_000, 2);
		const resolved = registry.register("request:1");
		const rejected = registry.register("request:2");
		const failure = new Error("rejected");

		expect(registry.size).toBe(2);
		expect(registry.settle("request:1", 42)).toBe(true);
		expect(registry.reject("request:2", failure)).toBe(true);
		expect(await resolved).toBe(42);
		await expect(rejected).rejects.toBe(failure);
		expect(registry.size).toBe(0);
		expect(registry.settle("missing", 0)).toBe(false);
	});

	test("enforces unique identifiers and bounded pending capacity", async () => {
		const registry = new RequestRegistry<void>(1_000, 1);
		const pending = registry.register("request:1");

		expect(() => registry.register("request:1")).toThrow(ShardingCapacityError);
		expect(() => registry.register("request:2")).toThrow(ShardingCapacityError);
		registry.rejectAll();
		await expect(pending).rejects.toBeInstanceOf(ShardingStateError);
		expect(registry.size).toBe(0);
	});

	test("expires requests and removes their timers from retained state", async () => {
		const registry = new RequestRegistry<void>(5, 1);
		const pending = registry.register("request:timeout");

		await expect(pending).rejects.toBeInstanceOf(ShardingTimeoutError);
		expect(registry.size).toBe(0);
	});

	test("rejectAll is idempotent and preserves a caller-owned reason", async () => {
		const registry = new RequestRegistry<void>(1_000, 2);
		const first = registry.register("request:1");
		const second = registry.register("request:2");
		const reason = new ShardingStateError("shutdown");

		registry.rejectAll(reason);
		registry.rejectAll(reason);
		await expect(first).rejects.toBe(reason);
		await expect(second).rejects.toBe(reason);
		expect(registry.size).toBe(0);
	});
});
