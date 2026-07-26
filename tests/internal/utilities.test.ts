import { describe, expect, test } from "bun:test";
import {
	ShardingCapacityError,
	ShardingConfigurationError,
	ShardingRemoteError,
	ShardingStateError,
} from "../../src/errors/ShardingError";
import {
	assertConfigurationKeys,
	snapshotConfigurationArray,
	snapshotConfigurationRecord,
} from "../../src/internal/configuration";
import { remoteError, serializeError } from "../../src/internal/errors";
import { ListenerSet } from "../../src/internal/listeners";
import {
	normalizeReconnectPolicy,
	normalizeRequestPolicy,
	normalizeRestartPolicy,
	reconnectDelay,
} from "../../src/internal/policies";
import { abortableSleep } from "../../src/internal/sleep";
import {
	hasUnpairedSurrogate,
	requireBoundedString,
	requireIdentifier,
	requireToken,
} from "../../src/internal/validation";

describe("internal configuration and lifecycle utilities", () => {
	test("snapshots getter-free records and dense arrays", () => {
		const record = snapshotConfigurationRecord({ enabled: true }, "options");
		const array = snapshotConfigurationArray(["one", "two"], "values", 2);

		expect(record).toEqual({ enabled: true });
		expect(array).toEqual(["one", "two"]);
		expect(Object.isFrozen(record)).toBe(true);
		expect(Object.isFrozen(array)).toBe(true);
		expect(() => assertConfigurationKeys(record, new Set(["enabled"]), "options")).not.toThrow();
		expect(() => assertConfigurationKeys(record, new Set(), "options")).toThrow(ShardingConfigurationError);
	});

	test("rejects accessors and sparse configuration without invoking code", () => {
		let reads = 0;
		const record = {};
		Object.defineProperty(record, "value", {
			enumerable: true,
			get: () => {
				reads += 1;
				return true;
			},
		});

		expect(() => snapshotConfigurationRecord(record, "options")).toThrow(ShardingConfigurationError);
		expect(() => snapshotConfigurationArray(new Array(1), "values", 1)).toThrow(ShardingConfigurationError);
		expect(reads).toBe(0);
	});

	test("bounds listener ownership and supports idempotent removal", () => {
		const listeners = new ListenerSet<() => void>(1);
		let calls = 0;
		const listener = (): void => {
			calls += 1;
		};
		const remove = listeners.add(listener);

		expect(() => listeners.add(() => undefined)).toThrow(ShardingCapacityError);
		listeners.forEach((current) => {
			current();
		});
		expect(calls).toBe(1);
		remove();
		remove();
		expect(listeners.size).toBe(0);
		listeners.clear();
	});

	test("normalizes policies and calculates deterministic bounded reconnect delay", () => {
		const request = normalizeRequestPolicy({ maxPending: 4, timeoutMs: 200 });
		const reconnect = normalizeReconnectPolicy({
			initialDelayMs: 200,
			jitterRatio: 0.5,
			maxDelayMs: 2_000,
			multiplier: 2,
		});
		const restart = normalizeRestartPolicy({
			initialDelayMs: 100,
			maxAttempts: 3,
			maxDelayMs: 1_000,
			windowMs: 5_000,
		});

		expect(request).toEqual({ maxPending: 4, timeoutMs: 200 });
		expect(reconnectDelay(reconnect, 1, () => 0.5)).toBe(400);
		expect(restart.maxAttempts).toBe(3);
		expect(() => reconnectDelay(reconnect, 0, () => 1)).toThrow(ShardingConfigurationError);
	});

	test("serializes safe domain errors and recreates remote failures", () => {
		const serialized = serializeError(new ShardingRemoteError("REMOTE_CODE", "remote\u0000 message"));
		const recreated = remoteError(serialized);

		expect(serialized).toEqual({
			code: "REMOTE_CODE",
			message: "remote message",
			name: "ShardingRemoteError",
		});
		expect(recreated).toBeInstanceOf(ShardingRemoteError);
		expect(recreated.remoteCode).toBe("REMOTE_CODE");
		expect(recreated.cause).toBeInstanceOf(Error);
	});

	test("validates identifiers, tokens, and Unicode strings", () => {
		expect(requireIdentifier("bridge:west-1", "id")).toBe("bridge:west-1");
		expect(requireToken("0123456789abcdef", "token")).toBe("0123456789abcdef");
		expect(requireBoundedString("😀", "value", 2)).toBe("😀");
		expect(hasUnpairedSurrogate("\ud800")).toBe(true);
		expect(() => requireIdentifier("unsafe value", "id")).toThrow(ShardingConfigurationError);
		expect(() => requireToken("short", "token")).toThrow(ShardingConfigurationError);
		expect(() => requireBoundedString("\ud800", "value")).toThrow(ShardingConfigurationError);
	});

	test("aborts sleep promptly and retains the cancellation reason", async () => {
		const controller = new AbortController();
		const reason = new ShardingStateError("cancelled");
		controller.abort(reason);

		await expect(abortableSleep(1_000, controller.signal)).rejects.toBe(reason);
		await expect(abortableSleep(-1, new AbortController().signal)).rejects.toBeInstanceOf(ShardingConfigurationError);
	});
});
