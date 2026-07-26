import { describe, expect, test } from "bun:test";
import { ShardingConfigurationError, ShardingProtocolError } from "../../src/errors/ShardingError";
import { DEFAULT_PAYLOAD_POLICY, normalizePayload, normalizePayloadPolicy } from "../../src/internal/payload";

describe("payload normalization", () => {
	test("creates immutable getter-free JSON snapshots without mutating input", () => {
		const input = {
			enabled: true,
			nested: {
				items: ["one", 2, null],
			},
		};
		const normalized = normalizePayload(input, DEFAULT_PAYLOAD_POLICY);

		expect(normalized).toEqual(input);
		expect(normalized).not.toBe(input);
		expect(Object.isFrozen(normalized)).toBe(true);
		if (typeof normalized !== "object" || normalized === null || Array.isArray(normalized)) {
			throw new Error("Expected a normalized record.");
		}
		const nested = Reflect.get(normalized, "nested");
		if (typeof nested !== "object" || nested === null) {
			throw new Error("Expected a normalized nested record.");
		}
		expect(Object.isFrozen(nested)).toBe(true);
		expect(input).toEqual({
			enabled: true,
			nested: {
				items: ["one", 2, null],
			},
		});
	});

	test("enforces aggregate UTF-8 byte, depth, and node limits", () => {
		expect(() => normalizePayload("é", { maxBytes: 3, maxDepth: 2, maxNodes: 2 })).toThrow(ShardingProtocolError);
		expect(() => normalizePayload({ nested: { tooDeep: true } }, { maxBytes: 128, maxDepth: 2, maxNodes: 8 })).toThrow(
			ShardingProtocolError,
		);
		expect(() => normalizePayload([1, 2, 3], { maxBytes: 128, maxDepth: 2, maxNodes: 3 })).toThrow(
			ShardingProtocolError,
		);
	});

	test("rejects cycles, sparse arrays, named array properties, and custom prototypes", () => {
		const cyclic: { self?: unknown } = {};
		cyclic.self = cyclic;
		expect(() => normalizePayload(cyclic, DEFAULT_PAYLOAD_POLICY)).toThrow(ShardingProtocolError);

		expect(() => normalizePayload(new Array(1), DEFAULT_PAYLOAD_POLICY)).toThrow(ShardingProtocolError);
		const named = [1];
		Object.defineProperty(named, "label", { enumerable: true, value: "unsafe" });
		expect(() => normalizePayload(named, DEFAULT_PAYLOAD_POLICY)).toThrow(ShardingProtocolError);

		class CustomPayload {
			public readonly value = 1;
		}
		expect(() => normalizePayload(new CustomPayload(), DEFAULT_PAYLOAD_POLICY)).toThrow(ShardingProtocolError);
	});

	test("does not invoke accessors while rejecting them", () => {
		let reads = 0;
		const input = {};
		Object.defineProperty(input, "value", {
			enumerable: true,
			get: () => {
				reads += 1;
				return "unsafe";
			},
		});

		expect(() => normalizePayload(input, DEFAULT_PAYLOAD_POLICY)).toThrow(ShardingProtocolError);
		expect(reads).toBe(0);
	});

	test("normalizes policy defaults and rejects unknown or unsafe overrides", () => {
		expect(normalizePayloadPolicy({ maxBytes: 64 })).toEqual({
			maxBytes: 64,
			maxDepth: 32,
			maxNodes: 10_000,
		});
		expect(Object.isFrozen(normalizePayloadPolicy(undefined))).toBe(true);
		expect(() => normalizePayloadPolicy({ maxDepth: 0 })).toThrow(ShardingConfigurationError);
		const unsafePolicy = {
			maxBytes: 64,
			unexpected: 1,
		};
		expect(() => normalizePayloadPolicy(unsafePolicy)).toThrow(ShardingConfigurationError);
	});
});
