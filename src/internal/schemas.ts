import { type } from "arktype";
import { ShardingConfigurationError } from "../errors/ShardingError";
import type { $JsonValue } from "../types/common";
import { snapshotConfigurationRecord } from "./configuration";
import { MAX_IDENTIFIER_LENGTH, MAX_SHARDS } from "./limits";

const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:@/-]*$/u;

/** ArkType schema for safe string identifiers. */
export const IdentifierSchema = type("string").narrow(
	(value) => value.length > 0 && value.length <= MAX_IDENTIFIER_LENGTH && IDENTIFIER_PATTERN.test(value),
);

/** ArkType schema for safe integers. */
export const SafeIntegerSchema = type("number").narrow(Number.isSafeInteger);

/** ArkType schema for non-negative integers. */
export const NonNegativeIntegerSchema = SafeIntegerSchema.narrow((value) => value >= 0);

/** ArkType schema for positive integers. */
export const PositiveIntegerSchema = SafeIntegerSchema.narrow((value) => value > 0);

/** ArkType schema for valid shard IDs (0 to MAX_SHARDS - 1). */
export const ShardIdSchema = NonNegativeIntegerSchema.narrow((value) => value < MAX_SHARDS);

/** ArkType schema for max shard counts (1 to MAX_SHARDS). */
export const MaxShardsSchema = PositiveIntegerSchema.narrow((value) => value <= MAX_SHARDS);

/** ArkType schema for recursive JSON-compatible values. */
export const JsonValueSchema = type("unknown").narrow((value): boolean => isJsonValue(value));

/** ArkType schema for payload policies with exact keys. */
export const PayloadPolicySchema = type({
	"+": "reject",
	maxBytes: PositiveIntegerSchema,
	maxDepth: PositiveIntegerSchema,
	maxNodes: PositiveIntegerSchema,
});

/** ArkType schema for request policies with exact keys. */
export const RequestPolicySchema = type({
	"+": "reject",
	maxPending: PositiveIntegerSchema,
	timeoutMs: PositiveIntegerSchema,
});

/** ArkType schema for reconnect policies with exact keys. */
export const ReconnectPolicySchema = type({
	"+": "reject",
	initialDelayMs: PositiveIntegerSchema,
	jitterRatio: type("number").narrow((val) => val >= 0 && val <= 1),
	maxDelayMs: PositiveIntegerSchema,
	multiplier: type("number").narrow((val) => val >= 1),
});

/** ArkType schema for restart policies with exact keys. */
export const RestartPolicySchema = type({
	"+": "reject",
	initialDelayMs: PositiveIntegerSchema,
	maxAttempts: NonNegativeIntegerSchema,
	maxDelayMs: PositiveIntegerSchema,
	windowMs: PositiveIntegerSchema,
});

/** Validates that a value matches an ArkType schema, wrapping errors in ShardingConfigurationError. */
export function validateWithSchema<T>(schema: type.Any, value: unknown, name: string): T {
	const snapshot = snapshotConfigurationRecord(value, name);
	const result = schema(snapshot);
	if (result instanceof type.errors) {
		throw new ShardingConfigurationError(`${name} is invalid.`, { cause: result });
	}
	return Object.freeze(result as T);
}

function isJsonValue(value: unknown): value is $JsonValue {
	if (value === null || typeof value === "boolean" || typeof value === "string") return true;
	if (typeof value === "number") return Number.isFinite(value);
	if (Array.isArray(value)) return value.every(isJsonValue);
	if (typeof value !== "object") return false;
	for (const key of Object.keys(value)) {
		const descriptor = Reflect.getOwnPropertyDescriptor(value, key);
		if (descriptor === undefined || !("value" in descriptor) || !isJsonValue(descriptor.value)) return false;
	}
	return true;
}
