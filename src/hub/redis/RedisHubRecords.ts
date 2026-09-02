import { type } from "arktype";
import { ShardingConfigurationError, ShardingPersistenceError } from "../../errors/ShardingError";
import { snapshotConfigurationRecord } from "../../internal/configuration";
import { MAX_IDENTIFIER_LENGTH, MAX_SHARDS } from "../../internal/limits";
import { DEFAULT_PAYLOAD_POLICY, normalizePayload } from "../../internal/payload";
import type { $JsonValue } from "../../types/common";
import type { $PersistedAssignment, $PersistedBridge, $PersistedShard } from "../../types/hub";

const IDENTIFIER_PATTERN = /^[A-Za-z0-9_][A-Za-z0-9._:@/-]*$/u;
const Identifier = type("string").narrow(
	(value) => value.length > 0 && value.length <= MAX_IDENTIFIER_LENGTH && IDENTIFIER_PATTERN.test(value),
);
const SafeInteger = type("number").narrow(Number.isSafeInteger);
const NonNegativeInteger = SafeInteger.narrow((value) => value >= 0);
const PositiveInteger = SafeInteger.narrow((value) => value > 0);
const ShardId = NonNegativeInteger.narrow((value) => value < MAX_SHARDS);
const MaxShards = PositiveInteger.narrow((value) => value <= MAX_SHARDS);
const ShardState = type("'assigned' | 'starting' | 'ready' | 'stopping' | 'stopped' | 'failed'");
const JsonValue = type("unknown").narrow((value): boolean => isJsonValue(value));

const Assignment = type({
	"+": "reject",
	bridgeId: Identifier,
	epoch: PositiveInteger,
	shardId: ShardId,
	updatedAt: NonNegativeInteger,
});
const Bridge = type({
	"+": "reject",
	connected: "boolean",
	generation: Identifier,
	id: Identifier,
	maxShards: MaxShards,
	updatedAt: NonNegativeInteger,
});
const Shard = type({
	"+": "reject",
	assignmentEpoch: PositiveInteger,
	bridgeId: Identifier,
	processGeneration: PositiveInteger,
	shardId: ShardId,
	state: ShardState,
	updatedAt: NonNegativeInteger,
});
const Analytics = type({
	"+": "reject",
	bridgeId: Identifier,
	collectedAt: NonNegativeInteger,
	data: JsonValue,
	id: Identifier,
	shardId: ShardId.or("null"),
});

/** Validated analytics record paired with its stored JSON encoding. */
export interface EncodedAnalyticsRecord {
	/** Unique record identifier used as the hash field and sorted-set member. */
	readonly id: string;

	/** Collection time in Unix milliseconds, used as the sorted-set score. */
	readonly collectedAt: number;

	/** JSON encoding written to the analytics record hash. */
	readonly json: string;
}

/** Validates an assignment before a Redis write. */
export function normalizeAssignment(value: unknown): $PersistedAssignment {
	const result = Assignment(snapshotConfigurationRecord(value, "Assignment"));
	if (result instanceof type.errors) {
		throw new ShardingConfigurationError("Assignment is invalid.", { cause: result });
	}
	return Object.freeze(result);
}

/** Validates a Bridge record before a Redis write. */
export function normalizeBridge(value: unknown): $PersistedBridge {
	const result = Bridge(snapshotConfigurationRecord(value, "Bridge"));
	if (result instanceof type.errors) {
		throw new ShardingConfigurationError("Bridge is invalid.", { cause: result });
	}
	return Object.freeze(result);
}

/** Validates a shard record before a Redis write. */
export function normalizeShard(value: unknown): $PersistedShard {
	const result = Shard(snapshotConfigurationRecord(value, "Shard"));
	if (result instanceof type.errors) {
		throw new ShardingConfigurationError("Shard is invalid.", { cause: result });
	}
	return Object.freeze(result);
}

/** Validates and encodes an analytics record before a Redis write. */
export function encodeAnalyticsRecord(value: unknown): EncodedAnalyticsRecord {
	const input = snapshotConfigurationRecord(value, "Analytics record");
	const normalizedData = normalizePayload(input.data, DEFAULT_PAYLOAD_POLICY, "Analytics record data");
	const result = Analytics({
		bridgeId: input.bridgeId,
		collectedAt: input.collectedAt,
		data: normalizedData,
		id: input.id,
		shardId: input.shardId,
	});
	if (result instanceof type.errors) {
		throw new ShardingConfigurationError("Analytics record is invalid.", { cause: result });
	}
	const json = JSON.stringify(result);
	if (json === undefined) {
		throw new ShardingConfigurationError("Analytics record data could not be serialized.");
	}
	return Object.freeze({ collectedAt: result.collectedAt, id: result.id, json });
}

/** Encodes a validated record as the JSON stored in a Redis hash field. */
export function encodeRecord(value: object, name: string): string {
	const json = JSON.stringify(value);
	if (json === undefined) throw new ShardingPersistenceError(`${name} could not be serialized.`);
	return json;
}

/** Decodes and validates every assignment held in a Redis hash reply. */
export function decodeAssignments(reply: unknown): readonly $PersistedAssignment[] {
	return decodeHash(reply, "Assignments", (record) => {
		const result = Assignment(record);
		if (result instanceof type.errors) throw result;
		return Object.freeze(result);
	});
}

/** Decodes and validates every Bridge record held in a Redis hash reply. */
export function decodeBridges(reply: unknown): readonly $PersistedBridge[] {
	return decodeHash(reply, "Bridges", (record) => {
		const result = Bridge(record);
		if (result instanceof type.errors) throw result;
		return Object.freeze(result);
	});
}

/** Decodes and validates every shard record held in a Redis hash reply. */
export function decodeShards(reply: unknown): readonly $PersistedShard[] {
	return decodeHash(reply, "Shards", (record) => {
		const result = Shard(record);
		if (result instanceof type.errors) throw result;
		return Object.freeze(result);
	});
}

/** Validates a bounded non-negative integer used by a Redis operation. */
export function requireStorageInteger(value: unknown, name: string, minimum: number, maximum: number): number {
	const result = SafeInteger(value);
	if (result instanceof type.errors || result < minimum || result > maximum) {
		throw new ShardingPersistenceError(`${name} must be an integer from ${minimum} to ${maximum}.`, {
			cause: result instanceof type.errors ? result : undefined,
		});
	}
	return result;
}

function decodeHash<T>(reply: unknown, name: string, mapper: (record: unknown) => T): readonly T[] {
	const values = readHashValues(reply, name);
	const decoded: T[] = [];
	for (const [field, value] of values) {
		if (typeof value !== "string") {
			throw new ShardingPersistenceError(`${name} field ${field} is not a string.`);
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(value);
		} catch (cause) {
			throw new ShardingPersistenceError(`${name} field ${field} is not valid JSON.`, { cause });
		}
		try {
			decoded.push(mapper(snapshotConfigurationRecord(parsed, `${name} field ${field}`)));
		} catch (cause) {
			throw new ShardingPersistenceError(`${name} field ${field} is invalid.`, { cause });
		}
	}
	return Object.freeze(decoded);
}

function readHashValues(reply: unknown, name: string): readonly (readonly [string, unknown])[] {
	if (reply === null || reply === undefined) return [];
	if (Array.isArray(reply)) {
		if (reply.length % 2 !== 0) throw new ShardingPersistenceError(`${name} returned an unpaired hash reply.`);
		const entries: (readonly [string, unknown])[] = [];
		for (let index = 0; index < reply.length; index += 2) {
			const field = reply[index];
			if (typeof field !== "string") throw new ShardingPersistenceError(`${name} returned a non-string field.`);
			entries.push([field, reply[index + 1]]);
		}
		return entries;
	}
	if (typeof reply !== "object") throw new ShardingPersistenceError(`${name} returned a non-hash reply.`);
	const entries: (readonly [string, unknown])[] = [];
	for (const field of Object.keys(reply)) {
		const descriptor = Reflect.getOwnPropertyDescriptor(reply, field);
		if (descriptor === undefined || !("value" in descriptor)) {
			throw new ShardingPersistenceError(`${name} returned an inspectable-unsafe hash reply.`);
		}
		entries.push([field, descriptor.value]);
	}
	return entries;
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
