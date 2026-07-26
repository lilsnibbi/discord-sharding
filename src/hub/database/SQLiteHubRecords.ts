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
const AssignmentRow = type({
	"+": "reject",
	bridge_id: Identifier,
	epoch: PositiveInteger,
	shard_id: ShardId,
	updated_at: NonNegativeInteger,
});
const BridgeRow = type({
	"+": "reject",
	connected: "0 | 1",
	generation: Identifier,
	id: Identifier,
	max_shards: MaxShards,
	updated_at: NonNegativeInteger,
});
const ShardRow = type({
	"+": "reject",
	assignment_epoch: PositiveInteger,
	bridge_id: Identifier,
	process_generation: PositiveInteger,
	shard_id: ShardId,
	state: ShardState,
	updated_at: NonNegativeInteger,
});

interface NormalizedAnalyticsRecord {
	readonly bridgeId: string;
	readonly collectedAt: number;
	readonly dataJson: string;
	readonly id: string;
	readonly shardId: number | null;
}

/** Validates an assignment before a SQLite write. */
export function normalizeAssignment(value: unknown): $PersistedAssignment {
	const result = Assignment(snapshotConfigurationRecord(value, "Assignment"));
	if (result instanceof type.errors) {
		throw new ShardingConfigurationError("Assignment is invalid.", { cause: result });
	}
	return Object.freeze(result);
}

/** Validates a Bridge record before a SQLite write. */
export function normalizeBridge(value: unknown): $PersistedBridge {
	const result = Bridge(snapshotConfigurationRecord(value, "Bridge"));
	if (result instanceof type.errors) {
		throw new ShardingConfigurationError("Bridge is invalid.", { cause: result });
	}
	return Object.freeze(result);
}

/** Validates a shard record before a SQLite write. */
export function normalizeShard(value: unknown): $PersistedShard {
	const result = Shard(snapshotConfigurationRecord(value, "Shard"));
	if (result instanceof type.errors) {
		throw new ShardingConfigurationError("Shard is invalid.", { cause: result });
	}
	return Object.freeze(result);
}

/** Validates and serializes an analytics record before a SQLite write. */
export function normalizeAnalyticsRecord(value: unknown): NormalizedAnalyticsRecord {
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
	const dataJson = JSON.stringify(result.data);
	if (dataJson === undefined) {
		throw new ShardingConfigurationError("Analytics record data could not be serialized.");
	}
	return Object.freeze({
		bridgeId: result.bridgeId,
		collectedAt: result.collectedAt,
		dataJson,
		id: result.id,
		shardId: result.shardId,
	});
}

/** Maps and validates assignment rows returned by SQLite. */
export function mapAssignmentRows(value: unknown): readonly $PersistedAssignment[] {
	return mapRows(value, "Assignments query", (row) => {
		const result = AssignmentRow(row);
		if (result instanceof type.errors) throw result;
		return Object.freeze({
			bridgeId: result.bridge_id,
			epoch: result.epoch,
			shardId: result.shard_id,
			updatedAt: result.updated_at,
		});
	});
}

/** Maps and validates Bridge rows returned by SQLite. */
export function mapBridgeRows(value: unknown): readonly $PersistedBridge[] {
	return mapRows(value, "Bridges query", (row) => {
		const result = BridgeRow(row);
		if (result instanceof type.errors) throw result;
		return Object.freeze({
			connected: result.connected === 1,
			generation: result.generation,
			id: result.id,
			maxShards: result.max_shards,
			updatedAt: result.updated_at,
		});
	});
}

/** Maps and validates shard rows returned by SQLite. */
export function mapShardRows(value: unknown): readonly $PersistedShard[] {
	return mapRows(value, "Shards query", (row) => {
		const result = ShardRow(row);
		if (result instanceof type.errors) throw result;
		return Object.freeze({
			assignmentEpoch: result.assignment_epoch,
			bridgeId: result.bridge_id,
			processGeneration: result.process_generation,
			shardId: result.shard_id,
			state: result.state,
			updatedAt: result.updated_at,
		});
	});
}

/** Validates a bounded non-negative integer used by a SQLite operation. */
export function requireDatabaseInteger(value: unknown, name: string, minimum: number, maximum: number): number {
	const result = SafeInteger(value);
	if (result instanceof type.errors || result < minimum || result > maximum) {
		throw new ShardingPersistenceError(`${name} must be an integer from ${minimum} to ${maximum}.`, {
			cause: result instanceof type.errors ? result : undefined,
		});
	}
	return result;
}

function mapRows<T>(value: unknown, name: string, mapper: (row: unknown) => T): readonly T[] {
	if (!Array.isArray(value)) throw new ShardingPersistenceError(`${name} returned a non-array result.`);
	const mapped: T[] = [];
	for (let index = 0; index < value.length; index += 1) {
		try {
			mapped.push(mapper(value[index]));
		} catch (cause) {
			throw new ShardingPersistenceError(`${name} row ${index} is invalid.`, { cause });
		}
	}
	return Object.freeze(mapped);
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
