import { ShardingConfigurationError } from "../../errors/ShardingError";
import {
	assertConfigurationKeys,
	snapshotConfigurationArray,
	snapshotConfigurationRecord,
} from "../../internal/configuration";
import { MAX_BRIDGES, MAX_SHARDS } from "../../internal/limits";
import {
	requireIdentifier,
	requireNonNegativeInteger,
	requirePositiveInteger,
	requireShardId,
} from "../../internal/validation";
import type { $PersistedAssignment, $PersistedBridge, $PersistedShard, $PersistedShardState } from "../../types/hub";
import { RELEASED_ASSIGNMENT_PREFIX } from "./constants";
import type { LoadedState } from "./types";
import { isReleasedAssignment, releasedAssignmentOwner } from "./utilities";

export function normalizeLoadedState(value: unknown, totalShards: number): LoadedState {
	const root = snapshotConfigurationRecord(value, "Persisted Hub state");
	requireConfigurationFields(root, new Set(["assignments", "bridges", "shards"]), "Persisted Hub state");
	const assignments = new Map<number, $PersistedAssignment>();
	for (const [index, entry] of snapshotConfigurationArray(
		root.assignments,
		"Persisted assignments",
		MAX_SHARDS,
	).entries()) {
		const record = snapshotConfigurationRecord(entry, `Persisted assignments[${index}]`);
		requireConfigurationFields(
			record,
			new Set(["bridgeId", "epoch", "shardId", "updatedAt"]),
			`Persisted assignments[${index}]`,
		);
		const assignment: $PersistedAssignment = Object.freeze({
			bridgeId: requireIdentifier(record.bridgeId, `Persisted assignments[${index}].bridgeId`),
			epoch: requirePositiveInteger(record.epoch, `Persisted assignments[${index}].epoch`, Number.MAX_SAFE_INTEGER),
			shardId: requireShardId(record.shardId, `Persisted assignments[${index}].shardId`),
			updatedAt: requireNonNegativeInteger(
				record.updatedAt,
				`Persisted assignments[${index}].updatedAt`,
				Number.MAX_SAFE_INTEGER,
			),
		});
		if (assignment.shardId >= totalShards) {
			throw new ShardingConfigurationError(
				`Persisted assignment shard ${assignment.shardId} exceeds totalShards ${totalShards}.`,
			);
		}
		if (
			assignment.bridgeId.startsWith(RELEASED_ASSIGNMENT_PREFIX) &&
			assignment.bridgeId !== releasedAssignmentOwner(assignment.shardId)
		) {
			throw new ShardingConfigurationError(
				`Persisted assignment shard ${assignment.shardId} has an invalid tombstone.`,
			);
		}
		if (assignments.has(assignment.shardId)) {
			throw new ShardingConfigurationError(`Persisted assignment shard ${assignment.shardId} is duplicated.`);
		}
		assignments.set(assignment.shardId, assignment);
	}
	const bridges = new Map<string, $PersistedBridge>();
	for (const [index, entry] of snapshotConfigurationArray(root.bridges, "Persisted Bridges", MAX_BRIDGES).entries()) {
		const record = snapshotConfigurationRecord(entry, `Persisted Bridges[${index}]`);
		requireConfigurationFields(
			record,
			new Set(["connected", "generation", "id", "maxShards", "updatedAt"]),
			`Persisted Bridges[${index}]`,
		);
		if (typeof record.connected !== "boolean") {
			throw new ShardingConfigurationError(`Persisted Bridges[${index}].connected must be a boolean.`);
		}
		const bridge: $PersistedBridge = Object.freeze({
			connected: record.connected,
			generation: requireIdentifier(record.generation, `Persisted Bridges[${index}].generation`),
			id: requireIdentifier(record.id, `Persisted Bridges[${index}].id`),
			maxShards: requirePositiveInteger(record.maxShards, `Persisted Bridges[${index}].maxShards`, MAX_SHARDS),
			updatedAt: requireNonNegativeInteger(
				record.updatedAt,
				`Persisted Bridges[${index}].updatedAt`,
				Number.MAX_SAFE_INTEGER,
			),
		});
		if (bridge.id.startsWith(RELEASED_ASSIGNMENT_PREFIX)) {
			throw new ShardingConfigurationError(`Persisted Bridge ${bridge.id} uses a reserved identifier prefix.`);
		}
		if (bridges.has(bridge.id)) throw new ShardingConfigurationError(`Persisted Bridge ${bridge.id} is duplicated.`);
		bridges.set(bridge.id, bridge);
	}
	for (const assignment of assignments.values()) {
		if (!isReleasedAssignment(assignment) && !bridges.has(assignment.bridgeId)) {
			throw new ShardingConfigurationError(
				`Persisted assignment shard ${assignment.shardId} references unknown Bridge ${assignment.bridgeId}.`,
			);
		}
	}
	const shards = new Map<number, $PersistedShard>();
	for (const [index, entry] of snapshotConfigurationArray(root.shards, "Persisted shards", MAX_SHARDS).entries()) {
		const record = snapshotConfigurationRecord(entry, `Persisted shards[${index}]`);
		requireConfigurationFields(
			record,
			new Set(["assignmentEpoch", "bridgeId", "processGeneration", "shardId", "state", "updatedAt"]),
			`Persisted shards[${index}]`,
		);
		const shardId = requireShardId(record.shardId, `Persisted shards[${index}].shardId`);
		if (shardId >= totalShards) {
			throw new ShardingConfigurationError(`Persisted shard ${shardId} exceeds totalShards ${totalShards}.`);
		}
		const shard: $PersistedShard = Object.freeze({
			assignmentEpoch: requirePositiveInteger(
				record.assignmentEpoch,
				`Persisted shards[${index}].assignmentEpoch`,
				Number.MAX_SAFE_INTEGER,
			),
			bridgeId: requireIdentifier(record.bridgeId, `Persisted shards[${index}].bridgeId`),
			processGeneration: requirePositiveInteger(
				record.processGeneration,
				`Persisted shards[${index}].processGeneration`,
				Number.MAX_SAFE_INTEGER,
			),
			shardId,
			state: requirePersistedShardState(record.state, `Persisted shards[${index}].state`),
			updatedAt: requireNonNegativeInteger(
				record.updatedAt,
				`Persisted shards[${index}].updatedAt`,
				Number.MAX_SAFE_INTEGER,
			),
		});
		if (shards.has(shardId)) throw new ShardingConfigurationError(`Persisted shard ${shardId} is duplicated.`);
		shards.set(shardId, shard);
	}
	return { assignments, bridges, shards };
}

function requireConfigurationFields(
	record: Readonly<Record<string, unknown>>,
	fields: ReadonlySet<string>,
	name: string,
): void {
	assertConfigurationKeys(record, fields, name);
	for (const field of fields) {
		if (!Object.hasOwn(record, field)) throw new ShardingConfigurationError(`${name} is missing "${field}".`);
	}
}

function requirePersistedShardState(value: unknown, name: string): $PersistedShardState {
	switch (value) {
		case "assigned":
		case "starting":
		case "ready":
		case "stopping":
		case "stopped":
		case "failed":
			return value;
		default:
			throw new ShardingConfigurationError(`${name} is invalid.`);
	}
}
