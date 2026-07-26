import { ShardingConfigurationError } from "../../errors/ShardingError";
import { snapshotConfigurationArray, snapshotConfigurationRecord } from "../../internal/configuration";
import { MAX_BRIDGES, MAX_IDENTIFIER_LENGTH, MAX_SHARDS } from "../../internal/limits";
import type { $PersistedAssignment } from "../../types/hub";
import type {
	$AssignmentBridge,
	$AssignmentPlan,
	$AssignmentPlannerInput,
	$AssignmentStep,
	$BridgeAssignmentTarget,
} from "./types";

interface ConnectedBridgeState {
	readonly id: string;
	readonly maxShards: number;
	readonly assigned: $PersistedAssignment[];
}

/**
 * Produces one deterministic sticky-assignment reconciliation step.
 *
 * Assignments belonging to disconnected or unavailable Bridges remain
 * reserved. Connected assignments move only when needed for capacity or an
 * even distribution, and a planning pass never requests more than one change.
 *
 * @param input - Current Bridge capacities and persisted assignments.
 * @returns Current allocation details and the next safe mutation.
 */
export function planNextAssignment(input: $AssignmentPlannerInput): $AssignmentPlan {
	const snapshot = parseInput(input);
	const bridgeById = new Map<string, $AssignmentBridge>();
	for (const bridge of snapshot.bridges) {
		if (bridgeById.has(bridge.id)) {
			throw new ShardingConfigurationError(`Bridge "${bridge.id}" appears more than once.`);
		}
		bridgeById.set(bridge.id, bridge);
	}

	const assignmentByShard = new Map<number, $PersistedAssignment>();
	for (const assignment of snapshot.assignments) {
		if (assignmentByShard.has(assignment.shardId)) {
			throw new ShardingConfigurationError(`Shard ${assignment.shardId} has more than one assignment.`);
		}
		assignmentByShard.set(assignment.shardId, assignment);
	}

	const connected = [...bridgeById.values()]
		.filter((bridge) => bridge.connected)
		.sort((left, right) => compareIdentifiers(left.id, right.id))
		.map(
			(bridge): ConnectedBridgeState => ({
				id: bridge.id,
				maxShards: bridge.maxShards,
				assigned: [],
			}),
		);
	const connectedById = new Map(connected.map((bridge) => [bridge.id, bridge]));
	const reservedShardIds: number[] = [];
	const unassignedShardIds: number[] = [];

	for (let shardId = 0; shardId < snapshot.totalShards; shardId += 1) {
		const assignment = assignmentByShard.get(shardId);
		if (assignment === undefined) {
			unassignedShardIds.push(shardId);
			continue;
		}
		const owner = connectedById.get(assignment.bridgeId);
		if (owner === undefined) {
			reservedShardIds.push(shardId);
			continue;
		}
		owner.assigned.push(assignment);
	}

	const assignableShardCount = snapshot.totalShards - reservedShardIds.length;
	const targetCounts = calculateTargetCounts(connected, assignableShardCount);
	const targets = connected.map((bridge): $BridgeAssignmentTarget => {
		const targetCount = targetCounts.get(bridge.id);
		if (targetCount === undefined) {
			throw new ShardingConfigurationError(`No assignment target was calculated for Bridge "${bridge.id}".`);
		}
		return Object.freeze({
			bridgeId: bridge.id,
			currentCount: bridge.assigned.length,
			targetCount,
		});
	});
	const nextStep = chooseNextStep(connected, targetCounts, unassignedShardIds);

	return Object.freeze({
		targets: Object.freeze(targets),
		reservedShardIds: Object.freeze(reservedShardIds),
		unassignedShardIds: Object.freeze(unassignedShardIds),
		nextStep,
	});
}

function chooseNextStep(
	connected: readonly ConnectedBridgeState[],
	targetCounts: ReadonlyMap<string, number>,
	unassignedShardIds: readonly number[],
): $AssignmentStep | null {
	const deficits = connected
		.map((bridge) => ({
			bridge,
			count: requireTargetCount(targetCounts, bridge.id) - bridge.assigned.length,
		}))
		.filter((entry) => entry.count > 0)
		.sort((left, right) => right.count - left.count || compareIdentifiers(left.bridge.id, right.bridge.id));

	const firstDeficit = deficits[0];
	const firstUnassignedShard = unassignedShardIds[0];
	if (firstDeficit !== undefined && firstUnassignedShard !== undefined) {
		return Object.freeze({
			kind: "assign",
			shardId: firstUnassignedShard,
			toBridgeId: firstDeficit.bridge.id,
			nextEpoch: 1,
		});
	}

	const excesses = connected
		.map((bridge) => ({
			bridge,
			count: bridge.assigned.length - requireTargetCount(targetCounts, bridge.id),
		}))
		.filter((entry) => entry.count > 0)
		.sort((left, right) => right.count - left.count || compareIdentifiers(left.bridge.id, right.bridge.id));
	const firstExcess = excesses[0];
	if (firstExcess === undefined) return null;

	const assignment = [...firstExcess.bridge.assigned].sort((left, right) => right.shardId - left.shardId)[0];
	if (assignment === undefined) {
		throw new ShardingConfigurationError(`Bridge "${firstExcess.bridge.id}" has an invalid assignment count.`);
	}
	if (firstDeficit === undefined) {
		return Object.freeze({
			kind: "unassign",
			shardId: assignment.shardId,
			fromBridgeId: firstExcess.bridge.id,
			currentEpoch: assignment.epoch,
		});
	}
	if (assignment.epoch >= Number.MAX_SAFE_INTEGER) {
		throw new ShardingConfigurationError(`Shard ${assignment.shardId} assignment epoch cannot be incremented.`);
	}
	return Object.freeze({
		kind: "transfer",
		shardId: assignment.shardId,
		fromBridgeId: firstExcess.bridge.id,
		toBridgeId: firstDeficit.bridge.id,
		currentEpoch: assignment.epoch,
		nextEpoch: assignment.epoch + 1,
	});
}

function calculateTargetCounts(
	connected: readonly ConnectedBridgeState[],
	assignableShardCount: number,
): ReadonlyMap<string, number> {
	const targets = new Map<string, number>();
	if (connected.length === 0) return targets;

	let totalCapacity = 0;
	for (const bridge of connected) totalCapacity += bridge.maxShards;
	const assignedTarget = Math.min(assignableShardCount, totalCapacity);

	let low = 0;
	let high = assignedTarget;
	while (low < high) {
		const candidate = Math.ceil((low + high) / 2);
		let used = 0;
		for (const bridge of connected) used += Math.min(bridge.maxShards, candidate);
		if (used <= assignedTarget) {
			low = candidate;
		} else {
			high = candidate - 1;
		}
	}

	let allocated = 0;
	for (const bridge of connected) {
		const target = Math.min(bridge.maxShards, low);
		targets.set(bridge.id, target);
		allocated += target;
	}

	const extraCandidates = connected
		.filter((bridge) => bridge.maxShards > low)
		.sort((left, right) => right.assigned.length - left.assigned.length || compareIdentifiers(left.id, right.id));
	let remainder = assignedTarget - allocated;
	for (const bridge of extraCandidates) {
		if (remainder === 0) break;
		const target = targets.get(bridge.id);
		if (target === undefined) {
			throw new ShardingConfigurationError(`No assignment target was initialized for Bridge "${bridge.id}".`);
		}
		targets.set(bridge.id, target + 1);
		remainder -= 1;
	}
	if (remainder !== 0) {
		throw new ShardingConfigurationError(
			"Connected Bridge capacity could not satisfy the calculated assignment target.",
		);
	}
	return targets;
}

function parseInput(value: unknown): $AssignmentPlannerInput {
	const input = snapshotConfigurationRecord(value, "Assignment planner input");
	const totalShards = input.totalShards;
	if (
		typeof totalShards !== "number" ||
		!Number.isSafeInteger(totalShards) ||
		totalShards <= 0 ||
		totalShards > MAX_SHARDS
	) {
		throw new ShardingConfigurationError(
			`Assignment planner input.totalShards must be a positive integer no greater than ${MAX_SHARDS}.`,
		);
	}

	const bridgeValues = snapshotConfigurationArray(input.bridges, "Assignment planner input.bridges", MAX_BRIDGES);
	const bridges = bridgeValues.map((bridge, index) => parseBridge(bridge, index));
	const assignmentValues = snapshotConfigurationArray(
		input.assignments,
		"Assignment planner input.assignments",
		totalShards,
	);
	const assignments = assignmentValues.map((assignment, index) => parseAssignment(assignment, index, totalShards));
	return Object.freeze({
		totalShards,
		bridges: Object.freeze(bridges),
		assignments: Object.freeze(assignments),
	});
}

function parseBridge(value: unknown, index: number): $AssignmentBridge {
	const bridge = snapshotConfigurationRecord(value, `Assignment planner input.bridges[${index}]`);
	const id = bridge.id;
	if (
		typeof id !== "string" ||
		id.length === 0 ||
		id.length > MAX_IDENTIFIER_LENGTH ||
		id.trim() !== id ||
		hasControlCharacter(id)
	) {
		throw new ShardingConfigurationError(
			`Assignment planner input.bridges[${index}].id must be a trimmed identifier of at most ${MAX_IDENTIFIER_LENGTH} characters.`,
		);
	}
	if (typeof bridge.connected !== "boolean") {
		throw new ShardingConfigurationError(`Assignment planner input.bridges[${index}].connected must be a boolean.`);
	}
	const maxShards = bridge.maxShards;
	if (typeof maxShards !== "number" || !Number.isSafeInteger(maxShards) || maxShards < 0 || maxShards > MAX_SHARDS) {
		throw new ShardingConfigurationError(
			`Assignment planner input.bridges[${index}].maxShards must be a non-negative integer no greater than ${MAX_SHARDS}.`,
		);
	}
	return Object.freeze({
		id,
		connected: bridge.connected,
		maxShards,
	});
}

function parseAssignment(value: unknown, index: number, totalShards: number): $PersistedAssignment {
	const assignment = snapshotConfigurationRecord(value, `Assignment planner input.assignments[${index}]`);
	const shardId = assignment.shardId;
	if (typeof shardId !== "number" || !Number.isSafeInteger(shardId) || shardId < 0 || shardId >= totalShards) {
		throw new ShardingConfigurationError(
			`Assignment planner input.assignments[${index}].shardId must be within the configured shard range.`,
		);
	}
	const bridgeId = assignment.bridgeId;
	if (
		typeof bridgeId !== "string" ||
		bridgeId.length === 0 ||
		bridgeId.length > MAX_IDENTIFIER_LENGTH ||
		bridgeId.trim() !== bridgeId ||
		hasControlCharacter(bridgeId)
	) {
		throw new ShardingConfigurationError(
			`Assignment planner input.assignments[${index}].bridgeId must be a valid Bridge identifier.`,
		);
	}
	const epoch = assignment.epoch;
	if (typeof epoch !== "number" || !Number.isSafeInteger(epoch) || epoch <= 0) {
		throw new ShardingConfigurationError(
			`Assignment planner input.assignments[${index}].epoch must be a positive safe integer.`,
		);
	}
	const updatedAt = assignment.updatedAt;
	if (typeof updatedAt !== "number" || !Number.isSafeInteger(updatedAt) || updatedAt < 0) {
		throw new ShardingConfigurationError(
			`Assignment planner input.assignments[${index}].updatedAt must be a non-negative safe integer.`,
		);
	}
	return Object.freeze({
		shardId,
		bridgeId,
		epoch,
		updatedAt,
	});
}

function requireTargetCount(targets: ReadonlyMap<string, number>, bridgeId: string): number {
	const target = targets.get(bridgeId);
	if (target === undefined) {
		throw new ShardingConfigurationError(`No assignment target exists for Bridge "${bridgeId}".`);
	}
	return target;
}

function compareIdentifiers(left: string, right: string): number {
	if (left < right) return -1;
	if (left > right) return 1;
	return 0;
}

function hasControlCharacter(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code <= 31 || code === 127) return true;
	}
	return false;
}
