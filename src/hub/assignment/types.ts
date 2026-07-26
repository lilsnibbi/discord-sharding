import type { $PersistedAssignment } from "../../types/hub";

/**
 * Bridge capacity considered by the assignment planner.
 */
export interface $AssignmentBridge {
	/**
	 * Stable Bridge identifier.
	 */
	readonly id: string;

	/**
	 * Whether the Bridge currently has a synchronized Hub connection.
	 */
	readonly connected: boolean;

	/**
	 * Maximum shard processes the Bridge currently accepts.
	 */
	readonly maxShards: number;
}

/**
 * Immutable state supplied to the assignment planner.
 */
export interface $AssignmentPlannerInput {
	/**
	 * Complete global shard count.
	 */
	readonly totalShards: number;

	/**
	 * Known connected and disconnected Bridges.
	 */
	readonly bridges: readonly $AssignmentBridge[];

	/**
	 * Existing sticky assignments.
	 */
	readonly assignments: readonly $PersistedAssignment[];
}

/**
 * Desired assignment count for one connected Bridge.
 */
export interface $BridgeAssignmentTarget {
	/**
	 * Stable Bridge identifier.
	 */
	readonly bridgeId: string;

	/**
	 * Current sticky assignment count.
	 */
	readonly currentCount: number;

	/**
	 * Capacity-aware balanced target.
	 */
	readonly targetCount: number;
}

/**
 * First ownership for a currently unassigned shard.
 */
export interface $AssignShardStep {
	readonly kind: "assign";
	readonly shardId: number;
	readonly toBridgeId: string;
	readonly nextEpoch: number;
}

/**
 * Generation-fenced ownership transfer requiring source shutdown first.
 */
export interface $TransferShardStep {
	readonly kind: "transfer";
	readonly shardId: number;
	readonly fromBridgeId: string;
	readonly toBridgeId: string;
	readonly currentEpoch: number;
	readonly nextEpoch: number;
}

/**
 * Source shutdown required when no connected destination has capacity.
 */
export interface $UnassignShardStep {
	readonly kind: "unassign";
	readonly shardId: number;
	readonly fromBridgeId: string;
	readonly currentEpoch: number;
}

/**
 * At most one assignment mutation returned by a planning pass.
 */
export type $AssignmentStep = $AssignShardStep | $TransferShardStep | $UnassignShardStep;

/**
 * Deterministic result of one pure assignment planning pass.
 */
export interface $AssignmentPlan {
	/**
	 * Connected Bridge targets ordered by identifier.
	 */
	readonly targets: readonly $BridgeAssignmentTarget[];

	/**
	 * Shards reserved for disconnected or unavailable owners.
	 */
	readonly reservedShardIds: readonly number[];

	/**
	 * Shards with no current owner.
	 */
	readonly unassignedShardIds: readonly number[];

	/**
	 * Next safe mutation, or `null` when the current state meets the plan.
	 */
	readonly nextStep: $AssignmentStep | null;
}
