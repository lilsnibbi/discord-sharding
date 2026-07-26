import type { $AnalyticsRecord } from "./analytics";

/**
 * Stored record that remembers which Bridge owns a shard.
 */
export interface $PersistedAssignment {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Bridge that owns the assignment.
	 */
	readonly bridgeId: string;

	/**
	 * Ownership version used to reject stale processes.
	 */
	readonly epoch: number;

	/**
	 * Unix time in milliseconds of the latest change.
	 */
	readonly updatedAt: number;
}

/**
 * Stored status for one Bridge deployment.
 */
export interface $PersistedBridge {
	/**
	 * Stable operator-supplied Bridge identifier.
	 */
	readonly id: string;

	/**
	 * Latest accepted running Bridge instance.
	 */
	readonly generation: string;

	/**
	 * Latest declared shard process capacity.
	 */
	readonly maxShards: number;

	/**
	 * Whether the Hub last observed the Bridge as connected.
	 */
	readonly connected: boolean;

	/**
	 * Unix time in milliseconds of the latest change.
	 */
	readonly updatedAt: number;
}

/**
 * Shard process state stored by the Hub.
 */
export type $PersistedShardState = "assigned" | "starting" | "ready" | "stopping" | "stopped" | "failed";

/**
 * Stored status for one shard process.
 */
export interface $PersistedShard {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Assigned Bridge identifier.
	 */
	readonly bridgeId: string;

	/**
	 * Ownership version that authorized the process.
	 */
	readonly assignmentEpoch: number;

	/**
	 * Local process version assigned by the Bridge.
	 */
	readonly processGeneration: number;

	/**
	 * Latest process state.
	 */
	readonly state: $PersistedShardState;

	/**
	 * Unix time in milliseconds of the latest change.
	 */
	readonly updatedAt: number;
}

/**
 * Stored Hub state loaded during startup.
 */
export interface $PersistedHubState {
	/**
	 * Sticky assignments known to storage.
	 */
	readonly assignments: readonly $PersistedAssignment[];

	/**
	 * Latest stored Bridge records.
	 */
	readonly bridges: readonly $PersistedBridge[];

	/**
	 * Latest stored shard records.
	 */
	readonly shards: readonly $PersistedShard[];
}

/**
 * Storage interface used by {@link HubClient}.
 *
 * Most applications use the built-in SQLite storage through
 * `$HubClientOptions.databasePath`. Implement this interface only when custom
 * storage ownership is required.
 */
export interface $HubPersistence {
	/**
	 * Applies pending schema migrations.
	 */
	migrate(): Promise<void>;

	/**
	 * Loads assignments and the latest Bridge and shard status.
	 */
	loadState(): Promise<$PersistedHubState>;

	/**
	 * Stores the latest owner for one shard.
	 *
	 * @param assignment - Validated assignment snapshot.
	 */
	saveAssignment(assignment: $PersistedAssignment): Promise<void>;

	/**
	 * Stores the latest status for one Bridge.
	 *
	 * @param bridge - Validated Bridge snapshot.
	 */
	saveBridge(bridge: $PersistedBridge): Promise<void>;

	/**
	 * Stores the latest status for one shard process.
	 *
	 * @param shard - Validated shard snapshot.
	 */
	saveShard(shard: $PersistedShard): Promise<void>;

	/**
	 * Appends one analytics record.
	 *
	 * @param record - Validated analytics sample.
	 */
	appendAnalytics(record: $AnalyticsRecord): Promise<void>;

	/**
	 * Deletes at most one batch of analytics records.
	 *
	 * @param before - Delete records collected at or before this Unix time in milliseconds.
	 * @param batchSize - Maximum records deleted by this call.
	 * @returns Number of deleted records.
	 */
	clearAnalyticsBatch(before: number, batchSize: number): Promise<number>;

	/**
	 * Releases owned storage resources.
	 */
	close(): Promise<void>;
}
