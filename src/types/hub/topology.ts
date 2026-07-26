/**
 * Current owner of one shard in a Hub topology snapshot.
 */
export interface $HubAssignment {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Assigned Bridge identifier.
	 */
	readonly bridgeId: string;

	/**
	 * Current ownership version.
	 */
	readonly epoch: number;
}

/**
 * Bridge status included in a Hub topology snapshot.
 */
export interface $HubBridgeTopology {
	/**
	 * Stable Bridge identifier.
	 */
	readonly id: string;

	/**
	 * Latest accepted running Bridge instance.
	 */
	readonly generation: string;

	/**
	 * Whether the Bridge is connected and synchronized.
	 */
	readonly connected: boolean;

	/**
	 * Declared shard process capacity.
	 */
	readonly maxShards: number;

	/**
	 * Sorted assigned shard identifiers.
	 */
	readonly assignedShardIds: readonly number[];

	/**
	 * Sorted Discord-ready shard identifiers.
	 */
	readonly readyShardIds: readonly number[];
}

/**
 * Read-only snapshot of the current Hub topology.
 */
export interface $HubTopology {
	/**
	 * Global Discord shard count.
	 */
	readonly totalShards: number;

	/**
	 * Known Bridge deployments.
	 */
	readonly bridges: readonly $HubBridgeTopology[];

	/**
	 * Shard assignments ordered by shard identifier.
	 */
	readonly assignments: readonly $HubAssignment[];

	/**
	 * Shards that currently have no available Bridge capacity.
	 */
	readonly unassignedShardIds: readonly number[];

	/**
	 * Unix time in milliseconds when the snapshot was created.
	 */
	readonly generatedAt: number;
}
