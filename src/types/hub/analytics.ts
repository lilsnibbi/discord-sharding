import type { $JsonValue } from "../common";

/**
 * Analytics record stored by a Bridge or the Hub.
 */
export interface $AnalyticsRecord {
	/**
	 * Unique record identifier.
	 */
	readonly id: string;

	/**
	 * Bridge that collected the sample.
	 */
	readonly bridgeId: string;

	/**
	 * Related shard, or `null` for deployment-wide data.
	 */
	readonly shardId: number | null;

	/**
	 * Unix time in milliseconds when the sample was collected.
	 */
	readonly collectedAt: number;

	/**
	 * Read-only analytics values.
	 */
	readonly data: $JsonValue;
}

/**
 * Options for deleting retained Hub analytics.
 */
export interface $ClearAnalyticsOptions {
	/**
	 * Delete samples collected at or before this time.
	 *
	 * @defaultValue Current time
	 */
	readonly before?: number;

	/**
	 * Maximum records deleted in each database batch.
	 *
	 * @defaultValue `1000`
	 */
	readonly batchSize?: number;
}
