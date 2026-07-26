import type { $Sleep } from "../../types/common";

/**
 * Identify operation invoked only after the scheduler grants its Discord bucket.
 *
 * @returns The result of the caller's actual login operation.
 */
export type $IdentifyOperation<Result> = () => Promise<Result>;

/**
 * Internal identify scheduler configuration.
 */
export interface $IdentifySchedulerOptions {
	/**
	 * Maximum identify requests waiting for a grant.
	 *
	 * @defaultValue `10000`
	 */
	readonly maxPending?: number;

	/**
	 * Monotonic clock in milliseconds.
	 *
	 * @defaultValue `performance.now`
	 */
	readonly now?: () => number;

	/**
	 * Abort-aware sleep used while waiting for a bucket or session reset.
	 */
	readonly sleep?: $Sleep;
}
