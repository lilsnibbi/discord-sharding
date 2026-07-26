/**
 * Primitive value that can be sent through Sharding.
 */
export type $JsonPrimitive = boolean | number | string | null;

/**
 * Read-only object that can be sent through Sharding.
 */
export interface $JsonObject {
	/**
	 * Maps each key to another supported JSON value.
	 */
	readonly [key: string]: $JsonValue;
}

/**
 * Value that can be sent between shards.
 */
export type $JsonValue = $JsonObject | $JsonPrimitive | readonly $JsonValue[];

/**
 * Size limits for values sent between Sharding clients.
 */
export interface $PayloadPolicy {
	/**
	 * Maximum UTF-8 size of one encoded value.
	 *
	 * @defaultValue `1048576`
	 */
	readonly maxBytes: number;

	/**
	 * Maximum nesting depth, including the root value.
	 *
	 * @defaultValue `32`
	 */
	readonly maxDepth: number;

	/**
	 * Maximum total number of values and containers.
	 *
	 * @defaultValue `10000`
	 */
	readonly maxNodes: number;
}

/**
 * Timeout and capacity settings for requests waiting for replies.
 */
export interface $RequestPolicy {
	/**
	 * Default response deadline in milliseconds.
	 *
	 * @defaultValue `15000`
	 */
	readonly timeoutMs: number;

	/**
	 * Maximum number of requests waiting at once.
	 *
	 * @defaultValue `256`
	 */
	readonly maxPending: number;
}

/**
 * Delay settings used while a Bridge reconnects to the Hub.
 */
export interface $ReconnectPolicy {
	/**
	 * Delay before the first retry in milliseconds.
	 *
	 * @defaultValue `500`
	 */
	readonly initialDelayMs: number;

	/**
	 * Largest retry delay in milliseconds.
	 *
	 * @defaultValue `30000`
	 */
	readonly maxDelayMs: number;

	/**
	 * Multiplier applied after each failed connection.
	 *
	 * @defaultValue `2`
	 */
	readonly multiplier: number;

	/**
	 * Random delay adjustment from `0` through `1`.
	 *
	 * @defaultValue `0.2`
	 */
	readonly jitterRatio: number;
}

/**
 * Limits how often failed shard processes may restart.
 */
export interface $RestartPolicy {
	/**
	 * Maximum restart attempts inside the rolling window.
	 *
	 * @defaultValue `5`
	 */
	readonly maxAttempts: number;

	/**
	 * Time window used to count restart attempts.
	 *
	 * @defaultValue `60000`
	 */
	readonly windowMs: number;

	/**
	 * Initial restart delay in milliseconds.
	 *
	 * @defaultValue `1000`
	 */
	readonly initialDelayMs: number;

	/**
	 * Largest restart delay in milliseconds.
	 *
	 * @defaultValue `30000`
	 */
	readonly maxDelayMs: number;
}

/**
 * Cancellable delay used by schedulers and reconnect loops.
 *
 * @param milliseconds - Delay in milliseconds.
 * @param signal - Signal that cancels the wait.
 */
export type $Sleep = (milliseconds: number, signal: AbortSignal) => Promise<void>;

/**
 * Receives background failures that are not returned by a method call.
 *
 * @param error - Normalized failure.
 * @param context - Short description of the failed operation.
 */
export type $ErrorListener = (error: Error, context: string) => void;
