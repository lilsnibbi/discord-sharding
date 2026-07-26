/**
 * Discord identify limits returned by `GET /gateway/bot`.
 */
export interface $GatewaySessionStartLimit {
	/**
	 * Maximum identifies allowed in the current reset period.
	 */
	readonly total: number;

	/**
	 * Identifies still available in the current reset period.
	 */
	readonly remaining: number;

	/**
	 * Milliseconds until Discord resets the allowance.
	 */
	readonly reset_after: number;

	/**
	 * Number of identify groups that may run concurrently.
	 */
	readonly max_concurrency: number;
}

/**
 * Discord Gateway details used while starting the Hub.
 */
export interface $GatewayBotInfo {
	/**
	 * Secure Discord Gateway URL.
	 */
	readonly url: string;

	/**
	 * Discord's recommended shard count.
	 */
	readonly shards: number;

	/**
	 * Current identify limits.
	 */
	readonly session_start_limit: $GatewaySessionStartLimit;
}

/**
 * Fetch-compatible function used to read Discord Gateway details.
 *
 * @param input - Request URL or request object.
 * @param init - Request options.
 */
export type $GatewayFetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
