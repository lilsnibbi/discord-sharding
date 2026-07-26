import type { $ErrorListener, $PayloadPolicy, $RequestPolicy, $Sleep } from "../common";
import type { $GatewayFetch } from "./gateway";
import type { $HubPersistence } from "./persistence";

/**
 * Lifecycle state reported by {@link HubClient}.
 */
export type $HubState = "idle" | "starting" | "running" | "stopping" | "stopped" | "failed";

/**
 * Settings used to create a {@link HubClient}.
 */
export interface $HubClientOptions {
	/**
	 * Discord bot token used only to read Gateway details.
	 */
	readonly botToken: string;

	/**
	 * Token shared with Bridge deployments.
	 */
	readonly bridgeToken: string;

	/**
	 * Separate token required by Hub administration routes.
	 */
	readonly adminToken: string;

	/**
	 * SQLite path used by built-in Hub storage.
	 *
	 * Use `":memory:"` only for tests or intentionally ephemeral deployments.
	 *
	 * @defaultValue `"./sharding-hub.sqlite"`
	 */
	readonly databasePath?: string;

	/**
	 * Custom Hub storage. Usually omitted in favour of {@link databasePath}.
	 */
	readonly persistence?: $HubPersistence;

	/**
	 * Global shard count. Uses Discord's recommendation when omitted.
	 */
	readonly totalShards?: number;

	/**
	 * Hostname used by the Hub HTTP and WebSocket server.
	 *
	 * @defaultValue `"0.0.0.0"`
	 */
	readonly hostname?: string;

	/**
	 * Port used by the Hub HTTP and WebSocket server.
	 *
	 * @defaultValue `3000`
	 */
	readonly port?: number;

	/**
	 * Discord Gateway Bot endpoint override.
	 *
	 * @defaultValue `"https://discord.com/api/v10/gateway/bot"`
	 */
	readonly gatewayEndpoint?: string | URL;

	/**
	 * Custom function used to fetch Discord Gateway details.
	 */
	readonly fetch?: $GatewayFetch;

	/**
	 * Timeout and capacity settings for operations waiting for replies.
	 */
	readonly request?: Partial<$RequestPolicy>;

	/**
	 * Limits for JSON-compatible values sent through Sharding.
	 */
	readonly payload?: Partial<$PayloadPolicy>;

	/**
	 * Maximum WebSocket data buffered for one Bridge.
	 *
	 * @defaultValue `4194304`
	 */
	readonly maxBufferedBytes?: number;

	/**
	 * Maximum messages waiting to be sent to one Bridge.
	 *
	 * @defaultValue `1024`
	 */
	readonly maxQueuedMessages?: number;

	/**
	 * Maximum broadcast evaluations running at once.
	 *
	 * @defaultValue `32`
	 */
	readonly maxEvaluations?: number;

	/**
	 * Delay between preparing and starting a broadcast evaluation.
	 *
	 * @defaultValue `25`
	 */
	readonly evaluationCommitLeadMs?: number;

	/**
	 * Custom monotonic clock used by scheduling.
	 *
	 * @defaultValue `performance.now`
	 */
	readonly now?: () => number;

	/**
	 * Custom Unix-millisecond clock used for stored timestamps.
	 *
	 * @defaultValue `Date.now`
	 */
	readonly wallClock?: () => number;

	/**
	 * Custom cancellable delay used by scheduling.
	 */
	readonly sleep?: $Sleep;

	/**
	 * Receives background failures that are not returned by a method call.
	 */
	readonly onError?: $ErrorListener;
}
