import type {
	$ErrorListener,
	$PayloadPolicy,
	$ReconnectPolicy,
	$RequestPolicy,
	$RestartPolicy,
	$Sleep,
} from "./common";
import type { $AnalyticsRecord } from "./hub";

/**
 * Lifecycle state reported by {@link BridgeClient}.
 */
export type $BridgeState = "idle" | "starting" | "running" | "stopping" | "stopped" | "failed";

/**
 * State of a shard process managed by a Bridge.
 */
export type $BridgeShardState = "starting" | "ready" | "stopping" | "stopped" | "failed";

/**
 * Read-only summary of a shard process managed by {@link BridgeClient}.
 */
export interface $BridgeShardSnapshot {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Hub-issued ownership version for this shard.
	 */
	readonly assignmentEpoch: number;

	/**
	 * Local version of the currently running process.
	 */
	readonly processGeneration: number;

	/**
	 * Latest process state.
	 */
	readonly state: $BridgeShardState;
}

/**
 * Exit information reported by a custom shard process.
 */
export interface $ShardProcessExit {
	/**
	 * Exit code, or `null` when terminated by a signal.
	 */
	readonly code: number | null;

	/**
	 * Numeric termination signal, when supplied by Bun.
	 */
	readonly signal: number | null;

	/**
	 * Error reported while starting or running the process.
	 */
	readonly error?: Error;
}

/**
 * Callbacks supplied to a custom shard process for one process lifetime.
 */
export interface $ShardProcessCallbacks {
	/**
	 * Receives IPC data from the child process.
	 *
	 * @param message - IPC value.
	 */
	readonly onMessage: (message: unknown) => void;

	/**
	 * Receives the process exit once.
	 *
	 * @param exit - Normalized exit information.
	 */
	readonly onExit: (exit: $ShardProcessExit) => void;
}

/**
 * Read-only launch settings passed to a custom shard process factory.
 */
export interface $ShardProcessContext {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Global shard count.
	 */
	readonly totalShards: number;

	/**
	 * Hub-issued ownership version for the shard.
	 */
	readonly assignmentEpoch: number;

	/**
	 * Local version assigned to this process launch.
	 */
	readonly processGeneration: number;

	/**
	 * TypeScript file that starts the shard application.
	 */
	readonly script: string;

	/**
	 * Arguments placed after the entrypoint.
	 */
	readonly args: readonly string[];

	/**
	 * Optional process working directory.
	 */
	readonly cwd?: string;

	/**
	 * Complete environment for the child process.
	 */
	readonly environment: Readonly<Record<string, string>>;

	/**
	 * IPC and exit callbacks for this process launch.
	 */
	readonly callbacks: $ShardProcessCallbacks;
}

/**
 * Process handle returned by a custom shard process factory.
 */
export interface $ShardProcess {
	/**
	 * Process identifier reported by Bun.
	 */
	readonly pid: number;

	/**
	 * Resolves with the exit code after the child stops.
	 */
	readonly exited: Promise<number>;

	/**
	 * Whether termination has already been requested.
	 */
	readonly killed: boolean;

	/**
	 * Sends one protocol message to the child process.
	 *
	 * @param message - Validated protocol message.
	 */
	send(message: object): void | Promise<void>;

	/**
	 * Requests process termination.
	 *
	 * @param signal - Optional numeric force signal.
	 */
	kill(signal?: number): void;
}

/**
 * Creates one shard process for a Bridge.
 *
 * @param context - Read-only launch settings and callbacks.
 */
export type $ShardProcessFactory = (context: $ShardProcessContext) => $ShardProcess;

/**
 * Creates a WebSocket used by a Bridge to connect to the Hub.
 *
 * @param url - Hub WebSocket URL.
 * @param headers - Required authentication and Bridge headers.
 */
export type $BridgeSocketFactory = (url: string, headers: Readonly<Record<string, string>>) => WebSocket;

/**
 * Filters used when reading local Bridge analytics.
 */
export interface $BridgeAnalyticsQuery {
	/**
	 * Maximum records returned, newest first.
	 *
	 * @defaultValue `100`
	 */
	readonly limit?: number;

	/**
	 * Return only records for this shard.
	 */
	readonly shardId?: number;
}

/**
 * Settings used to create a {@link BridgeClient}.
 */
export interface $BridgeClientOptions {
	/**
	 * Stable name for this deployment.
	 *
	 * Reuse this name when the same running Bridge reconnects. Before replacing
	 * a stopped Bridge instance, confirm its Discord sessions ended and release
	 * its assignments from the Hub.
	 */
	readonly id: string;

	/**
	 * Public or private HTTP URL of the Hub.
	 */
	readonly hubUrl: string | URL;

	/**
	 * Token configured for Bridge connections on the Hub.
	 */
	readonly token: string;

	/**
	 * Maximum shard processes this deployment can host.
	 */
	readonly maxShards: number;

	/**
	 * TypeScript application entrypoint started for each shard.
	 */
	readonly shardScript: string;

	/**
	 * Arguments passed to each shard entrypoint.
	 */
	readonly args?: readonly string[];

	/**
	 * Working directory used by shard processes.
	 */
	readonly cwd?: string;

	/**
	 * Environment changes applied to each shard process.
	 *
	 * Set a value to `undefined` to remove an inherited key.
	 */
	readonly env?: Readonly<Record<string, string | undefined>>;

	/**
	 * Path to the local analytics SQLite database.
	 *
	 * @defaultValue `"./sharding-bridge.sqlite"`
	 */
	readonly analyticsPath?: string;

	/**
	 * Delay settings for Hub reconnection. Retries continue until shutdown.
	 */
	readonly reconnect?: Partial<$ReconnectPolicy>;

	/**
	 * Limits for restarting failed shard processes.
	 */
	readonly restart?: Partial<$RestartPolicy>;

	/**
	 * Timeout and capacity settings for operations waiting for replies.
	 */
	readonly request?: Partial<$RequestPolicy>;

	/**
	 * Limits for JSON-compatible values sent through Sharding.
	 */
	readonly payload?: Partial<$PayloadPolicy>;

	/**
	 * Maximum child startup duration in milliseconds.
	 *
	 * @defaultValue `30000`
	 */
	readonly startupTimeoutMs?: number;

	/**
	 * Maximum graceful and forceful shutdown duration per phase.
	 *
	 * @defaultValue `10000`
	 */
	readonly shutdownTimeoutMs?: number;

	/**
	 * Maximum WebSocket bytes buffered locally.
	 *
	 * @defaultValue `4194304`
	 */
	readonly maxBufferedBytes?: number;

	/**
	 * Custom shard process factory, mainly for specialized runtimes and tests.
	 */
	readonly processFactory?: $ShardProcessFactory;

	/**
	 * Custom Hub WebSocket factory, mainly for specialized runtimes and tests.
	 */
	readonly socketFactory?: $BridgeSocketFactory;

	/**
	 * Custom cancellable delay used between connection attempts.
	 */
	readonly sleep?: $Sleep;

	/**
	 * Random source used for reconnect jitter.
	 *
	 * @defaultValue `Math.random`
	 */
	readonly random?: () => number;

	/**
	 * Receives background failures that are not returned by a method call.
	 */
	readonly onError?: $ErrorListener;
}

/**
 * Local Bridge analytics operation used by custom integrations.
 */
export interface $BridgeAnalytics {
	/**
	 * Reads persisted records without removing them.
	 *
	 * @param query - Optional limit and shard filter.
	 */
	read(query?: $BridgeAnalyticsQuery): Promise<readonly $AnalyticsRecord[]>;

	/**
	 * Deletes at most one bounded batch.
	 *
	 * @param before - Delete records collected at or before this Unix millisecond.
	 * @param batchSize - Maximum records removed.
	 */
	clear(before: number, batchSize: number): Promise<number>;

	/**
	 * Appends one validated sample.
	 *
	 * @param record - Analytics sample.
	 */
	append(record: $AnalyticsRecord): Promise<void>;

	/**
	 * Releases the SQLite connection.
	 */
	close(): Promise<void>;
}
