import type { $ErrorListener, $PayloadPolicy, $RequestPolicy } from "./common";
import type { $DiscordClient } from "./discord";

/**
 * Lifecycle state reported by {@link ShardClient}.
 */
export type $ShardClientState = "idle" | "running" | "closing" | "closed" | "failed";

/**
 * Shard count summary for one Bridge known to the Hub.
 */
export interface $ShardBridgeSummary {
	/**
	 * Stable Bridge identifier.
	 */
	readonly bridgeId: string;

	/**
	 * Number of shards currently assigned to that Bridge.
	 */
	readonly shardCount: number;
}

/**
 * Frozen self-description of one shard process.
 *
 * Constructed from spawn-time identity and updated from Hub topology
 * synchronization. Read {@link ShardClient.identity} again after a
 * maintenance window to observe topology changes.
 */
export interface $ShardIdentity {
	/**
	 * Unique identifier of this shard process incarnation.
	 */
	readonly instanceId: string;

	/**
	 * Zero-based Discord shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Global Discord shard count.
	 */
	readonly totalShards: number;

	/**
	 * Hub-issued ownership version for this shard process.
	 */
	readonly assignmentEpoch: number;

	/**
	 * Local process version used to reject messages from older restarts.
	 */
	readonly processGeneration: number;

	/**
	 * Identifier of the Bridge this shard is connected to, or `null` before
	 * the Bridge has reported it.
	 */
	readonly bridgeId: string | null;

	/**
	 * Number of shards assigned to this shard's Bridge, or `0` before the
	 * first topology report.
	 */
	readonly bridgeShardCount: number;

	/**
	 * Number of Bridges known to the Hub, or `0` before the first topology
	 * report.
	 */
	readonly totalBridges: number;

	/**
	 * Shard count summary for every Bridge known to the Hub.
	 */
	readonly bridges: readonly $ShardBridgeSummary[];
}

/**
 * Bridge maintenance state available inside a shard process.
 */
export interface $ShardBridge {
	/**
	 * Whether Hub-dependent operations are temporarily unavailable.
	 */
	readonly isInMaintenance: boolean;

	/**
	 * Registers a listener for maintenance-state changes.
	 *
	 * @param listener - Callback receiving the new maintenance state.
	 * @returns Cleanup callback that removes the observer.
	 */
	onMaintenanceChange(listener: (maintenance: boolean) => void): () => void;
}

/**
 * Communication channel used by {@link ShardClient} to reach its Bridge.
 */
export interface $ShardTransport {
	/**
	 * Sends one protocol message to the Bridge.
	 *
	 * @param message - Protocol message.
	 */
	send(message: object): void | Promise<void>;

	/**
	 * Registers a listener for messages from the Bridge.
	 *
	 * @param listener - Callback receiving incoming transport data.
	 * @returns Cleanup callback that removes the listener.
	 */
	onMessage(listener: (message: unknown) => void): () => void;

	/**
	 * Registers a listener for transport disconnection.
	 *
	 * @param listener - Callback invoked after the transport disconnects.
	 * @returns Cleanup callback that removes the listener.
	 */
	onDisconnect?(listener: () => void): () => void;

	/**
	 * Whether Sharding may exit the child process after cleanup.
	 */
	readonly ownsProcess?: boolean;
}

/**
 * Information supplied to an incoming message or request handler.
 */
export interface $ShardMessageContext {
	/**
	 * Source shard, or `null` for a Hub management message.
	 */
	readonly sourceShardId: number | null;

	/**
	 * Signal aborted when the ShardClient closes.
	 */
	readonly signal: AbortSignal;
}

/**
 * Handles a one-way message sent to this shard.
 *
 * @param payload - Validated JSON-compatible application value.
 * @param context - Source shard and lifecycle signal.
 */
export type $ShardMessageListener = (payload: unknown, context: $ShardMessageContext) => void | Promise<void>;

/**
 * Handles a request sent to this shard.
 *
 * @param payload - Validated JSON-compatible application value.
 * @param context - Source identity and lifecycle signal.
 * @returns JSON-compatible response value.
 */
export type $ShardRequestHandler = (payload: unknown, context: $ShardMessageContext) => unknown | Promise<unknown>;

/**
 * Trusted developer function run on each Discord-ready shard.
 *
 * @param botClient - Ready Discord client owned by the destination shard.
 * @param context - JSON-compatible context supplied by the caller.
 * @returns JSON-compatible result.
 */
export type $BroadcastEvaluator<TClient extends $DiscordClient, TContext, TResult> = (
	botClient: TClient,
	context: TContext,
) => TResult | Promise<TResult>;

/**
 * Settings used to create a {@link ShardClient}.
 */
export interface $ShardClientOptions {
	/**
	 * Shard identifier. Reads `SHARDING_SHARD_ID` when omitted.
	 */
	readonly shardId?: number;

	/**
	 * Global shard count. Reads `SHARDING_TOTAL_SHARDS` when omitted.
	 */
	readonly totalShards?: number;

	/**
	 * Hub-issued ownership version. Reads `SHARDING_ASSIGNMENT_EPOCH` when omitted.
	 */
	readonly assignmentEpoch?: number;

	/**
	 * Local process version. Reads `SHARDING_PROCESS_GENERATION` when omitted.
	 */
	readonly processGeneration?: number;

	/**
	 * Owning Bridge identifier. Reads `SHARDING_BRIDGE_ID` when omitted.
	 */
	readonly bridgeId?: string;

	/**
	 * Bridge transport. Uses Bun process IPC when omitted.
	 */
	readonly transport?: $ShardTransport;

	/**
	 * Timeout and capacity settings for operations waiting for replies.
	 */
	readonly request?: Partial<$RequestPolicy>;

	/**
	 * Limits for JSON-compatible values sent through Sharding.
	 */
	readonly payload?: Partial<$PayloadPolicy>;

	/**
	 * Frequency of process and Discord analytics samples.
	 *
	 * @defaultValue `15000`
	 */
	readonly analyticsIntervalMs?: number | false;

	/**
	 * Frequency used to detect the Discord ready transition.
	 *
	 * @defaultValue `100`
	 */
	readonly readyPollIntervalMs?: number;

	/**
	 * Maximum prepared evaluations retained before commit.
	 *
	 * @defaultValue `64`
	 */
	readonly maxPreparedEvaluations?: number;

	/**
	 * Maximum evaluator source length.
	 *
	 * @defaultValue `65536`
	 */
	readonly maxEvaluatorSourceLength?: number;

	/**
	 * Request handler installed when the client is created.
	 */
	readonly onRequest?: $ShardRequestHandler;

	/**
	 * Runs before the Discord client is destroyed during an authorized shutdown.
	 */
	readonly onShutdown?: () => void | Promise<void>;

	/**
	 * Receives background failures that are not returned by a method call.
	 */
	readonly onError?: $ErrorListener;
}
