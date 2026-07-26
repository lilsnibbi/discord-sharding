import type { $BridgeAnalyticsQuery, $BridgeClientOptions, $BridgeShardSnapshot, $BridgeState } from "../types/bridge";
import type { $AnalyticsRecord } from "../types/hub";
import { BridgeRuntime } from "./runtime/BridgeRuntime";

/**
 * Connects one bot deployment to the Hub and runs its assigned shard processes.
 *
 * Run one Bridge in each deployment. Existing Discord sessions remain alive
 * while the Hub is unavailable, but new Hub-dependent work waits for recovery.
 */
export class BridgeClient implements AsyncDisposable {
	/** Stable deployment identifier used for shard assignments. */
	public readonly id: string;

	/** Maximum number of shard processes this deployment may run. */
	public readonly maxShards: number;

	/** Unique identifier for this running Bridge instance. */
	public readonly generation: string;

	readonly #runtime: BridgeRuntime;

	/**
	 * Creates a Bridge without opening SQLite, WebSockets, or shard processes.
	 *
	 * @param options - Hub connection, shard entrypoint, capacity, and lifecycle settings.
	 */
	public constructor(options: $BridgeClientOptions) {
		this.#runtime = new BridgeRuntime(options);
		this.id = this.#runtime.id;
		this.maxShards = this.#runtime.maxShards;
		this.generation = this.#runtime.generation;
	}

	/** Current Bridge lifecycle state. */
	public get state(): $BridgeState {
		return this.#runtime.state;
	}

	/** Whether the Hub and local shards have finished synchronizing. */
	public get connected(): boolean {
		return this.#runtime.connected;
	}

	/** Whether Hub-dependent shard operations are temporarily unavailable. */
	public get isInMaintenance(): boolean {
		return this.#runtime.isInMaintenance;
	}

	/** Read-only snapshot of shard processes currently owned by this Bridge. */
	public get shards(): ReadonlyMap<number, $BridgeShardSnapshot> {
		return this.#runtime.shards;
	}

	/**
	 * Opens local analytics storage and starts connecting to the Hub.
	 *
	 * This does not wait for the Hub. Connection retries continue until
	 * {@link stop} is called.
	 *
	 * @returns This Bridge after local resources are ready.
	 */
	public async start(): Promise<this> {
		await this.#runtime.start();
		return this;
	}

	/**
	 * Waits until the Hub and existing local shards finish synchronizing.
	 *
	 * @param timeoutMs - Maximum wait in milliseconds. Uses the request timeout when omitted.
	 */
	public async waitUntilConnected(timeoutMs?: number): Promise<void> {
		await this.#runtime.waitUntilConnected(timeoutMs);
	}

	/**
	 * Registers a listener for maintenance-state changes.
	 *
	 * @param listener - Callback receiving the new maintenance state.
	 * @returns Cleanup callback that removes the listener.
	 */
	public onMaintenanceChange(listener: (maintenance: boolean) => void): () => void {
		return this.#runtime.onMaintenanceChange(listener);
	}

	/**
	 * Reads local analytics without deleting them.
	 *
	 * @param query - Optional result limit and shard filter.
	 * @returns Newest records first.
	 */
	public async getAnalytics(query?: $BridgeAnalyticsQuery): Promise<readonly $AnalyticsRecord[]> {
		return this.#runtime.getAnalytics(query);
	}

	/**
	 * Deletes local analytics in small batches.
	 *
	 * @param before - Delete records collected at or before this Unix time in milliseconds.
	 * @param batchSize - Maximum records deleted per batch.
	 * @returns Total number of deleted records.
	 */
	public async clearAnalytics(before?: number, batchSize?: number): Promise<number> {
		return this.#runtime.clearAnalytics(before, batchSize);
	}

	/**
	 * Stops reconnecting and releases the Hub socket, shard processes, and SQLite.
	 *
	 * Repeated calls share the same shutdown operation.
	 */
	public stop(): Promise<void> {
		return this.#runtime.stop();
	}

	/** Calls {@link stop} when the Bridge is owned with `await using`. */
	public async [Symbol.asyncDispose](): Promise<void> {
		await this.stop();
	}
}
