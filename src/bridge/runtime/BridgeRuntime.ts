import {
	ShardingCapacityError,
	ShardingConfigurationError,
	ShardingStateError,
	ShardingTimeoutError,
} from "../../errors/ShardingError";
import { createRequestId, requirePositiveInteger } from "../../internal/validation";
import { createWireMessage } from "../../protocol/codec";
import type { $BridgeAnalyticsQuery } from "../../types/bridge";
import type { $AnalyticsRecord } from "../../types/hub";
import { BridgeAnalyticsStore } from "../database/BridgeAnalyticsStore";
import { BridgeConnection } from "./BridgeConnection";
import { toError } from "./protocol";
import type { $ConnectionWaiter } from "./types";

const DEFAULT_CONNECTION_TIMEOUT_MS = 30_000;
const MAX_MAINTENANCE_LISTENERS = 256;

export class BridgeRuntime extends BridgeConnection {
	public async start(): Promise<this> {
		if (this.lifecycleState === "running") return this;
		if (this.lifecycleState !== "idle")
			throw new ShardingStateError(`Cannot start BridgeClient from ${this.lifecycleState}.`);
		this.lifecycleState = "starting";
		try {
			this.analytics = new BridgeAnalyticsStore(this.options.analyticsPath);
			this.lifecycleState = "running";
			const reconnectTask = this.runConnectionLoop();
			this.reconnectTask = reconnectTask;
			void reconnectTask.catch((cause: unknown) => this.report(toError(cause), "Hub reconnect loop"));
			return this;
		} catch (cause) {
			this.lifecycleState = "failed";
			await this.analytics?.close();
			this.analytics = undefined;
			throw cause;
		}
	}

	/**
	 * Waits until the Bridge and its existing shards acknowledge one Hub topology.
	 *
	 * @param timeoutMs - Maximum wait in milliseconds.
	 */
	public async waitUntilConnected(timeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS): Promise<void> {
		if (this.connectionReady) return;
		if (this.lifecycleState !== "running") throw new ShardingStateError(`BridgeClient is ${this.lifecycleState}.`);
		requirePositiveInteger(timeoutMs, "timeoutMs");
		if (this.connectionWaiters.size >= this.options.request.maxPending) {
			throw new ShardingCapacityError("Bridge connection waiter capacity reached.");
		}
		await new Promise<void>((resolve, reject) => {
			const waiter: $ConnectionWaiter = {
				reject,
				resolve,
				timer: setTimeout(() => {
					this.connectionWaiters.delete(waiter);
					reject(new ShardingTimeoutError(`Bridge did not synchronize within ${timeoutMs}ms.`));
				}, timeoutMs),
			};
			this.connectionWaiters.add(waiter);
			if (this.connectionReady) this.resolveConnectionWaiters();
		});
	}

	/**
	 * Registers a Bridge maintenance observer.
	 *
	 * @param listener - Callback invoked after each maintenance change.
	 * @returns Cleanup callback that removes the listener.
	 */
	public onMaintenanceChange(listener: (maintenance: boolean) => void): () => void {
		if (typeof listener !== "function") {
			throw new ShardingConfigurationError("Maintenance listener must be a function.");
		}
		if (!this.maintenanceListeners.has(listener) && this.maintenanceListeners.size >= MAX_MAINTENANCE_LISTENERS) {
			throw new ShardingCapacityError(`Maintenance listener limit of ${MAX_MAINTENANCE_LISTENERS} has been reached.`);
		}
		this.maintenanceListeners.add(listener);
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			this.maintenanceListeners.delete(listener);
		};
	}

	/**
	 * Reads retained local SQLite analytics.
	 *
	 * @param query - Optional newest-first limit and shard filter.
	 * @returns Persisted records.
	 */
	public async getAnalytics(query?: $BridgeAnalyticsQuery): Promise<readonly $AnalyticsRecord[]> {
		return this.requireAnalytics().read(query);
	}

	/**
	 * Deletes local analytics in bounded batches until the cutoff is clear.
	 *
	 * @param before - Inclusive Unix-millisecond cutoff.
	 * @param batchSize - Maximum rows deleted per transaction.
	 * @returns Total rows deleted.
	 */
	public async clearAnalytics(before = Date.now(), batchSize = 1_000): Promise<number> {
		requirePositiveInteger(batchSize, "batchSize", 10_000);
		if (!Number.isSafeInteger(before) || before < 0) {
			throw new ShardingConfigurationError("before must be a non-negative Unix millisecond.");
		}
		const analytics = this.requireAnalytics();
		let total = 0;
		while (true) {
			const removed = await analytics.clear(before, batchSize);
			total += removed;
			if (removed < batchSize) return total;
			await Bun.sleep(0);
		}
	}

	/**
	 * Stops reconnection, closes the Hub socket, terminates owned shard processes,
	 * rejects retained routing work, and closes SQLite.
	 */
	public stop(): Promise<void> {
		if (this.stopPromise !== undefined) return this.stopPromise;
		const operation = this.performStop();
		this.stopPromise = operation;
		void operation.then(
			() => {
				if (this.stopPromise === operation) this.stopPromise = undefined;
			},
			() => {
				if (this.stopPromise === operation) this.stopPromise = undefined;
			},
		);
		return operation;
	}

	/**
	 * Releases this Bridge when used with `await using`.
	 */
	public async [Symbol.asyncDispose](): Promise<void> {
		await this.stop();
	}

	protected async performStop(): Promise<void> {
		if (this.lifecycleState === "stopped") return;
		this.lifecycleState = "stopping";
		this.lifecycle.abort(new ShardingStateError("BridgeClient stopped."));
		const socket = this.socket;
		this.socket = undefined;
		try {
			socket?.close(1000, "Bridge shutdown");
		} catch {
			// Socket cleanup is best effort after lifecycle cancellation.
		}
		await this.setDisconnected(new ShardingStateError("BridgeClient stopped."));
		this.rejectConnectionWaiters(new ShardingStateError("BridgeClient stopped."));
		const failures: unknown[] = [];
		const reconnectTask = this.reconnectTask;
		if (reconnectTask !== undefined) {
			try {
				await reconnectTask;
			} catch (cause) {
				failures.push(cause);
			} finally {
				if (this.reconnectTask === reconnectTask) this.reconnectTask = undefined;
			}
		}
		await Promise.all(
			[...this.processes.values()].map(async (managed) => {
				try {
					const message = createWireMessage(
						"shard.control.shutdown",
						createRequestId(`shutdown-${managed.shardId}`),
						{ commandId: "bridge-stop", reason: "Bridge shutdown" },
						this.payloadPolicy,
					);
					await managed.stop(message);
					if (this.processes.get(managed.shardId) === managed) this.processes.delete(managed.shardId);
					this.shardInboundQueues.delete(managed);
				} catch (cause) {
					failures.push(cause);
				}
			}),
		);
		try {
			await this.analytics?.close();
			this.analytics = undefined;
		} catch (cause) {
			failures.push(cause);
		}
		if (failures.length === 0) {
			this.assignments.clear();
			this.nextProcessGeneration.clear();
			this.outbound.clear();
			this.inboundRoutes.clear();
			this.syncAcknowledgements.clear();
			this.shardInboundQueues.clear();
			this.maintenanceListeners.clear();
			this.synchronizedConnectionGeneration = 0;
			this.connectionTopologyVersion = 0;
			this.topologyVersion = 0;
			this.totalShards = 0;
		}
		this.lifecycleState = failures.length === 0 ? "stopped" : "failed";
		if (failures.length > 0) {
			throw new AggregateError(failures, "Bridge shutdown did not release every resource cleanly.");
		}
	}
}
