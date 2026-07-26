import { ShardingCapacityError, ShardingConfigurationError } from "../errors/ShardingError";
import type { $ShardBridge } from "../types/shard";
import { MAX_SHARD_LISTENERS, toError } from "./runtime";

export class ShardBridgeState implements $ShardBridge {
	#maintenance = true;
	readonly #listeners = new Set<(maintenance: boolean) => void>();
	readonly #report: (error: Error, context: string) => void;

	public constructor(report: (error: Error, context: string) => void) {
		this.#report = report;
	}

	public get isInMaintenance(): boolean {
		return this.#maintenance;
	}

	public onMaintenanceChange(listener: (maintenance: boolean) => void): () => void {
		if (typeof listener !== "function") {
			throw new ShardingConfigurationError("Maintenance listener must be a function.");
		}
		if (!this.#listeners.has(listener) && this.#listeners.size >= MAX_SHARD_LISTENERS) {
			throw new ShardingCapacityError(`Maintenance listener limit of ${MAX_SHARD_LISTENERS} has been reached.`);
		}
		this.#listeners.add(listener);
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			this.#listeners.delete(listener);
		};
	}

	public update(maintenance: boolean): void {
		if (maintenance === this.#maintenance) return;
		this.#maintenance = maintenance;
		for (const listener of [...this.#listeners]) {
			try {
				const result: unknown = listener(maintenance);
				if (result instanceof Promise) {
					void result.catch((cause: unknown) => this.#report(toError(cause), "maintenance listener"));
				}
			} catch (cause) {
				this.#report(toError(cause), "maintenance listener");
			}
		}
	}

	public clear(): void {
		this.#listeners.clear();
	}
}
