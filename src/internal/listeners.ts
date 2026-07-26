import { ShardingCapacityError, ShardingConfigurationError } from "../errors/ShardingError";

export class ListenerSet<Listener extends (...parameters: never[]) => unknown> {
	readonly #listeners = new Set<Listener>();
	readonly #maximum: number;

	public constructor(maximum: number) {
		if (!Number.isSafeInteger(maximum) || maximum <= 0) {
			throw new ShardingConfigurationError("Listener limit must be a positive integer.");
		}
		this.#maximum = maximum;
	}

	public get size(): number {
		return this.#listeners.size;
	}

	public add(listener: Listener): () => void {
		if (typeof listener !== "function") throw new ShardingConfigurationError("Listener must be a function.");
		if (!this.#listeners.has(listener) && this.#listeners.size >= this.#maximum) {
			throw new ShardingCapacityError(`Listener limit of ${this.#maximum} has been reached.`);
		}
		this.#listeners.add(listener);
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			this.#listeners.delete(listener);
		};
	}

	public forEach(callback: (listener: Listener) => void): void {
		for (const listener of [...this.#listeners]) callback(listener);
	}

	public clear(): void {
		this.#listeners.clear();
	}
}
