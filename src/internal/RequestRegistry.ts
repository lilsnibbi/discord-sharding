import {
	ShardingCapacityError,
	ShardingConfigurationError,
	ShardingStateError,
	ShardingTimeoutError,
} from "../errors/ShardingError";
import { MAX_IDENTIFIER_LENGTH, MAX_PENDING_REQUESTS, MAX_TIMER_MS } from "./limits";

interface PendingRequest<Value> {
	readonly reject: (error: unknown) => void;
	readonly resolve: (value: Value) => void;
	readonly timer: ReturnType<typeof setTimeout>;
}

export class RequestRegistry<Value> {
	readonly #defaultTimeoutMs: number;
	readonly #maxPending: number;
	readonly #pending = new Map<string, PendingRequest<Value>>();

	public constructor(defaultTimeoutMs: number, maxPending: number) {
		if (!Number.isSafeInteger(defaultTimeoutMs) || defaultTimeoutMs <= 0 || defaultTimeoutMs > MAX_TIMER_MS) {
			throw new ShardingConfigurationError(`Request timeout must be between 1 and ${MAX_TIMER_MS} milliseconds.`);
		}
		if (!Number.isSafeInteger(maxPending) || maxPending <= 0 || maxPending > MAX_PENDING_REQUESTS) {
			throw new ShardingConfigurationError(`Maximum pending requests must be between 1 and ${MAX_PENDING_REQUESTS}.`);
		}
		this.#defaultTimeoutMs = defaultTimeoutMs;
		this.#maxPending = maxPending;
	}

	public get size(): number {
		return this.#pending.size;
	}

	public register(id: string, timeoutMs = this.#defaultTimeoutMs): Promise<Value> {
		if (typeof id !== "string" || id.length === 0 || id.length > MAX_IDENTIFIER_LENGTH) {
			throw new ShardingConfigurationError(
				`Request ID must contain between 1 and ${MAX_IDENTIFIER_LENGTH} characters.`,
			);
		}
		if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > MAX_TIMER_MS) {
			throw new ShardingConfigurationError(`Request timeout must be between 1 and ${MAX_TIMER_MS} milliseconds.`);
		}
		if (this.#pending.has(id)) {
			throw new ShardingCapacityError(`Request ID ${id} is already pending.`);
		}
		if (this.#pending.size >= this.#maxPending) {
			throw new ShardingCapacityError(`Pending request limit of ${this.#maxPending} has been reached.`);
		}

		return new Promise<Value>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.#pending.delete(id);
				reject(new ShardingTimeoutError(`Request ${id} timed out after ${timeoutMs}ms.`));
			}, timeoutMs);
			this.#pending.set(id, { reject, resolve, timer });
		});
	}

	public settle(id: string, value: Value): boolean {
		const pending = this.#pending.get(id);
		if (pending === undefined) return false;
		this.#pending.delete(id);
		clearTimeout(pending.timer);
		pending.resolve(value);
		return true;
	}

	public reject(id: string, error: unknown): boolean {
		const pending = this.#pending.get(id);
		if (pending === undefined) return false;
		this.#pending.delete(id);
		clearTimeout(pending.timer);
		pending.reject(error);
		return true;
	}

	public rejectAll(error = new ShardingStateError("Request registry closed.")): void {
		for (const pending of this.#pending.values()) {
			clearTimeout(pending.timer);
			pending.reject(error);
		}
		this.#pending.clear();
	}
}
