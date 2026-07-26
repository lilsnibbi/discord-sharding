import { ShardingCapacityError, ShardingConfigurationError, ShardingStateError } from "../../errors/ShardingError";
import { assertConfigurationKeys, snapshotConfigurationRecord } from "../../internal/configuration";
import { MAX_PENDING_REQUESTS, MAX_SHARDS, MAX_TIMER_MS } from "../../internal/limits";
import { abortableSleep } from "../../internal/sleep";
import type { $Sleep } from "../../types/common";
import type { $GatewaySessionStartLimit } from "../../types/hub";
import type { $IdentifyOperation, $IdentifySchedulerOptions } from "./types";

const IDENTIFY_BUCKET_INTERVAL_MS = 5_000;
const DEFAULT_MAX_PENDING = 10_000;
const OPTION_KEYS = new Set(["maxPending", "now", "sleep"]);
const WAKE_REASON = Object.freeze({ kind: "identify-scheduler-wake" });

interface PendingIdentify {
	readonly id: number;
	readonly shardId: number;
	readonly grant: () => void;
	readonly cancel: (reason: unknown) => void;
	readonly removeAbortListener: () => void;
}

interface SchedulerConfiguration {
	readonly maxPending: number;
	readonly now: () => number;
	readonly sleep: $Sleep;
}

/**
 * Grants actual Discord login operations within global session and bucket limits.
 *
 * Different identify buckets may be granted together. Operations in the same
 * `shardId % maxConcurrency` bucket begin at least five seconds apart.
 */
export class IdentifyScheduler {
	readonly #maxConcurrency: number;
	readonly #maxPending: number;
	readonly #nextBucketGrantAt: number[];
	readonly #now: () => number;
	readonly #pending: PendingIdentify[] = [];
	readonly #resetAfterMs: number;
	readonly #sleep: $Sleep;
	readonly #total: number;
	#closed = false;
	#lastNow = Number.NEGATIVE_INFINITY;
	#nextRequestId = 1;
	#processing = false;
	#remaining: number;
	#resetAt: number;
	#waitController: AbortController | null = null;

	/**
	 * Creates a scheduler from validated Discord Gateway session metadata.
	 *
	 * @param limit - Current Discord session-start allowance.
	 * @param options - Queue bound and deterministic clock dependencies.
	 */
	public constructor(limit: $GatewaySessionStartLimit, options: $IdentifySchedulerOptions = {}) {
		const session = parseSessionLimit(limit);
		const configuration = parseOptions(options);
		this.#total = session.total;
		this.#remaining = session.remaining;
		this.#resetAfterMs = session.reset_after;
		this.#maxConcurrency = session.max_concurrency;
		this.#maxPending = configuration.maxPending;
		this.#now = configuration.now;
		this.#sleep = configuration.sleep;

		const startedAt = this.#readNow();
		this.#resetAt = startedAt + this.#resetAfterMs;
		this.#nextBucketGrantAt = Array.from({ length: this.#maxConcurrency }, () => startedAt);
	}

	/**
	 * Queues an actual Discord login operation for its identify bucket.
	 *
	 * Cancellation applies only while the operation is waiting. Once granted,
	 * ownership of cancellation belongs to the login operation.
	 *
	 * @param shardId - Zero-based Discord shard identifier.
	 * @param operation - Login operation called immediately after a grant.
	 * @param signal - Optional cancellation while waiting.
	 * @returns The login operation's result.
	 */
	public schedule<Result>(
		shardId: number,
		operation: $IdentifyOperation<Result>,
		signal?: AbortSignal,
	): Promise<Result> {
		if (this.#closed) {
			return Promise.reject(new ShardingStateError("Identify scheduler is closed."));
		}
		assertShardId(shardId);
		if (typeof operation !== "function") {
			throw new ShardingConfigurationError("Identify operation must be a function.");
		}
		if (signal !== undefined && !isAbortSignal(signal)) {
			throw new ShardingConfigurationError("Identify cancellation must be an AbortSignal.");
		}
		if (signal?.aborted) {
			return Promise.reject(createCancellationError(signal.reason));
		}
		if (this.#pending.length >= this.#maxPending) {
			return Promise.reject(new ShardingCapacityError(`Identify queue reached its ${this.#maxPending}-request limit.`));
		}

		const requestId = this.#nextRequestId;
		if (requestId >= Number.MAX_SAFE_INTEGER) {
			return Promise.reject(new ShardingCapacityError("Identify request identifiers are exhausted."));
		}
		this.#nextRequestId += 1;

		const promise = new Promise<Result>((resolve, reject) => {
			let settled = false;
			const settleRejected = (reason: unknown): void => {
				if (settled) return;
				settled = true;
				reject(reason);
			};
			const grant = (): void => {
				if (settled) return;
				settled = true;
				let result: Promise<Result>;
				try {
					result = Promise.resolve(operation());
				} catch (cause) {
					reject(cause);
					return;
				}
				void result.then(resolve, reject);
			};
			const cancel = (reason: unknown): void => {
				settleRejected(createCancellationError(reason));
			};
			let removeAbortListener = (): void => undefined;
			const pending: PendingIdentify = {
				id: requestId,
				shardId,
				grant,
				cancel,
				removeAbortListener: () => removeAbortListener(),
			};

			if (signal !== undefined) {
				const abort = (): void => {
					const removed = this.#removePending(requestId);
					if (!removed) return;
					removeAbortListener();
					cancel(signal.reason);
					this.#wake();
				};
				signal.addEventListener("abort", abort, { once: true });
				removeAbortListener = () => signal.removeEventListener("abort", abort);
				if (signal.aborted) {
					removeAbortListener();
					cancel(signal.reason);
					return;
				}
			}

			this.#pending.push(pending);
		});
		this.#ensureProcessing();
		this.#wake();
		return promise;
	}

	/**
	 * Stops granting new identifies and rejects every queued operation.
	 *
	 * Already granted login operations are not interrupted.
	 *
	 * @param reason - Optional shutdown reason retained as the cancellation cause.
	 */
	public close(reason?: unknown): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#wake();
		const pending = this.#pending.splice(0);
		for (const request of pending) {
			request.removeAbortListener();
			request.cancel(reason ?? new ShardingStateError("Identify scheduler was closed."));
		}
	}

	/**
	 * Number of login operations currently waiting for a grant.
	 */
	public get pendingCount(): number {
		return this.#pending.length;
	}

	/**
	 * Current locally tracked Discord session-start allowance.
	 */
	public get remaining(): number {
		return this.#remaining;
	}

	#ensureProcessing(): void {
		if (this.#processing || this.#closed || this.#pending.length === 0) return;
		this.#processing = true;
		void this.#process().finally(() => {
			this.#processing = false;
			if (!this.#closed && this.#pending.length > 0) this.#ensureProcessing();
		});
	}

	async #process(): Promise<void> {
		try {
			while (!this.#closed && this.#pending.length > 0) {
				const now = this.#readNow();
				this.#refreshSession(now);

				if (this.#remaining > 0) {
					const eligibleIndex = this.#findEligibleIndex(now);
					if (eligibleIndex !== -1) {
						this.#grant(eligibleIndex);
						continue;
					}
				}

				const wakeAt = this.#remaining === 0 ? this.#resetAt : this.#findNextBucketTime();
				const delay = Math.max(0, Math.ceil(wakeAt - now));
				await this.#wait(delay);
			}
		} catch (cause) {
			if (this.#closed) return;
			this.#closed = true;
			const error = cause instanceof Error ? cause : new ShardingStateError("Identify scheduler failed.", { cause });
			const pending = this.#pending.splice(0);
			for (const request of pending) {
				request.removeAbortListener();
				request.cancel(error);
			}
		}
	}

	#refreshSession(now: number): void {
		if (now < this.#resetAt) return;
		this.#remaining = this.#total;
		this.#resetAt = now + this.#resetAfterMs;
	}

	#findEligibleIndex(now: number): number {
		for (let index = 0; index < this.#pending.length; index += 1) {
			const request = this.#pending[index];
			if (request === undefined) continue;
			const bucket = request.shardId % this.#maxConcurrency;
			const nextGrantAt = this.#nextBucketGrantAt[bucket];
			if (nextGrantAt !== undefined && nextGrantAt <= now) return index;
		}
		return -1;
	}

	#findNextBucketTime(): number {
		let next = Number.POSITIVE_INFINITY;
		for (const request of this.#pending) {
			const bucket = request.shardId % this.#maxConcurrency;
			const nextGrantAt = this.#nextBucketGrantAt[bucket];
			if (nextGrantAt !== undefined && nextGrantAt < next) next = nextGrantAt;
		}
		if (!Number.isFinite(next)) {
			throw new ShardingStateError("Identify scheduler could not determine its next bucket deadline.");
		}
		return next;
	}

	#grant(index: number): void {
		const request = this.#pending[index];
		if (request === undefined) {
			throw new ShardingStateError("Identify scheduler selected a missing request.");
		}
		this.#pending.splice(index, 1);
		request.removeAbortListener();

		const now = this.#readNow();
		const bucket = request.shardId % this.#maxConcurrency;
		this.#nextBucketGrantAt[bucket] = now + IDENTIFY_BUCKET_INTERVAL_MS;
		this.#remaining -= 1;
		request.grant();
	}

	async #wait(milliseconds: number): Promise<void> {
		if (milliseconds > MAX_TIMER_MS) {
			throw new ShardingStateError(`Identify wait exceeded ${MAX_TIMER_MS}ms.`);
		}
		const controller = new AbortController();
		this.#waitController = controller;
		try {
			await this.#sleep(milliseconds, controller.signal);
		} catch (cause) {
			if (controller.signal.aborted && (controller.signal.reason === WAKE_REASON || this.#closed)) return;
			throw new ShardingStateError("Identify scheduler sleep failed.", { cause });
		} finally {
			if (this.#waitController === controller) this.#waitController = null;
		}
	}

	#wake(): void {
		const controller = this.#waitController;
		if (controller !== null && !controller.signal.aborted) controller.abort(WAKE_REASON);
	}

	#removePending(requestId: number): boolean {
		const index = this.#pending.findIndex((request) => request.id === requestId);
		if (index === -1) return false;
		this.#pending.splice(index, 1);
		return true;
	}

	#readNow(): number {
		let now: number;
		try {
			now = this.#now();
		} catch (cause) {
			throw new ShardingStateError("Identify monotonic clock failed.", { cause });
		}
		if (!Number.isFinite(now) || now < 0) {
			throw new ShardingStateError("Identify monotonic clock must return a finite non-negative number.");
		}
		if (now < this.#lastNow) {
			throw new ShardingStateError("Identify monotonic clock moved backwards.");
		}
		this.#lastNow = now;
		return now;
	}
}

function parseSessionLimit(value: unknown): $GatewaySessionStartLimit {
	const limit = snapshotConfigurationRecord(value, "Identify session limit");
	const total = requireInteger(limit.total, "Identify session limit.total", 1, MAX_SHARDS);
	const remaining = requireInteger(limit.remaining, "Identify session limit.remaining", 0, MAX_SHARDS);
	const resetAfter = requireInteger(limit.reset_after, "Identify session limit.reset_after", 0, MAX_TIMER_MS);
	const maxConcurrency = requireInteger(limit.max_concurrency, "Identify session limit.max_concurrency", 1, MAX_SHARDS);
	if (remaining > total) {
		throw new ShardingConfigurationError(
			"Identify session limit.remaining cannot be greater than Identify session limit.total.",
		);
	}
	return Object.freeze({
		total,
		remaining,
		reset_after: resetAfter,
		max_concurrency: maxConcurrency,
	});
}

function parseOptions(value: unknown): SchedulerConfiguration {
	const options = snapshotConfigurationRecord(value, "Identify scheduler options");
	assertConfigurationKeys(options, OPTION_KEYS, "Identify scheduler options");
	const maxPending = options.maxPending ?? DEFAULT_MAX_PENDING;
	if (
		typeof maxPending !== "number" ||
		!Number.isSafeInteger(maxPending) ||
		maxPending <= 0 ||
		maxPending > MAX_PENDING_REQUESTS
	) {
		throw new ShardingConfigurationError(
			`Identify scheduler options.maxPending must be a positive integer no greater than ${MAX_PENDING_REQUESTS}.`,
		);
	}
	const now = options.now ?? performance.now.bind(performance);
	if (!isClock(now)) {
		throw new ShardingConfigurationError("Identify scheduler options.now must be a function.");
	}
	const sleep = options.sleep ?? abortableSleep;
	if (!isSleep(sleep)) {
		throw new ShardingConfigurationError("Identify scheduler options.sleep must be a function.");
	}
	return Object.freeze({ maxPending, now, sleep });
}

function requireInteger(value: unknown, name: string, minimum: number, maximum: number): number {
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
		const description = minimum === 0 ? "a non-negative" : "a positive";
		throw new ShardingConfigurationError(`${name} must be ${description} integer no greater than ${maximum}.`);
	}
	return value;
}

function assertShardId(value: number): void {
	if (!Number.isSafeInteger(value) || value < 0 || value >= MAX_SHARDS) {
		throw new ShardingConfigurationError(`Identify shardId must be a non-negative integer less than ${MAX_SHARDS}.`);
	}
}

function createCancellationError(reason: unknown): ShardingStateError {
	return new ShardingStateError("Identify request was cancelled.", {
		...(reason === undefined ? {} : { cause: reason }),
	});
}

function isAbortSignal(value: unknown): value is AbortSignal {
	try {
		return value instanceof AbortSignal;
	} catch {
		return false;
	}
}

function isClock(value: unknown): value is () => number {
	return typeof value === "function";
}

function isSleep(value: unknown): value is $Sleep {
	return typeof value === "function";
}
