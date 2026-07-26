import { ShardingConfigurationError, ShardingProtocolError, ShardingStateError } from "../errors/ShardingError";
import { remoteError } from "../internal/errors";
import { MAX_SHARDS } from "../internal/limits";
import { normalizePayload } from "../internal/payload";
import { createRequestId, requireShardId } from "../internal/validation";
import { requireExactKeys, requireOptionalKeys } from "../protocol/codec";
import { readArray, readBoolean, readError, readPayload, readShardId } from "../protocol/readers";
import type { $DiscordClient } from "../types/discord";
import type { $BroadcastEvaluator } from "../types/shard";
import { copyRecord, validateEvalResponse, validateResponse } from "./runtime";
import { ShardInbound } from "./ShardInbound";

const ROUTE_RESPONSE_SUCCESS_KEYS = new Set(["value"]);

/**
 * Connects one application-owned Discord client to its Bridge.
 *
 * Create one instance inside each shard process. Call {@link start}, then use
 * {@link login} instead of calling the Discord client login method directly.
 */
export class ShardClient<Client extends $DiscordClient = $DiscordClient> extends ShardInbound<Client> {
	#loginPromise: Promise<string> | undefined;

	/**
	 * Waits for a Hub identify grant, then logs in the Discord client.
	 *
	 * @param token - Optional token passed only to the local Discord client.
	 * @returns The token result returned by the Discord client.
	 */
	public login(token?: string): Promise<string> {
		if (token !== undefined && (typeof token !== "string" || token.length === 0)) {
			throw new ShardingConfigurationError("login token must be a non-empty string when supplied.");
		}
		if (this.#loginPromise !== undefined) return this.#loginPromise;
		this.#loginPromise = this.#performLogin(token);
		void this.#loginPromise.catch(() => {
			this.#loginPromise = undefined;
		});
		return this.#loginPromise;
	}

	/**
	 * Sends a one-way message to another shard through the Hub.
	 *
	 * @param targetShardId - Shard that should receive the message.
	 * @param payload - JSON-compatible value to send.
	 */
	public async send(targetShardId: number, payload: unknown): Promise<void> {
		await this.#route(targetShardId, "message", payload);
	}

	/**
	 * Sends a request to another shard and waits for its handler response.
	 *
	 * @typeParam Result - Response type expected by the caller.
	 * @param targetShardId - Shard that should handle the request.
	 * @param payload - JSON-compatible request value.
	 * @param timeoutMs - Optional maximum wait in milliseconds.
	 * @returns Value returned by the destination request handler.
	 */
	public async request<Result = unknown>(targetShardId: number, payload: unknown, timeoutMs?: number): Promise<Result> {
		const value = await this.#route(targetShardId, "request", payload, timeoutMs);
		return value as Result;
	}

	/**
	 * Runs a trusted function on every shard that is Discord-ready when the call begins.
	 *
	 * The function is not sandboxed. Pass only developer-written code, never user
	 * or network input. Its context and return value must be JSON-compatible.
	 *
	 * @typeParam Context - Serializable context type.
	 * @typeParam Result - Serializable result type.
	 * @param evaluator - Trusted function to run on each ready shard.
	 * @param context - Optional JSON-compatible context.
	 * @param timeoutMs - Optional maximum wait in milliseconds.
	 * @returns Read-only results keyed by shard identifier.
	 */
	public async broadcastEval<Context = null, Result = unknown>(
		evaluator: $BroadcastEvaluator<Client, Context, Result>,
		context?: Context,
		timeoutMs?: number,
	): Promise<ReadonlyMap<number, Awaited<Result>>> {
		this.assertRunning();
		if (typeof evaluator !== "function") throw new ShardingConfigurationError("evaluator must be a function.");
		const source = Function.prototype.toString.call(evaluator);
		if (source.length === 0 || source.length > this.configuration.maxEvaluatorSourceLength) {
			throw new ShardingConfigurationError(
				`Evaluator source must contain between 1 and ${this.configuration.maxEvaluatorSourceLength} characters.`,
			);
		}
		const normalizedContext = normalizePayload(
			context === undefined ? null : context,
			this.payloadPolicy,
			"eval context",
		);
		const id = createRequestId(`eval-${this.id}`);
		const response = this.requests.register(id, timeoutMs);
		void response.catch(() => undefined);
		try {
			await this.sendWire("shard.eval.request", id, {
				context: normalizedContext,
				evaluator: source,
			});
		} catch (cause) {
			this.requests.reject(id, cause);
			throw cause;
		}
		const message = await response;
		const data = message.data;
		validateEvalResponse(data);
		if (!readBoolean(data, "ok")) throw remoteError(readError(data.error, "eval response error"));
		const entries = readArray(data, "results", MAX_SHARDS);
		const results = new Map<number, Awaited<Result>>();
		for (const entry of entries) {
			if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
				throw new ShardingProtocolError("Evaluation result entry must be an object.");
			}
			const record = copyRecord(entry, "Evaluation result entry");
			requireExactKeys(record, new Set(["shardId", "value"]), "Evaluation result entry");
			const shardId = readShardId(record);
			if (results.has(shardId)) throw new ShardingProtocolError(`Duplicate evaluation result for shard ${shardId}.`);
			results.set(shardId, readPayload(record, "value") as Awaited<Result>);
		}
		return results;
	}

	async #performLogin(token: string | undefined): Promise<string> {
		if (this.stateValue === "idle") await this.start();
		this.assertRunning();
		if (this.safeIsReady()) {
			throw new ShardingStateError("discord.js is already ready; its identify was not granted by this ShardClient.");
		}
		const id = createRequestId(`identify-${this.id}`);
		const response = this.requests.register(id);
		void response.catch(() => undefined);
		try {
			await this.sendWire("shard.identify.request", id, {});
		} catch (cause) {
			this.requests.reject(id, cause);
			throw cause;
		}
		const message = await response;
		const data = message.data;
		requireOptionalKeys(data, new Set(["granted"]), new Set(["error"]), "shard.control.identify.response data");
		if (!readBoolean(data, "granted")) {
			throw remoteError(readError(data.error, "identify response error"));
		}
		if (Object.hasOwn(data, "error")) {
			throw new ShardingProtocolError("Granted identify response cannot include error.");
		}
		const result = await this.loginDiscordClient(token);
		this.observeReady();
		return result;
	}

	async #route(
		targetShardId: number,
		kind: "message" | "request",
		payload: unknown,
		timeoutMs?: number,
	): Promise<unknown> {
		this.assertRunning();
		requireShardId(targetShardId, "targetShardId");
		if (targetShardId >= this.totalShards) {
			throw new ShardingConfigurationError("targetShardId must be less than totalShards.");
		}
		const normalized = normalizePayload(payload, this.payloadPolicy, "route payload");
		const id = createRequestId(`route-${this.id}`);
		const response = this.requests.register(id, timeoutMs);
		void response.catch(() => undefined);
		try {
			await this.sendWire("shard.route.request", id, {
				kind,
				payload: normalized,
				targetShardId,
			});
		} catch (cause) {
			this.requests.reject(id, cause);
			throw cause;
		}
		const message = await response;
		const data = message.data;
		validateResponse(data, "route response", ROUTE_RESPONSE_SUCCESS_KEYS);
		if (!readBoolean(data, "ok")) throw remoteError(readError(data.error, "route response error"));
		return Object.hasOwn(data, "value") ? readPayload(data, "value") : null;
	}
}
