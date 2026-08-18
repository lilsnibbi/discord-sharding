import {
	ShardingCapacityError,
	ShardingConfigurationError,
	ShardingProtocolError,
	ShardingStateError,
} from "../errors/ShardingError";
import { MAX_TIMER_MS } from "../internal/limits";
import { normalizePayload } from "../internal/payload";
import { abortableSleep } from "../internal/sleep";
import { parseWireMessage, requireExactKeys } from "../protocol/codec";
import {
	readEvaluator,
	readInteger,
	readNullableShardId,
	readPayload,
	readRouteKind,
	readShardId,
	readString,
} from "../protocol/readers";
import { BRIDGE_TO_SHARD_TYPES, type ParsedWireMessage } from "../protocol/types";
import type { $DiscordClient } from "../types/discord";
import type { $ShardMessageContext, $ShardMessageListener, $ShardRequestHandler } from "../types/shard";
import { MAX_SHARD_LISTENERS, toError } from "./runtime";
import { ShardCore } from "./ShardCore";

type EvaluatorFunction<Client extends $DiscordClient> = (client: Client, context: unknown) => unknown;

interface PreparedEvaluation<Client extends $DiscordClient> {
	readonly evaluator: EvaluatorFunction<Client>;
	readonly context: unknown;
	readonly timer: ReturnType<typeof setTimeout>;
}

export class ShardInbound<Client extends $DiscordClient> extends ShardCore<Client> {
	readonly #messageListeners = new Set<$ShardMessageListener>();
	readonly #handlerTasks = new Set<Promise<void>>();
	readonly #prepared = new Map<string, PreparedEvaluation<Client>>();
	#requestHandler: $ShardRequestHandler | undefined = this.configuration.onRequest;
	#activeHandlers = 0;
	#activeEvaluations = 0;

	/**
	 * Number of incoming message or request handlers currently running.
	 */
	public get activeHandlers(): number {
		return this.#activeHandlers;
	}

	/**
	 * Registers a listener for one-way messages sent to this shard.
	 *
	 * @param listener - Callback that handles each incoming message.
	 * @returns Cleanup callback that removes the listener.
	 */
	public onMessage(listener: $ShardMessageListener): () => void {
		if (typeof listener !== "function") throw new ShardingConfigurationError("Message listener must be a function.");
		if (!this.#messageListeners.has(listener) && this.#messageListeners.size >= MAX_SHARD_LISTENERS) {
			throw new ShardingCapacityError(`Message listener limit of ${MAX_SHARD_LISTENERS} has been reached.`);
		}
		this.#messageListeners.add(listener);
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			this.#messageListeners.delete(listener);
		};
	}

	/**
	 * Registers the single request handler for this shard.
	 *
	 * @param handler - Callback that handles incoming requests.
	 * @returns Cleanup callback that removes the handler.
	 */
	public onRequest(handler: $ShardRequestHandler): () => void {
		if (typeof handler !== "function") throw new ShardingConfigurationError("Request handler must be a function.");
		if (this.#requestHandler !== undefined) {
			throw new ShardingStateError("A targeted request handler is already installed.");
		}
		this.#requestHandler = handler;
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			if (this.#requestHandler === handler) this.#requestHandler = undefined;
		};
	}

	protected override async handleWireMessage(value: unknown): Promise<void> {
		if (this.stateValue !== "running" && this.stateValue !== "closing") return;
		const message = parseWireMessage(value, BRIDGE_TO_SHARD_TYPES, this.payloadPolicy);
		switch (message.type) {
			case "shard.control.maintenance":
				await this.handleMaintenanceMessage(message);
				return;
			case "shard.control.topology":
				this.handleTopologyMessage(message);
				return;
			case "shard.control.identify.response":
			case "shard.control.route.response":
			case "shard.control.eval.response":
				this.settleRequest(message);
				return;
			case "shard.control.route.request":
				this.#admitRouteRequest(message);
				return;
			case "shard.control.eval.prepare":
				await this.#handleEvalPrepare(message);
				return;
			case "shard.control.eval.commit":
				this.#handleEvalCommit(message);
				return;
			case "shard.control.eval.cancel":
				this.#handleEvalCancel(message);
				return;
			case "shard.control.shutdown":
				await this.handleShutdownMessage(message);
				return;
			default:
				throw new ShardingProtocolError(`Unhandled shard control message ${message.type}.`);
		}
	}

	protected override cleanupInboundResources(): void {
		for (const prepared of this.#prepared.values()) clearTimeout(prepared.timer);
		this.#prepared.clear();
		this.#handlerTasks.clear();
	}

	protected override clearApplicationState(): void {
		this.#messageListeners.clear();
		this.#requestHandler = undefined;
	}

	#admitRouteRequest(message: ParsedWireMessage): void {
		requireExactKeys(message.data, new Set(["kind", "payload", "sourceShardId"]), "shard.control.route.request data");
		if (this.#activeHandlers >= this.requestPolicy.maxPending) {
			void this.sendFailure(
				"shard.route.response",
				message.id,
				new ShardingCapacityError("Shard handler capacity reached."),
			).catch((cause: unknown) => this.report(toError(cause), "route capacity response"));
			return;
		}
		const kind = readRouteKind(message.data);
		const payload = readPayload(message.data, "payload");
		const sourceShardId = readNullableShardId(message.data, "sourceShardId");
		const context: $ShardMessageContext = Object.freeze({
			signal: this.lifecycle.signal,
			sourceShardId,
		});
		this.#activeHandlers += 1;
		const task = this.#runRouteRequest(message.id, kind, payload, context);
		this.#handlerTasks.add(task);
		void task
			.catch((cause: unknown) => this.report(toError(cause), "route response"))
			.finally(() => this.#handlerTasks.delete(task));
	}

	async #runRouteRequest(
		id: string,
		kind: "message" | "request",
		payload: unknown,
		context: $ShardMessageContext,
	): Promise<void> {
		try {
			if (kind === "message") {
				for (const listener of [...this.#messageListeners]) {
					await listener(payload, context);
				}
				if (this.stateValue === "running") {
					await this.sendWire("shard.route.response", id, { ok: true, value: null });
				}
				return;
			}
			if (this.#requestHandler === undefined) throw new ShardingStateError("Destination shard has no request handler.");
			const result = await this.#requestHandler(payload, context);
			const normalized = normalizePayload(result, this.payloadPolicy, "route response");
			if (this.stateValue === "running") {
				await this.sendWire("shard.route.response", id, { ok: true, value: normalized });
			}
		} catch (cause) {
			if (this.stateValue === "running") await this.sendFailure("shard.route.response", id, cause);
		} finally {
			this.#activeHandlers -= 1;
		}
	}

	async #handleEvalPrepare(message: ParsedWireMessage): Promise<void> {
		requireExactKeys(
			message.data,
			new Set(["context", "evaluator", "sourceShardId"]),
			"shard.control.eval.prepare data",
		);
		try {
			if (this.#prepared.has(message.id)) throw new ShardingStateError("Evaluation is already prepared.");
			if (this.#prepared.size + this.#activeEvaluations >= this.configuration.maxPreparedEvaluations) {
				throw new ShardingCapacityError("Prepared evaluation capacity reached.");
			}
			readShardId(message.data, "sourceShardId");
			const source = readEvaluator(message.data);
			if (source.length > this.configuration.maxEvaluatorSourceLength) {
				throw new ShardingProtocolError(
					`Evaluator source exceeds ${this.configuration.maxEvaluatorSourceLength} characters.`,
				);
			}
			const context = readPayload(message.data, "context");
			const candidate: unknown = Function(`"use strict"; return (${source});`)();
			if (typeof candidate !== "function") {
				throw new ShardingProtocolError("Evaluator source did not produce a function.");
			}
			const evaluator = (client: Client, evaluationContext: unknown): unknown =>
				Reflect.apply(candidate, undefined, [client, evaluationContext]);
			const timer = setTimeout(
				() => this.#prepared.delete(message.id),
				Math.min(MAX_TIMER_MS, this.requestPolicy.timeoutMs * 2),
			);
			this.#prepared.set(message.id, { context, evaluator, timer });
			await this.sendWire("shard.eval.prepared", message.id, { ok: true });
		} catch (cause) {
			await this.sendFailure("shard.eval.prepared", message.id, cause);
		}
	}

	#handleEvalCommit(message: ParsedWireMessage): void {
		requireExactKeys(message.data, new Set(["executeAt"]), "shard.control.eval.commit data");
		const executeAt = readInteger(message.data, "executeAt", 0, Number.MAX_SAFE_INTEGER);
		const prepared = this.#prepared.get(message.id);
		if (prepared === undefined) {
			void this.sendFailure(
				"shard.eval.result",
				message.id,
				new ShardingStateError("Evaluation was not prepared or already expired."),
			);
			return;
		}
		const delay = executeAt - Date.now();
		if (delay > MAX_TIMER_MS) {
			this.#prepared.delete(message.id);
			clearTimeout(prepared.timer);
			void this.sendFailure(
				"shard.eval.result",
				message.id,
				new ShardingProtocolError(`Evaluation schedule cannot exceed ${MAX_TIMER_MS} milliseconds.`),
			);
			return;
		}
		this.#prepared.delete(message.id);
		clearTimeout(prepared.timer);
		this.#activeEvaluations += 1;
		void this.#executeEvaluation(message.id, prepared, executeAt);
	}

	#handleEvalCancel(message: ParsedWireMessage): void {
		requireExactKeys(message.data, new Set(["reason"]), "shard.control.eval.cancel data");
		readString(message.data, "reason", 512);
		const prepared = this.#prepared.get(message.id);
		if (prepared === undefined) return;
		clearTimeout(prepared.timer);
		this.#prepared.delete(message.id);
	}

	async #executeEvaluation(id: string, prepared: PreparedEvaluation<Client>, executeAt: number): Promise<void> {
		try {
			const delay = Math.max(0, executeAt - Date.now());
			await abortableSleep(delay, this.lifecycle.signal);
			if (!this.safeIsReady()) throw new ShardingStateError("Shard is not Discord-ready at evaluation commit.");
			const result = await prepared.evaluator(this.botClient, prepared.context);
			const normalized = normalizePayload(result, this.payloadPolicy, "evaluation result");
			if (this.stateValue === "running") {
				await this.sendWire("shard.eval.result", id, { ok: true, value: normalized });
			}
		} catch (cause) {
			if (this.stateValue === "running") await this.sendFailure("shard.eval.result", id, cause);
		} finally {
			this.#activeEvaluations -= 1;
		}
	}
}
