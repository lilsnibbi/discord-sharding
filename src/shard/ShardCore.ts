import { heapStats } from "bun:jsc";
import { ShardingCapacityError, ShardingProtocolError, ShardingStateError } from "../errors/ShardingError";
import { serializeError } from "../internal/errors";
import { normalizePayload } from "../internal/payload";
import { RequestRegistry } from "../internal/RequestRegistry";
import { createRequestId } from "../internal/validation";
import { createWireMessage, requireExactKeys } from "../protocol/codec";
import { readBoolean, readInteger, readString } from "../protocol/readers";
import {
	type ParsedWireMessage,
	SHARD_TO_BRIDGE_TYPES,
	type WireDataMap,
	type WireMessageType,
} from "../protocol/types";
import type { $PayloadPolicy, $RequestPolicy } from "../types/common";
import type { $DiscordClient } from "../types/discord";
import type { $ShardBridge, $ShardClientOptions, $ShardClientState } from "../types/shard";
import {
	cacheSize,
	createShardConfiguration,
	finiteOrNull,
	integerOrNull,
	type ShardConfiguration,
	toError,
	withDeadline,
} from "./runtime";
import { ShardBridgeState } from "./ShardBridgeState";

const HEARTBEAT_INTERVAL_MS = 10_000;

export abstract class ShardCore<Client extends $DiscordClient> implements AsyncDisposable {
	/**
	 * Zero-based shard identifier assigned by the Hub.
	 */
	public readonly id: number;

	/**
	 * Total shard count configured for the bot.
	 */
	public readonly totalShards: number;

	/**
	 * Hub-issued ownership version for this shard process.
	 */
	public readonly assignmentEpoch: number;

	/**
	 * Local process version used to reject messages from older restarts.
	 */
	public readonly processGeneration: number;

	/**
	 * Application-owned Discord client for this shard.
	 */
	public readonly botClient: Client;

	/**
	 * Current Bridge maintenance state and change listener.
	 */
	public readonly bridge: $ShardBridge;

	protected readonly configuration: ShardConfiguration;
	protected readonly requestPolicy: $RequestPolicy;
	protected readonly payloadPolicy: $PayloadPolicy;
	protected readonly requests: RequestRegistry<ParsedWireMessage>;
	protected readonly lifecycle = new AbortController();
	protected stateValue: $ShardClientState = "idle";

	readonly #maintenance: ShardBridgeState;
	#removeTransportListener: (() => void) | undefined;
	#removeTransportDisconnectListener: (() => void) | undefined;
	#analyticsTimer: ReturnType<typeof setInterval> | undefined;
	#heartbeatTimer: ReturnType<typeof setInterval> | undefined;
	#readyTimer: ReturnType<typeof setInterval> | undefined;
	#readySent = false;
	#queuedInboundMessages = 0;
	#inboundQueue = Promise.resolve();

	/**
	 * Creates a shard runtime around an application-owned Discord client.
	 *
	 * @param botClient - Discord client configured for this shard.
	 * @param options - Optional identity, transport, limits, and lifecycle settings.
	 */
	public constructor(botClient: Client, options: $ShardClientOptions = {}) {
		const configuration = createShardConfiguration(botClient, options);
		this.configuration = configuration;
		this.botClient = botClient;
		this.id = configuration.id;
		this.totalShards = configuration.totalShards;
		this.assignmentEpoch = configuration.assignmentEpoch;
		this.processGeneration = configuration.processGeneration;
		this.requestPolicy = configuration.requestPolicy;
		this.payloadPolicy = configuration.payloadPolicy;
		this.requests = new RequestRegistry(this.requestPolicy.timeoutMs, this.requestPolicy.maxPending);
		this.#maintenance = new ShardBridgeState((error, context) => this.report(error, context));
		this.bridge = this.#maintenance;
	}

	/**
	 * Current ShardClient lifecycle state.
	 */
	public get state(): $ShardClientState {
		return this.stateValue;
	}

	/**
	 * Whether the wrapped Discord client currently reports ready.
	 */
	public get isReady(): boolean {
		return this.safeIsReady();
	}

	/**
	 * Number of outgoing requests currently waiting for responses.
	 */
	public get pendingRequests(): number {
		return this.requests.size;
	}

	/**
	 * Starts Bridge communication, maintenance updates, readiness checks, and analytics.
	 *
	 * @returns This client after the Bridge accepts its startup message.
	 */
	public async start(): Promise<this> {
		if (this.stateValue === "running") return this;
		if (this.stateValue !== "idle") throw new ShardingStateError(`Cannot start ShardClient from ${this.stateValue}.`);
		this.stateValue = "running";
		try {
			this.#removeTransportListener = this.configuration.transport.onMessage((message) =>
				this.#enqueueMessage(message),
			);
			this.#removeTransportDisconnectListener = this.configuration.transport.onDisconnect?.(() => {
				this.#handleTransportDisconnect();
			});
			if (this.stateValue !== "running") {
				throw new ShardingStateError("Shard IPC disconnected during startup.");
			}
			await this.sendWire("shard.booted", createRequestId(`boot-${this.id}`), {
				assignmentEpoch: this.assignmentEpoch,
				processGeneration: this.processGeneration,
				shardId: this.id,
				totalShards: this.totalShards,
			});
			await this.#sendHeartbeat();
			this.#heartbeatTimer = setInterval(() => {
				void this.#sendHeartbeat().catch((cause: unknown) => this.#failTransport(cause, "heartbeat"));
			}, HEARTBEAT_INTERVAL_MS);
			this.#readyTimer = setInterval(() => this.observeReady(), this.configuration.readyPollIntervalMs);
			if (this.configuration.analyticsIntervalMs !== false) {
				this.#analyticsTimer = setInterval(() => {
					void this.#emitAnalytics().catch((cause: unknown) => this.report(toError(cause), "analytics"));
				}, this.configuration.analyticsIntervalMs);
			}
			this.observeReady();
			return this;
		} catch (cause) {
			this.stateValue = "failed";
			this.#cleanupOwnedResources();
			throw cause;
		}
	}

	/**
	 * Stops this client and releases its listeners, timers, and pending work.
	 *
	 * Repeated calls are safe.
	 */
	public async close(): Promise<void> {
		if (this.stateValue === "closed" || this.stateValue === "closing") return;
		this.stateValue = "closing";
		this.lifecycle.abort(new ShardingStateError("ShardClient closed."));
		this.requests.rejectAll(new ShardingStateError("ShardClient closed."));
		this.#cleanupOwnedResources();
		this.#maintenance.update(true);
		this.#maintenance.clear();
		this.clearApplicationState();
		this.stateValue = "closed";
	}

	/**
	 * Calls {@link close} when the client is owned with `await using`.
	 */
	public async [Symbol.asyncDispose](): Promise<void> {
		await this.close();
	}

	protected abstract handleWireMessage(value: unknown): Promise<void>;

	protected abstract cleanupInboundResources(): void;

	protected abstract clearApplicationState(): void;

	protected async loginDiscordClient(token: string | undefined): Promise<string> {
		return this.configuration.discordClient.login(token);
	}

	protected settleRequest(message: ParsedWireMessage): void {
		this.requests.settle(message.id, message);
	}

	protected async handleMaintenanceMessage(message: ParsedWireMessage): Promise<void> {
		requireExactKeys(message.data, new Set(["acknowledge", "maintenance", "topologyVersion"]), `${message.type} data`);
		const acknowledge = readBoolean(message.data, "acknowledge");
		const maintenance = readBoolean(message.data, "maintenance");
		const topologyVersion = readInteger(message.data, "topologyVersion", 1);
		this.#maintenance.update(maintenance);
		if (acknowledge) await this.sendWire("shard.sync.ack", message.id, { topologyVersion });
	}

	protected async handleShutdownMessage(message: ParsedWireMessage): Promise<void> {
		requireExactKeys(message.data, new Set(["commandId", "reason"]), "shard.control.shutdown data");
		const commandId = readString(message.data, "commandId");
		readString(message.data, "reason", 512);
		const failures: unknown[] = [];
		if (this.configuration.onShutdown !== undefined) {
			try {
				await withDeadline(
					Promise.resolve(this.configuration.onShutdown()),
					this.requestPolicy.timeoutMs,
					this.lifecycle.signal,
					"Shard shutdown callback timed out.",
				);
			} catch (cause) {
				failures.push(cause);
			}
		}
		try {
			this.configuration.discordClient.destroy();
		} catch (cause) {
			failures.push(cause);
		}
		if (failures.length === 0) {
			try {
				await this.sendWire("shard.shutdown.complete", message.id, { commandId });
			} catch (cause) {
				failures.push(cause);
			}
		}
		const ownsProcess = this.configuration.transport.ownsProcess;
		await this.close();
		if (failures.length > 0) {
			const error = new AggregateError(failures, "Shard shutdown cleanup failed.");
			this.report(error, "shutdown cleanup");
			if (ownsProcess) setTimeout(() => process.exit(1), 0);
			throw error;
		}
		if (ownsProcess) setTimeout(() => process.exit(0), 0);
	}

	protected async sendWire<Type extends WireMessageType>(
		type: Type,
		id: string,
		data: WireDataMap[Type],
	): Promise<void> {
		if (!SHARD_TO_BRIDGE_TYPES.has(type)) {
			throw new ShardingProtocolError(`${type} cannot be sent from a shard.`);
		}
		const message = createWireMessage(type, id, data, this.payloadPolicy);
		await withDeadline(
			Promise.resolve(this.configuration.transport.send(message)),
			this.requestPolicy.timeoutMs,
			this.lifecycle.signal,
			`Shard IPC send for ${type} timed out.`,
		);
	}

	protected async sendFailure(
		type: "shard.eval.prepared" | "shard.eval.result" | "shard.route.response",
		id: string,
		cause: unknown,
	): Promise<void> {
		const error = serializeError(cause);
		if (type === "shard.eval.prepared") {
			await this.sendWire(type, id, { error, ok: false });
			return;
		}
		await this.sendWire(type, id, { error, ok: false });
	}

	protected assertRunning(): void {
		if (this.stateValue !== "running") throw new ShardingStateError(`ShardClient is ${this.stateValue}.`);
	}

	protected safeIsReady(): boolean {
		try {
			return this.configuration.discordClient.isReady();
		} catch (cause) {
			this.report(toError(cause), "discord.js readiness");
			return false;
		}
	}

	protected observeReady(): void {
		if (this.#readySent || this.stateValue !== "running" || !this.safeIsReady()) return;
		this.#readySent = true;
		void this.sendWire("shard.ready", createRequestId(`ready-${this.id}`), {
			assignmentEpoch: this.assignmentEpoch,
			processGeneration: this.processGeneration,
			shardId: this.id,
		}).catch((cause: unknown) => this.report(toError(cause), "ready notification"));
	}

	protected report(error: Error, context: string): void {
		try {
			this.configuration.onError?.(error, context);
		} catch {
			// Error observers cannot own or recurse into client lifecycle.
		}
	}

	async #sendHeartbeat(): Promise<void> {
		if (this.stateValue !== "running") return;
		await this.sendWire("shard.heartbeat", createRequestId(`heartbeat-${this.id}`), {
			assignmentEpoch: this.assignmentEpoch,
			processGeneration: this.processGeneration,
			shardId: this.id,
		});
	}

	async #emitAnalytics(): Promise<void> {
		if (this.stateValue !== "running") return;
		const heap = heapStats();
		const payload = normalizePayload(
			{
				discord: {
					channels: cacheSize(this.botClient.channels),
					guilds: cacheSize(this.botClient.guilds),
					ping: finiteOrNull(this.botClient.ws?.ping),
					ready: this.safeIsReady(),
					status: integerOrNull(this.botClient.ws?.status),
					uptimeMs: finiteOrNull(this.botClient.uptime),
					users: cacheSize(this.botClient.users),
				},
				process: {
					extraMemoryBytes: heap.extraMemorySize,
					globalObjectCount: heap.globalObjectCount,
					heapCapacityBytes: heap.heapCapacity,
					heapSizeBytes: heap.heapSize,
					objectCount: heap.objectCount,
					protectedObjectCount: heap.protectedObjectCount,
					uptimeSeconds: Bun.nanoseconds() / 1_000_000_000,
				},
			},
			this.payloadPolicy,
			"analytics payload",
		);
		await this.sendWire("shard.analytics", createRequestId(`analytics-${this.id}`), {
			collectedAt: Date.now(),
			payload,
		});
	}

	#enqueueMessage(value: unknown): void {
		if (this.stateValue !== "running" && this.stateValue !== "closing") return;
		if (this.#queuedInboundMessages >= this.requestPolicy.maxPending) {
			this.#failTransport(new ShardingCapacityError("Shard inbound IPC capacity reached."), "inbound capacity");
			return;
		}
		this.#queuedInboundMessages += 1;
		this.#inboundQueue = this.#inboundQueue
			.then(async () => {
				if (this.stateValue !== "running" && this.stateValue !== "closing") return;
				await this.handleWireMessage(value);
			})
			.catch((cause: unknown) => this.#failTransport(cause, "message"))
			.finally(() => {
				this.#queuedInboundMessages -= 1;
			});
	}

	#cleanupOwnedResources(): void {
		if (this.#analyticsTimer !== undefined) clearInterval(this.#analyticsTimer);
		if (this.#heartbeatTimer !== undefined) clearInterval(this.#heartbeatTimer);
		if (this.#readyTimer !== undefined) clearInterval(this.#readyTimer);
		this.#analyticsTimer = undefined;
		this.#heartbeatTimer = undefined;
		this.#readyTimer = undefined;
		const remove = this.#removeTransportListener;
		this.#removeTransportListener = undefined;
		try {
			remove?.();
		} catch (cause) {
			this.report(toError(cause), "IPC listener cleanup");
		}
		const removeDisconnect = this.#removeTransportDisconnectListener;
		this.#removeTransportDisconnectListener = undefined;
		try {
			removeDisconnect?.();
		} catch (cause) {
			this.report(toError(cause), "IPC disconnect listener cleanup");
		}
		this.cleanupInboundResources();
	}

	#handleTransportDisconnect(): void {
		this.#failTransport(new ShardingStateError("Bridge IPC channel disconnected."), "disconnect");
	}

	#failTransport(cause: unknown, context: string): void {
		if (this.stateValue !== "running") return;
		const error = toError(cause);
		this.report(error, `IPC ${context}`);
		this.stateValue = "failed";
		this.lifecycle.abort(error);
		this.requests.rejectAll(new ShardingStateError("Bridge IPC transport failed.", { cause: error }));
		this.#cleanupOwnedResources();
		this.#maintenance.update(true);
		this.#maintenance.clear();
		this.clearApplicationState();
		try {
			this.configuration.discordClient.destroy();
		} catch (destroyCause) {
			this.report(toError(destroyCause), "discord.js cleanup after IPC failure");
		}
		if (this.configuration.transport.ownsProcess) setTimeout(() => process.exit(1), 0);
	}
}
