import {
	type $AnalyticsRecord,
	type $BridgeClientOptions,
	type $BridgeSocketFactory,
	type $DiscordClient,
	type $HubPersistence,
	type $PersistedAssignment,
	type $PersistedBridge,
	type $PersistedHubState,
	type $PersistedShard,
	type $ShardMessageContext,
	type $ShardProcess,
	type $ShardProcessContext,
	type $ShardProcessExit,
	type $ShardProcessFactory,
	type $ShardTransport,
	BridgeClient,
	HubClient,
	ShardClient,
} from "../../src/index";

interface Signal {
	readonly promise: Promise<void>;
	readonly resolve: () => void;
}

interface ReceivedMessage {
	readonly payload: unknown;
	readonly shardId: number;
	readonly sourceShardId: number | null;
}

export interface ReportedError {
	readonly context: string;
	readonly error: Error;
}

const BRIDGE_TOKEN = "0123456789abcdef";
export const UNREADY_SHARD_ID = 3;

let nextProcessId = 40_000;

class MemoryHubPersistence implements $HubPersistence {
	readonly #analytics: $AnalyticsRecord[] = [];
	readonly #assignments = new Map<number, $PersistedAssignment>();
	readonly #bridges = new Map<string, $PersistedBridge>();
	readonly #shards = new Map<number, $PersistedShard>();

	public appendAnalytics(record: $AnalyticsRecord): Promise<void> {
		this.#analytics.push(record);
		return Promise.resolve();
	}

	public clearAnalyticsBatch(before: number, batchSize: number): Promise<number> {
		let removed = 0;
		for (let index = this.#analytics.length - 1; index >= 0 && removed < batchSize; index -= 1) {
			const record = this.#analytics[index];
			if (record === undefined || record.collectedAt > before) continue;
			this.#analytics.splice(index, 1);
			removed += 1;
		}
		return Promise.resolve(removed);
	}

	public close(): Promise<void> {
		return Promise.resolve();
	}

	public loadState(): Promise<$PersistedHubState> {
		return Promise.resolve({
			assignments: [...this.#assignments.values()],
			bridges: [...this.#bridges.values()],
			shards: [...this.#shards.values()],
		});
	}

	public migrate(): Promise<void> {
		return Promise.resolve();
	}

	public saveAssignment(assignment: $PersistedAssignment): Promise<void> {
		this.#assignments.set(assignment.shardId, assignment);
		return Promise.resolve();
	}

	public saveBridge(bridge: $PersistedBridge): Promise<void> {
		this.#bridges.set(bridge.id, bridge);
		return Promise.resolve();
	}

	public saveShard(shard: $PersistedShard): Promise<void> {
		this.#shards.set(shard.shardId, shard);
		return Promise.resolve();
	}
}

class EmbeddedDiscordClient implements $DiscordClient {
	public destroyed = 0;
	public ready: boolean;

	public constructor(ready: boolean) {
		this.ready = ready;
	}

	public destroy(): void {
		this.destroyed += 1;
		this.ready = false;
	}

	public isReady(): boolean {
		return this.ready;
	}

	public login(token?: string): Promise<string> {
		this.ready = true;
		return Promise.resolve(token ?? "embedded-token");
	}
}

class EmbeddedShardTransport implements $ShardTransport {
	readonly #sendToBridge: (message: object) => void;
	#listener: ((message: unknown) => void) | undefined;
	#disconnectListener: (() => void) | undefined;
	#closed = false;

	public constructor(sendToBridge: (message: object) => void) {
		this.#sendToBridge = sendToBridge;
	}

	public deliver(message: object): void {
		if (this.#closed) throw new Error("Embedded shard transport is closed.");
		this.#listener?.(message);
	}

	public onDisconnect(listener: () => void): () => void {
		this.#disconnectListener = listener;
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			if (this.#disconnectListener === listener) this.#disconnectListener = undefined;
		};
	}

	public onMessage(listener: (message: unknown) => void): () => void {
		this.#listener = listener;
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			if (this.#listener === listener) this.#listener = undefined;
		};
	}

	public send(message: object): void {
		if (this.#closed) throw new Error("Embedded shard transport is closed.");
		this.#sendToBridge(message);
	}

	public close(): void {
		this.#closed = true;
		this.#listener = undefined;
		this.#disconnectListener = undefined;
	}
}

class EmbeddedShardProcess implements $ShardProcess {
	public readonly client: ShardClient<EmbeddedDiscordClient>;
	public readonly discord: EmbeddedDiscordClient;
	public readonly exited: Promise<number>;
	public readonly pid: number;
	readonly #context: $ShardProcessContext;
	readonly #onStartupError: (error: Error) => void;
	readonly #resolveExit: (code: number) => void;
	readonly #transport: EmbeddedShardTransport;
	#finished = false;
	#killed = false;

	public constructor(
		context: $ShardProcessContext,
		ready: boolean,
		onRequest: (payload: unknown) => unknown | Promise<unknown>,
		onMessage: (payload: unknown, messageContext: $ShardMessageContext) => void | Promise<void>,
		onStartupError: (error: Error) => void,
	) {
		this.#context = context;
		this.#onStartupError = onStartupError;
		this.pid = nextProcessId += 1;
		let resolveExit = (_code: number): void => undefined;
		this.exited = new Promise<number>((resolve) => {
			resolveExit = resolve;
		});
		this.#resolveExit = resolveExit;
		this.#transport = new EmbeddedShardTransport((message) => context.callbacks.onMessage(message));
		this.discord = new EmbeddedDiscordClient(ready);
		this.client = new ShardClient(this.discord, {
			analyticsIntervalMs: false,
			assignmentEpoch: context.assignmentEpoch,
			onRequest,
			processGeneration: context.processGeneration,
			readyPollIntervalMs: 100,
			request: {
				maxPending: 64,
				timeoutMs: 1_000,
			},
			shardId: context.shardId,
			totalShards: context.totalShards,
			transport: this.#transport,
		});
		this.client.onMessage(onMessage);
		queueMicrotask(() => {
			void this.client.start().catch((cause: unknown) => {
				const error = toError(cause);
				this.#onStartupError(error);
				this.#finish({ code: 1, error, signal: null });
			});
		});
	}

	public get killed(): boolean {
		return this.#killed;
	}

	public crash(): void {
		if (this.#finished) return;
		void this.client.close();
		this.#transport.close();
		this.#finish({ code: 1, signal: null });
	}

	public kill(signal?: number): void {
		if (this.#finished) return;
		this.#killed = true;
		void this.client.close();
		this.#transport.close();
		this.#finish({ code: null, signal: signal ?? null });
	}

	public send(message: object): void | Promise<void> {
		if (this.#finished) throw new Error("Embedded shard process has exited.");
		this.#transport.deliver(message);
		if (Reflect.get(message, "type") === "shard.control.shutdown") return this.#finishAfterShutdown();
	}

	async #finishAfterShutdown(): Promise<void> {
		for (let attempt = 0; attempt < 500; attempt += 1) {
			if (this.client.state === "closed") {
				this.#transport.close();
				this.#finish({ code: 0, signal: null });
				return;
			}
			await Bun.sleep(1);
		}
		throw new Error(`Embedded shard ${this.#context.shardId} did not finish its authorized shutdown.`);
	}

	#finish(exit: $ShardProcessExit): void {
		if (this.#finished) return;
		this.#finished = true;
		this.#context.callbacks.onExit(exit);
		this.#resolveExit(exit.code ?? 1);
	}
}

export class EmbeddedShardFleet {
	public readonly clients = new Map<number, ShardClient<EmbeddedDiscordClient>>();
	public readonly discords = new Map<number, EmbeddedDiscordClient>();
	public readonly errors: Error[] = [];
	public readonly messages: ReceivedMessage[] = [];
	public readonly processes = new Map<number, EmbeddedShardProcess>();
	public readonly requestActions: string[] = [];
	public slowCompletions = 0;
	public readonly slowRelease = createSignal();
	public readonly slowStarted = createSignal();
	public readonly factory: $ShardProcessFactory;
	readonly #ready: (shardId: number) => boolean;

	public constructor(ready: (shardId: number) => boolean) {
		this.#ready = ready;
		this.factory = (context): $ShardProcess => {
			const processHandle = new EmbeddedShardProcess(
				context,
				this.#ready(context.shardId),
				(payload) => this.#handleRequest(context.shardId, payload),
				(payload, messageContext) => {
					this.messages.push({
						payload,
						shardId: context.shardId,
						sourceShardId: messageContext.sourceShardId,
					});
				},
				(error) => this.errors.push(error),
			);
			this.clients.set(context.shardId, processHandle.client);
			this.discords.set(context.shardId, processHandle.discord);
			this.processes.set(context.shardId, processHandle);
			return processHandle;
		};
	}

	public crash(shardId: number): void {
		const processHandle = this.processes.get(shardId);
		if (processHandle === undefined) throw new Error(`No embedded process owns shard ${shardId}.`);
		processHandle.crash();
	}

	public readyShardIds(): readonly number[] {
		return [...this.clients]
			.filter(([shardId, client]) => client.state === "running" && this.discords.get(shardId)?.isReady() === true)
			.map(([shardId]) => shardId)
			.sort((left, right) => left - right);
	}

	async #handleRequest(shardId: number, payload: unknown): Promise<unknown> {
		const action = readAction(payload);
		this.requestActions.push(action);
		if (action === "invalid-result") return { unsupported: 1n };
		if (action === "slow") {
			this.slowStarted.resolve();
			await this.slowRelease.promise;
			this.slowCompletions += 1;
		}
		return {
			action,
			shardId,
		};
	}
}

function createSignal(): Signal {
	let resolve = (): void => undefined;
	const promise = new Promise<void>((settle) => {
		resolve = settle;
	});
	return { promise, resolve };
}

export function readAction(payload: unknown): string {
	if (typeof payload !== "object" || payload === null || Array.isArray(payload)) return "unknown";
	const action = Reflect.get(payload, "action");
	return typeof action === "string" ? action : "unknown";
}

function toError(value: unknown): Error {
	return value instanceof Error ? value : new Error("Operation failed with a non-Error value.", { cause: value });
}

export function nativeSocket(url: string, headers: Readonly<Record<string, string>>): WebSocket {
	const socket: unknown = Reflect.construct(WebSocket, [url, { headers }]);
	if (!(socket instanceof WebSocket)) throw new Error("Native WebSocket construction failed.");
	return socket;
}

export function createHub(totalShards: number, errors: ReportedError[]): HubClient {
	return new HubClient({
		adminToken: "admin-token-0001",
		botToken: "discord-token-01",
		bridgeToken: BRIDGE_TOKEN,
		fetch: () =>
			Promise.resolve(
				Response.json({
					session_start_limit: {
						max_concurrency: 2,
						remaining: 1_000,
						reset_after: 60_000,
						total: 1_000,
					},
					shards: totalShards,
					url: "wss://gateway.discord.gg",
				}),
			),
		hostname: "127.0.0.1",
		onError: (error, context) => errors.push({ context, error }),
		persistence: new MemoryHubPersistence(),
		port: 0,
		request: {
			maxPending: 64,
			timeoutMs: 1_000,
		},
		totalShards,
	});
}

export function createBridge(
	hubUrl: URL,
	id: string,
	maxShards: number,
	fleet: EmbeddedShardFleet,
	errors: ReportedError[],
	socketFactory?: $BridgeSocketFactory,
): BridgeClient {
	const options: $BridgeClientOptions = {
		analyticsPath: ":memory:",
		hubUrl,
		id,
		maxShards,
		onError: (error, context) => errors.push({ context, error }),
		processFactory: fleet.factory,
		random: () => 0.5,
		reconnect: {
			initialDelayMs: 100,
			jitterRatio: 0,
			maxDelayMs: 100,
			multiplier: 1,
		},
		request: {
			maxPending: 64,
			timeoutMs: 1_000,
		},
		restart: {
			initialDelayMs: 10_000,
			maxAttempts: 1,
			maxDelayMs: 10_000,
			windowMs: 60_000,
		},
		shardScript: "./unused-embedded-shard.ts",
		shutdownTimeoutMs: 500,
		startupTimeoutMs: 1_000,
		...(socketFactory === undefined ? {} : { socketFactory }),
		token: BRIDGE_TOKEN,
	};
	return new BridgeClient(options);
}

export function requireHubUrl(hub: HubClient): URL {
	const url = hub.url;
	if (url === null) throw new Error("Hub did not expose its listening URL.");
	return url;
}

export function requireClient(fleet: EmbeddedShardFleet, shardId: number): ShardClient<EmbeddedDiscordClient> {
	const client = fleet.clients.get(shardId);
	if (client === undefined) throw new Error(`Shard ${shardId} has no embedded client.`);
	return client;
}

export function sortedReadyTopology(hub: HubClient): readonly number[] {
	return hub
		.getTopology()
		.bridges.flatMap((bridge) => bridge.readyShardIds)
		.sort((left, right) => left - right);
}

export async function waitFor(predicate: () => boolean, description: string, timeoutMs = 3_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() <= deadline) {
		if (predicate()) return;
		await Bun.sleep(1);
	}
	throw new Error(`Timed out waiting for ${description}.`);
}

export async function stopAll(...resources: Array<BridgeClient | HubClient>): Promise<void> {
	const failures: unknown[] = [];
	for (const resource of resources) {
		try {
			await resource.stop();
		} catch (cause) {
			failures.push(cause);
		}
	}
	if (failures.length > 0) throw new AggregateError(failures, "Runtime stack cleanup failed.");
}
