import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { createWireMessage, parseWireMessage } from "../../src/protocol/codec";
import { type ParsedWireMessage, SHARD_TO_BRIDGE_TYPES, type WireMessageType } from "../../src/protocol/types";
import { ShardClient } from "../../src/shard/ShardClient";
import type { $DiscordClient } from "../../src/types/discord";
import type { $ShardClientOptions, $ShardTransport } from "../../src/types/shard";

export class FakeDiscordClient implements $DiscordClient {
	public destroyed = 0;
	public loginTokens: (string | undefined)[] = [];
	public ready = false;

	public destroy(): void {
		this.destroyed += 1;
		this.ready = false;
	}

	public isReady(): boolean {
		return this.ready;
	}

	public async login(token?: string): Promise<string> {
		this.loginTokens.push(token);
		this.ready = true;
		return token ?? "environment-token";
	}
}

export class FakeShardTransport implements $ShardTransport {
	public readonly sent: object[] = [];
	public listenerRemovals = 0;
	public disconnectListenerRemovals = 0;
	public sendFailure: Error | undefined;
	public sendOperation: Promise<void> | undefined;
	#listener: ((message: unknown) => void) | undefined;
	#disconnectListener: (() => void) | undefined;

	public onMessage(listener: (message: unknown) => void): () => void {
		this.#listener = listener;
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			if (this.#listener === listener) this.#listener = undefined;
			this.listenerRemovals += 1;
		};
	}

	public send(message: object): void | Promise<void> {
		if (this.sendFailure !== undefined) throw this.sendFailure;
		this.sent.push(message);
		return this.sendOperation;
	}

	public receive(message: object): void {
		this.#listener?.(message);
	}

	public onDisconnect(listener: () => void): () => void {
		this.#disconnectListener = listener;
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			if (this.#disconnectListener === listener) this.#disconnectListener = undefined;
			this.disconnectListenerRemovals += 1;
		};
	}

	public disconnect(): void {
		this.#disconnectListener?.();
	}
}

export function createClient(
	botClient = new FakeDiscordClient(),
	transport = new FakeShardTransport(),
	options: Partial<$ShardClientOptions> = {},
): {
	readonly botClient: FakeDiscordClient;
	readonly client: ShardClient<FakeDiscordClient>;
	readonly transport: FakeShardTransport;
} {
	return {
		botClient,
		client: new ShardClient(botClient, {
			analyticsIntervalMs: false,
			assignmentEpoch: 3,
			processGeneration: 4,
			readyPollIntervalMs: 100,
			request: { maxPending: 8, timeoutMs: 1_000 },
			shardId: 1,
			totalShards: 4,
			transport,
			...options,
		}),
		transport,
	};
}

export async function waitForSent(
	transport: FakeShardTransport,
	type: WireMessageType,
	startIndex = 0,
): Promise<ParsedWireMessage> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		for (let index = startIndex; index < transport.sent.length; index += 1) {
			const candidate = transport.sent[index];
			if (candidate === undefined) continue;
			const parsed = parseWireMessage(candidate, SHARD_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY);
			if (parsed.type === type) return parsed;
		}
		await Bun.sleep(1);
	}
	throw new Error(`Timed out waiting for ${type}.`);
}

export function controlMessage<Type extends WireMessageType>(
	type: Type,
	id: string,
	data: Parameters<typeof createWireMessage<Type>>[2],
): object {
	return createWireMessage(type, id, data, DEFAULT_PAYLOAD_POLICY);
}
