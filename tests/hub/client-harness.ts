import { HubClient } from "../../src/hub/HubClient";
import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { DEFAULT_RESTART_POLICY } from "../../src/internal/policies";
import { createWireMessage, encodeWireMessage, parseWireMessage } from "../../src/protocol/codec";
import {
	HUB_TO_BRIDGE_TYPES,
	type ParsedWireMessage,
	type WireDataMap,
	type WireMessageType,
} from "../../src/protocol/types";
import type { $Sleep } from "../../src/types/common";
import type { $GatewayFetch, $HubPersistence, $PersistedBridge } from "../../src/types/hub";

export { MemoryHubPersistence } from "./client-persistence";

export interface Deferred<Value> {
	readonly promise: Promise<Value>;
	readonly resolve: (value: Value) => void;
}

export interface BridgeHarness {
	readonly send: <Type extends WireMessageType>(type: Type, id: string, data: WireDataMap[Type]) => void;
	readonly socket: FakeHubSocket;
	readonly waitForMessage: (type: WireMessageType, startIndex?: number) => Promise<ParsedWireMessage>;
	readonly waitForMessages: (
		type: WireMessageType,
		count: number,
		startIndex?: number,
	) => Promise<readonly ParsedWireMessage[]>;
}

export interface UpgradeResult {
	readonly response?: Response;
	readonly socket?: FakeHubSocket;
}

export type UnknownFunction = (...arguments_: unknown[]) => unknown;

export const BRIDGE_ID = "bridge-a";
export const BRIDGE_GENERATION = "generation-a";
export const BRIDGE_TOKEN = "bridge-token-001";

export class FakeHubSocket {
	public bufferedAmount = 0;
	public closeCode: number | undefined;
	public closeReason: string | undefined;
	public data: unknown;
	public readyState: number = WebSocket.OPEN;
	public readonly sent: string[] = [];
	public sendStatus = 1;
	readonly #onClose: (socket: FakeHubSocket, code: number, reason: string) => void;

	public constructor(data: unknown, onClose: (socket: FakeHubSocket, code: number, reason: string) => void) {
		this.data = data;
		this.#onClose = onClose;
	}

	public close(code = 1000, reason = ""): void {
		if (this.readyState === WebSocket.CLOSED) return;
		this.readyState = WebSocket.CLOSED;
		this.closeCode = code;
		this.closeReason = reason;
		this.#onClose(this, code, reason);
	}

	public getBufferedAmount(): number {
		return this.bufferedAmount;
	}

	public send(data: string): number {
		if (this.readyState !== WebSocket.OPEN) return 0;
		this.sent.push(data);
		return this.sendStatus;
	}
}

export class FakeHubServer {
	public readonly url = new URL("http://127.0.0.1:32001");
	public stopCalls = 0;
	readonly #sockets: FakeHubSocket[] = [];
	#options: unknown;

	public configure(options: unknown): void {
		this.#options = options;
	}

	public async fetch(request: Request): Promise<Response | undefined> {
		const callback = requireFunction(readProperty(this.#options, "fetch"), "Hub fetch handler");
		const result: unknown = await Promise.resolve(Reflect.apply(callback, undefined, [request, this]));
		if (result === undefined || result instanceof Response) return result;
		throw new Error("Hub fetch handler returned an invalid response.");
	}

	public message(socket: FakeHubSocket, value: string | Uint8Array): void {
		const callback = this.#webSocketCallback("message");
		Reflect.apply(callback, undefined, [socket, value]);
	}

	public drain(socket: FakeHubSocket): void {
		const callback = this.#webSocketCallback("drain");
		Reflect.apply(callback, undefined, [socket]);
	}

	public stop(force?: boolean): Promise<void> {
		void force;
		this.stopCalls += 1;
		return Promise.resolve();
	}

	public upgrade(_request: Request, options: unknown): boolean {
		const data = readProperty(options, "data");
		const socket = new FakeHubSocket(data, (closed, code, reason) => {
			const callback = this.#webSocketCallback("close");
			Reflect.apply(callback, undefined, [closed, code, reason]);
		});
		this.#sockets.push(socket);
		const callback = this.#webSocketCallback("open");
		Reflect.apply(callback, undefined, [socket]);
		return true;
	}

	public get lastSocket(): FakeHubSocket | undefined {
		return this.#sockets.at(-1);
	}

	#webSocketCallback(name: string): UnknownFunction {
		const webSocket = readProperty(this.#options, "websocket");
		return requireFunction(readProperty(webSocket, name), `Hub WebSocket ${name} handler`);
	}
}

export interface FakeServeInstallation {
	readonly restore: () => void;
	readonly server: FakeHubServer;
}

export function installFakeServe(): FakeServeInstallation {
	const original = Reflect.get(Bun, "serve");
	const server = new FakeHubServer();
	const replacement = (options: unknown): FakeHubServer => {
		server.configure(options);
		return server;
	};
	if (!Reflect.set(Bun, "serve", replacement)) throw new Error("Could not install the fake Bun server.");
	let restored = false;
	return {
		restore: () => {
			if (restored) return;
			restored = true;
			if (!Reflect.set(Bun, "serve", original)) throw new Error("Could not restore Bun.serve.");
		},
		server,
	};
}

export function createDeferred<Value>(): Deferred<Value> {
	let settle = (_value: Value): void => {
		throw new Error("Deferred promise was resolved before initialization.");
	};
	const promise = new Promise<Value>((resolve) => {
		settle = resolve;
	});
	return { promise, resolve: settle };
}

export const gatewayFetch: $GatewayFetch = () =>
	Promise.resolve(
		Response.json({
			session_start_limit: {
				max_concurrency: 1,
				remaining: 1_000,
				reset_after: 60_000,
				total: 1_000,
			},
			shards: 2,
			url: "wss://gateway.discord.gg",
		}),
	);

export function createHub(
	persistence: $HubPersistence,
	options: {
		readonly maxPending?: number;
		readonly maxQueuedMessages?: number;
		readonly fetch?: $GatewayFetch;
		readonly totalShards?: number;
		readonly now?: () => number;
		readonly sleep?: $Sleep;
		readonly wallClock?: () => number;
	} = {},
): HubClient {
	return new HubClient({
		adminToken: "admin-token-0001",
		botToken: "discord-token-01",
		bridgeToken: BRIDGE_TOKEN,
		fetch: options.fetch ?? gatewayFetch,
		hostname: "127.0.0.1",
		...(options.maxQueuedMessages === undefined ? {} : { maxQueuedMessages: options.maxQueuedMessages }),
		persistence,
		port: 0,
		request: {
			maxPending: options.maxPending ?? 8,
			timeoutMs: 100,
		},
		...(options.now === undefined ? {} : { now: options.now }),
		...(options.sleep === undefined ? {} : { sleep: options.sleep }),
		totalShards: options.totalShards ?? 1,
		wallClock: options.wallClock ?? monotonicClock(),
	});
}

export function monotonicClock(): () => number {
	let timestamp = 1_000;
	return () => {
		timestamp += 1;
		return timestamp;
	};
}

export function persistedBridge(
	id = BRIDGE_ID,
	generation = BRIDGE_GENERATION,
	maxShards = 1,
	updatedAt = 1,
): $PersistedBridge {
	return Object.freeze({
		connected: false,
		generation,
		id,
		maxShards,
		updatedAt,
	});
}

export async function attemptUpgrade(
	server: FakeHubServer,
	options: {
		readonly authorization?: string;
		readonly bridgeGeneration?: string;
		readonly bridgeId?: string;
		readonly connectionGeneration?: number;
	} = {},
): Promise<UpgradeResult> {
	const headers = new Headers();
	if (options.authorization !== undefined) headers.set("authorization", options.authorization);
	if (options.bridgeGeneration !== undefined) {
		headers.set("x-sharding-bridge-generation", options.bridgeGeneration);
	}
	if (options.bridgeId !== undefined) headers.set("x-sharding-bridge-id", options.bridgeId);
	if (options.connectionGeneration !== undefined) {
		headers.set("x-sharding-connection-generation", String(options.connectionGeneration));
	}
	const response = await server.fetch(new Request("http://hub.test/bridge", { headers }));
	if (response !== undefined) return { response };
	const socket = server.lastSocket;
	if (socket === undefined) throw new Error("Successful upgrade did not create a socket.");
	return { socket };
}

export async function openBridge(
	server: FakeHubServer,
	connectionGeneration = 1,
	acknowledgeSynchronization = false,
	bridgeGeneration = BRIDGE_GENERATION,
	bridgeId = BRIDGE_ID,
): Promise<BridgeHarness> {
	const upgraded = await attemptUpgrade(server, {
		authorization: `Bearer ${BRIDGE_TOKEN}`,
		bridgeGeneration,
		bridgeId,
		connectionGeneration,
	});
	if (upgraded.socket === undefined) {
		throw new Error(`Bridge upgrade failed with ${String(upgraded.response?.status)}.`);
	}
	const socket = upgraded.socket;
	const parseMessages = (): readonly ParsedWireMessage[] =>
		socket.sent.map((encoded) => parseWireMessage(encoded, HUB_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY));
	const send = <Type extends WireMessageType>(type: Type, id: string, data: WireDataMap[Type]): void => {
		server.message(
			socket,
			encodeWireMessage(createWireMessage(type, id, data, DEFAULT_PAYLOAD_POLICY), DEFAULT_PAYLOAD_POLICY),
		);
	};
	const waitForMessages = async (
		type: WireMessageType,
		count: number,
		startIndex = 0,
	): Promise<readonly ParsedWireMessage[]> => {
		for (let attempt = 0; attempt < 500; attempt += 1) {
			const matches = parseMessages()
				.slice(startIndex)
				.filter((message) => message.type === type);
			if (matches.length >= count) return matches.slice(0, count);
			await Bun.sleep(1);
		}
		throw new Error(`Timed out waiting for ${count} ${type} messages.`);
	};
	const harness: BridgeHarness = {
		send,
		socket,
		waitForMessage: async (type, startIndex = 0) => {
			const message = (await waitForMessages(type, 1, startIndex))[0];
			if (message === undefined) throw new Error(`No ${type} message was returned.`);
			return message;
		},
		waitForMessages,
	};
	if (acknowledgeSynchronization) {
		void autoAcknowledgeSynchronizations(harness);
	}
	return harness;
}

export async function autoAcknowledgeSynchronizations(bridge: BridgeHarness): Promise<void> {
	let inspected = 0;
	while (bridge.socket.readyState === WebSocket.OPEN) {
		for (; inspected < bridge.socket.sent.length; inspected += 1) {
			const encoded = bridge.socket.sent[inspected];
			if (encoded === undefined) continue;
			const message = parseWireMessage(encoded, HUB_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY);
			if (message.type !== "hub.sync") continue;
			const topologyVersion = message.data.topologyVersion;
			if (typeof topologyVersion !== "number") throw new Error("Hub sync omitted its topology version.");
			bridge.send("bridge.sync.ready", message.id, { topologyVersion });
		}
		await Bun.sleep(1);
	}
}

export function sendHello(
	bridge: BridgeHarness,
	options: {
		readonly bridgeGeneration?: string;
		readonly bridgeId?: string;
		readonly connectionGeneration?: number;
		readonly maxShards?: number;
		readonly restartPolicy?: WireDataMap["bridge.hello"]["restartPolicy"];
		readonly runningShards?: WireDataMap["bridge.hello"]["runningShards"];
	} = {},
): void {
	bridge.send("bridge.hello", "hello:1", {
		bridgeGeneration: options.bridgeGeneration ?? BRIDGE_GENERATION,
		bridgeId: options.bridgeId ?? BRIDGE_ID,
		connectionGeneration: options.connectionGeneration ?? 1,
		maxShards: options.maxShards ?? 1,
		restartPolicy: options.restartPolicy ?? DEFAULT_RESTART_POLICY,
		runningShards: options.runningShards ?? [],
	});
}

export async function synchronizeBridge(
	bridge: BridgeHarness,
	options: {
		readonly bridgeGeneration?: string;
		readonly bridgeId?: string;
		readonly connectionGeneration?: number;
		readonly maxShards?: number;
		readonly restartPolicy?: WireDataMap["bridge.hello"]["restartPolicy"];
		readonly runningShards?: WireDataMap["bridge.hello"]["runningShards"];
	} = {},
): Promise<void> {
	sendHello(bridge, options);
	const synchronization = await bridge.waitForMessage("hub.sync");
	const topologyVersion = synchronization.data.topologyVersion;
	if (typeof topologyVersion !== "number") throw new Error("Hub sync omitted its topology version.");
	bridge.send("bridge.sync.ready", synchronization.id, { topologyVersion });
}

export async function waitFor(predicate: () => boolean, description: string): Promise<void> {
	for (let attempt = 0; attempt < 500; attempt += 1) {
		if (predicate()) return;
		await Bun.sleep(1);
	}
	throw new Error(`Timed out waiting for ${description}.`);
}

export function readProperty(value: unknown, key: string): unknown {
	if ((typeof value !== "object" && typeof value !== "function") || value === null) {
		throw new Error(`Cannot read ${key} from a non-object value.`);
	}
	return Reflect.get(value, key);
}

export function requireFunction(value: unknown, name: string): UnknownFunction {
	if (!isUnknownFunction(value)) throw new Error(`${name} is not callable.`);
	return value;
}

export function isUnknownFunction(value: unknown): value is UnknownFunction {
	return typeof value === "function";
}

export function sentMessages(socket: FakeHubSocket, startIndex = 0): readonly ParsedWireMessage[] {
	return socket.sent
		.slice(startIndex)
		.map((encoded) => parseWireMessage(encoded, HUB_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY));
}

export function requireNumberProperty(value: Readonly<Record<string, unknown>>, key: string): number {
	const field = Reflect.get(value, key);
	if (typeof field !== "number") throw new Error(`${key} must be numeric.`);
	return field;
}
