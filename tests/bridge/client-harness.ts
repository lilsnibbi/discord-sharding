import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { createWireMessage, encodeWireMessage, parseWireMessage } from "../../src/protocol/codec";
import {
	BRIDGE_TO_HUB_TYPES,
	BRIDGE_TO_SHARD_TYPES,
	type ParsedWireMessage,
	type WireDataMap,
	type WireMessageType,
} from "../../src/protocol/types";
import type {
	$BridgeClientOptions,
	$BridgeSocketFactory,
	$ShardProcess,
	$ShardProcessContext,
	$ShardProcessExit,
	$ShardProcessFactory,
} from "../../src/types/bridge";
import type { $Sleep } from "../../src/types/common";

export interface $HubHarness {
	readonly authorization: () => string | null;
	readonly close: () => void;
	readonly closeCode: () => number | null;
	readonly failNextBridgeMessage: (type: WireMessageType) => void;
	readonly factory: $BridgeSocketFactory;
	readonly received: readonly unknown[];
	readonly send: <Type extends WireMessageType>(type: Type, id: string, data: WireDataMap[Type]) => void;
	readonly url: string;
}

export class FakeWebSocket extends EventTarget implements WebSocket {
	public readonly CLOSED = WebSocket.CLOSED;
	public readonly CLOSING = WebSocket.CLOSING;
	public readonly CONNECTING = WebSocket.CONNECTING;
	public readonly OPEN = WebSocket.OPEN;
	public binaryType: BinaryType = "blob";
	public bufferedAmount = 0;
	public extensions = "";
	public onclose: ((this: WebSocket, event: CloseEvent) => void) | null = null;
	public onerror: ((this: WebSocket, event: Event) => void) | null = null;
	public onmessage: ((this: WebSocket, event: MessageEvent) => void) | null = null;
	public onopen: ((this: WebSocket, event: Event) => void) | null = null;
	public protocol = "";
	public readyState: 0 | 1 | 2 | 3 = WebSocket.CONNECTING;
	public readonly url: string;
	readonly #sent: unknown[];
	readonly #failSend: (message: Readonly<Record<string, unknown>>) => boolean;

	public constructor(url: string, sent: unknown[], failSend: (message: Readonly<Record<string, unknown>>) => boolean) {
		super();
		this.url = url;
		this.#sent = sent;
		this.#failSend = failSend;
	}

	public close(code = 1000, reason = ""): void {
		if (this.readyState === WebSocket.CLOSED) return;
		this.readyState = WebSocket.CLOSED;
		this.dispatchEvent(
			new CloseEvent("close", {
				code,
				reason,
				wasClean: code === 1000,
			}),
		);
	}

	public open(): void {
		if (this.readyState !== WebSocket.CONNECTING) return;
		this.readyState = WebSocket.OPEN;
		this.dispatchEvent(new Event("open"));
	}

	public receive(data: string): void {
		if (this.readyState !== WebSocket.OPEN) throw new Error("Fake Hub socket is not open.");
		this.dispatchEvent(new MessageEvent("message", { data }));
	}

	public send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
		if (this.readyState !== WebSocket.OPEN) throw new Error("Fake Hub socket is not open.");
		if (typeof data !== "string") throw new Error("Bridge tests expect JSON string WebSocket messages.");
		const decoded: unknown = JSON.parse(data);
		if (typeof decoded !== "object" || decoded === null || Array.isArray(decoded)) {
			throw new Error("Bridge sent an invalid protocol envelope.");
		}
		const message = decoded as Readonly<Record<string, unknown>>;
		if (this.#failSend(message)) throw new Error(`Rejected Bridge message ${String(message.type)}.`);
		this.#sent.push(message);
	}
}

export function createHubHarness(): $HubHarness {
	const received: unknown[] = [];
	let socket: FakeWebSocket | undefined;
	let authorization: string | null = null;
	let lastCloseCode: number | null = null;
	let failingType: WireMessageType | undefined;
	return {
		authorization: () => authorization,
		close: () => {
			socket?.close(1000, "Test complete");
		},
		closeCode: () => lastCloseCode,
		failNextBridgeMessage: (type) => {
			failingType = type;
		},
		factory: (url, headers) => {
			authorization = headers.Authorization ?? null;
			const next = new FakeWebSocket(url, received, (message) => {
				if (message.type !== failingType) return false;
				failingType = undefined;
				return true;
			});
			next.addEventListener("close", (event) => {
				if (event instanceof CloseEvent) lastCloseCode = event.code;
			});
			socket = next;
			queueMicrotask(() => next.open());
			return next;
		},
		received,
		send: (type, id, data) => {
			if (socket === undefined) throw new Error("Hub socket is not open.");
			const message = createWireMessage(type, id, data, DEFAULT_PAYLOAD_POLICY);
			socket.receive(encodeWireMessage(message, DEFAULT_PAYLOAD_POLICY));
		},
		url: "http://hub.test",
	};
}

export interface $ShardHarness {
	readonly contexts: readonly $ShardProcessContext[];
	readonly factory: $ShardProcessFactory;
	readonly sent: readonly object[];
}

export function createShardHarness(): $ShardHarness {
	const contexts: $ShardProcessContext[] = [];
	const sent: object[] = [];
	let nextPid = 2_000;
	const factory: $ShardProcessFactory = (context): $ShardProcess => {
		contexts.push(context);
		let resolveExit = (_code: number): void => undefined;
		let exited = false;
		let killed = false;
		const exitPromise = new Promise<number>((resolve) => {
			resolveExit = resolve;
		});
		const finish = (exit: $ShardProcessExit): void => {
			if (exited) return;
			exited = true;
			context.callbacks.onExit(exit);
			resolveExit(exit.code ?? 0);
		};
		nextPid += 1;
		return {
			exited: exitPromise,
			get killed(): boolean {
				return killed;
			},
			pid: nextPid,
			kill(signal?: number): void {
				killed = true;
				finish({ code: null, signal: signal ?? null });
			},
			send(message: object): void {
				sent.push(message);
				if (messageType(message) === "shard.control.shutdown") {
					queueMicrotask(() => finish({ code: 0, signal: null }));
				}
			},
		};
	};
	return { contexts, factory, sent };
}

export async function waitForHubMessage(
	hub: $HubHarness,
	type: WireMessageType,
	startIndex = 0,
): Promise<ParsedWireMessage> {
	for (let attempt = 0; attempt < 200; attempt += 1) {
		for (let index = startIndex; index < hub.received.length; index += 1) {
			const value = hub.received[index];
			const parsed = parseWireMessage(value, BRIDGE_TO_HUB_TYPES, DEFAULT_PAYLOAD_POLICY);
			if (parsed.type === type) return parsed;
		}
		await Bun.sleep(1);
	}
	throw new Error(`Timed out waiting for ${type}.`);
}

export async function waitForShardMessage(
	harness: $ShardHarness,
	type: WireMessageType,
	startIndex = 0,
): Promise<ParsedWireMessage> {
	for (let attempt = 0; attempt < 200; attempt += 1) {
		for (let index = startIndex; index < harness.sent.length; index += 1) {
			const value = harness.sent[index];
			if (value === undefined) continue;
			const parsed = parseWireMessage(value, BRIDGE_TO_SHARD_TYPES, DEFAULT_PAYLOAD_POLICY);
			if (parsed.type === type) return parsed;
		}
		await Bun.sleep(1);
	}
	throw new Error(`Timed out waiting for ${type}.`);
}

export async function waitFor(predicate: () => boolean, name: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt += 1) {
		if (predicate()) return;
		await Bun.sleep(1);
	}
	throw new Error(`Timed out waiting for ${name}.`);
}

export function messageType(message: object): unknown {
	return Reflect.get(message, "type");
}

export function createBridgeOptions(
	hubUrl: string,
	overrides: {
		readonly processFactory?: $ShardProcessFactory;
		readonly requestMaxPending?: number;
		readonly requestTimeoutMs?: number;
		readonly sleep?: $Sleep;
		readonly socketFactory?: $BridgeSocketFactory;
	} = {},
): $BridgeClientOptions {
	return {
		analyticsPath: ":memory:",
		hubUrl,
		id: "bridge:test",
		maxShards: 2,
		...(overrides.processFactory === undefined ? {} : { processFactory: overrides.processFactory }),
		reconnect: {
			initialDelayMs: 100,
			jitterRatio: 0,
			maxDelayMs: 100,
			multiplier: 1,
		},
		request: {
			maxPending: overrides.requestMaxPending ?? 8,
			timeoutMs: overrides.requestTimeoutMs ?? 1_000,
		},
		shardScript: "./shard.ts",
		...(overrides.sleep === undefined ? {} : { sleep: overrides.sleep }),
		...(overrides.socketFactory === undefined ? {} : { socketFactory: overrides.socketFactory }),
		token: "0123456789abcdef",
	};
}
