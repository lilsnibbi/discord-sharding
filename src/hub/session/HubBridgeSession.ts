import {
	ShardingCapacityError,
	ShardingConfigurationError,
	ShardingProtocolError,
	ShardingStateError,
	ShardingTimeoutError,
	ShardingTransportError,
} from "../../errors/ShardingError";
import { createWireMessage, encodeWireMessage } from "../../protocol/codec";
import {
	HUB_TO_BRIDGE_TYPES,
	type ShardIdentityData,
	type WireDataMap,
	type WireMessageType,
} from "../../protocol/types";
import type { $PayloadPolicy, $RestartPolicy } from "../../types/common";

export type $HubBridgeSessionPhase = "awaiting-hello" | "synchronizing" | "ready" | "closed";

export interface $HubBridgeSessionHeaders {
	readonly bridgeGeneration: string;
	readonly bridgeId: string;
	readonly connectionGeneration: number;
}

export interface $HubSessionRunningShard extends ShardIdentityData {
	readonly ready: boolean;
}

interface QueuedMessage {
	readonly bytes: number;
	readonly encoded: string;
}

interface HubSessionSocket {
	readonly readyState: number;
	close(code?: number, reason?: string): void;
	getBufferedAmount(): number;
	send(data: string): number;
}

interface PendingSynchronization {
	readonly id: string;
	readonly reject: (reason: unknown) => void;
	readonly resolve: () => void;
	readonly timer: ReturnType<typeof setTimeout>;
	readonly topologyVersion: number;
}

type InboundOperation = () => Promise<void>;

export class HubBridgeSession {
	public readonly bridgeGeneration: string;
	public readonly bridgeId: string;
	public readonly connectionGeneration: number;
	public readonly lifecycle = new AbortController();
	public readonly runningShards = new Map<number, $HubSessionRunningShard>();
	public maxShards = 0;
	public phase: $HubBridgeSessionPhase = "awaiting-hello";
	public restartPolicy: $RestartPolicy | undefined;
	public synchronizedVersion = 0;

	readonly #maxBufferedBytes: number;
	readonly #maxInboundMessages: number;
	readonly #maxQueuedMessages: number;
	readonly #payloadPolicy: $PayloadPolicy;
	readonly #inboundQueue: InboundOperation[] = [];
	readonly #queue: QueuedMessage[] = [];
	readonly #socket: HubSessionSocket;
	readonly #syncTimeoutMs: number;
	#blocked = false;
	#helloTimer: ReturnType<typeof setTimeout> | undefined;
	#inboundDrain: Promise<void> | undefined;
	#pendingSync: PendingSynchronization | undefined;
	#queuedBytes = 0;

	public constructor(
		socket: HubSessionSocket,
		headers: $HubBridgeSessionHeaders,
		payloadPolicy: $PayloadPolicy,
		maxBufferedBytes: number,
		maxQueuedMessages: number,
		maxInboundMessages: number,
		syncTimeoutMs: number,
		helloTimeoutMs: number,
	) {
		this.#socket = socket;
		this.bridgeGeneration = headers.bridgeGeneration;
		this.bridgeId = headers.bridgeId;
		this.connectionGeneration = headers.connectionGeneration;
		this.#payloadPolicy = payloadPolicy;
		this.#maxBufferedBytes = maxBufferedBytes;
		this.#maxQueuedMessages = maxQueuedMessages;
		this.#maxInboundMessages = maxInboundMessages;
		this.#syncTimeoutMs = syncTimeoutMs;
		this.#helloTimer = setTimeout(() => {
			if (this.phase !== "awaiting-hello") return;
			this.close(1008, "Bridge hello timed out");
		}, helloTimeoutMs);
	}

	/**
	 * Number of inbound messages currently running or waiting for this session.
	 */
	public get inboundMessages(): number {
		if (this.phase === "closed") return 0;
		return this.#inboundQueue.length + (this.#inboundDrain === undefined ? 0 : 1);
	}

	/**
	 * Adds one inbound operation to this session's ordered, bounded queue.
	 *
	 * @param operation - Message operation to run after earlier messages settle.
	 */
	public enqueueInbound(operation: InboundOperation): void {
		if (this.phase === "closed") return;
		if (this.inboundMessages >= this.#maxInboundMessages) {
			throw new ShardingCapacityError(
				`Bridge ${this.bridgeId} inbound capacity of ${this.#maxInboundMessages} was reached.`,
			);
		}
		this.#inboundQueue.push(operation);
		this.#startInboundDrain();
	}

	/**
	 * Waits for the active inbound operation and every queued operation to settle.
	 */
	public async waitForInboundIdle(): Promise<void> {
		while (this.#inboundDrain !== undefined) {
			await this.#inboundDrain;
		}
	}

	/**
	 * Closes this session with the WebSocket code appropriate for a failure.
	 *
	 * @param error - Validated failure raised while processing session work.
	 */
	public fail(error: Error): void {
		const close = closeForError(error);
		this.close(close.code, close.reason);
	}

	public acceptHello(
		maxShards: number,
		restartPolicy: $RestartPolicy,
		runningShards: readonly $HubSessionRunningShard[],
	): void {
		if (this.phase !== "awaiting-hello") throw new ShardingStateError("Bridge hello was already accepted.");
		if (this.#helloTimer !== undefined) clearTimeout(this.#helloTimer);
		this.#helloTimer = undefined;
		this.maxShards = maxShards;
		this.restartPolicy = restartPolicy;
		this.runningShards.clear();
		for (const shard of runningShards) this.runningShards.set(shard.shardId, shard);
		this.phase = "synchronizing";
	}

	public beginSynchronization(id: string, topologyVersion: number, keepReady = false): Promise<void> {
		if (this.phase === "closed") return Promise.reject(new ShardingStateError("Bridge session is closed."));
		if (this.#pendingSync !== undefined) {
			return Promise.reject(new ShardingStateError("Bridge session already has a pending topology sync."));
		}
		if (!(keepReady && this.phase === "ready")) this.phase = "synchronizing";
		return new Promise<void>((resolve, reject) => {
			const timer = setTimeout(() => {
				if (this.#pendingSync?.id !== id) return;
				this.#pendingSync = undefined;
				reject(new ShardingTimeoutError(`Bridge ${this.bridgeId} did not acknowledge topology ${topologyVersion}.`));
				this.close(1002, "Topology sync timed out");
			}, this.#syncTimeoutMs);
			this.#pendingSync = {
				id,
				reject,
				resolve,
				timer,
				topologyVersion,
			};
		});
	}

	public acknowledgeSynchronization(id: string, topologyVersion: number): void {
		const pending = this.#pendingSync;
		if (pending === undefined || pending.id !== id || pending.topologyVersion !== topologyVersion) {
			throw new ShardingProtocolError("Bridge topology acknowledgement is stale or unexpected.");
		}
		clearTimeout(pending.timer);
		this.#pendingSync = undefined;
		this.synchronizedVersion = topologyVersion;
		this.phase = "ready";
		pending.resolve();
	}

	public send<Type extends WireMessageType>(type: Type, id: string, data: WireDataMap[Type]): void {
		if (!HUB_TO_BRIDGE_TYPES.has(type)) throw new ShardingProtocolError(`${type} cannot be sent from a Hub.`);
		if (this.phase === "closed" || this.#socket.readyState !== WebSocket.OPEN) {
			throw new ShardingTransportError(`Bridge ${this.bridgeId} WebSocket is not open.`);
		}
		const message = createWireMessage(type, id, data, this.#payloadPolicy);
		const encoded = encodeWireMessage(message, this.#payloadPolicy);
		const bytes = new TextEncoder().encode(encoded).byteLength;
		if (
			this.#socket.getBufferedAmount() + this.#queuedBytes + bytes > this.#maxBufferedBytes ||
			(this.#blocked && this.#queue.length >= this.#maxQueuedMessages)
		) {
			this.close(1013, "Outbound capacity reached");
			throw new ShardingCapacityError(`Bridge ${this.bridgeId} WebSocket outbound capacity was reached.`);
		}
		if (this.#blocked) {
			this.#queue.push(Object.freeze({ bytes, encoded }));
			this.#queuedBytes += bytes;
			return;
		}
		this.#write(encoded);
	}

	public drain(): void {
		if (this.phase === "closed") return;
		this.#blocked = false;
		while (!this.#blocked) {
			const next = this.#queue.shift();
			if (next === undefined) return;
			this.#queuedBytes -= next.bytes;
			if (this.#socket.getBufferedAmount() + next.bytes > this.#maxBufferedBytes) {
				this.close(1013, "Outbound capacity reached");
				return;
			}
			this.#write(next.encoded);
		}
	}

	public close(code: number, reason: string): void {
		if (this.phase === "closed") return;
		try {
			this.#socket.close(code, reason);
		} finally {
			this.finish(new ShardingTransportError(`Bridge ${this.bridgeId} session closed: ${reason}`));
		}
	}

	public finish(reason: Error): void {
		if (this.phase === "closed") return;
		this.phase = "closed";
		if (this.#helloTimer !== undefined) clearTimeout(this.#helloTimer);
		this.#helloTimer = undefined;
		const pending = this.#pendingSync;
		this.#pendingSync = undefined;
		if (pending !== undefined) {
			clearTimeout(pending.timer);
			pending.reject(reason);
		}
		this.#inboundQueue.length = 0;
		this.#queue.length = 0;
		this.#queuedBytes = 0;
		this.lifecycle.abort(reason);
	}

	#startInboundDrain(): void {
		if (this.phase === "closed" || this.#inboundDrain !== undefined || this.#inboundQueue.length === 0) return;
		const operation = this.#drainInboundQueue();
		this.#inboundDrain = operation;
		void operation.then(
			() => this.#finishInboundDrain(operation),
			() => this.#finishInboundDrain(operation),
		);
	}

	async #drainInboundQueue(): Promise<void> {
		while (this.phase !== "closed") {
			const operation = this.#inboundQueue.shift();
			if (operation === undefined) return;
			await operation();
		}
	}

	#finishInboundDrain(operation: Promise<void>): void {
		if (this.#inboundDrain !== operation) return;
		this.#inboundDrain = undefined;
		if (this.phase === "closed") {
			this.#inboundQueue.length = 0;
			return;
		}
		this.#startInboundDrain();
	}

	#write(encoded: string): void {
		let status: number;
		try {
			status = this.#socket.send(encoded);
		} catch (cause) {
			this.close(1011, "Outbound send failed");
			throw new ShardingTransportError(`Could not send to Bridge ${this.bridgeId}.`, { cause });
		}
		if (status === 0) {
			this.close(1013, "Outbound message dropped");
			throw new ShardingCapacityError(`Bridge ${this.bridgeId} WebSocket dropped an outbound message.`);
		}
		if (status === -1) this.#blocked = true;
	}
}

function closeForError(error: Error): { readonly code: number; readonly reason: string } {
	if (error instanceof ShardingCapacityError) return { code: 1013, reason: "Capacity failure" };
	if (error instanceof ShardingProtocolError || error instanceof ShardingConfigurationError) {
		return { code: 1002, reason: "Protocol failure" };
	}
	if (error instanceof ShardingStateError) return { code: 1008, reason: "Policy failure" };
	return { code: 1011, reason: "Internal failure" };
}

export interface $HubBridgeSessionSocketData {
	readonly headers: $HubBridgeSessionHeaders;
	session: HubBridgeSession | undefined;
}
