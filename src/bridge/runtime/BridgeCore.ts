import {
	ShardingCapacityError,
	ShardingProtocolError,
	ShardingStateError,
	ShardingTransportError,
} from "../../errors/ShardingError";
import { createRequestId } from "../../internal/validation";
import { createWireMessage, encodeWireMessage } from "../../protocol/codec";
import {
	BRIDGE_TO_HUB_TYPES,
	BRIDGE_TO_SHARD_TYPES,
	type WireDataMap,
	type WireMessageType,
} from "../../protocol/types";
import type { $BridgeClientOptions, $BridgeShardSnapshot, $BridgeState } from "../../types/bridge";
import type { $PayloadPolicy } from "../../types/common";
import type { BridgeAnalyticsStore } from "../database/BridgeAnalyticsStore";
import type { ManagedShardProcess } from "../shards/ManagedShardProcess";
import { normalizeBridgeOptions } from "./configuration";
import { clearSyncAcknowledgements, identityOf, toError } from "./protocol";
import type {
	$ConnectionWaiter,
	$InboundRoute,
	$NormalizedBridgeOptions,
	$OutboundOperation,
	$OutboundOperationKind,
	$ShardInboundQueue,
	$SyncAcknowledgement,
} from "./types";

export abstract class BridgeCore {
	public readonly id: string;
	public readonly maxShards: number;
	public readonly generation: string;

	protected readonly options: $NormalizedBridgeOptions;
	protected readonly payloadPolicy: $PayloadPolicy;
	protected readonly processes = new Map<number, ManagedShardProcess>();
	protected readonly assignments = new Map<number, number>();
	protected readonly nextProcessGeneration = new Map<number, number>();
	protected readonly outbound = new Map<string, $OutboundOperation>();
	protected readonly inboundRoutes = new Map<string, $InboundRoute>();
	protected readonly syncAcknowledgements = new Map<string, $SyncAcknowledgement>();
	protected readonly connectionWaiters = new Set<$ConnectionWaiter>();
	protected readonly shardInboundQueues = new Map<ManagedShardProcess, $ShardInboundQueue>();
	protected readonly maintenanceListeners = new Set<(maintenance: boolean) => void>();
	protected readonly lifecycle = new AbortController();
	protected analytics: BridgeAnalyticsStore | undefined;
	protected socket: WebSocket | undefined;
	protected connectionGeneration = 0;
	protected synchronizedConnectionGeneration = 0;
	protected connectionTopologyVersion = 0;
	protected topologyVersion = 0;
	protected totalShards = 0;
	protected connectionReady = false;
	protected maintenance = true;
	protected lifecycleState: $BridgeState = "idle";
	protected reconnectTask: Promise<void> | undefined;
	protected stopPromise: Promise<void> | undefined;
	protected heartbeatTimer: ReturnType<typeof setInterval> | undefined;
	protected inboundMessages = 0;
	protected syncId: string | undefined;

	public constructor(options: $BridgeClientOptions) {
		this.options = normalizeBridgeOptions(options);
		this.payloadPolicy = this.options.payload;
		this.id = this.options.id;
		this.maxShards = this.options.maxShards;
		this.generation = Bun.randomUUIDv7();
	}

	public get state(): $BridgeState {
		return this.lifecycleState;
	}

	public get connected(): boolean {
		return this.connectionReady;
	}

	public get isInMaintenance(): boolean {
		return this.maintenance;
	}

	public get shards(): ReadonlyMap<number, $BridgeShardSnapshot> {
		const snapshot = new Map<number, $BridgeShardSnapshot>();
		for (const [shardId, managed] of this.processes) {
			snapshot.set(
				shardId,
				Object.freeze({
					assignmentEpoch: managed.assignmentEpoch,
					processGeneration: managed.processGeneration,
					shardId,
					state: managed.state,
				}),
			);
		}
		return snapshot;
	}

	protected abstract sendOutboundFailure(
		managed: ManagedShardProcess,
		kind: $OutboundOperationKind,
		id: string,
		cause: unknown,
	): Promise<void>;

	protected async notifyShardState(
		managed: ManagedShardProcess,
		state: $BridgeShardSnapshot["state"],
		required = false,
	): Promise<void> {
		if (this.socket === undefined) {
			if (required) throw new ShardingTransportError("Hub connection closed during shard startup.");
			return;
		}
		await this.sendHub("bridge.shard.state", createRequestId(`state-${managed.shardId}`), {
			...identityOf(managed),
			state,
		});
	}

	protected handleProcessExit(managed: ManagedShardProcess, error: Error | undefined): void {
		if (this.processes.get(managed.shardId) !== managed) return;
		this.processes.delete(managed.shardId);
		this.shardInboundQueues.delete(managed);
		for (const [id, pending] of this.outbound) {
			if (pending.managed !== managed) continue;
			clearTimeout(pending.timer);
			this.outbound.delete(id);
		}
		if (error !== undefined) this.report(error, `shard ${managed.shardId} exit`);
	}

	protected async stopUnassigned(managed: ManagedShardProcess, reason: string): Promise<void> {
		if (this.processes.get(managed.shardId) !== managed) return;
		const message = createWireMessage(
			"shard.control.shutdown",
			createRequestId(`unassign-${managed.shardId}`),
			{ commandId: "assignment-changed", reason },
			this.payloadPolicy,
		);
		await managed.stop(message);
		if (this.processes.get(managed.shardId) === managed) this.processes.delete(managed.shardId);
		this.shardInboundQueues.delete(managed);
	}

	protected async sendHub<Type extends WireMessageType>(
		type: Type,
		id: string,
		data: WireDataMap[Type],
	): Promise<void> {
		if (!BRIDGE_TO_HUB_TYPES.has(type)) throw new ShardingProtocolError(`${type} cannot be sent from a Bridge.`);
		const socket = this.socket;
		if (socket === undefined || socket.readyState !== WebSocket.OPEN) {
			throw new ShardingTransportError("Hub WebSocket is not open.");
		}
		const message = createWireMessage(type, id, data, this.payloadPolicy);
		const encoded = encodeWireMessage(message, this.payloadPolicy);
		const bytes = new TextEncoder().encode(encoded).byteLength;
		if (socket.bufferedAmount + bytes > this.options.maxBufferedBytes) {
			socket.close(1013, "Outbound backpressure limit reached");
			throw new ShardingCapacityError("Hub WebSocket backpressure limit reached.");
		}
		try {
			socket.send(encoded);
		} catch (cause) {
			try {
				socket.close(1011, "Outbound send failed");
			} catch {
				// The send failure remains the authoritative transport error.
			}
			throw new ShardingTransportError("Could not send to Hub.", { cause });
		}
	}

	protected async sendShard<Type extends WireMessageType>(
		managed: ManagedShardProcess,
		type: Type,
		id: string,
		data: WireDataMap[Type],
	): Promise<void> {
		if (!BRIDGE_TO_SHARD_TYPES.has(type)) throw new ShardingProtocolError(`${type} cannot be sent to a shard.`);
		const message = createWireMessage(type, id, data, this.payloadPolicy);
		await managed.send(message);
	}

	protected async setDisconnected(cause: Error): Promise<void> {
		this.connectionReady = false;
		this.syncId = undefined;
		this.updateMaintenance(true);
		clearSyncAcknowledgements(this.syncAcknowledgements);
		const cleanup: Promise<void>[] = [];
		for (const [id, pending] of this.outbound) {
			clearTimeout(pending.timer);
			this.outbound.delete(id);
			cleanup.push(this.sendOutboundFailure(pending.managed, pending.kind, id, cause));
		}
		for (const route of this.inboundRoutes.values()) clearTimeout(route.timer);
		this.inboundRoutes.clear();
		for (const managed of this.processes.values()) {
			cleanup.push(
				this.sendShard(managed, "shard.control.maintenance", createRequestId(`maintenance-${managed.shardId}`), {
					acknowledge: false,
					maintenance: true,
					topologyVersion: Math.max(1, this.topologyVersion),
				}).catch((sendCause: unknown) => this.report(toError(sendCause), `shard ${managed.shardId} maintenance`)),
			);
		}
		await Promise.all(cleanup);
	}

	protected updateMaintenance(maintenance: boolean): void {
		if (maintenance === this.maintenance) return;
		this.maintenance = maintenance;
		for (const listener of [...this.maintenanceListeners]) {
			try {
				const result: unknown = listener(maintenance);
				if (result instanceof Promise) {
					void result.catch((cause: unknown) => this.report(toError(cause), "maintenance listener"));
				}
			} catch (cause) {
				this.report(toError(cause), "maintenance listener");
			}
		}
	}

	protected resolveConnectionWaiters(): void {
		for (const waiter of this.connectionWaiters) {
			clearTimeout(waiter.timer);
			waiter.resolve();
		}
		this.connectionWaiters.clear();
	}

	protected rejectConnectionWaiters(cause: Error): void {
		for (const waiter of this.connectionWaiters) {
			clearTimeout(waiter.timer);
			waiter.reject(cause);
		}
		this.connectionWaiters.clear();
	}

	protected requireAnalytics(): BridgeAnalyticsStore {
		if (this.analytics === undefined) throw new ShardingStateError("Bridge analytics are not open.");
		return this.analytics;
	}

	protected report(error: Error, context: string): void {
		try {
			this.options.onError?.(error, context);
		} catch {
			// Error observers cannot recurse into Bridge lifecycle.
		}
	}
}
