import type { SerializedError } from "../internal/errors";

export const PROTOCOL_VERSION = 1;

export type RouteKind = "message" | "request";

export interface ShardIdentityData {
	readonly assignmentEpoch: number;
	readonly processGeneration: number;
	readonly shardId: number;
}

export interface RunningShardData extends ShardIdentityData {
	readonly ready: boolean;
}

export interface AssignmentData {
	readonly epoch: number;
	readonly shardId: number;
}

export interface AnalyticsData extends ShardIdentityData {
	readonly collectedAt: number;
	readonly payload: unknown;
}

export interface RouteRequestData extends ShardIdentityData {
	readonly kind: RouteKind;
	readonly payload: unknown;
	readonly targetShardId: number;
}

export interface RoutedRequestData {
	readonly kind: RouteKind;
	readonly payload: unknown;
	readonly sourceShardId: number | null;
	readonly target: ShardIdentityData;
}

export interface RouteResponseData extends ShardIdentityData {
	readonly error?: SerializedError;
	readonly ok: boolean;
	readonly sourceShardId: number | null;
	readonly value?: unknown;
}

export interface EvalResultEntryData {
	readonly shardId: number;
	readonly value: unknown;
}

export interface WireDataMap {
	readonly "bridge.analytics": AnalyticsData;
	readonly "bridge.eval.prepared": ShardIdentityData & {
		readonly error?: SerializedError;
		readonly ok: boolean;
	};
	readonly "bridge.eval.request": ShardIdentityData & {
		readonly context: unknown;
		readonly evaluator: string;
	};
	readonly "bridge.eval.result": ShardIdentityData & {
		readonly error?: SerializedError;
		readonly ok: boolean;
		readonly value?: unknown;
	};
	readonly "bridge.heartbeat": {
		readonly bridgeGeneration: string;
		readonly connectionGeneration: number;
		readonly sentAt: number;
	};
	readonly "bridge.hello": {
		readonly bridgeGeneration: string;
		readonly bridgeId: string;
		readonly connectionGeneration: number;
		readonly maxShards: number;
		readonly restartPolicy: {
			readonly initialDelayMs: number;
			readonly maxAttempts: number;
			readonly maxDelayMs: number;
			readonly windowMs: number;
		};
		readonly runningShards: readonly RunningShardData[];
	};
	readonly "bridge.identify.request": ShardIdentityData;
	readonly "bridge.route.request": RouteRequestData;
	readonly "bridge.route.response": RouteResponseData;
	readonly "bridge.shard.state": ShardIdentityData & {
		readonly state: "starting" | "ready" | "stopping" | "stopped" | "failed";
	};
	readonly "bridge.shard.stopped": ShardIdentityData & {
		readonly commandId: string;
	};
	readonly "bridge.sync.ready": {
		readonly topologyVersion: number;
	};
	readonly "hub.eval.cancel": {
		readonly reason: string;
		readonly target: ShardIdentityData;
	};
	readonly "hub.eval.commit": {
		readonly executeAt: number;
		readonly target: ShardIdentityData;
	};
	readonly "hub.eval.prepare": {
		readonly context: unknown;
		readonly evaluator: string;
		readonly sourceShardId: number;
		readonly target: ShardIdentityData;
	};
	readonly "hub.eval.response": {
		readonly error?: SerializedError;
		readonly ok: boolean;
		readonly results?: readonly EvalResultEntryData[];
		readonly sourceShardId: number;
	};
	readonly "hub.identify.response": {
		readonly error?: SerializedError;
		readonly granted: boolean;
		readonly shardId: number;
	};
	readonly "hub.route.request": RoutedRequestData;
	readonly "hub.route.response": RouteResponseData;
	readonly "hub.shard.start": {
		readonly assignmentEpoch: number;
		readonly shardId: number;
		readonly totalShards: number;
	};
	readonly "hub.shard.stop": ShardIdentityData & {
		readonly reason: string;
	};
	readonly "hub.sync": {
		readonly assignments: readonly AssignmentData[];
		readonly bridgeGeneration: string;
		readonly connectionGeneration: number;
		readonly topologyVersion: number;
		readonly totalShards: number;
	};
	readonly "shard.analytics": {
		readonly collectedAt: number;
		readonly payload: unknown;
	};
	readonly "shard.booted": ShardIdentityData & {
		readonly totalShards: number;
	};
	readonly "shard.eval.prepared": {
		readonly error?: SerializedError;
		readonly ok: boolean;
	};
	readonly "shard.eval.request": {
		readonly context: unknown;
		readonly evaluator: string;
	};
	readonly "shard.eval.result": {
		readonly error?: SerializedError;
		readonly ok: boolean;
		readonly value?: unknown;
	};
	readonly "shard.heartbeat": ShardIdentityData;
	readonly "shard.identify.request": Record<never, never>;
	readonly "shard.ready": ShardIdentityData;
	readonly "shard.route.request": {
		readonly kind: RouteKind;
		readonly payload: unknown;
		readonly targetShardId: number;
	};
	readonly "shard.route.response": {
		readonly error?: SerializedError;
		readonly ok: boolean;
		readonly value?: unknown;
	};
	readonly "shard.shutdown.complete": {
		readonly commandId: string;
	};
	readonly "shard.sync.ack": {
		readonly topologyVersion: number;
	};
	readonly "shard.control.eval.cancel": {
		readonly reason: string;
	};
	readonly "shard.control.eval.commit": {
		readonly executeAt: number;
	};
	readonly "shard.control.eval.prepare": {
		readonly context: unknown;
		readonly evaluator: string;
		readonly sourceShardId: number;
	};
	readonly "shard.control.eval.response": {
		readonly error?: SerializedError;
		readonly ok: boolean;
		readonly results?: readonly EvalResultEntryData[];
	};
	readonly "shard.control.identify.response": {
		readonly error?: SerializedError;
		readonly granted: boolean;
	};
	readonly "shard.control.maintenance": {
		readonly acknowledge: boolean;
		readonly maintenance: boolean;
		readonly topologyVersion: number;
	};
	readonly "shard.control.route.request": {
		readonly kind: RouteKind;
		readonly payload: unknown;
		readonly sourceShardId: number | null;
	};
	readonly "shard.control.route.response": {
		readonly error?: SerializedError;
		readonly ok: boolean;
		readonly value?: unknown;
	};
	readonly "shard.control.shutdown": {
		readonly commandId: string;
		readonly reason: string;
	};
}

export type WireMessageType = keyof WireDataMap;

export type WireMessage<Type extends WireMessageType = WireMessageType> = {
	readonly data: WireDataMap[Type];
	readonly id: string;
	readonly type: Type;
	readonly version: typeof PROTOCOL_VERSION;
};

export interface ParsedWireMessage {
	readonly data: Readonly<Record<string, unknown>>;
	readonly id: string;
	readonly type: WireMessageType;
	readonly version: typeof PROTOCOL_VERSION;
}

export const BRIDGE_TO_HUB_TYPES: ReadonlySet<WireMessageType> = new Set([
	"bridge.analytics",
	"bridge.eval.prepared",
	"bridge.eval.request",
	"bridge.eval.result",
	"bridge.heartbeat",
	"bridge.hello",
	"bridge.identify.request",
	"bridge.route.request",
	"bridge.route.response",
	"bridge.shard.state",
	"bridge.shard.stopped",
	"bridge.sync.ready",
]);

export const HUB_TO_BRIDGE_TYPES: ReadonlySet<WireMessageType> = new Set([
	"hub.eval.cancel",
	"hub.eval.commit",
	"hub.eval.prepare",
	"hub.eval.response",
	"hub.identify.response",
	"hub.route.request",
	"hub.route.response",
	"hub.shard.start",
	"hub.shard.stop",
	"hub.sync",
]);

export const SHARD_TO_BRIDGE_TYPES: ReadonlySet<WireMessageType> = new Set([
	"shard.analytics",
	"shard.booted",
	"shard.eval.prepared",
	"shard.eval.request",
	"shard.eval.result",
	"shard.heartbeat",
	"shard.identify.request",
	"shard.ready",
	"shard.route.request",
	"shard.route.response",
	"shard.shutdown.complete",
	"shard.sync.ack",
]);

export const BRIDGE_TO_SHARD_TYPES: ReadonlySet<WireMessageType> = new Set([
	"shard.control.eval.cancel",
	"shard.control.eval.commit",
	"shard.control.eval.prepare",
	"shard.control.eval.response",
	"shard.control.identify.response",
	"shard.control.maintenance",
	"shard.control.route.request",
	"shard.control.route.response",
	"shard.control.shutdown",
]);

export const ALL_WIRE_TYPES: ReadonlySet<WireMessageType> = new Set([
	...BRIDGE_TO_HUB_TYPES,
	...HUB_TO_BRIDGE_TYPES,
	...SHARD_TO_BRIDGE_TYPES,
	...BRIDGE_TO_SHARD_TYPES,
]);
