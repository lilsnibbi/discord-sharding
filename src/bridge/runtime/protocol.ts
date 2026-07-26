import {
	ShardingCapacityError,
	ShardingConfigurationError,
	ShardingProtocolError,
	ShardingStateError,
} from "../../errors/ShardingError";
import { MAX_SHARDS } from "../../internal/limits";
import { requireExactKeys, requireOptionalKeys } from "../../protocol/codec";
import { readArray, readBoolean, readError, readInteger, readPayload, readShardId } from "../../protocol/readers";
import type { EvalResultEntryData, ShardIdentityData } from "../../protocol/types";
import type { ManagedShardProcess } from "../shards/ManagedShardProcess";
import type { $SyncAcknowledgement } from "./types";

export function readIdentity(value: Readonly<Record<string, unknown>>): ShardIdentityData {
	return Object.freeze({
		assignmentEpoch: readInteger(value, "assignmentEpoch", 1, Number.MAX_SAFE_INTEGER),
		processGeneration: readInteger(value, "processGeneration", 1, Number.MAX_SAFE_INTEGER),
		shardId: readShardId(value),
	});
}

export function identityOf(managed: ManagedShardProcess): ShardIdentityData {
	return Object.freeze({
		assignmentEpoch: managed.assignmentEpoch,
		processGeneration: managed.processGeneration,
		shardId: managed.shardId,
	});
}

export function matchesIdentity(managed: ManagedShardProcess, identity: ShardIdentityData): boolean {
	return (
		managed.shardId === identity.shardId &&
		managed.assignmentEpoch === identity.assignmentEpoch &&
		managed.processGeneration === identity.processGeneration
	);
}

export function parseResponse(
	data: Readonly<Record<string, unknown>>,
	name: string,
):
	| { readonly error: ReturnType<typeof readError>; readonly ok: false }
	| { readonly ok: true; readonly value?: unknown } {
	requireOptionalKeys(data, new Set(["ok"]), new Set(["error", "value"]), name);
	if (!readBoolean(data, "ok")) {
		if (Object.hasOwn(data, "value")) throw new ShardingProtocolError(`${name} cannot include value after failure.`);
		return Object.freeze({ error: readError(data.error, `${name}.error`), ok: false });
	}
	if (Object.hasOwn(data, "error")) throw new ShardingProtocolError(`${name} cannot include error after success.`);
	return Object.hasOwn(data, "value")
		? Object.freeze({ ok: true, value: readPayload(data, "value") })
		: Object.freeze({ ok: true });
}

export function parseHubRouteResponse(data: Readonly<Record<string, unknown>>): {
	readonly result:
		| { readonly error: ReturnType<typeof readError>; readonly ok: false }
		| { readonly ok: true; readonly value?: unknown };
	readonly sourceShardId: number;
} {
	requireOptionalKeys(
		data,
		new Set(["assignmentEpoch", "ok", "processGeneration", "shardId", "sourceShardId"]),
		new Set(["error", "value"]),
		"hub.route.response data",
	);
	readIdentity(data);
	const result = readBoolean(data, "ok")
		? Object.hasOwn(data, "value")
			? Object.freeze({ ok: true as const, value: readPayload(data, "value") })
			: Object.freeze({ ok: true as const })
		: Object.freeze({ error: readError(data.error, "hub.route.response data.error"), ok: false as const });
	if (result.ok && Object.hasOwn(data, "error")) {
		throw new ShardingProtocolError("hub.route.response data cannot include error after success.");
	}
	if (!result.ok && Object.hasOwn(data, "value")) {
		throw new ShardingProtocolError("hub.route.response data cannot include value after failure.");
	}
	return Object.freeze({
		result,
		sourceShardId: readShardId(data, "sourceShardId"),
	});
}

export function parseEvalResponse(
	data: Readonly<Record<string, unknown>>,
):
	| { readonly error: ReturnType<typeof readError>; readonly ok: false }
	| { readonly ok: true; readonly results: readonly EvalResultEntryData[] } {
	requireOptionalKeys(data, new Set(["ok", "sourceShardId"]), new Set(["error", "results"]), "hub.eval.response data");
	if (!readBoolean(data, "ok")) {
		if (Object.hasOwn(data, "results")) {
			throw new ShardingProtocolError("hub.eval.response data cannot include results after failure.");
		}
		return Object.freeze({ error: readError(data.error, "hub.eval.response error"), ok: false });
	}
	if (Object.hasOwn(data, "error")) {
		throw new ShardingProtocolError("hub.eval.response data cannot include error after success.");
	}
	const results = readArray(data, "results", MAX_SHARDS).map((entry, index): EvalResultEntryData => {
		const record = copyRecord(entry, `hub.eval.response data.results[${index}]`);
		requireExactKeys(record, new Set(["shardId", "value"]), `hub.eval.response data.results[${index}]`);
		return Object.freeze({
			shardId: readShardId(record),
			value: readPayload(record, "value"),
		});
	});
	return Object.freeze({ ok: true, results: Object.freeze(results) });
}

export function copyRecord(value: unknown, name: string): Readonly<Record<string, unknown>> {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new ShardingProtocolError(`${name} must be an object.`);
	}
	const output: Record<string, unknown> = Object.create(null);
	for (const key of Object.keys(value)) {
		const descriptor = Reflect.getOwnPropertyDescriptor(value, key);
		if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) {
			throw new ShardingProtocolError(`${name}.${key} must be a getter-free data field.`);
		}
		output[key] = descriptor.value;
	}
	return Object.freeze(output);
}

export function clearSyncAcknowledgements(acknowledgements: Map<string, $SyncAcknowledgement>): void {
	for (const acknowledgement of acknowledgements.values()) clearTimeout(acknowledgement.timer);
	acknowledgements.clear();
}

export function mapsEqual(left: ReadonlyMap<number, number>, right: ReadonlyMap<number, number>): boolean {
	if (left.size !== right.size) return false;
	for (const [key, value] of left) {
		if (right.get(key) !== value) return false;
	}
	return true;
}

export function closeSocketForHubError(socket: WebSocket, error: Error): void {
	if (error instanceof ShardingCapacityError) {
		socket.close(1013, "Inbound capacity reached");
		return;
	}
	if (error instanceof ShardingStateError) {
		socket.close(1008, "Hub message violates Bridge state");
		return;
	}
	if (error instanceof ShardingProtocolError || error instanceof ShardingConfigurationError) {
		socket.close(1002, "Protocol failure");
		return;
	}
	socket.close(1011, "Hub message processing failed");
}

export function toError(value: unknown): Error {
	if (value instanceof Error) return value;
	return new Error("Operation failed with a non-Error value.", { cause: value });
}
