import { ShardingProtocolError } from "../errors/ShardingError";
import { MAX_SHARDS } from "../internal/limits";
import { normalizeRestartPolicy } from "../internal/policies";
import { requireExactKeys, requireOptionalKeys } from "../protocol/codec";
import {
	readArray,
	readBoolean,
	readError,
	readInteger,
	readRecord,
	readShardId,
	readString,
} from "../protocol/readers";
import type { ParsedWireMessage, ShardIdentityData } from "../protocol/types";
import type { $RestartPolicy } from "../types/common";
import type { $PersistedShardState } from "../types/hub";
import type { $HubSessionRunningShard } from "./session/HubBridgeSession";

export interface $ParsedBridgeHello {
	readonly bridgeGeneration: string;
	readonly bridgeId: string;
	readonly connectionGeneration: number;
	readonly maxShards: number;
	readonly restartPolicy: $RestartPolicy;
	readonly runningShards: readonly $HubSessionRunningShard[];
}

export type $ParsedOperationResponse =
	| { readonly error: ReturnType<typeof readError>; readonly ok: false }
	| { readonly ok: true; readonly value?: unknown };

const SHARD_STATES: ReadonlySet<string> = new Set(["starting", "ready", "stopping", "stopped", "failed"]);

export function parseBridgeHello(message: ParsedWireMessage): $ParsedBridgeHello {
	requireExactKeys(
		message.data,
		new Set(["bridgeGeneration", "bridgeId", "connectionGeneration", "maxShards", "restartPolicy", "runningShards"]),
		"bridge.hello data",
	);
	const maxShards = readInteger(message.data, "maxShards", 1, MAX_SHARDS);
	const restartPolicyRecord = readRecord(message.data, "restartPolicy");
	let restartPolicy: $RestartPolicy;
	try {
		restartPolicy = normalizeRestartPolicy(restartPolicyRecord);
	} catch (cause) {
		throw new ShardingProtocolError("bridge.hello restartPolicy is invalid.", { cause });
	}
	const runningShards = readArray(message.data, "runningShards", maxShards).map(
		(value, index): $HubSessionRunningShard => {
			if (typeof value !== "object" || value === null || Array.isArray(value)) {
				throw new ShardingProtocolError(`bridge.hello runningShards[${index}] must be an object.`);
			}
			const record = copyRecord(value, `bridge.hello runningShards[${index}]`);
			requireExactKeys(
				record,
				new Set(["assignmentEpoch", "processGeneration", "ready", "shardId"]),
				`bridge.hello runningShards[${index}]`,
			);
			return Object.freeze({
				...readIdentity(record),
				ready: readBoolean(record, "ready"),
			});
		},
	);
	const seen = new Set<number>();
	for (const shard of runningShards) {
		if (seen.has(shard.shardId)) throw new ShardingProtocolError(`Bridge hello repeats shard ${shard.shardId}.`);
		seen.add(shard.shardId);
	}
	return Object.freeze({
		bridgeGeneration: readString(message.data, "bridgeGeneration"),
		bridgeId: readString(message.data, "bridgeId"),
		connectionGeneration: readInteger(message.data, "connectionGeneration", 1, Number.MAX_SAFE_INTEGER),
		maxShards,
		restartPolicy,
		runningShards: Object.freeze(runningShards),
	});
}

export function parseHeartbeat(message: ParsedWireMessage): {
	readonly bridgeGeneration: string;
	readonly connectionGeneration: number;
	readonly sentAt: number;
} {
	requireExactKeys(
		message.data,
		new Set(["bridgeGeneration", "connectionGeneration", "sentAt"]),
		"bridge.heartbeat data",
	);
	return Object.freeze({
		bridgeGeneration: readString(message.data, "bridgeGeneration"),
		connectionGeneration: readInteger(message.data, "connectionGeneration", 1, Number.MAX_SAFE_INTEGER),
		sentAt: readInteger(message.data, "sentAt", 0, Number.MAX_SAFE_INTEGER),
	});
}

export function readIdentity(data: Readonly<Record<string, unknown>>): ShardIdentityData {
	return Object.freeze({
		assignmentEpoch: readInteger(data, "assignmentEpoch", 1, Number.MAX_SAFE_INTEGER),
		processGeneration: readInteger(data, "processGeneration", 1, Number.MAX_SAFE_INTEGER),
		shardId: readShardId(data),
	});
}

export function readShardState(data: Readonly<Record<string, unknown>>): $PersistedShardState {
	const value = readString(data, "state");
	if (!SHARD_STATES.has(value)) throw new ShardingProtocolError(`Shard state "${value}" is invalid.`);
	switch (value) {
		case "starting":
		case "ready":
		case "stopping":
		case "stopped":
		case "failed":
			return value;
	}
	throw new ShardingProtocolError(`Shard state "${value}" is invalid.`);
}

export function parseOperationResponse(
	data: Readonly<Record<string, unknown>>,
	name: string,
	allowValue: boolean,
): $ParsedOperationResponse {
	const optional = allowValue ? new Set(["error", "value"]) : new Set(["error"]);
	requireOptionalKeys(data, new Set(["ok"]), optional, name);
	if (!readBoolean(data, "ok")) {
		if (Object.hasOwn(data, "value")) throw new ShardingProtocolError(`${name} cannot include value when ok is false.`);
		return Object.freeze({ error: readError(data.error, `${name}.error`), ok: false });
	}
	if (Object.hasOwn(data, "error")) throw new ShardingProtocolError(`${name} cannot include error when ok is true.`);
	if (!allowValue || !Object.hasOwn(data, "value")) return Object.freeze({ ok: true });
	return Object.freeze({ ok: true, value: data.value });
}

function copyRecord(value: object, name: string): Readonly<Record<string, unknown>> {
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
