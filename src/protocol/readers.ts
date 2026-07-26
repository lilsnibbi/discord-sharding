import { ShardingProtocolError } from "../errors/ShardingError";
import type { SerializedError } from "../internal/errors";
import { MAX_EVALUATOR_SOURCE_LENGTH, MAX_IDENTIFIER_LENGTH, MAX_SHARDS, MAX_TIMER_MS } from "../internal/limits";
import { requireExactKeys, requireOptionalKeys } from "./codec";

const ERROR_KEYS = new Set(["code", "message", "name"]);
const textEncoder = new TextEncoder();

export function readString(
	value: Readonly<Record<string, unknown>>,
	key: string,
	maximum = MAX_IDENTIFIER_LENGTH,
): string {
	const candidate = value[key];
	if (typeof candidate !== "string" || candidate.length === 0 || candidate.length > maximum) {
		throw new ShardingProtocolError(`${key} must contain between 1 and ${maximum} characters.`);
	}
	return candidate;
}

export function readEvaluator(value: Readonly<Record<string, unknown>>, key = "evaluator"): string {
	return readString(value, key, MAX_EVALUATOR_SOURCE_LENGTH);
}

export function readBoolean(value: Readonly<Record<string, unknown>>, key: string): boolean {
	const candidate = value[key];
	if (typeof candidate !== "boolean") throw new ShardingProtocolError(`${key} must be a boolean.`);
	return candidate;
}

export function readInteger(
	value: Readonly<Record<string, unknown>>,
	key: string,
	minimum = 0,
	maximum = MAX_TIMER_MS,
): number {
	const candidate = value[key];
	if (typeof candidate !== "number" || !Number.isSafeInteger(candidate) || candidate < minimum || candidate > maximum) {
		throw new ShardingProtocolError(`${key} must be an integer from ${minimum} through ${maximum}.`);
	}
	return candidate;
}

export function readShardId(value: Readonly<Record<string, unknown>>, key = "shardId"): number {
	return readInteger(value, key, 0, MAX_SHARDS - 1);
}

export function readNullableShardId(value: Readonly<Record<string, unknown>>, key: string): number | null {
	const candidate = value[key];
	return candidate === null ? null : readShardId(value, key);
}

export function readRecord(value: Readonly<Record<string, unknown>>, key: string): Readonly<Record<string, unknown>> {
	const candidate = value[key];
	if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
		throw new ShardingProtocolError(`${key} must be an object.`);
	}
	const result: Record<string, unknown> = Object.create(null);
	for (const property of Object.keys(candidate)) {
		const descriptor = Reflect.getOwnPropertyDescriptor(candidate, property);
		if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) {
			throw new ShardingProtocolError(`${key}.${property} must be a getter-free data field.`);
		}
		result[property] = descriptor.value;
	}
	return Object.freeze(result);
}

export function readArray(value: Readonly<Record<string, unknown>>, key: string, maximum: number): readonly unknown[] {
	const candidate = value[key];
	if (!Array.isArray(candidate) || candidate.length > maximum) {
		throw new ShardingProtocolError(`${key} must be an array containing at most ${maximum} values.`);
	}
	return candidate;
}

export function readPayload(value: Readonly<Record<string, unknown>>, key: string): unknown {
	if (!Object.hasOwn(value, key)) throw new ShardingProtocolError(`${key} is required.`);
	return value[key];
}

export function readRouteKind(value: Readonly<Record<string, unknown>>, key = "kind"): "message" | "request" {
	const candidate = value[key];
	if (candidate !== "message" && candidate !== "request") {
		throw new ShardingProtocolError(`${key} must be "message" or "request".`);
	}
	return candidate;
}

export function readError(value: unknown, name = "error"): SerializedError {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new ShardingProtocolError(`${name} must be an object.`);
	}
	const record: Record<string, unknown> = Object.create(null);
	for (const key of Object.keys(value)) {
		const descriptor = Reflect.getOwnPropertyDescriptor(value, key);
		if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) {
			throw new ShardingProtocolError(`${name}.${key} must be a getter-free data field.`);
		}
		record[key] = descriptor.value;
	}
	requireExactKeys(record, ERROR_KEYS, name);
	return Object.freeze({
		code: readString(record, "code", 64),
		message: readString(record, "message", 2_048),
		name: readString(record, "name", 128),
	});
}

export function validateResponseShape(value: Readonly<Record<string, unknown>>, name: string): void {
	const ok = readBoolean(value, "ok");
	if (ok) {
		requireOptionalKeys(value, new Set(["ok"]), new Set(["value"]), name);
		return;
	}
	requireExactKeys(value, new Set(["error", "ok"]), name);
	readError(value.error, `${name}.error`);
}

export function encodedBytes(value: string): number {
	return textEncoder.encode(value).byteLength;
}
