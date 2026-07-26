import { ShardingConfigurationError, ShardingProtocolError } from "../errors/ShardingError";
import {
	assertBoundedNonNegativeInteger,
	assertBoundedPositiveInteger,
	MAX_ARGUMENT_LENGTH,
	MAX_IDENTIFIER_LENGTH,
	MAX_SHARDS,
	MAX_TIMER_MS,
} from "./limits";

const IDENTIFIER_PATTERN = /^[A-Za-z0-9_][A-Za-z0-9._:@/-]*$/u;
const TOKEN_MAX_LENGTH = 4_096;

export function requireIdentifier(value: unknown, name: string): string {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > MAX_IDENTIFIER_LENGTH ||
		!IDENTIFIER_PATTERN.test(value) ||
		hasControlCharacter(value)
	) {
		throw new ShardingConfigurationError(
			`${name} must contain 1 to ${MAX_IDENTIFIER_LENGTH} safe identifier characters.`,
		);
	}
	return value;
}

export function requireProtocolIdentifier(value: unknown, name: string): string {
	try {
		return requireIdentifier(value, name);
	} catch (cause) {
		throw new ShardingProtocolError(`${name} is invalid.`, { cause });
	}
}

export function requireToken(value: unknown, name: string): string {
	if (
		typeof value !== "string" ||
		value.length < 16 ||
		value.length > TOKEN_MAX_LENGTH ||
		value.trim() !== value ||
		hasControlCharacter(value)
	) {
		throw new ShardingConfigurationError(
			`${name} must contain between 16 and ${TOKEN_MAX_LENGTH} characters without surrounding whitespace.`,
		);
	}
	return value;
}

export function requireShardId(value: unknown, name = "shardId"): number {
	if (typeof value !== "number") throw new ShardingConfigurationError(`${name} must be a number.`);
	assertBoundedNonNegativeInteger(value, name, MAX_SHARDS - 1);
	return value;
}

export function requireTotalShards(value: unknown, name = "totalShards"): number {
	if (typeof value !== "number") throw new ShardingConfigurationError(`${name} must be a number.`);
	assertBoundedPositiveInteger(value, name, MAX_SHARDS);
	return value;
}

export function requirePositiveInteger(value: unknown, name: string, maximum = MAX_TIMER_MS): number {
	if (typeof value !== "number") throw new ShardingConfigurationError(`${name} must be a number.`);
	assertBoundedPositiveInteger(value, name, maximum);
	return value;
}

export function requireNonNegativeInteger(value: unknown, name: string, maximum = MAX_TIMER_MS): number {
	if (typeof value !== "number") throw new ShardingConfigurationError(`${name} must be a number.`);
	assertBoundedNonNegativeInteger(value, name, maximum);
	return value;
}

export function requireBoundedString(value: unknown, name: string, maximum = MAX_ARGUMENT_LENGTH): string {
	if (typeof value !== "string" || value.length === 0 || value.length > maximum || hasUnpairedSurrogate(value)) {
		throw new ShardingConfigurationError(`${name} must contain between 1 and ${maximum} valid characters.`);
	}
	return value;
}

export function parseEnvironmentInteger(name: string, fallback: number | undefined, minimum: number): number {
	const raw = Bun.env[name];
	if (raw === undefined) {
		if (fallback !== undefined) return fallback;
		throw new ShardingConfigurationError(`${name} is required in a shard process.`);
	}
	if (!/^(?:0|[1-9]\d*)$/u.test(raw)) {
		throw new ShardingConfigurationError(`${name} must be a non-negative decimal integer.`);
	}
	const value = Number(raw);
	if (!Number.isSafeInteger(value) || value < minimum || value > MAX_TIMER_MS) {
		throw new ShardingConfigurationError(`${name} is outside the supported range.`);
	}
	return value;
}

export function createRequestId(prefix: string): string {
	requireIdentifier(prefix, "Request ID prefix");
	return `${prefix}:${Bun.randomUUIDv7()}`;
}

export function hasControlCharacter(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code <= 31 || code === 127) return true;
	}
	return false;
}

export function hasUnpairedSurrogate(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code < 0xd800 || code > 0xdfff) continue;
		if (code <= 0xdbff) {
			const next = value.charCodeAt(index + 1);
			if (next >= 0xdc00 && next <= 0xdfff) {
				index += 1;
				continue;
			}
		}
		return true;
	}
	return false;
}
