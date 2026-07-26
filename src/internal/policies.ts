import { ShardingConfigurationError } from "../errors/ShardingError";
import type { $ReconnectPolicy, $RequestPolicy, $RestartPolicy } from "../types/common";
import { assertConfigurationKeys, snapshotConfigurationRecord } from "./configuration";
import {
	assertBoundedPositiveInteger,
	MAX_PENDING_REQUESTS,
	MAX_RESTART_ATTEMPTS,
	MAX_TIMER_MS,
	MIN_RECURRING_INTERVAL_MS,
} from "./limits";

export const DEFAULT_REQUEST_POLICY: $RequestPolicy = Object.freeze({
	maxPending: 256,
	timeoutMs: 15_000,
});

export const DEFAULT_RECONNECT_POLICY: $ReconnectPolicy = Object.freeze({
	initialDelayMs: 500,
	jitterRatio: 0.2,
	maxDelayMs: 30_000,
	multiplier: 2,
});

export const DEFAULT_RESTART_POLICY: $RestartPolicy = Object.freeze({
	initialDelayMs: 1_000,
	maxAttempts: 5,
	maxDelayMs: 30_000,
	windowMs: 60_000,
});

const REQUEST_KEYS = new Set(["maxPending", "timeoutMs"]);
const RECONNECT_KEYS = new Set(["initialDelayMs", "jitterRatio", "maxDelayMs", "multiplier"]);
const RESTART_KEYS = new Set(["initialDelayMs", "maxAttempts", "maxDelayMs", "windowMs"]);

export function normalizeRequestPolicy(input: Partial<$RequestPolicy> | undefined): $RequestPolicy {
	const options = input === undefined ? undefined : snapshotConfigurationRecord(input, "request");
	if (options !== undefined) assertConfigurationKeys(options, REQUEST_KEYS, "request");
	const timeoutMs = readNumber(options?.timeoutMs, DEFAULT_REQUEST_POLICY.timeoutMs, "request.timeoutMs");
	const maxPending = readNumber(options?.maxPending, DEFAULT_REQUEST_POLICY.maxPending, "request.maxPending");
	assertBoundedPositiveInteger(timeoutMs, "request.timeoutMs", MAX_TIMER_MS);
	assertBoundedPositiveInteger(maxPending, "request.maxPending", MAX_PENDING_REQUESTS);
	return Object.freeze({ maxPending, timeoutMs });
}

export function normalizeReconnectPolicy(input: Partial<$ReconnectPolicy> | undefined): $ReconnectPolicy {
	const options = input === undefined ? undefined : snapshotConfigurationRecord(input, "reconnect");
	if (options !== undefined) assertConfigurationKeys(options, RECONNECT_KEYS, "reconnect");
	const initialDelayMs = readNumber(
		options?.initialDelayMs,
		DEFAULT_RECONNECT_POLICY.initialDelayMs,
		"reconnect.initialDelayMs",
	);
	const maxDelayMs = readNumber(options?.maxDelayMs, DEFAULT_RECONNECT_POLICY.maxDelayMs, "reconnect.maxDelayMs");
	const multiplier = readNumber(options?.multiplier, DEFAULT_RECONNECT_POLICY.multiplier, "reconnect.multiplier");
	const jitterRatio = readNumber(options?.jitterRatio, DEFAULT_RECONNECT_POLICY.jitterRatio, "reconnect.jitterRatio");
	assertBoundedPositiveInteger(initialDelayMs, "reconnect.initialDelayMs");
	assertBoundedPositiveInteger(maxDelayMs, "reconnect.maxDelayMs");
	if (maxDelayMs < initialDelayMs) {
		throw new ShardingConfigurationError("reconnect.maxDelayMs cannot be less than reconnect.initialDelayMs.");
	}
	if (!Number.isFinite(multiplier) || multiplier < 1 || multiplier > 16) {
		throw new ShardingConfigurationError("reconnect.multiplier must be between 1 and 16.");
	}
	if (!Number.isFinite(jitterRatio) || jitterRatio < 0 || jitterRatio > 1) {
		throw new ShardingConfigurationError("reconnect.jitterRatio must be between 0 and 1.");
	}
	return Object.freeze({ initialDelayMs, jitterRatio, maxDelayMs, multiplier });
}

export function normalizeRestartPolicy(input: Partial<$RestartPolicy> | undefined): $RestartPolicy {
	const options = input === undefined ? undefined : snapshotConfigurationRecord(input, "restart");
	if (options !== undefined) assertConfigurationKeys(options, RESTART_KEYS, "restart");
	const maxAttempts = readNumber(options?.maxAttempts, DEFAULT_RESTART_POLICY.maxAttempts, "restart.maxAttempts");
	const windowMs = readNumber(options?.windowMs, DEFAULT_RESTART_POLICY.windowMs, "restart.windowMs");
	const initialDelayMs = readNumber(
		options?.initialDelayMs,
		DEFAULT_RESTART_POLICY.initialDelayMs,
		"restart.initialDelayMs",
	);
	const maxDelayMs = readNumber(options?.maxDelayMs, DEFAULT_RESTART_POLICY.maxDelayMs, "restart.maxDelayMs");
	assertBoundedPositiveInteger(maxAttempts, "restart.maxAttempts", MAX_RESTART_ATTEMPTS);
	assertBoundedPositiveInteger(windowMs, "restart.windowMs");
	assertBoundedPositiveInteger(initialDelayMs, "restart.initialDelayMs");
	assertBoundedPositiveInteger(maxDelayMs, "restart.maxDelayMs");
	if (initialDelayMs < MIN_RECURRING_INTERVAL_MS) {
		throw new ShardingConfigurationError(
			`restart.initialDelayMs must be at least ${MIN_RECURRING_INTERVAL_MS} milliseconds.`,
		);
	}
	if (maxDelayMs < initialDelayMs) {
		throw new ShardingConfigurationError("restart.maxDelayMs cannot be less than restart.initialDelayMs.");
	}
	return Object.freeze({ initialDelayMs, maxAttempts, maxDelayMs, windowMs });
}

export function reconnectDelay(policy: $ReconnectPolicy, attempt: number, random: () => number): number {
	const exponent = Math.max(0, Math.min(attempt, 52));
	const base = Math.min(policy.maxDelayMs, policy.initialDelayMs * policy.multiplier ** exponent);
	const sample = random();
	if (!Number.isFinite(sample) || sample < 0 || sample >= 1) {
		throw new ShardingConfigurationError(
			"Reconnect random source must return a value from 0 up to, but not including, 1.",
		);
	}
	const jitter = base * policy.jitterRatio * (sample * 2 - 1);
	return Math.max(MIN_RECURRING_INTERVAL_MS, Math.min(policy.maxDelayMs, Math.round(base + jitter)));
}

function readNumber(value: unknown, fallback: number, name: string): number {
	if (value === undefined) return fallback;
	if (typeof value !== "number") throw new ShardingConfigurationError(`${name} must be a number.`);
	return value;
}
