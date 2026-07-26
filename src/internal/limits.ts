import { ShardingConfigurationError } from "../errors/ShardingError";

/** Maximum child-process argument count accepted by one Bridge. */
export const MAX_ARGUMENT_COUNT = 1_024;
/** Maximum UTF-16 length of one child-process argument. */
export const MAX_ARGUMENT_LENGTH = 32_768;
/** Maximum number of explicit child environment overrides. */
export const MAX_ENVIRONMENT_ENTRIES = 4_096;
/** Maximum UTF-16 length of one child environment value. */
export const MAX_ENVIRONMENT_VALUE_LENGTH = 1_048_576;
/** Hard maximum UTF-8 size of one JSON-compatible IPC payload. */
export const MAX_PAYLOAD_BYTES = 16_777_216;
/** Hard maximum nesting depth of one JSON-compatible IPC payload. */
export const MAX_PAYLOAD_DEPTH = 128;
/** Hard maximum aggregate node count of one JSON-compatible IPC payload. */
export const MAX_PAYLOAD_NODES = 100_000;
/** Maximum pending request or inbound-handler capacity per endpoint. */
export const MAX_PENDING_REQUESTS = 65_536;
/** Minimum interval for repeating heartbeat work and automatic restart backoff. */
export const MIN_RECURRING_INTERVAL_MS = 100;
/** Maximum automatic restart attempts retained inside one window. */
export const MAX_RESTART_ATTEMPTS = 10_000;
/** Maximum supported Discord shard count. */
export const MAX_SHARDS = 100_000;
/** Largest delay accepted by Bun's 32-bit timer implementation. */
export const MAX_TIMER_MS = 2_147_483_647;
/** Maximum supported Bridge count retained by one Hub. */
export const MAX_BRIDGES = 10_000;
/** Maximum identifier length accepted by protocols and configuration. */
export const MAX_IDENTIFIER_LENGTH = 128;
/** Maximum evaluator source length accepted by the Hub. */
export const MAX_EVALUATOR_SOURCE_LENGTH = 1_048_576;
/** Maximum queued WebSocket bytes retained by one endpoint. */
export const MAX_BUFFERED_BYTES = 67_108_864;
/** Maximum queued WebSocket message count retained by one endpoint. */
export const MAX_QUEUED_MESSAGES = 65_536;

/**
 * Requires a positive, platform-bounded integer.
 *
 * @param value - Runtime value to validate.
 * @param name - Human-readable field name used in validation errors.
 * @param maximum - Inclusive upper bound.
 */
export function assertBoundedPositiveInteger(value: number, name: string, maximum = MAX_TIMER_MS): void {
	if (!Number.isSafeInteger(value) || value <= 0 || value > maximum) {
		throw new ShardingConfigurationError(`${name} must be a positive integer no greater than ${maximum}.`);
	}
}

/**
 * Requires a non-negative, platform-bounded integer.
 *
 * @param value - Runtime value to validate.
 * @param name - Human-readable field name used in validation errors.
 * @param maximum - Inclusive upper bound.
 */
export function assertBoundedNonNegativeInteger(value: number, name: string, maximum = MAX_TIMER_MS): void {
	if (!Number.isSafeInteger(value) || value < 0 || value > maximum) {
		throw new ShardingConfigurationError(`${name} must be a non-negative integer no greater than ${maximum}.`);
	}
}

/**
 * Narrows a runtime value to a non-array object record.
 *
 * @param value - Runtime value to validate.
 * @param name - Human-readable field name used in validation errors.
 */
export function assertRecord(value: unknown, name: string): asserts value is Record<string, unknown> {
	let isArray = false;
	try {
		isArray = Array.isArray(value);
	} catch (cause) {
		throw new ShardingConfigurationError(`${name} could not be inspected safely.`, { cause });
	}
	if (typeof value !== "object" || value === null || isArray) {
		throw new ShardingConfigurationError(`${name} must be an object.`);
	}
}
