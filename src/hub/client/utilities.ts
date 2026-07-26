import {
	ShardingCapacityError,
	ShardingConfigurationError,
	ShardingProtocolError,
	ShardingStateError,
	ShardingTimeoutError,
	ShardingTransportError,
} from "../../errors/ShardingError";
import { requireIdentifier } from "../../internal/validation";
import { readBoolean, readError, readPayload } from "../../protocol/readers";
import type { ShardIdentityData } from "../../protocol/types";
import type { $JsonValue } from "../../types/common";
import type { $PersistedAssignment, $PersistedShard, $PersistedShardState } from "../../types/hub";
import type { $ParsedOperationResponse } from "../protocol";
import { parseOperationResponse } from "../protocol";
import { RELEASED_ASSIGNMENT_PREFIX } from "./constants";
import type { RestartAssignmentIdentity } from "./types";

export function responseFields(
	data: Readonly<Record<string, unknown>>,
	name: string,
	allowValue: boolean,
): $ParsedOperationResponse {
	const projected: Record<string, unknown> = Object.create(null);
	projected.ok = readBoolean(data, "ok");
	if (Object.hasOwn(data, "error")) projected.error = readError(data.error, `${name}.error`);
	if (Object.hasOwn(data, "value")) projected.value = readPayload(data, "value");
	return parseOperationResponse(projected, name, allowValue);
}

export function persistedShard(
	bridgeId: string,
	identity: ShardIdentityData,
	state: $PersistedShardState,
	updatedAt: number,
): $PersistedShard {
	return Object.freeze({
		assignmentEpoch: identity.assignmentEpoch,
		bridgeId,
		processGeneration: identity.processGeneration,
		shardId: identity.shardId,
		state,
		updatedAt,
	});
}

export function classifyShardStateUpdate(
	previous: $PersistedShard | undefined,
	bridgeId: string,
	identity: ShardIdentityData,
	state: $PersistedShardState,
): "apply" | "duplicate" | "stale" {
	if (previous === undefined || previous.assignmentEpoch < identity.assignmentEpoch) return "apply";
	if (previous.assignmentEpoch > identity.assignmentEpoch) return "stale";
	if (previous.bridgeId !== bridgeId) {
		throw new ShardingProtocolError(`Shard ${identity.shardId} changed owners without advancing its assignment epoch.`);
	}
	if (previous.processGeneration < identity.processGeneration) return "apply";
	if (previous.processGeneration > identity.processGeneration) return "stale";
	if (previous.state === state) return "duplicate";
	if (isValidShardStateTransition(previous.state, state)) return "apply";
	throw new ShardingProtocolError(
		`Shard ${identity.shardId} cannot transition from ${previous.state} to ${state} in the same process generation.`,
	);
}

export function isValidShardStateTransition(previous: $PersistedShardState, next: $PersistedShardState): boolean {
	switch (previous) {
		case "assigned":
			return next === "starting" || next === "ready" || next === "failed" || next === "stopped";
		case "starting":
			return next === "ready" || next === "stopping" || next === "failed" || next === "stopped";
		case "ready":
			return next === "stopping" || next === "failed" || next === "stopped";
		case "stopping":
			return next === "failed" || next === "stopped";
		case "failed":
		case "stopped":
			return false;
	}
}

export function createAssignment(
	shardId: number,
	bridgeId: string,
	epoch: number,
	updatedAt: number,
): $PersistedAssignment {
	return Object.freeze({ bridgeId, epoch, shardId, updatedAt });
}

export function releasedAssignmentOwner(shardId: number): string {
	return `${RELEASED_ASSIGNMENT_PREFIX}${shardId}`;
}

export function isReleasedAssignment(assignment: $PersistedAssignment | undefined): boolean {
	return assignment !== undefined && assignment.bridgeId === releasedAssignmentOwner(assignment.shardId);
}

export function assignmentMatches(
	assignment: $PersistedAssignment | undefined,
	bridgeId: string,
	identity: RestartAssignmentIdentity,
): boolean {
	return (
		assignment !== undefined &&
		!isReleasedAssignment(assignment) &&
		assignment.bridgeId === bridgeId &&
		assignment.shardId === identity.shardId &&
		assignment.epoch === identity.assignmentEpoch
	);
}

export function sameAssignment(left: $PersistedAssignment | undefined, right: $PersistedAssignment): boolean {
	return (
		left !== undefined &&
		left.bridgeId === right.bridgeId &&
		left.epoch === right.epoch &&
		left.shardId === right.shardId
	);
}

export function identitiesEqual(left: ShardIdentityData, right: ShardIdentityData): boolean {
	return (
		left.assignmentEpoch === right.assignmentEpoch &&
		left.processGeneration === right.processGeneration &&
		left.shardId === right.shardId
	);
}

export function incrementEpoch(epoch: number, shardId: number): number {
	if (!Number.isSafeInteger(epoch) || epoch < 1 || epoch >= Number.MAX_SAFE_INTEGER) {
		throw new ShardingStateError(`Shard ${shardId} assignment epoch cannot be incremented.`);
	}
	return epoch + 1;
}

export function identityKey(bridgeId: string, identity: ShardIdentityData): string {
	return `${bridgeId}:${identity.shardId}:${identity.assignmentEpoch}:${identity.processGeneration}`;
}

export function shardSessionKey(bridgeId: string, shardId: number): string {
	return `${bridgeId}:${shardId}`;
}

export function normalizeGatewayEndpoint(value: unknown): string | URL {
	if (typeof value !== "string" && !(value instanceof URL)) {
		throw new ShardingConfigurationError("gatewayEndpoint must be an absolute HTTPS URL.");
	}
	let url: URL;
	try {
		url = new URL(value);
	} catch (cause) {
		throw new ShardingConfigurationError("gatewayEndpoint must be an absolute HTTPS URL.", { cause });
	}
	if (url.protocol !== "https:") throw new ShardingConfigurationError("gatewayEndpoint must use HTTPS.");
	return url.href;
}

export function parseHeaderInteger(value: string | null, name: string): number {
	if (value === null || !/^[1-9]\d*$/u.test(value)) {
		throw new ShardingConfigurationError(`${name} must be a positive decimal integer.`);
	}
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed)) throw new ShardingConfigurationError(`${name} is outside the supported range.`);
	return parsed;
}

export function parseOptionalQueryInteger(value: string | null, name: string): number | undefined {
	if (value === null) return undefined;
	if (!/^(?:0|[1-9]\d*)$/u.test(value)) {
		throw new ShardingConfigurationError(`${name} must be a non-negative decimal integer.`);
	}
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed)) throw new ShardingConfigurationError(`${name} is outside the supported range.`);
	return parsed;
}

export function matchReleasePath(pathname: string): string | null {
	const match = /^\/bridges\/([^/]+)$/u.exec(pathname);
	if (match === null) return null;
	const encoded = match[1];
	if (encoded === undefined) return null;
	let decoded: string;
	try {
		decoded = decodeURIComponent(encoded);
	} catch (cause) {
		throw new ShardingConfigurationError("Bridge path contains invalid percent encoding.", { cause });
	}
	return requireIdentifier(decoded, "bridgeId");
}

export function constantTimeTokenMatch(header: string | null, expectedToken: string): boolean {
	if (header === null) return false;
	const expected = new TextEncoder().encode(`Bearer ${expectedToken}`);
	const received = new TextEncoder().encode(header);
	const length = Math.max(expected.length, received.length);
	let mismatch = expected.length ^ received.length;
	for (let index = 0; index < length; index += 1) {
		mismatch |= (expected[index] ?? 0) ^ (received[index] ?? 0);
	}
	return mismatch === 0;
}

export function textResponse(body: string, status: number): Response {
	return new Response(body, {
		headers: {
			"content-type": "text/plain; charset=utf-8",
		},
		status,
	});
}

export function errorStatus(error: Error): number {
	if (error instanceof ShardingConfigurationError || error instanceof ShardingProtocolError) return 400;
	if (error instanceof ShardingStateError) return 409;
	if (error instanceof ShardingCapacityError) return 429;
	if (error instanceof ShardingTimeoutError) return 504;
	if (error instanceof ShardingTransportError) return 503;
	return 500;
}

export function errorCode(error: Error): string {
	if (
		error instanceof ShardingConfigurationError ||
		error instanceof ShardingProtocolError ||
		error instanceof ShardingStateError ||
		error instanceof ShardingCapacityError ||
		error instanceof ShardingTimeoutError ||
		error instanceof ShardingTransportError
	) {
		return error.code;
	}
	return "ERROR";
}

export function toError(value: unknown): Error {
	if (value instanceof Error) return value;
	return new Error("Operation failed with a non-Error value.", { cause: value });
}

export function isJsonValue(value: unknown): value is $JsonValue {
	if (value === null || typeof value === "boolean" || typeof value === "string") return true;
	if (typeof value === "number") return Number.isFinite(value);
	if (Array.isArray(value)) {
		for (const entry of value) {
			if (!isJsonValue(entry)) return false;
		}
		return true;
	}
	if (typeof value !== "object") return false;
	for (const key of Object.keys(value)) {
		const descriptor = Reflect.getOwnPropertyDescriptor(value, key);
		if (descriptor === undefined || !("value" in descriptor) || !isJsonValue(descriptor.value)) return false;
	}
	return true;
}
