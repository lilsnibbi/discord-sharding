import { ShardingConfigurationError, ShardingProtocolError } from "../errors/ShardingError";
import type { $PayloadPolicy } from "../types/common";
import { assertConfigurationKeys, snapshotConfigurationRecord } from "./configuration";
import { assertBoundedPositiveInteger, MAX_PAYLOAD_BYTES, MAX_PAYLOAD_DEPTH, MAX_PAYLOAD_NODES } from "./limits";

/** Default aggregate limits for one JSON-compatible IPC payload. */
export const DEFAULT_PAYLOAD_POLICY: $PayloadPolicy = Object.freeze({
	maxBytes: 1_048_576,
	maxDepth: 32,
	maxNodes: 10_000,
});

const PAYLOAD_POLICY_KEYS = new Set(["maxBytes", "maxDepth", "maxNodes"]);

interface $PayloadTraversal {
	readonly active: WeakSet<object>;
	readonly name: string;
	readonly policy: $PayloadPolicy;
	bytes: number;
	depth: number;
	nodes: number;
}

interface $PayloadMeasurements {
	readonly bytes: number;
	readonly depth: number;
	readonly nodes: number;
}

const encoder = new TextEncoder();
const normalizedPayloads = new WeakMap<object, $PayloadMeasurements>();

/**
 * Validates and freezes a complete payload policy.
 *
 * @param input - Optional policy overrides.
 * @returns A frozen policy with defaults applied.
 */
export function normalizePayloadPolicy(input: Partial<$PayloadPolicy> | undefined): $PayloadPolicy {
	const snapshot = input === undefined ? undefined : snapshotConfigurationRecord(input, "payload");
	if (snapshot !== undefined) assertConfigurationKeys(snapshot, PAYLOAD_POLICY_KEYS, "payload");
	const maxBytes = readPolicyNumber(snapshot?.maxBytes, DEFAULT_PAYLOAD_POLICY.maxBytes, "payload.maxBytes");
	const maxDepth = readPolicyNumber(snapshot?.maxDepth, DEFAULT_PAYLOAD_POLICY.maxDepth, "payload.maxDepth");
	const maxNodes = readPolicyNumber(snapshot?.maxNodes, DEFAULT_PAYLOAD_POLICY.maxNodes, "payload.maxNodes");
	assertBoundedPositiveInteger(maxBytes, "payload.maxBytes", MAX_PAYLOAD_BYTES);
	assertBoundedPositiveInteger(maxDepth, "payload.maxDepth", MAX_PAYLOAD_DEPTH);
	assertBoundedPositiveInteger(maxNodes, "payload.maxNodes", MAX_PAYLOAD_NODES);
	const policy: $PayloadPolicy = { maxBytes, maxDepth, maxNodes };
	return Object.freeze(policy);
}

function readPolicyNumber(value: unknown, fallback: number, name: string): number {
	if (value === undefined) return fallback;
	if (typeof value !== "number") {
		throw new ShardingConfigurationError(`${name} must be a number.`);
	}
	return value;
}

/**
 * Creates an immutable, getter-free JSON-compatible payload snapshot.
 *
 * @param value - Untrusted outbound or inbound application payload.
 * @param policy - Aggregate byte, depth, and node limits.
 * @param name - Human-readable payload name used in validation errors.
 * @returns An immutable JSON-compatible snapshot.
 * @throws {@link ShardingProtocolError} When the payload is unsafe, unsupported, cyclic, or over a limit.
 */
export function normalizePayload(value: unknown, policy: $PayloadPolicy, name = "IPC payload"): unknown {
	if (typeof value === "object" && value !== null) {
		const measurements = normalizedPayloads.get(value);
		if (
			measurements !== undefined &&
			measurements.bytes <= policy.maxBytes &&
			measurements.depth <= policy.maxDepth &&
			measurements.nodes <= policy.maxNodes
		) {
			return value;
		}
	}
	const traversal: $PayloadTraversal = {
		active: new WeakSet(),
		bytes: 0,
		depth: 0,
		name,
		nodes: 0,
		policy,
	};
	const normalized = visitPayload(value, 1, traversal);
	if (typeof normalized === "object" && normalized !== null) {
		normalizedPayloads.set(normalized, {
			bytes: traversal.bytes,
			depth: traversal.depth,
			nodes: traversal.nodes,
		});
	}
	return normalized;
}

function visitPayload(value: unknown, depth: number, traversal: $PayloadTraversal): unknown {
	traversal.nodes += 1;
	traversal.depth = Math.max(traversal.depth, depth);
	if (traversal.nodes > traversal.policy.maxNodes) {
		throw new ShardingProtocolError(`${traversal.name} exceeds ${traversal.policy.maxNodes} JSON nodes.`);
	}
	if (depth > traversal.policy.maxDepth) {
		throw new ShardingProtocolError(`${traversal.name} exceeds a JSON depth of ${traversal.policy.maxDepth}.`);
	}

	if (value === null) {
		addBytes(traversal, 4);
		return null;
	}

	switch (typeof value) {
		case "boolean":
			addBytes(traversal, value ? 4 : 5);
			return value;
		case "number": {
			if (!Number.isFinite(value)) {
				throw new ShardingProtocolError(`${traversal.name} numbers must be finite.`);
			}
			const serialized = JSON.stringify(value);
			if (serialized === undefined) {
				throw new ShardingProtocolError(`${traversal.name} contains an unserializable number.`);
			}
			addBytes(traversal, serialized.length);
			return Object.is(value, -0) ? 0 : value;
		}
		case "string":
			addSerializedStringBytes(traversal, value);
			return value;
		case "object":
			return visitContainer(value, depth, traversal);
		default:
			throw new ShardingProtocolError(
				`${traversal.name} must contain JSON-compatible null, boolean, number, string, array, or object values.`,
			);
	}
}

function visitContainer(value: object, depth: number, traversal: $PayloadTraversal): unknown {
	if (traversal.active.has(value)) {
		throw new ShardingProtocolError(`${traversal.name} cannot contain cycles.`);
	}
	traversal.active.add(value);
	try {
		let isArray: boolean;
		try {
			isArray = Array.isArray(value);
		} catch (cause) {
			throw new ShardingProtocolError(`${traversal.name} container type could not be inspected safely.`, { cause });
		}
		return isArray ? visitArray(value as readonly unknown[], depth, traversal) : visitObject(value, depth, traversal);
	} finally {
		traversal.active.delete(value);
	}
}

function visitArray(value: readonly unknown[], depth: number, traversal: $PayloadTraversal): readonly unknown[] {
	const descriptors = inspectDescriptors(value, traversal.name, traversal.policy.maxNodes + 1);
	const lengthDescriptor = descriptors.get("length");
	const lengthValue: unknown = lengthDescriptor?.value;
	if (
		typeof lengthValue !== "number" ||
		!Number.isSafeInteger(lengthValue) ||
		lengthValue < 0 ||
		lengthValue > traversal.policy.maxNodes
	) {
		throw new ShardingProtocolError(`${traversal.name} contains an invalid or oversized array.`);
	}
	const length = lengthValue;
	for (const [key, descriptor] of descriptors) {
		if (typeof key === "symbol") {
			throw new ShardingProtocolError(`${traversal.name} cannot contain symbol properties.`);
		}
		if (typeof key !== "string") {
			throw new ShardingProtocolError(`${traversal.name} contains an invalid property key.`);
		}
		if (key === "length" || descriptor.enumerable !== true) continue;
		if (!isCanonicalArrayIndex(key) || Number(key) >= length) {
			throw new ShardingProtocolError(`${traversal.name} arrays cannot contain named enumerable properties.`);
		}
	}

	addBytes(traversal, 2 + Math.max(0, length - 1));
	const snapshot: unknown[] = [];
	for (let index = 0; index < length; index += 1) {
		const descriptor = descriptors.get(String(index));
		if (descriptor === undefined || !("value" in descriptor)) {
			throw new ShardingProtocolError(`${traversal.name} arrays must be dense and getter-free.`);
		}
		const child: unknown = descriptor.value;
		snapshot.push(visitPayload(child, depth + 1, traversal));
	}
	return Object.freeze(snapshot);
}

function visitObject(value: object, depth: number, traversal: $PayloadTraversal): Readonly<Record<string, unknown>> {
	let prototype: object | null;
	try {
		prototype = Object.getPrototypeOf(value);
	} catch (cause) {
		throw new ShardingProtocolError(`${traversal.name} has an unreadable prototype.`, { cause });
	}
	if (prototype !== Object.prototype && prototype !== null) {
		throw new ShardingProtocolError(`${traversal.name} objects must use Object.prototype or a null prototype.`);
	}

	const descriptors = inspectDescriptors(value, traversal.name, traversal.policy.maxNodes);
	addBytes(traversal, 2);
	const snapshot: Record<string, unknown> = Object.create(null);
	let hasEntry = false;
	for (const [key, descriptor] of descriptors) {
		if (typeof key === "symbol") {
			throw new ShardingProtocolError(`${traversal.name} cannot contain symbol properties.`);
		}
		if (typeof key !== "string") {
			throw new ShardingProtocolError(`${traversal.name} contains an invalid property key.`);
		}
		if (descriptor.enumerable !== true) continue;
		if (!("value" in descriptor)) {
			throw new ShardingProtocolError(`${traversal.name} objects must be getter-free.`);
		}
		if (hasEntry) addBytes(traversal, 1);
		hasEntry = true;
		addSerializedStringBytes(traversal, key);
		addBytes(traversal, 1);
		const child: unknown = descriptor.value;
		snapshot[key] = visitPayload(child, depth + 1, traversal);
	}
	return Object.freeze(snapshot);
}

function inspectDescriptors(
	value: object,
	name: string,
	maximum: number,
): ReadonlyMap<PropertyKey, PropertyDescriptor> {
	let keys: readonly PropertyKey[];
	try {
		keys = Reflect.ownKeys(value);
	} catch (cause) {
		throw new ShardingProtocolError(`${name} properties could not be inspected safely.`, { cause });
	}
	if (keys.length > maximum) {
		throw new ShardingProtocolError(`${name} contains too many own properties.`);
	}
	const descriptors = new Map<PropertyKey, PropertyDescriptor>();
	for (const key of keys) {
		let descriptor: PropertyDescriptor | undefined;
		try {
			descriptor = Reflect.getOwnPropertyDescriptor(value, key);
		} catch (cause) {
			throw new ShardingProtocolError(`${name} property ${String(key)} could not be inspected safely.`, { cause });
		}
		if (descriptor === undefined) {
			throw new ShardingProtocolError(`${name} changed while its properties were being inspected.`);
		}
		descriptors.set(key, descriptor);
	}
	return descriptors;
}

function isCanonicalArrayIndex(value: string): boolean {
	if (value.length > 10) return false;
	if (!/^(?:0|[1-9]\d*)$/u.test(value)) return false;
	const parsed = Number(value);
	return Number.isSafeInteger(parsed) && parsed >= 0;
}

function addSerializedStringBytes(traversal: $PayloadTraversal, value: string): void {
	if (value.length > traversal.policy.maxBytes) {
		throw new ShardingProtocolError(`${traversal.name} exceeds ${traversal.policy.maxBytes} UTF-8 bytes.`);
	}
	const serialized = JSON.stringify(value);
	addBytes(traversal, encoder.encode(serialized).byteLength);
}

function addBytes(traversal: $PayloadTraversal, count: number): void {
	if (count > traversal.policy.maxBytes - traversal.bytes) {
		throw new ShardingProtocolError(`${traversal.name} exceeds ${traversal.policy.maxBytes} UTF-8 bytes.`);
	}
	traversal.bytes += count;
}
