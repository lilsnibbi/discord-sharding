import { ShardingProtocolError } from "../errors/ShardingError";
import { snapshotConfigurationRecord } from "../internal/configuration";
import { normalizePayload } from "../internal/payload";
import { requireProtocolIdentifier } from "../internal/validation";
import type { $PayloadPolicy } from "../types/common";
import {
	ALL_WIRE_TYPES,
	type ParsedWireMessage,
	PROTOCOL_VERSION,
	type WireDataMap,
	type WireMessage,
	type WireMessageType,
} from "./types";

const MESSAGE_KEYS = new Set(["data", "id", "type", "version"]);
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

export function createWireMessage<Type extends WireMessageType>(
	type: Type,
	id: string,
	data: WireDataMap[Type],
	policy: $PayloadPolicy,
): WireMessage<Type> {
	if (!ALL_WIRE_TYPES.has(type)) throw new ShardingProtocolError(`Unsupported protocol message type ${type}.`);
	requireProtocolIdentifier(id, "Protocol message ID");
	const normalizedData = normalizePayload(data, policy, `${type} data`) as WireDataMap[Type];
	const message: WireMessage<Type> = Object.freeze({
		data: normalizedData,
		id,
		type,
		version: PROTOCOL_VERSION,
	});
	return normalizePayload(message, policy, "Protocol message") as WireMessage<Type>;
}

export function encodeWireMessage(message: object, policy: $PayloadPolicy): string {
	const normalized = normalizePayload(message, policy, "Protocol message");
	const encoded = JSON.stringify(normalized);
	if (encoded === undefined) throw new ShardingProtocolError("Protocol message could not be encoded.");
	return encoded;
}

export function parseWireMessage(
	value: unknown,
	allowedTypes: ReadonlySet<WireMessageType>,
	policy: $PayloadPolicy,
): ParsedWireMessage {
	try {
		const decoded = decodeInput(value, policy.maxBytes);
		const normalized = normalizePayload(decoded, policy, "Protocol message");
		const message = snapshotConfigurationRecord(normalized, "Protocol message", MESSAGE_KEYS.size);
		requireExactKeys(message, MESSAGE_KEYS, "Protocol message");
		if (message.version !== PROTOCOL_VERSION) {
			throw new ShardingProtocolError(`Unsupported protocol version ${String(message.version)}.`);
		}
		const id = requireProtocolIdentifier(message.id, "Protocol message ID");
		const type = requireMessageType(message.type, allowedTypes);
		const data = snapshotConfigurationRecord(message.data, `${type} data`, policy.maxNodes);
		return Object.freeze({ data, id, type, version: PROTOCOL_VERSION });
	} catch (cause) {
		if (cause instanceof ShardingProtocolError) throw cause;
		throw new ShardingProtocolError("Protocol message failed structural validation.", { cause });
	}
}

export function requireData(
	message: ParsedWireMessage,
	expectedType: WireMessageType,
	keys: ReadonlySet<string>,
): Readonly<Record<string, unknown>> {
	if (message.type !== expectedType) {
		throw new ShardingProtocolError(`Expected ${expectedType}, received ${message.type}.`);
	}
	requireExactKeys(message.data, keys, `${expectedType} data`);
	return message.data;
}

export function requireExactKeys(
	value: Readonly<Record<string, unknown>>,
	keys: ReadonlySet<string>,
	name: string,
): void {
	const actual = Object.keys(value);
	if (actual.length !== keys.size) throw new ShardingProtocolError(`${name} contains unexpected or missing fields.`);
	for (const key of actual) {
		if (!keys.has(key)) throw new ShardingProtocolError(`${name} contains unexpected field "${key}".`);
	}
}

export function requireOptionalKeys(
	value: Readonly<Record<string, unknown>>,
	required: ReadonlySet<string>,
	optional: ReadonlySet<string>,
	name: string,
): void {
	for (const key of required) {
		if (!Object.hasOwn(value, key)) throw new ShardingProtocolError(`${name} is missing field "${key}".`);
	}
	const maximum = required.size + optional.size;
	const actual = Object.keys(value);
	if (actual.length < required.size || actual.length > maximum) {
		throw new ShardingProtocolError(`${name} contains unexpected or missing fields.`);
	}
	for (const key of actual) {
		if (!required.has(key) && !optional.has(key)) {
			throw new ShardingProtocolError(`${name} contains unexpected field "${key}".`);
		}
	}
}

function decodeInput(value: unknown, maximumBytes: number): unknown {
	if (typeof value === "string") {
		if (textEncoder.encode(value).byteLength > maximumBytes) {
			throw new ShardingProtocolError(`Protocol message exceeds ${maximumBytes} UTF-8 bytes.`);
		}
		return parseJson(value);
	}
	if (value instanceof Uint8Array) {
		if (value.byteLength > maximumBytes) {
			throw new ShardingProtocolError(`Protocol message exceeds ${maximumBytes} UTF-8 bytes.`);
		}
		let text: string;
		try {
			text = textDecoder.decode(value);
		} catch (cause) {
			throw new ShardingProtocolError("Protocol message is not valid UTF-8.", { cause });
		}
		return parseJson(text);
	}
	return value;
}

function parseJson(value: string): unknown {
	try {
		return JSON.parse(value);
	} catch (cause) {
		throw new ShardingProtocolError("Protocol message is not valid JSON.", { cause });
	}
}

function requireMessageType(value: unknown, allowed: ReadonlySet<WireMessageType>): WireMessageType {
	if (typeof value !== "string" || !ALL_WIRE_TYPES.has(value as WireMessageType)) {
		throw new ShardingProtocolError("Protocol message type is not supported.");
	}
	const type = value as WireMessageType;
	if (!allowed.has(type))
		throw new ShardingProtocolError(`Protocol message type ${type} is not valid on this channel.`);
	return type;
}
