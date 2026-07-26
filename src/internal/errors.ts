import { ShardingError, ShardingRemoteError } from "../errors/ShardingError";

export interface SerializedError {
	readonly code: string;
	readonly message: string;
	readonly name: string;
}

const MAX_ERROR_TEXT_LENGTH = 2_048;

export function serializeError(value: unknown): SerializedError {
	if (value instanceof ShardingRemoteError) {
		return Object.freeze({
			code: sanitize(value.remoteCode, "REMOTE"),
			message: sanitize(value.message, "Remote operation failed."),
			name: sanitize(value.name, "Error"),
		});
	}
	if (value instanceof ShardingError) {
		return Object.freeze({
			code: value.code,
			message: sanitize(value.message, "Sharding operation failed."),
			name: sanitize(value.name, "Error"),
		});
	}
	if (value instanceof Error) {
		return Object.freeze({
			code: "ERROR",
			message: sanitize(value.message, "Operation failed."),
			name: sanitize(value.name, "Error"),
		});
	}
	return Object.freeze({
		code: "ERROR",
		message: "Operation failed with a non-Error value.",
		name: "Error",
	});
}

export function remoteError(error: SerializedError): ShardingRemoteError {
	const cause = new Error(error.message);
	cause.name = error.name;
	return new ShardingRemoteError(error.code, error.message, { cause });
}

function sanitize(value: string, fallback: string): string {
	if (typeof value !== "string" || value.length === 0) return fallback;
	let output = "";
	for (const character of value) {
		const code = character.charCodeAt(0);
		if (code >= 32 && code !== 127) output += character;
		if (output.length >= MAX_ERROR_TEXT_LENGTH) break;
	}
	return output.length === 0 ? fallback : output;
}
