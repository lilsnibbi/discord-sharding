import { ShardingConfigurationError, ShardingProtocolError, ShardingTransportError } from "../errors/ShardingError";
import { assertConfigurationKeys, snapshotConfigurationRecord } from "../internal/configuration";
import { MAX_SHARDS, MAX_TIMER_MS } from "../internal/limits";
import type { $GatewayBotInfo, $GatewayFetch, $GatewaySessionStartLimit } from "../types/hub";

const DEFAULT_GATEWAY_ENDPOINT = "https://discord.com/api/v10/gateway/bot";
const DEFAULT_GATEWAY_TIMEOUT_MS = 10_000;
const MAX_BOT_TOKEN_LENGTH = 4_096;
const MAX_ENDPOINT_LENGTH = 8_192;
const MAX_GATEWAY_RESPONSE_BYTES = 65_536;
const MAX_GATEWAY_RESPONSE_CHUNKS = 4_096;
const REQUEST_OPTION_KEYS = new Set(["endpoint", "fetch", "signal", "timeoutMs"]);

interface GatewayBotInfoRequestOptions {
	readonly endpoint?: string | URL;
	readonly fetch?: $GatewayFetch;
	readonly signal?: AbortSignal;
	readonly timeoutMs?: number;
}

interface ParsedGatewayBotInfoRequestOptions {
	readonly endpoint: string;
	readonly fetch: $GatewayFetch;
	readonly signal?: AbortSignal;
	readonly timeoutMs: number;
}

/**
 * Retrieves validated Discord Gateway Bot metadata through Bun's Web-standard
 * fetch implementation.
 *
 * The request and streamed response body share one deadline. Response bodies
 * are cancelled when rejected and cannot exceed 65,536 bytes.
 *
 * @param token - Raw Discord bot token without an authorization prefix.
 * @param options - Optional endpoint, fetch implementation, cancellation, and deadline.
 * @returns An immutable metadata snapshot.
 */
export async function fetchGatewayBotInfo(
	token: string,
	options: GatewayBotInfoRequestOptions = {},
): Promise<$GatewayBotInfo> {
	assertBotToken(token);
	const requestOptions = parseRequestOptions(options);
	const timeoutController = new AbortController();
	const timeoutReason = new Error(`Discord Gateway Bot request timed out after ${requestOptions.timeoutMs}ms.`);
	timeoutReason.name = "TimeoutError";
	const requestSignal =
		requestOptions.signal === undefined
			? timeoutController.signal
			: AbortSignal.any([requestOptions.signal, timeoutController.signal]);
	const requestInit: RequestInit = {
		headers: {
			Accept: "application/json",
			Authorization: `Bot ${token}`,
		},
		method: "GET",
		signal: requestSignal,
	};

	let removeAbortListener = (): void => undefined;
	const cancellation = new Promise<never>((_resolve, reject) => {
		const abort = (): void => {
			reject(createAbortError(requestSignal, timeoutReason, requestOptions.timeoutMs));
		};
		requestSignal.addEventListener("abort", abort, { once: true });
		removeAbortListener = () => requestSignal.removeEventListener("abort", abort);
		if (requestSignal.aborted) abort();
	});
	const timer = setTimeout(() => timeoutController.abort(timeoutReason), requestOptions.timeoutMs);

	const operation = requestGatewayBotInfo(
		requestOptions.fetch,
		requestOptions.endpoint,
		requestInit,
		requestSignal,
		timeoutReason,
		requestOptions.timeoutMs,
	);
	void operation.catch(() => undefined);

	try {
		return await Promise.race([operation, cancellation]);
	} finally {
		clearTimeout(timer);
		removeAbortListener();
	}
}

async function requestGatewayBotInfo(
	fetcher: $GatewayFetch,
	endpoint: string,
	init: RequestInit,
	signal: AbortSignal,
	timeoutReason: Error,
	timeoutMs: number,
): Promise<$GatewayBotInfo> {
	if (signal.aborted) throw createAbortError(signal, timeoutReason, timeoutMs);

	let response: Response;
	try {
		response = await fetcher(endpoint, init);
	} catch (cause) {
		if (signal.aborted) throw createAbortError(signal, timeoutReason, timeoutMs);
		throw new ShardingTransportError("Discord Gateway Bot request failed.", { cause });
	}
	if (!isResponse(response)) {
		throw new ShardingTransportError("Discord Gateway Bot request did not return a Response.");
	}
	if (signal.aborted) {
		const error = createAbortError(signal, timeoutReason, timeoutMs);
		void cancelResponseBody(response, error).catch(() => undefined);
		throw error;
	}

	let responseIsSuccessful: boolean;
	let status: number;
	let statusText: string;
	try {
		responseIsSuccessful = response.ok;
		status = response.status;
		statusText = response.statusText;
	} catch (cause) {
		throw new ShardingTransportError("Discord Gateway Bot response metadata could not be read.", { cause });
	}
	if (!responseIsSuccessful) {
		const suffix = statusText.length > 0 ? ` ${statusText}` : "";
		const message = `Discord Gateway Bot request returned HTTP ${status}${suffix}.`;
		const error =
			status === 401 || status === 403 ? new ShardingConfigurationError(message) : new ShardingTransportError(message);
		await cancelResponseBody(response, error);
		throw error;
	}

	const body = await readBoundedGatewayJson(response, signal);
	try {
		return parseGatewayBotInfo(body);
	} catch (cause) {
		if (cause instanceof ShardingProtocolError) throw cause;
		throw new ShardingProtocolError("Discord Gateway Bot response metadata was invalid.", { cause });
	}
}

async function readBoundedGatewayJson(response: Response, signal: AbortSignal): Promise<unknown> {
	let declaredLength: string | null;
	try {
		declaredLength = response.headers.get("content-length");
	} catch (cause) {
		throw new ShardingTransportError("Discord Gateway Bot response headers could not be read.", { cause });
	}
	if (declaredLength !== null) {
		if (!/^\d+$/u.test(declaredLength)) {
			const error = new ShardingProtocolError("Discord Gateway Bot response had an invalid Content-Length.");
			await cancelResponseBody(response, error);
			throw error;
		}
		const parsedLength = Number(declaredLength);
		if (!Number.isSafeInteger(parsedLength) || parsedLength > MAX_GATEWAY_RESPONSE_BYTES) {
			const error = new ShardingProtocolError(
				`Discord Gateway Bot response exceeded ${MAX_GATEWAY_RESPONSE_BYTES} bytes.`,
			);
			await cancelResponseBody(response, error);
			throw error;
		}
	}

	let body: ReadableStream<Uint8Array> | null;
	try {
		body = response.body;
	} catch (cause) {
		throw new ShardingTransportError("Discord Gateway Bot response body could not be read.", { cause });
	}
	if (body === null) {
		throw new ShardingProtocolError("Discord Gateway Bot response body was empty.");
	}

	let reader: ReadableStreamDefaultReader<Uint8Array>;
	try {
		reader = body.getReader();
	} catch (cause) {
		throw new ShardingTransportError("Discord Gateway Bot response body could not be opened.", { cause });
	}

	const bytes = new Uint8Array(MAX_GATEWAY_RESPONSE_BYTES);
	let chunkCount = 0;
	let totalBytes = 0;
	let removeAbortListener = (): void => undefined;
	const cancellation = new Promise<never>((_resolve, reject) => {
		const abort = (): void => {
			const error = new ShardingTransportError("Discord Gateway Bot response reading was cancelled.", {
				cause: signal.reason,
			});
			void reader.cancel(error).catch(() => undefined);
			reject(error);
		};
		signal.addEventListener("abort", abort, { once: true });
		removeAbortListener = () => signal.removeEventListener("abort", abort);
		if (signal.aborted) abort();
	});

	try {
		while (true) {
			const result = await Promise.race([reader.read(), cancellation]);
			if (result.done) break;
			chunkCount += 1;
			const chunk = result.value;
			if (!(chunk instanceof Uint8Array)) {
				const error = new ShardingProtocolError("Discord Gateway Bot response stream returned a non-byte chunk.");
				await reader.cancel(error);
				throw error;
			}
			if (chunkCount > MAX_GATEWAY_RESPONSE_CHUNKS || chunk.byteLength > MAX_GATEWAY_RESPONSE_BYTES - totalBytes) {
				const error = new ShardingProtocolError(
					`Discord Gateway Bot response exceeded ${MAX_GATEWAY_RESPONSE_BYTES} bytes.`,
				);
				try {
					await reader.cancel(error);
				} catch (cause) {
					throw new ShardingTransportError("Discord Gateway Bot response overflowed and stream cancellation failed.", {
						cause: new AggregateError([error, cause]),
					});
				}
				throw error;
			}
			bytes.set(chunk, totalBytes);
			totalBytes += chunk.byteLength;
		}
	} catch (cause) {
		if (cause instanceof ShardingProtocolError || cause instanceof ShardingTransportError) throw cause;
		throw new ShardingTransportError("Discord Gateway Bot response stream failed.", { cause });
	} finally {
		removeAbortListener();
		try {
			reader.releaseLock();
		} catch {
			// A custom stream may retain its reader after cancellation.
		}
	}

	let text: string;
	try {
		text = new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, totalBytes));
	} catch (cause) {
		throw new ShardingProtocolError("Discord Gateway Bot response was not valid UTF-8.", { cause });
	}
	try {
		return JSON.parse(text);
	} catch (cause) {
		throw new ShardingProtocolError("Discord Gateway Bot response was not valid JSON.", { cause });
	}
}

function parseRequestOptions(value: unknown): ParsedGatewayBotInfoRequestOptions {
	const snapshot = snapshotConfigurationRecord(value, "Gateway request options");
	assertConfigurationKeys(snapshot, REQUEST_OPTION_KEYS, "Gateway request options");

	const endpoint = parseEndpoint(snapshot.endpoint ?? DEFAULT_GATEWAY_ENDPOINT);
	const fetcher = snapshot.fetch ?? globalThis.fetch;
	if (!isGatewayFetch(fetcher)) {
		throw new ShardingConfigurationError("Gateway request options.fetch must be a function.");
	}
	const timeoutMs = snapshot.timeoutMs ?? DEFAULT_GATEWAY_TIMEOUT_MS;
	if (typeof timeoutMs !== "number") {
		throw new ShardingConfigurationError("Gateway request options.timeoutMs must be a number.");
	}
	assertPositiveInteger(timeoutMs, "Gateway request options.timeoutMs", MAX_TIMER_MS);

	const signal = snapshot.signal;
	if (signal !== undefined && !isAbortSignal(signal)) {
		throw new ShardingConfigurationError("Gateway request options.signal must be an AbortSignal.");
	}

	return Object.freeze({
		endpoint,
		fetch: fetcher,
		timeoutMs,
		...(signal === undefined ? {} : { signal }),
	});
}

function parseGatewayBotInfo(value: unknown): $GatewayBotInfo {
	let body: Readonly<Record<string, unknown>>;
	let limit: Readonly<Record<string, unknown>>;
	try {
		body = snapshotConfigurationRecord(value, "Gateway Bot response");
		limit = snapshotConfigurationRecord(body.session_start_limit, "Gateway Bot response.session_start_limit");
	} catch (cause) {
		throw new ShardingProtocolError("Discord Gateway Bot response must contain getter-free objects.", { cause });
	}

	const url = requireWebSocketUrl(body.url);
	const shards = requireInteger(body.shards, "shards", 1, MAX_SHARDS);
	const total = requireInteger(limit.total, "session_start_limit.total", 1, MAX_SHARDS);
	const remaining = requireInteger(limit.remaining, "session_start_limit.remaining", 0, MAX_SHARDS);
	const resetAfter = requireInteger(limit.reset_after, "session_start_limit.reset_after", 0, MAX_TIMER_MS);
	const maxConcurrency = requireInteger(limit.max_concurrency, "session_start_limit.max_concurrency", 1, MAX_SHARDS);
	if (remaining > total) {
		throw new ShardingProtocolError("session_start_limit.remaining cannot be greater than session_start_limit.total.");
	}

	const sessionStartLimit: $GatewaySessionStartLimit = Object.freeze({
		total,
		remaining,
		reset_after: resetAfter,
		max_concurrency: maxConcurrency,
	});
	return Object.freeze({
		url,
		shards,
		session_start_limit: sessionStartLimit,
	});
}

function requireInteger(value: unknown, path: string, minimum: number, maximum: number): number {
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
		const description = minimum === 0 ? "a non-negative" : "a positive";
		throw new ShardingProtocolError(`${path} must be ${description} safe integer no greater than ${maximum}.`);
	}
	return value;
}

function requireWebSocketUrl(value: unknown): string {
	if (typeof value !== "string") {
		throw new ShardingProtocolError("url must be a secure WebSocket URL.");
	}
	try {
		const url = new URL(value);
		if (url.protocol !== "wss:") {
			throw new ShardingProtocolError("url must use the wss: protocol.");
		}
	} catch (cause) {
		if (cause instanceof ShardingProtocolError) throw cause;
		throw new ShardingProtocolError("url must be a valid secure WebSocket URL.", { cause });
	}
	return value;
}

function assertBotToken(token: string): void {
	if (
		typeof token !== "string" ||
		token.length === 0 ||
		token.length > MAX_BOT_TOKEN_LENGTH ||
		token.trim() !== token ||
		/\s/u.test(token) ||
		hasAsciiControlCharacter(token)
	) {
		throw new ShardingConfigurationError(
			`Bot token must contain at most ${MAX_BOT_TOKEN_LENGTH} characters without whitespace or an authorization prefix.`,
		);
	}
}

function parseEndpoint(value: unknown): string {
	if (typeof value !== "string" && !isUrl(value)) {
		throw new ShardingConfigurationError("Gateway endpoint must be an absolute HTTPS URL.");
	}
	if (typeof value === "string" && value.length > MAX_ENDPOINT_LENGTH) {
		throw new ShardingConfigurationError(`Gateway endpoint must be at most ${MAX_ENDPOINT_LENGTH} characters.`);
	}

	let endpoint: URL;
	try {
		endpoint = new URL(value);
	} catch (cause) {
		throw new ShardingConfigurationError("Gateway endpoint must be an absolute HTTPS URL.", { cause });
	}
	if (endpoint.protocol !== "https:") {
		throw new ShardingConfigurationError("Gateway endpoint must use HTTPS.");
	}
	if (endpoint.href.length > MAX_ENDPOINT_LENGTH) {
		throw new ShardingConfigurationError(`Gateway endpoint must be at most ${MAX_ENDPOINT_LENGTH} characters.`);
	}
	return endpoint.href;
}

function assertPositiveInteger(value: number, name: string, maximum: number): void {
	if (!Number.isSafeInteger(value) || value <= 0 || value > maximum) {
		throw new ShardingConfigurationError(`${name} must be a positive integer no greater than ${maximum}.`);
	}
}

function createAbortError(signal: AbortSignal, timeoutReason: Error, timeoutMs: number): ShardingTransportError {
	if (signal.reason === timeoutReason) {
		return new ShardingTransportError(`Discord Gateway Bot request timed out after ${timeoutMs}ms.`, {
			cause: timeoutReason,
		});
	}
	return new ShardingTransportError("Discord Gateway Bot request was cancelled.", {
		...(signal.reason === undefined ? {} : { cause: signal.reason }),
	});
}

async function cancelResponseBody(response: Response, reason: Error): Promise<void> {
	try {
		const body = response.body;
		if (body === null || body.locked) return;
		await body.cancel(reason);
	} catch (cause) {
		throw new ShardingTransportError("Discord Gateway Bot response body cancellation failed.", {
			cause: new AggregateError([reason, cause]),
		});
	}
}

function hasAsciiControlCharacter(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code <= 31 || code === 127) return true;
	}
	return false;
}

function isGatewayFetch(value: unknown): value is $GatewayFetch {
	return typeof value === "function";
}

function isAbortSignal(value: unknown): value is AbortSignal {
	try {
		return value instanceof AbortSignal;
	} catch {
		return false;
	}
}

function isResponse(value: unknown): value is Response {
	try {
		return value instanceof Response;
	} catch {
		return false;
	}
}

function isUrl(value: unknown): value is URL {
	try {
		return value instanceof URL;
	} catch {
		return false;
	}
}
