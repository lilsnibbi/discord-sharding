import {
	ShardingConfigurationError,
	ShardingProtocolError,
	ShardingStateError,
	ShardingTimeoutError,
	ShardingTransportError,
} from "../errors/ShardingError";
import { assertConfigurationKeys, snapshotConfigurationRecord } from "../internal/configuration";
import { MAX_EVALUATOR_SOURCE_LENGTH, MAX_PENDING_REQUESTS, MIN_RECURRING_INTERVAL_MS } from "../internal/limits";
import { normalizePayloadPolicy } from "../internal/payload";
import { normalizeRequestPolicy } from "../internal/policies";
import {
	parseEnvironmentInteger,
	requireIdentifier,
	requirePositiveInteger,
	requireShardId,
	requireTotalShards,
} from "../internal/validation";
import { requireOptionalKeys } from "../protocol/codec";
import { readBoolean, readError } from "../protocol/readers";
import type { $PayloadPolicy, $RequestPolicy } from "../types/common";
import type { $DiscordClient } from "../types/discord";
import type { $ShardClientOptions, $ShardRequestHandler, $ShardTransport } from "../types/shard";

interface DisconnectAwareShardTransport extends $ShardTransport {
	readonly onDisconnect: (listener: () => void) => () => void;
}

export interface CapturedShardTransport {
	readonly onDisconnect?: (listener: () => void) => () => void;
	readonly onMessage: (listener: (message: unknown) => void) => () => void;
	readonly ownsProcess: boolean;
	readonly send: (message: object) => void | Promise<void>;
}

export interface CapturedDiscordClient {
	readonly destroy: () => void;
	readonly isReady: () => boolean;
	readonly login: (token?: string) => Promise<string>;
}

export interface ShardConfiguration {
	readonly analyticsIntervalMs: number | false;
	readonly assignmentEpoch: number;
	readonly bridgeId: string | null;
	readonly discordClient: CapturedDiscordClient;
	readonly id: number;
	readonly maxEvaluatorSourceLength: number;
	readonly maxPreparedEvaluations: number;
	readonly onError: ((error: Error, context: string) => void) | undefined;
	readonly onRequest: $ShardRequestHandler | undefined;
	readonly onShutdown: (() => void | Promise<void>) | undefined;
	readonly payloadPolicy: $PayloadPolicy;
	readonly processGeneration: number;
	readonly readyPollIntervalMs: number;
	readonly requestPolicy: $RequestPolicy;
	readonly totalShards: number;
	readonly transport: CapturedShardTransport;
}

const DEFAULT_ANALYTICS_INTERVAL_MS = 15_000;
const DEFAULT_READY_POLL_INTERVAL_MS = 100;
const DEFAULT_MAX_PREPARED_EVALUATIONS = 64;
const DEFAULT_MAX_EVALUATOR_SOURCE_LENGTH = 65_536;
export const MAX_SHARD_LISTENERS = 256;
const RESPONSE_REQUIRED_KEYS = new Set(["ok"]);
const EVAL_RESPONSE_SUCCESS_KEYS = new Set(["results"]);
const OPTION_KEYS = new Set([
	"analyticsIntervalMs",
	"assignmentEpoch",
	"bridgeId",
	"maxEvaluatorSourceLength",
	"maxPreparedEvaluations",
	"onError",
	"onRequest",
	"onShutdown",
	"payload",
	"processGeneration",
	"readyPollIntervalMs",
	"request",
	"shardId",
	"totalShards",
	"transport",
]);

export function createShardConfiguration(botClient: $DiscordClient, options: $ShardClientOptions): ShardConfiguration {
	if (typeof botClient !== "object" || botClient === null) {
		throw new ShardingConfigurationError("botClient must be a discord.js client object.");
	}
	const discordClient = captureDiscordClient(botClient);
	const optionSnapshot = snapshotConfigurationRecord(options, "ShardClient options");
	assertConfigurationKeys(optionSnapshot, OPTION_KEYS, "ShardClient options");
	const id = resolveIdentity(optionSnapshot.shardId, "SHARDING_SHARD_ID", 0, requireShardId);
	const totalShards = resolveIdentity(optionSnapshot.totalShards, "SHARDING_TOTAL_SHARDS", 1, requireTotalShards);
	if (id >= totalShards) throw new ShardingConfigurationError("shardId must be less than totalShards.");
	const assignmentEpoch = resolveIdentity(optionSnapshot.assignmentEpoch, "SHARDING_ASSIGNMENT_EPOCH", 1, (value) =>
		requirePositiveInteger(value, "assignmentEpoch"),
	);
	const processGeneration = resolveIdentity(
		optionSnapshot.processGeneration,
		"SHARDING_PROCESS_GENERATION",
		1,
		(value) => requirePositiveInteger(value, "processGeneration"),
	);
	const bridgeIdValue = optionSnapshot.bridgeId ?? Bun.env.SHARDING_BRIDGE_ID;
	const bridgeId = bridgeIdValue === undefined ? null : requireIdentifier(bridgeIdValue, "bridgeId");
	const transport = captureTransport(optionSnapshot.transport ?? createProcessTransport());
	const requestPolicy = normalizeRequestPolicy(readOptionalOptions<$RequestPolicy>(optionSnapshot.request, "request"));
	const payloadPolicy = normalizePayloadPolicy(readOptionalOptions<$PayloadPolicy>(optionSnapshot.payload, "payload"));
	const analyticsIntervalMs = normalizeInterval(
		optionSnapshot.analyticsIntervalMs,
		DEFAULT_ANALYTICS_INTERVAL_MS,
		"analyticsIntervalMs",
	);
	const readyPollIntervalMs = normalizeInterval(
		optionSnapshot.readyPollIntervalMs,
		DEFAULT_READY_POLL_INTERVAL_MS,
		"readyPollIntervalMs",
	);
	if (readyPollIntervalMs === false) {
		throw new ShardingConfigurationError("readyPollIntervalMs cannot be disabled.");
	}
	return Object.freeze({
		analyticsIntervalMs,
		assignmentEpoch,
		bridgeId,
		discordClient,
		id,
		maxEvaluatorSourceLength: requirePositiveInteger(
			optionSnapshot.maxEvaluatorSourceLength ?? DEFAULT_MAX_EVALUATOR_SOURCE_LENGTH,
			"maxEvaluatorSourceLength",
			MAX_EVALUATOR_SOURCE_LENGTH,
		),
		maxPreparedEvaluations: requirePositiveInteger(
			optionSnapshot.maxPreparedEvaluations ?? DEFAULT_MAX_PREPARED_EVALUATIONS,
			"maxPreparedEvaluations",
			MAX_PENDING_REQUESTS,
		),
		onError: readOptionalFunction<(error: Error, context: string) => void>(optionSnapshot.onError, "onError"),
		onRequest: readOptionalFunction<$ShardRequestHandler>(optionSnapshot.onRequest, "onRequest"),
		onShutdown: readOptionalFunction<() => void | Promise<void>>(optionSnapshot.onShutdown, "onShutdown"),
		payloadPolicy,
		processGeneration,
		readyPollIntervalMs,
		requestPolicy,
		totalShards,
		transport,
	});
}

export async function withDeadline<Value>(
	operation: Promise<Value>,
	timeoutMs: number,
	signal: AbortSignal,
	timeoutMessage: string,
): Promise<Value> {
	if (signal.aborted) throw signal.reason ?? new ShardingStateError("ShardClient operation was aborted.");
	let timer: ReturnType<typeof setTimeout> | undefined;
	let onAbort: (() => void) | undefined;
	const timeout = new Promise<never>((_resolve, reject) => {
		timer = setTimeout(() => reject(new ShardingTimeoutError(timeoutMessage)), timeoutMs);
	});
	const aborted = new Promise<never>((_resolve, reject) => {
		onAbort = (): void => reject(signal.reason ?? new ShardingStateError("ShardClient operation was aborted."));
		signal.addEventListener("abort", onAbort, { once: true });
	});
	try {
		return await Promise.race([operation, timeout, aborted]);
	} finally {
		if (timer !== undefined) clearTimeout(timer);
		if (onAbort !== undefined) signal.removeEventListener("abort", onAbort);
	}
}

export function validateResponse(
	data: Readonly<Record<string, unknown>>,
	name: string,
	successFields: ReadonlySet<string>,
): void {
	requireOptionalKeys(data, RESPONSE_REQUIRED_KEYS, new Set(["error", ...successFields]), name);
	const ok = readBoolean(data, "ok");
	if (!ok) {
		if (!Object.hasOwn(data, "error")) throw new ShardingProtocolError(`${name} is missing error.`);
		readError(data.error, `${name} error`);
		for (const field of successFields) {
			if (Object.hasOwn(data, field)) {
				throw new ShardingProtocolError(`${name} cannot include ${field} after failure.`);
			}
		}
		return;
	}
	if (Object.hasOwn(data, "error")) throw new ShardingProtocolError(`${name} cannot include error after success.`);
}

export function validateEvalResponse(data: Readonly<Record<string, unknown>>): void {
	validateResponse(data, "eval response", EVAL_RESPONSE_SUCCESS_KEYS);
	if (readBoolean(data, "ok") && !Object.hasOwn(data, "results")) {
		throw new ShardingProtocolError("Successful eval response is missing results.");
	}
}

export function copyRecord(value: object, name: string): Readonly<Record<string, unknown>> {
	const result: Record<string, unknown> = Object.create(null);
	for (const key of Object.keys(value)) {
		const descriptor = Reflect.getOwnPropertyDescriptor(value, key);
		if (descriptor === undefined || !("value" in descriptor) || descriptor.enumerable !== true) {
			throw new ShardingProtocolError(`${name}.${key} must be a getter-free data field.`);
		}
		result[key] = descriptor.value;
	}
	return Object.freeze(result);
}

export function cacheSize(manager: { readonly cache?: { readonly size: number } } | undefined): number | null {
	const size = manager?.cache?.size;
	return typeof size === "number" && Number.isSafeInteger(size) && size >= 0 ? size : null;
}

export function finiteOrNull(value: number | null | undefined): number | null {
	return typeof value === "number" && Number.isFinite(value) ? value : null;
}

export function integerOrNull(value: number | undefined): number | null {
	return typeof value === "number" && Number.isSafeInteger(value) ? value : null;
}

export function toError(value: unknown): Error {
	if (value instanceof Error) return value;
	return new Error("Operation failed with a non-Error value.", { cause: value });
}

function resolveIdentity(
	option: unknown,
	environmentName: string,
	minimum: number,
	validate: (value: unknown) => number,
): number {
	const value = option === undefined ? parseEnvironmentInteger(environmentName, undefined, minimum) : option;
	return validate(value);
}

function normalizeInterval(value: unknown, fallback: number, name: string): number | false {
	if (value === false) return false;
	if (value !== undefined && typeof value !== "number") {
		throw new ShardingConfigurationError(`${name} must be a number or false.`);
	}
	const interval = value ?? fallback;
	requirePositiveInteger(interval, name);
	if (interval < MIN_RECURRING_INTERVAL_MS) {
		throw new ShardingConfigurationError(`${name} must be at least ${MIN_RECURRING_INTERVAL_MS} milliseconds.`);
	}
	return interval;
}

function captureDiscordClient(client: object): CapturedDiscordClient {
	const destroy = readObjectMember(client, "destroy", "botClient");
	const isReady = readObjectMember(client, "isReady", "botClient");
	const login = readObjectMember(client, "login", "botClient");
	if (typeof destroy !== "function" || typeof isReady !== "function" || typeof login !== "function") {
		throw new ShardingConfigurationError("botClient must provide login(), destroy(), and isReady().");
	}
	return Object.freeze({
		destroy(): void {
			let result: unknown;
			try {
				result = Reflect.apply(destroy, client, []);
			} catch (cause) {
				throw new ShardingTransportError("discord.js destroy() failed.", { cause });
			}
			if (result !== undefined) {
				throw new ShardingTransportError("discord.js destroy() returned an unsupported value.");
			}
		},
		isReady(): boolean {
			let result: unknown;
			try {
				result = Reflect.apply(isReady, client, []);
			} catch (cause) {
				throw new ShardingTransportError("discord.js isReady() failed.", { cause });
			}
			if (typeof result !== "boolean") {
				throw new ShardingTransportError("discord.js isReady() must return a boolean.");
			}
			return result;
		},
		login(token?: string): Promise<string> {
			let result: unknown;
			try {
				result = Reflect.apply(login, client, token === undefined ? [] : [token]);
			} catch (cause) {
				throw new ShardingTransportError("discord.js login() failed.", { cause });
			}
			if (!(result instanceof Promise)) {
				throw new ShardingTransportError("discord.js login() must return a Promise.");
			}
			return result.then(
				(value: unknown) => {
					if (typeof value !== "string") {
						throw new ShardingTransportError("discord.js login() must resolve to a token string.");
					}
					return value;
				},
				(cause: unknown) => {
					throw new ShardingTransportError("discord.js login() failed.", { cause });
				},
			);
		},
	});
}

function captureTransport(transport: unknown): CapturedShardTransport {
	if (typeof transport !== "object" || transport === null) {
		throw new ShardingConfigurationError("transport must be an object.");
	}
	const send = readObjectMember(transport, "send", "transport");
	const onMessage = readObjectMember(transport, "onMessage", "transport");
	const onDisconnect = readObjectMember(transport, "onDisconnect", "transport");
	const ownsProcess = readObjectMember(transport, "ownsProcess", "transport");
	if (typeof send !== "function" || typeof onMessage !== "function") {
		throw new ShardingConfigurationError("transport must provide send() and onMessage().");
	}
	if (onDisconnect !== undefined && typeof onDisconnect !== "function") {
		throw new ShardingConfigurationError("transport.onDisconnect must be a function when supplied.");
	}
	if (ownsProcess !== undefined && typeof ownsProcess !== "boolean") {
		throw new ShardingConfigurationError("transport.ownsProcess must be a boolean when supplied.");
	}
	const captured: CapturedShardTransport = {
		onMessage(listener): () => void {
			let active = true;
			const guardedListener = (message: unknown): void => {
				if (active) listener(message);
			};
			let cleanup: unknown;
			try {
				cleanup = Reflect.apply(onMessage, transport, [guardedListener]);
			} catch (cause) {
				active = false;
				throw new ShardingTransportError("Could not register the shard IPC listener.", { cause });
			}
			if (typeof cleanup !== "function") {
				active = false;
				throw new ShardingConfigurationError("transport.onMessage() must return a cleanup function.");
			}
			return (): void => {
				if (!active) return;
				active = false;
				Reflect.apply(cleanup, transport, []);
			};
		},
		ownsProcess: ownsProcess === true,
		send(message): void | Promise<void> {
			let result: unknown;
			try {
				result = Reflect.apply(send, transport, [message]);
			} catch (cause) {
				throw new ShardingTransportError("Could not send shard IPC.", { cause });
			}
			if (result === undefined) return;
			if (result instanceof Promise) {
				return result.then(
					() => undefined,
					(cause: unknown) => {
						throw new ShardingTransportError("Could not send shard IPC.", { cause });
					},
				);
			}
			throw new ShardingTransportError("transport.send() returned an unsupported value.");
		},
	};
	const capturedOnDisconnect: CapturedShardTransport["onDisconnect"] =
		typeof onDisconnect === "function"
			? (listener: () => void): (() => void) => {
					let active = true;
					const guardedListener = (): void => {
						if (active) listener();
					};
					let cleanup: unknown;
					try {
						cleanup = Reflect.apply(onDisconnect, transport, [guardedListener]);
					} catch (cause) {
						active = false;
						throw new ShardingTransportError("Could not register the shard IPC disconnect listener.", { cause });
					}
					if (typeof cleanup !== "function") {
						active = false;
						throw new ShardingConfigurationError("transport.onDisconnect() must return a cleanup function.");
					}
					return (): void => {
						if (!active) return;
						active = false;
						Reflect.apply(cleanup, transport, []);
					};
				}
			: undefined;
	return Object.freeze({
		...captured,
		...(capturedOnDisconnect === undefined ? {} : { onDisconnect: capturedOnDisconnect }),
	});
}

function readObjectMember(value: object, key: string, name: string): unknown {
	try {
		let owner: object | null = value;
		while (owner !== null) {
			const descriptor = Reflect.getOwnPropertyDescriptor(owner, key);
			if (descriptor !== undefined) {
				if (!("value" in descriptor)) {
					throw new ShardingConfigurationError(`${name}.${key} must be a data property.`);
				}
				return descriptor.value;
			}
			owner = Reflect.getPrototypeOf(owner);
		}
		return undefined;
	} catch (cause) {
		if (cause instanceof ShardingConfigurationError) throw cause;
		throw new ShardingConfigurationError(`${name}.${key} could not be inspected.`, { cause });
	}
}

function createProcessTransport(): DisconnectAwareShardTransport {
	if (typeof process.send !== "function") {
		throw new ShardingConfigurationError("ShardClient requires Bun IPC or a custom transport.");
	}
	const send = process.send.bind(process);
	return Object.freeze({
		ownsProcess: true,
		onMessage(listener: (message: unknown) => void): () => void {
			const handler = (message: unknown): void => listener(message);
			process.on("message", handler);
			let active = true;
			return (): void => {
				if (!active) return;
				active = false;
				process.off("message", handler);
			};
		},
		onDisconnect(listener: () => void): () => void {
			process.on("disconnect", listener);
			let active = true;
			return (): void => {
				if (!active) return;
				active = false;
				process.off("disconnect", listener);
			};
		},
		send(message: object): void {
			send(message);
		},
	});
}

function readOptionalOptions<Type extends object>(value: unknown, name: string): Partial<Type> | undefined {
	return value === undefined ? undefined : (snapshotConfigurationRecord(value, name) as Partial<Type>);
}

function readOptionalFunction<FunctionType>(value: unknown, name: string): FunctionType | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "function") throw new ShardingConfigurationError(`${name} must be a function.`);
	return value as FunctionType;
}
