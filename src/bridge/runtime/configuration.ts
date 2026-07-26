import { ShardingConfigurationError, ShardingTransportError } from "../../errors/ShardingError";
import {
	assertConfigurationKeys,
	snapshotConfigurationArray,
	snapshotConfigurationRecord,
} from "../../internal/configuration";
import {
	MAX_ARGUMENT_COUNT,
	MAX_ARGUMENT_LENGTH,
	MAX_BUFFERED_BYTES,
	MAX_ENVIRONMENT_ENTRIES,
	MAX_ENVIRONMENT_VALUE_LENGTH,
	MAX_SHARDS,
	MAX_TIMER_MS,
} from "../../internal/limits";
import { normalizePayloadPolicy } from "../../internal/payload";
import { normalizeReconnectPolicy, normalizeRequestPolicy, normalizeRestartPolicy } from "../../internal/policies";
import { abortableSleep } from "../../internal/sleep";
import {
	requireBoundedString,
	requireIdentifier,
	requirePositiveInteger,
	requireToken,
} from "../../internal/validation";
import type { $BridgeClientOptions, $BridgeSocketFactory, $ShardProcessFactory } from "../../types/bridge";
import type { $PayloadPolicy, $Sleep } from "../../types/common";
import { createBunProcessFactory } from "../shards/ManagedShardProcess";
import type { $NormalizedBridgeOptions } from "./types";

const DEFAULT_ANALYTICS_PATH = "./sharding-bridge.sqlite";
const DEFAULT_STARTUP_TIMEOUT_MS = 30_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;
const DEFAULT_MAX_BUFFERED_BYTES = 4_194_304;
const OPTION_KEYS = new Set([
	"analyticsPath",
	"args",
	"cwd",
	"env",
	"hubUrl",
	"id",
	"maxBufferedBytes",
	"maxShards",
	"onError",
	"payload",
	"processFactory",
	"random",
	"reconnect",
	"request",
	"restart",
	"shardScript",
	"shutdownTimeoutMs",
	"sleep",
	"socketFactory",
	"startupTimeoutMs",
	"token",
]);

export function normalizeBridgeOptions(input: $BridgeClientOptions): $NormalizedBridgeOptions {
	const options = snapshotConfigurationRecord(input, "BridgeClient options");
	assertConfigurationKeys(options, OPTION_KEYS, "BridgeClient options");
	const id = requireIdentifier(options.id, "id");
	const token = requireToken(options.token, "token");
	const maxShards = requirePositiveInteger(options.maxShards, "maxShards", MAX_SHARDS);
	const shardScript = requireProcessText(
		requireBoundedString(options.shardScript, "shardScript", MAX_ENVIRONMENT_VALUE_LENGTH),
		"shardScript",
	);
	const args = normalizeArguments(options.args);
	const cwdValue = normalizeOptionalString(options.cwd, "cwd", MAX_ENVIRONMENT_VALUE_LENGTH);
	const cwd = cwdValue === undefined ? undefined : requireProcessText(cwdValue, "cwd");
	const environment = normalizeEnvironment(options.env);
	const analyticsPath =
		normalizeOptionalString(options.analyticsPath, "analyticsPath", MAX_ENVIRONMENT_VALUE_LENGTH) ??
		DEFAULT_ANALYTICS_PATH;
	const request = normalizeRequestPolicy(readPartialObject(options.request, "request"));
	const payload = normalizePayloadPolicy(
		readPartialObject(options.payload, "payload") as Partial<$PayloadPolicy> | undefined,
	);
	const reconnect = normalizeReconnectPolicy(readPartialObject(options.reconnect, "reconnect"));
	const restart = normalizeRestartPolicy(readPartialObject(options.restart, "restart"));
	const startupTimeoutMs = requirePositiveInteger(
		options.startupTimeoutMs ?? DEFAULT_STARTUP_TIMEOUT_MS,
		"startupTimeoutMs",
		MAX_TIMER_MS,
	);
	const shutdownTimeoutMs = requirePositiveInteger(
		options.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS,
		"shutdownTimeoutMs",
		MAX_TIMER_MS,
	);
	const maxBufferedBytes = requirePositiveInteger(
		options.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES,
		"maxBufferedBytes",
		MAX_BUFFERED_BYTES,
	);
	const processFactory =
		options.processFactory === undefined
			? createBunProcessFactory()
			: requireFunction<$ShardProcessFactory>(options.processFactory, "processFactory");
	const socketFactory =
		options.socketFactory === undefined
			? defaultSocketFactory
			: requireFunction<$BridgeSocketFactory>(options.socketFactory, "socketFactory");
	const sleep = options.sleep === undefined ? abortableSleep : requireFunction<$Sleep>(options.sleep, "sleep");
	const random = options.random === undefined ? Math.random : requireFunction<() => number>(options.random, "random");
	const onError =
		options.onError === undefined
			? undefined
			: requireFunction<(error: Error, context: string) => void>(options.onError, "onError");
	return Object.freeze({
		analyticsPath,
		args,
		...(cwd === undefined ? {} : { cwd }),
		environment,
		hubUrl: normalizeHubUrl(options.hubUrl),
		id,
		maxBufferedBytes,
		maxShards,
		...(onError === undefined ? {} : { onError }),
		payload,
		processFactory,
		random,
		reconnect,
		request,
		restart,
		shardScript,
		shutdownTimeoutMs,
		sleep,
		socketFactory,
		startupTimeoutMs,
		token,
	});
}

function normalizeHubUrl(value: unknown): string {
	if (typeof value !== "string" && !(value instanceof URL)) {
		throw new ShardingConfigurationError("hubUrl must be an absolute HTTP or HTTPS URL.");
	}
	let url: URL;
	try {
		url = new URL(value);
	} catch (cause) {
		throw new ShardingConfigurationError("hubUrl must be an absolute HTTP or HTTPS URL.", { cause });
	}
	if (url.protocol !== "http:" && url.protocol !== "https:") {
		throw new ShardingConfigurationError("hubUrl must use HTTP or HTTPS.");
	}
	url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
	url.pathname = "/bridge";
	url.search = "";
	url.hash = "";
	return url.href;
}

function defaultSocketFactory(url: string, headers: Readonly<Record<string, string>>): WebSocket {
	const socket: unknown = Reflect.construct(WebSocket, [url, { headers }]);
	if (!(socket instanceof WebSocket)) {
		throw new ShardingTransportError("WebSocket constructor returned an invalid client.");
	}
	return socket;
}

function normalizeArguments(value: unknown): readonly string[] {
	if (value === undefined) return Object.freeze([]);
	const array = snapshotConfigurationArray(value, "args", MAX_ARGUMENT_COUNT);
	return Object.freeze(
		array.map((argument, index) =>
			requireProcessText(requireBoundedString(argument, `args[${index}]`, MAX_ARGUMENT_LENGTH), `args[${index}]`),
		),
	);
}

function normalizeEnvironment(value: unknown): Readonly<Record<string, string | undefined>> {
	if (value === undefined) return Object.freeze({});
	const record = snapshotConfigurationRecord(value, "env", MAX_ENVIRONMENT_ENTRIES);
	const output: Record<string, string | undefined> = Object.create(null);
	for (const [key, entry] of Object.entries(record)) {
		requireEnvironmentKey(requireBoundedString(key, "Environment key", MAX_ARGUMENT_LENGTH));
		if (entry !== undefined && (typeof entry !== "string" || entry.length > MAX_ENVIRONMENT_VALUE_LENGTH)) {
			throw new ShardingConfigurationError(
				`Environment value ${key} must be undefined or at most ${MAX_ENVIRONMENT_VALUE_LENGTH} characters.`,
			);
		}
		if (entry?.includes("\0") === true) {
			throw new ShardingConfigurationError(`Environment value ${key} cannot contain NUL.`);
		}
		output[key] = entry;
	}
	return Object.freeze(output);
}

export function buildEnvironment(
	overrides: Readonly<Record<string, string | undefined>>,
	shardId: number,
	totalShards: number,
	assignmentEpoch: number,
	processGeneration: number,
): Readonly<Record<string, string>> {
	const environment: Record<string, string> = Object.create(null);
	for (const [key, value] of Object.entries(Bun.env)) {
		if (value !== undefined) {
			requireEnvironmentKey(key);
			if (value.length > MAX_ENVIRONMENT_VALUE_LENGTH) {
				throw new ShardingConfigurationError(
					`Environment value ${key} cannot exceed ${MAX_ENVIRONMENT_VALUE_LENGTH} characters.`,
				);
			}
			if (value.includes("\0")) throw new ShardingConfigurationError(`Environment value ${key} cannot contain NUL.`);
			environment[key] = value;
		}
	}
	for (const [key, value] of Object.entries(overrides)) {
		if (value === undefined) delete environment[key];
		else environment[key] = value;
	}
	environment.SHARDING_SHARD_ID = String(shardId);
	environment.SHARDING_TOTAL_SHARDS = String(totalShards);
	environment.SHARDING_ASSIGNMENT_EPOCH = String(assignmentEpoch);
	environment.SHARDING_PROCESS_GENERATION = String(processGeneration);
	if (Object.keys(environment).length > MAX_ENVIRONMENT_ENTRIES) {
		throw new ShardingConfigurationError(`Child environment cannot exceed ${MAX_ENVIRONMENT_ENTRIES} entries.`);
	}
	return Object.freeze(environment);
}

function requireProcessText(value: string, name: string): string {
	if (containsControlCharacter(value)) {
		throw new ShardingConfigurationError(`${name} cannot contain control characters.`);
	}
	return value;
}

function requireEnvironmentKey(value: string): string {
	if (
		value.length === 0 ||
		value.length > MAX_ARGUMENT_LENGTH ||
		value.includes("=") ||
		containsControlCharacter(value)
	) {
		throw new ShardingConfigurationError("Environment keys cannot be empty or contain control characters or '='.");
	}
	return value;
}

function containsControlCharacter(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code <= 31 || code === 127) return true;
	}
	return false;
}

function readPartialObject(value: unknown, name: string): Readonly<Record<string, unknown>> | undefined {
	return value === undefined ? undefined : snapshotConfigurationRecord(value, name);
}

function normalizeOptionalString(value: unknown, name: string, maximum: number): string | undefined {
	return value === undefined ? undefined : requireBoundedString(value, name, maximum);
}

function requireFunction<FunctionType>(value: unknown, name: string): FunctionType {
	if (typeof value !== "function") throw new ShardingConfigurationError(`${name} must be a function.`);
	return value as FunctionType;
}
