import { ShardingConfigurationError } from "../../errors/ShardingError";
import { assertConfigurationKeys, snapshotConfigurationRecord } from "../../internal/configuration";
import { MAX_BUFFERED_BYTES, MAX_PENDING_REQUESTS, MAX_QUEUED_MESSAGES, MAX_TIMER_MS } from "../../internal/limits";
import { normalizePayloadPolicy } from "../../internal/payload";
import { normalizeRequestPolicy } from "../../internal/policies";
import { abortableSleep } from "../../internal/sleep";
import {
	requireBoundedString,
	requireNonNegativeInteger,
	requirePositiveInteger,
	requireToken,
	requireTotalShards,
} from "../../internal/validation";
import type { $Sleep } from "../../types/common";
import type { $GatewayFetch, $HubClientOptions } from "../../types/hub";
import {
	DEFAULT_DATABASE_PATH,
	DEFAULT_EVALUATION_COMMIT_LEAD_MS,
	DEFAULT_GATEWAY_ENDPOINT,
	DEFAULT_HOSTNAME,
	DEFAULT_MAX_BUFFERED_BYTES,
	DEFAULT_MAX_EVALUATIONS,
	DEFAULT_MAX_QUEUED_MESSAGES,
	DEFAULT_PORT,
	OPTION_KEYS,
} from "./constants";
import { normalizeHubPersistence } from "./persistenceAdapter";
import type { NormalizedHubOptions } from "./types";
import { normalizeGatewayEndpoint } from "./utilities";

export function normalizeOptions(input: $HubClientOptions): NormalizedHubOptions {
	const options = snapshotConfigurationRecord(input, "HubClient options");
	assertConfigurationKeys(options, OPTION_KEYS, "HubClient options");
	const botToken = requireToken(options.botToken, "botToken");
	const bridgeToken = requireToken(options.bridgeToken, "bridgeToken");
	const adminToken = requireToken(options.adminToken, "adminToken");
	if (bridgeToken === adminToken) {
		throw new ShardingConfigurationError("bridgeToken and adminToken must be different secrets.");
	}
	const persistence = normalizeHubPersistence(options.persistence);
	const databasePath =
		options.databasePath === undefined
			? DEFAULT_DATABASE_PATH
			: requireBoundedString(options.databasePath, "databasePath", 32_768);
	if (persistence !== undefined && options.databasePath !== undefined) {
		throw new ShardingConfigurationError("Provide persistence or databasePath, not both.");
	}
	const totalShards =
		options.totalShards === undefined ? undefined : requireTotalShards(options.totalShards, "totalShards");
	const hostname =
		options.hostname === undefined ? DEFAULT_HOSTNAME : requireBoundedString(options.hostname, "hostname", 253);
	const port = options.port === undefined ? DEFAULT_PORT : requireNonNegativeInteger(options.port, "port", 65_535);
	const gatewayEndpoint = normalizeGatewayEndpoint(options.gatewayEndpoint ?? DEFAULT_GATEWAY_ENDPOINT);
	const fetcher = options.fetch ?? globalThis.fetch;
	if (!isGatewayFetch(fetcher)) throw new ShardingConfigurationError("fetch must be a function.");
	const request = normalizeRequestPolicy(
		options.request === undefined ? undefined : snapshotConfigurationRecord(options.request, "request"),
	);
	const payload = normalizePayloadPolicy(
		options.payload === undefined ? undefined : snapshotConfigurationRecord(options.payload, "payload"),
	);
	const maxBufferedBytes = requirePositiveInteger(
		options.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES,
		"maxBufferedBytes",
		MAX_BUFFERED_BYTES,
	);
	const maxQueuedMessages = requirePositiveInteger(
		options.maxQueuedMessages ?? DEFAULT_MAX_QUEUED_MESSAGES,
		"maxQueuedMessages",
		MAX_QUEUED_MESSAGES,
	);
	const maxEvaluations = requirePositiveInteger(
		options.maxEvaluations ?? DEFAULT_MAX_EVALUATIONS,
		"maxEvaluations",
		MAX_PENDING_REQUESTS,
	);
	const evaluationCommitLeadMs = requireNonNegativeInteger(
		options.evaluationCommitLeadMs ?? DEFAULT_EVALUATION_COMMIT_LEAD_MS,
		"evaluationCommitLeadMs",
		MAX_TIMER_MS,
	);
	const now = options.now ?? performance.now.bind(performance);
	if (!isClock(now)) throw new ShardingConfigurationError("now must be a function.");
	const wallClock = options.wallClock ?? Date.now;
	if (!isClock(wallClock)) throw new ShardingConfigurationError("wallClock must be a function.");
	const sleep = options.sleep ?? abortableSleep;
	if (!isSleep(sleep)) throw new ShardingConfigurationError("sleep must be a function.");
	const onError = options.onError;
	if (onError !== undefined && !isErrorListener(onError)) {
		throw new ShardingConfigurationError("onError must be a function.");
	}
	return Object.freeze({
		adminToken,
		botToken,
		bridgeToken,
		databasePath,
		evaluationCommitLeadMs,
		fetch: fetcher,
		gatewayEndpoint,
		hostname,
		maxBufferedBytes,
		maxEvaluations,
		maxQueuedMessages,
		now,
		...(onError === undefined ? {} : { onError }),
		payload,
		...(persistence === undefined ? {} : { persistence }),
		port,
		request,
		sleep,
		...(totalShards === undefined ? {} : { totalShards }),
		wallClock,
	});
}

function isGatewayFetch(value: unknown): value is $GatewayFetch {
	return typeof value === "function";
}

function isClock(value: unknown): value is () => number {
	return typeof value === "function";
}

function isSleep(value: unknown): value is $Sleep {
	return typeof value === "function";
}

function isErrorListener(value: unknown): value is (error: Error, context: string) => void {
	return typeof value === "function";
}
