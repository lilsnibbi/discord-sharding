import { RedisClient } from "bun";
import { ShardingConfigurationError, ShardingPersistenceError } from "../../errors/ShardingError";
import type {
	$AnalyticsRecord,
	$HubPersistence,
	$PersistedAssignment,
	$PersistedBridge,
	$PersistedHubState,
	$PersistedShard,
} from "../../types/hub";
import {
	decodeAssignments,
	decodeBridges,
	decodeShards,
	encodeAnalyticsRecord,
	encodeRecord,
	normalizeAssignment,
	normalizeBridge,
	normalizeShard,
	requireStorageInteger,
} from "./RedisHubRecords";
import {
	APPEND_ANALYTICS_SOURCE,
	CLEAR_ANALYTICS_SOURCE,
	type RedisCommandClient,
	RedisScript,
	SAVE_ASSIGNMENT_SOURCE,
	SAVE_BRIDGE_SOURCE,
	SAVE_SHARD_SOURCE,
} from "./scripts";

const MAX_ANALYTICS_CLEAR_BATCH_SIZE = 10_000;
const MAX_URL_LENGTH = 32_768;
const MAX_KEY_PREFIX_LENGTH = 256;
const KEY_PREFIX_PATTERN = /^[A-Za-z0-9_][A-Za-z0-9._:-]*$/u;
const SUPPORTED_PROTOCOLS = new Set(["redis:", "rediss:", "redis+tls:", "redis+unix:", "redis+tls+unix:"]);
const SCHEMA_VERSION = "1";
const CONNECTION_TIMEOUT_MS = 10_000;

interface HubKeys {
	readonly analyticsIndex: string;
	readonly analyticsRecords: string;
	readonly assignments: string;
	readonly bridges: string;
	readonly schema: string;
	readonly shards: string;
}

interface HubScripts {
	readonly appendAnalytics: RedisScript;
	readonly clearAnalytics: RedisScript;
	readonly saveAssignment: RedisScript;
	readonly saveBridge: RedisScript;
	readonly saveShard: RedisScript;
}

/**
 * Persists Hub topology and analytics with Bun's native Redis client.
 *
 * Assignment, Bridge, and shard records are stored as JSON in three hashes so
 * startup reads the whole topology in three round trips. Analytics use a sorted
 * set keyed by collection time alongside a record hash, which keeps bounded
 * oldest-first deletion cheap.
 *
 * Every conditional write runs as a server-side Lua script, so a stale epoch or
 * timestamp is rejected atomically even when several Hub instances share one
 * Redis server.
 *
 * Configure Redis with AOF persistence (`appendonly yes`) for this data. With
 * snapshot-only durability a crash can lose recent assignment writes, which
 * would let a restarted Hub load a stale ownership epoch.
 */
export class RedisHubPersistence implements $HubPersistence {
	readonly #client: RedisCommandClient;
	readonly #keys: HubKeys;
	readonly #scripts: HubScripts;
	#closed = false;
	#closePromise: Promise<void> | undefined;
	#migrationPromise: Promise<void> | undefined;

	/**
	 * Opens Redis-backed Hub persistence without connecting.
	 *
	 * @param url - Redis connection URL, for example `redis://127.0.0.1:6379`.
	 * @param keyPrefix - Namespace applied to every key owned by this Hub.
	 * @param client - Optional command client used by deterministic tests.
	 */
	public constructor(url: string, keyPrefix: string, client?: RedisCommandClient) {
		const prefix = requireKeyPrefix(keyPrefix);
		this.#keys = Object.freeze({
			analyticsIndex: `${prefix}:analytics:index`,
			analyticsRecords: `${prefix}:analytics:records`,
			assignments: `${prefix}:assignments`,
			bridges: `${prefix}:bridges`,
			schema: `${prefix}:schema`,
			shards: `${prefix}:shards`,
		});
		this.#scripts = Object.freeze({
			appendAnalytics: new RedisScript(APPEND_ANALYTICS_SOURCE),
			clearAnalytics: new RedisScript(CLEAR_ANALYTICS_SOURCE),
			saveAssignment: new RedisScript(SAVE_ASSIGNMENT_SOURCE),
			saveBridge: new RedisScript(SAVE_BRIDGE_SOURCE),
			saveShard: new RedisScript(SAVE_SHARD_SOURCE),
		});
		if (client !== undefined) {
			this.#client = client;
			return;
		}
		const validated = requireRedisUrl(url);
		try {
			this.#client = new RedisClient(validated, {
				autoReconnect: true,
				connectionTimeout: CONNECTION_TIMEOUT_MS,
				enableOfflineQueue: true,
			});
		} catch (cause) {
			throw new ShardingPersistenceError("Could not initialize Hub Redis persistence.", { cause });
		}
	}

	/** Connects, verifies the stored schema version, and caches Lua scripts. */
	public migrate(): Promise<void> {
		this.#ensureOpen();
		this.#migrationPromise ??= runPersistenceOperation("Could not migrate Hub Redis persistence.", async () => {
			const stored = await this.#client.send("SET", [this.#keys.schema, SCHEMA_VERSION, "NX", "GET"]);
			if (stored !== null && stored !== SCHEMA_VERSION) {
				throw new ShardingPersistenceError(
					`Hub Redis schema version ${String(stored)} is not supported by this release.`,
				);
			}
			for (const script of Object.values(this.#scripts)) await script.load(this.#client);
		});
		return this.#migrationPromise;
	}

	/** Loads a validated immutable Hub state snapshot. */
	public async loadState(): Promise<$PersistedHubState> {
		this.#ensureOpen();
		return runPersistenceOperation("Could not load Hub state.", async () => {
			const [assignmentReply, bridgeReply, shardReply] = await Promise.all([
				this.#client.send("HGETALL", [this.#keys.assignments]),
				this.#client.send("HGETALL", [this.#keys.bridges]),
				this.#client.send("HGETALL", [this.#keys.shards]),
			]);
			const assignments = [...decodeAssignments(assignmentReply)].sort((left, right) => left.shardId - right.shardId);
			const bridges = [...decodeBridges(bridgeReply)].sort((left, right) => left.id.localeCompare(right.id));
			const shards = [...decodeShards(shardReply)].sort((left, right) => left.shardId - right.shardId);
			return Object.freeze({
				assignments: Object.freeze(assignments),
				bridges: Object.freeze(bridges),
				shards: Object.freeze(shards),
			});
		});
	}

	/** Stores one sticky assignment unless the stored record is newer. */
	public async saveAssignment(assignment: $PersistedAssignment): Promise<void> {
		this.#ensureOpen();
		const value = normalizeAssignment(assignment);
		await runPersistenceOperation("Could not save a shard assignment.", () =>
			this.#scripts.saveAssignment.run(
				this.#client,
				[this.#keys.assignments],
				[String(value.shardId), encodeRecord(value, "Assignment"), String(value.epoch), String(value.updatedAt)],
			),
		);
	}

	/** Stores the latest Bridge status unless the stored record is newer. */
	public async saveBridge(bridge: $PersistedBridge): Promise<void> {
		this.#ensureOpen();
		const value = normalizeBridge(bridge);
		await runPersistenceOperation("Could not save Bridge state.", () =>
			this.#scripts.saveBridge.run(
				this.#client,
				[this.#keys.bridges],
				[value.id, encodeRecord(value, "Bridge"), String(value.updatedAt)],
			),
		);
	}

	/** Stores the latest shard process status unless the stored record is newer. */
	public async saveShard(shard: $PersistedShard): Promise<void> {
		this.#ensureOpen();
		const value = normalizeShard(shard);
		await runPersistenceOperation("Could not save shard state.", () =>
			this.#scripts.saveShard.run(
				this.#client,
				[this.#keys.shards],
				[String(value.shardId), encodeRecord(value, "Shard"), String(value.assignmentEpoch), String(value.updatedAt)],
			),
		);
	}

	/** Appends one global analytics sample. */
	public async appendAnalytics(record: $AnalyticsRecord): Promise<void> {
		this.#ensureOpen();
		const value = encodeAnalyticsRecord(record);
		await runPersistenceOperation("Could not append Hub analytics.", async () => {
			const written = await this.#scripts.appendAnalytics.run(
				this.#client,
				[this.#keys.analyticsIndex, this.#keys.analyticsRecords],
				[value.id, String(value.collectedAt), value.json],
			);
			if (written === 0) {
				throw new ShardingPersistenceError(`Analytics record ${value.id} already exists.`);
			}
		});
	}

	/** Deletes at most one bounded batch of the oldest analytics records. */
	public async clearAnalyticsBatch(before: number, batchSize: number): Promise<number> {
		this.#ensureOpen();
		const cutoff = requireStorageInteger(before, "before", 0, Number.MAX_SAFE_INTEGER);
		const boundedBatchSize = requireStorageInteger(batchSize, "batchSize", 1, MAX_ANALYTICS_CLEAR_BATCH_SIZE);
		return runPersistenceOperation("Could not clear Hub analytics.", async () => {
			const removed = await this.#scripts.clearAnalytics.run(
				this.#client,
				[this.#keys.analyticsIndex, this.#keys.analyticsRecords],
				[String(cutoff), String(boundedBatchSize)],
			);
			if (removed > boundedBatchSize) {
				throw new ShardingPersistenceError("Redis removed more analytics records than requested.");
			}
			return removed;
		});
	}

	/** Releases the Redis connection idempotently. */
	public close(): Promise<void> {
		if (this.#closePromise !== undefined) return this.#closePromise;
		this.#closed = true;
		this.#closePromise = runPersistenceOperation("Could not close the Hub Redis connection.", () => {
			this.#client.close();
		});
		return this.#closePromise;
	}

	#ensureOpen(): void {
		if (this.#closed) throw new ShardingPersistenceError("Hub persistence is closed.");
	}
}

function requireRedisUrl(value: unknown): string {
	if (typeof value !== "string" || value.length === 0 || value.length > MAX_URL_LENGTH) {
		throw new ShardingConfigurationError(`redisUrl must contain between 1 and ${MAX_URL_LENGTH} characters.`);
	}
	let url: URL;
	try {
		url = new URL(value);
	} catch (cause) {
		throw new ShardingConfigurationError("redisUrl must be an absolute Redis URL.", { cause });
	}
	if (!SUPPORTED_PROTOCOLS.has(url.protocol)) {
		throw new ShardingConfigurationError(
			"redisUrl must use redis:, rediss:, redis+tls:, redis+unix:, or redis+tls+unix:.",
		);
	}
	return value;
}

function requireKeyPrefix(value: unknown): string {
	if (typeof value !== "string" || value.length === 0 || value.length > MAX_KEY_PREFIX_LENGTH) {
		throw new ShardingConfigurationError(`keyPrefix must contain between 1 and ${MAX_KEY_PREFIX_LENGTH} characters.`);
	}
	if (!KEY_PREFIX_PATTERN.test(value)) {
		throw new ShardingConfigurationError("keyPrefix must start alphanumerically and use only [A-Za-z0-9._:-].");
	}
	return value;
}

async function runPersistenceOperation<T>(message: string, operation: () => T | Promise<T>): Promise<T> {
	try {
		return await operation();
	} catch (cause) {
		if (cause instanceof ShardingPersistenceError) throw cause;
		throw new ShardingPersistenceError(message, { cause });
	}
}
