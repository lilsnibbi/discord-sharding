import { Database, type Statement } from "bun:sqlite";
import { ShardingPersistenceError } from "../../errors/ShardingError";
import { assertConfigurationKeys, snapshotConfigurationRecord } from "../../internal/configuration";
import { MAX_SHARDS } from "../../internal/limits";
import { DEFAULT_PAYLOAD_POLICY, normalizePayload } from "../../internal/payload";
import { requireIdentifier, requireShardId } from "../../internal/validation";
import type { $BridgeAnalytics, $BridgeAnalyticsQuery } from "../../types/bridge";
import type { $JsonValue } from "../../types/common";
import type { $AnalyticsRecord } from "../../types/hub";

const DEFAULT_READ_LIMIT = 100;
const MAX_READ_LIMIT = 10_000;
const MAX_CLEAR_BATCH_SIZE = 10_000;
const ANALYTICS_QUERY_KEYS = new Set(["limit", "shardId"]);
const ANALYTICS_RECORD_KEYS = new Set(["id", "bridgeId", "shardId", "collectedAt", "data"]);
const ANALYTICS_ROW_KEYS = new Set(["id", "bridge_id", "shard_id", "collected_at", "data_json"]);

type AppendStatement = Statement<unknown, [string, string, number | null, number, string]>;
type ReadStatement = Statement<unknown, [number]>;
type ReadShardStatement = Statement<unknown, [number, number]>;
type ClearStatement = Statement<unknown, [number, number]>;

interface BridgeAnalyticsResources {
	readonly append: AppendStatement;
	readonly clear: ClearStatement;
	readonly database: Database;
	readonly read: ReadStatement;
	readonly readShard: ReadShardStatement;
}

interface NormalizedAnalyticsRecord {
	readonly collectedAt: number;
	readonly dataJson: string;
	readonly bridgeId: string;
	readonly id: string;
	readonly shardId: number | null;
}

/**
 * Stores Bridge analytics in a local Bun SQLite database.
 *
 * Samples remain available until {@link clear} removes them explicitly.
 */
export class BridgeAnalyticsStore implements $BridgeAnalytics {
	readonly #appendStatement: AppendStatement;
	readonly #clearStatement: ClearStatement;
	readonly #database: Database;
	readonly #readShardStatement: ReadShardStatement;
	readonly #readStatement: ReadStatement;
	#closed = false;
	#closePromise: Promise<void> | undefined;

	/**
	 * Opens or creates a local analytics database.
	 *
	 * @param path - SQLite file path, or `":memory:"` for an isolated in-memory store.
	 */
	public constructor(path = "./sharding-bridge.sqlite") {
		const resources = createResources(path);
		this.#appendStatement = resources.append;
		this.#clearStatement = resources.clear;
		this.#database = resources.database;
		this.#readShardStatement = resources.readShard;
		this.#readStatement = resources.read;
	}

	/**
	 * Appends one analytics sample.
	 *
	 * Existing samples are never removed automatically.
	 *
	 * @param record - Analytics sample to validate and persist.
	 */
	public async append(record: $AnalyticsRecord): Promise<void> {
		this.#ensureOpen();
		const normalized = normalizeAnalyticsRecord(record, "Analytics record");
		try {
			this.#appendStatement.run(
				normalized.id,
				normalized.bridgeId,
				normalized.shardId,
				normalized.collectedAt,
				normalized.dataJson,
			);
		} catch (cause) {
			throw new ShardingPersistenceError("Could not append Bridge analytics.", { cause });
		}
	}

	/**
	 * Reads analytics samples in newest-first order.
	 *
	 * @param query - Optional result limit and shard filter.
	 * @returns Immutable validated analytics records.
	 */
	public async read(query?: $BridgeAnalyticsQuery): Promise<readonly $AnalyticsRecord[]> {
		this.#ensureOpen();
		const normalized = normalizeQuery(query);
		try {
			const rows =
				normalized.shardId === undefined
					? this.#readStatement.all(normalized.limit)
					: this.#readShardStatement.all(normalized.shardId, normalized.limit);
			return mapAnalyticsRows(rows, "Bridge analytics query");
		} catch (cause) {
			if (cause instanceof ShardingPersistenceError) throw cause;
			throw new ShardingPersistenceError("Could not read Bridge analytics.", { cause });
		}
	}

	/**
	 * Deletes at most one bounded batch of analytics samples.
	 *
	 * @param before - Inclusive collection-time cutoff in Unix milliseconds.
	 * @param batchSize - Maximum rows removed by this call.
	 * @returns Number of deleted rows.
	 */
	public async clear(before: number, batchSize: number): Promise<number> {
		this.#ensureOpen();
		const cutoff = requireSafeNonNegativeInteger(before, "before");
		const boundedBatchSize = requirePositiveInteger(batchSize, "batchSize", MAX_CLEAR_BATCH_SIZE);
		try {
			return this.#clearStatement.run(cutoff, boundedBatchSize).changes;
		} catch (cause) {
			throw new ShardingPersistenceError("Could not clear Bridge analytics.", { cause });
		}
	}

	/**
	 * Finalizes prepared statements and closes the SQLite connection.
	 *
	 * Repeated calls return the same completion promise.
	 */
	public close(): Promise<void> {
		if (this.#closePromise !== undefined) return this.#closePromise;
		this.#closed = true;
		this.#closePromise = this.#closeResources();
		return this.#closePromise;
	}

	async #closeResources(): Promise<void> {
		let firstFailure: unknown;
		firstFailure = finalize(this.#appendStatement, firstFailure);
		firstFailure = finalize(this.#readStatement, firstFailure);
		firstFailure = finalize(this.#readShardStatement, firstFailure);
		firstFailure = finalize(this.#clearStatement, firstFailure);
		try {
			this.#database.close();
		} catch (cause) {
			if (firstFailure === undefined) firstFailure = cause;
		}
		if (firstFailure !== undefined) {
			throw new ShardingPersistenceError("Could not close Bridge analytics storage.", { cause: firstFailure });
		}
	}

	#ensureOpen(): void {
		if (this.#closed) {
			throw new ShardingPersistenceError("Bridge analytics storage is closed.");
		}
	}
}

function createResources(path: string): BridgeAnalyticsResources {
	if (typeof path !== "string" || path.length === 0 || path.length > 32_768) {
		throw new ShardingPersistenceError("Bridge analytics path must contain between 1 and 32768 characters.");
	}

	let database: Database | undefined;
	try {
		database = new Database(path, {
			create: true,
			readwrite: true,
			safeIntegers: false,
			strict: true,
		});
		database.run("PRAGMA journal_mode = WAL");
		database.run("PRAGMA synchronous = NORMAL");
		database.run("PRAGMA foreign_keys = ON");
		database.run("PRAGMA busy_timeout = 5000");
		database.run(`
			CREATE TABLE IF NOT EXISTS bridge_analytics (
				id TEXT PRIMARY KEY NOT NULL,
				bridge_id TEXT NOT NULL,
				shard_id INTEGER,
				collected_at INTEGER NOT NULL,
				data_json TEXT NOT NULL,
				CHECK (shard_id IS NULL OR (shard_id >= 0 AND shard_id < ${MAX_SHARDS})),
				CHECK (collected_at >= 0),
				CHECK (json_valid(data_json))
			)
		`);
		database.run(`
			CREATE INDEX IF NOT EXISTS bridge_analytics_collected_idx
			ON bridge_analytics (collected_at DESC, id DESC)
		`);
		database.run(`
			CREATE INDEX IF NOT EXISTS bridge_analytics_shard_collected_idx
			ON bridge_analytics (shard_id, collected_at DESC, id DESC)
		`);

		const append = database.query<unknown, [string, string, number | null, number, string]>(`
			INSERT INTO bridge_analytics (id, bridge_id, shard_id, collected_at, data_json)
			VALUES (?, ?, ?, ?, ?)
		`);
		const read = database.query<unknown, [number]>(`
			SELECT id, bridge_id, shard_id, collected_at, data_json
			FROM bridge_analytics
			ORDER BY collected_at DESC, id DESC
			LIMIT ?
		`);
		const readShard = database.query<unknown, [number, number]>(`
			SELECT id, bridge_id, shard_id, collected_at, data_json
			FROM bridge_analytics
			WHERE shard_id = ?
			ORDER BY collected_at DESC, id DESC
			LIMIT ?
		`);
		const clear = database.query<unknown, [number, number]>(`
			DELETE FROM bridge_analytics
			WHERE rowid IN (
				SELECT rowid
				FROM bridge_analytics
				WHERE collected_at <= ?
				ORDER BY collected_at ASC, id ASC
				LIMIT ?
			)
		`);
		return { append, clear, database, read, readShard };
	} catch (cause) {
		try {
			database?.close();
		} catch {
			// Preserve the initialization failure as the actionable cause.
		}
		throw new ShardingPersistenceError("Could not initialize Bridge analytics storage.", { cause });
	}
}

function normalizeQuery(query: $BridgeAnalyticsQuery | undefined): {
	readonly limit: number;
	readonly shardId?: number;
} {
	if (query === undefined) return { limit: DEFAULT_READ_LIMIT };
	const snapshot = snapshotConfigurationRecord(query, "Bridge analytics query");
	assertConfigurationKeys(snapshot, ANALYTICS_QUERY_KEYS, "Bridge analytics query");
	const limit =
		snapshot.limit === undefined
			? DEFAULT_READ_LIMIT
			: requirePositiveInteger(snapshot.limit, "Bridge analytics query limit", MAX_READ_LIMIT);
	if (snapshot.shardId === undefined) return { limit };
	return { limit, shardId: requireShardId(snapshot.shardId, "Bridge analytics query shardId") };
}

function normalizeAnalyticsRecord(value: unknown, name: string): NormalizedAnalyticsRecord {
	const snapshot = snapshotConfigurationRecord(value, name);
	assertConfigurationKeys(snapshot, ANALYTICS_RECORD_KEYS, name);
	const shardId = snapshot.shardId === null ? null : requireShardId(snapshot.shardId, `${name} shardId`);
	const data = normalizeJsonValue(snapshot.data, `${name} data`);
	const dataJson = JSON.stringify(data);
	if (dataJson === undefined) {
		throw new ShardingPersistenceError(`${name} data could not be serialized.`);
	}
	return Object.freeze({
		bridgeId: requireIdentifier(snapshot.bridgeId, `${name} bridgeId`),
		collectedAt: requireSafeNonNegativeInteger(snapshot.collectedAt, `${name} collectedAt`),
		dataJson,
		id: requireIdentifier(snapshot.id, `${name} id`),
		shardId,
	});
}

function mapAnalyticsRows(value: unknown, name: string): readonly $AnalyticsRecord[] {
	if (!Array.isArray(value)) {
		throw new ShardingPersistenceError(`${name} returned a non-array result.`);
	}
	const records: $AnalyticsRecord[] = [];
	for (let index = 0; index < value.length; index += 1) {
		const row: unknown = value[index];
		const record = requireExactRow(row, ANALYTICS_ROW_KEYS, `${name} row ${index}`);
		const shardValue = record.shard_id;
		records.push(
			Object.freeze({
				bridgeId: requireIdentifier(record.bridge_id, `${name} bridge_id`),
				collectedAt: requireSafeNonNegativeInteger(record.collected_at, `${name} collected_at`),
				data: parseJsonValue(record.data_json, `${name} data_json`),
				id: requireIdentifier(record.id, `${name} id`),
				shardId: shardValue === null ? null : requireShardId(shardValue, `${name} shard_id`),
			}),
		);
	}
	return Object.freeze(records);
}

function parseJsonValue(value: unknown, name: string): $JsonValue {
	if (typeof value !== "string") {
		throw new ShardingPersistenceError(`${name} must be stored as JSON text.`);
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(value);
	} catch (cause) {
		throw new ShardingPersistenceError(`${name} contains invalid JSON.`, { cause });
	}
	return normalizeJsonValue(parsed, name);
}

function normalizeJsonValue(value: unknown, name: string): $JsonValue {
	const normalized = normalizePayload(value, DEFAULT_PAYLOAD_POLICY, name);
	if (!isJsonValue(normalized)) {
		throw new ShardingPersistenceError(`${name} is not a JSON value.`);
	}
	return normalized;
}

function isJsonValue(value: unknown): value is $JsonValue {
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
		const entry: unknown = Reflect.get(value, key);
		if (!isJsonValue(entry)) return false;
	}
	return true;
}

function requireExactRow(
	value: unknown,
	expectedKeys: ReadonlySet<string>,
	name: string,
): Readonly<Record<string, unknown>> {
	if (!isUnknownRecord(value)) {
		throw new ShardingPersistenceError(`${name} must be an object.`);
	}
	const keys = Object.keys(value);
	if (keys.length !== expectedKeys.size) {
		throw new ShardingPersistenceError(`${name} contains an unexpected column set.`);
	}
	for (const key of keys) {
		if (!expectedKeys.has(key)) {
			throw new ShardingPersistenceError(`${name} contains unexpected column "${key}".`);
		}
	}
	return value;
}

function isUnknownRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireSafeNonNegativeInteger(value: unknown, name: string): number {
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
		throw new ShardingPersistenceError(`${name} must be a non-negative safe integer.`);
	}
	return value;
}

function requirePositiveInteger(value: unknown, name: string, maximum: number): number {
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0 || value > maximum) {
		throw new ShardingPersistenceError(`${name} must be a positive integer no greater than ${maximum}.`);
	}
	return value;
}

function finalize(statement: { finalize(): void }, firstFailure: unknown): unknown {
	try {
		statement.finalize();
	} catch (cause) {
		return firstFailure ?? cause;
	}
	return firstFailure;
}
