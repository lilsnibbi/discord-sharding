import { Database, type Statement } from "bun:sqlite";
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
	mapAssignmentRows,
	mapBridgeRows,
	mapShardRows,
	normalizeAnalyticsRecord,
	normalizeAssignment,
	normalizeBridge,
	normalizeShard,
	requireDatabaseInteger,
} from "./SQLiteHubRecords";
import { runSQLiteMigrations } from "./SQLiteMigrationRunner";

const MAX_ANALYTICS_CLEAR_BATCH_SIZE = 10_000;

type MigrationRunner = (database: Database) => Promise<void>;

interface Statements {
	readonly appendAnalytics: Statement<unknown, [string, string, number | null, number, string]>;
	readonly clearAnalytics: Statement<unknown, [number, number]>;
	readonly loadAssignments: Statement<unknown, []>;
	readonly loadBridges: Statement<unknown, []>;
	readonly loadShards: Statement<unknown, []>;
	readonly saveAssignment: Statement<unknown, [number, string, number, number]>;
	readonly saveBridge: Statement<unknown, [string, string, number, number, number]>;
	readonly saveShard: Statement<unknown, [number, string, number, number, string, number]>;
}

/** Persists Hub topology and analytics with Bun's native SQLite client. */
export class SQLiteHubPersistence implements $HubPersistence {
	readonly #database: Database;
	readonly #migrationRunner: MigrationRunner;
	#statements: Statements | undefined;
	#closed = false;
	#closePromise: Promise<void> | undefined;
	#migrationPromise: Promise<void> | undefined;

	/**
	 * Opens file-backed Hub persistence.
	 *
	 * @param path - SQLite path, or `":memory:"` for an ephemeral database.
	 * @param database - Optional native database used by deterministic tests.
	 * @param migrationRunner - Optional migration runner used by deterministic tests.
	 */
	public constructor(path: string, database?: Database, migrationRunner: MigrationRunner = runSQLiteMigrations) {
		if (typeof path !== "string" || path.length === 0 || path.length > 32_768) {
			throw new ShardingConfigurationError("databasePath must contain between 1 and 32768 characters.");
		}
		if (typeof migrationRunner !== "function") {
			throw new ShardingConfigurationError("migrationRunner must be a function.");
		}
		this.#migrationRunner = migrationRunner;
		let opened = database;
		try {
			opened ??= new Database(path, { create: true, readwrite: true, safeIntegers: false, strict: true });
			opened.run("PRAGMA foreign_keys = ON");
			opened.run("PRAGMA busy_timeout = 5000");
			opened.run("PRAGMA synchronous = FULL");
			if (path !== ":memory:") opened.run("PRAGMA journal_mode = WAL");
			this.#database = opened;
		} catch (cause) {
			try {
				opened?.close();
			} catch {
				// Preserve the initialization failure.
			}
			throw new ShardingPersistenceError("Could not initialize Hub SQLite persistence.", { cause });
		}
	}

	/** Applies packaged migrations once per persistence lifetime. */
	public migrate(): Promise<void> {
		this.#ensureOpen();
		this.#migrationPromise ??= runPersistenceOperation("Could not migrate the Hub SQLite database.", async () => {
			await this.#migrationRunner(this.#database);
			this.#statements ??= prepareStatements(this.#database);
		});
		return this.#migrationPromise;
	}

	/** Loads a validated immutable Hub state snapshot. */
	public async loadState(): Promise<$PersistedHubState> {
		this.#ensureOpen();
		return runPersistenceOperation("Could not load Hub state.", () => {
			const statements = this.#requireStatements();
			const assignments = mapAssignmentRows(statements.loadAssignments.all());
			const bridges = mapBridgeRows(statements.loadBridges.all());
			const shards = mapShardRows(statements.loadShards.all());
			return Object.freeze({ assignments, bridges, shards });
		});
	}

	/** Creates or replaces one sticky assignment. */
	public async saveAssignment(assignment: $PersistedAssignment): Promise<void> {
		this.#ensureOpen();
		const value = normalizeAssignment(assignment);
		await runPersistenceOperation("Could not save a shard assignment.", () => {
			this.#requireStatements().saveAssignment.run(value.shardId, value.bridgeId, value.epoch, value.updatedAt);
		});
	}

	/** Stores the latest Bridge status. */
	public async saveBridge(bridge: $PersistedBridge): Promise<void> {
		this.#ensureOpen();
		const value = normalizeBridge(bridge);
		await runPersistenceOperation("Could not save Bridge state.", () => {
			this.#requireStatements().saveBridge.run(
				value.id,
				value.generation,
				value.maxShards,
				value.connected ? 1 : 0,
				value.updatedAt,
			);
		});
	}

	/** Stores the latest shard process status. */
	public async saveShard(shard: $PersistedShard): Promise<void> {
		this.#ensureOpen();
		const value = normalizeShard(shard);
		await runPersistenceOperation("Could not save shard state.", () => {
			this.#requireStatements().saveShard.run(
				value.shardId,
				value.bridgeId,
				value.assignmentEpoch,
				value.processGeneration,
				value.state,
				value.updatedAt,
			);
		});
	}

	/** Appends one global analytics sample. */
	public async appendAnalytics(record: $AnalyticsRecord): Promise<void> {
		this.#ensureOpen();
		const value = normalizeAnalyticsRecord(record);
		await runPersistenceOperation("Could not append Hub analytics.", () => {
			this.#requireStatements().appendAnalytics.run(
				value.id,
				value.bridgeId,
				value.shardId,
				value.collectedAt,
				value.dataJson,
			);
		});
	}

	/** Deletes at most one bounded analytics batch. */
	public async clearAnalyticsBatch(before: number, batchSize: number): Promise<number> {
		this.#ensureOpen();
		const cutoff = requireDatabaseInteger(before, "before", 0, Number.MAX_SAFE_INTEGER);
		const boundedBatchSize = requireDatabaseInteger(batchSize, "batchSize", 1, MAX_ANALYTICS_CLEAR_BATCH_SIZE);
		return runPersistenceOperation(
			"Could not clear Hub analytics.",
			() => this.#requireStatements().clearAnalytics.run(cutoff, boundedBatchSize).changes,
		);
	}

	/** Finalizes prepared statements and closes SQLite idempotently. */
	public close(): Promise<void> {
		if (this.#closePromise !== undefined) return this.#closePromise;
		this.#closed = true;
		this.#closePromise = runPersistenceOperation("Could not close the Hub SQLite database.", () => {
			let firstFailure: unknown;
			for (const statement of Object.values(this.#statements ?? {})) {
				try {
					statement.finalize();
				} catch (cause) {
					firstFailure ??= cause;
				}
			}
			try {
				this.#database.close();
			} catch (cause) {
				firstFailure ??= cause;
			}
			if (firstFailure !== undefined) throw firstFailure;
		});
		return this.#closePromise;
	}

	#ensureOpen(): void {
		if (this.#closed) throw new ShardingPersistenceError("Hub persistence is closed.");
	}

	#requireStatements(): Statements {
		this.#ensureOpen();
		if (this.#statements === undefined) {
			throw new ShardingPersistenceError("Hub persistence must be migrated before use.");
		}
		return this.#statements;
	}
}

function prepareStatements(database: Database): Statements {
	return {
		appendAnalytics: database.query(
			"INSERT INTO analytics (id, bridge_id, shard_id, collected_at, data_json) VALUES (?, ?, ?, ?, ?)",
		),
		clearAnalytics: database.query(`
			DELETE FROM analytics
			WHERE rowid IN (
				SELECT rowid FROM analytics
				WHERE collected_at <= ?
				ORDER BY collected_at ASC, id ASC
				LIMIT ?
			)
		`),
		loadAssignments: database.query(
			"SELECT shard_id, bridge_id, epoch, updated_at FROM assignments ORDER BY shard_id ASC",
		),
		loadBridges: database.query(
			"SELECT id, generation, max_shards, connected, updated_at FROM bridges ORDER BY id ASC",
		),
		loadShards: database.query(`
			SELECT shard_id, bridge_id, assignment_epoch, process_generation, state, updated_at
			FROM shards ORDER BY shard_id ASC
		`),
		saveAssignment: database.query(`
			INSERT INTO assignments (shard_id, bridge_id, epoch, updated_at)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (shard_id) DO UPDATE SET
				bridge_id = excluded.bridge_id,
				epoch = excluded.epoch,
				updated_at = excluded.updated_at
			WHERE excluded.epoch > assignments.epoch
				OR (excluded.epoch = assignments.epoch AND excluded.updated_at > assignments.updated_at)
		`),
		saveBridge: database.query(`
			INSERT INTO bridges (id, generation, max_shards, connected, updated_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT (id) DO UPDATE SET
				generation = excluded.generation,
				max_shards = excluded.max_shards,
				connected = excluded.connected,
				updated_at = excluded.updated_at
			WHERE excluded.updated_at > bridges.updated_at
		`),
		saveShard: database.query(`
			INSERT INTO shards (
				shard_id, bridge_id, assignment_epoch, process_generation, state, updated_at
			) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (shard_id) DO UPDATE SET
				bridge_id = excluded.bridge_id,
				assignment_epoch = excluded.assignment_epoch,
				process_generation = excluded.process_generation,
				state = excluded.state,
				updated_at = excluded.updated_at
			WHERE excluded.assignment_epoch > shards.assignment_epoch
				OR (
					excluded.assignment_epoch = shards.assignment_epoch
					AND excluded.updated_at > shards.updated_at
				)
		`),
	};
}

async function runPersistenceOperation<T>(message: string, operation: () => T | Promise<T>): Promise<T> {
	try {
		return await operation();
	} catch (cause) {
		if (cause instanceof ShardingPersistenceError) throw cause;
		throw new ShardingPersistenceError(message, { cause });
	}
}
