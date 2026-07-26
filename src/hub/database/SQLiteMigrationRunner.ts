import type { Database } from "bun:sqlite";
import { ShardingPersistenceError } from "../../errors/ShardingError";

const PACKAGE_ROOT = `${import.meta.dir}/../../..`;
const MIGRATIONS_DIRECTORY = `${PACKAGE_ROOT}/migrations`;
const MIGRATION_NAME = /^[0-9]{14}_[a-z0-9]+(?:_[a-z0-9]+)*$/u;

interface Migration {
	readonly checksum: string;
	readonly name: string;
	readonly sql: string;
}

/** Applies every packaged SQLite migration exactly once. */
export async function runSQLiteMigrations(database: Database): Promise<void> {
	try {
		database.exec(`
			CREATE TABLE IF NOT EXISTS __sharding_migrations (
				name TEXT PRIMARY KEY NOT NULL,
				checksum TEXT NOT NULL,
				applied_at INTEGER NOT NULL CHECK (applied_at >= 0)
			)
		`);
		const migrations = await readMigrations();
		const applied = database
			.query<unknown, []>("SELECT name, checksum FROM __sharding_migrations ORDER BY name ASC")
			.all();
		const appliedByName = new Map<string, string>();
		for (const value of applied) {
			if (!isMigrationRow(value)) throw new Error("Migration history contains an invalid row.");
			appliedByName.set(value.name, value.checksum);
		}
		for (const [name] of appliedByName) {
			if (!migrations.some((migration) => migration.name === name)) {
				throw new Error(`Applied migration ${name} is missing from the package.`);
			}
		}
		const apply = database.transaction((migration: Migration) => {
			database.exec(migration.sql);
			database
				.query<unknown, [string, string, number]>(
					"INSERT INTO __sharding_migrations (name, checksum, applied_at) VALUES (?, ?, ?)",
				)
				.run(migration.name, migration.checksum, Date.now());
		});
		for (const migration of migrations) {
			const checksum = appliedByName.get(migration.name);
			if (checksum !== undefined) {
				if (checksum !== migration.checksum) {
					throw new Error(`Applied migration ${migration.name} has changed.`);
				}
				continue;
			}
			apply.immediate(migration);
		}
	} catch (cause) {
		if (cause instanceof ShardingPersistenceError) throw cause;
		throw new ShardingPersistenceError("Could not migrate the Hub SQLite database.", { cause });
	}
}

async function readMigrations(): Promise<readonly Migration[]> {
	const directories = new Set<string>();
	for await (const path of new Bun.Glob("*/migration.sql").scan({
		absolute: false,
		cwd: MIGRATIONS_DIRECTORY,
		onlyFiles: true,
	})) {
		const name = path.replaceAll("\\", "/").split("/")[0];
		if (name === undefined || !MIGRATION_NAME.test(name)) {
			throw new Error(`Invalid migration path ${path}.`);
		}
		directories.add(name);
	}
	if (directories.size === 0) throw new Error("No packaged SQLite migrations were found.");
	const migrations: Migration[] = [];
	for (const name of [...directories].sort()) {
		const sql = await Bun.file(`${MIGRATIONS_DIRECTORY}/${name}/migration.sql`).text();
		if (sql.trim().length === 0) throw new Error(`Migration ${name} is empty.`);
		const checksum = new Bun.CryptoHasher("sha256").update(sql).digest("hex");
		migrations.push(Object.freeze({ checksum, name, sql }));
	}
	return Object.freeze(migrations);
}

function isMigrationRow(value: unknown): value is { readonly checksum: string; readonly name: string } {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
	const name = Reflect.get(value, "name");
	const checksum = Reflect.get(value, "checksum");
	return (
		typeof name === "string" && MIGRATION_NAME.test(name) && typeof checksum === "string" && checksum.length === 64
	);
}
