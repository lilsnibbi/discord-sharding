import { Database } from "bun:sqlite";
import { runSQLiteMigrations } from "../src/hub/database/SQLiteMigrationRunner";
import { ROOT_DIRECTORY } from "./repository";

const [command, ...rawCommandArguments] = Bun.argv.slice(2);
const commandArguments = rawCommandArguments[0] === "--" ? rawCommandArguments.slice(1) : rawCommandArguments;

switch (command) {
	case "check":
		assertNoArguments(command, commandArguments);
		await checkMigrations();
		break;
	case "migrate":
		assertNoArguments(command, commandArguments);
		await migrate();
		break;
	case "new":
		await createMigration(commandArguments);
		break;
	default:
		throw new Error("Usage: bun scripts/database.ts <check|migrate|new> [migration name]");
}

async function migrate(): Promise<void> {
	const path = requireDatabasePath();
	const database = new Database(path, { create: true, readwrite: true, safeIntegers: false, strict: true });
	try {
		await runSQLiteMigrations(database);
	} finally {
		database.close();
	}
	console.log(`SQLite migrations applied to ${path}.`);
}

async function checkMigrations(): Promise<void> {
	const database = new Database(":memory:", { strict: true });
	try {
		await runSQLiteMigrations(database);
		const integrity = database.query<{ readonly integrity_check: string }, []>("PRAGMA integrity_check").get();
		if (integrity?.integrity_check !== "ok") throw new Error("SQLite migration integrity check failed.");
		const requiredTables = new Set(["__sharding_migrations", "analytics", "assignments", "bridges", "shards"]);
		const rows = database
			.query<{ readonly name: string }, []>("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name ASC")
			.all();
		for (const row of rows) requiredTables.delete(row.name);
		if (requiredTables.size > 0) {
			throw new Error(`SQLite migrations are missing tables: ${[...requiredTables].join(", ")}`);
		}
	} finally {
		database.close();
	}
	console.log("SQLite migrations apply cleanly from an empty database.");
}

async function createMigration(parts: readonly string[]): Promise<void> {
	if (parts.length === 0) throw new Error("db:new requires a short migration name");
	const slug = parts
		.join("_")
		.trim()
		.toLowerCase()
		.replace(/[^a-z0-9]+/gu, "_")
		.replace(/^_+|_+$/gu, "");
	if (slug.length === 0 || slug.length > 80) {
		throw new Error("Migration names must contain 1 to 80 letters or numbers");
	}
	const stamp = new Date().toISOString().replace(/\D/gu, "").slice(0, 14);
	const directory = `${ROOT_DIRECTORY}/migrations/${stamp}_${slug}`;
	if (await Bun.file(`${directory}/migration.sql`).exists()) throw new Error(`Migration ${stamp}_${slug} exists`);
	await Bun.write(`${directory}/migration.sql`, `-- ${slug.replaceAll("_", " ")}\n`);
	console.log(`Created migrations/${stamp}_${slug}/migration.sql`);
}

function requireDatabasePath(): string {
	const value = Bun.env.SHARDING_DATABASE_PATH;
	if (value === undefined || value.length === 0 || value.length > 32_768) {
		throw new Error("Set SHARDING_DATABASE_PATH to the target SQLite file");
	}
	return value;
}

function assertNoArguments(commandName: string, values: readonly string[]): void {
	if (values.length > 0) throw new Error(`db:${commandName} does not accept arguments`);
}
