/** Published package name. */
export const EXPECTED_PACKAGE_NAME = "@lilsnibbi/discord-sharding";

/** Raw TypeScript package entry point. */
export const EXPECTED_ENTRY_POINT = "./src/index.ts";

/** Files and directories intentionally included in the package. */
export const EXPECTED_FILES = ["src", "examples", "docs", "README.md", "SECURITY.md", "LICENSE"] as const;

/** Exact production packages used by the raw TypeScript source. */
export const EXPECTED_DEPENDENCIES = {
	arktype: "2.2.3",
} as const;

const EXPECTED_SCHEMA = "https://json.schemastore.org/package.json";
const EXPECTED_HOMEPAGE = "https://github.com/lilsnibbi/discord-sharding#readme";
const EXPECTED_BUGS_URL = "https://github.com/lilsnibbi/discord-sharding/issues";
const EXPECTED_REPOSITORY_URL = "git+https://github.com/lilsnibbi/discord-sharding.git";
const EXPECTED_DESCRIPTION = "Bun-native Discord shard orchestration across Hub, Bridge, and shard processes.";
const EXPECTED_KEYWORDS = ["bun", "discord", "ipc", "redis", "sharding", "websocket"] as const;
const EXPECTED_DEV_DEPENDENCIES = {
	"@biomejs/biome": "2.5.5",
	"@types/bun": "1.3.14",
	typescript: "7.0.2",
} as const;
const EXPECTED_SCRIPTS = {
	check: "bun scripts/check.ts",
	"check:docs": "bun scripts/check-docs.ts",
	"check:examples": "bun scripts/check-examples.ts",
	"check:fix": "biome check --write .",
	"check:imports": "bun scripts/check-imports.ts",
	"check:jsdoc": "bun scripts/check-jsdoc.ts",
	"check:lines": "bun scripts/check-lines.ts",
	clean: "bun scripts/clean.ts",
	format: "biome format --write .",
	"format:check": "biome format .",
	lint: "biome lint .",
	"lint:fix": "biome lint --write .",
	"pack:check": "bun scripts/check-packages.ts",
	"release:pack": "bun scripts/release.ts pack",
	"release:verify": "bun scripts/release.ts verify",
	test: "bun test --path-ignore-patterns 'tests/performance/**'",
	"test:coverage": "bun scripts/coverage.ts",
	"test:performance": "bun test tests/performance",
	"test:redis": "bun test tests/integration/redis.test.ts",
	typecheck: "bunx --bun --no-install tsc --project tsconfig.json --noEmit",
	verify: "bun scripts/verify.ts",
} as const satisfies Readonly<Record<string, string>>;
const REVIEWED_MANIFEST_KEYS = [
	"$schema",
	"author",
	"bugs",
	"bundleDependencies",
	"bundledDependencies",
	"dependencies",
	"description",
	"devDependencies",
	"engines",
	"exports",
	"files",
	"homepage",
	"keywords",
	"license",
	"main",
	"module",
	"name",
	"optionalDependencies",
	"packageManager",
	"peerDependencies",
	"peerDependenciesMeta",
	"private",
	"publishConfig",
	"repository",
	"scripts",
	"sideEffects",
	"type",
	"types",
	"version",
	"workspaces",
] as const;

/** Validated metadata consumed by packing and release checks. */
export interface $ValidatedPackageManifest {
	readonly exportTargets: readonly string[];
	readonly name: string;
	readonly version: string;
}

/**
 * Validates source or packed package metadata.
 *
 * @param value Untrusted package manifest value.
 * @param packed Whether the manifest came from the generated package archive.
 */
export function validatePackageManifest(value: unknown, packed: boolean): $ValidatedPackageManifest {
	const context = packed ? "packed package manifest" : "package manifest";
	const manifest = requireRecord(value, context);
	if (manifest.$schema !== EXPECTED_SCHEMA) throw new Error(`${context} has unexpected schema metadata`);
	const name = requireString(manifest, "name", context);
	if (name !== EXPECTED_PACKAGE_NAME) throw new Error(`${context} has unexpected name "${name}"`);
	if (requireString(manifest, "description", context) !== EXPECTED_DESCRIPTION) {
		throw new Error(`${name} has an unexpected description`);
	}

	const version = requireString(manifest, "version", context);
	if (!isSemVer(version)) throw new Error(`${context} has invalid version "${version}"`);
	if (manifest.private !== undefined) throw new Error(`${name} must not declare private metadata`);
	if (manifest.workspaces !== undefined) throw new Error(`${name} must not declare workspaces`);
	if (manifest.main !== undefined || manifest.module !== undefined) {
		throw new Error(`${name} must use its reviewed raw TypeScript exports without legacy entry points`);
	}
	if (manifest.type !== "module") throw new Error(`${name} must be ESM`);
	if (manifest.types !== EXPECTED_ENTRY_POINT) throw new Error(`${name} must expose raw TypeScript declarations`);
	if (manifest.sideEffects !== false) throw new Error(`${name} must declare sideEffects=false`);
	if (manifest.packageManager !== "bun@1.3.14") throw new Error(`${name} must pin Bun 1.3.14`);
	if (manifest.author !== "lilsnibbi") throw new Error(`${name} must attribute lilsnibbi as author`);
	if (manifest.license !== "Apache-2.0") throw new Error(`${name} must use Apache-2.0`);
	if (manifest.homepage !== EXPECTED_HOMEPAGE) throw new Error(`${name} has unexpected homepage metadata`);

	const bugs = requireRecord(manifest.bugs, `${context} bugs`);
	if (bugs.url !== EXPECTED_BUGS_URL || !sameStringSet(Object.keys(bugs), ["url"])) {
		throw new Error(`${name} has unexpected bugs metadata`);
	}
	const keywords = requireStringArray(manifest.keywords, `${context} keywords`);
	if (!sameStringSet(keywords, EXPECTED_KEYWORDS)) throw new Error(`${name} has unexpected keywords`);

	const engines = requireRecord(manifest.engines, `${context} engines`);
	if (engines.bun !== ">=1.3.14" || !sameStringSet(Object.keys(engines), ["bun"])) {
		throw new Error(`${name} must require only Bun >=1.3.14`);
	}

	const publishConfig = requireRecord(manifest.publishConfig, `${context} publishConfig`);
	if (publishConfig.access !== "public" || !sameStringSet(Object.keys(publishConfig), ["access"])) {
		throw new Error(`${name} must publish with public access`);
	}

	const repository = requireRecord(manifest.repository, `${context} repository`);
	if (
		repository.type !== "git" ||
		repository.url !== EXPECTED_REPOSITORY_URL ||
		!sameStringSet(Object.keys(repository), ["type", "url"])
	) {
		throw new Error(`${name} has unexpected repository metadata`);
	}

	const scripts = readStringRecord(manifest.scripts, `${context} scripts`);
	if (!sameStringRecord(scripts, EXPECTED_SCRIPTS)) throw new Error(`${name} has unexpected package scripts`);

	const devDependencies = readStringRecord(manifest.devDependencies, `${context} devDependencies`);
	if (!sameStringRecord(devDependencies, EXPECTED_DEV_DEPENDENCIES)) {
		throw new Error(`${name} must retain only the reviewed development dependencies`);
	}
	const dependencies = readStringRecord(manifest.dependencies, `${context} dependencies`);
	if (!sameStringRecord(dependencies, EXPECTED_DEPENDENCIES)) {
		throw new Error(`${name} must publish only the reviewed ArkType dependency`);
	}
	const optionalDependencies = readStringRecord(manifest.optionalDependencies, `${context} optionalDependencies`);
	if (Object.keys(optionalDependencies).length !== 0) {
		throw new Error(`${name} must not publish optionalDependencies`);
	}
	const peers = readStringRecord(manifest.peerDependencies, `${context} peerDependencies`);
	if (!sameStringRecord(peers, { "discord.js": "^14.27.0" })) {
		throw new Error(`${name} must declare only the reviewed discord.js peer`);
	}
	const peerMetadata = requireRecord(manifest.peerDependenciesMeta, `${context} peerDependenciesMeta`);
	const discordMetadata = requireRecord(peerMetadata["discord.js"], `${context} discord.js peer metadata`);
	if (
		!sameStringSet(Object.keys(peerMetadata), ["discord.js"]) ||
		discordMetadata.optional !== true ||
		!sameStringSet(Object.keys(discordMetadata), ["optional"])
	) {
		throw new Error(`${name} must keep the discord.js peer optional`);
	}
	for (const field of ["bundleDependencies", "bundledDependencies"] as const) {
		const bundled = manifest[field];
		if (bundled !== undefined && (!Array.isArray(bundled) || bundled.length > 0)) {
			throw new Error(`${name} must not publish ${field}`);
		}
	}

	const files = requireStringArray(manifest.files, `${context} files`);
	if (!sameStringSet(files, EXPECTED_FILES)) {
		throw new Error(`${name} must publish only raw source, examples, and complete package documentation`);
	}

	const exportsMap = requireRecord(manifest.exports, `${context} exports`);
	if (!sameStringSet(Object.keys(exportsMap), [".", "./package.json"])) {
		throw new Error(`${name} must expose only its root API and package.json`);
	}
	const rootExport = requireRecord(exportsMap["."], `${context} root export`);
	if (!sameStringSet(Object.keys(rootExport), ["types", "bun", "import", "default"])) {
		throw new Error(`${name} root export has unexpected conditions`);
	}
	for (const condition of ["types", "bun", "import", "default"] as const) {
		if (rootExport[condition] !== EXPECTED_ENTRY_POINT) {
			throw new Error(`${name} ${condition} export must target ${EXPECTED_ENTRY_POINT}`);
		}
	}
	if (exportsMap["./package.json"] !== "./package.json") {
		throw new Error(`${name} must export its package.json`);
	}

	const unexpectedKeys = Object.keys(manifest).filter(
		(key) => !(REVIEWED_MANIFEST_KEYS as readonly string[]).includes(key),
	);
	if (unexpectedKeys.length > 0) {
		throw new Error(`${name} has unreviewed manifest fields: ${unexpectedKeys.sort().join(", ")}`);
	}
	if (packed && JSON.stringify(manifest).includes("workspace:")) {
		throw new Error(`${name} retained a workspace protocol after packing`);
	}

	return {
		exportTargets: [EXPECTED_ENTRY_POINT, "./package.json"],
		name,
		version,
	};
}

/**
 * Narrows an untrusted value to a plain record.
 *
 * @param value Value to validate.
 * @param context Diagnostic context.
 */
export function requireRecord(value: unknown, context: string): Readonly<Record<string, unknown>> {
	if (!isRecord(value)) throw new TypeError(`${context} must be an object`);
	return value;
}

/**
 * Reads a required non-empty string from an untrusted record.
 *
 * @param value Record containing the property.
 * @param key Property name.
 * @param context Diagnostic context.
 */
export function requireString(value: Readonly<Record<string, unknown>>, key: string, context: string): string {
	const result = value[key];
	if (typeof result !== "string" || result.length === 0) {
		throw new TypeError(`${context} ${key} must be a non-empty string`);
	}
	return result;
}

function isSemVer(value: string): boolean {
	const identifier = "[0-9A-Za-z-]+";
	const prereleaseIdentifier = "(?:0|[1-9]\\d*|\\d*[A-Za-z-][0-9A-Za-z-]*)";
	const pattern = new RegExp(
		`^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-${prereleaseIdentifier}(?:\\.${prereleaseIdentifier})*)?(?:\\+${identifier}(?:\\.${identifier})*)?$`,
	);
	return pattern.test(value);
}

function readStringRecord(value: unknown, context: string): Readonly<Record<string, string>> {
	if (value === undefined) return {};
	const record = requireRecord(value, context);
	const result: Record<string, string> = {};
	for (const [key, entry] of Object.entries(record)) {
		if (typeof entry !== "string" || entry.length === 0) {
			throw new TypeError(`${context}.${key} must be a non-empty string`);
		}
		result[key] = entry;
	}
	return result;
}

function requireStringArray(value: unknown, context: string): readonly string[] {
	if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string")) {
		throw new TypeError(`${context} must be an array of strings`);
	}
	return value;
}

function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
	const leftSet = new Set(left);
	const rightSet = new Set(right);
	return (
		left.length === leftSet.size &&
		right.length === rightSet.size &&
		leftSet.size === rightSet.size &&
		[...leftSet].every((entry) => rightSet.has(entry))
	);
}

function sameStringRecord(left: Readonly<Record<string, string>>, right: Readonly<Record<string, string>>): boolean {
	const leftKeys = Object.keys(left);
	const rightKeys = Object.keys(right);
	return sameStringSet(leftKeys, rightKeys) && leftKeys.every((key) => left[key] === right[key]);
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
