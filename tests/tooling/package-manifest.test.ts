import { describe, expect, test } from "bun:test";
import {
	EXPECTED_DEPENDENCIES,
	EXPECTED_ENTRY_POINT,
	EXPECTED_FILES,
	EXPECTED_PACKAGE_NAME,
	requireRecord,
	validatePackageManifest,
} from "../../scripts/package-manifest";

const manifestValue: unknown = await Bun.file(`${import.meta.dir}/../../package.json`).json();
const validManifest = requireRecord(manifestValue, "test package manifest");
const validBugsUrl = "https://github.com/lilsnibbi/discord-sharding/issues";
const validRepositoryUrl = "git+https://github.com/lilsnibbi/discord-sharding.git";

describe("package manifest validation", () => {
	test("accepts the reviewed raw TypeScript package", () => {
		const result = validatePackageManifest(validManifest, false);
		expect(result.name).toBe(EXPECTED_PACKAGE_NAME);
		expect(result.exportTargets).toEqual([EXPECTED_ENTRY_POINT, "./package.json"]);
	});

	test("publishes the reviewed package identity", () => {
		expect(validManifest.name).toBe(EXPECTED_PACKAGE_NAME);
		expect(validManifest.author).toBe("lilsnibbi");
		expect(validManifest.license).toBe("Apache-2.0");
		expect(validManifest.homepage).toBe("https://github.com/lilsnibbi/discord-sharding#readme");
		expect(requireRecord(validManifest.bugs, "test bugs").url).toBe(
			"https://github.com/lilsnibbi/discord-sharding/issues",
		);
		expect(requireRecord(validManifest.repository, "test repository").url).toBe(
			"git+https://github.com/lilsnibbi/discord-sharding.git",
		);
	});

	test.each([
		["private", true],
		["workspaces", ["packages/*"]],
		["main", "./src/index.ts"],
		["module", "./src/index.ts"],
	] as const)("rejects legacy %s metadata", (field, value) => {
		expect(() => validatePackageManifest({ ...validManifest, [field]: value }, false)).toThrow();
	});

	test.each([
		["name", "@example/sharding", 'unexpected name "@example/sharding"'],
		["description", "Another package", "unexpected description"],
		["version", "1.0", 'invalid version "1.0"'],
		["author", "Someone Else", "attribute lilsnibbi as author"],
		["license", "MIT", "must use Apache-2.0"],
		["homepage", "https://example.invalid", "unexpected homepage metadata"],
		["packageManager", "bun@1.0.0", "must pin Bun 1.3.14"],
		["type", "commonjs", "must be ESM"],
		["types", "./dist/index.d.ts", "raw TypeScript declarations"],
		["sideEffects", true, "sideEffects=false"],
		["$schema", "https://example.invalid/schema.json", "unexpected schema metadata"],
	] as const)("rejects altered %s metadata", (field, value, message) => {
		expect(() => validatePackageManifest({ ...validManifest, [field]: value }, false)).toThrow(message);
	});

	test.each([
		["bugs", { url: "https://example.invalid/issues" }, "unexpected bugs metadata"],
		["bugs", { email: "security@example.invalid", url: validBugsUrl }, "unexpected bugs metadata"],
		["repository", { type: "git", url: "git+https://example.invalid/other.git" }, "unexpected repository metadata"],
		["repository", { url: validRepositoryUrl }, "unexpected repository metadata"],
		["engines", { bun: ">=1.0.0" }, "must require only Bun >=1.3.14"],
		["engines", { bun: ">=1.3.14", node: ">=22" }, "must require only Bun >=1.3.14"],
		["publishConfig", { access: "restricted" }, "must publish with public access"],
		["publishConfig", { access: "public", registry: "https://example.invalid" }, "must publish with public access"],
		["keywords", ["bun", "discord"], "unexpected keywords"],
		["keywords", ["bun", "discord", "ipc", "sharding", "websocket", "extra"], "unexpected keywords"],
	] as const)("rejects unreviewed %s metadata", (field, value, message) => {
		expect(() => validatePackageManifest({ ...validManifest, [field]: value }, false)).toThrow(message);
	});

	test("rejects unreviewed development dependencies", () => {
		const devDependencies = requireRecord(validManifest.devDependencies, "test development dependencies");
		expect(() =>
			validatePackageManifest({ ...validManifest, devDependencies: { ...devDependencies, example: "1.0.0" } }, false),
		).toThrow("reviewed development dependencies");
	});

	test("rejects manifests that are not records or lack required strings", () => {
		expect(() => validatePackageManifest([], false)).toThrow("must be an object");
		expect(() => validatePackageManifest({ ...validManifest, name: "" }, false)).toThrow(
			"name must be a non-empty string",
		);
		expect(() => validatePackageManifest({ ...validManifest, keywords: ["bun", 1] }, false)).toThrow(
			"keywords must be an array of strings",
		);
		expect(() => validatePackageManifest({ ...validManifest, dependencies: { arktype: "" } }, false)).toThrow(
			"dependencies.arktype must be a non-empty string",
		);
	});

	test("rejects a packed peer dependency that retained a workspace protocol", () => {
		const packed = {
			...validManifest,
			dependencies: { arktype: "2.2.3" },
			peerDependencies: { "discord.js": "workspace:^14.27.0" },
		};
		expect(() => validatePackageManifest(packed, true)).toThrow("reviewed discord.js peer");
	});

	test("rejects a package.json export that does not target the manifest", () => {
		const exportsMap = requireRecord(validManifest.exports, "test exports");
		expect(() =>
			validatePackageManifest(
				{ ...validManifest, exports: { ...exportsMap, "./package.json": "./src/package.json" } },
				false,
			),
		).toThrow("must export its package.json");
	});

	test("rejects bundled dependency metadata", () => {
		expect(() => validatePackageManifest({ ...validManifest, bundleDependencies: ["arktype"] }, false)).toThrow(
			"must not publish bundleDependencies",
		);
		expect(() => validatePackageManifest({ ...validManifest, bundledDependencies: {} }, false)).toThrow(
			"must not publish bundledDependencies",
		);
	});

	test("rejects compiled package files", () => {
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					files: ["dist", ...EXPECTED_FILES.slice(1)],
				},
				false,
			),
		).toThrow("publish only raw source");
	});

	test.each(["docs", "SECURITY.md"] as const)("requires published %s", (requiredFile) => {
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					files: EXPECTED_FILES.filter((file) => file !== requiredFile),
				},
				false,
			),
		).toThrow("complete package documentation");
	});

	test.each(["types", "bun", "import", "default"] as const)("rejects a compiled %s export", (condition) => {
		const exportsMap = requireRecord(validManifest.exports, "test exports");
		const rootExport = requireRecord(exportsMap["."], "test root export");
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					exports: {
						...exportsMap,
						".": {
							...rootExport,
							[condition]: "./dist/index.js",
						},
					},
				},
				false,
			),
		).toThrow(`must target ${EXPECTED_ENTRY_POINT}`);
	});

	test("rejects an altered ArkType version", () => {
		const dependencies = requireRecord(validManifest.dependencies, "test dependencies");
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					dependencies: {
						...dependencies,
						arktype: "2.2.2",
					},
				},
				false,
			),
		).toThrow("reviewed ArkType dependency");
	});

	test("rejects another production dependency", () => {
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					dependencies: {
						...EXPECTED_DEPENDENCIES,
						example: "1.0.0",
					},
				},
				false,
			),
		).toThrow("reviewed ArkType dependency");
	});

	test("requires ArkType", () => {
		expect(() => validatePackageManifest({ ...validManifest, dependencies: {} }, false)).toThrow(
			"reviewed ArkType dependency",
		);
	});

	test("rejects optional dependencies", () => {
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					optionalDependencies: {
						example: "1.0.0",
					},
				},
				false,
			),
		).toThrow("must not publish optionalDependencies");
	});

	test("rejects an unreviewed peer dependency", () => {
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					peerDependencies: {
						"discord.js": "^14.27.0",
						example: "1.0.0",
					},
				},
				false,
			),
		).toThrow("reviewed discord.js peer");
	});

	test("requires the discord.js peer to remain optional", () => {
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					peerDependenciesMeta: {
						"discord.js": {
							optional: false,
						},
					},
				},
				false,
			),
		).toThrow("keep the discord.js peer optional");
	});

	test("rejects accidental public subpath exports", () => {
		const exportsMap = requireRecord(validManifest.exports, "test exports");
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					exports: {
						...exportsMap,
						"./internal": "./src/internal.ts",
					},
				},
				false,
			),
		).toThrow("only its root API");
	});

	test("rejects install lifecycle hooks", () => {
		const scripts = requireRecord(validManifest.scripts, "test scripts");
		expect(() =>
			validatePackageManifest(
				{
					...validManifest,
					scripts: {
						...scripts,
						postinstall: "bun run unexpected",
					},
				},
				false,
			),
		).toThrow("unexpected package scripts");
	});

	test.each(["bin", "imports", "typesVersions"] as const)("rejects unreviewed %s metadata", (field) => {
		expect(() => validatePackageManifest({ ...validManifest, [field]: {} }, false)).toThrow(
			"unreviewed manifest fields",
		);
	});
});
