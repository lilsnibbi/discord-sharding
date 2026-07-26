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

describe("package manifest validation", () => {
	test("accepts the reviewed raw TypeScript package", () => {
		const result = validatePackageManifest(validManifest, false);
		expect(result.name).toBe(EXPECTED_PACKAGE_NAME);
		expect(result.exportTargets).toEqual([EXPECTED_ENTRY_POINT, "./package.json"]);
	});

	test.each([
		["private", true],
		["workspaces", ["packages/*"]],
		["main", "./src/index.ts"],
		["module", "./src/index.ts"],
	] as const)("rejects legacy %s metadata", (field, value) => {
		expect(() => validatePackageManifest({ ...validManifest, [field]: value }, false)).toThrow();
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
