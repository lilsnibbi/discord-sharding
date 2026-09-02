import { $ } from "bun";
import { removeDirectChildDirectory } from "./cleanup";
import {
	type $ValidatedPackageManifest,
	EXPECTED_PACKAGE_NAME,
	requireRecord,
	requireString,
	validatePackageManifest,
} from "./package-manifest";
import { BUN_EXECUTABLE, ROOT_DIRECTORY } from "./repository";
import { runInherited } from "./run";

const temporaryBase = `${ROOT_DIRECTORY}/.tmp`;

/**
 * Packs, inspects, installs, type-checks, and executes the package. When a
 * destination is supplied, the exact validated archive is retained there.
 *
 * @param destination Optional directory for the verified package archive.
 */
export async function createVerifiedPackageArtifact(destination?: string): Promise<string | undefined> {
	const temporaryRoot = `${temporaryBase}/pack-check-${crypto.randomUUID()}`;
	const tarballDirectory = `${temporaryRoot}/tarball`;
	assertTemporaryPath(temporaryRoot);

	let result: string | undefined;
	let failure: { readonly cause: unknown } | undefined;
	try {
		await $`mkdir -p ${tarballDirectory}`.quiet();
		await runInherited(
			[BUN_EXECUTABLE, "pm", "pack", "--ignore-scripts", "--destination", tarballDirectory],
			ROOT_DIRECTORY,
		);
		const tarballs = await globFiles(tarballDirectory, "*.tgz");
		if (tarballs.length !== 1) {
			throw new Error(`Expected one package archive, received ${tarballs.length}`);
		}
		const tarball = tarballs[0];
		if (tarball === undefined) throw new Error("Package archive was not created");
		await inspectTarball(tarball, `${temporaryRoot}/inspect`);
		await validateConsumer(`${temporaryRoot}/consumer`, tarball);

		if (destination !== undefined) result = await copyVerifiedTarball(tarball, destination);
		console.log("Raw TypeScript package files, exports, types, and runtime imports are valid.");
	} catch (cause) {
		failure = { cause };
	}

	const cleanupFailure = await removeDirectChildDirectory(temporaryRoot, temporaryBase).then(
		() => undefined,
		(cause: unknown) => ({ cause }),
	);
	if (failure) throw failure.cause;
	if (cleanupFailure) {
		throw new Error(`Unable to clean temporary package directory: ${temporaryRoot}`, {
			cause: cleanupFailure.cause,
		});
	}
	return result;
}

if (import.meta.main) await createVerifiedPackageArtifact();

async function inspectTarball(tarball: string, destination: string): Promise<$ValidatedPackageManifest> {
	const archive = new Bun.Archive(await Bun.file(tarball).bytes());
	const archiveFiles = await archive.files();
	const listing = [...archiveFiles.keys()].sort();
	for (const path of listing) assertArchivePath(path);

	for (const path of [
		"package/package.json",
		"package/README.md",
		"package/SECURITY.md",
		"package/LICENSE",
		"package/src/index.ts",
		"package/docs/README.md",
		"package/docs/api-reference.md",
		"package/docs/architecture.md",
		"package/docs/examples.md",
		"package/docs/getting-started.md",
		"package/docs/operations.md",
		"package/docs/performance.md",
		"package/docs/troubleshooting.md",
	]) {
		if (!listing.includes(path)) throw new Error(`Package archive is missing ${path}`);
	}
	if (!listing.some((path) => path.startsWith("package/examples/") && path.endsWith(".ts"))) {
		throw new Error("Package archive contains no TypeScript examples");
	}

	for (const path of listing) {
		if (isCompiledArtifact(path)) throw new Error(`Package archive contains a compiled artifact: ${path}`);
		if (!isReviewedArchivePath(path)) throw new Error(`Package archive contains an unexpected path: ${path}`);
	}

	await $`mkdir -p ${destination}`.quiet();
	await archive.extract(destination);
	const manifestValue: unknown = await Bun.file(`${destination}/package/package.json`).json();
	const manifest = validatePackageManifest(manifestValue, true);
	for (const target of manifest.exportTargets) {
		const packedPath = `package/${target.slice(2)}`;
		if (!listing.includes(packedPath)) throw new Error(`Package export target is absent from its archive: ${target}`);
		if (!(await Bun.file(`${destination}/${packedPath}`).exists())) {
			throw new Error(`Package export target does not exist after extraction: ${target}`);
		}
	}
	return manifest;
}

async function validateConsumer(directory: string, tarball: string): Promise<void> {
	await $`mkdir -p ${directory}`.quiet();
	const rootValue: unknown = await Bun.file(`${ROOT_DIRECTORY}/package.json`).json();
	const rootManifest = requireRecord(rootValue, "root package manifest");
	const rootDevDependencies = requireRecord(rootManifest.devDependencies, "root development dependencies");
	const packageManager = requireString(rootManifest, "packageManager", "root package manifest");
	const typescriptVersion = requireString(rootDevDependencies, "typescript", "root development dependencies");
	const bunTypesVersion = requireString(rootDevDependencies, "@types/bun", "root development dependencies");

	await Bun.write(
		`${directory}/package.json`,
		JSON.stringify(
			{
				name: "sharding-packed-consumer",
				private: true,
				type: "module",
				packageManager,
				dependencies: {
					[EXPECTED_PACKAGE_NAME]: `file:${normalizePath(tarball)}`,
				},
				devDependencies: {
					"@types/bun": bunTypesVersion,
					typescript: typescriptVersion,
				},
			},
			null,
			2,
		),
	);
	await Bun.write(
		`${directory}/bunfig.toml`,
		`env = false

[install]
linker = "isolated"
`,
	);
	await Bun.write(
		`${directory}/tsconfig.json`,
		JSON.stringify(
			{
				compilerOptions: {
					exactOptionalPropertyTypes: true,
					forceConsistentCasingInFileNames: true,
					isolatedModules: true,
					lib: ["ESNext", "DOM", "DOM.Iterable"],
					module: "Preserve",
					moduleDetection: "force",
					moduleResolution: "Bundler",
					noEmit: true,
					noImplicitReturns: true,
					noUncheckedIndexedAccess: true,
					noUnusedLocals: true,
					noUnusedParameters: true,
					resolveJsonModule: true,
					skipLibCheck: false,
					strict: true,
					target: "ESNext",
					types: ["bun"],
					useUnknownInCatchVariables: true,
					verbatimModuleSyntax: true,
				},
				include: ["index.ts"],
			},
			null,
			2,
		),
	);
	await Bun.write(
		`${directory}/index.ts`,
		`import * as sharding from ${JSON.stringify(EXPECTED_PACKAGE_NAME)};
import manifest from ${JSON.stringify(`${EXPECTED_PACKAGE_NAME}/package.json`)};

const { BridgeClient, HubClient, ShardClient } = sharding;
const runtimeExports = Object.keys(sharding).sort();
if (JSON.stringify(runtimeExports) !== JSON.stringify(["BridgeClient", "HubClient", "ShardClient"])) {
	throw new Error(\`Unexpected public runtime exports: \${runtimeExports.join(", ")}\`);
}
const publicClasses: ReadonlyArray<unknown> = [BridgeClient, HubClient, ShardClient];
if (publicClasses.some((value) => typeof value !== "function")) {
	throw new Error("A required public class is unavailable");
}
if (manifest.name !== ${JSON.stringify(EXPECTED_PACKAGE_NAME)}) {
	throw new Error("The package.json export is invalid");
}
console.log("packed public imports ok");
`,
	);

	await runInherited([BUN_EXECUTABLE, "install", "--ignore-scripts"], directory);
	await runInherited([BUN_EXECUTABLE, "x", "--bun", "--no-install", "tsc", "--project", "tsconfig.json"], directory);
	await runInherited([BUN_EXECUTABLE, "run", "index.ts"], directory);
}

async function copyVerifiedTarball(tarball: string, destination: string): Promise<string> {
	await $`mkdir -p ${destination}`.quiet();
	const filename = tarball.split(/[\\/]/).at(-1);
	if (!filename) throw new Error("Unable to determine the package archive filename");
	const output = `${destination}/${filename}`;
	await Bun.write(output, Bun.file(tarball));
	return output;
}

async function globFiles(directory: string, pattern: string): Promise<string[]> {
	const files: string[] = [];
	for await (const path of new Bun.Glob(pattern).scan({ absolute: true, cwd: directory, onlyFiles: true })) {
		files.push(path);
	}
	return files.sort();
}

function isCompiledArtifact(path: string): boolean {
	const lower = path.toLowerCase();
	if (lower.split("/").includes("dist")) return true;
	return /\.(?:cjs|d\.cts|d\.mts|d\.ts|js|jsx|map|mjs|node|tsbuildinfo|wasm)$/.test(lower);
}

function isReviewedArchivePath(path: string): boolean {
	return (
		path === "package/package.json" ||
		path === "package/README.md" ||
		path === "package/SECURITY.md" ||
		path === "package/LICENSE" ||
		(path.startsWith("package/src/") && path.endsWith(".ts")) ||
		(path.startsWith("package/docs/") && path.endsWith(".md")) ||
		(path.startsWith("package/examples/") && path.endsWith(".ts"))
	);
}

function assertTemporaryPath(path: string): void {
	if (!path.startsWith(`${temporaryBase}/`) || path === temporaryBase) {
		throw new Error(`Refusing to use unsafe temporary path: ${path}`);
	}
}

function assertArchivePath(path: string): void {
	const segments = path.split("/");
	if (
		path.includes("\\") ||
		!path.startsWith("package/") ||
		segments.some((segment) => segment.length === 0 || segment === "." || segment === "..")
	) {
		throw new Error(`Package archive contains an unsafe path: ${path}`);
	}
}

function normalizePath(path: string): string {
	return path.replaceAll("\\", "/");
}
