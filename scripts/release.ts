import { $ } from "bun";
import { createVerifiedPackageArtifact } from "./check-packages";
import { requireRecord, validatePackageManifest } from "./package-manifest";
import { ROOT_DIRECTORY } from "./repository";

async function verify(tagArgument?: string): Promise<string> {
	const manifestValue: unknown = await Bun.file(`${ROOT_DIRECTORY}/package.json`).json();
	const manifest = validatePackageManifest(requireRecord(manifestValue, "root package manifest"), false);
	await validateRequiredFiles();

	const expectedTag = `v${manifest.version}`;
	if (tagArgument !== undefined && tagArgument !== expectedTag) {
		throw new Error(`Release tag "${tagArgument}" must match "${expectedTag}"`);
	}
	console.log(`Verified ${manifest.name} ${manifest.version}`);
	return manifest.version;
}

async function validateRequiredFiles(): Promise<void> {
	for (const path of ["src/index.ts", "README.md", "SECURITY.md", "LICENSE"]) {
		if (!(await Bun.file(`${ROOT_DIRECTORY}/${path}`).exists())) {
			throw new Error(`Required package file is missing: ${path}`);
		}
	}
	if ((await globFiles(`${ROOT_DIRECTORY}/examples`, "*.ts")).length === 0) {
		throw new Error("At least one TypeScript example is required");
	}
	if ((await globFiles(`${ROOT_DIRECTORY}/docs`, "*.md")).length === 0) {
		throw new Error("Published package documentation is required");
	}
}

async function pack(tagArgument?: string): Promise<void> {
	if (tagArgument === undefined) {
		throw new Error("release:pack requires the exact release tag, for example v0.1.4");
	}
	await verify(tagArgument);
	await assertCleanWorktree();

	const destination = `${ROOT_DIRECTORY}/release-assets`;
	assertReleasePath(destination);
	await $`rm -rf ${destination}`.quiet();
	await $`mkdir -p ${destination}`.quiet();

	const tarball = await createVerifiedPackageArtifact(destination);
	if (tarball === undefined) throw new Error("Verified package archive was not retained");
	await writeChecksum(tarball);
	console.log(`Prepared the verified package archive in ${destination}`);
}

async function assertCleanWorktree(): Promise<void> {
	const child = Bun.spawn(["git", "status", "--porcelain=v1"], {
		cwd: ROOT_DIRECTORY,
		stderr: "pipe",
		stdout: "pipe",
	});
	const [exitCode, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	if (exitCode !== 0) {
		throw new Error(`Unable to inspect release worktree: ${stderr.trim() || `git exited ${exitCode}`}`);
	}
	if (stdout.trim().length > 0) throw new Error("Release archives must be produced from a clean reviewed worktree");
}

async function writeChecksum(path: string): Promise<void> {
	const digest = await crypto.subtle.digest("SHA-256", await Bun.file(path).arrayBuffer());
	const checksum = [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
	const filename = path.split(/[\\/]/).at(-1);
	if (!filename) throw new Error(`Unable to determine release filename for ${path}`);
	await Bun.write(`${path}.sha256`, `${checksum}  ${filename}\n`);
}

async function globFiles(directory: string, pattern: string): Promise<string[]> {
	const files: string[] = [];
	for await (const path of new Bun.Glob(pattern).scan({ absolute: true, cwd: directory, onlyFiles: true })) {
		files.push(path);
	}
	return files.sort();
}

function assertReleasePath(path: string): void {
	if (path !== `${ROOT_DIRECTORY}/release-assets`) {
		throw new Error(`Refusing to replace unsafe release path: ${path}`);
	}
}

const [command, ...rawCommandArguments] = Bun.argv.slice(2);
const commandArguments = rawCommandArguments[0] === "--" ? rawCommandArguments.slice(1) : rawCommandArguments;
if (commandArguments.length > 1) throw new Error("Usage: bun scripts/release.ts <verify|pack> [v<version>]");
const [tag] = commandArguments;
switch (command) {
	case "pack":
		await pack(tag);
		break;
	case "verify":
		await verify(tag);
		break;
	default:
		throw new Error("Usage: bun scripts/release.ts <verify|pack> [v<version>]");
}
