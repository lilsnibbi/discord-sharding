import { ROOT_DIRECTORY } from "./repository";

const MAXIMUM_LINES = 500;
const EXCLUDED_ROOTS = new Set([".git", ".tmp", "coverage", "node_modules", "release-assets"]);

/**
 * Enforces the repository-wide maintained-file line limit.
 */
export async function checkMaintainedFileLengths(): Promise<void> {
	const violations: { readonly lines: number; readonly path: string }[] = [];
	let checked = 0;
	for await (const scannedPath of new Bun.Glob("**/*").scan({
		cwd: ROOT_DIRECTORY,
		dot: true,
		onlyFiles: true,
	})) {
		const path = normalizePath(scannedPath);
		const root = path.split("/", 1)[0];
		if (root !== undefined && EXCLUDED_ROOTS.has(root)) continue;
		checked += 1;
		const lines = countLines(await Bun.file(`${ROOT_DIRECTORY}/${path}`).bytes());
		if (lines > MAXIMUM_LINES) violations.push({ lines, path });
	}
	violations.sort((left, right) => right.lines - left.lines || left.path.localeCompare(right.path));
	if (violations.length > 0) {
		for (const violation of violations) console.error(`${violation.path}: ${violation.lines} lines`);
		throw new Error(
			`${violations.length} maintained file${violations.length === 1 ? "" : "s"} exceed ${MAXIMUM_LINES} lines.`,
		);
	}
	console.log(`Validated ${checked} maintained files at no more than ${MAXIMUM_LINES} lines each.`);
}

if (import.meta.main) await checkMaintainedFileLengths();

function countLines(bytes: Uint8Array): number {
	if (bytes.length === 0) return 0;
	let lines = 1;
	for (const byte of bytes) {
		if (byte === 10) lines += 1;
	}
	if (bytes.at(-1) === 10) lines -= 1;
	return lines;
}

function normalizePath(path: string): string {
	return path.replaceAll("\\", "/");
}
