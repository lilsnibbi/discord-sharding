import { $ } from "bun";

/**
 * Removes one exact child directory after validating its absolute parent.
 *
 * Windows uses PowerShell because Bun Shell cannot reliably remove the
 * junction graph produced by the isolated package linker.
 *
 * @param path Absolute child directory to remove.
 * @param expectedParent Absolute directory that must directly contain it.
 */
export async function removeDirectChildDirectory(path: string, expectedParent: string): Promise<void> {
	const validated = validateDirectChild(path, expectedParent);
	if (navigator.platform === "Win32") {
		await removeWithPowerShell(validated.path, validated.parent);
		return;
	}

	const cleanup = await $`rm -rf ${validated.path}`.quiet().nothrow();
	if (cleanup.exitCode !== 0) throw new Error(`Bun Shell could not remove ${validated.path}`);
}

async function removeWithPowerShell(path: string, expectedParent: string): Promise<void> {
	const powershell = Bun.which("powershell.exe") ?? Bun.which("powershell") ?? Bun.which("pwsh") ?? "powershell.exe";
	const environment: Record<string, string | undefined> = {
		...Bun.env,
		SHARDING_CLEANUP_PARENT: expectedParent,
		SHARDING_CLEANUP_TARGET: path,
	};
	const script = `$target = [System.IO.Path]::GetFullPath($env:SHARDING_CLEANUP_TARGET)
$expectedParent = [System.IO.Path]::GetFullPath($env:SHARDING_CLEANUP_PARENT)
$actualParent = [System.IO.Directory]::GetParent($target).FullName
if (-not [System.String]::Equals($actualParent, $expectedParent, [System.StringComparison]::OrdinalIgnoreCase)) {
	throw "Refusing to remove a path outside the expected parent directory."
}
if (Test-Path -LiteralPath $target) {
	Remove-Item -LiteralPath $target -Recurse -Force -ErrorAction Stop
}`;
	const child = Bun.spawn([powershell, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script], {
		env: environment,
		stderr: "pipe",
		stdin: "ignore",
		stdout: "pipe",
	});
	const [exitCode, stderr, stdout] = await Promise.all([
		child.exited,
		new Response(child.stderr).text(),
		new Response(child.stdout).text(),
	]);
	if (exitCode !== 0) {
		const diagnostic = `${stdout}\n${stderr}`.trim();
		throw new Error(`PowerShell could not remove ${path}${diagnostic.length > 0 ? `\n${diagnostic}` : ""}`);
	}
}

function validateDirectChild(path: string, expectedParent: string): { readonly parent: string; readonly path: string } {
	const normalizedPath = normalizeAbsolutePath(path);
	const normalizedParent = normalizeAbsolutePath(expectedParent);
	const comparedPath = navigator.platform === "Win32" ? normalizedPath.toLowerCase() : normalizedPath;
	const comparedParent = navigator.platform === "Win32" ? normalizedParent.toLowerCase() : normalizedParent;
	const prefix = `${comparedParent}/`;
	if (!comparedPath.startsWith(prefix) || comparedPath.slice(prefix.length).includes("/")) {
		throw new Error(`Refusing to remove a path that is not a direct child of ${normalizedParent}: ${normalizedPath}`);
	}
	return { parent: normalizedParent, path: normalizedPath };
}

function normalizeAbsolutePath(path: string): string {
	const normalized = path.replaceAll("\\", "/").replace(/\/+$/, "");
	const segments = normalized.split("/");
	if (
		!(/^[A-Za-z]:\//.test(normalized) || normalized.startsWith("/")) ||
		segments.some((segment) => segment === "." || segment === "..")
	) {
		throw new Error(`Cleanup paths must be absolute and must not contain traversal segments: ${path}`);
	}
	return normalized;
}
