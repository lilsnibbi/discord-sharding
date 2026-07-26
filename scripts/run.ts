/**
 * Runs a command with inherited output and rejects on a non-zero exit code.
 *
 * @param command Executable followed by its arguments.
 * @param cwd Working directory for the child process.
 */
export async function runInherited(command: readonly string[], cwd: string): Promise<void> {
	const child = Bun.spawn([...command], {
		cwd,
		env: Bun.env,
		stderr: "inherit",
		stdin: "ignore",
		stdout: "inherit",
	});
	const exitCode = await child.exited;
	if (exitCode !== 0) throw new Error(`Command failed (${exitCode}): ${command.join(" ")}`);
}
