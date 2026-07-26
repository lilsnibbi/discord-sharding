/** Absolute Bun executable used by repository scripts. */
export const BUN_EXECUTABLE = Bun.which("bun") ?? "bun";

/** Absolute repository root. */
export const ROOT_DIRECTORY = import.meta.dir.replace(/[\\/]scripts$/, "");
