import { API } from "typescript/unstable/sync";

/** Absolute Bun executable used by repository scripts. */
export const BUN_EXECUTABLE = Bun.which("bun") ?? "bun";

/** Absolute repository root. */
export const ROOT_DIRECTORY = import.meta.dir.replace(/[\\/]scripts$/, "");

/**
 * Constructs the TypeScript synchronous compiler service, retrying on startup failure.
 *
 * The service spawns a native compiler subprocess and reads its stdout file descriptor
 * synchronously; under Bun that descriptor is occasionally not yet attached to the process
 * handle at the instant the constructor inspects it, which throws instead of returning a
 * usable client. A short, bounded retry absorbs that startup race without masking a
 * persistently broken compiler service.
 */
export async function createSyncApi(options: ConstructorParameters<typeof API>[0]): Promise<API> {
	const attempts = 3;
	for (let attempt = 1; ; attempt++) {
		try {
			return new API(options);
		} catch (cause) {
			if (attempt >= attempts) {
				throw new Error("Unable to start the TypeScript synchronous compiler service.", { cause });
			}
			await Bun.sleep(attempt * 50);
		}
	}
}
