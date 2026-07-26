import { ShardingConfigurationError, ShardingStateError } from "../errors/ShardingError";
import type { $Sleep } from "../types/common";
import { MAX_TIMER_MS } from "./limits";

/**
 * Default abort-aware timer used for identify pacing and restart backoff.
 */
export const abortableSleep: $Sleep = async (milliseconds, signal) => {
	if (!Number.isSafeInteger(milliseconds) || milliseconds < 0 || milliseconds > MAX_TIMER_MS) {
		throw new ShardingConfigurationError(`Sleep delay must be an integer between 0 and ${MAX_TIMER_MS}.`);
	}
	if (signal.aborted) throw signal.reason ?? new ShardingStateError("Sleep was cancelled.");
	await new Promise<void>((resolve, reject) => {
		let settled = false;
		const finish = (): void => {
			if (settled) return;
			settled = true;
			signal.removeEventListener("abort", abort);
			resolve();
		};
		const abort = (): void => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			signal.removeEventListener("abort", abort);
			reject(signal.reason ?? new ShardingStateError("Sleep was cancelled."));
		};
		const timer = setTimeout(finish, milliseconds);
		signal.addEventListener("abort", abort, { once: true });
		if (signal.aborted) abort();
	});
};
