import { ShardingTransportError } from "../errors/ShardingError";
import type { $Sleep } from "../types/common";
import type { $GatewayBotInfo, $GatewayFetch } from "../types/hub";
import { fetchGatewayBotInfo } from "./gateway";

/** Number of Discord Gateway Bot requests one Hub startup may make. */
const MAX_ATTEMPTS = 4;
/** Delay before the second Discord Gateway Bot request. */
const INITIAL_DELAY_MS = 500;
/** Ceiling applied to the exponential delay between requests. */
const MAX_DELAY_MS = 4_000;

export interface $GatewayStartupOptions {
	readonly endpoint: string | URL;
	readonly fetch?: $GatewayFetch;
	readonly signal: AbortSignal;
	readonly sleep: $Sleep;
}

/**
 * Loads Discord Gateway Bot metadata with bounded retries.
 *
 * Discord is the Hub's only external dependency and a single unavailable
 * response would otherwise stop the whole deployment from starting, so
 * transport failures are retried with exponential backoff. Configuration and
 * protocol failures describe conditions that cannot resolve themselves and are
 * raised on the first attempt. Shutdown cancels the wait between attempts but
 * never cancels a request that is already in flight.
 *
 * @param token - Raw Discord bot token without an authorization prefix.
 * @param options - Endpoint, fetch implementation, cancellation, and clock.
 * @returns Validated Discord Gateway Bot metadata.
 */
export async function loadGatewayBotInfo(token: string, options: $GatewayStartupOptions): Promise<$GatewayBotInfo> {
	let lastCause: unknown;
	for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
		try {
			return await fetchGatewayBotInfo(token, {
				endpoint: options.endpoint,
				...(options.fetch === undefined ? {} : { fetch: options.fetch }),
			});
		} catch (cause) {
			if (!(cause instanceof ShardingTransportError)) throw cause;
			lastCause = cause;
			if (attempt === MAX_ATTEMPTS - 1 || options.signal.aborted) break;
			try {
				await options.sleep(Math.min(MAX_DELAY_MS, INITIAL_DELAY_MS * 2 ** attempt), options.signal);
			} catch {
				break;
			}
		}
	}
	throw new ShardingTransportError(
		`Discord Gateway Bot metadata was unavailable after ${MAX_ATTEMPTS} attempts.`,
		lastCause === undefined ? {} : { cause: lastCause },
	);
}
