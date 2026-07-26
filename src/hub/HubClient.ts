import type { $HubClientOptions } from "../types/hub";
import { HubLifecycle } from "./client/HubLifecycle";

/**
 * Coordinates every Bridge and shard for one Discord bot.
 *
 * Run one Hub for the bot. Call {@link start} before accepting Bridges and
 * {@link stop} during shutdown.
 */
export class HubClient extends HubLifecycle {
	/**
	 * Creates a Hub without opening its database or network server.
	 *
	 * @param options - Bot, authentication, database, server, and capacity settings.
	 */
	// biome-ignore lint/complexity/noUselessConstructor: public constructor required to expose protected superclass constructor
	public constructor(options: $HubClientOptions) {
		super(options);
	}
}
