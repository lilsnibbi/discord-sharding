/**
 * Discord cache information read for built-in analytics.
 */
export interface $DiscordCache {
	/**
	 * Number of cached values.
	 */
	readonly size: number;
}

/**
 * Discord manager information read for built-in analytics.
 */
export interface $DiscordManager {
	/**
	 * Manager cache, when enabled by the application.
	 */
	readonly cache?: $DiscordCache;
}

/**
 * Discord Gateway information read for built-in analytics.
 */
export interface $DiscordWebSocketManager {
	/**
	 * Current average Gateway latency in milliseconds.
	 */
	readonly ping?: number;

	/**
	 * Current discord.js WebSocket status code.
	 */
	readonly status?: number;
}

/**
 * Parts of a Discord client used by {@link ShardClient}.
 *
 * A discord.js v14 `Client` already satisfies this interface. The application
 * keeps ownership of the client and its token.
 */
export interface $DiscordClient {
	/**
	 * Ends the Discord connection during an authorized shard shutdown.
	 */
	destroy(): void;

	/**
	 * Reports whether the client is ready.
	 */
	isReady(): boolean;

	/**
	 * Starts login after the Hub grants an identify slot.
	 *
	 * @param token - Optional bot token accepted by discord.js.
	 */
	login(token?: string): Promise<string>;

	/**
	 * Cached channel manager used for analytics, when available.
	 */
	readonly channels?: $DiscordManager;

	/**
	 * Cached guild manager used for analytics, when available.
	 */
	readonly guilds?: $DiscordManager;

	/**
	 * Time at which the client most recently became ready.
	 */
	readonly readyAt?: Date | null;

	/**
	 * Milliseconds since the client most recently became ready.
	 */
	readonly uptime?: number | null;

	/**
	 * Cached user manager used for analytics, when available.
	 */
	readonly users?: $DiscordManager;

	/**
	 * Discord Gateway manager used for latency and state analytics.
	 */
	readonly ws?: $DiscordWebSocketManager;
}
