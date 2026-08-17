import { type $DiscordClient, ShardClient } from "@lilsnibbi/discord-sharding";

/**
 * Starts one Bridge-managed shard with an application-owned Discord client.
 *
 * Create the client with `SHARDING_SHARD_ID` as its only shard and
 * `SHARDING_TOTAL_SHARDS` as its shard count before calling this helper.
 *
 * @param botClient - Ready-capable client created by the application.
 * @param options - Optional bot token and pre-start handler registration.
 * @returns The running shard client.
 */
export async function startShard<Client extends $DiscordClient>(
	botClient: Client,
	options: {
		readonly configure?: (shard: ShardClient<Client>) => void;
		readonly token?: string;
	} = {},
): Promise<ShardClient<Client>> {
	const shard = new ShardClient(botClient, {
		onError(error, context) {
			console.error(`Shard background failure (${context}).`, error);
		},
	});

	try {
		options.configure?.(shard);
		await shard.start();
		await shard.login(options.token);
		return shard;
	} catch (cause) {
		await shard.close();
		throw cause;
	}
}
