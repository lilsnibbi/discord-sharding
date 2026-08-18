import { ShardClient } from "../../src/index";

/**
 * Minimal stand-in for an application Discord client.
 *
 * The fixture never reaches Discord. It only has to satisfy the readiness
 * contract that `ShardClient` observes after an identify grant.
 */
class FixtureDiscordClient {
	#ready = false;

	public async login(token?: string): Promise<string> {
		await Bun.sleep(5);
		this.#ready = true;
		return token ?? "fixture";
	}

	public isReady(): boolean {
		return this.#ready;
	}

	public destroy(): void {
		this.#ready = false;
	}
}

function requireEnvironment(name: string): string {
	const value = Bun.env[name];
	if (value === undefined || value.length === 0) throw new Error(`${name} is required.`);
	return value;
}

const outputDirectory = requireEnvironment("SHARDING_FIXTURE_OUTPUT");
const assignedShardId = Number(requireEnvironment("SHARDING_SHARD_ID"));
const shard = new ShardClient(new FixtureDiscordClient(), {
	analyticsIntervalMs: false,
	onError: (error, context) => {
		console.error(`shard ${assignedShardId} ${context}: ${error.message}`);
	},
	readyPollIntervalMs: 100,
});

await shard.start();
await shard.login("fixture-token");

const { processGeneration } = shard.identity;
await Bun.write(
	`${outputDirectory}/shard-${shard.id}-generation-${processGeneration}.json`,
	JSON.stringify({
		pid: process.pid,
		processGeneration,
		shardId: shard.id,
		totalShards: shard.totalShards,
	}),
);

// The process now idles. The ShardClient heartbeat keeps the event loop alive
// until the Bridge stops the process or the test kills it externally.
