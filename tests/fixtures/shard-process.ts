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
	onRequest: (payload): { readonly echo: unknown; readonly from: number } => ({
		echo: payload,
		from: assignedShardId,
	}),
	readyPollIntervalMs: 100,
});

await shard.start();
await shard.login("fixture-token");

const target = (shard.id + 1) % shard.totalShards;
const deadline = Date.now() + 20_000;
let result: unknown;
let failure: string | undefined;
while (Date.now() < deadline) {
	try {
		result = await shard.request(target, { from: shard.id });
		failure = undefined;
		break;
	} catch (cause) {
		failure = cause instanceof Error ? cause.message : String(cause);
		await Bun.sleep(50);
	}
}

await Bun.write(
	`${outputDirectory}/shard-${shard.id}.json`,
	JSON.stringify({
		...(failure === undefined ? {} : { failure }),
		result,
		shardId: shard.id,
		target,
		totalShards: shard.totalShards,
	}),
);
