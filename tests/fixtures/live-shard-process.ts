import { ShardClient } from "../../src/index";
import { LiveGatewayClient } from "../utilities/live-gateway-client";

function requireEnvironment(name: string): string {
	const value = Bun.env[name];
	if (value === undefined || value.length === 0) throw new Error(`${name} is required.`);
	return value;
}

const outputDirectory = requireEnvironment("SHARDING_FIXTURE_OUTPUT");
const shardId = Number(requireEnvironment("SHARDING_SHARD_ID"));
const totalShards = Number(requireEnvironment("SHARDING_TOTAL_SHARDS"));

const shard = new ShardClient(new LiveGatewayClient({ shardId, totalShards }), {
	analyticsIntervalMs: false,
	onError: (error, context) => {
		console.error(`live shard ${shardId} ${context}: ${error.message}`);
	},
	onRequest: (payload): { readonly echo: unknown; readonly from: number } => ({
		echo: payload,
		from: shardId,
	}),
	readyPollIntervalMs: 100,
});

await shard.start();
await shard.login(requireEnvironment("TOKEN"));

const target = (shard.id + 1) % shard.totalShards;
const deadline = Date.now() + 60_000;
let result: unknown;
let failure: string | undefined;
while (Date.now() < deadline) {
	try {
		result = await shard.request(target, { from: shard.id });
		failure = undefined;
		break;
	} catch (cause) {
		failure = cause instanceof Error ? cause.message : String(cause);
		await Bun.sleep(100);
	}
}

await Bun.write(
	`${outputDirectory}/live-shard-${shard.id}.json`,
	JSON.stringify({
		...(failure === undefined ? {} : { failure }),
		gatewayReady: shard.isReady,
		identity: shard.identity,
		result,
		shardId: shard.id,
		target,
		totalShards: shard.totalShards,
	}),
);
