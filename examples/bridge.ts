import { BridgeClient } from "@snibbilabs/sharding";

function requireEnvironment(name: string): string {
	const value = Bun.env[name];
	if (value === undefined || value.length === 0) throw new Error(`${name} is required.`);
	return value;
}

function requirePositiveInteger(name: string): number {
	const value = Number(requireEnvironment(name));
	if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer.`);
	return value;
}

const bridge = await new BridgeClient({
	hubUrl: requireEnvironment("SHARDING_HUB_URL"),
	id: requireEnvironment("SHARDING_BRIDGE_ID"),
	maxShards: requirePositiveInteger("SHARDING_MAX_SHARDS"),
	onError(error, context) {
		console.error(`Bridge background failure (${context}).`, error);
	},
	shardScript: requireEnvironment("SHARDING_SHARD_SCRIPT"),
	token: requireEnvironment("SHARDING_BRIDGE_TOKEN"),
}).start();

let stopping = false;

async function shutdown(signal: string): Promise<void> {
	if (stopping) return;
	stopping = true;
	console.info(`Stopping Bridge after ${signal}.`);
	await bridge.stop();
}

for (const signal of ["SIGINT", "SIGTERM"]) {
	process.once(signal, () => {
		void shutdown(signal).catch((cause: unknown) => {
			console.error("Bridge shutdown failed.", cause);
			process.exitCode = 1;
		});
	});
}
