import { HubClient } from "@lilsnibbi/discord-sharding";

function requireEnvironment(name: string): string {
	const value = Bun.env[name];
	if (value === undefined || value.length === 0) throw new Error(`${name} is required.`);
	return value;
}

const hub = await new HubClient({
	adminToken: requireEnvironment("SHARDING_ADMIN_TOKEN"),
	botToken: requireEnvironment("DISCORD_BOT_TOKEN"),
	bridgeToken: requireEnvironment("SHARDING_BRIDGE_TOKEN"),
	databasePath: Bun.env.SHARDING_DATABASE_PATH ?? "./sharding-hub.sqlite",
	hostname: Bun.env.SHARDING_HUB_HOST ?? "0.0.0.0",
	onError(error, context) {
		console.error(`Hub background failure (${context}).`, error);
	},
	port: Bun.env.SHARDING_HUB_PORT === undefined ? 3000 : requirePositiveInteger("SHARDING_HUB_PORT"),
}).start();

let stopping = false;

async function shutdown(signal: string): Promise<void> {
	if (stopping) return;
	stopping = true;
	console.info(`Stopping Hub after ${signal}.`);
	await hub.stop();
}

function requirePositiveInteger(name: string): number {
	const value = Number(requireEnvironment(name));
	if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer.`);
	return value;
}

for (const signal of ["SIGINT", "SIGTERM"]) {
	process.once(signal, () => {
		void shutdown(signal).catch((cause: unknown) => {
			console.error("Hub shutdown failed.", cause);
			process.exitCode = 1;
		});
	});
}
