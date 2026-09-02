import { describe, expect, test } from "bun:test";
import { $ } from "bun";
import { RedisHubPersistence } from "../../src/hub/redis/RedisHubPersistence";
import { BridgeClient, HubClient } from "../../src/index";
import { FakeRedisClient } from "../utilities/fake-redis";

const TOTAL_SHARDS = 2;
const FIXTURE = `${import.meta.dir}/../fixtures/shard-process.ts`;

interface FixtureReport {
	readonly failure?: string;
	readonly result?: { readonly echo?: { readonly from?: number }; readonly from?: number };
	readonly shardId: number;
	readonly target: number;
	readonly totalShards: number;
}

async function reserveFreePort(): Promise<number> {
	const server = Bun.serve({ fetch: () => new Response("reserved"), hostname: "127.0.0.1", port: 0 });
	const port: number | undefined = server.port;
	await server.stop(true);
	if (port === undefined) throw new Error("Could not reserve a free port.");
	return port;
}

function createHub(redis: FakeRedisClient, errors: string[], port = 0): HubClient {
	return new HubClient({
		adminToken: "admin-token-0001",
		botToken: "discord-token-01",
		bridgeToken: "bridge-token-0001",
		fetch: () =>
			Promise.resolve(
				Response.json({
					session_start_limit: { max_concurrency: 2, remaining: 1_000, reset_after: 60_000, total: 1_000 },
					shards: TOTAL_SHARDS,
					url: "wss://gateway.discord.gg",
				}),
			),
		hostname: "127.0.0.1",
		onError: (error, context) => errors.push(`${context}: ${error.message}`),
		persistence: new RedisHubPersistence("redis://127.0.0.1:6379", "hub", redis.connection()),
		port,
		totalShards: TOTAL_SHARDS,
	});
}

async function waitFor(predicate: () => boolean, description: string, timeoutMs = 30_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() <= deadline) {
		if (predicate()) return;
		await Bun.sleep(25);
	}
	throw new Error(`Timed out waiting for ${description}.`);
}

function shardGenerations(bridge: BridgeClient): string {
	return [...bridge.shards.values()]
		.map((entry) => `${entry.shardId}:${entry.state}:${entry.processGeneration}`)
		.sort()
		.join(" ");
}

async function collectReports(directory: string, timeoutMs: number): Promise<Map<number, FixtureReport>> {
	const reports = new Map<number, FixtureReport>();
	const deadline = Date.now() + timeoutMs;
	while (Date.now() <= deadline) {
		for (let shardId = 0; shardId < TOTAL_SHARDS; shardId += 1) {
			if (reports.has(shardId)) continue;
			const file = Bun.file(`${directory}/shard-${shardId}.json`);
			if (!(await file.exists())) continue;
			reports.set(shardId, (await file.json()) as FixtureReport);
		}
		if (reports.size === TOTAL_SHARDS) return reports;
		await Bun.sleep(25);
	}
	throw new Error(`Only ${reports.size} of ${TOTAL_SHARDS} shard processes reported a routing result.`);
}

describe("real Bun shard subprocesses", () => {
	test("spawns, identifies, routes between, and stops real shard processes", async () => {
		const workspace = `${import.meta.dir}/../../.tmp/shard-process-${Bun.randomUUIDv7()}`;
		await Bun.write(`${workspace}/.keep`, "");
		const hubErrors: string[] = [];
		const bridgeErrors: string[] = [];
		const hub = createHub(new FakeRedisClient(), hubErrors);
		let bridge: BridgeClient | undefined;
		try {
			await hub.start();
			const hubUrl = hub.url;
			if (hubUrl === null) throw new Error("Hub did not expose its listening URL.");
			bridge = new BridgeClient({
				analyticsPath: `${workspace}/bridge.sqlite`,
				env: { SHARDING_FIXTURE_OUTPUT: workspace },
				hubUrl: hubUrl.href,
				id: "bridge-subprocess",
				maxShards: TOTAL_SHARDS,
				onError: (error, context) => bridgeErrors.push(`${context}: ${error.message}`),
				shardScript: FIXTURE,
				token: "bridge-token-0001",
			});
			await bridge.start();
			await bridge.waitUntilConnected(20_000);

			const reports = await collectReports(workspace, 40_000);
			for (const [shardId, report] of reports) {
				expect(report.failure).toBeUndefined();
				expect(report.totalShards).toBe(TOTAL_SHARDS);
				expect(report.target).toBe((shardId + 1) % TOTAL_SHARDS);
				expect(report.result?.from).toBe(report.target);
				expect(report.result?.echo?.from).toBe(shardId);
			}

			const topology = hub.getTopology();
			expect(topology.unassignedShardIds).toEqual([]);
			expect(topology.bridges[0]?.readyShardIds).toEqual([0, 1]);
			expect([...bridge.shards.values()].every((entry) => entry.processGeneration === 1)).toBe(true);
		} finally {
			await bridge?.stop();
			await hub.stop();
			await $`rm -rf ${workspace}`.quiet().nothrow();
		}
		expect(bridgeErrors).toEqual([]);
		expect(hubErrors).toEqual([]);
	}, 90_000);
	test("keeps shard processes alive across a Hub outage and resumes without restarting them", async () => {
		const workspace = `${import.meta.dir}/../../.tmp/hub-outage-${Bun.randomUUIDv7()}`;
		await Bun.write(`${workspace}/.keep`, "");
		const port = await reserveFreePort();
		const redis = new FakeRedisClient();
		const hubErrors: string[] = [];
		const bridgeErrors: string[] = [];
		let hub = createHub(redis, hubErrors, port);
		let bridge: BridgeClient | undefined;
		try {
			await hub.start();
			bridge = new BridgeClient({
				analyticsPath: `${workspace}/bridge.sqlite`,
				env: { SHARDING_FIXTURE_OUTPUT: workspace },
				hubUrl: `http://127.0.0.1:${port}`,
				id: "bridge-outage",
				maxShards: TOTAL_SHARDS,
				onError: (error, context) => bridgeErrors.push(`${context}: ${error.message}`),
				reconnect: { initialDelayMs: 100, jitterRatio: 0, maxDelayMs: 500, multiplier: 2 },
				shardScript: FIXTURE,
				token: "bridge-token-0001",
			});
			await bridge.start();
			await bridge.waitUntilConnected(20_000);
			await waitFor(
				() =>
					bridge?.shards.size === TOTAL_SHARDS && [...bridge.shards.values()].every((entry) => entry.state === "ready"),
				"every shard Discord-ready",
			);
			const before = shardGenerations(bridge);

			await hub.stop();
			await waitFor(() => bridge?.isInMaintenance === true, "maintenance after Hub loss");
			await Bun.sleep(500);
			expect(bridge.shards.size).toBe(TOTAL_SHARDS);

			hub = createHub(redis, hubErrors, port);
			await hub.start();
			await waitFor(() => bridge?.isInMaintenance === false, "maintenance cleared after the Hub returned");
			await waitFor(
				() => hub.getTopology().bridges.some((entry) => entry.readyShardIds.length === TOTAL_SHARDS),
				"every shard Discord-ready again",
			);
			expect(shardGenerations(bridge)).toBe(before);
		} finally {
			await bridge?.stop();
			await hub.stop();
			await $`rm -rf ${workspace}`.quiet().nothrow();
		}
		expect(hubErrors).toEqual([]);
	}, 90_000);
});
