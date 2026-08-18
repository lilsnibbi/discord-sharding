import { describe, expect, test } from "bun:test";
import { $ } from "bun";
import { BridgeClient, HubClient } from "../../src/index";

const TOTAL_SHARDS = 2;
const FIXTURE = `${import.meta.dir}/../fixtures/shard-process.ts`;

interface FixtureReport {
	readonly failure?: string;
	readonly result?: { readonly echo?: { readonly from?: number }; readonly from?: number };
	readonly shardId: number;
	readonly target: number;
	readonly totalShards: number;
}

function createHub(databasePath: string, errors: string[]): HubClient {
	return new HubClient({
		adminToken: "admin-token-0001",
		botToken: "discord-token-01",
		bridgeToken: "bridge-token-0001",
		databasePath,
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
		port: 0,
		totalShards: TOTAL_SHARDS,
	});
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
		const hub = createHub(`${workspace}/hub.sqlite`, hubErrors);
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
});
