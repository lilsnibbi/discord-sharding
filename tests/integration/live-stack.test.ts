import { describe, expect, test } from "bun:test";
import { $ } from "bun";
import { BridgeClient, HubClient } from "../../src/index";
import type { $HubEventName, $ShardIdentity } from "../../src/index";

const token = process.env.TOKEN;
const TOTAL_SHARDS = 2;
const BRIDGE_ID = "bridge-live";
const FIXTURE = `${import.meta.dir}/../fixtures/live-shard-process.ts`;

interface LiveReport {
	readonly failure?: string;
	readonly gatewayReady: boolean;
	readonly identity: $ShardIdentity;
	readonly result?: { readonly echo?: { readonly from?: number }; readonly from?: number };
	readonly shardId: number;
	readonly target: number;
	readonly totalShards: number;
}

async function collectReports(directory: string, timeoutMs: number): Promise<Map<number, LiveReport>> {
	const reports = new Map<number, LiveReport>();
	const deadline = Date.now() + timeoutMs;
	while (Date.now() <= deadline) {
		for (let shardId = 0; shardId < TOTAL_SHARDS; shardId += 1) {
			if (reports.has(shardId)) continue;
			const file = Bun.file(`${directory}/live-shard-${shardId}.json`);
			if (!(await file.exists())) continue;
			reports.set(shardId, (await file.json()) as LiveReport);
		}
		if (reports.size === TOTAL_SHARDS) return reports;
		await Bun.sleep(50);
	}
	throw new Error(`Only ${reports.size} of ${TOTAL_SHARDS} live shard processes reported.`);
}

describe.skipIf(token === undefined || token.length === 0)("live full stack (real Discord gateway)", () => {
	test("boots Hub, Bridge, and real shard subprocesses against the live gateway", async () => {
		const workspace = `${import.meta.dir}/../../.tmp/live-stack-${Bun.randomUUIDv7()}`;
		await Bun.write(`${workspace}/.keep`, "");
		const hubErrors: string[] = [];
		const bridgeErrors: string[] = [];
		const observed: $HubEventName[] = [];
		if (token === undefined) throw new Error("TOKEN is required.");
		const hub = new HubClient({
			adminToken: "admin-token-0001",
			botToken: token,
			bridgeToken: "bridge-token-0001",
			databasePath: `${workspace}/hub.sqlite`,
			hostname: "127.0.0.1",
			onError: (error, context) => hubErrors.push(`${context}: ${error.message}`),
			port: 0,
			totalShards: TOTAL_SHARDS,
		});
		for (const event of [
			"bridgeConnected",
			"bridgeSynchronized",
			"shardAssigned",
			"shardReady",
		] as const satisfies readonly $HubEventName[]) {
			hub.events.on(event, () => {
				observed.push(event);
			});
		}
		let bridge: BridgeClient | undefined;
		try {
			await hub.start();
			const hubUrl = hub.url;
			if (hubUrl === null) throw new Error("Hub did not expose its listening URL.");
			bridge = new BridgeClient({
				analyticsPath: `${workspace}/bridge.sqlite`,
				env: { SHARDING_FIXTURE_OUTPUT: workspace },
				hubUrl: hubUrl.href,
				id: BRIDGE_ID,
				maxShards: TOTAL_SHARDS,
				onError: (error, context) => bridgeErrors.push(`${context}: ${error.message}`),
				shardScript: FIXTURE,
				startupTimeoutMs: 120_000,
				token: "bridge-token-0001",
			});
			await bridge.start();
			await bridge.waitUntilConnected(20_000);

			const reports = await collectReports(workspace, 120_000);
			for (const [shardId, report] of reports) {
				expect(report.failure).toBeUndefined();
				expect(report.gatewayReady).toBe(true);
				expect(report.totalShards).toBe(TOTAL_SHARDS);
				expect(report.result?.from).toBe(report.target);
				expect(report.result?.echo?.from).toBe(shardId);
				expect(report.identity.shardId).toBe(shardId);
				expect(report.identity.bridgeId).toBe(BRIDGE_ID);
				expect(report.identity.bridgeShardCount).toBe(TOTAL_SHARDS);
				expect(report.identity.totalBridges).toBe(1);
				expect(report.identity.bridges).toEqual([{ bridgeId: BRIDGE_ID, shardCount: TOTAL_SHARDS }]);
				expect(report.identity.instanceId).toContain(`${BRIDGE_ID}:${shardId}:`);
			}

			const topology = hub.getTopology();
			expect(topology.unassignedShardIds).toEqual([]);
			expect(topology.bridges[0]?.readyShardIds).toEqual([0, 1]);
			expect(observed).toContain("bridgeConnected");
			expect(observed).toContain("bridgeSynchronized");
			expect(observed.filter((event) => event === "shardAssigned")).toHaveLength(TOTAL_SHARDS);
			expect(observed.filter((event) => event === "shardReady")).toHaveLength(TOTAL_SHARDS);
		} finally {
			await bridge?.stop();
			await hub.stop();
			await $`rm -rf ${workspace}`.quiet().nothrow();
		}
		expect(bridgeErrors).toEqual([]);
		expect(hubErrors).toEqual([]);
	}, 240_000);
});
