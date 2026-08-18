import { describe, expect, test } from "bun:test";
import { $ } from "bun";
import { BridgeClient, HubClient } from "../../src/index";

const TOTAL_SHARDS = 1;
const SHARD_ID = 0;
const FIXTURE = `${import.meta.dir}/../fixtures/shard-process-pid.ts`;

interface FixtureReport {
	readonly pid: number;
	readonly processGeneration: number;
	readonly shardId: number;
	readonly totalShards: number;
}

interface ObservedEvent {
	readonly name: "shardFailed" | "shardReady" | "shardRestartScheduled";
	readonly shardId: number;
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
					session_start_limit: { max_concurrency: 1, remaining: 1_000, reset_after: 60_000, total: 1_000 },
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

async function waitFor(predicate: () => boolean, description: string, timeoutMs = 30_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() <= deadline) {
		if (predicate()) return;
		await Bun.sleep(25);
	}
	throw new Error(`Timed out waiting for ${description}.`);
}

async function collectReport(path: string, timeoutMs: number): Promise<FixtureReport> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() <= deadline) {
		const file = Bun.file(path);
		if (await file.exists()) {
			try {
				return (await file.json()) as FixtureReport;
			} catch {
				// A partially written report is retried until the deadline.
			}
		}
		await Bun.sleep(25);
	}
	throw new Error(`Shard process never wrote ${path}.`);
}

describe("shard subprocess crash recovery", () => {
	test("restarts a killed shard process under the declared policy without duplicating it", async () => {
		const workspace = `${import.meta.dir}/../../.tmp/shard-crash-${Bun.randomUUIDv7()}`;
		await Bun.write(`${workspace}/.keep`, "");
		const hubErrors: string[] = [];
		const bridgeErrors: string[] = [];
		const events: ObservedEvent[] = [];
		const count = (name: ObservedEvent["name"]): number => events.filter((event) => event.name === name).length;
		const hub = createHub(`${workspace}/hub.sqlite`, hubErrors);
		hub.events.on("shardFailed", (payload) => {
			events.push({ name: "shardFailed", shardId: payload.shardId });
		});
		hub.events.on("shardReady", (payload) => {
			events.push({ name: "shardReady", shardId: payload.shardId });
		});
		hub.events.on("shardRestartScheduled", (payload) => {
			events.push({ name: "shardRestartScheduled", shardId: payload.shardId });
		});
		let bridge: BridgeClient | undefined;
		try {
			await hub.start();
			const hubUrl = hub.url;
			if (hubUrl === null) throw new Error("Hub did not expose its listening URL.");
			bridge = new BridgeClient({
				analyticsPath: `${workspace}/bridge.sqlite`,
				env: { SHARDING_FIXTURE_OUTPUT: workspace },
				hubUrl: hubUrl.href,
				id: "bridge-crash",
				maxShards: TOTAL_SHARDS,
				onError: (error, context) => bridgeErrors.push(`${context}: ${error.message}`),
				restart: { initialDelayMs: 100, maxAttempts: 3, maxDelayMs: 500, windowMs: 60_000 },
				shardScript: FIXTURE,
				token: "bridge-token-0001",
			});
			await bridge.start();
			await bridge.waitUntilConnected(20_000);

			const first = await collectReport(`${workspace}/shard-${SHARD_ID}-generation-1.json`, 40_000);
			expect(first.shardId).toBe(SHARD_ID);
			expect(first.totalShards).toBe(TOTAL_SHARDS);
			expect(first.processGeneration).toBe(1);
			expect(Number.isSafeInteger(first.pid)).toBe(true);
			expect(first.pid).toBeGreaterThan(0);
			await waitFor(() => count("shardReady") >= 1, "the first shardReady event");

			process.kill(first.pid, "SIGKILL");

			await waitFor(() => count("shardFailed") >= 1, "the Hub to observe the crash");
			await waitFor(() => count("shardRestartScheduled") >= 1, "a policy-bounded restart to be scheduled");
			const failedIndex = events.findIndex((event) => event.name === "shardFailed");
			const scheduledIndex = events.findIndex((event) => event.name === "shardRestartScheduled");
			expect(failedIndex).toBeGreaterThanOrEqual(0);
			expect(events[failedIndex]?.shardId).toBe(SHARD_ID);
			expect(scheduledIndex).toBeGreaterThan(failedIndex);
			expect(events[scheduledIndex]?.shardId).toBe(SHARD_ID);

			await waitFor(() => count("shardReady") >= 2, "the restarted shard to become Discord-ready", 40_000);
			const second = await collectReport(`${workspace}/shard-${SHARD_ID}-generation-2.json`, 40_000);
			expect(second.shardId).toBe(SHARD_ID);
			expect(second.processGeneration).toBe(2);
			expect(second.pid).not.toBe(first.pid);

			expect(count("shardFailed")).toBe(1);
			expect(count("shardRestartScheduled")).toBe(1);
			expect(count("shardReady")).toBe(2);

			const topology = hub.getTopology();
			expect(topology.unassignedShardIds).toEqual([]);
			expect(topology.bridges[0]?.readyShardIds).toEqual([SHARD_ID]);

			expect(bridge.shards.size).toBe(1);
			const snapshot = bridge.shards.get(SHARD_ID);
			expect(snapshot?.state).toBe("ready");
			expect(snapshot?.processGeneration).toBe(2);
		} finally {
			await bridge?.stop();
			await hub.stop();
			await $`rm -rf ${workspace}`.quiet().nothrow();
		}
		expect(hubErrors).toEqual([]);
	}, 120_000);
});
