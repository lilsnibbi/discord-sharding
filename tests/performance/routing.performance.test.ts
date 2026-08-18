import { describe, expect, test } from "bun:test";
import type { BridgeClient, HubClient } from "../../src/index";
import {
	createBridge,
	createHub,
	EmbeddedShardFleet,
	type ReportedError,
	requireClient,
	requireHubUrl,
	sortedReadyTopology,
	stopAll,
	waitFor,
} from "../utilities/runtime-stack";

const PERFORMANCE_BUDGET_MS = 3_000;
const TOTAL_REQUESTS = 200;
const WAVE_SIZE = 50;

interface RunningStack {
	readonly bridgeA: BridgeClient;
	readonly bridgeB: BridgeClient;
	readonly fleetA: EmbeddedShardFleet;
	readonly fleetB: EmbeddedShardFleet;
	readonly hub: HubClient;
	readonly hubErrors: ReportedError[];
}

async function startStack(): Promise<RunningStack> {
	const hubErrors: ReportedError[] = [];
	const bridgeErrors: ReportedError[] = [];
	const hub = createHub(4, hubErrors);
	const fleetA = new EmbeddedShardFleet(() => true);
	const fleetB = new EmbeddedShardFleet(() => true);
	await hub.start();
	const hubUrl = requireHubUrl(hub);
	const bridgeA = createBridge(hubUrl, "bridge-perf-a", 2, fleetA, bridgeErrors);
	await bridgeA.start();
	await bridgeA.waitUntilConnected(3_000);
	await waitFor(() => fleetA.clients.size === 2, "the first Bridge assignments");
	const bridgeB = createBridge(hubUrl, "bridge-perf-b", 2, fleetB, bridgeErrors);
	await bridgeB.start();
	await bridgeB.waitUntilConnected(3_000);
	await waitFor(() => sortedReadyTopology(hub).length === 4, "four Discord-ready shards");
	return { bridgeA, bridgeB, fleetA, fleetB, hub, hubErrors };
}

async function measureRequestWaves(
	source: ReturnType<typeof requireClient>,
	targetId: number,
	label: string,
): Promise<number> {
	const startedAt = performance.now();
	for (let wave = 0; wave * WAVE_SIZE < TOTAL_REQUESTS; wave += 1) {
		const responses = await Promise.all(
			Array.from({ length: WAVE_SIZE }, (_value, index) =>
				source.request<{ readonly action: string; readonly shardId: number }>(targetId, {
					action: `${label}-${wave}-${index}`,
				}),
			),
		);
		for (let index = 0; index < responses.length; index += 1) {
			expect(responses[index]).toEqual({
				action: `${label}-${wave}-${index}`,
				shardId: targetId,
			});
		}
	}
	return performance.now() - startedAt;
}

describe("routing performance budgets", () => {
	test("routes two hundred same-Bridge requests within a bounded budget", async () => {
		const stack = await startStack();
		try {
			const [sourceId, targetId] = stack.fleetA.readyShardIds();
			if (sourceId === undefined || targetId === undefined) {
				throw new Error("The first Bridge must own two ready shards.");
			}
			const source = requireClient(stack.fleetA, sourceId);

			const elapsed = await measureRequestWaves(source, targetId, "same-bridge");

			expect(stack.fleetA.requestActions).toHaveLength(TOTAL_REQUESTS);
			expect(stack.hubErrors).toEqual([]);
			expect(elapsed).toBeLessThan(PERFORMANCE_BUDGET_MS);
		} finally {
			await stopAll(stack.bridgeB, stack.bridgeA, stack.hub);
		}
	}, 15_000);

	test("routes two hundred cross-Bridge requests within a bounded budget", async () => {
		const stack = await startStack();
		try {
			const sourceId = stack.fleetA.readyShardIds()[0];
			const targetId = stack.fleetB.readyShardIds()[0];
			if (sourceId === undefined || targetId === undefined) {
				throw new Error("Both Bridges must own at least one ready shard.");
			}
			const source = requireClient(stack.fleetA, sourceId);

			const elapsed = await measureRequestWaves(source, targetId, "cross-bridge");

			expect(stack.fleetB.requestActions).toHaveLength(TOTAL_REQUESTS);
			expect(stack.hubErrors).toEqual([]);
			expect(elapsed).toBeLessThan(PERFORMANCE_BUDGET_MS);
		} finally {
			await stopAll(stack.bridgeB, stack.bridgeA, stack.hub);
		}
	}, 15_000);
});
