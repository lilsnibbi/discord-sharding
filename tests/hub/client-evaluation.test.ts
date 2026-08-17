import { describe, expect, test } from "bun:test";
import type { WireDataMap } from "../../src/protocol/types";
import {
	BRIDGE_GENERATION,
	BRIDGE_ID,
	type BridgeHarness,
	createHub,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	persistedBridge,
	readErrorField,
	readTargetShardId,
	sentMessages,
	synchronizeBridge,
	waitFor,
} from "./client-harness";

const EVALUATION: WireDataMap["bridge.eval.request"] = {
	assignmentEpoch: 1,
	context: { input: 2 },
	evaluator: "(context) => context.input",
	processGeneration: 1,
	shardId: 0,
};

const READY_SHARDS = [
	{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 },
	{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 },
] as const;

function twoShardPersistence(): MemoryHubPersistence {
	return new MemoryHubPersistence({
		assignments: [
			{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
			{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 1 },
		],
		bridges: [persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 2)],
	});
}

async function readyBridge(server: Parameters<typeof openBridge>[0]): Promise<BridgeHarness> {
	const bridge = await openBridge(server);
	await synchronizeBridge(bridge, { maxShards: 2, runningShards: [...READY_SHARDS] });
	return bridge;
}

describe("HubClient evaluation failures", () => {
	test("rejects an evaluation once the Hub evaluation capacity is reached", async () => {
		const installed = installFakeServe();
		const hub = createHub(twoShardPersistence(), { maxEvaluations: 1, totalShards: 2 });
		try {
			await hub.start();
			const bridge = await readyBridge(installed.server);
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.eval.request", "eval:first", EVALUATION);
			await bridge.waitForMessages("hub.eval.prepare", 2, startIndex);
			bridge.send("bridge.eval.request", "eval:second", EVALUATION);

			const rejected = await bridge.waitForMessage("hub.eval.response", startIndex);
			expect(rejected.id).toBe("eval:second");
			expect(rejected.data.ok).toBe(false);
			expect(readErrorField(rejected, "code")).toBe("CAPACITY");
			expect(readErrorField(rejected, "message")).toContain("evaluation capacity of 1");
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("cancels every target when one shard fails to prepare", async () => {
		const installed = installFakeServe();
		const hub = createHub(twoShardPersistence(), { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await readyBridge(installed.server);
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.eval.request", "eval:prepare-failure", EVALUATION);
			await bridge.waitForMessages("hub.eval.prepare", 2, startIndex);
			bridge.send("bridge.eval.prepared", "eval:prepare-failure", {
				assignmentEpoch: 1,
				ok: true,
				processGeneration: 1,
				shardId: 0,
			});
			bridge.send("bridge.eval.prepared", "eval:prepare-failure", {
				assignmentEpoch: 1,
				error: { code: "STATE", message: "Shard is unavailable.", name: "ShardingStateError" },
				ok: false,
				processGeneration: 1,
				shardId: 1,
			});

			const cancellations = await bridge.waitForMessages("hub.eval.cancel", 2, startIndex);
			const failure = await bridge.waitForMessage("hub.eval.response", startIndex);

			expect(cancellations.map((message) => readTargetShardId(message)).sort()).toEqual([0, 1]);
			expect(failure.data.ok).toBe(false);
			expect(readErrorField(failure, "message")).toBe("Shard is unavailable.");
			expect(sentMessages(bridge.socket, startIndex).filter((message) => message.type === "hub.eval.commit")).toEqual(
				[],
			);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("fails an evaluation whose result omits its value", async () => {
		const installed = installFakeServe();
		const hub = createHub(twoShardPersistence(), { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await readyBridge(installed.server);
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.eval.request", "eval:missing-value", EVALUATION);
			const preparations = await bridge.waitForMessages("hub.eval.prepare", 2, startIndex);
			for (const preparation of preparations) {
				bridge.send("bridge.eval.prepared", "eval:missing-value", {
					assignmentEpoch: 1,
					ok: true,
					processGeneration: 1,
					shardId: readTargetShardId(preparation),
				});
			}
			await bridge.waitForMessages("hub.eval.commit", 2, startIndex);
			bridge.send("bridge.eval.result", "eval:missing-value", {
				assignmentEpoch: 1,
				ok: true,
				processGeneration: 1,
				shardId: 0,
			});

			const failure = await bridge.waitForMessage("hub.eval.response", startIndex);
			expect(failure.data.ok).toBe(false);
			expect(readErrorField(failure, "code")).toBe("PROTOCOL");
			expect(readErrorField(failure, "message")).toContain("did not include a value");
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("expires an evaluation that never finishes preparing", async () => {
		const installed = installFakeServe();
		const hub = createHub(twoShardPersistence(), { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await readyBridge(installed.server);
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.eval.request", "eval:expired", EVALUATION);
			await bridge.waitForMessages("hub.eval.prepare", 2, startIndex);

			const cancellations = await bridge.waitForMessages("hub.eval.cancel", 2, startIndex);
			const expired = await bridge.waitForMessage("hub.eval.response", startIndex);

			expect(cancellations).toHaveLength(2);
			expect(expired.data.ok).toBe(false);
			expect(readErrorField(expired, "code")).toBe("TIMEOUT");
			expect(readErrorField(expired, "message")).toContain("eval:expired");
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("fails an evaluation whose commit time leaves the supported range", async () => {
		const installed = installFakeServe();
		let timestamp = Number.MAX_SAFE_INTEGER - 1_000;
		const hub = createHub(twoShardPersistence(), {
			evaluationCommitLeadMs: 2_147_483_647,
			totalShards: 2,
			wallClock: () => {
				timestamp += 1;
				return timestamp;
			},
		});
		try {
			await hub.start();
			const bridge = await readyBridge(installed.server);
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.eval.request", "eval:overflow", EVALUATION);
			const preparations = await bridge.waitForMessages("hub.eval.prepare", 2, startIndex);
			for (const preparation of preparations) {
				bridge.send("bridge.eval.prepared", "eval:overflow", {
					assignmentEpoch: 1,
					ok: true,
					processGeneration: 1,
					shardId: readTargetShardId(preparation),
				});
			}

			const failure = await bridge.waitForMessage("hub.eval.response", startIndex);
			expect(failure.data.ok).toBe(false);
			expect(readErrorField(failure, "message")).toContain("commit time exceeded the supported range");
			expect(sentMessages(bridge.socket, startIndex).filter((message) => message.type === "hub.eval.commit")).toEqual(
				[],
			);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test.each([
		[
			"prepares the same target twice",
			(bridge: BridgeHarness): void => {
				for (let attempt = 0; attempt < 2; attempt += 1) {
					bridge.send("bridge.eval.prepared", "eval:protocol", {
						assignmentEpoch: 1,
						ok: true,
						processGeneration: 1,
						shardId: 0,
					});
				}
			},
		],
		[
			"returns a result before the commit",
			(bridge: BridgeHarness): void => {
				bridge.send("bridge.eval.result", "eval:protocol", {
					assignmentEpoch: 1,
					ok: true,
					processGeneration: 1,
					shardId: 0,
					value: 1,
				});
			},
		],
	])("closes a Bridge that %s", async (_description, act) => {
		const installed = installFakeServe();
		const hub = createHub(twoShardPersistence(), { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await readyBridge(installed.server);
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const startIndex = bridge.socket.sent.length;
			bridge.send("bridge.eval.request", "eval:protocol", EVALUATION);
			await bridge.waitForMessages("hub.eval.prepare", 2, startIndex);
			act(bridge);

			await waitFor(() => bridge.socket.closeCode !== undefined, "evaluation protocol close");
			expect(bridge.socket.closeCode).toBe(1002);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});
});
