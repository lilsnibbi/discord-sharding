import { describe, expect, test } from "bun:test";
import type { WireDataMap } from "../../src/protocol/types";
import {
	BRIDGE_GENERATION,
	BRIDGE_ID,
	createHub,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	persistedBridge,
	sentMessages,
	synchronizeBridge,
	waitFor,
} from "./client-harness";

describe("HubClient routing and evaluation", () => {
	test("routes same-Bridge requests through the Hub and aggregates evaluations", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1_000_000 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 1_000_000 },
			],
			bridges: [persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 2, 1_000_000)],
		});
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				maxShards: 2,
				runningShards: [
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 },
				],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const routeStart = bridge.socket.sent.length;
			bridge.send("bridge.route.request", "route:1", {
				assignmentEpoch: 1,
				kind: "request",
				payload: { ping: true },
				processGeneration: 1,
				shardId: 0,
				targetShardId: 1,
			});
			const routed = await bridge.waitForMessage("hub.route.request", routeStart);
			expect(routed.data).toMatchObject({ sourceShardId: 0, target: { shardId: 1 } });
			bridge.send("bridge.route.response", "route:1", {
				assignmentEpoch: 1,
				ok: true,
				processGeneration: 1,
				shardId: 1,
				sourceShardId: 0,
				value: { pong: true },
			});
			const routeResponse = await bridge.waitForMessage("hub.route.response", routeStart);
			expect(routeResponse.data).toMatchObject({ ok: true, sourceShardId: 0, value: { pong: true } });

			const evaluationStart = bridge.socket.sent.length;
			bridge.send("bridge.eval.request", "eval:1", {
				assignmentEpoch: 1,
				context: { input: 2 },
				evaluator: "(context) => context.input * 2",
				processGeneration: 1,
				shardId: 0,
			});
			const preparations = await bridge.waitForMessages("hub.eval.prepare", 2, evaluationStart);
			for (const preparation of preparations) {
				const target = preparation.data.target;
				if (typeof target !== "object" || target === null) throw new Error("Evaluation target is missing.");
				const shardId = Reflect.get(target, "shardId");
				if (typeof shardId !== "number") throw new Error("Evaluation target shard is missing.");
				bridge.send("bridge.eval.prepared", "eval:1", {
					assignmentEpoch: 1,
					ok: true,
					processGeneration: 1,
					shardId,
				});
			}
			const commits = await bridge.waitForMessages("hub.eval.commit", 2, evaluationStart);
			for (const commit of commits) {
				expect(commit.data.executeAt).toBeLessThan(1_000_000);
				const target = commit.data.target;
				if (typeof target !== "object" || target === null) throw new Error("Evaluation target is missing.");
				const shardId = Reflect.get(target, "shardId");
				if (typeof shardId !== "number") throw new Error("Evaluation target shard is missing.");
				bridge.send("bridge.eval.result", "eval:1", {
					assignmentEpoch: 1,
					ok: true,
					processGeneration: 1,
					shardId,
					value: shardId + 10,
				});
			}
			const result = await bridge.waitForMessage("hub.eval.response", evaluationStart);
			expect(result.data).toEqual({
				ok: true,
				results: [
					{ shardId: 0, value: 10 },
					{ shardId: 1, value: 11 },
				],
				sourceShardId: 0,
			});
		} finally {
			await hub.stop();
			installed.restore();
		}
	});

	test("does not let duplicate route identifiers replace the original request", async () => {
		const installed = installFakeServe();
		const persistence = new MemoryHubPersistence({
			assignments: [
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 0, updatedAt: 1 },
				{ bridgeId: BRIDGE_ID, epoch: 1, shardId: 1, updatedAt: 2 },
			],
			bridges: [persistedBridge(BRIDGE_ID, BRIDGE_GENERATION, 2)],
		});
		const hub = createHub(persistence, { totalShards: 2 });
		try {
			await hub.start();
			const bridge = await openBridge(installed.server);
			await synchronizeBridge(bridge, {
				maxShards: 2,
				runningShards: [
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 0 },
					{ assignmentEpoch: 1, processGeneration: 1, ready: true, shardId: 1 },
				],
			});
			await waitFor(() => hub.getTopology().bridges[0]?.readyShardIds.length === 2, "ready shard topology");

			const startIndex = bridge.socket.sent.length;
			const routeData: WireDataMap["bridge.route.request"] = {
				assignmentEpoch: 1,
				kind: "request",
				payload: { sequence: 1 },
				processGeneration: 1,
				shardId: 0,
				targetShardId: 1,
			};
			bridge.send("bridge.route.request", "route:duplicate", routeData);
			await bridge.waitForMessage("hub.route.request", startIndex);
			bridge.send("bridge.route.request", "route:duplicate", routeData);
			await bridge.waitForMessage("hub.route.response", startIndex);
			bridge.send("bridge.route.request", "route:duplicate", {
				...routeData,
				shardId: 1,
				targetShardId: 0,
			});
			const failures = await bridge.waitForMessages("hub.route.response", 2, startIndex);
			expect(failures.every((response) => response.data.ok === false)).toBe(true);
			expect(
				sentMessages(bridge.socket, startIndex).filter((message) => message.type === "hub.route.request"),
			).toHaveLength(1);

			bridge.send("bridge.route.response", "route:duplicate", {
				assignmentEpoch: 1,
				ok: true,
				processGeneration: 1,
				shardId: 1,
				sourceShardId: 0,
				value: { completed: true },
			});
			const responses = await bridge.waitForMessages("hub.route.response", 3, startIndex);
			expect(responses.filter((response) => response.data.ok === true)).toHaveLength(1);
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);

			const evaluationStart = bridge.socket.sent.length;
			const evaluationData: WireDataMap["bridge.eval.request"] = {
				assignmentEpoch: 1,
				context: { sequence: 1 },
				evaluator: "(context) => context.sequence",
				processGeneration: 1,
				shardId: 0,
			};
			bridge.send("bridge.eval.request", "eval:duplicate", evaluationData);
			const preparations = await bridge.waitForMessages("hub.eval.prepare", 2, evaluationStart);
			bridge.send("bridge.eval.request", "eval:duplicate", evaluationData);
			await bridge.waitForMessage("hub.eval.response", evaluationStart);
			bridge.send("bridge.eval.request", "eval:duplicate", { ...evaluationData, shardId: 1 });
			const evaluationFailures = await bridge.waitForMessages("hub.eval.response", 2, evaluationStart);
			expect(evaluationFailures.every((response) => response.data.ok === false)).toBe(true);
			expect(
				sentMessages(bridge.socket, evaluationStart).filter((message) => message.type === "hub.eval.prepare"),
			).toHaveLength(2);
			expect(
				sentMessages(bridge.socket, evaluationStart).filter((message) => message.type === "hub.eval.cancel"),
			).toHaveLength(0);
			for (const preparation of preparations) {
				const target = preparation.data.target;
				if (typeof target !== "object" || target === null) throw new Error("Evaluation target is missing.");
				const shardId = Reflect.get(target, "shardId");
				if (typeof shardId !== "number") throw new Error("Evaluation target shard is missing.");
				bridge.send("bridge.eval.prepared", "eval:duplicate", {
					assignmentEpoch: 1,
					ok: true,
					processGeneration: 1,
					shardId,
				});
			}
			const commits = await bridge.waitForMessages("hub.eval.commit", 2, evaluationStart);
			for (const commit of commits) {
				const target = commit.data.target;
				if (typeof target !== "object" || target === null) throw new Error("Evaluation target is missing.");
				const shardId = Reflect.get(target, "shardId");
				if (typeof shardId !== "number") throw new Error("Evaluation target shard is missing.");
				bridge.send("bridge.eval.result", "eval:duplicate", {
					assignmentEpoch: 1,
					ok: true,
					processGeneration: 1,
					shardId,
					value: shardId,
				});
			}
			const evaluationResponses = await bridge.waitForMessages("hub.eval.response", 3, evaluationStart);
			expect(evaluationResponses.filter((response) => response.data.ok === true)).toHaveLength(1);
			expect(bridge.socket.readyState).toBe(WebSocket.OPEN);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});
});
