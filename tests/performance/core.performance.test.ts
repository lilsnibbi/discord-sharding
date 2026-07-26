import { describe, expect, test } from "bun:test";
import { planNextAssignment } from "../../src/hub/assignment";
import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { createWireMessage, encodeWireMessage, parseWireMessage } from "../../src/protocol/codec";
import { SHARD_TO_BRIDGE_TYPES } from "../../src/protocol/types";
import type { $PersistedAssignment } from "../../src/types/hub";

const PERFORMANCE_BUDGET_MS = 3_000;

describe("core performance budgets", () => {
	test("plans a ten-thousand-shard topology within a bounded budget", () => {
		const assignments: $PersistedAssignment[] = [];
		for (let shardId = 0; shardId < 10_000; shardId += 1) {
			assignments.push({
				bridgeId: `bridge-${shardId % 20}`,
				epoch: 1,
				shardId,
				updatedAt: 1,
			});
		}
		const bridges = Array.from({ length: 20 }, (_value, index) => ({
			connected: true,
			id: `bridge-${index}`,
			maxShards: 1_000,
		}));

		const startedAt = performance.now();
		const plan = planNextAssignment({
			assignments,
			bridges,
			totalShards: 10_000,
		});
		const elapsed = performance.now() - startedAt;

		expect(plan.nextStep).toBeNull();
		expect(plan.targets).toHaveLength(20);
		expect(elapsed).toBeLessThan(PERFORMANCE_BUDGET_MS);
	});

	test("round-trips two thousand bounded protocol messages within a bounded budget", () => {
		const message = createWireMessage(
			"shard.route.request",
			"performance-route",
			{
				kind: "message",
				payload: { content: "x".repeat(512), sequence: 1 },
				targetShardId: 1,
			},
			DEFAULT_PAYLOAD_POLICY,
		);
		const encoded = encodeWireMessage(message, DEFAULT_PAYLOAD_POLICY);

		const startedAt = performance.now();
		let parsedType = "";
		for (let index = 0; index < 2_000; index += 1) {
			parsedType = parseWireMessage(encoded, SHARD_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY).type;
		}
		const elapsed = performance.now() - startedAt;

		expect(parsedType).toBe("shard.route.request");
		expect(elapsed).toBeLessThan(PERFORMANCE_BUDGET_MS);
	});
});
