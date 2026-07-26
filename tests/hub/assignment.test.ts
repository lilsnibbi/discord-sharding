import { describe, expect, test } from "bun:test";
import { ShardingConfigurationError } from "../../src/errors/ShardingError";
import { planNextAssignment } from "../../src/hub/assignment";
import type { $PersistedAssignment } from "../../src/types/hub";

function assignment(shardId: number, bridgeId: string, epoch = shardId + 1): $PersistedAssignment {
	return Object.freeze({ shardId, bridgeId, epoch, updatedAt: 1 });
}

describe("planNextAssignment", () => {
	test("preserves disconnected assignments while filling connected capacity", () => {
		const plan = planNextAssignment({
			totalShards: 5,
			bridges: [
				{ id: "connected", connected: true, maxShards: 3 },
				{ id: "offline", connected: false, maxShards: 4 },
			],
			assignments: [assignment(0, "offline"), assignment(1, "connected")],
		});

		expect(plan.reservedShardIds).toEqual([0]);
		expect(plan.unassignedShardIds).toEqual([2, 3, 4]);
		expect(plan.targets).toEqual([{ bridgeId: "connected", currentCount: 1, targetCount: 3 }]);
		expect(plan.nextStep).toEqual({
			kind: "assign",
			shardId: 2,
			toBridgeId: "connected",
			nextEpoch: 1,
		});
	});

	test("keeps an already balanced sticky allocation unchanged", () => {
		const assignments = [
			assignment(0, "alpha"),
			assignment(1, "alpha"),
			assignment(2, "beta"),
			assignment(3, "beta"),
			assignment(4, "beta"),
		];

		const plan = planNextAssignment({
			totalShards: 5,
			bridges: [
				{ id: "alpha", connected: true, maxShards: 5 },
				{ id: "beta", connected: true, maxShards: 5 },
			],
			assignments,
		});

		expect(plan.targets).toEqual([
			{ bridgeId: "alpha", currentCount: 2, targetCount: 2 },
			{ bridgeId: "beta", currentCount: 3, targetCount: 3 },
		]);
		expect(plan.nextStep).toBeNull();
		expect(assignments.map((entry) => entry.bridgeId)).toEqual(["alpha", "alpha", "beta", "beta", "beta"]);
		expect(Object.isFrozen(plan)).toBe(true);
	});

	test("returns exactly one fenced transfer after capacity is lowered", () => {
		const plan = planNextAssignment({
			totalShards: 4,
			bridges: [
				{ id: "alpha", connected: true, maxShards: 1 },
				{ id: "beta", connected: true, maxShards: 3 },
			],
			assignments: [
				assignment(0, "alpha", 4),
				assignment(1, "alpha", 5),
				assignment(2, "alpha", 6),
				assignment(3, "beta", 7),
			],
		});

		expect(plan.targets).toEqual([
			{ bridgeId: "alpha", currentCount: 3, targetCount: 1 },
			{ bridgeId: "beta", currentCount: 1, targetCount: 3 },
		]);
		expect(plan.nextStep).toEqual({
			kind: "transfer",
			shardId: 2,
			fromBridgeId: "alpha",
			toBridgeId: "beta",
			currentEpoch: 6,
			nextEpoch: 7,
		});
	});

	test("stops one excess shard when no destination has capacity", () => {
		const plan = planNextAssignment({
			totalShards: 2,
			bridges: [{ id: "alpha", connected: true, maxShards: 0 }],
			assignments: [assignment(0, "alpha"), assignment(1, "alpha")],
		});

		expect(plan.nextStep).toEqual({
			kind: "unassign",
			shardId: 1,
			fromBridgeId: "alpha",
			currentEpoch: 2,
		});
	});

	test("rejects duplicate shard and Bridge records", () => {
		expect(() =>
			planNextAssignment({
				totalShards: 1,
				bridges: [
					{ id: "same", connected: true, maxShards: 1 },
					{ id: "same", connected: false, maxShards: 1 },
				],
				assignments: [],
			}),
		).toThrow(ShardingConfigurationError);
		expect(() =>
			planNextAssignment({
				totalShards: 1,
				bridges: [{ id: "alpha", connected: true, maxShards: 1 }],
				assignments: [assignment(0, "alpha"), assignment(0, "alpha")],
			}),
		).toThrow(ShardingConfigurationError);
	});
});
