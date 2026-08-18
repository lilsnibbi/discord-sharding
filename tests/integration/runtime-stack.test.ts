import { describe, expect, test } from "bun:test";
import { ShardingTimeoutError } from "../../src/errors/ShardingError";
import type { $BridgeSocketFactory, BridgeClient } from "../../src/index";
import {
	createBridge,
	createHub,
	EmbeddedShardFleet,
	nativeSocket,
	type ReportedError,
	readAction,
	requireClient,
	requireHubUrl,
	sortedReadyTopology,
	stopAll,
	UNREADY_SHARD_ID,
	waitFor,
} from "../utilities/runtime-stack";

describe("real Hub, Bridge, and embedded shard stack", () => {
	test("routes across Bridges and isolates readiness, serialization, and caller expiry", async () => {
		const hubErrors: ReportedError[] = [];
		const bridgeErrors: ReportedError[] = [];
		const hub = createHub(4, hubErrors);
		const fleetA = new EmbeddedShardFleet((shardId) => shardId !== UNREADY_SHARD_ID);
		const fleetB = new EmbeddedShardFleet((shardId) => shardId !== UNREADY_SHARD_ID);
		let bridgeA: BridgeClient | undefined;
		let bridgeB: BridgeClient | undefined;
		try {
			await hub.start();
			const hubUrl = requireHubUrl(hub);
			bridgeA = createBridge(hubUrl, "bridge-stack-a", 2, fleetA, bridgeErrors);
			await bridgeA.start();
			await bridgeA.waitUntilConnected(3_000);
			await waitFor(() => fleetA.clients.size === 2, "the first Bridge assignments");

			bridgeB = createBridge(hubUrl, "bridge-stack-b", 2, fleetB, bridgeErrors);
			await bridgeB.start();
			await bridgeB.waitUntilConnected(3_000);
			await waitFor(() => fleetA.clients.size + fleetB.clients.size === 4, "all embedded shard processes");
			await waitFor(() => sortedReadyTopology(hub).length === 3, "three Discord-ready shards");

			const sourceId = fleetA.readyShardIds()[0];
			const targetId = fleetB.readyShardIds()[0];
			if (sourceId === undefined || targetId === undefined) {
				throw new Error("Both Bridges must own at least one ready shard.");
			}
			const source = requireClient(fleetA, sourceId);
			const target = requireClient(fleetB, targetId);

			await source.send(targetId, { action: "message", value: 7 });
			await waitFor(
				() =>
					fleetB.messages.some(
						(message) =>
							message.shardId === targetId &&
							message.sourceShardId === sourceId &&
							readAction(message.payload) === "message",
					),
				"cross-Bridge message delivery",
			);

			expect(
				await source.request<{ readonly action: string; readonly shardId: number }>(targetId, {
					action: "request",
				}),
			).toEqual({
				action: "request",
				shardId: targetId,
			});

			const requestsBeforeInvalidInput = fleetB.requestActions.length;
			await expect(source.request(targetId, { unsupported: 1n })).rejects.toThrow();
			expect(fleetB.requestActions).toHaveLength(requestsBeforeInvalidInput);

			await expect(source.request(targetId, { action: "invalid-result" })).rejects.toThrow();
			expect(
				await source.request<{ readonly action: string; readonly shardId: number }>(targetId, {
					action: "after-invalid-result",
				}),
			).toEqual({
				action: "after-invalid-result",
				shardId: targetId,
			});

			const slowRequest = source.request(targetId, { action: "slow" }, 20);
			await fleetB.slowStarted.promise;
			await expect(slowRequest).rejects.toBeInstanceOf(ShardingTimeoutError);
			expect(target.activeHandlers).toBe(1);
			fleetB.slowRelease.resolve();
			await waitFor(
				() => fleetB.slowCompletions === 1 && target.activeHandlers === 0,
				"destination developer code after caller expiry",
			);
			expect(
				await source.request<{ readonly action: string; readonly shardId: number }>(targetId, {
					action: "after-timeout",
				}),
			).toEqual({
				action: "after-timeout",
				shardId: targetId,
			});

			const readyBeforeCrash = [...fleetA.readyShardIds(), ...fleetB.readyShardIds()];
			const deadShardId = readyBeforeCrash.find((shardId) => shardId !== sourceId && shardId !== targetId);
			if (deadShardId === undefined) throw new Error("A third ready shard is required for the crash snapshot.");
			const deadFleet = fleetA.processes.has(deadShardId) ? fleetA : fleetB;
			deadFleet.crash(deadShardId);
			await waitFor(() => {
				const ready = sortedReadyTopology(hub);
				return (
					ready.length === 2 &&
					ready.includes(sourceId) &&
					ready.includes(targetId) &&
					!ready.includes(deadShardId) &&
					!ready.includes(UNREADY_SHARD_ID)
				);
			}, "dead and unready shards to leave the ready topology");

			const results = await source.broadcastEval(
				(_discord, context: { readonly value: number }) => context.value * 2,
				{ value: 6 },
				500,
			);
			expect([...results.entries()].sort(([left], [right]) => left - right)).toEqual([
				[sourceId, 12],
				[targetId, 12],
			]);
			expect(bridgeA.connected).toBe(true);
			expect(bridgeB.connected).toBe(true);
			expect(hubErrors).toEqual([]);
			expect(fleetA.errors).toEqual([]);
			expect(fleetB.errors).toEqual([]);
		} finally {
			fleetA.slowRelease.resolve();
			fleetB.slowRelease.resolve();
			await stopAll(...(bridgeB === undefined ? [] : [bridgeB]), ...(bridgeA === undefined ? [] : [bridgeA]), hub);
		}
	}, 10_000);

	test("routes between two shards on one Bridge and reports the cluster identity", async () => {
		const hubErrors: ReportedError[] = [];
		const bridgeErrors: ReportedError[] = [];
		const hub = createHub(4, hubErrors);
		const fleetA = new EmbeddedShardFleet((shardId) => shardId !== UNREADY_SHARD_ID);
		const fleetB = new EmbeddedShardFleet((shardId) => shardId !== UNREADY_SHARD_ID);
		let bridgeA: BridgeClient | undefined;
		let bridgeB: BridgeClient | undefined;
		try {
			await hub.start();
			const hubUrl = requireHubUrl(hub);
			bridgeA = createBridge(hubUrl, "bridge-same-a", 2, fleetA, bridgeErrors);
			await bridgeA.start();
			await bridgeA.waitUntilConnected(3_000);
			await waitFor(() => fleetA.clients.size === 2, "the first Bridge assignments");

			bridgeB = createBridge(hubUrl, "bridge-same-b", 2, fleetB, bridgeErrors);
			await bridgeB.start();
			await bridgeB.waitUntilConnected(3_000);
			await waitFor(() => fleetA.clients.size + fleetB.clients.size === 4, "all embedded shard processes");
			await waitFor(() => sortedReadyTopology(hub).length === 3, "three Discord-ready shards");

			const [sourceId, targetId] = fleetA.readyShardIds();
			if (sourceId === undefined || targetId === undefined) {
				throw new Error("The first Bridge must own two ready shards.");
			}
			const source = requireClient(fleetA, sourceId);
			const target = requireClient(fleetA, targetId);

			await waitFor(
				() => source.identity.totalBridges === 2 && target.identity.totalBridges === 2,
				"the two-Bridge topology to reach both same-Bridge shards",
			);
			await waitFor(
				() => hub.getTopology().bridges.every((bridge) => bridge.connected),
				"the Hub to finish synchronizing both Bridges",
			);
			const expectedCluster = [
				{ bridgeId: "bridge-same-a", shardCount: 2 },
				{ bridgeId: "bridge-same-b", shardCount: 2 },
			];
			for (const [shardId, client] of [
				[sourceId, source],
				[targetId, target],
			] as const) {
				expect(client.identity.shardId).toBe(shardId);
				expect(client.identity.totalShards).toBe(4);
				expect(client.identity.bridgeId).toBe("bridge-same-a");
				expect(client.identity.bridgeShardCount).toBe(2);
				expect(client.identity.totalBridges).toBe(2);
				expect([...client.identity.bridges].sort((left, right) => left.bridgeId.localeCompare(right.bridgeId))).toEqual(
					expectedCluster,
				);
			}

			await source.send(targetId, { action: "same-bridge-message", value: 11 });
			await waitFor(
				() =>
					fleetA.messages.some(
						(message) =>
							message.shardId === targetId &&
							message.sourceShardId === sourceId &&
							readAction(message.payload) === "same-bridge-message",
					),
				"same-Bridge message delivery",
			);

			expect(
				await source.request<{ readonly action: string; readonly shardId: number }>(targetId, {
					action: "same-bridge-request",
				}),
			).toEqual({
				action: "same-bridge-request",
				shardId: targetId,
			});

			expect(bridgeA.connected).toBe(true);
			expect(bridgeB.connected).toBe(true);
			expect(hubErrors).toEqual([]);
			expect(fleetA.errors).toEqual([]);
			expect(fleetB.errors).toEqual([]);
		} finally {
			await stopAll(...(bridgeB === undefined ? [] : [bridgeB]), ...(bridgeA === undefined ? [] : [bridgeA]), hub);
		}
	}, 10_000);

	test("rejects one real unauthorized handshake and reconnects with valid authentication", async () => {
		const hubErrors: ReportedError[] = [];
		const bridgeErrors: ReportedError[] = [];
		const hub = createHub(1, hubErrors);
		const fleet = new EmbeddedShardFleet(() => true);
		let attempts = 0;
		const retryingSocketFactory: $BridgeSocketFactory = (url, headers) => {
			attempts += 1;
			return nativeSocket(
				url,
				attempts === 1
					? {
							...headers,
							Authorization: "Bearer rejected-token",
						}
					: headers,
			);
		};
		let bridge: BridgeClient | undefined;
		try {
			await hub.start();
			bridge = createBridge(requireHubUrl(hub), "bridge-auth-recovery", 1, fleet, bridgeErrors, retryingSocketFactory);
			await bridge.start();
			await bridge.waitUntilConnected(3_000);
			await waitFor(() => sortedReadyTopology(hub).length === 1, "authenticated retry shard readiness");

			expect(attempts).toBeGreaterThanOrEqual(2);
			expect(bridgeErrors.length).toBeGreaterThanOrEqual(1);
			expect(bridge.connected).toBe(true);
			expect(hub.getTopology().bridges.map((entry) => entry.id)).toEqual(["bridge-auth-recovery"]);
			expect(hubErrors).toEqual([]);
		} finally {
			await stopAll(...(bridge === undefined ? [] : [bridge]), hub);
		}
	}, 10_000);
});
