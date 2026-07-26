import { describe, expect, test } from "bun:test";
import {
	ShardingConfigurationError,
	ShardingStateError,
	ShardingTimeoutError,
	ShardingTransportError,
} from "../../src/errors/ShardingError";
import { MAX_SHARDS, MAX_TIMER_MS } from "../../src/internal/limits";
import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { parseWireMessage } from "../../src/protocol/codec";
import { SHARD_TO_BRIDGE_TYPES } from "../../src/protocol/types";
import { ShardClient } from "../../src/shard/ShardClient";
import {
	controlMessage,
	createClient,
	FakeDiscordClient,
	FakeShardTransport,
	waitForSent,
} from "../utilities/shard-client";

describe("ShardClient", () => {
	test("gates the actual discord.js login behind a correlated identify grant", async () => {
		const { botClient, client, transport } = createClient();
		await client.start();

		const login = client.login("bot-token");
		const identify = await waitForSent(transport, "shard.identify.request");
		expect(botClient.loginTokens).toEqual([]);
		expect(client.pendingRequests).toBe(1);

		transport.receive(
			controlMessage("shard.control.identify.response", identify.id, {
				granted: true,
			}),
		);
		expect(await login).toBe("bot-token");
		expect(botClient.loginTokens).toEqual(["bot-token"]);
		expect(client.pendingRequests).toBe(0);
		const ready = await waitForSent(transport, "shard.ready");
		expect(ready.data).toEqual({
			assignmentEpoch: 3,
			processGeneration: 4,
			shardId: 1,
		});
		await client.close();
	});

	test("tracks maintenance and acknowledges the synchronized topology", async () => {
		const { client, transport } = createClient();
		const changes: boolean[] = [];
		client.bridge.onMaintenanceChange((maintenance) => {
			changes.push(maintenance);
		});
		await client.start();

		transport.receive(
			controlMessage("shard.control.maintenance", "sync:1", {
				acknowledge: true,
				maintenance: false,
				topologyVersion: 7,
			}),
		);
		const acknowledgement = await waitForSent(transport, "shard.sync.ack");

		expect(client.bridge.isInMaintenance).toBe(false);
		expect(changes).toEqual([false]);
		expect(acknowledgement.id).toBe("sync:1");
		expect(acknowledgement.data).toEqual({ topologyVersion: 7 });
		const beforeUnacknowledgedUpdate = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.maintenance", "maintenance:offline", {
				acknowledge: false,
				maintenance: true,
				topologyVersion: 7,
			}),
		);
		await Bun.sleep(2);
		const laterAcknowledgements = transport.sent
			.slice(beforeUnacknowledgedUpdate)
			.map((entry) => parseWireMessage(entry, SHARD_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY))
			.filter((entry) => entry.type === "shard.sync.ack");
		expect(laterAcknowledgements).toHaveLength(0);
		await client.close();
		expect(client.bridge.isInMaintenance).toBe(true);
	});

	test("sends an immediate generation-bound heartbeat and fails closed on IPC disconnect", async () => {
		const { botClient, client, transport } = createClient();
		await client.start();

		const heartbeat = await waitForSent(transport, "shard.heartbeat");
		expect(heartbeat.data).toEqual({
			assignmentEpoch: 3,
			processGeneration: 4,
			shardId: 1,
		});

		transport.disconnect();
		expect(client.state).toBe("failed");
		expect(botClient.destroyed).toBe(1);
		expect(transport.listenerRemovals).toBe(1);
		expect(transport.disconnectListenerRemovals).toBe(1);
	});

	test("admits inbound handlers in order and rejects schedules beyond Bun's timer range", async () => {
		const botClient = new FakeDiscordClient();
		botClient.ready = true;
		const { client, transport } = createClient(botClient);
		let releaseFirst = (): void => undefined;
		const firstPending = new Promise<void>((resolve) => {
			releaseFirst = resolve;
		});
		const calls: number[] = [];
		await client.start();
		client.onRequest(async (payload) => {
			const record = payload as { readonly order: number };
			calls.push(record.order);
			if (record.order === 1) await firstPending;
			return record.order;
		});

		transport.receive(
			controlMessage("shard.control.route.request", "route:serial:1", {
				kind: "request",
				payload: { order: 1 },
				sourceShardId: 0,
			}),
		);
		transport.receive(
			controlMessage("shard.control.route.request", "route:serial:2", {
				kind: "request",
				payload: { order: 2 },
				sourceShardId: 0,
			}),
		);
		await Bun.sleep(2);
		expect(calls).toEqual([1, 2]);
		releaseFirst();
		for (let attempt = 0; attempt < 100; attempt += 1) {
			const responseIds = transport.sent
				.map((entry) => parseWireMessage(entry, SHARD_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY))
				.filter((entry) => entry.type === "shard.route.response")
				.map((entry) => entry.id);
			if (responseIds.includes("route:serial:1") && responseIds.includes("route:serial:2")) break;
			await Bun.sleep(1);
		}
		const responseIds = transport.sent
			.map((entry) => parseWireMessage(entry, SHARD_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY))
			.filter((entry) => entry.type === "shard.route.response")
			.map((entry) => entry.id);
		expect(responseIds).toContain("route:serial:1");
		expect(responseIds).toContain("route:serial:2");

		const beforePrepare = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.eval.prepare", "eval:too-far", {
				context: null,
				evaluator: "() => 1",
				sourceShardId: 0,
			}),
		);
		await waitForSent(transport, "shard.eval.prepared", beforePrepare);
		const beforeCommit = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.eval.commit", "eval:too-far", {
				executeAt: Date.now() + MAX_TIMER_MS + 60_000,
			}),
		);
		const result = await waitForSent(transport, "shard.eval.result", beforeCommit);
		expect(result.data.ok).toBe(false);
		await client.close();
	});

	test("does not let a hung application handler block maintenance or authorized shutdown", async () => {
		const botClient = new FakeDiscordClient();
		const transport = new FakeShardTransport();
		const { client } = createClient(botClient, transport);
		await client.start();
		client.onRequest(async () => new Promise<never>(() => undefined));
		transport.receive(
			controlMessage("shard.control.route.request", "route:hung", {
				kind: "request",
				payload: null,
				sourceShardId: 0,
			}),
		);
		for (let attempt = 0; attempt < 100 && client.activeHandlers === 0; attempt += 1) await Bun.sleep(1);
		expect(client.activeHandlers).toBe(1);

		const beforeControl = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.maintenance", "sync:after-hung", {
				acknowledge: true,
				maintenance: true,
				topologyVersion: 9,
			}),
		);
		transport.receive(
			controlMessage("shard.control.shutdown", "shutdown:after-hung", {
				commandId: "shutdown:after-hung",
				reason: "test shutdown",
			}),
		);
		const acknowledgement = await waitForSent(transport, "shard.sync.ack", beforeControl);
		expect(acknowledgement.id).toBe("sync:after-hung");
		for (let attempt = 0; attempt < 100 && client.state !== "closed"; attempt += 1) await Bun.sleep(1);
		expect(client.state).toBe("closed");
		expect(botClient.destroyed).toBe(1);
	});

	test("always destroys Discord state and reports a failed shutdown callback", async () => {
		const botClient = new FakeDiscordClient();
		const transport = new FakeShardTransport();
		const errors: Error[] = [];
		const { client } = createClient(botClient, transport, {
			onError: (error) => errors.push(error),
			onShutdown: () => {
				throw new Error("application cleanup failed");
			},
		});
		await client.start();
		const beforeShutdown = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.shutdown", "shutdown:failed", {
				commandId: "shutdown:failed",
				reason: "test failure",
			}),
		);
		for (let attempt = 0; attempt < 100 && client.state !== "closed"; attempt += 1) await Bun.sleep(1);

		expect(client.state).toBe("closed");
		expect(botClient.destroyed).toBe(1);
		expect(errors.some((error) => error instanceof AggregateError)).toBe(true);
		const completions = transport.sent
			.slice(beforeShutdown)
			.map((entry) => parseWireMessage(entry, SHARD_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY))
			.filter((entry) => entry.type === "shard.shutdown.complete");
		expect(completions).toHaveLength(0);
	});

	test("counts running evaluations against the prepared-evaluation limit", async () => {
		const botClient = new FakeDiscordClient();
		botClient.ready = true;
		const transport = new FakeShardTransport();
		const { client } = createClient(botClient, transport, { maxPreparedEvaluations: 1 });
		await client.start();
		const beforeFirstPrepare = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.eval.prepare", "eval:running", {
				context: null,
				evaluator: "async () => new Promise(() => undefined)",
				sourceShardId: 0,
			}),
		);
		await waitForSent(transport, "shard.eval.prepared", beforeFirstPrepare);
		transport.receive(
			controlMessage("shard.control.eval.commit", "eval:running", {
				executeAt: Date.now(),
			}),
		);
		await Bun.sleep(2);

		const beforeSecondPrepare = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.eval.prepare", "eval:second", {
				context: null,
				evaluator: "() => 2",
				sourceShardId: 0,
			}),
		);
		const rejected = await waitForSent(transport, "shard.eval.prepared", beforeSecondPrepare);
		expect(rejected.data.ok).toBe(false);
		await client.close();
	});

	test("rejects unknown and accessor-backed option boundaries without invoking accessors", () => {
		expect(() =>
			Reflect.construct(ShardClient, [new FakeDiscordClient(), { analyticsIntervalMs: false, unexpected: true }]),
		).toThrow(ShardingConfigurationError);

		let reads = 0;
		const options = { analyticsIntervalMs: false };
		Object.defineProperty(options, "transport", {
			enumerable: true,
			get: () => {
				reads += 1;
				return new FakeShardTransport();
			},
		});
		expect(() => Reflect.construct(ShardClient, [new FakeDiscordClient(), options])).toThrow(
			ShardingConfigurationError,
		);
		expect(reads).toBe(0);

		const discordClient = new FakeDiscordClient();
		Object.defineProperty(discordClient, "login", {
			get: () => {
				reads += 1;
				return async (): Promise<string> => "unsafe";
			},
		});
		expect(() => new ShardClient(discordClient)).toThrow(ShardingConfigurationError);
		expect(reads).toBe(0);

		const transport = {
			onMessage:
				(_listener: (message: unknown) => void): (() => void) =>
				() =>
					undefined,
		};
		Object.defineProperty(transport, "send", {
			get: () => {
				reads += 1;
				return (): void => undefined;
			},
		});
		expect(() =>
			Reflect.construct(ShardClient, [
				new FakeDiscordClient(),
				{
					analyticsIntervalMs: false,
					assignmentEpoch: 1,
					processGeneration: 1,
					shardId: 0,
					totalShards: 1,
					transport,
				},
			]),
		).toThrow(ShardingConfigurationError);
		expect(reads).toBe(0);
	});

	test("runs environment identities through their field-specific limits", () => {
		const previousShardId = Bun.env.SHARDING_SHARD_ID;
		const previousTotalShards = Bun.env.SHARDING_TOTAL_SHARDS;
		try {
			Bun.env.SHARDING_SHARD_ID = String(MAX_SHARDS + 1);
			Bun.env.SHARDING_TOTAL_SHARDS = "1";
			expect(() => new ShardClient(new FakeDiscordClient())).toThrow(ShardingConfigurationError);

			Bun.env.SHARDING_SHARD_ID = "0";
			Bun.env.SHARDING_TOTAL_SHARDS = String(MAX_SHARDS + 1);
			expect(() => new ShardClient(new FakeDiscordClient())).toThrow(ShardingConfigurationError);
		} finally {
			if (previousShardId === undefined) delete Bun.env.SHARDING_SHARD_ID;
			else Bun.env.SHARDING_SHARD_ID = previousShardId;
			if (previousTotalShards === undefined) delete Bun.env.SHARDING_TOTAL_SHARDS;
			else Bun.env.SHARDING_TOTAL_SHARDS = previousTotalShards;
		}
	});

	test("correlates outbound routing and dispatches inbound requests", async () => {
		const { client, transport } = createClient();
		await client.start();
		client.onRequest(async (payload, context) => ({
			payload,
			source: context.sourceShardId,
		}));

		const outbound = client.request<{ readonly accepted: boolean }>(2, { action: "ping" });
		const routed = await waitForSent(transport, "shard.route.request");
		expect(routed.data).toEqual({
			kind: "request",
			payload: { action: "ping" },
			targetShardId: 2,
		});
		transport.receive(
			controlMessage("shard.control.route.response", routed.id, {
				ok: true,
				value: { accepted: true },
			}),
		);
		expect(await outbound).toEqual({ accepted: true });

		const sentBeforeInbound = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.route.request", "inbound:1", {
				kind: "request",
				payload: { value: 42 },
				sourceShardId: 3,
			}),
		);
		const response = await waitForSent(transport, "shard.route.response", sentBeforeInbound);
		expect(response.id).toBe("inbound:1");
		expect(response.data).toEqual({
			ok: true,
			value: {
				payload: { value: 42 },
				source: 3,
			},
		});
		expect(client.activeHandlers).toBe(0);
		await client.close();
	});

	test("correlates broadcast results and runs prepared evaluations only after commit", async () => {
		const botClient = new FakeDiscordClient();
		botClient.ready = true;
		const { client, transport } = createClient(botClient);
		await client.start();

		const evaluation = client.broadcastEval(
			async (_discord, context: { readonly value: number }) => context.value * 2,
			{ value: 5 },
		);
		const request = await waitForSent(transport, "shard.eval.request");
		transport.receive(
			controlMessage("shard.control.eval.response", request.id, {
				ok: true,
				results: [
					{ shardId: 0, value: 10 },
					{ shardId: 1, value: 10 },
				],
			}),
		);
		expect([...(await evaluation).entries()]).toEqual([
			[0, 10],
			[1, 10],
		]);

		const beforePrepare = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.eval.prepare", "eval:prepared", {
				context: { value: 9 },
				evaluator: "(_client, context) => context.value + 1",
				sourceShardId: 0,
			}),
		);
		const prepared = await waitForSent(transport, "shard.eval.prepared", beforePrepare);
		expect(prepared.data).toEqual({ ok: true });
		const beforeCommit = transport.sent.length;
		transport.receive(
			controlMessage("shard.control.eval.commit", "eval:prepared", {
				executeAt: Date.now(),
			}),
		);
		const result = await waitForSent(transport, "shard.eval.result", beforeCommit);
		expect(result.data).toEqual({ ok: true, value: 10 });
		await client.close();
	});

	test("rejects pending work and removes transport ownership during close", async () => {
		const { client, transport } = createClient();
		await client.start();
		const pending = client.request(2, { action: "wait" });
		await waitForSent(transport, "shard.route.request");

		await client.close();
		await expect(pending).rejects.toBeInstanceOf(ShardingStateError);
		expect(client.state).toBe("closed");
		expect(client.pendingRequests).toBe(0);
		expect(transport.listenerRemovals).toBe(1);
		await client.close();
		expect(transport.listenerRemovals).toBe(1);
	});

	test("rolls back listener ownership when startup transport delivery fails", async () => {
		const botClient = new FakeDiscordClient();
		const transport = new FakeShardTransport();
		transport.sendFailure = new Error("IPC unavailable");
		const { client } = createClient(botClient, transport);

		await expect(client.start()).rejects.toBeInstanceOf(ShardingTransportError);
		expect(client.state).toBe("failed");
		expect(transport.listenerRemovals).toBe(1);
		expect(transport.disconnectListenerRemovals).toBe(1);
	});

	test("bounds a custom transport send that never settles", async () => {
		const botClient = new FakeDiscordClient();
		const transport = new FakeShardTransport();
		transport.sendOperation = new Promise<void>(() => undefined);
		const { client } = createClient(botClient, transport, {
			request: { maxPending: 2, timeoutMs: 5 },
		});

		await expect(client.start()).rejects.toBeInstanceOf(ShardingTimeoutError);
		expect(client.state).toBe("failed");
		expect(transport.listenerRemovals).toBe(1);
		expect(transport.disconnectListenerRemovals).toBe(1);
	});
});
