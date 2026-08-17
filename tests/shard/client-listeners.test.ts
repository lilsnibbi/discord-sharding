import { describe, expect, test } from "bun:test";
import { ShardingCapacityError, ShardingConfigurationError, ShardingStateError } from "../../src/errors/ShardingError";
import { MAX_SHARD_LISTENERS } from "../../src/shard/runtime";
import { controlMessage, createClient, FakeDiscordClient, waitForSent } from "../utilities/shard-client";

describe("ShardClient listeners and evaluation cancellation", () => {
	test("bounds message listeners and removes them idempotently", async () => {
		const { client } = createClient();
		await client.start();
		try {
			const removals = Array.from({ length: MAX_SHARD_LISTENERS }, () => client.onMessage(() => undefined));

			expect(() => client.onMessage(() => undefined)).toThrow(ShardingCapacityError);
			const first = removals[0];
			if (first === undefined) throw new Error("Listener removal callback is missing.");
			first();
			first();
			expect(() => client.onMessage(() => undefined)).not.toThrow();
			expect(() => client.onMessage(Reflect.get({}, "missing"))).toThrow(ShardingConfigurationError);
		} finally {
			await client.close();
		}
	});

	test("installs one request handler at a time and releases it on removal", async () => {
		const { client, transport } = createClient();
		await client.start();
		try {
			const remove = client.onRequest(() => ({ handled: "first" }));

			expect(() => client.onRequest(() => undefined)).toThrow(ShardingStateError);
			expect(() => client.onRequest(Reflect.get({}, "missing"))).toThrow(ShardingConfigurationError);
			remove();
			remove();

			const reinstalled = client.onRequest(() => ({ handled: "second" }));
			const startIndex = transport.sent.length;
			transport.receive(
				controlMessage("shard.control.route.request", "route:handler", {
					kind: "request",
					payload: { ping: true },
					sourceShardId: 0,
				}),
			);
			const response = await waitForSent(transport, "shard.route.response", startIndex);

			expect(response.data).toMatchObject({ ok: true, value: { handled: "second" } });
			reinstalled();
		} finally {
			await client.close();
		}
	});

	test("delivers one-way messages to every retained listener", async () => {
		const { client, transport } = createClient();
		await client.start();
		try {
			const received: unknown[] = [];
			const removeFirst = client.onMessage((payload) => {
				received.push(payload);
			});
			client.onMessage((payload) => {
				received.push(payload);
			});

			transport.receive(
				controlMessage("shard.control.route.request", "route:message", {
					kind: "message",
					payload: { sequence: 1 },
					sourceShardId: 0,
				}),
			);
			await waitForSent(transport, "shard.route.response");
			expect(received).toHaveLength(2);

			removeFirst();
			transport.receive(
				controlMessage("shard.control.route.request", "route:message-2", {
					kind: "message",
					payload: { sequence: 2 },
					sourceShardId: 0,
				}),
			);
			await waitForSent(transport, "shard.route.response", transport.sent.length - 1);
			expect(received).toHaveLength(3);
		} finally {
			await client.close();
		}
	});

	test("bounds maintenance listeners, reports listener failures, and removes them idempotently", async () => {
		const errors: string[] = [];
		const { client, transport } = createClient(new FakeDiscordClient(), undefined, {
			onError: (_error, context) => {
				errors.push(context);
			},
		});
		await client.start();
		try {
			const removals = Array.from({ length: MAX_SHARD_LISTENERS - 2 }, () =>
				client.bridge.onMaintenanceChange(() => undefined),
			);
			client.bridge.onMaintenanceChange(() => {
				throw new Error("Synchronous maintenance listener failure.");
			});
			client.bridge.onMaintenanceChange(() => Promise.reject(new Error("Async maintenance listener failure.")));

			expect(() => client.bridge.onMaintenanceChange(() => undefined)).toThrow(ShardingCapacityError);
			expect(() => client.bridge.onMaintenanceChange(Reflect.get({}, "missing"))).toThrow(ShardingConfigurationError);

			transport.receive(
				controlMessage("shard.control.maintenance", "maintenance:1", {
					acknowledge: true,
					maintenance: false,
					topologyVersion: 1,
				}),
			);
			await waitForSent(transport, "shard.sync.ack");
			await Bun.sleep(1);

			expect(client.bridge.isInMaintenance).toBeFalse();
			expect(errors.filter((context) => context === "maintenance listener")).toHaveLength(2);

			const first = removals[0];
			if (first === undefined) throw new Error("Listener removal callback is missing.");
			first();
			first();
			expect(() => client.bridge.onMaintenanceChange(() => undefined)).not.toThrow();
		} finally {
			await client.close();
		}
	});

	test("drops a prepared evaluation that the Hub cancels before its commit", async () => {
		const botClient = new FakeDiscordClient();
		botClient.ready = true;
		const { client, transport } = createClient(botClient);
		await client.start();
		try {
			transport.receive(
				controlMessage("shard.control.eval.prepare", "eval:cancelled", {
					context: { value: 1 },
					evaluator: "(_client, context) => context.value",
					sourceShardId: 0,
				}),
			);
			await waitForSent(transport, "shard.eval.prepared");

			transport.receive(
				controlMessage("shard.control.eval.cancel", "eval:cancelled", {
					reason: "Evaluation cancelled before all results were available.",
				}),
			);
			transport.receive(
				controlMessage("shard.control.eval.cancel", "eval:unknown", {
					reason: "Unknown evaluations are ignored.",
				}),
			);
			const beforeCommit = transport.sent.length;
			transport.receive(
				controlMessage("shard.control.eval.commit", "eval:cancelled", {
					executeAt: Date.now(),
				}),
			);
			const rejected = await waitForSent(transport, "shard.eval.result", beforeCommit);

			expect(rejected.data).toMatchObject({ ok: false });
			expect(rejected.id).toBe("eval:cancelled");
		} finally {
			await client.close();
		}
	});

	test("emits periodic analytics that describe the Discord client and process", async () => {
		const botClient = new FakeDiscordClient();
		botClient.ready = true;
		const { client, transport } = createClient(botClient, undefined, { analyticsIntervalMs: 100 });
		await client.start();
		try {
			const analytics = await waitForSent(transport, "shard.analytics");
			const payload = analytics.data.payload;
			if (typeof payload !== "object" || payload === null) throw new Error("Analytics payload is missing.");
			const discord = Reflect.get(payload, "discord");
			const process = Reflect.get(payload, "process");
			if (typeof discord !== "object" || discord === null) throw new Error("Analytics omitted Discord metrics.");
			if (typeof process !== "object" || process === null) throw new Error("Analytics omitted process metrics.");

			expect(typeof analytics.data.collectedAt).toBe("number");
			expect(Reflect.get(discord, "ready")).toBeTrue();
			expect(typeof Reflect.get(process, "heapSizeBytes")).toBe("number");
			expect(typeof Reflect.get(process, "uptimeSeconds")).toBe("number");
		} finally {
			await client.close();
		}
	});
});
