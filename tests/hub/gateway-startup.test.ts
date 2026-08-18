import { describe, expect, test } from "bun:test";
import { ShardingConfigurationError, ShardingTransportError } from "../../src/errors/ShardingError";
import { loadGatewayBotInfo } from "../../src/hub/gatewayStartup";
import type { $Sleep } from "../../src/types/common";

const TOKEN = "secret.token";

function metadataResponse(): Response {
	return Response.json({
		session_start_limit: { max_concurrency: 16, remaining: 900, reset_after: 3_600_000, total: 1_000 },
		shards: 4,
		url: "wss://gateway.discord.gg",
	});
}

function recordingSleep(delays: number[]): $Sleep {
	return async (milliseconds) => {
		delays.push(milliseconds);
	};
}

describe("loadGatewayBotInfo", () => {
	test("retries transport failures with exponential backoff until Discord answers", async () => {
		const delays: number[] = [];
		let attempts = 0;
		const info = await loadGatewayBotInfo(TOKEN, {
			endpoint: "https://discord.test/gateway/bot",
			fetch: () => {
				attempts += 1;
				if (attempts < 3) return Promise.resolve(new Response("busy", { status: 503 }));
				return Promise.resolve(metadataResponse());
			},
			signal: new AbortController().signal,
			sleep: recordingSleep(delays),
		});
		expect(attempts).toBe(3);
		expect(delays).toEqual([500, 1_000]);
		expect(info.shards).toBe(4);
		expect(info.session_start_limit.max_concurrency).toBe(16);
	});

	test("raises permanent authorization failures on the first attempt", async () => {
		const delays: number[] = [];
		let attempts = 0;
		await expect(
			loadGatewayBotInfo(TOKEN, {
				endpoint: "https://discord.test/gateway/bot",
				fetch: () => {
					attempts += 1;
					return Promise.resolve(new Response("nope", { status: 401, statusText: "Unauthorized" }));
				},
				signal: new AbortController().signal,
				sleep: recordingSleep(delays),
			}),
		).rejects.toBeInstanceOf(ShardingConfigurationError);
		expect(attempts).toBe(1);
		expect(delays).toEqual([]);
	});

	test("gives up after its attempt budget and retains the last transport failure", async () => {
		const delays: number[] = [];
		let attempts = 0;
		const failure = await loadGatewayBotInfo(TOKEN, {
			endpoint: "https://discord.test/gateway/bot",
			fetch: () => {
				attempts += 1;
				return Promise.reject(new Error("socket closed"));
			},
			signal: new AbortController().signal,
			sleep: recordingSleep(delays),
		}).catch((cause: unknown) => cause);
		expect(attempts).toBe(4);
		expect(delays).toEqual([500, 1_000, 2_000]);
		expect(failure).toBeInstanceOf(ShardingTransportError);
		expect((failure as Error).message).toContain("after 4 attempts");
		expect((failure as Error).cause).toBeInstanceOf(ShardingTransportError);
	});

	test("stops retrying once Hub shutdown cancels startup", async () => {
		const controller = new AbortController();
		const delays: number[] = [];
		let attempts = 0;
		const failure = await loadGatewayBotInfo(TOKEN, {
			endpoint: "https://discord.test/gateway/bot",
			fetch: () => {
				attempts += 1;
				controller.abort(new Error("HubClient stopped."));
				return Promise.resolve(new Response("busy", { status: 503 }));
			},
			signal: controller.signal,
			sleep: recordingSleep(delays),
		}).catch((cause: unknown) => cause);
		expect(attempts).toBe(1);
		expect(delays).toEqual([]);
		expect(failure).toBeInstanceOf(ShardingTransportError);
	});
});
