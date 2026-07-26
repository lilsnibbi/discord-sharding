import { describe, expect, test } from "bun:test";
import {
	ShardingConfigurationError,
	ShardingProtocolError,
	ShardingTransportError,
} from "../../src/errors/ShardingError";
import { fetchGatewayBotInfo } from "../../src/hub/gateway";

const gatewayBody = {
	url: "wss://gateway.discord.gg/",
	shards: 8,
	session_start_limit: {
		total: 1_000,
		remaining: 999,
		reset_after: 14_400_000,
		max_concurrency: 2,
	},
};

describe("fetchGatewayBotInfo", () => {
	test("uses bot authorization and returns immutable validated metadata", async () => {
		let authorization: string | null = null;
		let requestedUrl = "";

		const result = await fetchGatewayBotInfo("secret.token", {
			fetch: async (input, init) => {
				requestedUrl = String(input);
				authorization = new Headers(init?.headers).get("authorization");
				return Response.json(gatewayBody);
			},
		});

		expect(requestedUrl).toBe("https://discord.com/api/v10/gateway/bot");
		expect(String(authorization)).toBe("Bot secret.token");
		expect(result).toEqual(gatewayBody);
		expect(Object.isFrozen(result)).toBe(true);
		expect(Object.isFrozen(result.session_start_limit)).toBe(true);
	});

	test("rejects malformed metadata with a protocol error", async () => {
		await expect(
			fetchGatewayBotInfo("secret.token", {
				fetch: async () =>
					Response.json({
						...gatewayBody,
						session_start_limit: {
							...gatewayBody.session_start_limit,
							remaining: 1_001,
						},
					}),
			}),
		).rejects.toBeInstanceOf(ShardingProtocolError);
	});

	test("bounds streamed bodies and cancels overflow", async () => {
		let cancelled = false;
		const body = new ReadableStream<Uint8Array>({
			cancel: () => {
				cancelled = true;
			},
			start: (controller) => {
				controller.enqueue(new Uint8Array(65_537));
			},
		});

		await expect(
			fetchGatewayBotInfo("secret.token", {
				fetch: async () => new Response(body),
			}),
		).rejects.toBeInstanceOf(ShardingProtocolError);
		expect(cancelled).toBe(true);
	});

	test("returns promptly when a non-cooperative fetch is cancelled", async () => {
		const controller = new AbortController();
		const request = fetchGatewayBotInfo("secret.token", {
			signal: controller.signal,
			fetch: async () => new Promise<Response>(() => undefined),
		});

		controller.abort(new Error("test cancellation"));
		await expect(request).rejects.toBeInstanceOf(ShardingTransportError);
	});

	test("rejects invalid credentials and endpoints before fetching", async () => {
		let fetched = false;
		const fetcher = async (): Promise<Response> => {
			fetched = true;
			return Response.json(gatewayBody);
		};

		await expect(fetchGatewayBotInfo("Bot token", { fetch: fetcher })).rejects.toBeInstanceOf(
			ShardingConfigurationError,
		);
		await expect(
			fetchGatewayBotInfo("secret.token", {
				endpoint: "http://discord.test/gateway",
				fetch: fetcher,
			}),
		).rejects.toBeInstanceOf(ShardingConfigurationError);
		expect(fetched).toBe(false);
	});
});
