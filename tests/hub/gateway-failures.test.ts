import { describe, expect, test } from "bun:test";
import {
	ShardingConfigurationError,
	ShardingProtocolError,
	ShardingTransportError,
} from "../../src/errors/ShardingError";
import { fetchGatewayBotInfo } from "../../src/hub/gateway";

const TOKEN = "secret.token";

function streamResponse(chunk: unknown, headers?: HeadersInit): Response {
	return new Response(
		new ReadableStream({
			start: (controller) => {
				controller.enqueue(chunk);
				controller.close();
			},
		}),
		headers === undefined ? {} : { headers },
	);
}

describe("fetchGatewayBotInfo failures", () => {
	test("reports transport failures for rejected, invalid, and unsuccessful responses", async () => {
		await expect(
			fetchGatewayBotInfo(TOKEN, {
				fetch: () => Promise.reject(new Error("socket closed")),
			}),
		).rejects.toBeInstanceOf(ShardingTransportError);

		const impostor: Response = Object.create(Response.prototype);
		await expect(
			fetchGatewayBotInfo(TOKEN, {
				fetch: () => Promise.resolve(impostor),
			}),
		).rejects.toThrow("response metadata could not be read");

		await expect(
			fetchGatewayBotInfo(TOKEN, {
				fetch: () => Promise.resolve(new Response("nope", { status: 401, statusText: "Unauthorized" })),
			}),
		).rejects.toThrow("returned HTTP 401 Unauthorized");
	});

	test("rejects malformed and oversized Content-Length declarations", async () => {
		await expect(
			fetchGatewayBotInfo(TOKEN, {
				fetch: () => Promise.resolve(streamResponse(new Uint8Array(1), { "content-length": "not-a-number" })),
			}),
		).rejects.toThrow("invalid Content-Length");

		await expect(
			fetchGatewayBotInfo(TOKEN, {
				fetch: () => Promise.resolve(streamResponse(new Uint8Array(1), { "content-length": "65537" })),
			}),
		).rejects.toThrow("exceeded 65536 bytes");
	});

	test("rejects empty bodies, non-byte chunks, and malformed JSON", async () => {
		await expect(
			fetchGatewayBotInfo(TOKEN, {
				fetch: () => Promise.resolve(new Response(null)),
			}),
		).rejects.toThrow("body was empty");

		await expect(
			fetchGatewayBotInfo(TOKEN, {
				fetch: () => Promise.resolve(streamResponse("a string chunk")),
			}),
		).rejects.toThrow("non-byte chunk");

		await expect(
			fetchGatewayBotInfo(TOKEN, {
				fetch: () => Promise.resolve(new Response("{ not json")),
			}),
		).rejects.toBeInstanceOf(ShardingProtocolError);
	});

	test("applies its own deadline to a fetch that never settles", async () => {
		await expect(
			fetchGatewayBotInfo(TOKEN, {
				fetch: () => new Promise<Response>(() => undefined),
				timeoutMs: 20,
			}),
		).rejects.toThrow("timed out after 20ms");
	});

	test("rejects unreviewed and invalid request options before fetching", async () => {
		let fetched = false;
		const fetcher = (): Promise<Response> => {
			fetched = true;
			return Promise.resolve(Response.json({}));
		};

		await expect(
			fetchGatewayBotInfo(TOKEN, Reflect.get({ options: { fetch: fetcher, retries: 3 } }, "options")),
		).rejects.toBeInstanceOf(ShardingConfigurationError);
		await expect(fetchGatewayBotInfo(TOKEN, { fetch: fetcher, timeoutMs: 0 })).rejects.toBeInstanceOf(
			ShardingConfigurationError,
		);
		await expect(fetchGatewayBotInfo("", { fetch: fetcher })).rejects.toBeInstanceOf(ShardingConfigurationError);
		expect(fetched).toBeFalse();
	});
});
