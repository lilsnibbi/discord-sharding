import { describe, expect, test } from "bun:test";
import { LiveGatewayClient } from "../utilities/live-gateway-client";

const token = process.env.TOKEN;

describe.skipIf(token === undefined || token.length === 0)("LiveGatewayClient (live gateway)", () => {
	test(
		"connects one shard to the real gateway and reaches READY",
		async () => {
			const client = new LiveGatewayClient({ shardId: 0, totalShards: 1 });
			expect(client.isReady()).toBe(false);
			try {
				await client.login(token);
				expect(client.isReady()).toBe(true);
			} finally {
				client.destroy();
			}
			expect(client.isReady()).toBe(false);
		},
		{ timeout: 90_000 },
	);
});
