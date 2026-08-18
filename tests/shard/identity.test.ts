import { describe, expect, test } from "bun:test";
import { controlMessage, createClient } from "../utilities/shard-client";

function topologyMessage(
	id: string,
	overrides: Partial<{
		bridgeId: string;
		bridges: readonly { readonly bridgeId: string; readonly shardCount: number }[];
		topologyVersion: number;
		totalShards: number;
	}> = {},
): object {
	return controlMessage("shard.control.topology", id, {
		bridgeId: "bridge-a",
		bridges: [
			{ bridgeId: "bridge-a", shardCount: 3 },
			{ bridgeId: "bridge-b", shardCount: 1 },
		],
		topologyVersion: 7,
		totalShards: 4,
		...overrides,
	});
}

describe("ShardClient identity", () => {
	test("exposes spawn-time identity before any topology report", () => {
		const { client } = createClient(undefined, undefined, { bridgeId: "bridge-a" });
		const identity = client.identity;
		expect(identity.shardId).toBe(1);
		expect(identity.totalShards).toBe(4);
		expect(identity.assignmentEpoch).toBe(3);
		expect(identity.processGeneration).toBe(4);
		expect(identity.bridgeId).toBe("bridge-a");
		expect(identity.instanceId).toBe("bridge-a:1:3:4");
		expect(identity.bridgeShardCount).toBe(0);
		expect(identity.totalBridges).toBe(0);
		expect(identity.bridges).toEqual([]);
	});

	test("defaults bridgeId to null without an option or environment value", () => {
		const { client } = createClient();
		expect(client.identity.bridgeId).toBeNull();
		expect(client.identity.instanceId).toBe("standalone:1:3:4");
	});

	test("is actually readonly: identity and nested values are frozen", () => {
		const { client } = createClient(undefined, undefined, { bridgeId: "bridge-a" });
		const identity = client.identity;
		expect(Object.isFrozen(identity)).toBe(true);
		expect(Object.isFrozen(identity.bridges)).toBe(true);
		expect(() => {
			(identity as { shardId: number }).shardId = 99;
		}).toThrow(TypeError);
	});

	test("applies a Bridge topology report and rebuilds the snapshot", async () => {
		const { client, transport } = createClient(undefined, undefined, { bridgeId: "bridge-a" });
		await client.start();
		transport.receive(topologyMessage("topology:1"));
		await Bun.sleep(5);
		const identity = client.identity;
		expect(identity.bridgeId).toBe("bridge-a");
		expect(identity.bridgeShardCount).toBe(3);
		expect(identity.totalBridges).toBe(2);
		expect(identity.bridges).toEqual([
			{ bridgeId: "bridge-a", shardCount: 3 },
			{ bridgeId: "bridge-b", shardCount: 1 },
		]);
		expect(Object.isFrozen(identity.bridges[0])).toBe(true);
		await client.close();
	});

	test("ignores stale topology versions", async () => {
		const { client, transport } = createClient(undefined, undefined, { bridgeId: "bridge-a" });
		await client.start();
		transport.receive(topologyMessage("topology:new", { topologyVersion: 9 }));
		await Bun.sleep(5);
		transport.receive(
			topologyMessage("topology:old", {
				bridges: [{ bridgeId: "bridge-a", shardCount: 1 }],
				topologyVersion: 8,
			}),
		);
		await Bun.sleep(5);
		expect(client.identity.bridgeShardCount).toBe(3);
		expect(client.identity.totalBridges).toBe(2);
		await client.close();
	});

	test("rejects a topology report whose totalShards contradicts this process", async () => {
		const errors: string[] = [];
		const { client, transport } = createClient(undefined, undefined, {
			bridgeId: "bridge-a",
			onError: (error) => {
				errors.push(error.message);
			},
		});
		await client.start();
		transport.receive(topologyMessage("topology:bad", { totalShards: 8 }));
		await Bun.sleep(10);
		expect(client.state).toBe("failed");
		expect(errors.some((message) => message.includes("totalShards"))).toBe(true);
	});

	test("rejects duplicate Bridge entries in one report", async () => {
		const { client, transport } = createClient(undefined, undefined, { bridgeId: "bridge-a" });
		await client.start();
		transport.receive(
			topologyMessage("topology:dupe", {
				bridges: [
					{ bridgeId: "bridge-a", shardCount: 1 },
					{ bridgeId: "bridge-a", shardCount: 2 },
				],
			}),
		);
		await Bun.sleep(10);
		expect(client.state).toBe("failed");
	});

	test("updates a null bridgeId from the first topology report", async () => {
		const { client, transport } = createClient();
		await client.start();
		expect(client.identity.bridgeId).toBeNull();
		transport.receive(topologyMessage("topology:1"));
		await Bun.sleep(5);
		expect(client.identity.bridgeId).toBe("bridge-a");
		expect(client.identity.instanceId).toBe("bridge-a:1:3:4");
		await client.close();
	});
});
