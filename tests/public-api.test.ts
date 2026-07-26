import { describe, expect, test } from "bun:test";
import * as sharding from "../src";

const PUBLIC_TYPE_EXPORTS = [
	"$AnalyticsRecord",
	"$BridgeAnalyticsQuery",
	"$BridgeClientOptions",
	"$BridgeShardSnapshot",
	"$BridgeShardState",
	"$BridgeSocketFactory",
	"$BridgeState",
	"$BroadcastEvaluator",
	"$ClearAnalyticsOptions",
	"$DiscordCache",
	"$DiscordClient",
	"$DiscordManager",
	"$DiscordWebSocketManager",
	"$ErrorListener",
	"$GatewayFetch",
	"$HubAssignment",
	"$HubBridgeTopology",
	"$HubClientOptions",
	"$HubPersistence",
	"$HubState",
	"$HubTopology",
	"$JsonObject",
	"$JsonPrimitive",
	"$JsonValue",
	"$PayloadPolicy",
	"$PersistedAssignment",
	"$PersistedBridge",
	"$PersistedHubState",
	"$PersistedShard",
	"$PersistedShardState",
	"$ReconnectPolicy",
	"$RequestPolicy",
	"$RestartPolicy",
	"$ShardBridge",
	"$ShardClientOptions",
	"$ShardClientState",
	"$ShardMessageContext",
	"$ShardMessageListener",
	"$ShardProcess",
	"$ShardProcessCallbacks",
	"$ShardProcessContext",
	"$ShardProcessExit",
	"$ShardProcessFactory",
	"$ShardRequestHandler",
	"$ShardTransport",
	"$Sleep",
] as const;

describe("public package entry point", () => {
	test("exposes only the three supported runtime clients", () => {
		expect(Object.keys(sharding).sort()).toEqual(["BridgeClient", "HubClient", "ShardClient"]);
		expect(sharding.BridgeClient).toBeFunction();
		expect(sharding.HubClient).toBeFunction();
		expect(sharding.ShardClient).toBeFunction();
	});

	test("keeps user-facing type exports explicit and narrow", async () => {
		const source = await Bun.file(`${import.meta.dir}/../src/index.ts`).text();
		expect(source).not.toContain("export type *");
		const exportedTypes = [...source.matchAll(/\$[A-Za-z][A-Za-z0-9]*/g)].map((match) => match[0]).sort();
		expect(exportedTypes).toEqual([...PUBLIC_TYPE_EXPORTS].sort());

		const documentation = await Bun.file(`${import.meta.dir}/../docs/api-reference.md`).text();
		const publicTypeSection = documentation.split("## Public type groups", 2)[1];
		if (publicTypeSection === undefined) throw new Error("API reference is missing its public type groups.");
		const documentedTypes = [...publicTypeSection.matchAll(/\$[A-Za-z][A-Za-z0-9]*/g)].map((match) => match[0]).sort();
		expect(documentedTypes).toEqual([...PUBLIC_TYPE_EXPORTS].sort());
	});
});
