import { describe, expect, test } from "bun:test";
import {
	BRIDGE_GENERATION,
	type BridgeHarness,
	createDeferred,
	createHub,
	type Deferred,
	type FakeHubServer,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	sendHello,
	waitFor,
} from "./client-harness";

describe("HubClient failure handling", () => {
	test("uses distinct close codes for protocol, capacity, and internal failures", async () => {
		const cases: ReadonlyArray<{
			readonly expectedCode: number;
			readonly prepare?: (persistence: MemoryHubPersistence) => Deferred<void> | undefined;
			readonly send: (bridge: BridgeHarness, gate: Deferred<void> | undefined) => Promise<void>;
		}> = [
			{
				expectedCode: 1002,
				send: (bridge) => {
					installedServer().message(bridge.socket, "{");
					return Promise.resolve();
				},
			},
			{
				expectedCode: 1013,
				prepare: (persistence) => {
					const started = createDeferred<void>();
					const release = createDeferred<void>();
					persistence.saveBridgeOperation = async (bridge) => {
						if (!bridge.connected) return;
						started.resolve();
						await release.promise;
					};
					capacityStarted = started;
					return release;
				},
				send: async (bridge) => {
					sendHello(bridge);
					await capacityStarted?.promise;
					bridge.send("bridge.heartbeat", "heartbeat:1", {
						bridgeGeneration: BRIDGE_GENERATION,
						connectionGeneration: 1,
						sentAt: 1,
					});
				},
			},
			{
				expectedCode: 1011,
				prepare: (persistence) => {
					persistence.saveBridgeOperation = (bridge) =>
						bridge.connected ? Promise.reject(new Error("Storage failed.")) : Promise.resolve();
					return undefined;
				},
				send: (bridge) => {
					sendHello(bridge);
					return Promise.resolve();
				},
			},
		];
		let activeServer: FakeHubServer | undefined;
		let capacityStarted: Deferred<void> | undefined;
		const installedServer = (): FakeHubServer => {
			if (activeServer === undefined) throw new Error("Fake server is not active.");
			return activeServer;
		};
		for (const entry of cases) {
			const installed = installFakeServe();
			activeServer = installed.server;
			const persistence = new MemoryHubPersistence();
			const gate = entry.prepare?.(persistence);
			const hub = createHub(persistence, { maxPending: entry.expectedCode === 1013 ? 1 : 8 });
			try {
				await hub.start();
				const bridge = await openBridge(installed.server);
				await entry.send(bridge, gate);
				await waitFor(() => bridge.socket.closeCode !== undefined, `WebSocket close ${entry.expectedCode}`);
				expect(bridge.socket.closeCode).toBe(entry.expectedCode);
			} finally {
				gate?.resolve();
				await hub.stop();
				installed.restore();
				activeServer = undefined;
				capacityStarted = undefined;
			}
		}
	});
});
