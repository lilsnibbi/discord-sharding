import { describe, expect, test } from "bun:test";
import { BridgeClient } from "../../src/bridge/BridgeClient";
import { ShardingConfigurationError, ShardingTimeoutError } from "../../src/errors/ShardingError";
import type { $Sleep } from "../../src/types/common";
import { createBridgeOptions, createHubHarness, FakeWebSocket, waitFor, waitForHubMessage } from "./client-harness";

describe("BridgeClient lifecycle and connection limits", () => {
	test("rejects unsafe process strings while retaining ordinary Unicode", () => {
		const options = createBridgeOptions("http://hub.test");
		expect(() => new BridgeClient({ ...options, args: ["bad\0argument"] })).toThrow(ShardingConfigurationError);
		expect(() => new BridgeClient({ ...options, cwd: "bad\ncwd" })).toThrow(ShardingConfigurationError);
		expect(() => new BridgeClient({ ...options, env: { "BAD=KEY": "value" } })).toThrow(ShardingConfigurationError);
		expect(() => new BridgeClient({ ...options, env: { VALID_KEY: "bad\0value" } })).toThrow(
			ShardingConfigurationError,
		);
		expect(
			() =>
				new BridgeClient({
					...options,
					args: ["こんにちは"],
					cwd: "C:\\ボット",
					env: { DISPLAY_NAME: "机器人" },
				}),
		).not.toThrow();
	});

	test("starts local resources without a Hub and stops its reconnect ownership", async () => {
		let sleepSignal: AbortSignal | undefined;
		let notifySleepStarted = (): void => undefined;
		const sleepStarted = new Promise<void>((resolve) => {
			notifySleepStarted = resolve;
		});
		const sleep: $Sleep = async (_milliseconds, signal) =>
			new Promise<void>((_resolve, reject) => {
				sleepSignal = signal;
				signal.addEventListener("abort", () => reject(signal.reason), { once: true });
				notifySleepStarted();
			});
		const bridge = new BridgeClient(
			createBridgeOptions("http://127.0.0.1:1", {
				requestMaxPending: 2,
				sleep,
				socketFactory: () => {
					throw new Error("Hub unavailable");
				},
			}),
		);

		await bridge.start();
		await sleepStarted;
		expect(sleepSignal).toBeInstanceOf(AbortSignal);
		expect(bridge.state).toBe("running");
		expect(bridge.isInMaintenance).toBe(true);
		await bridge.stop();
		expect(sleepSignal?.aborted).toBe(true);
		expect(bridge.state).toBe("stopped");
	});

	test("backs off repeated connections that close before synchronization", async () => {
		const delays: number[] = [];
		let releaseFinalSleep = (): void => undefined;
		let notifyFinalSleep = (): void => undefined;
		const finalSleep = new Promise<void>((resolve) => {
			notifyFinalSleep = resolve;
		});
		const sleep: $Sleep = async (milliseconds, signal) => {
			delays.push(milliseconds);
			if (delays.length < 3) return;
			notifyFinalSleep();
			await new Promise<void>((resolve, reject) => {
				releaseFinalSleep = resolve;
				signal.addEventListener("abort", () => reject(signal.reason), { once: true });
			});
		};
		const bridge = new BridgeClient({
			...createBridgeOptions("http://hub.test", {
				sleep,
				socketFactory: (url) => {
					const socket = new FakeWebSocket(url, [], () => false);
					queueMicrotask(() => {
						socket.open();
						queueMicrotask(() => socket.close(1008, "Authentication rejected"));
					});
					return socket;
				},
			}),
			reconnect: {
				initialDelayMs: 100,
				jitterRatio: 0,
				maxDelayMs: 400,
				multiplier: 2,
			},
		});
		try {
			await bridge.start();
			await finalSleep;
			expect(delays).toEqual([100, 200, 400]);
			expect(bridge.isInMaintenance).toBe(true);
		} finally {
			releaseFinalSleep();
			await bridge.stop();
		}
	});

	test("bounds pre-open and queued WebSocket work", async () => {
		const connectionErrors: Error[] = [];
		let notifyReconnect = (): void => undefined;
		const reconnectStarted = new Promise<void>((resolve) => {
			notifyReconnect = resolve;
		});
		const unopened = new BridgeClient({
			...createBridgeOptions("http://hub.test", {
				requestTimeoutMs: 5,
				sleep: async (_milliseconds, signal) => {
					notifyReconnect();
					await new Promise<void>((_resolve, reject) => {
						signal.addEventListener("abort", () => reject(signal.reason), { once: true });
					});
				},
				socketFactory: (url) => new FakeWebSocket(url, [], () => false),
			}),
			onError: (error) => connectionErrors.push(error),
		});
		await unopened.start();
		await reconnectStarted;
		expect(connectionErrors.some((error) => error instanceof ShardingTimeoutError)).toBe(true);
		await unopened.stop();

		const hub = createHubHarness();
		const bridge = new BridgeClient(
			createBridgeOptions(hub.url, {
				requestMaxPending: 2,
				socketFactory: hub.factory,
			}),
		);
		try {
			await bridge.start();
			await waitForHubMessage(hub, "bridge.hello");
			const syncData = {
				assignments: [],
				bridgeGeneration: bridge.generation,
				cluster: [],
				connectionGeneration: 1,
				topologyVersion: 1,
				totalShards: 2,
			} as const;
			hub.send("hub.sync", "sync:initial", syncData);
			await bridge.waitUntilConnected(1_000);
			await Bun.sleep(0);
			hub.send("hub.sync", "sync:queued:1", syncData);
			hub.send("hub.sync", "sync:queued:2", syncData);
			hub.send("hub.sync", "sync:queued:3", syncData);
			await waitFor(() => hub.closeCode() === 1013, "WebSocket queue backpressure");
		} finally {
			await bridge.stop();
			hub.close();
		}
	});
});
