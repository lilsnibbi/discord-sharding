import { describe, expect, test } from "bun:test";
import { type ManagedShardOptions, ManagedShardProcess } from "../../src/bridge/shards/ManagedShardProcess";
import {
	ShardingCapacityError,
	ShardingStateError,
	ShardingTimeoutError,
	ShardingTransportError,
} from "../../src/errors/ShardingError";
import type {
	$BridgeShardState,
	$ShardProcess,
	$ShardProcessContext,
	$ShardProcessExit,
	$ShardProcessFactory,
} from "../../src/types/bridge";

interface ProcessHarness {
	readonly factory: $ShardProcessFactory;
	readonly finish: (exit?: $ShardProcessExit) => void;
	readonly killSignals: readonly (number | undefined)[];
	readonly sent: readonly object[];
	readonly setSendOperation: (operation: Promise<void> | undefined) => void;
	readonly context: () => $ShardProcessContext;
}

function createProcessHarness(): ProcessHarness {
	let context: $ShardProcessContext | undefined;
	let resolveExit = (_code: number): void => undefined;
	let finished = false;
	let killed = false;
	let sendOperation: Promise<void> | undefined;
	const sent: object[] = [];
	const killSignals: (number | undefined)[] = [];
	const exited = new Promise<number>((resolve) => {
		resolveExit = resolve;
	});
	const finish = (exit: $ShardProcessExit = { code: 0, signal: null }): void => {
		if (finished) return;
		finished = true;
		context?.callbacks.onExit(exit);
		resolveExit(exit.code ?? 0);
	};
	const factory: $ShardProcessFactory = (createdContext): $ShardProcess => {
		context = createdContext;
		return {
			exited,
			get killed(): boolean {
				return killed;
			},
			pid: 1234,
			kill(signal?: number): void {
				killed = true;
				killSignals.push(signal);
				finish({ code: null, signal: signal ?? null });
			},
			send(message: object): void | Promise<void> {
				sent.push(message);
				return sendOperation;
			},
		};
	};
	return {
		factory,
		finish,
		killSignals,
		sent,
		setSendOperation: (operation) => {
			sendOperation = operation;
		},
		context: () => {
			if (context === undefined) throw new Error("Process has not been created.");
			return context;
		},
	};
}

function managedOptions(harness: ProcessHarness, overrides: Partial<ManagedShardOptions> = {}): ManagedShardOptions {
	return {
		args: [],
		assignmentEpoch: 3,
		callbacks: {
			onExit: () => undefined,
			onMessage: () => undefined,
			onState: () => undefined,
		},
		environment: Object.freeze({ SHARDING_SHARD_ID: "2" }),
		maxPendingSends: 2,
		processFactory: harness.factory,
		processGeneration: 4,
		requestTimeoutMs: 100,
		script: "./shard.ts",
		shardId: 2,
		shutdownTimeoutMs: 10,
		startupTimeoutMs: 100,
		totalShards: 8,
		...overrides,
	};
}

describe("ManagedShardProcess", () => {
	test("passes immutable generation context and reports state and messages", () => {
		const harness = createProcessHarness();
		const states: $BridgeShardState[] = [];
		const messages: unknown[] = [];
		const exits: { readonly exit: $ShardProcessExit; readonly intentional: boolean }[] = [];
		const managed = new ManagedShardProcess(
			managedOptions(harness, {
				callbacks: {
					onExit: (_managed, exit, intentional) => exits.push({ exit, intentional }),
					onMessage: (_managed, message) => messages.push(message),
					onState: (_managed, state) => states.push(state),
				},
			}),
		);

		managed.start();
		const context = harness.context();
		expect(context).toMatchObject({
			assignmentEpoch: 3,
			processGeneration: 4,
			shardId: 2,
			totalShards: 8,
		});
		expect(Object.isFrozen(context)).toBe(true);
		context.callbacks.onMessage({ type: "booted" });
		managed.markReady();
		harness.finish({ code: 1, signal: null });

		expect(messages).toEqual([{ type: "booted" }]);
		expect(states).toEqual(["ready", "failed"]);
		expect(exits).toEqual([{ exit: { code: 1, signal: null }, intentional: false }]);
		expect(managed.state).toBe("failed");
		expect(() => managed.markReady()).toThrow(ShardingStateError);
	});

	test("retains send admission until the underlying operation settles", async () => {
		const harness = createProcessHarness();
		let resolveSend = (): void => undefined;
		const sendOperation = new Promise<void>((resolve) => {
			resolveSend = resolve;
		});
		harness.setSendOperation(sendOperation);
		const managed = new ManagedShardProcess(
			managedOptions(harness, {
				maxPendingSends: 1,
				requestTimeoutMs: 5,
			}),
		);
		managed.start();

		const first = managed.send({ type: "first" });
		await expect(first).rejects.toBeInstanceOf(ShardingTimeoutError);
		expect(managed.pendingSends).toBe(1);
		await expect(managed.send({ type: "second" })).rejects.toBeInstanceOf(ShardingCapacityError);
		resolveSend();
		await sendOperation;
		await Promise.resolve();
		expect(managed.pendingSends).toBe(0);
		harness.finish();
	});

	test("performs graceful idempotent shutdown and confirms process exit", async () => {
		const harness = createProcessHarness();
		harness.setSendOperation(undefined);
		const states: $BridgeShardState[] = [];
		const managed = new ManagedShardProcess(
			managedOptions(harness, {
				callbacks: {
					onExit: () => undefined,
					onMessage: () => undefined,
					onState: (_managed, state) => states.push(state),
				},
			}),
		);
		managed.start();
		const first = managed.stop({ type: "shutdown" });
		const second = managed.stop({ type: "ignored" });
		expect(first).toBe(second);
		queueMicrotask(() => harness.finish());
		await first;

		expect(harness.sent).toEqual([{ type: "shutdown" }]);
		expect(harness.killSignals).toEqual([]);
		expect(states).toEqual(["stopping", "stopped"]);
		expect(managed.state).toBe("stopped");
	});

	test("terminates a process that does not complete graceful shutdown", async () => {
		const harness = createProcessHarness();
		const managed = new ManagedShardProcess(
			managedOptions(harness, {
				shutdownTimeoutMs: 5,
			}),
		);
		managed.start();

		await managed.stop(undefined);
		expect(harness.killSignals).toEqual([undefined]);
		expect(managed.state).toBe("stopped");
	});

	test("force-terminates a child that ignores the startup-timeout soft signal", async () => {
		const killSignals: (number | undefined)[] = [];
		let resolveExit = (_code: number): void => undefined;
		const exited = new Promise<number>((resolve) => {
			resolveExit = resolve;
		});
		const harness = createProcessHarness();
		const managed = new ManagedShardProcess(
			managedOptions(harness, {
				processFactory: (context) => ({
					exited,
					killed: false,
					pid: 4321,
					kill(signal?: number): void {
						killSignals.push(signal);
						if (signal === 9) {
							context.callbacks.onExit({ code: null, signal });
							resolveExit(0);
						}
					},
					send(): void {},
				}),
				shutdownTimeoutMs: 5,
				startupTimeoutMs: 5,
			}),
		);

		managed.start();
		for (let attempt = 0; attempt < 50 && killSignals.length < 2; attempt += 1) await Bun.sleep(1);

		expect(killSignals).toEqual([undefined, 9]);
		expect(managed.state).toBe("failed");
	});

	test("retains a failed stop for a later cleanup retry", async () => {
		let resolveExit = (_code: number): void => undefined;
		let context: $ShardProcessContext | undefined;
		let killAttempts = 0;
		const exited = new Promise<number>((resolve) => {
			resolveExit = resolve;
		});
		const harness = createProcessHarness();
		const managed = new ManagedShardProcess(
			managedOptions(harness, {
				processFactory: (createdContext) => {
					context = createdContext;
					return {
						exited,
						killed: false,
						pid: 9876,
						kill(): void {
							killAttempts += 1;
							if (killAttempts <= 2) throw new Error("temporary termination failure");
							context?.callbacks.onExit({ code: 0, signal: null });
							resolveExit(0);
						},
						send(): void {},
					};
				},
				shutdownTimeoutMs: 5,
			}),
		);
		managed.start();

		await expect(managed.stop(undefined)).rejects.toBeInstanceOf(ShardingTransportError);
		await managed.stop(undefined);

		expect(killAttempts).toBe(3);
		expect(managed.state).toBe("stopped");
	});

	test("converts spawn and unavailable-process failures to domain errors", async () => {
		const harness = createProcessHarness();
		const managed = new ManagedShardProcess(
			managedOptions(harness, {
				processFactory: () => {
					throw new Error("spawn failed");
				},
			}),
		);

		expect(() => managed.start()).toThrow(ShardingTransportError);
		expect(managed.state).toBe("failed");
		await expect(managed.send({ type: "message" })).rejects.toBeInstanceOf(ShardingStateError);
	});
});
