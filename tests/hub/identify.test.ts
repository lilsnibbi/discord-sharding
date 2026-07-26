import { describe, expect, test } from "bun:test";
import { ShardingCapacityError, ShardingStateError } from "../../src/errors/ShardingError";
import { IdentifyScheduler } from "../../src/hub/identify";
import type { $Sleep } from "../../src/types/common";
import type { $GatewaySessionStartLimit } from "../../src/types/hub";

interface VirtualClock {
	readonly now: () => number;
	readonly sleep: $Sleep;
}

function createVirtualClock(): VirtualClock {
	let time = 0;
	return {
		now: () => time,
		sleep: async (milliseconds, signal) => {
			if (signal.aborted) throw signal.reason;
			time += milliseconds;
			await Promise.resolve();
			if (signal.aborted) throw signal.reason;
		},
	};
}

function sessionLimit(overrides: Partial<$GatewaySessionStartLimit> = {}): $GatewaySessionStartLimit {
	return {
		total: overrides.total ?? 10,
		remaining: overrides.remaining ?? 10,
		reset_after: overrides.reset_after ?? 60_000,
		max_concurrency: overrides.max_concurrency ?? 2,
	};
}

describe("IdentifyScheduler", () => {
	test("starts actual login operations in the same bucket at least five seconds apart", async () => {
		const clock = createVirtualClock();
		const scheduler = new IdentifyScheduler(sessionLimit(), clock);
		const grants: { readonly shardId: number; readonly at: number }[] = [];

		const first = scheduler.schedule(0, async () => {
			grants.push({ shardId: 0, at: clock.now() });
			return "first";
		});
		const second = scheduler.schedule(2, async () => {
			grants.push({ shardId: 2, at: clock.now() });
			return "second";
		});

		expect(await Promise.all([first, second])).toEqual(["first", "second"]);
		expect(grants).toEqual([
			{ shardId: 0, at: 0 },
			{ shardId: 2, at: 5_000 },
		]);
		scheduler.close();
	});

	test("allows different identify buckets in the same window", async () => {
		const clock = createVirtualClock();
		const scheduler = new IdentifyScheduler(sessionLimit(), clock);
		const grants: number[] = [];

		await Promise.all([
			scheduler.schedule(0, async () => {
				grants.push(clock.now());
				return 0;
			}),
			scheduler.schedule(1, async () => {
				grants.push(clock.now());
				return 1;
			}),
		]);

		expect(grants).toEqual([0, 0]);
		scheduler.close();
	});

	test("waits for the session allowance reset before granting another login", async () => {
		const clock = createVirtualClock();
		const scheduler = new IdentifyScheduler(sessionLimit({ total: 2, remaining: 1, reset_after: 20_000 }), clock);
		const grants: number[] = [];

		await Promise.all([
			scheduler.schedule(0, async () => {
				grants.push(clock.now());
				return 0;
			}),
			scheduler.schedule(1, async () => {
				grants.push(clock.now());
				return 1;
			}),
		]);

		expect(grants).toEqual([0, 20_000]);
		expect(scheduler.remaining).toBe(1);
		scheduler.close();
	});

	test("removes a cancelled operation while it waits for its bucket", async () => {
		let time = 0;
		let notifySleepStarted = (): void => undefined;
		const sleepStarted = new Promise<void>((resolve) => {
			notifySleepStarted = resolve;
		});
		const sleep: $Sleep = async (_milliseconds, signal) =>
			new Promise<void>((_resolve, reject) => {
				const abort = (): void => reject(signal.reason);
				signal.addEventListener("abort", abort, { once: true });
				notifySleepStarted();
				if (signal.aborted) abort();
			});
		const scheduler = new IdentifyScheduler(sessionLimit(), {
			now: () => time,
			sleep,
		});
		await scheduler.schedule(0, async () => 0);

		const controller = new AbortController();
		const waiting = scheduler.schedule(
			2,
			async () => {
				time += 5_000;
				return 2;
			},
			controller.signal,
		);
		await sleepStarted;
		controller.abort(new Error("cancelled by test"));

		await expect(waiting).rejects.toBeInstanceOf(ShardingStateError);
		expect(scheduler.pendingCount).toBe(0);
		scheduler.close();
	});

	test("bounds queued operations and rejects them during shutdown", async () => {
		let notifySleepStarted = (): void => undefined;
		const sleepStarted = new Promise<void>((resolve) => {
			notifySleepStarted = resolve;
		});
		const sleep: $Sleep = async (_milliseconds, signal) =>
			new Promise<void>((_resolve, reject) => {
				const abort = (): void => reject(signal.reason);
				signal.addEventListener("abort", abort, { once: true });
				notifySleepStarted();
				if (signal.aborted) abort();
			});
		const scheduler = new IdentifyScheduler(sessionLimit({ remaining: 0 }), {
			maxPending: 1,
			now: () => 0,
			sleep,
		});
		const pending = scheduler.schedule(0, async () => 0);
		await sleepStarted;

		await expect(scheduler.schedule(1, async () => 1)).rejects.toBeInstanceOf(ShardingCapacityError);
		scheduler.close();
		await expect(pending).rejects.toBeInstanceOf(ShardingStateError);
		expect(scheduler.pendingCount).toBe(0);
	});
});
