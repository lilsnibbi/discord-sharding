import { describe, expect, test } from "bun:test";
import {
	ShardingCapacityError,
	ShardingConfigurationError,
	ShardingProtocolError,
	ShardingStateError,
	ShardingTimeoutError,
} from "../../src/errors/ShardingError";
import { HubBridgeSession } from "../../src/hub/session/HubBridgeSession";
import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { DEFAULT_RESTART_POLICY } from "../../src/internal/policies";

interface Deferred {
	readonly promise: Promise<void>;
	readonly resolve: () => void;
}

class FakeSessionSocket {
	public bufferedAmount = 0;
	public closeCode: number | undefined;
	public closeReason: string | undefined;
	public readyState: number = WebSocket.OPEN;
	public readonly sent: string[] = [];
	public sendStatus = 1;

	public close(code = 1000, reason = ""): void {
		this.readyState = WebSocket.CLOSED;
		this.closeCode = code;
		this.closeReason = reason;
	}

	public getBufferedAmount(): number {
		return this.bufferedAmount;
	}

	public send(data: string): number {
		this.sent.push(data);
		return this.sendStatus;
	}
}

function createSession(
	socket: FakeSessionSocket,
	options: {
		readonly maxInboundMessages?: number;
		readonly maxQueuedMessages?: number;
		readonly syncTimeoutMs?: number;
	} = {},
): HubBridgeSession {
	return new HubBridgeSession(
		socket,
		{
			bridgeGeneration: "generation-a",
			bridgeId: "bridge-a",
			connectionGeneration: 1,
		},
		DEFAULT_PAYLOAD_POLICY,
		1_048_576,
		options.maxQueuedMessages ?? 2,
		options.maxInboundMessages ?? 2,
		options.syncTimeoutMs ?? 100,
		1_000,
	);
}

function createDeferred(): Deferred {
	let settle = (): void => {
		throw new Error("Deferred promise was resolved before initialization.");
	};
	const promise = new Promise<void>((resolve) => {
		settle = resolve;
	});
	return { promise, resolve: settle };
}

describe("HubBridgeSession inbound ownership", () => {
	test("runs inbound operations serially and enforces bounded admission", async () => {
		const socket = new FakeSessionSocket();
		const session = createSession(socket);
		const gate = createDeferred();
		const order: string[] = [];
		session.enqueueInbound(async () => {
			order.push("first:start");
			await gate.promise;
			order.push("first:end");
		});
		session.enqueueInbound(() => {
			order.push("second");
			return Promise.resolve();
		});

		expect(session.inboundMessages).toBe(2);
		expect(() => session.enqueueInbound(() => Promise.resolve())).toThrow(ShardingCapacityError);
		expect(order).toEqual(["first:start"]);
		gate.resolve();
		await session.waitForInboundIdle();
		expect(order).toEqual(["first:start", "first:end", "second"]);
		expect(session.inboundMessages).toBe(0);
		session.finish(new ShardingStateError("Test complete."));
	});

	test("drops queued operations on finish while allowing active cleanup to settle", async () => {
		const socket = new FakeSessionSocket();
		const session = createSession(socket);
		const gate = createDeferred();
		const order: string[] = [];
		session.enqueueInbound(async () => {
			order.push("active");
			await gate.promise;
			order.push("settled");
		});
		session.enqueueInbound(() => {
			order.push("queued");
			return Promise.resolve();
		});

		session.finish(new ShardingStateError("Session closed."));
		expect(session.inboundMessages).toBe(0);
		gate.resolve();
		await session.waitForInboundIdle();
		expect(order).toEqual(["active", "settled"]);
		expect(session.phase).toBe("closed");
	});
});

describe("HubBridgeSession transport lifecycle", () => {
	test("selects distinct WebSocket close codes for failure categories", () => {
		const cases: ReadonlyArray<{ readonly code: number; readonly error: Error }> = [
			{ code: 1002, error: new ShardingProtocolError("Invalid protocol.") },
			{ code: 1002, error: new ShardingConfigurationError("Invalid wire value.") },
			{ code: 1008, error: new ShardingStateError("Invalid operation state.") },
			{ code: 1013, error: new ShardingCapacityError("Capacity reached.") },
			{ code: 1011, error: new Error("Unexpected failure.") },
		];
		for (const entry of cases) {
			const socket = new FakeSessionSocket();
			const session = createSession(socket);
			session.fail(entry.error);
			expect(socket.closeCode).toBe(entry.code);
			expect(session.phase).toBe("closed");
		}
	});

	test("flushes queued outbound messages when backpressure drains", () => {
		const socket = new FakeSessionSocket();
		socket.sendStatus = -1;
		const session = createSession(socket, { maxQueuedMessages: 1 });
		session.send("hub.identify.response", "identify:1", {
			granted: true,
			shardId: 0,
		});
		session.send("hub.identify.response", "identify:2", {
			granted: true,
			shardId: 1,
		});
		expect(socket.sent).toHaveLength(1);

		socket.sendStatus = 1;
		session.drain();
		expect(socket.sent).toHaveLength(2);
		session.finish(new ShardingStateError("Test complete."));
	});

	test("closes and clears the outbound queue when backpressure exceeds its bound", () => {
		const socket = new FakeSessionSocket();
		socket.sendStatus = -1;
		const session = createSession(socket, { maxQueuedMessages: 1 });
		session.send("hub.identify.response", "identify:1", {
			granted: true,
			shardId: 0,
		});
		session.send("hub.identify.response", "identify:2", {
			granted: true,
			shardId: 1,
		});

		expect(() =>
			session.send("hub.identify.response", "identify:3", {
				granted: true,
				shardId: 2,
			}),
		).toThrow(ShardingCapacityError);
		expect(socket.closeCode).toBe(1013);
		expect(session.phase).toBe("closed");
		socket.sendStatus = 1;
		session.drain();
		expect(socket.sent).toHaveLength(1);
	});

	test("rejects and closes a synchronization that is not acknowledged", async () => {
		const socket = new FakeSessionSocket();
		const session = createSession(socket, { syncTimeoutMs: 5 });
		session.acceptHello(1, DEFAULT_RESTART_POLICY, []);
		const synchronization = session.beginSynchronization("sync:1", 2);

		await expect(synchronization).rejects.toBeInstanceOf(ShardingTimeoutError);
		expect(socket.closeCode).toBe(1002);
		expect(session.phase).toBe("closed");
	});
});
