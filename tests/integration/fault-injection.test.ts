import { describe, expect, test } from "bun:test";
import { ShardingRemoteError } from "../../src/errors/ShardingError";
import {
	type $DiscordClient,
	type $HubTopology,
	type $ShardMessageContext,
	type $ShardProcess,
	type $ShardProcessContext,
	type $ShardProcessExit,
	type $ShardProcessFactory,
	type $ShardTransport,
	BridgeClient,
	type HubClient,
	ShardClient,
} from "../../src/index";
import { DEFAULT_PAYLOAD_POLICY } from "../../src/internal/payload";
import { DEFAULT_RESTART_POLICY } from "../../src/internal/policies";
import { createWireMessage, encodeWireMessage, parseWireMessage } from "../../src/protocol/codec";
import {
	HUB_TO_BRIDGE_TYPES,
	type ParsedWireMessage,
	type WireDataMap,
	type WireMessageType,
} from "../../src/protocol/types";
import {
	BRIDGE_ID,
	createHub as createFakeHub,
	installFakeServe,
	MemoryHubPersistence,
	openBridge,
	requireNumberProperty,
	sendHello,
	waitFor as waitForFake,
} from "../hub/client-harness";
import {
	createHub,
	nativeSocket,
	type ReportedError,
	readAction,
	requireHubUrl,
	sortedReadyTopology,
	stopAll,
	waitFor,
} from "../utilities/runtime-stack";

/** Must match the bridgeToken configured by the runtime-stack Hub factory. */
const STACK_BRIDGE_TOKEN = "0123456789abcdef";
const REQUEST_BOMB_ACTION = "request-bomb";
const REQUEST_BOMB_MESSAGE = "Request handler exploded for fault injection.";
const LISTENER_BOMB_ACTION = "listener-bomb";
const LISTENER_BOMB_MESSAGE = "Message listener exploded for fault injection.";

let nextFaultPid = 60_000;

function observeHubEvents(hub: HubClient) {
	const log = {
		connected: [] as string[],
		disconnected: [] as string[],
		failedShardIds: [] as number[],
		stoppedShardIds: [] as number[],
		synchronized: [] as string[],
	};
	hub.events.on("bridgeConnected", (event) => {
		log.connected.push(`${event.bridgeId}#${event.connectionGeneration}`);
	});
	hub.events.on("bridgeDisconnected", (event) => void log.disconnected.push(event.bridgeId));
	hub.events.on("bridgeSynchronized", (event) => void log.synchronized.push(event.bridgeId));
	hub.events.on("shardFailed", (event) => void log.failedShardIds.push(event.shardId));
	hub.events.on("shardStopped", (event) => void log.stoppedShardIds.push(event.shardId));
	return log;
}

function assertSingleOwnership(topology: $HubTopology): void {
	const assignmentOwners = new Map<number, string>();
	for (const assignment of topology.assignments) {
		expect(assignmentOwners.has(assignment.shardId)).toBe(false);
		assignmentOwners.set(assignment.shardId, assignment.bridgeId);
	}
	const bridgeOwned = new Set<number>();
	for (const bridge of topology.bridges) {
		for (const shardId of bridge.assignedShardIds) {
			expect(bridgeOwned.has(shardId)).toBe(false);
			bridgeOwned.add(shardId);
			expect(assignmentOwners.get(shardId)).toBe(bridge.id);
		}
	}
}

function captureRejection(operation: Promise<unknown>): Promise<unknown> {
	return operation.then(() => undefined).catch((cause: unknown) => cause);
}

/** Protocol-level Bridge over one real WebSocket, used to stop at exact handshake points. */
class RawBridge {
	public readonly closed: Promise<number>;
	public readonly failures: Error[] = [];
	public readonly opened: Promise<void>;
	public readonly received: ParsedWireMessage[] = [];
	readonly #autopilot: boolean;
	readonly #hello: { bridgeGeneration: string; bridgeId: string; connectionGeneration: number };
	readonly #socket: WebSocket;
	#autoStates = 0;

	public constructor(hubUrl: URL, bridgeId: string, generation: string, connection: number, autopilot = false) {
		this.#autopilot = autopilot;
		this.#hello = { bridgeGeneration: generation, bridgeId, connectionGeneration: connection };
		const url = new URL(hubUrl);
		url.protocol = "ws:";
		url.pathname = "/bridge";
		this.#socket = nativeSocket(url.toString(), {
			Authorization: `Bearer ${STACK_BRIDGE_TOKEN}`,
			"X-Sharding-Bridge-Generation": generation,
			"X-Sharding-Bridge-Id": bridgeId,
			"X-Sharding-Connection-Generation": String(connection),
		});
		this.opened = new Promise((resolve) => this.#socket.addEventListener("open", () => resolve()));
		this.closed = new Promise((resolve) => this.#socket.addEventListener("close", (event) => resolve(event.code)));
		this.#socket.addEventListener("message", (event) => {
			try {
				this.#handleMessage(event.data);
			} catch (cause) {
				this.failures.push(cause instanceof Error ? cause : new Error("Raw Bridge message handling failed."));
			}
		});
	}

	public close(): void {
		this.#socket.close();
	}

	public send<Type extends WireMessageType>(type: Type, id: string, data: WireDataMap[Type]): void {
		const message = createWireMessage(type, id, data, DEFAULT_PAYLOAD_POLICY);
		this.#socket.send(encodeWireMessage(message, DEFAULT_PAYLOAD_POLICY));
	}

	public sendHello(maxShards: number): void {
		this.send("bridge.hello", `hello:${this.#hello.connectionGeneration}`, {
			...this.#hello,
			maxShards,
			restartPolicy: DEFAULT_RESTART_POLICY,
			runningShards: [],
		});
	}

	public waitForType(type: WireMessageType): Promise<void> {
		return waitFor(() => this.received.some((message) => message.type === type), `a ${type} message from the Hub`);
	}

	#handleMessage(value: unknown): void {
		const message = parseWireMessage(value, HUB_TO_BRIDGE_TYPES, DEFAULT_PAYLOAD_POLICY);
		this.received.push(message);
		if (!this.#autopilot) return;
		if (message.type === "hub.sync") {
			this.send("bridge.sync.ready", message.id, {
				topologyVersion: requireNumberProperty(message.data, "topologyVersion"),
			});
			return;
		}
		if (message.type === "hub.shard.start") {
			this.#autoStates += 1;
			this.send("bridge.shard.state", `state:auto:${this.#autoStates}`, {
				assignmentEpoch: requireNumberProperty(message.data, "assignmentEpoch"),
				processGeneration: 1,
				shardId: requireNumberProperty(message.data, "shardId"),
				state: "ready",
			});
		}
	}
}

class FaultDiscordClient implements $DiscordClient {
	public destroy = (): void => undefined;
	public isReady = (): boolean => true;
	public login = (): Promise<string> => Promise.resolve("fault-token");
}

/** In-process shard hosting a real ShardClient whose developer handlers throw on demand. */
class FaultShardProcess implements $ShardProcess {
	public readonly client: ShardClient<FaultDiscordClient>;
	public readonly exited: Promise<number>;
	public killed = false;
	public readonly pid: number;
	readonly #context: $ShardProcessContext;
	readonly #resolveExit: (code: number) => void;
	#finished = false;
	#listener: ((message: unknown) => void) | undefined;

	public constructor(context: $ShardProcessContext, fleet: FaultFleet) {
		this.#context = context;
		this.pid = nextFaultPid += 1;
		const exit = Promise.withResolvers<number>();
		this.exited = exit.promise;
		this.#resolveExit = exit.resolve;
		const transport: $ShardTransport = {
			onMessage: (listener) => {
				this.#listener = listener;
				return (): void => undefined;
			},
			send: (message): void => {
				if (this.#finished) throw new Error("Fault shard transport is closed.");
				context.callbacks.onMessage(message);
			},
		};
		this.client = new ShardClient(new FaultDiscordClient(), {
			analyticsIntervalMs: false,
			assignmentEpoch: context.assignmentEpoch,
			onRequest: (payload) => fleet.handleRequest(context.shardId, payload),
			processGeneration: context.processGeneration,
			readyPollIntervalMs: 100,
			request: { maxPending: 64, timeoutMs: 1_000 },
			shardId: context.shardId,
			totalShards: context.totalShards,
			transport,
		});
		this.client.onMessage((payload, messageContext) => fleet.handleMessage(context.shardId, payload, messageContext));
		queueMicrotask(() => {
			void this.client.start().catch((cause: unknown) => {
				fleet.errors.push(cause instanceof Error ? cause : new Error("Fault shard startup failed.", { cause }));
				this.#finish({ code: 1, signal: null });
			});
		});
	}

	public kill(signal?: number): void {
		if (this.#finished) return;
		this.killed = true;
		void this.client.close();
		this.#finish({ code: null, signal: signal ?? null });
	}

	public send(message: object): void | Promise<void> {
		if (this.#finished) throw new Error("Fault shard process has exited.");
		this.#listener?.(message);
		if (Reflect.get(message, "type") === "shard.control.shutdown") return this.#finishAfterShutdown();
	}

	async #finishAfterShutdown(): Promise<void> {
		for (let attempt = 0; attempt < 500; attempt += 1) {
			if (this.client.state === "closed") {
				this.#finish({ code: 0, signal: null });
				return;
			}
			await Bun.sleep(1);
		}
		throw new Error(`Fault shard ${this.#context.shardId} did not finish its authorized shutdown.`);
	}

	#finish(exit: $ShardProcessExit): void {
		if (this.#finished) return;
		this.#finished = true;
		this.#context.callbacks.onExit(exit);
		this.#resolveExit(exit.code ?? 1);
	}
}

class FaultFleet {
	public readonly clients = new Map<number, ShardClient<FaultDiscordClient>>();
	public readonly deliveries: Array<{ action: string; shardId: number; sourceShardId: number | null }> = [];
	public readonly errors: Error[] = [];
	public readonly processes = new Map<number, FaultShardProcess>();
	public readonly factory: $ShardProcessFactory = (context): $ShardProcess => {
		const processHandle = new FaultShardProcess(context, this);
		this.clients.set(context.shardId, processHandle.client);
		this.processes.set(context.shardId, processHandle);
		return processHandle;
	};

	public handleRequest(shardId: number, payload: unknown): unknown {
		const action = readAction(payload);
		if (action === REQUEST_BOMB_ACTION) throw new Error(REQUEST_BOMB_MESSAGE);
		return { action, shardId };
	}

	public handleMessage(shardId: number, payload: unknown, context: $ShardMessageContext): void {
		const action = readAction(payload);
		this.deliveries.push({ action, shardId, sourceShardId: context.sourceShardId });
		if (action === LISTENER_BOMB_ACTION) throw new Error(LISTENER_BOMB_MESSAGE);
	}
}

function createFaultBridge(hubUrl: URL, id: string, fleet: FaultFleet, errors: ReportedError[]): BridgeClient {
	return new BridgeClient({
		analyticsPath: ":memory:",
		hubUrl,
		id,
		maxShards: 2,
		onError: (error, context) => errors.push({ context, error }),
		processFactory: fleet.factory,
		random: () => 0.5,
		reconnect: { initialDelayMs: 100, jitterRatio: 0, maxDelayMs: 100, multiplier: 1 },
		request: { maxPending: 64, timeoutMs: 1_000 },
		restart: { initialDelayMs: 10_000, maxAttempts: 1, maxDelayMs: 10_000, windowMs: 60_000 },
		shardScript: "./unused-fault-shard.ts",
		shutdownTimeoutMs: 500,
		startupTimeoutMs: 1_000,
		token: STACK_BRIDGE_TOKEN,
	});
}

function requireFaultClient(fleet: FaultFleet, shardId: number): ShardClient<FaultDiscordClient> {
	const client = fleet.clients.get(shardId);
	if (client === undefined) throw new Error(`Shard ${shardId} has no fault client.`);
	return client;
}

describe("fault injection across the Hub, Bridge, and shard stack", () => {
	test("cleans up Bridge sockets killed before hello and accepts a well-behaved successor", async () => {
		const hubErrors: ReportedError[] = [];
		const bridgeErrors: ReportedError[] = [];
		const hub = createHub(1, hubErrors);
		const log = observeHubEvents(hub);
		const fleet = new FaultFleet();
		let bridge: BridgeClient | undefined;
		try {
			await hub.start();
			const hubUrl = requireHubUrl(hub);

			const silent = new RawBridge(hubUrl, "bridge-fault-live", "generation-half", 1);
			await silent.opened;
			silent.close();
			await silent.closed;
			const speaker = new RawBridge(hubUrl, "bridge-fault-live", "generation-half", 1);
			await speaker.opened;
			speaker.send("bridge.heartbeat", "heartbeat:early", {
				bridgeGeneration: "generation-half",
				connectionGeneration: 1,
				sentAt: 1,
			});
			expect(await speaker.closed).toBe(1002);
			expect(hub.state).toBe("running");
			expect(hub.getTopology().bridges).toEqual([]);
			expect(log.connected).toEqual([]);
			expect(hubErrors).toHaveLength(1);
			expect(hubErrors[0]?.error.message).toContain("hello before other messages");

			bridge = createFaultBridge(hubUrl, "bridge-fault-live", fleet, bridgeErrors);
			await bridge.start();
			await bridge.waitUntilConnected(3_000);
			await waitFor(() => sortedReadyTopology(hub).length === 1, "the successor Bridge shard readiness");

			const topology = hub.getTopology();
			assertSingleOwnership(topology);
			expect(topology.bridges.map((entry) => entry.id)).toEqual(["bridge-fault-live"]);
			expect(topology.assignments.map((entry) => `${entry.shardId}:${entry.bridgeId}`)).toEqual([
				"0:bridge-fault-live",
			]);
			expect(log.connected).toEqual(["bridge-fault-live#1"]);
			expect(bridgeErrors).toEqual([]);
			expect(fleet.errors).toEqual([]);
		} finally {
			await stopAll(...(bridge === undefined ? [] : [bridge]), hub);
		}
	}, 10_000);

	test("recovers sole ownership after a Bridge dies between hello and sync acknowledgement", async () => {
		const hubErrors: ReportedError[] = [];
		const hub = createHub(2, hubErrors);
		const log = observeHubEvents(hub);
		const bridgeId = "bridge-fault-sync";
		let reconnected: RawBridge | undefined;
		try {
			await hub.start();
			const hubUrl = requireHubUrl(hub);

			const halfSynchronized = new RawBridge(hubUrl, bridgeId, "gen-sync", 1);
			await halfSynchronized.opened;
			halfSynchronized.sendHello(2);
			await halfSynchronized.waitForType("hub.sync");
			halfSynchronized.close();
			await halfSynchronized.closed;
			await waitFor(() => log.disconnected.length === 1, "the half-synchronized Bridge disconnect");
			await waitFor(
				() => hubErrors.some((entry) => entry.context.includes("topology sync")),
				"the sync failure report",
			);
			expect(log.connected).toEqual([`${bridgeId}#1`]);
			expect(log.synchronized).toEqual([]);
			const between = hub.getTopology();
			expect(between.assignments).toEqual([]);
			expect(between.bridges.map((entry) => [entry.id, entry.connected])).toEqual([[bridgeId, false]]);

			const stale = new RawBridge(hubUrl, bridgeId, "gen-sync", 1);
			await stale.opened;
			stale.sendHello(2);
			expect(await stale.closed).toBe(1002);
			reconnected = new RawBridge(hubUrl, bridgeId, "gen-sync", 2, true);
			await reconnected.opened;
			reconnected.sendHello(2);
			await waitFor(() => {
				const entry = hub.getTopology().bridges.find((candidate) => candidate.id === bridgeId);
				return entry?.connected === true && entry.readyShardIds.length === 2;
			}, "the reconnected Bridge to own both ready shards");

			const topology = hub.getTopology();
			assertSingleOwnership(topology);
			expect(topology.assignments.map((entry) => entry.shardId)).toEqual([0, 1]);
			expect(topology.assignments.every((entry) => entry.bridgeId === bridgeId)).toBe(true);
			expect(log.connected).toEqual([`${bridgeId}#1`, `${bridgeId}#2`]);
			expect(log.synchronized).toContain(bridgeId);
			expect(reconnected.failures).toEqual([]);
		} finally {
			reconnected?.close();
			await stopAll(hub);
		}
	}, 10_000);

	test("contains throwing request handlers and message listeners without killing the destination", async () => {
		const hubErrors: ReportedError[] = [];
		const bridgeErrors: ReportedError[] = [];
		const hub = createHub(2, hubErrors);
		const log = observeHubEvents(hub);
		const fleet = new FaultFleet();
		let bridge: BridgeClient | undefined;
		try {
			await hub.start();
			bridge = createFaultBridge(requireHubUrl(hub), "bridge-fault-dev", fleet, bridgeErrors);
			await bridge.start();
			await bridge.waitUntilConnected(3_000);
			await waitFor(() => sortedReadyTopology(hub).length === 2, "both fault-injection shards to become ready");
			const source = requireFaultClient(fleet, 0);

			const requestFailure = await captureRejection(source.request(1, { action: REQUEST_BOMB_ACTION }));
			if (!(requestFailure instanceof ShardingRemoteError)) {
				throw new Error("A throwing request handler must reject the caller with a REMOTE error.");
			}
			expect(requestFailure.message).toContain(REQUEST_BOMB_MESSAGE);
			expect(await source.request<object>(1, { action: "after-bomb" })).toEqual({ action: "after-bomb", shardId: 1 });
			const sendFailure = await captureRejection(source.send(1, { action: LISTENER_BOMB_ACTION }));
			expect(fleet.deliveries).toContainEqual({ action: LISTENER_BOMB_ACTION, shardId: 1, sourceShardId: 0 });
			if (!(sendFailure instanceof ShardingRemoteError)) {
				throw new Error("A throwing message listener must surface to the sender as a REMOTE error.");
			}
			expect(sendFailure.message).toContain(LISTENER_BOMB_MESSAGE);
			await source.send(1, { action: "after-bomb-message" });
			await waitFor(
				() => fleet.deliveries.some((delivery) => delivery.action === "after-bomb-message"),
				"delivery after the listener failure",
			);
			expect(await source.request<object>(1, { action: "still-alive" })).toEqual({ action: "still-alive", shardId: 1 });
			expect(fleet.processes.get(1)?.killed).toBe(false);
			expect(requireFaultClient(fleet, 1).state).toBe("running");
			expect(sortedReadyTopology(hub)).toEqual([0, 1]);
			expect(bridge.connected).toBe(true);
			expect(log.failedShardIds).toEqual([]);
			expect(log.stoppedShardIds).toEqual([]);
			expect(fleet.errors).toEqual([]);
			expect(hubErrors).toEqual([]);
			expect(bridgeErrors).toEqual([]);
		} finally {
			await stopAll(...(bridge === undefined ? [] : [bridge]), hub);
		}
	}, 10_000);

	test("rejects a Bridge asserting a shard it never owned and keeps the original owner", async () => {
		const installed = installFakeServe();
		const hub = createFakeHub(new MemoryHubPersistence());
		try {
			await hub.start();
			const owner = await openBridge(installed.server, 1, true);
			sendHello(owner);
			const start = await owner.waitForMessage("hub.shard.start");
			const epoch = requireNumberProperty(start.data, "assignmentEpoch");
			const readyState = { assignmentEpoch: epoch, processGeneration: 1, shardId: 0, state: "ready" } as const;
			owner.send("bridge.shard.state", "state:owner:ready", readyState);
			await waitForFake(
				() => hub.getTopology().bridges.find((entry) => entry.id === BRIDGE_ID)?.readyShardIds.length === 1,
				"the owning Bridge ready shard",
			);

			const imposter = await openBridge(installed.server, 1, true, "generation-b", "bridge-b");
			sendHello(imposter, {
				bridgeGeneration: "generation-b",
				bridgeId: "bridge-b",
				runningShards: [{ assignmentEpoch: epoch, processGeneration: 1, ready: true, shardId: 0 }],
			});
			await waitForFake(
				() => hub.getTopology().bridges.some((entry) => entry.id === "bridge-b" && entry.connected),
				"the imposter Bridge synchronization",
			);
			const beforeAssertion = hub.getTopology();
			assertSingleOwnership(beforeAssertion);
			expect(beforeAssertion.assignments).toEqual([{ bridgeId: BRIDGE_ID, epoch, shardId: 0 }]);
			expect(beforeAssertion.bridges.find((entry) => entry.id === "bridge-b")?.assignedShardIds).toEqual([]);

			imposter.send("bridge.shard.state", "state:imposter:ready", readyState);
			await waitForFake(() => imposter.socket.closeCode !== undefined, "the imposter Bridge close");
			expect(imposter.socket.closeCode).toBe(1002);
			expect(owner.socket.readyState).toBe(WebSocket.OPEN);
			const after = hub.getTopology();
			assertSingleOwnership(after);
			expect(after.assignments).toEqual([{ bridgeId: BRIDGE_ID, epoch, shardId: 0 }]);
			expect(after.bridges.find((entry) => entry.id === BRIDGE_ID)?.readyShardIds).toEqual([0]);
		} finally {
			await hub.stop();
			installed.restore();
		}
	});
});
