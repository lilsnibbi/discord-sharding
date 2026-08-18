import { ShardingCapacityError, ShardingProtocolError, ShardingStateError } from "../../errors/ShardingError";
import type { ShardIdentityData } from "../../protocol/types";
import type {
	$ClearAnalyticsOptions,
	$HubAssignment,
	$HubBridgeTopology,
	$HubClientOptions,
	$HubState,
	$HubTopology,
	$PersistedAssignment,
	$PersistedBridge,
	$PersistedShard,
} from "../../types/hub";
import type { IdentifyScheduler } from "../identify/IdentifyScheduler";
import type { $HubBridgeSessionSocketData, HubBridgeSession } from "../session/HubBridgeSession";
import { IDLE_ASSIGNMENT_MUTATION } from "./constants";
import { normalizeOptions } from "./options";
import type {
	HubPersistenceAdapter,
	LoadedState,
	NormalizedHubOptions,
	PendingEvaluation,
	PendingRoute,
	PendingStart,
	PendingStop,
	RestartHistory,
} from "./types";
import { assignmentMatches, identitiesEqual, isReleasedAssignment, toError } from "./utilities";

export abstract class HubCore {
	protected readonly options: NormalizedHubOptions;
	protected assignmentMutationCount = 0;
	protected assignmentMutationTail: Promise<void> = IDLE_ASSIGNMENT_MUTATION;
	protected readonly assignments = new Map<number, $PersistedAssignment>();
	protected readonly bridges = new Map<string, $PersistedBridge>();
	protected readonly bridgePersistenceTails = new Map<string, Promise<void>>();
	protected readonly shards = new Map<number, $PersistedShard>();
	protected readonly shardPersistenceTails = new Map<number, Promise<void>>();
	protected readonly sessions = new Map<string, HubBridgeSession>();
	protected readonly socketSessions = new Set<HubBridgeSession>();
	protected readonly lastConnections = new Map<
		string,
		{ readonly connectionGeneration: number; readonly generation: string }
	>();
	protected readonly pendingIds = new Set<string>();
	protected readonly pendingRoutes = new Map<string, PendingRoute>();
	protected readonly pendingStops = new Map<string, PendingStop>();
	protected readonly evaluations = new Map<string, PendingEvaluation>();
	protected readonly identifyKeys = new Set<string>();
	protected readonly pendingStarts = new Map<string, PendingStart>();
	protected readonly restartHistory = new Map<number, RestartHistory>();
	protected readonly restartTimers = new Map<number, ReturnType<typeof setTimeout>>();
	protected readonly startupLifecycle = new AbortController();
	protected identifyScheduler: IdentifyScheduler | undefined;
	protected persistence: HubPersistenceAdapter | undefined;
	protected reconcilePromise: Promise<void> | undefined;
	protected reconcileRequested = false;
	protected server: Bun.Server<$HubBridgeSessionSocketData> | undefined;
	protected startPromise: Promise<this> | undefined;
	protected lifecycleState: $HubState = "idle";
	protected stopPromise: Promise<void> | undefined;
	protected lastPersistenceTimestamp = -1;
	protected lastRestartClock = 0;
	protected topologyVersion = 1;
	protected shardCount = 0;

	/**
	 * Deletes retained Hub analytics.
	 *
	 * @param options - Optional cutoff and batch size.
	 */
	public abstract clearAnalytics(options?: $ClearAnalyticsOptions): Promise<number>;

	/**
	 * Rebalances shard assignments and returns the updated topology.
	 */
	public abstract reconcile(): Promise<$HubTopology>;

	/**
	 * Releases assignments held by a disconnected Bridge.
	 *
	 * @param bridgeId - Stable Bridge identifier.
	 */
	public abstract releaseBridge(bridgeId: string): Promise<readonly number[]>;
	protected abstract closeSessionAfterFailure(session: HubBridgeSession, cause: unknown, context?: string): void;

	/**
	 * Creates the shared Hub runtime without opening resources.
	 *
	 * @param options - Bot, authentication, database, server, and capacity settings.
	 */
	protected constructor(options: $HubClientOptions) {
		this.options = normalizeOptions(options);
	}

	/**
	 * Current Hub lifecycle state.
	 */
	public get state(): $HubState {
		return this.lifecycleState;
	}

	/**
	 * Listening URL after startup, or `null` before the server starts.
	 */
	public get url(): URL | null {
		return this.server?.url ?? null;
	}

	/**
	 * Global Discord shard count selected during startup.
	 *
	 * Reading this before startup completes throws a state error.
	 */
	public get totalShards(): number {
		if (this.shardCount === 0) throw new ShardingStateError("HubClient has not loaded Discord gateway metadata.");
		return this.shardCount;
	}

	/**
	 * Returns a read-only snapshot of assignments, Bridge capacity, and ready shards.
	 *
	 * @returns Current topology ordered by stable identifiers.
	 */
	public getTopology(): $HubTopology {
		this.ensureRunning();
		const assignments: $HubAssignment[] = [...this.assignments.values()]
			.filter((assignment) => !isReleasedAssignment(assignment))
			.sort((left, right) => left.shardId - right.shardId)
			.map((assignment) =>
				Object.freeze({
					bridgeId: assignment.bridgeId,
					epoch: assignment.epoch,
					shardId: assignment.shardId,
				}),
			);
		const assignedShardIdsByBridge = new Map<string, number[]>();
		for (const assignment of assignments) {
			const shardIds = assignedShardIdsByBridge.get(assignment.bridgeId);
			if (shardIds === undefined) {
				assignedShardIdsByBridge.set(assignment.bridgeId, [assignment.shardId]);
			} else {
				shardIds.push(assignment.shardId);
			}
		}
		const bridges: $HubBridgeTopology[] = [...this.bridges.values()]
			.sort((left, right) => left.id.localeCompare(right.id))
			.map((bridge) => {
				const session = this.sessions.get(bridge.id);
				const assignedShardIds = assignedShardIdsByBridge.get(bridge.id) ?? [];
				const readyShardIds =
					session?.phase === "ready"
						? [...session.runningShards.values()]
								.filter(
									(shard) => shard.ready && assignmentMatches(this.assignments.get(shard.shardId), bridge.id, shard),
								)
								.map((shard) => shard.shardId)
								.sort((left, right) => left - right)
						: [];
				return Object.freeze({
					assignedShardIds: Object.freeze(assignedShardIds),
					connected: session?.phase === "ready",
					generation: bridge.generation,
					id: bridge.id,
					maxShards: bridge.maxShards,
					readyShardIds: Object.freeze(readyShardIds),
				});
			});
		const assigned = new Set(assignments.map((assignment) => assignment.shardId));
		const unassignedShardIds: number[] = [];
		for (let shardId = 0; shardId < this.shardCount; shardId += 1) {
			if (!assigned.has(shardId)) unassignedShardIds.push(shardId);
		}
		return Object.freeze({
			assignments: Object.freeze(assignments),
			bridges: Object.freeze(bridges),
			generatedAt: this.readWallClock(),
			totalShards: this.shardCount,
			unassignedShardIds: Object.freeze(unassignedShardIds),
		});
	}

	protected requireReadySession(bridgeId: string): HubBridgeSession {
		const session = this.sessions.get(bridgeId);
		if (session === undefined || session.phase !== "ready") {
			throw new ShardingStateError(`Bridge ${bridgeId} is not synchronized.`);
		}
		return session;
	}

	protected requireCurrentReadySession(session: HubBridgeSession): void {
		if (session.phase !== "ready" || this.sessions.get(session.bridgeId) !== session) {
			throw new ShardingStateError(`Bridge ${session.bridgeId} disconnected during assignment reconciliation.`);
		}
	}

	protected isCurrentSession(session: HubBridgeSession): boolean {
		return (
			this.lifecycleState === "running" && session.phase !== "closed" && this.sessions.get(session.bridgeId) === session
		);
	}

	protected bridgeOwnsAssignments(bridgeId: string): boolean {
		for (const assignment of this.assignments.values()) {
			if (assignment.bridgeId === bridgeId) return true;
		}
		return false;
	}

	protected requireAssignedIdentity(session: HubBridgeSession, identity: ShardIdentityData): void {
		const assignment = this.assignments.get(identity.shardId);
		if (!assignmentMatches(assignment, session.bridgeId, identity)) {
			throw new ShardingProtocolError(`Shard ${identity.shardId} ownership is stale.`);
		}
	}

	protected requireSessionIdentity(
		session: HubBridgeSession,
		identity: ShardIdentityData,
		requireReady: boolean,
	): void {
		if (session.phase !== "ready") throw new ShardingStateError(`Bridge ${session.bridgeId} is in maintenance.`);
		this.requireAssignedIdentity(session, identity);
		const running = session.runningShards.get(identity.shardId);
		if (running === undefined || !identitiesEqual(running, identity) || (requireReady && !running.ready)) {
			throw new ShardingProtocolError(`Shard ${identity.shardId} process identity is stale or not ready.`);
		}
	}

	protected reserveOperationId(id: string): void {
		if (this.pendingIds.has(id)) throw new ShardingCapacityError(`Operation ${id} is already pending.`);
		if (this.pendingIds.size >= this.options.request.maxPending) {
			throw new ShardingCapacityError(
				`Hub pending operation capacity of ${this.options.request.maxPending} was reached.`,
			);
		}
		this.pendingIds.add(id);
	}

	protected async runAssignmentMutation<Result>(operation: () => Promise<Result>): Promise<Result> {
		if (this.assignmentMutationCount >= this.options.request.maxPending) {
			throw new ShardingCapacityError(
				`Hub assignment mutation capacity of ${this.options.request.maxPending} was reached.`,
			);
		}
		this.assignmentMutationCount += 1;
		const predecessor = this.assignmentMutationTail;
		const result = predecessor.then(async () => {
			this.ensureRunning();
			return operation();
		});
		const settled = result.then(
			() => undefined,
			() => undefined,
		);
		this.assignmentMutationTail = settled;
		try {
			return await result;
		} finally {
			this.assignmentMutationCount -= 1;
			if (this.assignmentMutationTail === settled) {
				this.assignmentMutationTail = IDLE_ASSIGNMENT_MUTATION;
			}
		}
	}

	protected saveBridge(bridge: $PersistedBridge): Promise<void> {
		return this.runPersistenceWrite(this.bridgePersistenceTails, bridge.id, () =>
			this.requirePersistence().saveBridge(bridge),
		);
	}

	protected saveShard(shard: $PersistedShard): Promise<void> {
		return this.runPersistenceWrite(this.shardPersistenceTails, shard.shardId, () =>
			this.requirePersistence().saveShard(shard),
		);
	}

	protected runPersistenceWrite<Key>(
		tails: Map<Key, Promise<void>>,
		key: Key,
		operation: () => Promise<void>,
	): Promise<void> {
		const predecessor = tails.get(key) ?? IDLE_ASSIGNMENT_MUTATION;
		const result = predecessor.then(operation);
		const settled = result.then(
			() => undefined,
			() => undefined,
		);
		tails.set(key, settled);
		void settled.then(() => {
			if (tails.get(key) === settled) tails.delete(key);
		});
		return result;
	}

	protected async waitForPersistenceWrites(): Promise<void> {
		while (this.bridgePersistenceTails.size > 0 || this.shardPersistenceTails.size > 0) {
			await Promise.all([...this.bridgePersistenceTails.values(), ...this.shardPersistenceTails.values()]);
		}
	}

	protected requestReconciliation(): void {
		if (this.lifecycleState !== "running") return;
		void this.reconcile().catch((cause: unknown) => this.report(toError(cause), "assignment reconciliation"));
	}

	protected advanceTopology(): void {
		if (this.topologyVersion >= Number.MAX_SAFE_INTEGER) {
			throw new ShardingStateError("Topology version is exhausted.");
		}
		this.topologyVersion += 1;
	}

	protected ensureRunning(): void {
		if (this.lifecycleState !== "running") throw new ShardingStateError(`HubClient is ${this.lifecycleState}.`);
	}

	protected requirePersistence(): HubPersistenceAdapter {
		if (this.persistence === undefined) throw new ShardingStateError("Hub persistence is unavailable.");
		return this.persistence;
	}

	protected requireIdentifyScheduler(): IdentifyScheduler {
		if (this.identifyScheduler === undefined) throw new ShardingStateError("Identify scheduler is unavailable.");
		return this.identifyScheduler;
	}

	protected readWallClock(): number {
		let sampled: number;
		try {
			sampled = this.options.wallClock();
		} catch (cause) {
			throw new ShardingStateError("Hub wall clock failed.", { cause });
		}
		if (!Number.isSafeInteger(sampled) || sampled < 0) {
			throw new ShardingStateError("Hub wall clock must return a non-negative safe integer.");
		}
		return sampled;
	}

	protected nextPersistenceTimestamp(): number {
		let value = this.readWallClock();
		if (value <= this.lastPersistenceTimestamp) {
			if (this.lastPersistenceTimestamp >= Number.MAX_SAFE_INTEGER) {
				throw new ShardingStateError("Hub wall clock exhausted the supported integer range.");
			}
			value = this.lastPersistenceTimestamp + 1;
		}
		this.lastPersistenceTimestamp = value;
		return value;
	}

	protected readRestartClock(): number {
		let value: number;
		try {
			value = this.options.now();
		} catch (cause) {
			throw new ShardingStateError("Hub monotonic clock failed.", { cause });
		}
		if (!Number.isFinite(value) || value < 0) {
			throw new ShardingStateError("Hub monotonic clock must return a non-negative finite number.");
		}
		if (value < this.lastRestartClock) {
			throw new ShardingStateError("Hub monotonic clock moved backwards.");
		}
		this.lastRestartClock = value;
		return value;
	}

	protected observeLoadedTimestamps(state: LoadedState): void {
		for (const assignment of state.assignments.values()) {
			this.lastPersistenceTimestamp = Math.max(this.lastPersistenceTimestamp, assignment.updatedAt);
		}
		for (const bridge of state.bridges.values()) {
			this.lastPersistenceTimestamp = Math.max(this.lastPersistenceTimestamp, bridge.updatedAt);
		}
		for (const shard of state.shards.values()) {
			this.lastPersistenceTimestamp = Math.max(this.lastPersistenceTimestamp, shard.updatedAt);
		}
	}

	protected report(error: Error, context: string): void {
		try {
			this.options.onError?.(error, context);
		} catch {
			// Error observers cannot recurse into Hub lifecycle.
		}
	}
}
