import { ShardingStateError } from "../../errors/ShardingError";
import { assertConfigurationKeys, snapshotConfigurationRecord } from "../../internal/configuration";
import { requireIdentifier, requireNonNegativeInteger, requirePositiveInteger } from "../../internal/validation";
import type { $ClearAnalyticsOptions, $HubTopology, $PersistedAssignment, $PersistedBridge } from "../../types/hub";
import { SQLiteHubPersistence } from "../database/SQLiteHubPersistence";
import { loadGatewayBotInfo } from "../gatewayStartup";
import { IdentifyScheduler } from "../identify/IdentifyScheduler";
import { DEFAULT_ANALYTICS_BATCH_SIZE, IDLE_ASSIGNMENT_MUTATION } from "./constants";
import { HubServerController } from "./HubServerController";
import { normalizeLoadedState } from "./loadedState";
import { releasedAssignmentOwner, toError } from "./utilities";

export abstract class HubLifecycle extends HubServerController implements AsyncDisposable {
	/**
	 * Prepares storage, loads Discord shard limits, and starts the Hub server.
	 *
	 * @returns This Hub after it begins accepting connections.
	 */
	public async start(): Promise<this> {
		if (this.lifecycleState === "running") return this;
		if (this.lifecycleState === "starting") {
			const operation = this.startPromise;
			if (operation === undefined) throw new ShardingStateError("HubClient startup operation is unavailable.");
			return operation;
		}
		if (this.lifecycleState !== "idle")
			throw new ShardingStateError(`Cannot start HubClient from ${this.lifecycleState}.`);
		this.lifecycleState = "starting";
		const operation = this.performStart();
		this.startPromise = operation;
		return operation;
	}

	protected async performStart(): Promise<this> {
		try {
			const persistence = this.options.persistence ?? new SQLiteHubPersistence(this.options.databasePath);
			this.persistence = persistence;
			await persistence.migrate();
			const [stateResult, gatewayResult] = await Promise.allSettled([
				persistence.loadState(),
				loadGatewayBotInfo(this.options.botToken, {
					endpoint: this.options.gatewayEndpoint,
					...(this.options.fetch === undefined ? {} : { fetch: this.options.fetch }),
					signal: this.startupLifecycle.signal,
					sleep: this.options.sleep,
				}),
			]);
			const startupFailures: unknown[] = [];
			if (stateResult.status === "rejected") startupFailures.push(stateResult.reason);
			if (gatewayResult.status === "rejected") startupFailures.push(gatewayResult.reason);
			if (startupFailures.length === 1) throw startupFailures[0];
			if (startupFailures.length > 1) {
				throw new AggregateError(startupFailures, "Hub state and Discord gateway metadata both failed to load.");
			}
			if (stateResult.status !== "fulfilled" || gatewayResult.status !== "fulfilled") {
				throw new ShardingStateError("Hub startup results were unavailable.");
			}
			const rawState = stateResult.value;
			const gateway = gatewayResult.value;
			this.shardCount = this.options.totalShards ?? gateway.shards;
			const loaded = normalizeLoadedState(rawState, this.shardCount);
			this.observeLoadedTimestamps(loaded);
			for (const assignment of loaded.assignments.values()) this.assignments.set(assignment.shardId, assignment);
			for (const bridge of loaded.bridges.values()) {
				const disconnected: $PersistedBridge = Object.freeze({
					...bridge,
					connected: false,
					updatedAt: this.nextPersistenceTimestamp(),
				});
				this.bridges.set(bridge.id, disconnected);
				await this.saveBridge(disconnected);
			}
			for (const shard of loaded.shards.values()) this.shards.set(shard.shardId, shard);
			this.identifyScheduler = new IdentifyScheduler(gateway.session_start_limit, {
				maxPending: this.options.request.maxPending,
				now: this.options.now,
				sleep: this.options.sleep,
			});
			this.server = this.createServer();
			this.lifecycleState = "running";
			return this;
		} catch (cause) {
			this.lifecycleState = "failed";
			await this.rollbackStartup(cause);
			throw cause;
		}
	}

	/**
	 * Rebalances assignments one shard at a time until capacity is stable.
	 *
	 * @returns Updated topology after all queued changes finish.
	 */
	public override async reconcile(): Promise<$HubTopology> {
		this.ensureRunning();
		this.reconcileRequested = true;
		if (this.reconcilePromise === undefined) {
			this.reconcilePromise = this.runAssignmentMutation(() => this.runReconciliation());
		}
		const operation = this.reconcilePromise;
		try {
			await operation;
		} finally {
			if (this.reconcilePromise === operation) this.reconcilePromise = undefined;
		}
		return this.getTopology();
	}

	/**
	 * Releases assignments held by a disconnected Bridge.
	 *
	 * Call this only after confirming the old deployment and its Discord sessions
	 * have stopped. A connected Bridge cannot be released.
	 *
	 * @param bridgeId - Stable disconnected Bridge identifier.
	 * @returns Shard identifiers that were released.
	 */
	public override async releaseBridge(bridgeId: string): Promise<readonly number[]> {
		this.ensureRunning();
		const normalizedId = requireIdentifier(bridgeId, "bridgeId");
		return this.runAssignmentMutation(() => this.releaseBridgeAssignments(normalizedId));
	}

	protected async releaseBridgeAssignments(bridgeId: string): Promise<readonly number[]> {
		this.ensureRunning();
		const active = this.sessions.get(bridgeId);
		if (active !== undefined && active.phase !== "closed") {
			throw new ShardingStateError(`Bridge ${bridgeId} is still connected and cannot be released.`);
		}
		if (!this.bridges.has(bridgeId)) throw new ShardingStateError(`Bridge ${bridgeId} is not known.`);
		const persistence = this.requirePersistence();
		const assignments = [...this.assignments.values()]
			.filter((assignment) => assignment.bridgeId === bridgeId)
			.sort((left, right) => left.shardId - right.shardId);
		if (assignments.length === 0) return Object.freeze([]);
		this.advanceTopology();
		const released: number[] = [];
		const failures: unknown[] = [];
		for (const assignment of assignments) {
			try {
				const tombstone: $PersistedAssignment = Object.freeze({
					bridgeId: releasedAssignmentOwner(assignment.shardId),
					epoch: assignment.epoch,
					shardId: assignment.shardId,
					updatedAt: this.nextPersistenceTimestamp(),
				});
				await persistence.saveAssignment(tombstone);
				this.clearRestartState(tombstone.shardId);
				this.assignments.set(tombstone.shardId, tombstone);
				released.push(tombstone.shardId);
				this.hubEvents.emit("shardDeallocated", {
					bridgeId,
					reason: "released",
					shardId: tombstone.shardId,
				});
			} catch (cause) {
				failures.push(cause);
				break;
			}
		}
		try {
			await this.synchronizeSessions([...this.sessions.values()].filter((session) => session.phase === "ready"));
		} catch (cause) {
			failures.push(cause);
		} finally {
			this.requestReconciliation();
		}
		if (failures.length === 1) throw failures[0];
		if (failures.length > 1) {
			throw new AggregateError(failures, `Bridge ${bridgeId} release did not propagate every committed change.`);
		}
		return Object.freeze(released);
	}

	/**
	 * Deletes retained Hub analytics in small database batches.
	 *
	 * @param options - Optional inclusive cutoff and batch size.
	 * @returns Total number of deleted records.
	 */
	public override async clearAnalytics(options: $ClearAnalyticsOptions = {}): Promise<number> {
		this.ensureRunning();
		const input = snapshotConfigurationRecord(options, "clearAnalytics options");
		assertConfigurationKeys(input, new Set(["batchSize", "before"]), "clearAnalytics options");
		const before =
			input.before === undefined
				? this.readWallClock()
				: requireNonNegativeInteger(input.before, "before", Number.MAX_SAFE_INTEGER);
		const batchSize =
			input.batchSize === undefined
				? DEFAULT_ANALYTICS_BATCH_SIZE
				: requirePositiveInteger(input.batchSize, "batchSize", 10_000);
		let total = 0;
		while (true) {
			const removed = await this.requirePersistence().clearAnalyticsBatch(before, batchSize);
			total += removed;
			if (removed < batchSize) return total;
			await Bun.sleep(0);
		}
	}

	/**
	 * Stops the Hub and releases its server, sessions, pending work, and storage.
	 *
	 * Repeated calls share the same shutdown operation.
	 */
	public stop(): Promise<void> {
		if (this.stopPromise !== undefined) return this.stopPromise;
		this.stopPromise = this.performStop();
		return this.stopPromise;
	}

	/**
	 * Calls {@link stop} when the Hub is owned with `await using`.
	 */
	public async [Symbol.asyncDispose](): Promise<void> {
		await this.stop();
	}

	protected async performStop(): Promise<void> {
		if (this.lifecycleState === "stopped") return;
		if (!this.startupLifecycle.signal.aborted) {
			this.startupLifecycle.abort(new ShardingStateError("HubClient stopped."));
		}
		if (this.lifecycleState === "idle") {
			this.lifecycleState = "stopped";
			return;
		}
		if (this.lifecycleState === "starting") {
			const startup = this.startPromise;
			if (startup === undefined) {
				this.lifecycleState = "failed";
				throw new ShardingStateError("HubClient startup operation is unavailable during shutdown.");
			}
			try {
				await startup;
			} catch {
				// Startup performs its own rollback before shutdown continues.
			}
		}
		this.lifecycleState = "stopping";
		const failures: unknown[] = [];
		this.reconcileRequested = false;
		const reconciliation = this.reconcilePromise;
		const assignmentMutations = this.assignmentMutationTail;
		const sessions = [...this.socketSessions];
		this.identifyScheduler?.close(new ShardingStateError("HubClient stopped."));
		this.identifyScheduler = undefined;
		this.rejectPendingStarts(new ShardingStateError("HubClient stopped."));
		for (const timer of this.restartTimers.values()) clearTimeout(timer);
		this.restartTimers.clear();
		for (const [id, route] of this.pendingRoutes) {
			clearTimeout(route.timer);
			this.sendRouteFailure(route, id, new ShardingStateError("HubClient stopped."));
		}
		this.pendingRoutes.clear();
		for (const stop of this.pendingStops.values()) {
			clearTimeout(stop.timer);
			stop.reject(new ShardingStateError("HubClient stopped."));
		}
		this.pendingStops.clear();
		for (const [id, evaluation] of this.evaluations) {
			this.cancelEvaluation(id, evaluation, "HubClient stopped.");
		}
		this.pendingIds.clear();
		for (const session of sessions) {
			try {
				session.close(1012, "Hub shutdown");
			} catch (cause) {
				failures.push(cause);
			}
		}
		this.sessions.clear();
		this.socketSessions.clear();
		try {
			await this.server?.stop(true);
		} catch (cause) {
			failures.push(cause);
		}
		this.server = undefined;
		for (const session of sessions) {
			try {
				await session.waitForInboundIdle();
			} catch (cause) {
				failures.push(cause);
			}
		}
		await assignmentMutations;
		if (reconciliation !== undefined) {
			try {
				await reconciliation;
			} catch (cause) {
				this.report(toError(cause), "assignment reconciliation during shutdown");
			}
		}
		const persistence = this.persistence;
		if (persistence !== undefined) {
			for (const session of sessions) {
				const bridge = this.bridges.get(session.bridgeId);
				if (bridge === undefined) continue;
				const disconnected: $PersistedBridge = Object.freeze({
					...bridge,
					connected: false,
					updatedAt: this.nextPersistenceTimestamp(),
				});
				this.bridges.set(disconnected.id, disconnected);
				try {
					await this.saveBridge(disconnected);
				} catch (cause) {
					failures.push(cause);
				}
			}
		}
		await this.waitForPersistenceWrites();
		try {
			await persistence?.close();
		} catch (cause) {
			failures.push(cause);
		}
		this.persistence = undefined;
		this.rejectPendingStarts(new ShardingStateError("HubClient stopped."));
		for (const timer of this.restartTimers.values()) clearTimeout(timer);
		this.restartTimers.clear();
		this.restartHistory.clear();
		this.identifyKeys.clear();
		this.pendingRoutes.clear();
		this.pendingStops.clear();
		this.evaluations.clear();
		this.pendingIds.clear();
		this.assignmentMutationCount = 0;
		this.assignmentMutationTail = IDLE_ASSIGNMENT_MUTATION;
		this.reconcilePromise = undefined;
		this.reconcileRequested = false;
		this.assignments.clear();
		this.bridges.clear();
		this.bridgePersistenceTails.clear();
		this.shards.clear();
		this.shardPersistenceTails.clear();
		this.lastConnections.clear();
		this.lastPersistenceTimestamp = -1;
		this.lastRestartClock = 0;
		this.shardCount = 0;
		this.topologyVersion = 1;
		this.startPromise = undefined;
		this.hubEvents.removeAllListeners();
		this.lifecycleState = failures.length === 0 ? "stopped" : "failed";
		if (failures.length > 0) {
			throw new AggregateError(failures, "Hub shutdown did not release every resource cleanly.");
		}
	}

	protected async rollbackStartup(startupCause: unknown): Promise<void> {
		const cleanupFailures: unknown[] = [];
		this.identifyScheduler?.close(startupCause);
		this.identifyScheduler = undefined;
		const sessions = [...this.socketSessions];
		for (const session of sessions) {
			try {
				session.close(1011, "Hub startup failed");
			} catch (cause) {
				cleanupFailures.push(cause);
			}
		}
		this.sessions.clear();
		this.socketSessions.clear();
		try {
			await this.server?.stop(true);
		} catch (cause) {
			cleanupFailures.push(cause);
		}
		this.server = undefined;
		for (const session of sessions) {
			try {
				await session.waitForInboundIdle();
			} catch (cause) {
				cleanupFailures.push(cause);
			}
		}
		await this.waitForPersistenceWrites();
		try {
			await this.persistence?.close();
		} catch (cause) {
			cleanupFailures.push(cause);
		}
		this.persistence = undefined;
		this.rejectPendingStarts(toError(startupCause));
		for (const timer of this.restartTimers.values()) clearTimeout(timer);
		this.restartTimers.clear();
		for (const route of this.pendingRoutes.values()) clearTimeout(route.timer);
		this.pendingRoutes.clear();
		for (const stop of this.pendingStops.values()) {
			clearTimeout(stop.timer);
			stop.reject(toError(startupCause));
		}
		this.pendingStops.clear();
		for (const evaluation of this.evaluations.values()) clearTimeout(evaluation.timer);
		this.evaluations.clear();
		this.pendingIds.clear();
		this.assignmentMutationCount = 0;
		this.assignmentMutationTail = IDLE_ASSIGNMENT_MUTATION;
		this.identifyKeys.clear();
		this.restartHistory.clear();
		this.assignments.clear();
		this.bridges.clear();
		this.bridgePersistenceTails.clear();
		this.shards.clear();
		this.shardPersistenceTails.clear();
		this.lastConnections.clear();
		this.lastPersistenceTimestamp = -1;
		this.lastRestartClock = 0;
		this.reconcilePromise = undefined;
		this.reconcileRequested = false;
		this.shardCount = 0;
		this.topologyVersion = 1;
		this.startPromise = undefined;
		if (cleanupFailures.length > 0) {
			throw new AggregateError([startupCause, ...cleanupFailures], "Hub startup and rollback both failed.");
		}
	}
}
