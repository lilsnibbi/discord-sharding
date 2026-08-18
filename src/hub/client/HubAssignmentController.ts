import { ShardingStateError, ShardingTimeoutError } from "../../errors/ShardingError";
import { MAX_BRIDGES } from "../../internal/limits";
import { createRequestId } from "../../internal/validation";
import type { $PersistedAssignment } from "../../types/hub";
import { planNextAssignment } from "../assignment/planner";
import type { $AssignmentStep } from "../assignment/types";
import type { HubBridgeSession } from "../session/HubBridgeSession";
import { HubRestartController } from "./HubRestartController";
import {
	createAssignment,
	incrementEpoch,
	isReleasedAssignment,
	releasedAssignmentOwner,
	sameAssignment,
	shardSessionKey,
	toError,
} from "./utilities";

export abstract class HubAssignmentController extends HubRestartController {
	protected clusterSummary(): readonly { readonly bridgeId: string; readonly shardCount: number }[] {
		const counts = new Map<string, number>();
		for (const bridgeId of this.bridges.keys()) counts.set(bridgeId, 0);
		for (const assignment of this.assignments.values()) {
			if (isReleasedAssignment(assignment)) continue;
			counts.set(assignment.bridgeId, (counts.get(assignment.bridgeId) ?? 0) + 1);
		}
		return Object.freeze(
			[...counts.entries()]
				.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
				.map(([bridgeId, shardCount]) => Object.freeze({ bridgeId, shardCount })),
		);
	}

	protected synchronizeSession(session: HubBridgeSession): Promise<void> {
		if (session.phase === "closed") return Promise.reject(new ShardingStateError("Bridge session is closed."));
		const id = createRequestId(`sync-${session.bridgeId}`);
		const assignments = [...this.assignments.values()]
			.filter((assignment) => assignment.bridgeId === session.bridgeId)
			.sort((left, right) => left.shardId - right.shardId)
			.map((assignment) =>
				Object.freeze({
					epoch: assignment.epoch,
					shardId: assignment.shardId,
				}),
			);
		const acknowledgement = session.beginSynchronization(id, this.topologyVersion);
		try {
			session.send("hub.sync", id, {
				assignments: Object.freeze(assignments),
				bridgeGeneration: session.bridgeGeneration,
				cluster: this.clusterSummary(),
				connectionGeneration: session.connectionGeneration,
				topologyVersion: this.topologyVersion,
				totalShards: this.shardCount,
			});
		} catch (cause) {
			this.closeSessionAfterFailure(session, cause, "topology synchronization");
		}
		return acknowledgement;
	}

	protected async synchronizeSessions(sessions: readonly HubBridgeSession[]): Promise<void> {
		const unique = [...new Set(sessions)].filter((session) => session.phase !== "closed");
		await Promise.all(unique.map((session) => this.synchronizeSession(session)));
	}

	protected handleSessionReady(session: HubBridgeSession): void {
		if (this.sessions.get(session.bridgeId) !== session || session.phase !== "ready") return;
		void this.ensureAssignedProcesses(session).catch((cause: unknown) =>
			this.report(toError(cause), `Bridge ${session.bridgeId} assigned process startup`),
		);
		this.requestReconciliation();
	}

	protected async runReconciliation(): Promise<void> {
		let mutations = 0;
		while (this.reconcileRequested && this.lifecycleState === "running") {
			this.reconcileRequested = false;
			while (this.lifecycleState === "running") {
				const plan = planNextAssignment({
					assignments: [...this.assignments.values()].filter((assignment) => !isReleasedAssignment(assignment)),
					bridges: [...this.bridges.values()].map((bridge) =>
						Object.freeze({
							connected: this.sessions.get(bridge.id)?.phase === "ready",
							id: bridge.id,
							maxShards: bridge.maxShards,
						}),
					),
					totalShards: this.shardCount,
				});
				if (plan.nextStep === null) break;
				mutations += 1;
				if (mutations > this.shardCount * 2 + MAX_BRIDGES) {
					throw new ShardingStateError("Assignment reconciliation exceeded its bounded mutation count.");
				}
				await this.applyAssignmentStep(plan.nextStep);
			}
			if (this.lifecycleState !== "running") return;
			for (const session of this.sessions.values()) {
				if (session.phase === "ready") await this.ensureAssignedProcesses(session);
			}
		}
	}

	protected async applyAssignmentStep(step: $AssignmentStep): Promise<void> {
		switch (step.kind) {
			case "assign": {
				const target = this.requireReadySession(step.toBridgeId);
				const previous = this.assignments.get(step.shardId);
				const epoch =
					previous !== undefined && isReleasedAssignment(previous)
						? incrementEpoch(previous.epoch, step.shardId)
						: step.nextEpoch;
				const assignment = createAssignment(step.shardId, target.bridgeId, epoch, this.nextPersistenceTimestamp());
				this.requireCurrentReadySession(target);
				await this.requirePersistence().saveAssignment(assignment);
				this.clearRestartState(assignment.shardId);
				this.assignments.set(step.shardId, assignment);
				this.advanceTopology();
				await this.synchronizeSessions([target]);
				await this.sendStart(target, assignment);
				return;
			}
			case "transfer": {
				const source = this.requireReadySession(step.fromBridgeId);
				const target = this.requireReadySession(step.toBridgeId);
				const current = this.assignments.get(step.shardId);
				if (current === undefined || current.bridgeId !== source.bridgeId || current.epoch !== step.currentEpoch) {
					throw new ShardingStateError(`Shard ${step.shardId} assignment changed during transfer.`);
				}
				await this.stopShardIfRunning(source, current, "Shard ownership is moving to another Bridge.");
				try {
					this.requireCurrentReadySession(source);
					this.requireCurrentReadySession(target);
					const latest = this.assignments.get(step.shardId);
					if (latest === undefined || latest.bridgeId !== source.bridgeId || latest.epoch !== step.currentEpoch) {
						throw new ShardingStateError(`Shard ${step.shardId} assignment changed before transfer commit.`);
					}
				} catch (cause) {
					await this.restoreAbortedTransfer(source, current);
					throw cause;
				}
				const assignment = createAssignment(
					step.shardId,
					target.bridgeId,
					incrementEpoch(current.epoch, step.shardId),
					this.nextPersistenceTimestamp(),
				);
				await this.requirePersistence().saveAssignment(assignment);
				this.clearRestartState(assignment.shardId);
				this.assignments.set(step.shardId, assignment);
				this.advanceTopology();
				await this.synchronizeSessions([source, target]);
				await this.sendStart(target, assignment);
				return;
			}
			case "unassign": {
				const source = this.requireReadySession(step.fromBridgeId);
				const current = this.assignments.get(step.shardId);
				if (current === undefined || current.bridgeId !== source.bridgeId || current.epoch !== step.currentEpoch) {
					throw new ShardingStateError(`Shard ${step.shardId} assignment changed during release.`);
				}
				await this.stopShardIfRunning(source, current, "Bridge capacity no longer includes this shard.");
				this.requireCurrentReadySession(source);
				const latest = this.assignments.get(step.shardId);
				if (latest === undefined || latest.bridgeId !== source.bridgeId || latest.epoch !== step.currentEpoch) {
					throw new ShardingStateError(`Shard ${step.shardId} assignment changed before release commit.`);
				}
				const tombstone = createAssignment(
					current.shardId,
					releasedAssignmentOwner(current.shardId),
					current.epoch,
					this.nextPersistenceTimestamp(),
				);
				await this.requirePersistence().saveAssignment(tombstone);
				this.clearRestartState(tombstone.shardId);
				this.assignments.set(tombstone.shardId, tombstone);
				this.advanceTopology();
				await this.synchronizeSessions([source]);
				return;
			}
		}
	}

	protected async stopShardIfRunning(
		session: HubBridgeSession,
		assignment: $PersistedAssignment,
		reason: string,
	): Promise<void> {
		const startKey = shardSessionKey(session.bridgeId, assignment.shardId);
		const pendingStart = this.pendingStarts.get(startKey);
		if (pendingStart !== undefined) {
			if (pendingStart.session !== session || pendingStart.assignmentEpoch !== assignment.epoch) {
				throw new ShardingStateError(`Shard ${assignment.shardId} has a stale pending startup.`);
			}
			await pendingStart.completion;
			this.requireCurrentReadySession(session);
			if (!sameAssignment(this.assignments.get(assignment.shardId), assignment)) {
				throw new ShardingStateError(`Shard ${assignment.shardId} assignment changed during startup.`);
			}
		}
		const running = session.runningShards.get(assignment.shardId);
		if (running === undefined || running.assignmentEpoch !== assignment.epoch) return;
		const id = createRequestId(`stop-${assignment.shardId}`);
		this.reserveOperationId(id);
		const operation = new Promise<void>((resolve, reject) => {
			const timer = setTimeout(() => {
				const pending = this.pendingStops.get(id);
				if (pending === undefined) return;
				this.pendingStops.delete(id);
				this.pendingIds.delete(id);
				reject(new ShardingTimeoutError(`Shard ${assignment.shardId} stop timed out.`));
			}, this.options.request.timeoutMs);
			this.pendingStops.set(id, {
				identity: running,
				reject,
				resolve,
				session,
				timer,
			});
		});
		try {
			session.send("hub.shard.stop", id, {
				...running,
				reason,
			});
		} catch (cause) {
			const pending = this.pendingStops.get(id);
			if (pending !== undefined) {
				clearTimeout(pending.timer);
				this.pendingStops.delete(id);
				this.pendingIds.delete(id);
				pending.reject(cause);
			}
		}
		await operation;
	}

	protected async restoreAbortedTransfer(session: HubBridgeSession, assignment: $PersistedAssignment): Promise<void> {
		if (
			session.phase !== "ready" ||
			this.sessions.get(session.bridgeId) !== session ||
			!sameAssignment(this.assignments.get(assignment.shardId), assignment)
		) {
			return;
		}
		try {
			await this.sendStart(session, assignment);
		} catch (cause) {
			this.report(toError(cause), `Shard ${assignment.shardId} aborted transfer recovery`);
		}
	}

	protected async ensureAssignedProcesses(session: HubBridgeSession): Promise<void> {
		if (session.phase !== "ready" || this.sessions.get(session.bridgeId) !== session) return;
		for (const assignment of [...this.assignments.values()].sort((left, right) => left.shardId - right.shardId)) {
			if (assignment.bridgeId !== session.bridgeId) continue;
			const running = session.runningShards.get(assignment.shardId);
			if (running !== undefined && running.assignmentEpoch === assignment.epoch) continue;
			if (this.restartTimers.has(assignment.shardId) || this.restartLimitReached(session, assignment)) continue;
			await this.sendStart(session, assignment);
		}
	}
}
