import { ShardingStateError, ShardingTimeoutError } from "../../errors/ShardingError";
import { MAX_TIMER_MS } from "../../internal/limits";
import { createRequestId } from "../../internal/validation";
import type { ShardIdentityData } from "../../protocol/types";
import type { $RestartPolicy } from "../../types/common";
import type { $PersistedAssignment } from "../../types/hub";
import type { HubBridgeSession } from "../session/HubBridgeSession";
import { HubCore } from "./HubCore";
import type { RestartAssignmentIdentity } from "./types";
import { assignmentMatches, sameAssignment, shardSessionKey, toError } from "./utilities";

export abstract class HubRestartController extends HubCore {
	protected async sendStart(session: HubBridgeSession, assignment: $PersistedAssignment): Promise<void> {
		this.requireCurrentReadySession(session);
		const current = this.assignments.get(assignment.shardId);
		if (current !== assignment && !sameAssignment(current, assignment)) {
			throw new ShardingStateError(`Shard ${assignment.shardId} assignment changed before startup.`);
		}
		const key = shardSessionKey(session.bridgeId, assignment.shardId);
		const existingStart = this.pendingStarts.get(key);
		if (existingStart !== undefined) {
			if (existingStart.session !== session || existingStart.assignmentEpoch !== assignment.epoch) {
				throw new ShardingStateError(`Shard ${assignment.shardId} has a stale pending startup.`);
			}
			await existingStart.completion;
			return;
		}
		session.send("hub.shard.start", createRequestId(`start-${assignment.shardId}`), {
			assignmentEpoch: assignment.epoch,
			shardId: assignment.shardId,
			totalShards: this.shardCount,
		});
		let resolve = (): void => undefined;
		let reject = (_reason: unknown): void => undefined;
		const completion = new Promise<void>((settle, fail) => {
			resolve = settle;
			reject = fail;
		});
		void completion.catch(() => undefined);
		const timer = setTimeout(() => {
			const pending = this.pendingStarts.get(key);
			if (pending === undefined || pending.completion !== completion) return;
			this.pendingStarts.delete(key);
			const error = new ShardingTimeoutError(
				`Bridge ${session.bridgeId} did not report shard ${assignment.shardId} startup in time.`,
			);
			pending.reject(error);
			if (this.isCurrentSession(session)) {
				this.closeSessionAfterFailure(session, error, `shard ${assignment.shardId} startup`);
			}
		}, this.options.request.timeoutMs);
		this.pendingStarts.set(key, {
			assignmentEpoch: assignment.epoch,
			completion,
			reject,
			resolve,
			session,
			timer,
		});
		await completion;
	}

	protected completePendingStart(session: HubBridgeSession, identity: ShardIdentityData): void {
		const key = shardSessionKey(session.bridgeId, identity.shardId);
		const pending = this.pendingStarts.get(key);
		if (pending === undefined || pending.session !== session || pending.assignmentEpoch !== identity.assignmentEpoch) {
			return;
		}
		clearTimeout(pending.timer);
		this.pendingStarts.delete(key);
		pending.resolve();
	}

	protected rejectPendingStarts(reason: unknown): void {
		for (const pending of this.pendingStarts.values()) {
			clearTimeout(pending.timer);
			pending.reject(reason);
		}
		this.pendingStarts.clear();
	}

	protected clearRestartState(shardId: number): void {
		this.restartHistory.delete(shardId);
		const timer = this.restartTimers.get(shardId);
		if (timer !== undefined) clearTimeout(timer);
		this.restartTimers.delete(shardId);
	}

	protected scheduleRestart(session: HubBridgeSession, identity: RestartAssignmentIdentity): void {
		const assignment = this.assignments.get(identity.shardId);
		if (!assignmentMatches(assignment, session.bridgeId, identity)) return;
		const policy = session.restartPolicy;
		if (policy === undefined) return;
		const now = this.readRestartClock();
		const existing = this.restartHistory.get(identity.shardId);
		const attempts =
			existing?.bridgeId === session.bridgeId
				? existing.attempts.filter((timestamp) => timestamp > now - policy.windowMs)
				: [];
		if (attempts.length >= policy.maxAttempts) {
			this.restartHistory.set(identity.shardId, { attempts, bridgeId: session.bridgeId, policy });
			this.hubEvents.emit("shardRestartsExhausted", {
				bridgeId: session.bridgeId,
				shardId: identity.shardId,
				windowMs: policy.windowMs,
			});
			this.scheduleRestartWindowWake(session, identity, attempts[0] ?? now, policy, now);
			return;
		}
		attempts.push(now);
		this.restartHistory.set(identity.shardId, { attempts, bridgeId: session.bridgeId, policy });
		const delay = Math.min(policy.maxDelayMs, policy.initialDelayMs * 2 ** Math.min(52, attempts.length - 1));
		const previousTimer = this.restartTimers.get(identity.shardId);
		if (previousTimer !== undefined) clearTimeout(previousTimer);
		const timer = setTimeout(() => {
			this.restartTimers.delete(identity.shardId);
			const currentSession = this.sessions.get(session.bridgeId);
			const currentAssignment = this.assignments.get(identity.shardId);
			if (
				currentSession === undefined ||
				currentSession.phase !== "ready" ||
				currentAssignment === undefined ||
				currentAssignment.bridgeId !== session.bridgeId ||
				currentAssignment.epoch !== identity.assignmentEpoch
			) {
				return;
			}
			void this.sendStart(currentSession, currentAssignment).catch((cause: unknown) =>
				this.report(toError(cause), `Shard ${identity.shardId} restart`),
			);
		}, delay);
		this.restartTimers.set(identity.shardId, timer);
		this.hubEvents.emit("shardRestartScheduled", {
			attempt: attempts.length,
			bridgeId: session.bridgeId,
			delayMs: delay,
			shardId: identity.shardId,
		});
	}

	protected scheduleRestartWindowWake(
		session: HubBridgeSession,
		identity: RestartAssignmentIdentity,
		oldestAttempt: number,
		policy: $RestartPolicy,
		now: number,
	): void {
		const previousTimer = this.restartTimers.get(identity.shardId);
		if (previousTimer !== undefined) clearTimeout(previousTimer);
		const elapsed = Math.max(0, now - oldestAttempt);
		const delay = Math.max(1, Math.min(MAX_TIMER_MS, Math.ceil(policy.windowMs - elapsed)));
		const timer = setTimeout(() => {
			this.restartTimers.delete(identity.shardId);
			try {
				const currentSession = this.sessions.get(session.bridgeId);
				const currentAssignment = this.assignments.get(identity.shardId);
				if (
					currentSession === undefined ||
					currentSession.phase !== "ready" ||
					currentAssignment === undefined ||
					currentAssignment.bridgeId !== session.bridgeId ||
					currentAssignment.epoch !== identity.assignmentEpoch
				) {
					return;
				}
				this.scheduleRestart(currentSession, identity);
			} catch (cause) {
				this.report(toError(cause), `Shard ${identity.shardId} restart window`);
			}
		}, delay);
		this.restartTimers.set(identity.shardId, timer);
	}

	protected restartLimitReached(session: HubBridgeSession, assignment: $PersistedAssignment): boolean {
		const history = this.restartHistory.get(assignment.shardId);
		if (history === undefined) return false;
		if (history.bridgeId !== session.bridgeId) {
			this.clearRestartState(assignment.shardId);
			return false;
		}
		const now = this.readRestartClock();
		const attempts = history.attempts.filter((timestamp) => timestamp > now - history.policy.windowMs);
		if (attempts.length === 0) {
			this.restartHistory.delete(assignment.shardId);
			return false;
		}
		if (attempts.length !== history.attempts.length) {
			this.restartHistory.set(assignment.shardId, { ...history, attempts });
		}
		if (attempts.length >= history.policy.maxAttempts && !this.restartTimers.has(assignment.shardId)) {
			this.scheduleRestartWindowWake(
				session,
				{ assignmentEpoch: assignment.epoch, shardId: assignment.shardId },
				attempts[0] ?? now,
				history.policy,
				now,
			);
		}
		return attempts.length >= history.policy.maxAttempts;
	}
}
