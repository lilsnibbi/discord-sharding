import {
	ShardingCapacityError,
	ShardingProtocolError,
	ShardingStateError,
	ShardingTimeoutError,
	ShardingTransportError,
} from "../../errors/ShardingError";
import { type SerializedError, serializeError } from "../../internal/errors";
import { requireExactKeys, requireOptionalKeys } from "../../protocol/codec";
import { readEvaluator, readNullableShardId, readPayload, readRouteKind, readShardId } from "../../protocol/readers";
import type { ParsedWireMessage, ShardIdentityData } from "../../protocol/types";
import type { $PersistedBridge } from "../../types/hub";
import { readIdentity } from "../protocol";
import type { HubBridgeSession } from "../session/HubBridgeSession";
import { HubAssignmentController } from "./HubAssignmentController";
import type { EvaluationTarget, PendingEvaluation, PendingRoute } from "./types";
import { assignmentMatches, identitiesEqual, isReleasedAssignment, responseFields, toError } from "./utilities";

export abstract class HubRoutingController extends HubAssignmentController {
	protected handleRouteRequest(session: HubBridgeSession, message: ParsedWireMessage): void {
		requireExactKeys(
			message.data,
			new Set(["assignmentEpoch", "kind", "payload", "processGeneration", "shardId", "targetShardId"]),
			"bridge.route.request data",
		);
		const sourceIdentity = readIdentity(message.data);
		this.requireSessionIdentity(session, sourceIdentity, true);
		const targetShardId = readShardId(message.data, "targetShardId");
		const kind = readRouteKind(message.data);
		const payload = readPayload(message.data, "payload");
		let reserved = false;
		try {
			this.reserveOperationId(message.id);
			reserved = true;
			const target = this.requireReadyTarget(targetShardId);
			const timer = setTimeout(() => {
				const pending = this.pendingRoutes.get(message.id);
				if (pending === undefined) return;
				this.pendingRoutes.delete(message.id);
				this.pendingIds.delete(message.id);
				this.sendRouteFailure(pending, message.id, new ShardingTimeoutError(`Route ${message.id} timed out.`));
			}, this.options.request.timeoutMs);
			const pending: PendingRoute = {
				sourceIdentity,
				sourceSession: session,
				targetIdentity: target.identity,
				targetSession: target.session,
				timer,
			};
			this.pendingRoutes.set(message.id, pending);
			try {
				target.session.send("hub.route.request", message.id, {
					kind,
					payload,
					sourceShardId: sourceIdentity.shardId,
					target: target.identity,
				});
			} catch (cause) {
				clearTimeout(timer);
				this.pendingRoutes.delete(message.id);
				this.pendingIds.delete(message.id);
				this.sendRouteFailure(pending, message.id, cause);
			}
		} catch (cause) {
			if (reserved) this.pendingIds.delete(message.id);
			session.send("hub.route.response", message.id, {
				...sourceIdentity,
				error: serializeError(cause),
				ok: false,
				sourceShardId: sourceIdentity.shardId,
			});
		}
	}

	protected handleRouteResponse(session: HubBridgeSession, message: ParsedWireMessage): void {
		requireOptionalKeys(
			message.data,
			new Set(["assignmentEpoch", "ok", "processGeneration", "shardId", "sourceShardId"]),
			new Set(["error", "value"]),
			"bridge.route.response data",
		);
		const pending = this.pendingRoutes.get(message.id);
		if (pending === undefined) return;
		const identity = readIdentity(message.data);
		if (
			pending.targetSession !== session ||
			!identitiesEqual(pending.targetIdentity, identity) ||
			readNullableShardId(message.data, "sourceShardId") !== pending.sourceIdentity.shardId
		) {
			throw new ShardingProtocolError("Route response identity does not match its pending request.");
		}
		const response = responseFields(message.data, "bridge.route.response data", true);
		clearTimeout(pending.timer);
		this.pendingRoutes.delete(message.id);
		this.pendingIds.delete(message.id);
		if (pending.sourceSession.phase === "closed") return;
		pending.sourceSession.send("hub.route.response", message.id, {
			...identity,
			...response,
			sourceShardId: pending.sourceIdentity.shardId,
		});
	}

	protected handleEvaluationRequest(session: HubBridgeSession, message: ParsedWireMessage): void {
		requireExactKeys(
			message.data,
			new Set(["assignmentEpoch", "context", "evaluator", "processGeneration", "shardId"]),
			"bridge.eval.request data",
		);
		const sourceIdentity = readIdentity(message.data);
		this.requireSessionIdentity(session, sourceIdentity, true);
		let ownedEvaluation: PendingEvaluation | undefined;
		let reserved = false;
		try {
			if (this.evaluations.size >= this.options.maxEvaluations) {
				throw new ShardingCapacityError(`Hub evaluation capacity of ${this.options.maxEvaluations} was reached.`);
			}
			this.reserveOperationId(message.id);
			reserved = true;
			const targets = new Map<number, EvaluationTarget>();
			for (const target of this.readyTargets()) {
				targets.set(target.identity.shardId, {
					identity: target.identity,
					prepared: false,
					session: target.session,
				});
			}
			if (targets.size === 0) throw new ShardingStateError("No Discord-ready shards are available.");
			const evaluation: PendingEvaluation = {
				context: readPayload(message.data, "context"),
				evaluator: readEvaluator(message.data),
				phase: "preparing",
				sourceIdentity,
				sourceSession: session,
				targets,
				timer: setTimeout(() => {
					const pending = this.evaluations.get(message.id);
					if (pending !== undefined) {
						this.failEvaluation(
							message.id,
							pending,
							serializeError(new ShardingTimeoutError(`Evaluation ${message.id} timed out.`)),
						);
					}
				}, this.options.request.timeoutMs),
			};
			ownedEvaluation = evaluation;
			this.evaluations.set(message.id, evaluation);
			for (const target of targets.values()) {
				target.session.send("hub.eval.prepare", message.id, {
					context: evaluation.context,
					evaluator: evaluation.evaluator,
					sourceShardId: sourceIdentity.shardId,
					target: target.identity,
				});
			}
		} catch (cause) {
			if (ownedEvaluation !== undefined && this.evaluations.get(message.id) === ownedEvaluation) {
				this.failEvaluation(message.id, ownedEvaluation, serializeError(cause));
				return;
			}
			if (reserved) this.pendingIds.delete(message.id);
			session.send("hub.eval.response", message.id, {
				error: serializeError(cause),
				ok: false,
				sourceShardId: sourceIdentity.shardId,
			});
		}
	}

	protected handleEvaluationPrepared(session: HubBridgeSession, message: ParsedWireMessage): void {
		requireOptionalKeys(
			message.data,
			new Set(["assignmentEpoch", "ok", "processGeneration", "shardId"]),
			new Set(["error"]),
			"bridge.eval.prepared data",
		);
		const evaluation = this.evaluations.get(message.id);
		if (evaluation === undefined) return;
		if (evaluation.phase !== "preparing")
			throw new ShardingProtocolError("Evaluation prepare response arrived too late.");
		const identity = readIdentity(message.data);
		const target = evaluation.targets.get(identity.shardId);
		if (target === undefined || target.session !== session || !identitiesEqual(target.identity, identity)) {
			throw new ShardingProtocolError("Evaluation prepare identity is stale or unexpected.");
		}
		if (target.prepared) throw new ShardingProtocolError("Evaluation target prepared more than once.");
		const response = responseFields(message.data, "bridge.eval.prepared data", false);
		if (!response.ok) {
			this.failEvaluation(message.id, evaluation, response.error);
			return;
		}
		target.prepared = true;
		for (const candidate of evaluation.targets.values()) {
			if (!candidate.prepared) return;
		}
		evaluation.phase = "running";
		const executeAt = this.readWallClock() + this.options.evaluationCommitLeadMs;
		if (!Number.isSafeInteger(executeAt)) {
			this.failEvaluation(
				message.id,
				evaluation,
				serializeError(new ShardingStateError("Evaluation commit time exceeded the supported range.")),
			);
			return;
		}
		try {
			for (const candidate of evaluation.targets.values()) {
				candidate.session.send("hub.eval.commit", message.id, {
					executeAt,
					target: candidate.identity,
				});
			}
		} catch (cause) {
			this.failEvaluation(message.id, evaluation, serializeError(cause));
		}
	}

	protected handleEvaluationResult(session: HubBridgeSession, message: ParsedWireMessage): void {
		requireOptionalKeys(
			message.data,
			new Set(["assignmentEpoch", "ok", "processGeneration", "shardId"]),
			new Set(["error", "value"]),
			"bridge.eval.result data",
		);
		const evaluation = this.evaluations.get(message.id);
		if (evaluation === undefined) return;
		if (evaluation.phase !== "running") throw new ShardingProtocolError("Evaluation result arrived before commit.");
		const identity = readIdentity(message.data);
		const target = evaluation.targets.get(identity.shardId);
		if (target === undefined || target.session !== session || !identitiesEqual(target.identity, identity)) {
			throw new ShardingProtocolError("Evaluation result identity is stale or unexpected.");
		}
		if (Object.hasOwn(target, "result")) throw new ShardingProtocolError("Evaluation target returned more than once.");
		const response = responseFields(message.data, "bridge.eval.result data", true);
		if (!response.ok) {
			this.failEvaluation(message.id, evaluation, response.error);
			return;
		}
		if (!Object.hasOwn(response, "value")) {
			this.failEvaluation(
				message.id,
				evaluation,
				serializeError(new ShardingProtocolError("Evaluation result did not include a value.")),
			);
			return;
		}
		target.result = response.value;
		for (const candidate of evaluation.targets.values()) {
			if (!Object.hasOwn(candidate, "result")) return;
		}
		const results = [...evaluation.targets.values()]
			.sort((left, right) => left.identity.shardId - right.identity.shardId)
			.map((candidate) =>
				Object.freeze({
					shardId: candidate.identity.shardId,
					value: candidate.result,
				}),
			);
		this.completeEvaluation(message.id, evaluation);
		if (evaluation.sourceSession.phase === "closed") return;
		evaluation.sourceSession.send("hub.eval.response", message.id, {
			ok: true,
			results: Object.freeze(results),
			sourceShardId: evaluation.sourceIdentity.shardId,
		});
	}

	protected handleSessionClosed(session: HubBridgeSession): void {
		if (this.sessions.get(session.bridgeId) !== session) return;
		this.sessions.delete(session.bridgeId);
		for (const [key, pending] of this.pendingStarts) {
			if (pending.session !== session) continue;
			clearTimeout(pending.timer);
			this.pendingStarts.delete(key);
			pending.reject(new ShardingTransportError(`Bridge ${session.bridgeId} disconnected during shard startup.`));
		}
		for (const [id, pending] of this.pendingStops) {
			if (pending.session !== session) continue;
			clearTimeout(pending.timer);
			this.pendingStops.delete(id);
			this.pendingIds.delete(id);
			pending.reject(new ShardingTransportError(`Bridge ${session.bridgeId} disconnected during shard stop.`));
		}
		for (const [id, pending] of this.pendingRoutes) {
			if (pending.sourceSession === session) {
				clearTimeout(pending.timer);
				this.pendingRoutes.delete(id);
				this.pendingIds.delete(id);
				continue;
			}
			if (pending.targetSession !== session) continue;
			clearTimeout(pending.timer);
			this.pendingRoutes.delete(id);
			this.pendingIds.delete(id);
			this.sendRouteFailure(
				pending,
				id,
				new ShardingTransportError(`Destination Bridge ${session.bridgeId} disconnected.`),
			);
		}
		for (const [id, evaluation] of this.evaluations) {
			if (evaluation.sourceSession === session) {
				this.cancelEvaluation(id, evaluation, "Source Bridge disconnected.");
				continue;
			}
			for (const target of evaluation.targets.values()) {
				if (target.session !== session) continue;
				this.failEvaluation(
					id,
					evaluation,
					serializeError(new ShardingTransportError(`Evaluation Bridge ${session.bridgeId} disconnected.`)),
				);
				break;
			}
		}
		const bridge = this.bridges.get(session.bridgeId);
		if (bridge !== undefined) {
			const disconnected: $PersistedBridge = Object.freeze({
				...bridge,
				connected: false,
				updatedAt: this.nextPersistenceTimestamp(),
			});
			this.bridges.set(disconnected.id, disconnected);
			if (this.lifecycleState === "running") {
				void this.saveBridge(disconnected).catch((cause: unknown) =>
					this.report(toError(cause), `Bridge ${session.bridgeId} disconnect persistence`),
				);
			}
		}
		this.requestReconciliation();
	}

	protected sendRouteFailure(pending: PendingRoute, id: string, cause: unknown): void {
		if (pending.sourceSession.phase === "closed") return;
		try {
			pending.sourceSession.send("hub.route.response", id, {
				...pending.targetIdentity,
				error: serializeError(cause),
				ok: false,
				sourceShardId: pending.sourceIdentity.shardId,
			});
		} catch (sendCause) {
			this.report(toError(sendCause), `Route ${id} failure response`);
		}
	}

	protected failEvaluation(id: string, evaluation: PendingEvaluation, error: SerializedError): void {
		for (const target of evaluation.targets.values()) {
			if (target.session.phase === "closed") continue;
			try {
				target.session.send("hub.eval.cancel", id, {
					reason: "Evaluation cancelled before all results were available.",
					target: target.identity,
				});
			} catch (cause) {
				this.report(toError(cause), `Evaluation ${id} cancellation`);
			}
		}
		this.completeEvaluation(id, evaluation);
		if (evaluation.sourceSession.phase === "closed") return;
		try {
			evaluation.sourceSession.send("hub.eval.response", id, {
				error,
				ok: false,
				sourceShardId: evaluation.sourceIdentity.shardId,
			});
		} catch (cause) {
			this.report(toError(cause), `Evaluation ${id} failure response`);
		}
	}

	protected cancelEvaluation(id: string, evaluation: PendingEvaluation, reason: string): void {
		for (const target of evaluation.targets.values()) {
			if (target.session.phase === "closed") continue;
			try {
				target.session.send("hub.eval.cancel", id, {
					reason,
					target: target.identity,
				});
			} catch (cause) {
				this.report(toError(cause), `Evaluation ${id} cancellation`);
			}
		}
		this.completeEvaluation(id, evaluation);
	}

	protected completeEvaluation(id: string, evaluation: PendingEvaluation): void {
		if (this.evaluations.get(id) !== evaluation) return;
		clearTimeout(evaluation.timer);
		this.evaluations.delete(id);
		this.pendingIds.delete(id);
	}

	protected readyTargets(): readonly { readonly identity: ShardIdentityData; readonly session: HubBridgeSession }[] {
		const targets: Array<{ readonly identity: ShardIdentityData; readonly session: HubBridgeSession }> = [];
		for (const session of this.sessions.values()) {
			if (session.phase !== "ready") continue;
			for (const running of session.runningShards.values()) {
				if (!running.ready || !assignmentMatches(this.assignments.get(running.shardId), session.bridgeId, running)) {
					continue;
				}
				targets.push({ identity: running, session });
			}
		}
		return Object.freeze(targets.sort((left, right) => left.identity.shardId - right.identity.shardId));
	}

	protected requireReadyTarget(shardId: number): {
		readonly identity: ShardIdentityData;
		readonly session: HubBridgeSession;
	} {
		const assignment = this.assignments.get(shardId);
		if (assignment === undefined || isReleasedAssignment(assignment)) {
			throw new ShardingStateError(`Shard ${shardId} is not assigned.`);
		}
		const session = this.requireReadySession(assignment.bridgeId);
		const running = session.runningShards.get(shardId);
		if (running === undefined || !running.ready || running.assignmentEpoch !== assignment.epoch) {
			throw new ShardingStateError(`Shard ${shardId} is not Discord-ready.`);
		}
		return Object.freeze({ identity: running, session });
	}
}
