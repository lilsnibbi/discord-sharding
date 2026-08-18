import {
	ShardingCapacityError,
	ShardingProtocolError,
	ShardingStateError,
	ShardingTimeoutError,
} from "../../errors/ShardingError";
import { serializeError } from "../../internal/errors";
import { normalizePayload } from "../../internal/payload";
import { requireProtocolIdentifier } from "../../internal/validation";
import { requireExactKeys, requireOptionalKeys } from "../../protocol/codec";
import {
	readBoolean,
	readError,
	readEvaluator,
	readInteger,
	readNullableShardId,
	readPayload,
	readRecord,
	readRouteKind,
	readShardId,
	readString,
} from "../../protocol/readers";
import type { ParsedWireMessage, ShardIdentityData } from "../../protocol/types";
import type { $JsonValue } from "../../types/common";
import type { $AnalyticsRecord } from "../../types/hub";
import type { ManagedShardProcess } from "../shards/ManagedShardProcess";
import { BridgeCore } from "./BridgeCore";
import {
	identityOf,
	matchesIdentity,
	parseEvalResponse,
	parseHubRouteResponse,
	parseResponse,
	readIdentity,
	toError,
} from "./protocol";
import type { $OutboundOperation, $OutboundOperationKind } from "./types";

export abstract class BridgeRequests extends BridgeCore {
	protected async forwardIdentifyResponse(message: ParsedWireMessage): Promise<void> {
		requireOptionalKeys(
			message.data,
			new Set(["granted", "shardId"]),
			new Set(["error"]),
			"hub.identify.response data",
		);
		const shardId = readShardId(message.data);
		const pending = this.findOutbound(message.id, "identify");
		if (pending === undefined) return;
		if (shardId !== pending.managed.shardId) {
			throw new ShardingProtocolError("Identify response targets the wrong shard.");
		}
		const granted = readBoolean(message.data, "granted");
		if (granted && Object.hasOwn(message.data, "error")) {
			throw new ShardingProtocolError("Granted identify response cannot include error.");
		}
		this.completeOutbound(message.id, pending);
		await this.deliverToShard(pending.managed, "identify response", () =>
			this.sendShard(
				pending.managed,
				"shard.control.identify.response",
				message.id,
				granted ? { granted: true } : { error: readError(message.data.error), granted: false },
			),
		);
	}

	protected async forwardRouteRequest(message: ParsedWireMessage): Promise<void> {
		requireExactKeys(message.data, new Set(["kind", "payload", "sourceShardId", "target"]), "hub.route.request data");
		const target = readIdentity(readRecord(message.data, "target"));
		const sourceShardId = readNullableShardId(message.data, "sourceShardId");
		const kind = readRouteKind(message.data);
		const payload = readPayload(message.data, "payload");
		if (this.inboundRoutes.has(message.id)) {
			throw new ShardingProtocolError(`Inbound route ${message.id} is already pending.`);
		}
		if (this.inboundRoutes.size >= this.options.request.maxPending) {
			throw new ShardingCapacityError("Inbound route capacity reached.");
		}
		let managed: ManagedShardProcess;
		try {
			managed = this.requireManagedIdentity(target);
		} catch (cause) {
			await this.failRoutedRequest(message.id, target, sourceShardId, cause);
			return;
		}
		const timer = setTimeout(() => this.inboundRoutes.delete(message.id), this.options.request.timeoutMs);
		this.inboundRoutes.set(message.id, { managed, sourceShardId, timer });
		try {
			await this.sendShard(managed, "shard.control.route.request", message.id, {
				kind,
				payload,
				sourceShardId,
			});
		} catch (cause) {
			clearTimeout(timer);
			this.inboundRoutes.delete(message.id);
			await this.failRoutedRequest(message.id, target, sourceShardId, cause);
		}
	}

	protected async forwardRouteResponse(message: ParsedWireMessage): Promise<void> {
		const response = parseHubRouteResponse(message.data);
		const pending = this.findOutbound(message.id, "route");
		if (pending === undefined) return;
		if (response.sourceShardId !== pending.managed.shardId) {
			throw new ShardingProtocolError("Route response targets the wrong source shard.");
		}
		this.completeOutbound(message.id, pending);
		await this.deliverToShard(pending.managed, "route response", () =>
			this.sendShard(pending.managed, "shard.control.route.response", message.id, response.result),
		);
	}

	protected async forwardEvalPrepare(message: ParsedWireMessage): Promise<void> {
		requireExactKeys(
			message.data,
			new Set(["context", "evaluator", "sourceShardId", "target"]),
			"hub.eval.prepare data",
		);
		const target = readIdentity(readRecord(message.data, "target"));
		const context = readPayload(message.data, "context");
		const evaluator = readEvaluator(message.data);
		const sourceShardId = readShardId(message.data, "sourceShardId");
		try {
			const managed = this.requireManagedIdentity(target);
			await this.sendShard(managed, "shard.control.eval.prepare", message.id, { context, evaluator, sourceShardId });
		} catch (cause) {
			await this.sendHub("bridge.eval.prepared", message.id, {
				...target,
				error: serializeError(cause),
				ok: false,
			});
		}
	}

	protected async forwardEvalCommit(message: ParsedWireMessage): Promise<void> {
		requireExactKeys(message.data, new Set(["executeAt", "target"]), "hub.eval.commit data");
		const target = readIdentity(readRecord(message.data, "target"));
		const executeAt = readInteger(message.data, "executeAt", 0, Number.MAX_SAFE_INTEGER);
		try {
			const managed = this.requireManagedIdentity(target);
			await this.sendShard(managed, "shard.control.eval.commit", message.id, { executeAt });
		} catch (cause) {
			await this.sendHub("bridge.eval.result", message.id, {
				...target,
				error: serializeError(cause),
				ok: false,
			});
		}
	}

	protected async forwardEvalCancel(message: ParsedWireMessage): Promise<void> {
		requireExactKeys(message.data, new Set(["reason", "target"]), "hub.eval.cancel data");
		const target = readIdentity(readRecord(message.data, "target"));
		const reason = readString(message.data, "reason", 512);
		try {
			const managed = this.requireManagedIdentity(target);
			await this.sendShard(managed, "shard.control.eval.cancel", message.id, { reason });
		} catch (cause) {
			this.report(toError(cause), `shard ${target.shardId} evaluation cancellation`);
		}
	}

	protected async forwardEvalResponse(message: ParsedWireMessage): Promise<void> {
		const response = parseEvalResponse(message.data);
		const sourceShardId = readShardId(message.data, "sourceShardId");
		const pending = this.findOutbound(message.id, "eval");
		if (pending === undefined) return;
		if (sourceShardId !== pending.managed.shardId) {
			throw new ShardingProtocolError("Evaluation response targets the wrong source shard.");
		}
		this.completeOutbound(message.id, pending);
		await this.deliverToShard(pending.managed, "evaluation response", () =>
			this.sendShard(pending.managed, "shard.control.eval.response", message.id, response),
		);
	}

	protected async forwardShardIdentify(managed: ManagedShardProcess, id: string): Promise<void> {
		if (this.connectionReady) {
			await this.sendIdentifyRequest(managed, id);
			return;
		}
		void this.#holdIdentifyThroughMaintenance(managed, id).catch((cause: unknown) =>
			this.report(toError(cause), `shard ${managed.shardId} identify hold`),
		);
	}

	protected async sendIdentifyRequest(managed: ManagedShardProcess, id: string): Promise<void> {
		let reserved = false;
		try {
			this.reserveOutbound(id, managed, "identify");
			reserved = true;
			await this.sendHub("bridge.identify.request", id, identityOf(managed));
		} catch (cause) {
			if (reserved) this.discardOutbound(id, managed, "identify");
			await this.sendOutboundFailure(managed, "identify", id, cause);
		}
	}

	async #holdIdentifyThroughMaintenance(managed: ManagedShardProcess, id: string): Promise<void> {
		try {
			await this.awaitConnectionReady(this.options.request.timeoutMs);
		} catch (cause) {
			await this.sendOutboundFailure(managed, "identify", id, cause);
			return;
		}
		if (this.processes.get(managed.shardId) !== managed) return;
		await this.sendIdentifyRequest(managed, id);
	}

	protected async forwardShardRoute(managed: ManagedShardProcess, message: ParsedWireMessage): Promise<void> {
		requireExactKeys(message.data, new Set(["kind", "payload", "targetShardId"]), "shard.route.request data");
		let reserved = false;
		try {
			this.reserveOutbound(message.id, managed, "route");
			reserved = true;
			await this.sendHub("bridge.route.request", message.id, {
				...identityOf(managed),
				kind: readRouteKind(message.data),
				payload: readPayload(message.data, "payload"),
				targetShardId: readShardId(message.data, "targetShardId"),
			});
		} catch (cause) {
			if (reserved) this.discardOutbound(message.id, managed, "route");
			await this.sendOutboundFailure(managed, "route", message.id, cause);
		}
	}

	protected async forwardShardRouteResponse(managed: ManagedShardProcess, message: ParsedWireMessage): Promise<void> {
		const route = this.inboundRoutes.get(message.id);
		if (route === undefined) return;
		if (route.managed !== managed) {
			throw new ShardingProtocolError("Route response came from the wrong shard process generation.");
		}
		const response = parseResponse(message.data, "shard.route.response data");
		clearTimeout(route.timer);
		this.inboundRoutes.delete(message.id);
		await this.sendHub("bridge.route.response", message.id, {
			...identityOf(managed),
			...response,
			sourceShardId: route.sourceShardId,
		});
	}

	protected async forwardShardEval(managed: ManagedShardProcess, message: ParsedWireMessage): Promise<void> {
		requireExactKeys(message.data, new Set(["context", "evaluator"]), "shard.eval.request data");
		let reserved = false;
		try {
			this.reserveOutbound(message.id, managed, "eval");
			reserved = true;
			await this.sendHub("bridge.eval.request", message.id, {
				...identityOf(managed),
				context: readPayload(message.data, "context"),
				evaluator: readEvaluator(message.data),
			});
		} catch (cause) {
			if (reserved) this.discardOutbound(message.id, managed, "eval");
			await this.sendOutboundFailure(managed, "eval", message.id, cause);
		}
	}

	protected async forwardShardEvalPrepared(managed: ManagedShardProcess, message: ParsedWireMessage): Promise<void> {
		const response = parseResponse(message.data, "shard.eval.prepared data");
		await this.sendHub("bridge.eval.prepared", message.id, {
			...identityOf(managed),
			...response,
		});
	}

	protected async forwardShardEvalResult(managed: ManagedShardProcess, message: ParsedWireMessage): Promise<void> {
		const response = parseResponse(message.data, "shard.eval.result data");
		await this.sendHub("bridge.eval.result", message.id, {
			...identityOf(managed),
			...response,
		});
	}

	protected async handleShardAnalytics(managed: ManagedShardProcess, message: ParsedWireMessage): Promise<void> {
		requireExactKeys(message.data, new Set(["collectedAt", "payload"]), "shard.analytics data");
		const collectedAt = readInteger(message.data, "collectedAt", 0, Number.MAX_SAFE_INTEGER);
		const payload = normalizePayload(readPayload(message.data, "payload"), this.payloadPolicy, "analytics payload");
		const record: $AnalyticsRecord = Object.freeze({
			bridgeId: this.id,
			collectedAt,
			data: payload as $JsonValue,
			id: message.id,
			shardId: managed.shardId,
		});
		await this.requireAnalytics().append(record);
		if (this.connectionReady) {
			await this.sendHub("bridge.analytics", message.id, {
				...identityOf(managed),
				collectedAt,
				payload,
			});
		}
	}

	/**
	 * Runs one Hub-initiated delivery to a shard without failing the Hub socket.
	 *
	 * A shard process that exits between a Hub decision and its local delivery is
	 * an expected local condition, not a Hub protocol violation, so it must not
	 * take the whole Bridge connection and its remaining shards down with it.
	 *
	 * @param managed - Destination shard process.
	 * @param context - Human-readable delivery description used when reporting.
	 * @param delivery - Operation that sends the message over Bun IPC.
	 */
	protected async deliverToShard(
		managed: ManagedShardProcess,
		context: string,
		delivery: () => Promise<void>,
	): Promise<void> {
		try {
			await delivery();
		} catch (cause) {
			this.report(toError(cause), `shard ${managed.shardId} ${context}`);
		}
	}

	protected async failRoutedRequest(
		id: string,
		target: ShardIdentityData,
		sourceShardId: number | null,
		cause: unknown,
	): Promise<void> {
		await this.sendHub("bridge.route.response", id, {
			...target,
			error: serializeError(cause),
			ok: false,
			sourceShardId,
		});
	}

	protected requireManagedIdentity(identity: ShardIdentityData): ManagedShardProcess {
		const managed = this.processes.get(identity.shardId);
		if (managed === undefined || !matchesIdentity(managed, identity)) {
			throw new ShardingStateError(`Shard ${identity.shardId} process is unavailable or stale.`);
		}
		return managed;
	}

	protected reserveOutbound(id: string, managed: ManagedShardProcess, kind: $OutboundOperationKind): void {
		requireProtocolIdentifier(id, "Request ID");
		if (!this.connectionReady) throw new ShardingStateError("Bridge is in maintenance.");
		if (this.outbound.has(id)) throw new ShardingCapacityError(`Request ${id} is already pending.`);
		if (this.outbound.size >= this.options.request.maxPending) {
			throw new ShardingCapacityError("Bridge outbound request capacity reached.");
		}
		const timer = setTimeout(() => {
			this.outbound.delete(id);
			void this.sendOutboundFailure(managed, kind, id, new ShardingTimeoutError(`Request ${id} timed out.`));
		}, this.options.request.timeoutMs);
		this.outbound.set(id, { kind, managed, timer });
	}

	protected findOutbound(id: string, kind: $OutboundOperationKind): $OutboundOperation | undefined {
		const pending = this.outbound.get(id);
		if (pending === undefined) return undefined;
		if (pending.kind !== kind) throw new ShardingProtocolError(`Response ${id} has the wrong operation type.`);
		return pending;
	}

	protected completeOutbound(id: string, pending: $OutboundOperation): void {
		if (this.outbound.get(id) !== pending) return;
		clearTimeout(pending.timer);
		this.outbound.delete(id);
	}

	protected discardOutbound(id: string, managed: ManagedShardProcess, kind: $OutboundOperationKind): void {
		const pending = this.outbound.get(id);
		if (pending === undefined || pending.managed !== managed || pending.kind !== kind) return;
		clearTimeout(pending.timer);
		this.outbound.delete(id);
	}

	protected async sendOutboundFailure(
		managed: ManagedShardProcess,
		kind: $OutboundOperationKind,
		id: string,
		cause: unknown,
	): Promise<void> {
		const error = serializeError(cause);
		try {
			if (kind === "identify") {
				await this.sendShard(managed, "shard.control.identify.response", id, { error, granted: false });
			} else if (kind === "route") {
				await this.sendShard(managed, "shard.control.route.response", id, { error, ok: false });
			} else {
				await this.sendShard(managed, "shard.control.eval.response", id, { error, ok: false });
			}
		} catch (sendCause) {
			this.report(toError(sendCause), `reject ${kind} request`);
		}
	}
}
