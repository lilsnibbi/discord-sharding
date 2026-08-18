import { ShardingCapacityError, ShardingProtocolError, ShardingTimeoutError } from "../../errors/ShardingError";
import { MAX_SHARDS } from "../../internal/limits";
import { createRequestId, requireTotalShards } from "../../internal/validation";
import { createWireMessage, parseWireMessage, requireExactKeys } from "../../protocol/codec";
import { readInteger, readString } from "../../protocol/readers";
import { type ParsedWireMessage, SHARD_TO_BRIDGE_TYPES } from "../../protocol/types";
import type { ManagedShardProcess } from "../shards/ManagedShardProcess";
import { BridgeRequests } from "./BridgeRequests";
import { matchesIdentity, readIdentity, toError } from "./protocol";

/** How often running shard processes are checked for silence. */
export const SHARD_WATCHDOG_INTERVAL_MS = 10_000;
/**
 * Longest silence tolerated from a Discord-ready shard process.
 *
 * Shards heartbeat every ten seconds. A process whose event loop is blocked
 * stays alive and keeps its `ready` state, so the Hub would go on routing
 * traffic and broadcast evaluations to a shard that can never answer.
 */
export const SHARD_SILENCE_TIMEOUT_MS = 45_000;

export abstract class BridgeShards extends BridgeRequests {
	protected abstract handleSyncAcknowledgement(managed: ManagedShardProcess, message: ParsedWireMessage): void;

	protected async handleShardMessage(managed: ManagedShardProcess, value: unknown): Promise<void> {
		if (this.processes.get(managed.shardId) !== managed) return;
		const message = parseWireMessage(value, SHARD_TO_BRIDGE_TYPES, this.payloadPolicy);
		switch (message.type) {
			case "shard.booted":
				await this.handleShardBooted(managed, message);
				return;
			case "shard.ready":
				this.requireMessageIdentity(managed, message);
				try {
					managed.markReady();
				} catch (cause) {
					throw new ShardingProtocolError("Shard became ready from an invalid process state.", { cause });
				}
				return;
			case "shard.heartbeat":
				this.requireMessageIdentity(managed, message);
				return;
			case "shard.identify.request":
				requireExactKeys(message.data, new Set(), "shard.identify.request data");
				await this.forwardShardIdentify(managed, message.id);
				return;
			case "shard.route.request":
				await this.forwardShardRoute(managed, message);
				return;
			case "shard.route.response":
				await this.forwardShardRouteResponse(managed, message);
				return;
			case "shard.eval.request":
				await this.forwardShardEval(managed, message);
				return;
			case "shard.eval.prepared":
				await this.forwardShardEvalPrepared(managed, message);
				return;
			case "shard.eval.result":
				await this.forwardShardEvalResult(managed, message);
				return;
			case "shard.analytics":
				await this.handleShardAnalytics(managed, message);
				return;
			case "shard.sync.ack":
				this.handleSyncAcknowledgement(managed, message);
				return;
			case "shard.shutdown.complete":
				requireExactKeys(message.data, new Set(["commandId"]), "shard.shutdown.complete data");
				readString(message.data, "commandId");
				return;
			default:
				throw new ShardingProtocolError(`Unhandled shard message ${message.type}.`);
		}
	}

	protected enqueueShardMessage(managed: ManagedShardProcess, value: unknown): void {
		if (this.processes.get(managed.shardId) !== managed) return;
		const queue = this.shardInboundQueues.get(managed);
		if (queue === undefined || queue.failed) return;
		if (queue.pending >= this.options.request.maxPending) {
			queue.failed = true;
			const error = new ShardingCapacityError(`Shard ${managed.shardId} inbound IPC capacity reached.`);
			this.report(error, `shard ${managed.shardId} IPC`);
			void this.terminateMisbehavingShard(managed, error);
			return;
		}
		queue.lastMessageAt = Date.now();
		queue.pending += 1;
		queue.tail = queue.tail
			.then(async () => {
				if (queue.failed || this.processes.get(managed.shardId) !== managed) return;
				try {
					await this.handleShardMessage(managed, value);
				} catch (cause) {
					const error = toError(cause);
					this.report(error, `shard ${managed.shardId} IPC`);
					if (!(error instanceof ShardingProtocolError)) return;
					queue.failed = true;
					await this.terminateMisbehavingShard(managed, error);
				}
			})
			.finally(() => {
				queue.pending -= 1;
			});
	}

	protected sweepUnresponsiveShards(now: number): void {
		for (const managed of [...this.processes.values()]) {
			if (managed.state !== "ready") continue;
			const queue = this.shardInboundQueues.get(managed);
			if (queue === undefined || queue.failed) continue;
			const silence = now - queue.lastMessageAt;
			if (silence <= SHARD_SILENCE_TIMEOUT_MS) continue;
			queue.failed = true;
			this.report(
				new ShardingTimeoutError(`Shard ${managed.shardId} sent nothing for ${silence}ms.`),
				`shard ${managed.shardId} liveness`,
			);
			void managed
				.terminate()
				.catch((cause: unknown) => this.report(toError(cause), `shard ${managed.shardId} liveness`));
		}
	}

	protected async terminateMisbehavingShard(managed: ManagedShardProcess, cause: Error): Promise<void> {
		if (this.processes.get(managed.shardId) !== managed) return;
		const shutdown = createWireMessage(
			"shard.control.shutdown",
			createRequestId(`invalid-ipc-${managed.shardId}`),
			{ commandId: "invalid-shard-ipc", reason: "Shard IPC violated its Bridge contract." },
			this.payloadPolicy,
		);
		try {
			await managed.stop(shutdown);
			if (this.processes.get(managed.shardId) === managed) this.processes.delete(managed.shardId);
			this.shardInboundQueues.delete(managed);
		} catch (stopCause) {
			this.report(
				new AggregateError([cause, stopCause], `Shard ${managed.shardId} IPC failure cleanup did not complete.`),
				`shard ${managed.shardId} cleanup`,
			);
		}
	}

	protected async handleShardBooted(managed: ManagedShardProcess, message: ParsedWireMessage): Promise<void> {
		requireExactKeys(
			message.data,
			new Set(["assignmentEpoch", "processGeneration", "shardId", "totalShards"]),
			"shard.booted data",
		);
		this.requireMessageIdentity(managed, message);
		const totalShards = requireTotalShards(readInteger(message.data, "totalShards", 1, MAX_SHARDS));
		if (totalShards !== this.totalShards) {
			throw new ShardingProtocolError("Shard boot totalShards does not match the synchronized topology.");
		}
		await this.sendShardTopology(managed);
		const id = createRequestId(`maintenance-${managed.shardId}`);
		await this.sendShard(managed, "shard.control.maintenance", id, {
			acknowledge: false,
			maintenance: this.maintenance,
			topologyVersion: Math.max(1, this.topologyVersion),
		});
	}

	protected requireMessageIdentity(managed: ManagedShardProcess, message: ParsedWireMessage): void {
		const identity = readIdentity(message.data);
		if (!matchesIdentity(managed, identity)) throw new ShardingProtocolError("Shard IPC generation is stale.");
	}
}
