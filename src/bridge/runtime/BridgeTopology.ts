import {
	ShardingCapacityError,
	ShardingProtocolError,
	ShardingStateError,
	ShardingTransportError,
} from "../../errors/ShardingError";
import { serializeError } from "../../internal/errors";
import { MAX_SHARDS } from "../../internal/limits";
import { createRequestId } from "../../internal/validation";
import { createWireMessage, requireExactKeys } from "../../protocol/codec";
import { readArray, readInteger, readShardId, readString } from "../../protocol/readers";
import type { ParsedWireMessage } from "../../protocol/types";
import { type ManagedShardOptions, ManagedShardProcess } from "../shards/ManagedShardProcess";
import { BridgeShards } from "./BridgeShards";
import { buildEnvironment as buildShardEnvironment } from "./configuration";
import {
	clearSyncAcknowledgements,
	copyRecord,
	identityOf,
	mapsEqual,
	matchesIdentity,
	readIdentity,
	toError,
} from "./protocol";

export abstract class BridgeTopology extends BridgeShards {
	protected async handleSync(
		message: ParsedWireMessage,
		socket: WebSocket,
		connectionGeneration: number,
	): Promise<void> {
		requireExactKeys(
			message.data,
			new Set(["assignments", "bridgeGeneration", "connectionGeneration", "topologyVersion", "totalShards"]),
			"hub.sync data",
		);
		if (readString(message.data, "bridgeGeneration") !== this.generation) {
			throw new ShardingProtocolError("Hub sync targets a stale Bridge generation.");
		}
		if (readInteger(message.data, "connectionGeneration", 1) !== this.connectionGeneration) {
			throw new ShardingProtocolError("Hub sync targets a stale connection generation.");
		}
		const totalShards = readInteger(message.data, "totalShards", 1, MAX_SHARDS);
		const topologyVersion = readInteger(message.data, "topologyVersion", 1, Number.MAX_SAFE_INTEGER);
		const assignments = readArray(message.data, "assignments", this.maxShards);
		const nextAssignments = new Map<number, number>();
		for (const value of assignments) {
			const record = copyRecord(value, "Hub assignment");
			requireExactKeys(record, new Set(["epoch", "shardId"]), "Hub assignment");
			const shardId = readShardId(record);
			if (shardId >= totalShards) throw new ShardingProtocolError("Hub assignment shardId exceeds totalShards.");
			if (nextAssignments.has(shardId)) throw new ShardingProtocolError(`Duplicate assignment for shard ${shardId}.`);
			nextAssignments.set(shardId, readInteger(record, "epoch", 1, Number.MAX_SAFE_INTEGER));
		}
		if (topologyVersion < this.connectionTopologyVersion) {
			throw new ShardingProtocolError("Hub topology version cannot move backwards.");
		}
		const repeatsCurrentTopology =
			topologyVersion === this.connectionTopologyVersion &&
			this.connectionTopologyVersion > 0 &&
			totalShards === this.totalShards &&
			mapsEqual(nextAssignments, this.assignments);
		if (
			topologyVersion === this.connectionTopologyVersion &&
			this.connectionTopologyVersion > 0 &&
			!repeatsCurrentTopology
		) {
			throw new ShardingProtocolError("Hub changed topology content without advancing its version.");
		}
		if (repeatsCurrentTopology && this.connectionReady) {
			await this.sendHub("bridge.sync.ready", message.id, { topologyVersion });
			return;
		}
		await this.enterTopologySynchronization();
		const totalShardsChanged = this.totalShards !== 0 && totalShards !== this.totalShards;
		const stops: Promise<void>[] = [];
		for (const managed of this.processes.values()) {
			const epoch = nextAssignments.get(managed.shardId);
			if (!totalShardsChanged && epoch === managed.assignmentEpoch) continue;
			stops.push(
				this.stopUnassigned(
					managed,
					totalShardsChanged
						? "Global shard count changed; this process must restart."
						: "Shard is no longer assigned to this Bridge.",
				),
			);
		}
		await Promise.all(stops);
		if (this.socket !== socket || this.connectionGeneration !== connectionGeneration) return;
		this.assignments.clear();
		for (const [shardId, epoch] of nextAssignments) this.assignments.set(shardId, epoch);
		this.topologyVersion = topologyVersion;
		this.connectionTopologyVersion = topologyVersion;
		this.totalShards = totalShards;
		await this.synchronizeMaintenance(message.id);
	}

	protected async enterTopologySynchronization(): Promise<void> {
		const cause = new ShardingStateError("Bridge topology synchronization replaced pending Hub work.");
		this.connectionReady = false;
		this.syncId = undefined;
		this.updateMaintenance(true);
		clearSyncAcknowledgements(this.syncAcknowledgements);
		for (const [id, pending] of this.outbound) {
			clearTimeout(pending.timer);
			this.outbound.delete(id);
			await this.sendOutboundFailure(pending.managed, pending.kind, id, cause);
		}
		const error = serializeError(cause);
		for (const [id, route] of this.inboundRoutes) {
			clearTimeout(route.timer);
			this.inboundRoutes.delete(id);
			await this.sendHub("bridge.route.response", id, {
				...identityOf(route.managed),
				error,
				ok: false,
				sourceShardId: route.sourceShardId,
			});
		}
	}

	protected async synchronizeMaintenance(syncId: string): Promise<void> {
		clearSyncAcknowledgements(this.syncAcknowledgements);
		this.syncId = syncId;
		const processes = [...this.processes.values()].filter(
			(managed) =>
				this.assignments.get(managed.shardId) === managed.assignmentEpoch &&
				(managed.state === "starting" || managed.state === "ready"),
		);
		if (processes.length === 0) {
			await this.finishSynchronization(syncId);
			return;
		}
		for (const managed of processes) {
			const id = createRequestId(`sync-${managed.shardId}`);
			const timer = setTimeout(() => {
				this.syncAcknowledgements.delete(id);
				this.socket?.close(1002, "Shard topology acknowledgement timed out");
			}, this.options.request.timeoutMs);
			this.syncAcknowledgements.set(id, { shardId: managed.shardId, timer });
			try {
				await this.sendShard(managed, "shard.control.maintenance", id, {
					acknowledge: true,
					maintenance: false,
					topologyVersion: this.topologyVersion,
				});
			} catch (cause) {
				clearTimeout(timer);
				this.syncAcknowledgements.delete(id);
				throw cause;
			}
		}
	}

	protected async finishSynchronization(syncId: string): Promise<void> {
		if (this.syncId !== syncId) return;
		await this.sendHub("bridge.sync.ready", syncId, {
			topologyVersion: this.topologyVersion,
		});
		if (this.syncId !== syncId) return;
		this.syncId = undefined;
		this.connectionReady = true;
		this.synchronizedConnectionGeneration = this.connectionGeneration;
		this.updateMaintenance(false);
		this.resolveConnectionWaiters();
	}

	protected async handleStart(message: ParsedWireMessage): Promise<void> {
		requireExactKeys(message.data, new Set(["assignmentEpoch", "shardId", "totalShards"]), "hub.shard.start data");
		if (!this.connectionReady)
			throw new ShardingStateError("Hub cannot start a shard before topology synchronization.");
		const shardId = readShardId(message.data);
		const totalShards = readInteger(message.data, "totalShards", 1, MAX_SHARDS);
		const assignmentEpoch = readInteger(message.data, "assignmentEpoch", 1, Number.MAX_SAFE_INTEGER);
		if (totalShards !== this.totalShards) {
			throw new ShardingProtocolError("Shard start totalShards does not match the synchronized topology.");
		}
		if (this.assignments.get(shardId) !== assignmentEpoch) {
			throw new ShardingProtocolError("Shard start does not match the synchronized assignment.");
		}
		const existing = this.processes.get(shardId);
		if (existing !== undefined) {
			if (
				existing.assignmentEpoch === assignmentEpoch &&
				(existing.state === "starting" || existing.state === "ready")
			) {
				await this.notifyShardState(existing, existing.state, true);
				return;
			}
			await this.stopUnassigned(existing, "Shard process was superseded.");
		}
		if (this.processes.size >= this.maxShards) {
			throw new ShardingCapacityError(`Bridge capacity of ${this.maxShards} shards has been reached.`);
		}
		const processGeneration = (this.nextProcessGeneration.get(shardId) ?? 0) + 1;
		this.nextProcessGeneration.set(shardId, processGeneration);
		const environment = buildShardEnvironment(
			this.options.environment,
			shardId,
			totalShards,
			assignmentEpoch,
			processGeneration,
		);
		const managedOptions: ManagedShardOptions = {
			args: this.options.args,
			assignmentEpoch,
			callbacks: {
				onExit: (managed, exit) => this.handleProcessExit(managed, exit.error),
				onMessage: (managed, value) => this.enqueueShardMessage(managed, value),
				onState: (managed, state) => {
					void this.notifyShardState(managed, state).catch((cause: unknown) =>
						this.report(toError(cause), `shard ${managed.shardId} state`),
					);
				},
			},
			...(this.options.cwd === undefined ? {} : { cwd: this.options.cwd }),
			environment,
			maxPendingSends: this.options.request.maxPending,
			processFactory: this.options.processFactory,
			processGeneration,
			requestTimeoutMs: this.options.request.timeoutMs,
			script: this.options.shardScript,
			shardId,
			shutdownTimeoutMs: this.options.shutdownTimeoutMs,
			startupTimeoutMs: this.options.startupTimeoutMs,
			totalShards,
		};
		const managed = new ManagedShardProcess(managedOptions);
		this.processes.set(shardId, managed);
		this.shardInboundQueues.set(managed, { failed: false, pending: 0, tail: Promise.resolve() });
		try {
			managed.start();
			if (this.processes.get(shardId) !== managed || (managed.state !== "starting" && managed.state !== "ready")) {
				throw new ShardingTransportError(`Shard ${shardId} exited during startup.`);
			}
			await this.notifyShardState(managed, "starting", true);
		} catch (cause) {
			try {
				const shutdown = createWireMessage(
					"shard.control.shutdown",
					createRequestId(`startup-failed-${shardId}`),
					{ commandId: "startup-notification-failed", reason: "Shard startup could not be registered with the Hub." },
					this.payloadPolicy,
				);
				await managed.stop(shutdown);
				if (this.processes.get(shardId) === managed) this.processes.delete(shardId);
				this.shardInboundQueues.delete(managed);
			} catch (cleanupCause) {
				throw new AggregateError(
					[cause, cleanupCause],
					`Shard ${shardId} startup notification failed and its process could not be stopped.`,
				);
			}
			throw cause;
		}
	}

	protected async handleStop(message: ParsedWireMessage): Promise<void> {
		requireExactKeys(
			message.data,
			new Set(["assignmentEpoch", "processGeneration", "reason", "shardId"]),
			"hub.shard.stop data",
		);
		const identity = readIdentity(message.data);
		const reason = readString(message.data, "reason", 512);
		const managed = this.processes.get(identity.shardId);
		if (managed !== undefined && matchesIdentity(managed, identity)) {
			const shutdown = createWireMessage(
				"shard.control.shutdown",
				message.id,
				{ commandId: message.id, reason },
				this.payloadPolicy,
			);
			await managed.stop(shutdown);
			if (this.processes.get(identity.shardId) === managed) this.processes.delete(identity.shardId);
		}
		await this.sendHub("bridge.shard.stopped", message.id, {
			...identity,
			commandId: message.id,
		});
	}

	protected handleSyncAcknowledgement(managed: ManagedShardProcess, message: ParsedWireMessage): void {
		requireExactKeys(message.data, new Set(["topologyVersion"]), "shard.sync.ack data");
		const acknowledgement = this.syncAcknowledgements.get(message.id);
		if (acknowledgement === undefined) return;
		if (
			acknowledgement.shardId !== managed.shardId ||
			readInteger(message.data, "topologyVersion", 1, Number.MAX_SAFE_INTEGER) !== this.topologyVersion
		) {
			throw new ShardingProtocolError("Shard topology acknowledgement is stale or unexpected.");
		}
		clearTimeout(acknowledgement.timer);
		this.syncAcknowledgements.delete(message.id);
		if (this.syncAcknowledgements.size === 0) {
			const syncId = this.syncId;
			if (syncId === undefined) throw new ShardingProtocolError("Shard topology acknowledgement has no active sync.");
			void this.finishSynchronization(syncId).catch((cause: unknown) => {
				this.report(toError(cause), "Hub topology acknowledgement");
				this.socket?.close(1011, "Topology acknowledgement failed");
			});
		}
	}
}
