import { ShardingCapacityError, ShardingProtocolError } from "../../errors/ShardingError";
import { serializeError } from "../../internal/errors";
import { normalizePayload } from "../../internal/payload";
import { requireExactKeys } from "../../protocol/codec";
import { readInteger, readPayload, readString } from "../../protocol/readers";
import type { ParsedWireMessage } from "../../protocol/types";
import type { $AnalyticsRecord, $PersistedBridge } from "../../types/hub";
import { parseBridgeHello, parseHeartbeat, readIdentity, readShardState } from "../protocol";
import type { HubBridgeSession } from "../session/HubBridgeSession";
import { HubRoutingController } from "./HubRoutingController";
import {
	assignmentMatches,
	classifyShardStateUpdate,
	identitiesEqual,
	identityKey,
	isJsonValue,
	persistedShard,
	toError,
} from "./utilities";

export abstract class HubProtocolController extends HubRoutingController {
	protected async handleHello(session: HubBridgeSession, message: ParsedWireMessage): Promise<void> {
		await this.runAssignmentMutation(() => this.acceptHello(session, message));
	}

	protected async acceptHello(session: HubBridgeSession, message: ParsedWireMessage): Promise<void> {
		const hello = parseBridgeHello(message);
		if (
			hello.bridgeId !== session.bridgeId ||
			hello.bridgeGeneration !== session.bridgeGeneration ||
			hello.connectionGeneration !== session.connectionGeneration
		) {
			throw new ShardingProtocolError("Bridge hello does not match its authenticated upgrade headers.");
		}
		const current = this.sessions.get(session.bridgeId);
		if (current !== undefined && current !== session && current.phase !== "closed") {
			throw new ShardingProtocolError(`Bridge ${session.bridgeId} already has an active connection.`);
		}
		const retainedBridge = this.bridges.get(session.bridgeId);
		if (
			retainedBridge !== undefined &&
			retainedBridge.generation !== session.bridgeGeneration &&
			this.bridgeOwnsAssignments(session.bridgeId)
		) {
			throw new ShardingProtocolError(
				`Bridge ${session.bridgeId} must be explicitly released before a new process generation can inherit assignments.`,
			);
		}
		const previous = this.lastConnections.get(session.bridgeId);
		if (
			previous !== undefined &&
			previous.generation === session.bridgeGeneration &&
			session.connectionGeneration <= previous.connectionGeneration
		) {
			throw new ShardingProtocolError("Bridge connection generation is stale.");
		}
		for (const running of hello.runningShards) {
			if (running.shardId >= this.shardCount) {
				throw new ShardingProtocolError(`Bridge reported shard ${running.shardId} outside the global topology.`);
			}
		}
		if (this.assignments.size === 0 && hello.runningShards.length > 0) {
			throw new ShardingProtocolError(
				"Bridge reported retained shard processes but the Hub has no durable assignments.",
			);
		}
		session.acceptHello(hello.maxShards, hello.restartPolicy, hello.runningShards);
		this.lastConnections.set(session.bridgeId, {
			connectionGeneration: session.connectionGeneration,
			generation: session.bridgeGeneration,
		});
		this.sessions.set(session.bridgeId, session);
		const bridge: $PersistedBridge = Object.freeze({
			connected: true,
			generation: session.bridgeGeneration,
			id: session.bridgeId,
			maxShards: session.maxShards,
			updatedAt: this.nextPersistenceTimestamp(),
		});
		this.bridges.set(bridge.id, bridge);
		await this.saveBridge(bridge);
		this.hubEvents.emit("bridgeConnected", {
			bridgeId: session.bridgeId,
			connectionGeneration: session.connectionGeneration,
			generation: session.bridgeGeneration,
		});
		if (!this.isCurrentSession(session)) return;
		for (const running of session.runningShards.values()) {
			const assignment = this.assignments.get(running.shardId);
			if (!assignmentMatches(assignment, session.bridgeId, running)) continue;
			const previousShard = this.shards.get(running.shardId);
			if (
				previousShard !== undefined &&
				previousShard.bridgeId === session.bridgeId &&
				previousShard.assignmentEpoch === running.assignmentEpoch &&
				previousShard.processGeneration > running.processGeneration
			) {
				throw new ShardingProtocolError(
					`Bridge ${session.bridgeId} reported stale process generation ${running.processGeneration} for shard ${running.shardId}.`,
				);
			}
			const state = running.ready ? "ready" : "starting";
			const stateUpdate = classifyShardStateUpdate(previousShard, session.bridgeId, running, state);
			if (stateUpdate === "stale") {
				throw new ShardingProtocolError(
					`Bridge ${session.bridgeId} reported stale state for shard ${running.shardId}.`,
				);
			}
			if (stateUpdate === "duplicate") continue;
			const shard = persistedShard(session.bridgeId, running, state, this.nextPersistenceTimestamp());
			await this.saveShard(shard);
			if (!this.isCurrentSession(session)) return;
			this.shards.set(shard.shardId, shard);
		}
		for (const previousShard of [...this.shards.values()]) {
			if (
				previousShard.bridgeId !== session.bridgeId ||
				session.runningShards.has(previousShard.shardId) ||
				previousShard.state === "stopped" ||
				previousShard.state === "failed"
			) {
				continue;
			}
			const stopped = Object.freeze({
				...previousShard,
				state: "stopped" as const,
				updatedAt: this.nextPersistenceTimestamp(),
			});
			await this.saveShard(stopped);
			if (!this.isCurrentSession(session)) return;
			this.shards.set(stopped.shardId, stopped);
		}
		const synchronization = this.synchronizeSession(session);
		void synchronization.then(
			() => this.handleSessionReady(session),
			(cause: unknown) => {
				if (session.phase !== "closed") session.close(1002, "Topology sync failed");
				this.report(toError(cause), `Bridge ${session.bridgeId} topology sync`);
			},
		);
	}

	protected handleSyncReady(session: HubBridgeSession, message: ParsedWireMessage): void {
		requireExactKeys(message.data, new Set(["topologyVersion"]), "bridge.sync.ready data");
		session.acknowledgeSynchronization(
			message.id,
			readInteger(message.data, "topologyVersion", 1, Number.MAX_SAFE_INTEGER),
		);
	}

	protected handleHeartbeat(session: HubBridgeSession, message: ParsedWireMessage): void {
		const heartbeat = parseHeartbeat(message);
		if (
			heartbeat.bridgeGeneration !== session.bridgeGeneration ||
			heartbeat.connectionGeneration !== session.connectionGeneration
		) {
			throw new ShardingProtocolError("Bridge heartbeat generation is stale.");
		}
		session.send("hub.heartbeat", message.id, { sentAt: heartbeat.sentAt });
	}

	protected handleIdentifyRequest(session: HubBridgeSession, message: ParsedWireMessage): void {
		requireExactKeys(
			message.data,
			new Set(["assignmentEpoch", "processGeneration", "shardId"]),
			"bridge.identify.request data",
		);
		const identity = readIdentity(message.data);
		this.requireSessionIdentity(session, identity, false);
		const key = identityKey(session.bridgeId, identity);
		if (this.identifyKeys.has(key)) {
			session.send("hub.identify.response", message.id, {
				error: serializeError(new ShardingCapacityError("An identify is already pending for this shard process.")),
				granted: false,
				shardId: identity.shardId,
			});
			return;
		}
		this.identifyKeys.add(key);
		const scheduler = this.requireIdentifyScheduler();
		void scheduler
			.schedule(
				identity.shardId,
				async () => {
					this.requireSessionIdentity(session, identity, false);
					session.send("hub.identify.response", message.id, {
						granted: true,
						shardId: identity.shardId,
					});
				},
				session.lifecycle.signal,
			)
			.catch((cause: unknown) => {
				if (session.phase === "closed") return;
				try {
					session.send("hub.identify.response", message.id, {
						error: serializeError(cause),
						granted: false,
						shardId: identity.shardId,
					});
				} catch (sendCause) {
					this.report(toError(sendCause), `Bridge ${session.bridgeId} identify rejection`);
				}
			})
			.finally(() => {
				this.identifyKeys.delete(key);
			});
	}

	protected async handleShardState(session: HubBridgeSession, message: ParsedWireMessage): Promise<void> {
		requireExactKeys(
			message.data,
			new Set(["assignmentEpoch", "processGeneration", "shardId", "state"]),
			"bridge.shard.state data",
		);
		const identity = readIdentity(message.data);
		this.requireAssignedIdentity(session, identity);
		const state = readShardState(message.data);
		const previousShard = this.shards.get(identity.shardId);
		const stateUpdate = classifyShardStateUpdate(previousShard, session.bridgeId, identity, state);
		if (stateUpdate !== "apply") {
			if (stateUpdate === "duplicate" && (state === "starting" || state === "ready")) {
				session.runningShards.set(
					identity.shardId,
					Object.freeze({
						...identity,
						ready: state === "ready",
					}),
				);
				this.completePendingStart(session, identity);
				if (state === "ready") this.clearRestartState(identity.shardId);
			}
			return;
		}
		const shard = persistedShard(session.bridgeId, identity, state, this.nextPersistenceTimestamp());
		await this.saveShard(shard);
		if (!this.isCurrentSession(session)) return;
		if (state === "failed" || state === "stopped") {
			session.runningShards.delete(identity.shardId);
		} else {
			session.runningShards.set(
				identity.shardId,
				Object.freeze({
					...identity,
					ready: state === "ready",
				}),
			);
		}
		this.shards.set(identity.shardId, shard);
		this.completePendingStart(session, identity);
		if (state === "ready") {
			this.clearRestartState(identity.shardId);
			this.hubEvents.emit("shardReady", { bridgeId: session.bridgeId, shardId: identity.shardId });
		} else if (state === "failed") {
			this.hubEvents.emit("shardFailed", { bridgeId: session.bridgeId, shardId: identity.shardId });
			this.scheduleRestart(session, identity);
		} else if (state === "stopped") {
			this.hubEvents.emit("shardStopped", { bridgeId: session.bridgeId, shardId: identity.shardId });
		}
	}

	protected async handleShardStopped(session: HubBridgeSession, message: ParsedWireMessage): Promise<void> {
		requireExactKeys(
			message.data,
			new Set(["assignmentEpoch", "commandId", "processGeneration", "shardId"]),
			"bridge.shard.stopped data",
		);
		const pending = this.pendingStops.get(message.id);
		if (pending === undefined) return;
		const identity = readIdentity(message.data);
		if (
			pending.session !== session ||
			!identitiesEqual(pending.identity, identity) ||
			readString(message.data, "commandId") !== message.id
		) {
			throw new ShardingProtocolError("Shard stop acknowledgement is stale or unexpected.");
		}
		const shard = persistedShard(session.bridgeId, identity, "stopped", this.nextPersistenceTimestamp());
		await this.saveShard(shard);
		if (this.pendingStops.get(message.id) !== pending || !this.isCurrentSession(session)) return;
		clearTimeout(pending.timer);
		this.pendingStops.delete(message.id);
		this.pendingIds.delete(message.id);
		session.runningShards.delete(identity.shardId);
		this.shards.set(identity.shardId, shard);
		this.hubEvents.emit("shardStopped", { bridgeId: session.bridgeId, shardId: identity.shardId });
		pending.resolve();
	}

	protected async handleAnalytics(session: HubBridgeSession, message: ParsedWireMessage): Promise<void> {
		requireExactKeys(
			message.data,
			new Set(["assignmentEpoch", "collectedAt", "payload", "processGeneration", "shardId"]),
			"bridge.analytics data",
		);
		const identity = readIdentity(message.data);
		this.requireSessionIdentity(session, identity, false);
		const data = normalizePayload(readPayload(message.data, "payload"), this.options.payload, "analytics payload");
		if (!isJsonValue(data)) throw new ShardingProtocolError("Analytics payload is not JSON-compatible.");
		const record: $AnalyticsRecord = Object.freeze({
			bridgeId: session.bridgeId,
			collectedAt: readInteger(message.data, "collectedAt", 0, Number.MAX_SAFE_INTEGER),
			data,
			id: message.id,
			shardId: identity.shardId,
		});
		await this.requirePersistence().appendAnalytics(record);
	}
}
