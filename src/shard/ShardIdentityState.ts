import { ShardingProtocolError } from "../errors/ShardingError";
import { MAX_BRIDGES, MAX_SHARDS } from "../internal/limits";
import { requireExactKeys } from "../protocol/codec";
import { readArray, readInteger, readString } from "../protocol/readers";
import type { ParsedWireMessage } from "../protocol/types";
import type { $ShardBridgeSummary, $ShardIdentity } from "../types/shard";
import { copyRecord } from "./runtime";

const TOPOLOGY_KEYS = new Set(["bridgeId", "bridges", "topologyVersion", "totalShards"]);
const SUMMARY_KEYS = new Set(["bridgeId", "shardCount"]);

interface FixedIdentity {
	readonly assignmentEpoch: number;
	readonly processGeneration: number;
	readonly shardId: number;
	readonly totalShards: number;
}

/**
 * Holds one shard's frozen self-description and applies topology updates.
 *
 * Spawn-time identity is fixed; the Bridge and cluster view is replaced by
 * each `shard.control.topology` message the owning Bridge forwards.
 */
export class ShardIdentityState {
	readonly #fixed: FixedIdentity;
	#bridgeId: string | null;
	#bridges: readonly $ShardBridgeSummary[] = Object.freeze([]);
	#topologyVersion = 0;
	#snapshot: $ShardIdentity;

	public constructor(fixed: FixedIdentity, bridgeId: string | null) {
		this.#fixed = fixed;
		this.#bridgeId = bridgeId;
		this.#snapshot = this.#build();
	}

	/**
	 * Current frozen identity snapshot.
	 */
	public get snapshot(): $ShardIdentity {
		return this.#snapshot;
	}

	/**
	 * Applies one validated `shard.control.topology` message.
	 *
	 * @param message - Parsed wire message from the owning Bridge.
	 */
	public applyTopologyMessage(message: ParsedWireMessage): void {
		requireExactKeys(message.data, TOPOLOGY_KEYS, "shard.control.topology data");
		const bridgeId = readString(message.data, "bridgeId");
		const topologyVersion = readInteger(message.data, "topologyVersion", 1, Number.MAX_SAFE_INTEGER);
		const totalShards = readInteger(message.data, "totalShards", 1, MAX_SHARDS);
		if (totalShards !== this.#fixed.totalShards) {
			throw new ShardingProtocolError("Topology totalShards does not match this shard process.");
		}
		const entries = readArray(message.data, "bridges", MAX_BRIDGES);
		const bridges: $ShardBridgeSummary[] = [];
		const seen = new Set<string>();
		for (const value of entries) {
			if (typeof value !== "object" || value === null || Array.isArray(value)) {
				throw new ShardingProtocolError("Topology bridge entry must be an object.");
			}
			const record = copyRecord(value, "Topology bridge entry");
			requireExactKeys(record, SUMMARY_KEYS, "Topology bridge entry");
			const entryBridgeId = readString(record, "bridgeId");
			if (seen.has(entryBridgeId)) {
				throw new ShardingProtocolError(`Duplicate topology entry for Bridge ${entryBridgeId}.`);
			}
			seen.add(entryBridgeId);
			bridges.push(
				Object.freeze({ bridgeId: entryBridgeId, shardCount: readInteger(record, "shardCount", 0, MAX_SHARDS) }),
			);
		}
		if (topologyVersion < this.#topologyVersion) return;
		this.#topologyVersion = topologyVersion;
		this.#bridgeId = bridgeId;
		this.#bridges = Object.freeze(bridges);
		this.#snapshot = this.#build();
	}

	#build(): $ShardIdentity {
		const bridgeId = this.#bridgeId;
		const bridges = this.#bridges;
		let bridgeShardCount = 0;
		for (const summary of bridges) {
			if (summary.bridgeId === bridgeId) {
				bridgeShardCount = summary.shardCount;
				break;
			}
		}
		return Object.freeze({
			assignmentEpoch: this.#fixed.assignmentEpoch,
			bridgeId,
			bridgeShardCount,
			bridges,
			instanceId: `${bridgeId ?? "standalone"}:${this.#fixed.shardId}:${this.#fixed.assignmentEpoch}:${this.#fixed.processGeneration}`,
			processGeneration: this.#fixed.processGeneration,
			shardId: this.#fixed.shardId,
			totalBridges: bridges.length,
			totalShards: this.#fixed.totalShards,
		});
	}
}
