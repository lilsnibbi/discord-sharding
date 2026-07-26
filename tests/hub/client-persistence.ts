import type {
	$AnalyticsRecord,
	$HubPersistence,
	$PersistedAssignment,
	$PersistedBridge,
	$PersistedHubState,
	$PersistedShard,
} from "../../src/types/hub";

export class MemoryHubPersistence implements $HubPersistence {
	public readonly analytics: $AnalyticsRecord[] = [];
	public readonly assignments = new Map<number, $PersistedAssignment>();
	public readonly bridges = new Map<string, $PersistedBridge>();
	public readonly clearAnalyticsCalls: Array<{ readonly batchSize: number; readonly before: number }> = [];
	public readonly events: string[] = [];
	public readonly savedShards: $PersistedShard[] = [];
	public closeCalls = 0;
	public closeOperation: () => Promise<void> = () => Promise.resolve();
	public clearAnalyticsOperation: (before: number, batchSize: number) => Promise<number> = () => Promise.resolve(0);
	public loadStateOperation: () => Promise<void> = () => Promise.resolve();
	public migrateOperation: () => Promise<void> = () => Promise.resolve();
	public saveAssignmentOperation: (assignment: $PersistedAssignment) => Promise<void> = () => Promise.resolve();
	public saveBridgeOperation: (bridge: $PersistedBridge) => Promise<void> = () => Promise.resolve();
	public saveShardOperation: (shard: $PersistedShard) => Promise<void> = () => Promise.resolve();

	public constructor(initial: Partial<$PersistedHubState> = {}) {
		for (const assignment of initial.assignments ?? []) this.assignments.set(assignment.shardId, assignment);
		for (const bridge of initial.bridges ?? []) this.bridges.set(bridge.id, bridge);
		for (const shard of initial.shards ?? []) this.savedShards.push(shard);
	}

	public appendAnalytics(record: $AnalyticsRecord): Promise<void> {
		this.analytics.push(record);
		return Promise.resolve();
	}

	public clearAnalyticsBatch(before: number, batchSize: number): Promise<number> {
		this.clearAnalyticsCalls.push({ batchSize, before });
		return this.clearAnalyticsOperation(before, batchSize);
	}

	public async close(): Promise<void> {
		this.closeCalls += 1;
		await this.closeOperation();
	}

	public async loadState(): Promise<$PersistedHubState> {
		await this.loadStateOperation();
		return {
			assignments: [...this.assignments.values()],
			bridges: [...this.bridges.values()],
			shards: [...this.savedShards],
		};
	}

	public migrate(): Promise<void> {
		return this.migrateOperation();
	}

	public async saveAssignment(assignment: $PersistedAssignment): Promise<void> {
		this.events.push(`assignment:${assignment.shardId}:${assignment.bridgeId}:start`);
		await this.saveAssignmentOperation(assignment);
		this.assignments.set(assignment.shardId, assignment);
		this.events.push(`assignment:${assignment.shardId}:${assignment.bridgeId}:end`);
	}

	public async saveBridge(bridge: $PersistedBridge): Promise<void> {
		this.events.push(`bridge:${bridge.connected ? "connected" : "disconnected"}:start`);
		await this.saveBridgeOperation(bridge);
		this.bridges.set(bridge.id, bridge);
		this.events.push(`bridge:${bridge.connected ? "connected" : "disconnected"}:end`);
	}

	public async saveShard(shard: $PersistedShard): Promise<void> {
		await this.saveShardOperation(shard);
		this.savedShards.push(shard);
		this.events.push(`shard:${shard.state}`);
	}
}
