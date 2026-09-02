import type { ShardIdentityData } from "../../protocol/types";
import type { $PayloadPolicy, $RequestPolicy, $RestartPolicy, $Sleep } from "../../types/common";
import type {
	$AnalyticsRecord,
	$GatewayFetch,
	$PersistedAssignment,
	$PersistedBridge,
	$PersistedShard,
} from "../../types/hub";
import type { HubBridgeSession } from "../session/HubBridgeSession";

export interface NormalizedHubOptions {
	readonly adminToken: string;
	readonly botToken: string;
	readonly bridgeToken: string;
	readonly evaluationCommitLeadMs: number;
	readonly fetch: $GatewayFetch;
	readonly gatewayEndpoint: string | URL;
	readonly hostname: string;
	readonly keyPrefix: string;
	readonly maxBufferedBytes: number;
	readonly maxEvaluations: number;
	readonly maxQueuedMessages: number;
	readonly now: () => number;
	readonly onError?: (error: Error, context: string) => void;
	readonly payload: $PayloadPolicy;
	readonly persistence?: HubPersistenceAdapter;
	readonly port: number;
	readonly redisUrl: string;
	readonly request: $RequestPolicy;
	readonly sleep: $Sleep;
	readonly totalShards?: number;
	readonly wallClock: () => number;
}

export interface HubPersistenceAdapter {
	appendAnalytics(record: $AnalyticsRecord): Promise<void>;
	clearAnalyticsBatch(before: number, batchSize: number): Promise<number>;
	close(): Promise<void>;
	loadState(): Promise<unknown>;
	migrate(): Promise<void>;
	saveAssignment(assignment: $PersistedAssignment): Promise<void>;
	saveBridge(bridge: $PersistedBridge): Promise<void>;
	saveShard(shard: $PersistedShard): Promise<void>;
}

export interface PendingStop {
	readonly identity: ShardIdentityData;
	readonly reject: (reason: unknown) => void;
	readonly resolve: () => void;
	readonly session: HubBridgeSession;
	readonly timer: ReturnType<typeof setTimeout>;
}

export interface PendingStart {
	readonly assignmentEpoch: number;
	readonly completion: Promise<void>;
	readonly reject: (reason: unknown) => void;
	readonly resolve: () => void;
	readonly session: HubBridgeSession;
	readonly timer: ReturnType<typeof setTimeout>;
}

export interface PendingRoute {
	readonly sourceIdentity: ShardIdentityData;
	readonly sourceSession: HubBridgeSession;
	readonly targetIdentity: ShardIdentityData;
	readonly targetSession: HubBridgeSession;
	readonly timer: ReturnType<typeof setTimeout>;
}

export interface EvaluationTarget {
	readonly identity: ShardIdentityData;
	readonly session: HubBridgeSession;
	prepared: boolean;
	result?: unknown;
}

export interface PendingEvaluation {
	readonly context: unknown;
	readonly evaluator: string;
	phase: "preparing" | "running";
	readonly sourceIdentity: ShardIdentityData;
	readonly sourceSession: HubBridgeSession;
	readonly targets: Map<number, EvaluationTarget>;
	readonly timer: ReturnType<typeof setTimeout>;
}

export interface RestartHistory {
	readonly attempts: number[];
	readonly bridgeId: string;
	readonly policy: $RestartPolicy;
}

export interface RestartAssignmentIdentity {
	readonly assignmentEpoch: number;
	readonly shardId: number;
}

export interface LoadedState {
	readonly assignments: Map<number, $PersistedAssignment>;
	readonly bridges: Map<string, $PersistedBridge>;
	readonly shards: Map<number, $PersistedShard>;
}
