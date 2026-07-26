import type { $BridgeSocketFactory, $ShardProcessFactory } from "../../types/bridge";
import type { $PayloadPolicy, $ReconnectPolicy, $RequestPolicy, $RestartPolicy, $Sleep } from "../../types/common";
import type { ManagedShardProcess } from "../shards/ManagedShardProcess";

export interface $NormalizedBridgeOptions {
	readonly analyticsPath: string;
	readonly args: readonly string[];
	readonly cwd?: string;
	readonly environment: Readonly<Record<string, string | undefined>>;
	readonly hubUrl: string;
	readonly id: string;
	readonly maxBufferedBytes: number;
	readonly maxShards: number;
	readonly onError?: (error: Error, context: string) => void;
	readonly payload: $PayloadPolicy;
	readonly processFactory: $ShardProcessFactory;
	readonly random: () => number;
	readonly reconnect: $ReconnectPolicy;
	readonly request: $RequestPolicy;
	readonly restart: $RestartPolicy;
	readonly shardScript: string;
	readonly shutdownTimeoutMs: number;
	readonly sleep: $Sleep;
	readonly socketFactory: $BridgeSocketFactory;
	readonly startupTimeoutMs: number;
	readonly token: string;
}

export type $OutboundOperationKind = "eval" | "identify" | "route";

export interface $OutboundOperation {
	readonly kind: $OutboundOperationKind;
	readonly managed: ManagedShardProcess;
	readonly timer: ReturnType<typeof setTimeout>;
}

export interface $InboundRoute {
	readonly managed: ManagedShardProcess;
	readonly sourceShardId: number | null;
	readonly timer: ReturnType<typeof setTimeout>;
}

export interface $SyncAcknowledgement {
	readonly shardId: number;
	readonly timer: ReturnType<typeof setTimeout>;
}

export interface $ConnectionWaiter {
	readonly reject: (error: unknown) => void;
	readonly resolve: () => void;
	readonly timer: ReturnType<typeof setTimeout>;
}

export interface $ShardInboundQueue {
	failed: boolean;
	pending: number;
	tail: Promise<void>;
}
