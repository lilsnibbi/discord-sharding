/**
 * Payload for the `bridgeConnected` Hub event.
 */
export interface $HubBridgeConnectedEvent {
	/**
	 * Stable Bridge identifier.
	 */
	readonly bridgeId: string;

	/**
	 * Accepted running Bridge instance generation.
	 */
	readonly generation: string;

	/**
	 * Accepted connection attempt number within that generation.
	 */
	readonly connectionGeneration: number;
}

/**
 * Payload for the `bridgeSynchronized` Hub event.
 */
export interface $HubBridgeSynchronizedEvent {
	/**
	 * Stable Bridge identifier.
	 */
	readonly bridgeId: string;

	/**
	 * Topology version the Bridge and its shards acknowledged.
	 */
	readonly topologyVersion: number;
}

/**
 * Payload for the `bridgeDisconnected` Hub event.
 */
export interface $HubBridgeDisconnectedEvent {
	/**
	 * Stable Bridge identifier.
	 */
	readonly bridgeId: string;

	/**
	 * Bridge instance generation that disconnected.
	 */
	readonly generation: string;
}

/**
 * Payload for the `shardAssigned` Hub event.
 */
export interface $HubShardAssignedEvent {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Bridge that now owns the shard.
	 */
	readonly bridgeId: string;

	/**
	 * Committed ownership version.
	 */
	readonly epoch: number;
}

/**
 * Payload for the `shardDeallocated` Hub event.
 */
export interface $HubShardDeallocatedEvent {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Bridge that previously owned the shard.
	 */
	readonly bridgeId: string;

	/**
	 * Why the shard lost its owner.
	 */
	readonly reason: "capacity" | "released";
}

/**
 * Payload for the `shardReady`, `shardStopped`, and `shardFailed` Hub events.
 */
export interface $HubShardLifecycleEvent {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Bridge hosting the shard process.
	 */
	readonly bridgeId: string;
}

/**
 * Payload for the `shardRestartScheduled` Hub event.
 */
export interface $HubShardRestartScheduledEvent {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Bridge that will restart the shard.
	 */
	readonly bridgeId: string;

	/**
	 * Restart attempt number within the current policy window.
	 */
	readonly attempt: number;

	/**
	 * Backoff delay before the restart is issued, in milliseconds.
	 */
	readonly delayMs: number;
}

/**
 * Payload for the `shardRestartsExhausted` Hub event.
 */
export interface $HubShardRestartsExhaustedEvent {
	/**
	 * Zero-based shard identifier.
	 */
	readonly shardId: number;

	/**
	 * Bridge whose restart budget was exhausted.
	 */
	readonly bridgeId: string;

	/**
	 * Restart policy window in milliseconds; the shard stays parked until the
	 * window slides.
	 */
	readonly windowMs: number;
}

/**
 * Payload for the `error` Hub event.
 */
export interface $HubErrorEvent {
	/**
	 * Background failure that was not returned by a method call.
	 */
	readonly error: Error;

	/**
	 * Human-readable description of where the failure happened.
	 */
	readonly context: string;
}

/**
 * Every Hub event name mapped to its payload shape.
 */
export interface $HubEventMap {
	/**
	 * A Bridge completed its hello handshake and holds the active connection.
	 */
	readonly bridgeConnected: $HubBridgeConnectedEvent;

	/**
	 * A Bridge socket closed; its assignments are retained.
	 */
	readonly bridgeDisconnected: $HubBridgeDisconnectedEvent;

	/**
	 * A Bridge and its retained shards acknowledged one current topology.
	 */
	readonly bridgeSynchronized: $HubBridgeSynchronizedEvent;

	/**
	 * A background failure was reported; mirrors the `onError` option.
	 */
	readonly error: $HubErrorEvent;

	/**
	 * A shard was committed to a Bridge (first assignment or transfer).
	 */
	readonly shardAssigned: $HubShardAssignedEvent;

	/**
	 * A shard lost its owner through capacity planning or operator release.
	 */
	readonly shardDeallocated: $HubShardDeallocatedEvent;

	/**
	 * A shard process reported failure; a policy-bounded restart may follow.
	 */
	readonly shardFailed: $HubShardLifecycleEvent;

	/**
	 * A shard process became Discord-ready.
	 */
	readonly shardReady: $HubShardLifecycleEvent;

	/**
	 * A restart was scheduled for a failed shard process.
	 */
	readonly shardRestartScheduled: $HubShardRestartScheduledEvent;

	/**
	 * A shard exhausted its restart budget and is parked until the policy
	 * window slides.
	 */
	readonly shardRestartsExhausted: $HubShardRestartsExhaustedEvent;

	/**
	 * A shard process stopped after a Hub command or Bridge report.
	 */
	readonly shardStopped: $HubShardLifecycleEvent;
}

/**
 * Name of one Hub event.
 */
export type $HubEventName = keyof $HubEventMap;

/**
 * Handles one emitted Hub event.
 *
 * @param payload - Frozen event payload.
 */
export type $HubEventListener<Event extends $HubEventName> = (payload: $HubEventMap[Event]) => void | Promise<void>;

/**
 * Bounded EventEmitter-style subscription surface for Hub lifecycle events.
 *
 * Listener failures are reported through the Hub `onError` channel and never
 * interrupt Hub processing.
 */
export interface $HubEvents {
	/**
	 * Registers a listener for one event.
	 *
	 * @param event - Event name to observe.
	 * @param listener - Callback receiving each frozen payload.
	 * @returns Cleanup callback that removes the listener.
	 */
	on<Event extends $HubEventName>(event: Event, listener: $HubEventListener<Event>): () => void;

	/**
	 * Registers a listener removed after its first invocation.
	 *
	 * @param event - Event name to observe.
	 * @param listener - Callback receiving the first frozen payload.
	 * @returns Cleanup callback that removes the listener early.
	 */
	once<Event extends $HubEventName>(event: Event, listener: $HubEventListener<Event>): () => void;

	/**
	 * Removes a previously registered listener.
	 *
	 * @param event - Event name the listener was registered for.
	 * @param listener - The exact listener reference to remove.
	 */
	off<Event extends $HubEventName>(event: Event, listener: $HubEventListener<Event>): void;

	/**
	 * Number of listeners currently registered for one event.
	 *
	 * @param event - Event name to count.
	 */
	listenerCount(event: $HubEventName): number;
}
