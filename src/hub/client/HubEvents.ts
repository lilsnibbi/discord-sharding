import { ShardingCapacityError, ShardingConfigurationError } from "../../errors/ShardingError";
import type { $HubEventListener, $HubEventMap, $HubEventName, $HubEvents } from "../../types/hub";
import { toError } from "./utilities";

/** Maximum listeners retained per Hub event. */
export const MAX_HUB_EVENT_LISTENERS = 256;

type AnyListener = (payload: object) => void | Promise<void>;

/**
 * Bounded, failure-isolated EventEmitter for Hub lifecycle events.
 *
 * Emission is internal to the Hub; applications receive the read-only
 * `$HubEvents` surface. Listener failures are forwarded to the `report`
 * callback and never interrupt Hub processing or other listeners.
 */
export class HubEventEmitter implements $HubEvents {
	readonly #listeners = new Map<$HubEventName, Set<AnyListener>>();
	readonly #report: (error: Error, context: string) => void;

	/**
	 * Creates an emitter that funnels listener failures to one reporter.
	 *
	 * @param report - Receives listener failures; must never throw.
	 */
	public constructor(report: (error: Error, context: string) => void) {
		this.#report = report;
	}

	/**
	 * Registers a listener for one event.
	 *
	 * @param event - Event name to observe.
	 * @param listener - Callback receiving each frozen payload.
	 * @returns Cleanup callback that removes the listener.
	 */
	public on<Event extends $HubEventName>(event: Event, listener: $HubEventListener<Event>): () => void {
		if (typeof listener !== "function") throw new ShardingConfigurationError("Event listener must be a function.");
		const listeners = this.#listenersFor(event);
		if (!listeners.has(listener as AnyListener) && listeners.size >= MAX_HUB_EVENT_LISTENERS) {
			throw new ShardingCapacityError(`Listener limit of ${MAX_HUB_EVENT_LISTENERS} for ${event} has been reached.`);
		}
		listeners.add(listener as AnyListener);
		let active = true;
		return (): void => {
			if (!active) return;
			active = false;
			listeners.delete(listener as AnyListener);
		};
	}

	/**
	 * Registers a listener removed after its first invocation.
	 *
	 * @param event - Event name to observe.
	 * @param listener - Callback receiving the first frozen payload.
	 * @returns Cleanup callback that removes the listener early.
	 */
	public once<Event extends $HubEventName>(event: Event, listener: $HubEventListener<Event>): () => void {
		if (typeof listener !== "function") throw new ShardingConfigurationError("Event listener must be a function.");
		const remove = this.on(event, (payload) => {
			remove();
			return listener(payload);
		});
		return remove;
	}

	/**
	 * Removes a previously registered listener.
	 *
	 * @param event - Event name the listener was registered for.
	 * @param listener - The exact listener reference to remove.
	 */
	public off<Event extends $HubEventName>(event: Event, listener: $HubEventListener<Event>): void {
		this.#listeners.get(event)?.delete(listener as AnyListener);
	}

	/**
	 * Number of listeners currently registered for one event.
	 *
	 * @param event - Event name to count.
	 */
	public listenerCount(event: $HubEventName): number {
		return this.#listeners.get(event)?.size ?? 0;
	}

	/**
	 * Emits one event to every registered listener.
	 *
	 * The payload is frozen before delivery. Synchronous throws and rejected
	 * promises are reported; they never propagate to the emitting code path.
	 *
	 * @param event - Event name to emit.
	 * @param payload - JSON-compatible payload delivered to listeners.
	 */
	public emit<Event extends $HubEventName>(event: Event, payload: $HubEventMap[Event]): void {
		const listeners = this.#listeners.get(event);
		if (listeners === undefined || listeners.size === 0) return;
		const frozen = Object.freeze(payload);
		for (const listener of [...listeners]) {
			try {
				const result = listener(frozen);
				if (result instanceof Promise) {
					void result.catch((cause: unknown) => this.#report(toError(cause), `${event} event listener`));
				}
			} catch (cause) {
				this.#report(toError(cause), `${event} event listener`);
			}
		}
	}

	/**
	 * Removes every listener; called during Hub shutdown.
	 */
	public removeAllListeners(): void {
		this.#listeners.clear();
	}

	#listenersFor(event: $HubEventName): Set<AnyListener> {
		const existing = this.#listeners.get(event);
		if (existing !== undefined) return existing;
		const created = new Set<AnyListener>();
		this.#listeners.set(event, created);
		return created;
	}
}
