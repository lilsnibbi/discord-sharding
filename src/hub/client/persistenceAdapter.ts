import { ShardingConfigurationError } from "../../errors/ShardingError";
import type { $AnalyticsRecord, $PersistedAssignment, $PersistedBridge, $PersistedShard } from "../../types/hub";
import type { HubPersistenceAdapter } from "./types";

type PersistenceMethod = (...arguments_: unknown[]) => unknown;

export function normalizeHubPersistence(value: unknown): HubPersistenceAdapter | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "object" || value === null) {
		throw new ShardingConfigurationError("persistence must implement the complete Hub persistence contract.");
	}
	const appendAnalytics = capturePersistenceMethod(value, "appendAnalytics");
	const clearAnalyticsBatch = capturePersistenceMethod(value, "clearAnalyticsBatch");
	const close = capturePersistenceMethod(value, "close");
	const loadState = capturePersistenceMethod(value, "loadState");
	const migrate = capturePersistenceMethod(value, "migrate");
	const saveAssignment = capturePersistenceMethod(value, "saveAssignment");
	const saveBridge = capturePersistenceMethod(value, "saveBridge");
	const saveShard = capturePersistenceMethod(value, "saveShard");
	return Object.freeze({
		appendAnalytics: (record: $AnalyticsRecord) =>
			invokeVoidPersistenceMethod(value, appendAnalytics, [record], "persistence.appendAnalytics"),
		clearAnalyticsBatch: async (before: number, batchSize: number) => {
			const result = await invokePersistenceMethod(
				value,
				clearAnalyticsBatch,
				[before, batchSize],
				"persistence.clearAnalyticsBatch",
			);
			if (typeof result !== "number" || !Number.isSafeInteger(result) || result < 0 || result > batchSize) {
				throw new ShardingConfigurationError(
					"persistence.clearAnalyticsBatch must resolve to a valid deleted-row count.",
				);
			}
			return result;
		},
		close: () => invokeVoidPersistenceMethod(value, close, [], "persistence.close"),
		loadState: () => invokePersistenceMethod(value, loadState, [], "persistence.loadState"),
		migrate: () => invokeVoidPersistenceMethod(value, migrate, [], "persistence.migrate"),
		saveAssignment: (assignment: $PersistedAssignment) =>
			invokeVoidPersistenceMethod(value, saveAssignment, [assignment], "persistence.saveAssignment"),
		saveBridge: (bridge: $PersistedBridge) =>
			invokeVoidPersistenceMethod(value, saveBridge, [bridge], "persistence.saveBridge"),
		saveShard: (shard: $PersistedShard) =>
			invokeVoidPersistenceMethod(value, saveShard, [shard], "persistence.saveShard"),
	});
}

function capturePersistenceMethod(receiver: object, name: string): PersistenceMethod {
	let current: object | null = receiver;
	while (current !== null) {
		let descriptor: PropertyDescriptor | undefined;
		try {
			descriptor = Reflect.getOwnPropertyDescriptor(current, name);
		} catch (cause) {
			throw new ShardingConfigurationError(`persistence.${name} could not be inspected safely.`, { cause });
		}
		if (descriptor !== undefined) {
			if (!("value" in descriptor) || !isPersistenceMethod(descriptor.value)) {
				throw new ShardingConfigurationError(`persistence.${name} must be a getter-free function.`);
			}
			return descriptor.value;
		}
		try {
			current = Reflect.getPrototypeOf(current);
		} catch (cause) {
			throw new ShardingConfigurationError(`persistence.${name} prototype could not be inspected safely.`, {
				cause,
			});
		}
	}
	throw new ShardingConfigurationError(`persistence.${name} must be a function.`);
}

async function invokePersistenceMethod(
	receiver: object,
	method: PersistenceMethod,
	arguments_: readonly unknown[],
	name: string,
): Promise<unknown> {
	let result: unknown;
	try {
		result = Reflect.apply(method, receiver, arguments_);
	} catch (cause) {
		throw new ShardingConfigurationError(`${name} threw before returning a Promise.`, { cause });
	}
	if (!(result instanceof Promise)) {
		throw new ShardingConfigurationError(`${name} must return a Promise.`);
	}
	return result;
}

async function invokeVoidPersistenceMethod(
	receiver: object,
	method: PersistenceMethod,
	arguments_: readonly unknown[],
	name: string,
): Promise<void> {
	const result = await invokePersistenceMethod(receiver, method, arguments_, name);
	if (result !== undefined) throw new ShardingConfigurationError(`${name} must resolve without a value.`);
}

function isPersistenceMethod(value: unknown): value is PersistenceMethod {
	return typeof value === "function";
}
