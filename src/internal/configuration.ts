import { ShardingConfigurationError } from "../errors/ShardingError";
import { MAX_ARGUMENT_LENGTH, MAX_ENVIRONMENT_ENTRIES } from "./limits";

/**
 * Rejects unknown keys from an already snapshotted configuration record.
 *
 * @param value - Getter-free configuration snapshot.
 * @param allowed - Exact accepted property names.
 * @param name - Human-readable configuration name.
 */
export function assertConfigurationKeys(
	value: Readonly<Record<string, unknown>>,
	allowed: ReadonlySet<string>,
	name: string,
): void {
	for (const key of Object.keys(value)) {
		if (!allowed.has(key)) {
			throw new ShardingConfigurationError(`${name} contains unknown option "${key}".`);
		}
	}
}

/**
 * Copies a plain configuration object from getter-free own data properties.
 *
 * @param value - Untrusted configuration value.
 * @param name - Human-readable field name.
 * @param maxEntries - Maximum accepted own-property count.
 * @returns A frozen null-prototype snapshot.
 */
export function snapshotConfigurationRecord(
	value: unknown,
	name: string,
	maxEntries = MAX_ENVIRONMENT_ENTRIES,
): Readonly<Record<string, unknown>> {
	if (typeof value !== "object" || value === null) {
		throw new ShardingConfigurationError(`${name} must be an object.`);
	}
	let isArray: boolean;
	let keys: readonly PropertyKey[];
	let prototype: object | null;
	try {
		isArray = Array.isArray(value);
		prototype = Reflect.getPrototypeOf(value);
		keys = Reflect.ownKeys(value);
	} catch (cause) {
		throw new ShardingConfigurationError(`${name} could not be inspected safely.`, { cause });
	}
	if (isArray || (prototype !== Object.prototype && prototype !== null)) {
		throw new ShardingConfigurationError(`${name} must be a plain object.`);
	}
	if (keys.length > maxEntries) {
		throw new ShardingConfigurationError(`${name} may contain at most ${maxEntries} properties.`);
	}

	const snapshot: Record<string, unknown> = Object.create(null);
	for (const key of keys) {
		if (typeof key !== "string") {
			throw new ShardingConfigurationError(`${name} cannot contain symbol properties.`);
		}
		if (key.length === 0 || key.length > MAX_ARGUMENT_LENGTH) {
			throw new ShardingConfigurationError(
				`${name} property names must contain between 1 and ${MAX_ARGUMENT_LENGTH} characters.`,
			);
		}
		let descriptor: PropertyDescriptor | undefined;
		try {
			descriptor = Reflect.getOwnPropertyDescriptor(value, key);
		} catch (cause) {
			throw new ShardingConfigurationError(`${name}.${key} could not be inspected safely.`, { cause });
		}
		if (descriptor === undefined) {
			throw new ShardingConfigurationError(`${name} changed while it was being inspected.`);
		}
		if (descriptor.enumerable !== true || !("value" in descriptor)) {
			throw new ShardingConfigurationError(`${name}.${key} must be an enumerable getter-free data property.`);
		}
		snapshot[key] = descriptor.value;
	}
	return Object.freeze(snapshot);
}

/**
 * Copies a dense configuration array from getter-free indexed properties.
 *
 * @param value - Untrusted configuration value.
 * @param name - Human-readable field name.
 * @param maxLength - Maximum accepted array length.
 * @returns A frozen dense array snapshot.
 */
export function snapshotConfigurationArray(value: unknown, name: string, maxLength: number): readonly unknown[] {
	let isArray: boolean;
	let keys: readonly PropertyKey[];
	try {
		isArray = Array.isArray(value);
		keys = isArray ? Reflect.ownKeys(value as object) : [];
	} catch (cause) {
		throw new ShardingConfigurationError(`${name} could not be inspected safely.`, { cause });
	}
	if (!isArray) throw new ShardingConfigurationError(`${name} must be an array.`);
	if (keys.length > maxLength + 1) {
		throw new ShardingConfigurationError(`${name} may contain at most ${maxLength} values.`);
	}

	const descriptors = new Map<string, PropertyDescriptor>();
	for (const key of keys) {
		if (typeof key !== "string") {
			throw new ShardingConfigurationError(`${name} cannot contain symbol properties.`);
		}
		if (key !== "length" && !isArrayIndex(key)) {
			throw new ShardingConfigurationError(`${name} cannot contain named properties.`);
		}
		let descriptor: PropertyDescriptor | undefined;
		try {
			descriptor = Reflect.getOwnPropertyDescriptor(value as object, key);
		} catch (cause) {
			throw new ShardingConfigurationError(`${name}[${key}] could not be inspected safely.`, { cause });
		}
		if (descriptor === undefined) {
			throw new ShardingConfigurationError(`${name} changed while it was being inspected.`);
		}
		descriptors.set(key, descriptor);
	}

	const lengthDescriptor = descriptors.get("length");
	const length: unknown = lengthDescriptor?.value;
	if (typeof length !== "number" || !Number.isSafeInteger(length) || length < 0 || length > maxLength) {
		throw new ShardingConfigurationError(`${name} may contain at most ${maxLength} values.`);
	}
	for (const key of descriptors.keys()) {
		if (key !== "length" && Number(key) >= length) {
			throw new ShardingConfigurationError(`${name} cannot contain named properties.`);
		}
	}
	const snapshot: unknown[] = [];
	for (let index = 0; index < length; index += 1) {
		const descriptor = descriptors.get(String(index));
		if (descriptor === undefined || descriptor.enumerable !== true || !("value" in descriptor)) {
			throw new ShardingConfigurationError(`${name} must be dense and getter-free.`);
		}
		snapshot.push(descriptor.value);
	}
	return Object.freeze(snapshot);
}

function isArrayIndex(value: string): boolean {
	if (value.length > 10 || !/^(?:0|[1-9]\d*)$/u.test(value)) return false;
	const index = Number(value);
	return Number.isSafeInteger(index) && index >= 0;
}
