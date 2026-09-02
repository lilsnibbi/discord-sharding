import {
	APPEND_ANALYTICS_SOURCE,
	CLEAR_ANALYTICS_SOURCE,
	type RedisCommandClient,
	SAVE_ASSIGNMENT_SOURCE,
	SAVE_BRIDGE_SOURCE,
	SAVE_SHARD_SOURCE,
} from "../../src/hub/redis/scripts";

type ScriptHandler = (keys: readonly string[], args: readonly string[]) => number;

interface FakeRedisState {
	readonly hashes: Map<string, Map<string, string>>;
	readonly scripts: Map<string, string>;
	readonly strings: Map<string, string>;
	readonly zsets: Map<string, Map<string, number>>;
}

/**
 * In-memory stand-in for Bun's `RedisClient`.
 *
 * Implements only the commands Hub persistence issues. The five Lua scripts are
 * reproduced in TypeScript with the same comparison rules, so unit tests stay
 * deterministic and offline. The Lua itself is exercised by the live Redis
 * integration test.
 */
export class FakeRedisClient implements RedisCommandClient {
	public readonly commands: string[] = [];
	public closed = false;
	public failNextEvalSha = false;

	readonly #state: FakeRedisState;
	readonly #handlers = new Map<string, ScriptHandler>();

	public constructor(state?: FakeRedisState) {
		this.#state = state ?? {
			hashes: new Map<string, Map<string, string>>(),
			scripts: new Map<string, string>(),
			strings: new Map<string, string>(),
			zsets: new Map<string, Map<string, number>>(),
		};
		this.#handlers.set(SAVE_ASSIGNMENT_SOURCE, (keys, args) => this.#saveVersioned(keys, args, "epoch"));
		this.#handlers.set(SAVE_SHARD_SOURCE, (keys, args) => this.#saveVersioned(keys, args, "assignmentEpoch"));
		this.#handlers.set(SAVE_BRIDGE_SOURCE, (keys, args) => this.#saveBridge(keys, args));
		this.#handlers.set(APPEND_ANALYTICS_SOURCE, (keys, args) => this.#appendAnalytics(keys, args));
		this.#handlers.set(CLEAR_ANALYTICS_SOURCE, (keys, args) => this.#clearAnalytics(keys, args));
	}

	/** Hash keyspace shared by every connection to this fake server. */
	public get hashes(): Map<string, Map<string, string>> {
		return this.#state.hashes;
	}

	/** String keyspace shared by every connection to this fake server. */
	public get strings(): Map<string, string> {
		return this.#state.strings;
	}

	/** Sorted-set keyspace shared by every connection to this fake server. */
	public get zsets(): Map<string, Map<string, number>> {
		return this.#state.zsets;
	}

	/** Opens a second connection backed by the same fake server state. */
	public connection(): FakeRedisClient {
		return new FakeRedisClient(this.#state);
	}

	/** Drops every cached script so the next `EVALSHA` reports `NOSCRIPT`. */
	public flushScripts(): void {
		this.#state.scripts.clear();
	}

	/** Runs one supported Redis command. */
	public send(command: string, args: string[]): Promise<unknown> {
		this.commands.push(command);
		if (this.closed) return Promise.reject(new Error("ERR_REDIS_CONNECTION_CLOSED"));
		switch (command) {
			case "PING":
				return Promise.resolve("PONG");
			case "SET":
				return Promise.resolve(this.#set(args));
			case "SCRIPT":
				return Promise.resolve(this.#scriptLoad(args));
			case "EVALSHA":
				return this.#evalSha(args);
			case "EVAL":
				return Promise.resolve(this.#eval(args));
			case "HGETALL":
				return Promise.resolve(this.#hgetall(args));
			default:
				return Promise.reject(new Error(`Unsupported command ${command}.`));
		}
	}

	/** Marks the connection closed. */
	public close(): void {
		this.closed = true;
	}

	#set(args: readonly string[]): string | null {
		const [key, value, ...flags] = args;
		if (key === undefined || value === undefined) throw new Error("SET requires a key and value.");
		const previous = this.strings.get(key) ?? null;
		if (!flags.includes("NX") || previous === null) this.strings.set(key, value);
		return flags.includes("GET") ? previous : "OK";
	}

	#scriptLoad(args: readonly string[]): string {
		const [subcommand, source] = args;
		if (subcommand !== "LOAD" || source === undefined) throw new Error("Only SCRIPT LOAD is supported.");
		const sha = new Bun.CryptoHasher("sha1").update(source).digest("hex");
		this.#state.scripts.set(sha, source);
		return sha;
	}

	#evalSha(args: readonly string[]): Promise<number> {
		const [sha, ...rest] = args;
		if (sha === undefined) throw new Error("EVALSHA requires a digest.");
		if (this.failNextEvalSha) {
			this.failNextEvalSha = false;
			return Promise.reject(new Error("NOSCRIPT No matching script."));
		}
		const source = this.#state.scripts.get(sha);
		if (source === undefined) return Promise.reject(new Error("NOSCRIPT No matching script."));
		return Promise.resolve(this.#run(source, rest));
	}

	#eval(args: readonly string[]): number {
		const [source, ...rest] = args;
		if (source === undefined) throw new Error("EVAL requires a script.");
		return this.#run(source, rest);
	}

	#run(source: string, parameters: readonly string[]): number {
		const handler = this.#handlers.get(source);
		if (handler === undefined) throw new Error("Unknown script source.");
		const [count, ...rest] = parameters;
		const keyCount = Number(count);
		if (!Number.isSafeInteger(keyCount) || keyCount < 0 || keyCount > rest.length) {
			throw new Error("Invalid script key count.");
		}
		return handler(rest.slice(0, keyCount), rest.slice(keyCount));
	}

	#hgetall(args: readonly string[]): Record<string, string> {
		const [key] = args;
		if (key === undefined) throw new Error("HGETALL requires a key.");
		return Object.fromEntries(this.hashes.get(key) ?? new Map<string, string>());
	}

	#hash(key: string): Map<string, string> {
		let hash = this.hashes.get(key);
		if (hash === undefined) {
			hash = new Map<string, string>();
			this.hashes.set(key, hash);
		}
		return hash;
	}

	#zset(key: string): Map<string, number> {
		let zset = this.zsets.get(key);
		if (zset === undefined) {
			zset = new Map<string, number>();
			this.zsets.set(key, zset);
		}
		return zset;
	}

	#saveVersioned(keys: readonly string[], args: readonly string[], epochField: string): number {
		const [hashKey] = keys;
		const [field, json, epochText, updatedAtText] = args;
		if (hashKey === undefined || field === undefined || json === undefined)
			throw new Error("Invalid save script call.");
		const hash = this.#hash(hashKey);
		const existing = hash.get(field);
		if (existing !== undefined) {
			const current = JSON.parse(existing) as Record<string, number>;
			const epoch = Number(epochText);
			const updatedAt = Number(updatedAtText);
			const currentEpoch = current[epochField] ?? 0;
			if (epoch < currentEpoch) return 0;
			if (epoch === currentEpoch && updatedAt <= (current.updatedAt ?? 0)) return 0;
		}
		hash.set(field, json);
		return 1;
	}

	#saveBridge(keys: readonly string[], args: readonly string[]): number {
		const [hashKey] = keys;
		const [field, json, updatedAtText] = args;
		if (hashKey === undefined || field === undefined || json === undefined)
			throw new Error("Invalid Bridge script call.");
		const hash = this.#hash(hashKey);
		const existing = hash.get(field);
		if (existing !== undefined) {
			const current = JSON.parse(existing) as { updatedAt?: number };
			if (Number(updatedAtText) <= (current.updatedAt ?? 0)) return 0;
		}
		hash.set(field, json);
		return 1;
	}

	#appendAnalytics(keys: readonly string[], args: readonly string[]): number {
		const [indexKey, recordsKey] = keys;
		const [id, score, json] = args;
		if (indexKey === undefined || recordsKey === undefined) throw new Error("Invalid analytics script call.");
		if (id === undefined || score === undefined || json === undefined) throw new Error("Invalid analytics arguments.");
		const records = this.#hash(recordsKey);
		if (records.has(id)) return 0;
		this.#zset(indexKey).set(id, Number(score));
		records.set(id, json);
		return 1;
	}

	#clearAnalytics(keys: readonly string[], args: readonly string[]): number {
		const [indexKey, recordsKey] = keys;
		const [cutoffText, limitText] = args;
		if (indexKey === undefined || recordsKey === undefined) throw new Error("Invalid clear script call.");
		const cutoff = Number(cutoffText);
		const limit = Number(limitText);
		const zset = this.#zset(indexKey);
		const records = this.#hash(recordsKey);
		const expired = [...zset.entries()]
			.filter(([, score]) => score <= cutoff)
			.sort((left, right) => left[1] - right[1] || left[0].localeCompare(right[0]))
			.slice(0, limit);
		for (const [member] of expired) {
			zset.delete(member);
			records.delete(member);
		}
		return expired.length;
	}
}
