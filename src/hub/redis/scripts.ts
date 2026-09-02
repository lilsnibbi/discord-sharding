import { ShardingPersistenceError } from "../../errors/ShardingError";

/**
 * Minimal Redis command surface used by Hub persistence.
 *
 * Bun's native `RedisClient` satisfies this shape. Deterministic tests supply
 * their own implementation instead of reaching a live server.
 */
export interface RedisCommandClient {
	/**
	 * Runs one Redis command with string arguments.
	 *
	 * @param command - Redis command name.
	 * @param args - Positional string arguments.
	 * @returns Decoded Redis reply.
	 */
	send(command: string, args: string[]): Promise<unknown>;

	/**
	 * Releases the underlying connection.
	 */
	close(): void;
}

const NOSCRIPT_PATTERN = /NOSCRIPT/iu;
const UNLOADED_SCRIPT = "Script is not loaded.";

/**
 * Writes an assignment only when it is newer than the stored record.
 *
 * Mirrors the previous SQLite guard: a higher epoch always wins, an equal epoch
 * wins only with a strictly newer timestamp, and a lower epoch is discarded.
 */
export const SAVE_ASSIGNMENT_SOURCE = `
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
	local current = cjson.decode(existing)
	local epoch = tonumber(ARGV[3])
	local updatedAt = tonumber(ARGV[4])
	if epoch < current.epoch then return 0 end
	if epoch == current.epoch and updatedAt <= current.updatedAt then return 0 end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
`;

/**
 * Writes a Bridge record only when its timestamp is strictly newer.
 */
export const SAVE_BRIDGE_SOURCE = `
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
	local current = cjson.decode(existing)
	if tonumber(ARGV[3]) <= current.updatedAt then return 0 end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
`;

/**
 * Writes a shard record only when its epoch and timestamp are not stale.
 */
export const SAVE_SHARD_SOURCE = `
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
	local current = cjson.decode(existing)
	local epoch = tonumber(ARGV[3])
	local updatedAt = tonumber(ARGV[4])
	if epoch < current.assignmentEpoch then return 0 end
	if epoch == current.assignmentEpoch and updatedAt <= current.updatedAt then return 0 end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
`;

/**
 * Appends one analytics record to the score index and the record hash.
 *
 * Returns `0` when the identifier already exists, matching the previous
 * primary-key rejection.
 */
export const APPEND_ANALYTICS_SOURCE = `
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then return 0 end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
return 1
`;

/**
 * Deletes the oldest analytics records at or before a cutoff.
 *
 * Deletion is chunked because Lua `unpack` has a bounded argument count.
 */
export const CLEAR_ANALYTICS_SOURCE = `
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local total = #ids
if total == 0 then return 0 end
local index = 1
while index <= total do
	local chunk = {}
	local limit = math.min(index + 499, total)
	for position = index, limit do chunk[#chunk + 1] = ids[position] end
	redis.call('ZREM', KEYS[1], unpack(chunk))
	redis.call('HDEL', KEYS[2], unpack(chunk))
	index = limit + 1
end
return total
`;

/**
 * One server-side Lua script cached by its SHA-1 digest.
 *
 * Scripts are loaded once during migration and executed with `EVALSHA`. A
 * server restart or `SCRIPT FLUSH` clears the cache, so a `NOSCRIPT` reply
 * transparently falls back to `EVAL` and re-caches the digest.
 */
export class RedisScript {
	readonly #source: string;
	#sha: string | undefined;

	/**
	 * Wraps one Lua source body.
	 *
	 * @param source - Lua script executed on the Redis server.
	 */
	public constructor(source: string) {
		this.#source = source;
	}

	/**
	 * Caches the script on the server and remembers its digest.
	 *
	 * @param client - Connected Redis command client.
	 */
	public async load(client: RedisCommandClient): Promise<void> {
		const sha = await client.send("SCRIPT", ["LOAD", this.#source]);
		if (typeof sha !== "string" || sha.length === 0) {
			throw new ShardingPersistenceError("Redis did not return a script digest.");
		}
		this.#sha = sha;
	}

	/**
	 * Executes the script and returns its integer reply.
	 *
	 * @param client - Connected Redis command client.
	 * @param keys - Redis keys read or written by the script.
	 * @param args - Positional script arguments.
	 * @returns Integer reply produced by the script.
	 */
	public async run(client: RedisCommandClient, keys: readonly string[], args: readonly string[]): Promise<number> {
		const sha = this.#sha;
		if (sha === undefined) throw new ShardingPersistenceError(UNLOADED_SCRIPT);
		const parameters = [String(keys.length), ...keys, ...args];
		let reply: unknown;
		try {
			reply = await client.send("EVALSHA", [sha, ...parameters]);
		} catch (cause) {
			if (!isMissingScript(cause)) throw cause;
			reply = await client.send("EVAL", [this.#source, ...parameters]);
			// The write already succeeded. A failed re-cache only costs the next
			// call another NOSCRIPT fallback, so it must not fail this one.
			try {
				await this.load(client);
			} catch {
				this.#sha = sha;
			}
		}
		return requireIntegerReply(reply);
	}
}

function isMissingScript(cause: unknown): boolean {
	return cause instanceof Error && NOSCRIPT_PATTERN.test(cause.message);
}

function requireIntegerReply(reply: unknown): number {
	if (typeof reply === "bigint" && reply >= 0n && reply <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(reply);
	if (typeof reply !== "number" || !Number.isSafeInteger(reply)) {
		throw new ShardingPersistenceError("Redis script did not return an integer reply.");
	}
	return reply;
}
