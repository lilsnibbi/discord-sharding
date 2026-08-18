import type { $DiscordClient } from "../../src/types/discord";

const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";
const OP_DISPATCH = 0;
const OP_HEARTBEAT = 1;
const OP_IDENTIFY = 2;
const OP_RECONNECT = 7;
const OP_INVALID_SESSION = 9;
const OP_HELLO = 10;
const OP_HEARTBEAT_ACK = 11;
const FATAL_CLOSE_CODES = new Set([4004, 4010, 4011, 4013, 4014]);

interface GatewayPayload {
	readonly op: number;
	readonly d?: unknown;
	readonly s?: number | null;
	readonly t?: string | null;
}

/**
 * Minimal live Discord gateway client for verification tests only.
 *
 * Connects one real gateway shard with zero intents, no REST calls, and no
 * command registration. Satisfies the structural `$DiscordClient` contract so
 * it can stand in for discord.js inside a ShardClient.
 */
export class LiveGatewayClient implements $DiscordClient {
	readonly #shardId: number;
	readonly #totalShards: number;
	#socket: WebSocket | undefined;
	#heartbeatTimer: ReturnType<typeof setInterval> | undefined;
	#sequence: number | null = null;
	#ready = false;
	#destroyed = false;
	#lastPing = Number.NaN;
	#lastHeartbeatSentAt = 0;

	public constructor(options: { readonly shardId: number; readonly totalShards: number }) {
		this.#shardId = options.shardId;
		this.#totalShards = options.totalShards;
	}

	public get ws(): { readonly ping?: number } {
		return Number.isFinite(this.#lastPing) ? { ping: this.#lastPing } : {};
	}

	public isReady(): boolean {
		return this.#ready && this.#socket?.readyState === WebSocket.OPEN;
	}

	public destroy(): void {
		this.#destroyed = true;
		this.#ready = false;
		this.#stopHeartbeat();
		try {
			this.#socket?.close(1000);
		} catch {
			// Socket may already be closed.
		}
		this.#socket = undefined;
	}

	public async login(token?: string): Promise<string> {
		const resolved = token ?? process.env.TOKEN;
		if (resolved === undefined || resolved.length === 0) {
			throw new Error("LiveGatewayClient requires a bot token via login(token) or process.env.TOKEN.");
		}
		if (this.#destroyed) throw new Error("LiveGatewayClient was destroyed.");
		await this.#connect(resolved);
		return resolved;
	}

	async #connect(token: string): Promise<void> {
		const socket = new WebSocket(GATEWAY_URL);
		this.#socket = socket;
		await new Promise<void>((resolve, reject) => {
			const timeout = setTimeout(() => {
				reject(new Error(`Shard ${this.#shardId} timed out waiting for READY.`));
				socket.close(1000);
			}, 60_000);
			socket.addEventListener("message", (event) => {
				let payload: GatewayPayload;
				try {
					payload = JSON.parse(String(event.data)) as GatewayPayload;
				} catch {
					return;
				}
				if (typeof payload.s === "number") this.#sequence = payload.s;
				if (payload.op === OP_HELLO) {
					const data = payload.d as { readonly heartbeat_interval: number };
					this.#startHeartbeat(data.heartbeat_interval);
					this.#send({
						op: OP_IDENTIFY,
						d: {
							intents: 0,
							properties: { browser: "bun", device: "bun", os: process.platform },
							shard: [this.#shardId, this.#totalShards],
							token,
						},
					});
					return;
				}
				if (payload.op === OP_HEARTBEAT) {
					this.#send({ d: this.#sequence, op: OP_HEARTBEAT });
					return;
				}
				if (payload.op === OP_HEARTBEAT_ACK) {
					this.#lastPing = Date.now() - this.#lastHeartbeatSentAt;
					return;
				}
				if (payload.op === OP_DISPATCH && payload.t === "READY") {
					this.#ready = true;
					clearTimeout(timeout);
					resolve();
					return;
				}
				if (payload.op === OP_RECONNECT || payload.op === OP_INVALID_SESSION) {
					this.#ready = false;
					socket.close(4000);
				}
			});
			socket.addEventListener("close", (event) => {
				this.#ready = false;
				this.#stopHeartbeat();
				clearTimeout(timeout);
				if (FATAL_CLOSE_CODES.has(event.code)) {
					reject(new Error(`Gateway rejected shard ${this.#shardId} with close code ${event.code}.`));
					return;
				}
				reject(new Error(`Gateway closed shard ${this.#shardId} before READY (code ${event.code}).`));
			});
			socket.addEventListener("error", () => {
				// The close listener reports the terminal failure.
			});
		});
	}

	#send(payload: GatewayPayload): void {
		if (this.#socket?.readyState !== WebSocket.OPEN) return;
		this.#socket.send(JSON.stringify(payload));
	}

	#startHeartbeat(intervalMs: number): void {
		this.#stopHeartbeat();
		this.#heartbeatTimer = setInterval(() => {
			this.#lastHeartbeatSentAt = Date.now();
			this.#send({ d: this.#sequence, op: OP_HEARTBEAT });
		}, intervalMs);
	}

	#stopHeartbeat(): void {
		if (this.#heartbeatTimer !== undefined) clearInterval(this.#heartbeatTimer);
		this.#heartbeatTimer = undefined;
	}
}
