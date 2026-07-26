import {
	ShardingCapacityError,
	ShardingConfigurationError,
	ShardingStateError,
	ShardingTimeoutError,
	ShardingTransportError,
} from "../../errors/ShardingError";
import { MAX_PENDING_REQUESTS, MAX_TIMER_MS } from "../../internal/limits";
import type {
	$BridgeShardState,
	$ShardProcess,
	$ShardProcessContext,
	$ShardProcessExit,
	$ShardProcessFactory,
} from "../../types/bridge";

export interface ManagedShardCallbacks {
	readonly onExit: (managed: ManagedShardProcess, exit: $ShardProcessExit, intentional: boolean) => void;
	readonly onMessage: (managed: ManagedShardProcess, message: unknown) => void;
	readonly onState: (managed: ManagedShardProcess, state: $BridgeShardState) => void;
}

export interface ManagedShardOptions {
	readonly args: readonly string[];
	readonly assignmentEpoch: number;
	readonly callbacks: ManagedShardCallbacks;
	readonly cwd?: string;
	readonly environment: Readonly<Record<string, string>>;
	readonly maxPendingSends: number;
	readonly processFactory: $ShardProcessFactory;
	readonly processGeneration: number;
	readonly requestTimeoutMs: number;
	readonly script: string;
	readonly shardId: number;
	readonly shutdownTimeoutMs: number;
	readonly startupTimeoutMs: number;
	readonly totalShards: number;
}

export class ManagedShardProcess {
	public readonly shardId: number;
	public readonly assignmentEpoch: number;
	public readonly processGeneration: number;

	readonly #options: ManagedShardOptions;
	#process: $ShardProcess | undefined;
	#state: $BridgeShardState = "starting";
	#startupTimer: ReturnType<typeof setTimeout> | undefined;
	#stopPromise: Promise<void> | undefined;
	#exitSettled = false;
	#exitPromise: Promise<$ShardProcessExit>;
	#resolveExit: ((exit: $ShardProcessExit) => void) | undefined;
	#pendingSends = 0;
	#intentionalStop = false;

	public constructor(options: ManagedShardOptions) {
		validateManagedShardOptions(options);
		this.#options = options;
		this.shardId = options.shardId;
		this.assignmentEpoch = options.assignmentEpoch;
		this.processGeneration = options.processGeneration;
		this.#exitPromise = new Promise((resolve) => {
			this.#resolveExit = resolve;
		});
	}

	public get state(): $BridgeShardState {
		return this.#state;
	}

	public get pid(): number | null {
		return this.#process?.pid ?? null;
	}

	public get pendingSends(): number {
		return this.#pendingSends;
	}

	public start(): void {
		if (this.#process !== undefined) throw new ShardingStateError(`Shard ${this.shardId} is already started.`);
		const context: $ShardProcessContext = Object.freeze({
			args: this.#options.args,
			assignmentEpoch: this.assignmentEpoch,
			callbacks: Object.freeze({
				onExit: (exit: $ShardProcessExit) => this.#acceptExit(exit),
				onMessage: (message: unknown) => {
					if (!this.#exitSettled) this.#options.callbacks.onMessage(this, message);
				},
			}),
			...(this.#options.cwd === undefined ? {} : { cwd: this.#options.cwd }),
			environment: this.#options.environment,
			processGeneration: this.processGeneration,
			script: this.#options.script,
			shardId: this.shardId,
			totalShards: this.#options.totalShards,
		});
		let processHandle: $ShardProcess;
		try {
			processHandle = captureProcess(this.#options.processFactory(context));
		} catch (cause) {
			this.#transition("failed");
			throw new ShardingTransportError(`Could not spawn shard ${this.shardId}.`, { cause });
		}
		this.#process = processHandle;
		if (this.#exitSettled) {
			try {
				if (!processHandle.killed) processHandle.kill();
			} catch {
				// The synchronous exit already established the terminal state.
			}
			return;
		}
		void processHandle.exited.then(
			(code) => this.#acceptExit({ code: normalizeExitCode(code), signal: null }),
			(error: unknown) => this.#acceptExit({ code: null, error: toError(error), signal: null }),
		);
		this.#startupTimer = setTimeout(() => {
			if (this.#state !== "starting") return;
			this.#transition("failed");
			void this.#terminateFailedStartup(processHandle);
		}, this.#options.startupTimeoutMs);
	}

	public markReady(): void {
		if (this.#state === "ready") return;
		if (this.#state !== "starting") {
			throw new ShardingStateError(`Shard ${this.shardId} cannot become ready from ${this.#state}.`);
		}
		if (this.#startupTimer !== undefined) clearTimeout(this.#startupTimer);
		this.#startupTimer = undefined;
		this.#transition("ready");
	}

	public async send(message: object): Promise<void> {
		const processHandle = this.#process;
		if (processHandle === undefined || this.#exitSettled) {
			throw new ShardingStateError(`Shard ${this.shardId} process is not available.`);
		}
		if (this.#pendingSends >= this.#options.maxPendingSends) {
			throw new ShardingCapacityError(`Shard ${this.shardId} IPC send capacity reached.`);
		}
		this.#pendingSends += 1;
		let sendOperation: Promise<void>;
		try {
			sendOperation = Promise.resolve(processHandle.send(message));
		} catch (cause) {
			this.#pendingSends -= 1;
			throw new ShardingTransportError(`Could not send IPC to shard ${this.shardId}.`, { cause });
		}
		void sendOperation.then(
			() => {
				this.#pendingSends -= 1;
			},
			() => {
				this.#pendingSends -= 1;
			},
		);
		await withTimeout(sendOperation, this.#options.requestTimeoutMs, `IPC send to shard ${this.shardId} timed out.`);
	}

	public stop(shutdownMessage: object | undefined): Promise<void> {
		if (this.#stopPromise !== undefined) return this.#stopPromise;
		const operation = this.#performStop(shutdownMessage);
		this.#stopPromise = operation;
		void operation.then(
			() => {
				if (this.#stopPromise === operation) this.#stopPromise = undefined;
			},
			() => {
				if (this.#stopPromise === operation) this.#stopPromise = undefined;
			},
		);
		return operation;
	}

	async #performStop(shutdownMessage: object | undefined): Promise<void> {
		if (this.#exitSettled) return;
		this.#intentionalStop = true;
		this.#transition("stopping");
		if (this.#startupTimer !== undefined) clearTimeout(this.#startupTimer);
		this.#startupTimer = undefined;
		const processHandle = this.#process;
		if (processHandle === undefined) {
			this.#transition("stopped");
			return;
		}
		if (shutdownMessage !== undefined) {
			try {
				await this.send(shutdownMessage);
			} catch {
				// Termination below remains the authoritative cleanup path.
			}
		}
		if (await settlesWithin(this.#exitPromise, this.#options.shutdownTimeoutMs)) return;
		let softFailure: unknown;
		try {
			processHandle.kill();
		} catch (cause) {
			softFailure = cause;
		}
		if (softFailure === undefined && (await settlesWithin(this.#exitPromise, this.#options.shutdownTimeoutMs))) return;
		try {
			processHandle.kill(9);
		} catch (cause) {
			throw new ShardingTransportError(`Could not force-terminate shard ${this.shardId}.`, {
				cause:
					softFailure === undefined
						? cause
						: new AggregateError([softFailure, cause], `Shard ${this.shardId} termination signals failed.`),
			});
		}
		if (!(await settlesWithin(this.#exitPromise, this.#options.shutdownTimeoutMs))) {
			throw new ShardingTimeoutError(`Shard ${this.shardId} did not exit after force termination.`);
		}
	}

	async #terminateFailedStartup(processHandle: $ShardProcess): Promise<void> {
		try {
			processHandle.kill();
		} catch {
			// Force termination below remains available when soft termination fails.
		}
		if (await settlesWithin(this.#exitPromise, this.#options.shutdownTimeoutMs)) return;
		if (this.#intentionalStop || this.#state !== "failed") return;
		try {
			processHandle.kill(9);
		} catch {
			// The retained process handle allows Bridge shutdown to retry cleanup.
			return;
		}
		await settlesWithin(this.#exitPromise, this.#options.shutdownTimeoutMs);
	}

	#acceptExit(exit: $ShardProcessExit): void {
		if (this.#exitSettled) return;
		this.#exitSettled = true;
		if (this.#startupTimer !== undefined) clearTimeout(this.#startupTimer);
		this.#startupTimer = undefined;
		this.#resolveExit?.(Object.freeze(exit));
		this.#resolveExit = undefined;
		this.#transition(this.#intentionalStop ? "stopped" : "failed");
		this.#options.callbacks.onExit(this, exit, this.#intentionalStop);
	}

	#transition(state: $BridgeShardState): void {
		if (state === this.#state) return;
		this.#state = state;
		this.#options.callbacks.onState(this, state);
	}
}

export function createBunProcessFactory(): $ShardProcessFactory {
	return (context): $ShardProcess => {
		const subprocess = Bun.spawn([Bun.which("bun") ?? "bun", context.script, ...context.args], {
			...(context.cwd === undefined ? {} : { cwd: context.cwd }),
			env: context.environment,
			ipc(message) {
				context.callbacks.onMessage(message);
			},
			onExit(_subprocess, exitCode, signalCode, error) {
				context.callbacks.onExit({
					code: exitCode,
					...(error === undefined ? {} : { error }),
					signal: signalCode,
				});
			},
			serialization: "json",
			stderr: "inherit",
			stdin: "ignore",
			stdout: "inherit",
		});
		return {
			get exited(): Promise<number> {
				return subprocess.exited;
			},
			get killed(): boolean {
				return subprocess.killed;
			},
			get pid(): number {
				return subprocess.pid;
			},
			kill(signal?: number): void {
				subprocess.kill(signal);
			},
			send(message: object): void {
				subprocess.send(message);
			},
		};
	};
}

function captureProcess(value: unknown): $ShardProcess {
	if (typeof value !== "object" || value === null) {
		throw new ShardingConfigurationError("Process factory must return an object.");
	}
	const exited = Reflect.get(value, "exited");
	const killed = Reflect.get(value, "killed");
	const pid = Reflect.get(value, "pid");
	const send = Reflect.get(value, "send");
	const kill = Reflect.get(value, "kill");
	if (!(exited instanceof Promise)) throw new ShardingConfigurationError("Shard process exited must be a Promise.");
	if (typeof killed !== "boolean") throw new ShardingConfigurationError("Shard process killed must be a boolean.");
	if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) {
		throw new ShardingConfigurationError("Shard process pid must be a positive integer.");
	}
	if (typeof send !== "function" || typeof kill !== "function") {
		throw new ShardingConfigurationError("Shard process must provide send() and kill().");
	}
	const normalizedExited: Promise<number> = exited.then((result: unknown) => {
		if (typeof result !== "number" || !Number.isSafeInteger(result)) {
			throw new ShardingTransportError("Shard process exited with an invalid exit code.");
		}
		return result;
	});
	return Object.freeze({
		exited: normalizedExited,
		get killed(): boolean {
			const current: unknown = Reflect.get(value, "killed");
			return current === true;
		},
		kill(signal?: number): void {
			Reflect.apply(kill, value, signal === undefined ? [] : [signal]);
		},
		pid,
		send(message: object): void | Promise<void> {
			const result: unknown = Reflect.apply(send, value, [message]);
			if (result === undefined) return;
			if (result instanceof Promise) {
				return result.then(() => undefined);
			}
			throw new ShardingTransportError("Shard process send() returned an unsupported value.");
		},
	});
}

async function withTimeout(operation: Promise<void>, timeoutMs: number, message: string): Promise<void> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const timeout = new Promise<never>((_resolve, reject) => {
		timer = setTimeout(() => reject(new ShardingTimeoutError(message)), timeoutMs);
	});
	try {
		await Promise.race([operation, timeout]);
	} finally {
		if (timer !== undefined) clearTimeout(timer);
	}
}

async function settlesWithin<Value>(operation: Promise<Value>, timeoutMs: number): Promise<boolean> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const timeout = new Promise<false>((resolve) => {
		timer = setTimeout(() => resolve(false), timeoutMs);
	});
	try {
		return await Promise.race([operation.then(() => true), timeout]);
	} finally {
		if (timer !== undefined) clearTimeout(timer);
	}
}

function normalizeExitCode(value: number): number | null {
	return Number.isSafeInteger(value) ? value : null;
}

function toError(value: unknown): Error {
	if (value instanceof Error) return value;
	return new Error("Process operation failed with a non-Error value.", { cause: value });
}

export function validateManagedShardOptions(options: ManagedShardOptions): void {
	if (!Number.isSafeInteger(options.maxPendingSends) || options.maxPendingSends <= 0) {
		throw new ShardingConfigurationError("maxPendingSends must be a positive integer.");
	}
	if (options.maxPendingSends > MAX_PENDING_REQUESTS) {
		throw new ShardingConfigurationError(`maxPendingSends cannot exceed ${MAX_PENDING_REQUESTS}.`);
	}
	for (const [name, value] of [
		["requestTimeoutMs", options.requestTimeoutMs],
		["shutdownTimeoutMs", options.shutdownTimeoutMs],
		["startupTimeoutMs", options.startupTimeoutMs],
	] as const) {
		if (!Number.isSafeInteger(value) || value <= 0 || value > MAX_TIMER_MS) {
			throw new ShardingConfigurationError(`${name} must be between 1 and ${MAX_TIMER_MS}.`);
		}
	}
}
