import {
	ShardingProtocolError,
	ShardingStateError,
	ShardingTimeoutError,
	ShardingTransportError,
} from "../../errors/ShardingError";
import { reconnectDelay } from "../../internal/policies";
import { createRequestId } from "../../internal/validation";
import { parseWireMessage } from "../../protocol/codec";
import { HUB_TO_BRIDGE_TYPES } from "../../protocol/types";
import { BridgeTopology } from "./BridgeTopology";
import { closeSocketForHubError, toError } from "./protocol";

const HEARTBEAT_INTERVAL_MS = 10_000;

export abstract class BridgeConnection extends BridgeTopology {
	protected async runConnectionLoop(): Promise<void> {
		let attempt = 0;
		while (this.lifecycleState === "running" && !this.lifecycle.signal.aborted) {
			try {
				if (await this.connectOnce()) attempt = 0;
			} catch (cause) {
				if (this.lifecycle.signal.aborted) break;
				this.report(toError(cause), "Hub connection");
			}
			if (this.lifecycleState !== "running" || this.lifecycle.signal.aborted) break;
			const delay = reconnectDelay(this.options.reconnect, attempt, this.options.random);
			attempt += 1;
			try {
				await this.options.sleep(delay, this.lifecycle.signal);
			} catch {
				if (!this.lifecycle.signal.aborted) throw new ShardingStateError("Reconnect sleep failed.");
			}
		}
	}

	protected async connectOnce(): Promise<boolean> {
		this.connectionGeneration += 1;
		const connectionGeneration = this.connectionGeneration;
		const headers = Object.freeze({
			Authorization: `Bearer ${this.options.token}`,
			"X-Sharding-Bridge-Generation": this.generation,
			"X-Sharding-Bridge-Id": this.id,
			"X-Sharding-Connection-Generation": String(connectionGeneration),
		});
		let socket: WebSocket;
		try {
			socket = this.options.socketFactory(this.options.hubUrl, headers);
		} catch (cause) {
			throw new ShardingTransportError("Could not create Hub WebSocket.", { cause });
		}
		return new Promise<boolean>((resolve, reject) => {
			let opened = false;
			let finishing = false;
			let settled = false;
			let protocolFailed = false;
			let inboundQueue = Promise.resolve();
			let openTimer: ReturnType<typeof setTimeout> | undefined = setTimeout(() => {
				const error = new ShardingTimeoutError(
					`Hub WebSocket did not open within ${this.options.request.timeoutMs}ms.`,
				);
				finish(error);
				try {
					socket.close(1008, "Connection deadline exceeded");
				} catch {
					// The connection promise already owns the timeout failure.
				}
			}, this.options.request.timeoutMs);
			const settle = (error?: Error): void => {
				if (settled) return;
				settled = true;
				const synchronized = this.synchronizedConnectionGeneration === connectionGeneration;
				if (error !== undefined && !synchronized) reject(error);
				else resolve(synchronized);
			};
			const finish = (error?: Error): void => {
				if (finishing) return;
				finishing = true;
				cleanup();
				if (this.socket === socket) this.socket = undefined;
				const disconnectCleanup = this.setDisconnected(error ?? new ShardingTransportError("Hub WebSocket closed."));
				const pendingInbound = inboundQueue;
				void Promise.all([pendingInbound, disconnectCleanup]).then(
					() => settle(error),
					(cause: unknown) => settle(error ?? toError(cause)),
				);
			};
			const onOpen = (): void => {
				if (this.lifecycle.signal.aborted) {
					socket.close(1000, "Bridge stopped");
					return;
				}
				opened = true;
				if (openTimer !== undefined) clearTimeout(openTimer);
				openTimer = undefined;
				this.socket = socket;
				this.connectionTopologyVersion = 0;
				this.heartbeatTimer = setInterval(() => {
					void this.sendHeartbeat(connectionGeneration).catch((cause: unknown) => {
						this.report(toError(cause), "Hub heartbeat");
						socket.close(1011, "Heartbeat failed");
					});
				}, HEARTBEAT_INTERVAL_MS);
				void this.sendHello(connectionGeneration).catch((cause: unknown) => {
					this.report(toError(cause), "Bridge hello");
					socket.close(1002, "Invalid hello");
				});
			};
			const onMessage = (event: MessageEvent): void => {
				if (this.socket !== socket || connectionGeneration !== this.connectionGeneration) return;
				if (this.inboundMessages >= this.options.request.maxPending) {
					socket.close(1013, "Inbound capacity reached");
					return;
				}
				this.inboundMessages += 1;
				inboundQueue = inboundQueue
					.then(async () => {
						if (
							protocolFailed ||
							finishing ||
							this.socket !== socket ||
							connectionGeneration !== this.connectionGeneration
						) {
							return;
						}
						try {
							await this.handleHubMessage(event.data, socket, connectionGeneration);
						} catch (cause) {
							protocolFailed = true;
							const error = toError(cause);
							this.report(error, "Hub protocol");
							closeSocketForHubError(socket, error);
						}
					})
					.finally(() => {
						this.inboundMessages -= 1;
					});
			};
			const onClose = (event: CloseEvent): void => {
				const suffix = event.reason.length > 0 ? `: ${event.reason}` : "";
				finish(new ShardingTransportError(`Hub WebSocket closed with ${event.code}${suffix}.`));
			};
			const onError = (): void => {
				const error = new ShardingTransportError("Hub WebSocket reported a transport error.");
				if (!opened) finish(error);
			};
			const onAbort = (): void => {
				try {
					socket.close(1000, "Bridge stopped");
				} finally {
					finish();
				}
			};
			const cleanup = (): void => {
				socket.removeEventListener("open", onOpen);
				socket.removeEventListener("message", onMessage);
				socket.removeEventListener("close", onClose);
				socket.removeEventListener("error", onError);
				this.lifecycle.signal.removeEventListener("abort", onAbort);
				if (openTimer !== undefined) clearTimeout(openTimer);
				openTimer = undefined;
				if (this.heartbeatTimer !== undefined) clearInterval(this.heartbeatTimer);
				this.heartbeatTimer = undefined;
			};
			socket.addEventListener("open", onOpen);
			socket.addEventListener("message", onMessage);
			socket.addEventListener("close", onClose);
			socket.addEventListener("error", onError);
			this.lifecycle.signal.addEventListener("abort", onAbort, { once: true });
			if (this.lifecycle.signal.aborted) onAbort();
		});
	}

	protected async sendHello(connectionGeneration: number): Promise<void> {
		const runningShards = [...this.processes.values()]
			.sort((left, right) => left.shardId - right.shardId)
			.map((managed) =>
				Object.freeze({
					assignmentEpoch: managed.assignmentEpoch,
					processGeneration: managed.processGeneration,
					ready: managed.state === "ready",
					shardId: managed.shardId,
				}),
			);
		await this.sendHub("bridge.hello", createRequestId(`hello-${this.id}`), {
			bridgeGeneration: this.generation,
			bridgeId: this.id,
			connectionGeneration,
			maxShards: this.maxShards,
			restartPolicy: this.options.restart,
			runningShards,
		});
	}

	protected async sendHeartbeat(connectionGeneration: number): Promise<void> {
		await this.sendHub("bridge.heartbeat", createRequestId(`heartbeat-${this.id}`), {
			bridgeGeneration: this.generation,
			connectionGeneration,
			sentAt: Date.now(),
		});
	}

	protected async handleHubMessage(value: unknown, socket: WebSocket, connectionGeneration: number): Promise<void> {
		if (this.socket !== socket || this.connectionGeneration !== connectionGeneration) return;
		const message = parseWireMessage(value, HUB_TO_BRIDGE_TYPES, this.payloadPolicy);
		switch (message.type) {
			case "hub.sync":
				await this.handleSync(message, socket, connectionGeneration);
				return;
			case "hub.shard.start":
				await this.handleStart(message);
				return;
			case "hub.shard.stop":
				await this.handleStop(message);
				return;
			case "hub.identify.response":
				await this.forwardIdentifyResponse(message);
				return;
			case "hub.route.request":
				await this.forwardRouteRequest(message);
				return;
			case "hub.route.response":
				await this.forwardRouteResponse(message);
				return;
			case "hub.eval.prepare":
				await this.forwardEvalPrepare(message);
				return;
			case "hub.eval.commit":
				await this.forwardEvalCommit(message);
				return;
			case "hub.eval.cancel":
				await this.forwardEvalCancel(message);
				return;
			case "hub.eval.response":
				await this.forwardEvalResponse(message);
				return;
			default:
				throw new ShardingProtocolError(`Unhandled Hub message ${message.type}.`);
		}
	}
}
