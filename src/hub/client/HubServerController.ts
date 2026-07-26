import { ShardingConfigurationError, ShardingProtocolError, ShardingTransportError } from "../../errors/ShardingError";
import { requireIdentifier } from "../../internal/validation";
import { parseWireMessage } from "../../protocol/codec";
import { BRIDGE_TO_HUB_TYPES, type ParsedWireMessage } from "../../protocol/types";
import type { $HubBridgeSessionHeaders, $HubBridgeSessionSocketData } from "../session/HubBridgeSession";
import { HubBridgeSession } from "../session/HubBridgeSession";
import { HELLO_TIMEOUT_MS, RELEASED_ASSIGNMENT_PREFIX, WEBSOCKET_IDLE_TIMEOUT_SECONDS } from "./constants";
import { HubProtocolController } from "./HubProtocolController";
import {
	constantTimeTokenMatch,
	errorCode,
	errorStatus,
	matchReleasePath,
	parseHeaderInteger,
	parseOptionalQueryInteger,
	textResponse,
	toError,
} from "./utilities";

export abstract class HubServerController extends HubProtocolController {
	protected createServer(): Bun.Server<$HubBridgeSessionSocketData> {
		const hub = this;
		return Bun.serve<$HubBridgeSessionSocketData>({
			development: false,
			fetch(request, server) {
				return hub.handleFetch(request, server);
			},
			hostname: this.options.hostname,
			port: this.options.port,
			websocket: {
				backpressureLimit: this.options.maxBufferedBytes,
				closeOnBackpressureLimit: true,
				close(ws, code, reason) {
					hub.handleSocketClose(ws, code, reason);
				},
				drain(ws) {
					ws.data.session?.drain();
				},
				idleTimeout: WEBSOCKET_IDLE_TIMEOUT_SECONDS,
				maxPayloadLength: this.options.payload.maxBytes,
				message(ws, value) {
					return hub.handleSocketMessage(ws, value);
				},
				open(ws) {
					hub.handleSocketOpen(ws);
				},
				perMessageDeflate: false,
				sendPings: true,
			},
		});
	}

	protected async handleFetch(
		request: Request,
		server: Bun.Server<$HubBridgeSessionSocketData>,
	): Promise<Response | undefined> {
		const url = new URL(request.url);
		if (url.pathname === "/bridge") {
			if (!constantTimeTokenMatch(request.headers.get("authorization"), this.options.bridgeToken)) {
				return textResponse("Unauthorized", 401);
			}
			let headers: $HubBridgeSessionHeaders;
			try {
				const bridgeId = requireIdentifier(request.headers.get("x-sharding-bridge-id"), "Bridge ID");
				if (bridgeId.startsWith(RELEASED_ASSIGNMENT_PREFIX)) {
					throw new ShardingConfigurationError("Bridge ID uses a reserved prefix.");
				}
				headers = Object.freeze({
					bridgeGeneration: requireIdentifier(request.headers.get("x-sharding-bridge-generation"), "Bridge generation"),
					bridgeId,
					connectionGeneration: parseHeaderInteger(
						request.headers.get("x-sharding-connection-generation"),
						"Connection generation",
					),
				});
			} catch {
				return textResponse("Invalid Bridge headers", 400);
			}
			const upgraded = server.upgrade(request, {
				data: {
					headers,
					session: undefined,
				},
			});
			return upgraded ? undefined : textResponse("WebSocket upgrade failed", 400);
		}
		if (url.pathname === "/health" && request.method === "GET") {
			return Response.json({
				state: this.lifecycleState,
				totalShards: this.shardCount,
			});
		}
		if (!constantTimeTokenMatch(request.headers.get("authorization"), this.options.adminToken)) {
			return textResponse("Unauthorized", 401);
		}
		try {
			if (url.pathname === "/topology" && request.method === "GET") {
				return Response.json(this.getTopology());
			}
			if (url.pathname === "/reconcile" && request.method === "POST") {
				return Response.json(await this.reconcile());
			}
			if (url.pathname === "/analytics" && request.method === "DELETE") {
				const before = parseOptionalQueryInteger(url.searchParams.get("before"), "before");
				const batchSize = parseOptionalQueryInteger(url.searchParams.get("batchSize"), "batchSize");
				return Response.json({
					removed: await this.clearAnalytics({
						...(batchSize === undefined ? {} : { batchSize }),
						...(before === undefined ? {} : { before }),
					}),
				});
			}
			const releasedBridgeId = matchReleasePath(url.pathname);
			if (releasedBridgeId !== null && request.method === "DELETE") {
				return Response.json({ releasedShardIds: await this.releaseBridge(releasedBridgeId) });
			}
			return textResponse("Not found", 404);
		} catch (cause) {
			const error = toError(cause);
			this.report(error, "management request");
			return Response.json({ code: errorCode(error), message: error.message }, { status: errorStatus(error) });
		}
	}

	protected handleSocketOpen(socket: Bun.ServerWebSocket<$HubBridgeSessionSocketData>): void {
		if (this.lifecycleState !== "running") {
			socket.close(1012, "Hub unavailable");
			return;
		}
		const session = new HubBridgeSession(
			socket,
			socket.data.headers,
			this.options.payload,
			this.options.maxBufferedBytes,
			this.options.maxQueuedMessages,
			this.options.request.maxPending,
			this.options.request.timeoutMs,
			HELLO_TIMEOUT_MS,
		);
		socket.data.session = session;
		this.socketSessions.add(session);
	}

	protected handleSocketMessage(
		socket: Bun.ServerWebSocket<$HubBridgeSessionSocketData>,
		value: string | Uint8Array,
	): void {
		const session = socket.data.session;
		if (session === undefined || session.phase === "closed") return;
		try {
			const input = typeof value === "string" ? value : Uint8Array.from(value);
			session.enqueueInbound(async () => {
				if (session.phase === "closed") return;
				try {
					const message = parseWireMessage(input, BRIDGE_TO_HUB_TYPES, this.options.payload);
					await this.dispatchBridgeMessage(session, message);
				} catch (cause) {
					this.closeSessionAfterFailure(session, cause);
				}
			});
		} catch (cause) {
			this.closeSessionAfterFailure(session, cause);
		}
	}

	protected override closeSessionAfterFailure(
		session: HubBridgeSession,
		cause: unknown,
		context = "inbound message",
	): void {
		const error = toError(cause);
		this.report(error, `Bridge ${session.bridgeId} ${context}`);
		try {
			session.fail(error);
		} catch (closeCause) {
			this.report(toError(closeCause), `Bridge ${session.bridgeId} failure close`);
		}
	}

	protected handleSocketClose(
		socket: Bun.ServerWebSocket<$HubBridgeSessionSocketData>,
		code: number,
		reason: string,
	): void {
		const session = socket.data.session;
		if (session === undefined) return;
		socket.data.session = undefined;
		this.socketSessions.delete(session);
		const suffix = reason.length === 0 ? "" : `: ${reason}`;
		session.finish(new ShardingTransportError(`Bridge socket closed with ${code}${suffix}.`));
		this.handleSessionClosed(session);
	}

	protected async dispatchBridgeMessage(session: HubBridgeSession, message: ParsedWireMessage): Promise<void> {
		if (session.phase === "awaiting-hello" && message.type !== "bridge.hello") {
			throw new ShardingProtocolError("Bridge must send hello before other messages.");
		}
		if (session.phase !== "awaiting-hello" && message.type === "bridge.hello") {
			throw new ShardingProtocolError("Bridge hello cannot be repeated.");
		}
		switch (message.type) {
			case "bridge.hello":
				await this.handleHello(session, message);
				return;
			case "bridge.sync.ready":
				this.handleSyncReady(session, message);
				return;
			case "bridge.heartbeat":
				this.handleHeartbeat(session, message);
				return;
			case "bridge.identify.request":
				this.handleIdentifyRequest(session, message);
				return;
			case "bridge.route.request":
				this.handleRouteRequest(session, message);
				return;
			case "bridge.route.response":
				this.handleRouteResponse(session, message);
				return;
			case "bridge.eval.request":
				this.handleEvaluationRequest(session, message);
				return;
			case "bridge.eval.prepared":
				this.handleEvaluationPrepared(session, message);
				return;
			case "bridge.eval.result":
				this.handleEvaluationResult(session, message);
				return;
			case "bridge.shard.state":
				await this.handleShardState(session, message);
				return;
			case "bridge.shard.stopped":
				await this.handleShardStopped(session, message);
				return;
			case "bridge.analytics":
				await this.handleAnalytics(session, message);
				return;
			default:
				throw new ShardingProtocolError(`Unhandled Bridge message ${message.type}.`);
		}
	}
}
