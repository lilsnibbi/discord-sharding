import { describe, expect, test } from "bun:test";
import { ShardingProtocolError } from "../../src/errors/ShardingError";
import { normalizePayloadPolicy } from "../../src/internal/payload";
import {
	createWireMessage,
	encodeWireMessage,
	parseWireMessage,
	requireData,
	requireExactKeys,
	requireOptionalKeys,
} from "../../src/protocol/codec";
import { readError, readInteger, readRouteKind, validateResponseShape } from "../../src/protocol/readers";
import { BRIDGE_TO_SHARD_TYPES, HUB_TO_BRIDGE_TYPES, SHARD_TO_BRIDGE_TYPES } from "../../src/protocol/types";

const policy = normalizePayloadPolicy({
	maxBytes: 4_096,
	maxDepth: 12,
	maxNodes: 128,
});

describe("protocol codec", () => {
	test("round-trips a routed request through exact JSON validation", () => {
		const message = createWireMessage(
			"shard.route.request",
			"route:1",
			{
				kind: "request",
				payload: { action: "status" },
				targetShardId: 2,
			},
			policy,
		);
		const parsed = parseWireMessage(encodeWireMessage(message, policy), SHARD_TO_BRIDGE_TYPES, policy);
		const data = requireData(parsed, "shard.route.request", new Set(["kind", "payload", "targetShardId"]));

		expect(parsed.id).toBe("route:1");
		expect(readRouteKind(data)).toBe("request");
		expect(readInteger(data, "targetShardId")).toBe(2);
		expect(data.payload).toEqual({ action: "status" });
		expect(Object.isFrozen(parsed)).toBe(true);
		expect(Object.isFrozen(data)).toBe(true);
	});

	test("accepts UTF-8 bytes and rejects malformed encodings", () => {
		const message = createWireMessage(
			"shard.control.maintenance",
			"sync:1",
			{ acknowledge: false, maintenance: true, topologyVersion: 4 },
			policy,
		);
		const bytes = new TextEncoder().encode(encodeWireMessage(message, policy));

		expect(parseWireMessage(bytes, BRIDGE_TO_SHARD_TYPES, policy).type).toBe("shard.control.maintenance");
		expect(() => parseWireMessage(new Uint8Array([0xff]), BRIDGE_TO_SHARD_TYPES, policy)).toThrow(
			ShardingProtocolError,
		);
	});

	test("rejects wrong channels, versions, fields, and oversized data", () => {
		const message = {
			data: { acknowledge: false, maintenance: true, topologyVersion: 1 },
			id: "sync:1",
			type: "shard.control.maintenance",
			version: 1,
		};

		expect(() => parseWireMessage(message, HUB_TO_BRIDGE_TYPES, policy)).toThrow(ShardingProtocolError);
		expect(() => parseWireMessage({ ...message, version: 2 }, BRIDGE_TO_SHARD_TYPES, policy)).toThrow(
			ShardingProtocolError,
		);
		expect(() => parseWireMessage({ ...message, unexpected: true }, BRIDGE_TO_SHARD_TYPES, policy)).toThrow(
			ShardingProtocolError,
		);
		expect(() =>
			parseWireMessage(JSON.stringify(message), BRIDGE_TO_SHARD_TYPES, {
				...policy,
				maxBytes: 8,
			}),
		).toThrow(ShardingProtocolError);
	});

	test("returns an immutable snapshot instead of caller-owned outbound data", () => {
		const payload = { nested: { status: "before" } };
		const message = createWireMessage(
			"shard.route.request",
			"route:snapshot",
			{ kind: "message", payload, targetShardId: 0 },
			policy,
		);

		payload.nested.status = "after";

		expect(message.data.payload).toEqual({ nested: { status: "before" } });
		expect(Object.isFrozen(message.data)).toBe(true);
		expect(Object.isFrozen(message.data.payload)).toBe(true);
	});

	test("rejects cyclic, accessor-backed, and non-JSON payloads", () => {
		const cyclic: { self?: unknown } = {};
		cyclic.self = cyclic;
		expect(() =>
			createWireMessage(
				"shard.route.request",
				"route:cycle",
				{ kind: "message", payload: cyclic, targetShardId: 0 },
				policy,
			),
		).toThrow(ShardingProtocolError);

		let reads = 0;
		const payload = {};
		Object.defineProperty(payload, "secret", {
			enumerable: true,
			get: () => {
				reads += 1;
				return "unsafe";
			},
		});
		expect(() =>
			createWireMessage("shard.route.request", "route:getter", { kind: "message", payload, targetShardId: 0 }, policy),
		).toThrow(ShardingProtocolError);
		expect(reads).toBe(0);
		expect(() =>
			createWireMessage(
				"shard.route.request",
				"route:number",
				{ kind: "message", payload: Number.NaN, targetShardId: 0 },
				policy,
			),
		).toThrow(ShardingProtocolError);
	});

	test("validates exact response and serialized-error shapes", () => {
		const successful = Object.freeze({ ok: true, value: { accepted: true } });
		const failed = Object.freeze({
			error: Object.freeze({ code: "REMOTE_FAILURE", message: "No route", name: "Error" }),
			ok: false,
		});

		expect(() => validateResponseShape(successful, "response")).not.toThrow();
		expect(readError(failed.error)).toEqual({
			code: "REMOTE_FAILURE",
			message: "No route",
			name: "Error",
		});
		expect(() => validateResponseShape(failed, "response")).not.toThrow();
		expect(() => requireExactKeys(successful, new Set(["ok"]), "response")).toThrow(ShardingProtocolError);
		expect(() => requireOptionalKeys(successful, new Set(["ok"]), new Set(["value"]), "response")).not.toThrow();
	});
});
