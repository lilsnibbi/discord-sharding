import { describe, expect, test } from "bun:test";
import { SQLiteHubPersistence } from "../../src/hub/database/SQLiteHubPersistence";

describe("SQLite integration", () => {
	test("persists Hub state across file-backed instances", async () => {
		const path = `${import.meta.dir}/sqlite-integration-${Bun.randomUUIDv7()}.sqlite`;
		const bridgeId = `bridge-${Bun.randomUUIDv7()}`;
		const first = new SQLiteHubPersistence(path);
		try {
			await first.migrate();
			await first.saveBridge({
				connected: true,
				generation: "generation-file",
				id: bridgeId,
				maxShards: 2,
				updatedAt: 1,
			});
			await first.saveAssignment({ bridgeId, epoch: 7, shardId: 1, updatedAt: 2 });
			await first.saveShard({
				assignmentEpoch: 7,
				bridgeId,
				processGeneration: 3,
				shardId: 1,
				state: "ready",
				updatedAt: 3,
			});
		} finally {
			await first.close();
		}
		const second = new SQLiteHubPersistence(path);
		try {
			await second.migrate();
			const state = await second.loadState();
			expect(state.assignments).toEqual([{ bridgeId, epoch: 7, shardId: 1, updatedAt: 2 }]);
			expect(state.bridges[0]?.id).toBe(bridgeId);
			expect(state.shards[0]?.state).toBe("ready");
		} finally {
			await second.close();
			for (const candidate of [path, `${path}-shm`, `${path}-wal`]) {
				const file = Bun.file(candidate);
				if (await file.exists()) await file.delete();
			}
		}
	});
});
