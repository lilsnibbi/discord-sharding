CREATE TABLE bridges (
	id TEXT PRIMARY KEY NOT NULL,
	generation TEXT NOT NULL,
	max_shards INTEGER NOT NULL CHECK (max_shards > 0 AND max_shards <= 100000),
	connected INTEGER NOT NULL CHECK (connected IN (0, 1)),
	updated_at INTEGER NOT NULL CHECK (updated_at >= 0)
);

CREATE TABLE assignments (
	shard_id INTEGER PRIMARY KEY NOT NULL CHECK (shard_id >= 0 AND shard_id < 100000),
	bridge_id TEXT NOT NULL,
	epoch INTEGER NOT NULL CHECK (epoch > 0),
	updated_at INTEGER NOT NULL CHECK (updated_at >= 0)
);

CREATE TABLE shards (
	shard_id INTEGER PRIMARY KEY NOT NULL CHECK (shard_id >= 0 AND shard_id < 100000),
	bridge_id TEXT NOT NULL,
	assignment_epoch INTEGER NOT NULL CHECK (assignment_epoch > 0),
	process_generation INTEGER NOT NULL CHECK (process_generation > 0),
	state TEXT NOT NULL CHECK (state IN ('assigned', 'starting', 'ready', 'stopping', 'stopped', 'failed')),
	updated_at INTEGER NOT NULL CHECK (updated_at >= 0)
);

CREATE TABLE analytics (
	id TEXT PRIMARY KEY NOT NULL,
	bridge_id TEXT NOT NULL,
	shard_id INTEGER CHECK (shard_id IS NULL OR (shard_id >= 0 AND shard_id < 100000)),
	collected_at INTEGER NOT NULL CHECK (collected_at >= 0),
	data_json TEXT NOT NULL CHECK (json_valid(data_json))
);

CREATE INDEX assignments_bridge_idx ON assignments (bridge_id, shard_id);
CREATE INDEX bridges_connected_idx ON bridges (connected, updated_at);
CREATE INDEX shards_bridge_state_idx ON shards (bridge_id, state, shard_id);
CREATE INDEX analytics_collected_idx ON analytics (collected_at, id);
CREATE INDEX analytics_bridge_collected_idx ON analytics (bridge_id, collected_at, id);
CREATE INDEX analytics_shard_collected_idx ON analytics (shard_id, collected_at, id);
