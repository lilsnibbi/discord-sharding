export const DEFAULT_HOSTNAME = "0.0.0.0";
export const DEFAULT_PORT = 3_000;
export const DEFAULT_REDIS_URL = "redis://127.0.0.1:6379";
export const DEFAULT_KEY_PREFIX = "sharding";
export const DEFAULT_GATEWAY_ENDPOINT = "https://discord.com/api/v10/gateway/bot";
export const DEFAULT_MAX_BUFFERED_BYTES = 4_194_304;
export const DEFAULT_MAX_QUEUED_MESSAGES = 1_024;
export const DEFAULT_MAX_EVALUATIONS = 32;
export const DEFAULT_EVALUATION_COMMIT_LEAD_MS = 25;
export const DEFAULT_ANALYTICS_BATCH_SIZE = 1_000;
export const HELLO_TIMEOUT_MS = 10_000;
export const WEBSOCKET_IDLE_TIMEOUT_SECONDS = 45;
export const RELEASED_ASSIGNMENT_PREFIX = "__released__:";
export const IDLE_ASSIGNMENT_MUTATION = Promise.resolve();
export const OPTION_KEYS = new Set([
	"adminToken",
	"botToken",
	"bridgeToken",
	"evaluationCommitLeadMs",
	"fetch",
	"gatewayEndpoint",
	"hostname",
	"keyPrefix",
	"maxBufferedBytes",
	"maxEvaluations",
	"maxQueuedMessages",
	"now",
	"onError",
	"payload",
	"persistence",
	"port",
	"redisUrl",
	"request",
	"sleep",
	"totalShards",
	"wallClock",
]);
