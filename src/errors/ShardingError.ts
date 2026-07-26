type ShardingErrorCode =
	| "CAPACITY"
	| "CONFIGURATION"
	| "PERSISTENCE"
	| "PROTOCOL"
	| "REMOTE"
	| "STATE"
	| "TIMEOUT"
	| "TRANSPORT";

export class ShardingError extends Error {
	public readonly code: ShardingErrorCode;

	public constructor(code: ShardingErrorCode, message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "ShardingError";
		this.code = code;
	}
}

export class ShardingConfigurationError extends ShardingError {
	public constructor(message: string, options?: ErrorOptions) {
		super("CONFIGURATION", message, options);
		this.name = "ShardingConfigurationError";
	}
}

export class ShardingCapacityError extends ShardingError {
	public constructor(message: string, options?: ErrorOptions) {
		super("CAPACITY", message, options);
		this.name = "ShardingCapacityError";
	}
}

export class ShardingPersistenceError extends ShardingError {
	public constructor(message: string, options?: ErrorOptions) {
		super("PERSISTENCE", message, options);
		this.name = "ShardingPersistenceError";
	}
}

export class ShardingProtocolError extends ShardingError {
	public constructor(message: string, options?: ErrorOptions) {
		super("PROTOCOL", message, options);
		this.name = "ShardingProtocolError";
	}
}

export class ShardingRemoteError extends ShardingError {
	public readonly remoteCode: string;

	public constructor(remoteCode: string, message: string, options?: ErrorOptions) {
		super("REMOTE", message, options);
		this.name = "ShardingRemoteError";
		this.remoteCode = remoteCode;
	}
}

export class ShardingStateError extends ShardingError {
	public constructor(message: string, options?: ErrorOptions) {
		super("STATE", message, options);
		this.name = "ShardingStateError";
	}
}

export class ShardingTimeoutError extends ShardingError {
	public constructor(message: string, options?: ErrorOptions) {
		super("TIMEOUT", message, options);
		this.name = "ShardingTimeoutError";
	}
}

export class ShardingTransportError extends ShardingError {
	public constructor(message: string, options?: ErrorOptions) {
		super("TRANSPORT", message, options);
		this.name = "ShardingTransportError";
	}
}
