// 공용 형식. SDK 를 런타임에 import 하지 않도록 SDK 의 EventStore 와 같은 모양을 구조 타입으로 다시 적는다.
// (@modelcontextprotocol/sdk 1.32.2 와 @modelcontextprotocol/server 2.4.0 의 EventStore 선언이 같다)

/**
 * A JSON-RPC message as the SDK hands it to an EventStore. The priming event is the empty object `{}`.
 * Kept as `any` on purpose so the stores are assignable to both SDK versions' `EventStore` type.
 */
export type JSONRPCMessage = any;

export type StreamId = string;
export type EventId = string;

/** Callback the SDK passes to `replayEventsAfter`. */
export interface ReplaySink {
  send: (eventId: EventId, message: JSONRPCMessage) => Promise<void>;
}

/** Same shape as the SDK `EventStore` interface (v1 and v2). */
export interface EventStore {
  storeEvent(streamId: StreamId, message: JSONRPCMessage): Promise<EventId>;
  getStreamIdForEventId?(eventId: EventId): Promise<StreamId | undefined>;
  replayEventsAfter(lastEventId: EventId, sink: ReplaySink): Promise<StreamId>;
}

/** An EventStore that can also scope itself to a session and clean up after itself. */
export interface StreamStore extends EventStore {
  getStreamIdForEventId(eventId: EventId): Promise<StreamId | undefined>;
  /**
   * A view of this store limited to one MCP session. Stream keys become `<sessionId>/<streamId>`,
   * and event IDs that belong to another session look unknown (`undefined` / `''`).
   */
  forSession(sessionId: string): EventStore & { getStreamIdForEventId(eventId: EventId): Promise<StreamId | undefined> };
  /** Delete every stream that was written through `forSession(sessionId)`. */
  deleteSession(sessionId: string): Promise<void>;
  /** Apply the retention policy now. Resolves with the number of events removed. */
  prune(): Promise<number>;
  /** Release the underlying connection or file handle. Safe to call twice. */
  close(): Promise<void>;
}

export interface RetentionOptions {
  /** Drop events older than this many milliseconds. Default 1 hour. `Infinity` keeps them forever. */
  ttlMs?: number;
  /** Keep at most this many events per stream (oldest go first). Default 10 000. `Infinity` disables. */
  maxEventsPerStream?: number;
  /** Clock used for timestamps and pruning (inject a fake clock in tests). Default `Date.now`. */
  now?: () => number;
}

export const DEFAULT_TTL_MS = 60 * 60 * 1000;
export const DEFAULT_MAX_EVENTS_PER_STREAM = 10_000;

/** What another instance needs to know to serve an existing stateful session. */
export interface SessionRecord {
  sessionId: string;
  protocolVersion: string;
  clientCapabilities: Record<string, unknown>;
  clientInfo?: Record<string, unknown>;
  createdAt: number;
  lastSeenAt: number;
}

export interface SessionStore {
  get(sessionId: string): Promise<SessionRecord | undefined>;
  put(record: SessionRecord): Promise<void>;
  /** Refresh `lastSeenAt` (and the expiry) of a session. No-op for unknown IDs. */
  touch(sessionId: string): Promise<void>;
  delete(sessionId: string): Promise<void>;
  close(): Promise<void>;
}

/** Sends one Redis command, e.g. `['XADD', key, '*', 'm', json]`, and resolves with the raw reply. */
export type RedisCommand = (args: string[]) => Promise<unknown>;

/** Resolve retention options into concrete numbers. */
export function resolveRetention(opts: RetentionOptions = {}): Required<RetentionOptions> {
  const ttlMs = opts.ttlMs ?? DEFAULT_TTL_MS;
  const maxEventsPerStream = opts.maxEventsPerStream ?? DEFAULT_MAX_EVENTS_PER_STREAM;
  if (!(ttlMs > 0)) throw new RangeError(`ttlMs must be > 0 (got ${ttlMs})`);
  if (!(maxEventsPerStream >= 1)) throw new RangeError(`maxEventsPerStream must be >= 1 (got ${maxEventsPerStream})`);
  return { ttlMs, maxEventsPerStream, now: opts.now ?? Date.now };
}
