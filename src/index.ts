// 공개 API. 런타임 의존성 0 — SDK 는 선택 peer 로, 시나리오 도구(mcp-streamstore/conformance)에서만 동적으로 불러온다.
export type {
  EventId,
  EventStore,
  JSONRPCMessage,
  RedisCommand,
  ReplaySink,
  RetentionOptions,
  SessionRecord,
  SessionStore,
  StreamId,
  StreamStore,
} from './types.js';
export { DEFAULT_MAX_EVENTS_PER_STREAM, DEFAULT_TTL_MS } from './types.js';

export { MemoryEventStore } from './stores/memory.js';
export { SqliteEventStore, type SqliteEventStoreOptions } from './stores/sqlite.js';
export { RedisEventStore, type RedisEventStoreOptions } from './stores/redis.js';
export { SessionScopedEventStore, scopeKey, parseScopeKey } from './scope.js';

export {
  nodeRedisCommand,
  ioredisCommand,
  nodeRedisPubSub,
  ioredisPubSub,
  type PubSub,
  type NodeRedisLike,
  type IoredisLike,
} from './redis-clients.js';

export { MemorySessionStore, SqliteSessionStore, RedisSessionStore, type SessionStoreOptions } from './sessions/stores.js';
export { captureSession, rehydrate, RehydrateUnsupportedError, TESTED_SDK_VERSIONS, installedSdkVersion } from './sessions/rehydrate.js';

export { RedisServerEventBus, type RedisServerEventBusOptions, type ServerEvent, type ServerEventBus } from './bus/redis-bus.js';
