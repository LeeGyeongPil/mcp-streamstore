# Changelog

## 0.1.2

- Redis connection errors now end with a period and show the underlying cause.
- CI simplified; the latest-SDK check moved to a separate `sdk-latest.yml` workflow.
- Test suite grows to 93 tests; README and screenshots refreshed.

## 0.1.1

- CI workflow hardened so the test job is more robust on fresh runners. No library changes.

## 0.1.0

First release.

- `MemoryEventStore`, `SqliteEventStore` (`node:sqlite`) and `RedisEventStore` (Redis Streams) with TTL and per-stream size retention.
- `forSession`, `deleteSession`, `prune` for session-scoped replay.
- `Memory`/`Sqlite`/`RedisSessionStore`, `captureSession` and `rehydrate` for stateful sessions on another instance.
- `RedisServerEventBus` for SDK v2 cross-node list-changed events.
- `mcp-streamstore/conformance` test suite and `mcp-streamstore/testing` practice fake (mock) Redis.
- Demo CLI with `--lang en|ko`.
