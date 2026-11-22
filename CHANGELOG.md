# Changelog

## 0.1.4

- CI: the required job is offline only (ubuntu-24.04, Node 22.23.5, actions pinned by commit SHA, no service containers) and runs `npm run ci` (tests + demo), the same command you can run locally.
- The real Redis suite moved to a manual workflow (`redis-real`); locally it is still `npm run test:redis` with `REDIS_URL`.
- New tests: a CI contract check for the workflow files and a resume case over a delayed practice fake Redis.
- No library changes.

## 0.1.3

- CI reduced to a single job (ubuntu-latest, Node 22, redis:7 service); the separate `sdk-latest.yml` workflow was removed.
- README and CI comments updated to match. No library changes.

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
