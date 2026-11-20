// Installed-package type check: every "exports" subpath resolves to its declarations.
import { MemoryEventStore, RedisEventStore, SqliteEventStore, rehydrate, type SessionRecord, type StreamStore } from 'mcp-streamstore';
import { runEventStoreConformance, runResumeScenario, type ResumeScenarioResult } from 'mcp-streamstore/conformance';
import { createFakeRedis, type FakeRedis } from 'mcp-streamstore/testing';

const fake: FakeRedis = createFakeRedis();
const stores: StreamStore[] = [new MemoryEventStore(), new SqliteEventStore({ path: ':memory:' }), new RedisEventStore({ command: fake.command })];
const record: SessionRecord = { sessionId: 's', protocolVersion: '2025-11-25', clientCapabilities: {}, createdAt: 0, lastSeenAt: 0 };
const scenario: (sdk: 'v1' | 'v2') => Promise<ResumeScenarioResult> = (sdk) => runResumeScenario({ sdk, eventStore: () => stores[0]! });

export { record, rehydrate, runEventStoreConformance, scenario };
