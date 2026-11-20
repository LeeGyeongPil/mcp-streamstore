// AC-4: 세션형 서버에서 B 가 그대로 재개하면 400 "Server not initialized",
// SessionStore 기록으로 rehydrate 하면 200 재생 + 클라이언트 능력 복원. 내부 필드가 없으면 RehydrateUnsupportedError.
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, describe, it } from 'node:test';
import { join } from 'node:path';
import { runSessionStoreConformance } from '../src/conformance/index.js';
import { runResumeScenario, type SdkFlavor } from '../src/conformance/two-instance.js';
import {
  MemorySessionStore,
  RedisEventStore,
  RedisSessionStore,
  RehydrateUnsupportedError,
  SqliteEventStore,
  SqliteSessionStore,
  captureSession,
  installedSdkVersion,
  rehydrate,
  type SessionRecord,
} from '../src/index.js';
import { createFakeRedis, type FakeRedis } from '../src/testing/fake-redis.js';
import { tempDir } from './helpers.js';

const tmp = tempDir('rehydrate');
const closers: Array<{ close: () => Promise<void> }> = [];
after(async () => {
  for (const c of closers) await c.close();
  tmp.cleanup();
});
const keep = <T extends { close: () => Promise<void> }>(x: T): T => {
  closers.push(x);
  return x;
};

const CAPS = { sampling: {}, elicitation: { form: {} }, roots: { listChanged: true } };

for (const sdk of ['v1', 'v2'] as SdkFlavor[]) {
  describe(`stateful rehydration, SDK ${sdk}`, () => {
    it('B answers 400 "Server not initialized", then 200 with the replay after rehydrate (SQLite)', async () => {
      const path = join(tmp.dir, `stateful-${sdk}.db`);
      const sessions = keep(new SqliteSessionStore({ path }));
      const r = await runResumeScenario({
        sdk,
        stateful: true,
        sessions,
        rehydrateOnB: true,
        clientCapabilities: CAPS,
        eventStore: (_inst, sid) => keep(new SqliteEventStore({ path })).forSession(sid!),
      });
      const [plain, re] = r.attempts;
      assert.equal(plain!.status, 400);
      assert.match(plain!.body ?? '', /Server not initialized/);
      assert.equal(re!.status, 200, re!.body);
      assert.deepEqual(re!.replayed.map((e) => e.kind), ['notifications/message', 'notifications/message', 'result']);
      assert.deepEqual(r.clientCapabilitiesOnA, CAPS);
      assert.deepEqual(r.clientCapabilitiesOnB, CAPS, 'server.getClientCapabilities() on B equals the original');
      assert.equal(r.record?.sessionId, r.sessionId);
    });

    it('same flow over the practice fake (mock) Redis session + event stores', async () => {
      const fake = createFakeRedis();
      const r = await runResumeScenario({
        sdk,
        stateful: true,
        sessions: new RedisSessionStore({ command: fake.command }),
        rehydrateOnB: true,
        clientCapabilities: CAPS,
        eventStore: (_inst, sid) => new RedisEventStore({ command: fake.command }).forSession(sid!),
      });
      assert.deepEqual(r.attempts.map((a) => a.status), [400, 200]);
      assert.equal(r.attempts[1]!.replayed.length, 3);
      assert.deepEqual(r.clientCapabilitiesOnB, CAPS);
    });
  });
}

describe('rehydrate() guards', () => {
  const record: SessionRecord = {
    sessionId: 'sess-1',
    protocolVersion: '2025-11-25',
    clientCapabilities: { sampling: {} },
    createdAt: 1,
    lastSeenAt: 1,
  };

  it('unwraps the v1 Node StreamableHTTPServerTransport', async () => {
    const { StreamableHTTPServerTransport } = await import('@modelcontextprotocol/sdk/server/streamableHttp.js');
    const { McpServer } = await import('@modelcontextprotocol/sdk/server/mcp.js');
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: () => randomUUID() });
    const server = new McpServer({ name: 's', version: '1' });
    await server.connect(transport);
    try {
      rehydrate(transport, server, record);
      assert.equal(transport.sessionId, 'sess-1');
      assert.deepEqual(server.server.getClientCapabilities(), { sampling: {} });
    } finally {
      await server.close();
    }
  });

  it('throws RehydrateUnsupportedError (with the installed SDK version) for an unknown transport shape', () => {
    const fakeTransport = { sessionId: undefined, handleRequest: async () => new Response() };
    const fakeServer = { getClientCapabilities: () => undefined };
    assert.throws(
      () => rehydrate(fakeTransport, fakeServer, record),
      (e: unknown) => {
        assert.ok(e instanceof RehydrateUnsupportedError);
        assert.match(e.message, /_initialized/);
        assert.ok(e.message.includes(`@modelcontextprotocol/sdk ${installedSdkVersion('@modelcontextprotocol/sdk')}`), e.message);
        assert.match(e.message, /@modelcontextprotocol\/sdk 1\.32\.2/);
        return true;
      },
    );
  });

  it('refuses stateless transports and servers without getClientCapabilities', async () => {
    const { WebStandardStreamableHTTPServerTransport } = await import('@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js');
    const { McpServer } = await import('@modelcontextprotocol/sdk/server/mcp.js');
    const stateless = new WebStandardStreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    assert.throws(() => rehydrate(stateless, new McpServer({ name: 's', version: '1' }), record), RehydrateUnsupportedError);
    const stateful = new WebStandardStreamableHTTPServerTransport({ sessionIdGenerator: () => 'x' });
    assert.throws(() => rehydrate(stateful, {}, record), RehydrateUnsupportedError);
    assert.throws(() => rehydrate(stateful, new McpServer({ name: 's', version: '1' }), { ...record, sessionId: '' }), TypeError);
  });

  it('captureSession() before initialize explains what is missing', async () => {
    const { WebStandardStreamableHTTPServerTransport } = await import('@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js');
    const { McpServer } = await import('@modelcontextprotocol/sdk/server/mcp.js');
    const t = new WebStandardStreamableHTTPServerTransport({ sessionIdGenerator: () => 'x' });
    assert.throws(() => captureSession(t, new McpServer({ name: 's', version: '1' })), /no sessionId yet/);
  });
});

// SessionStore 적합성: 메모리·SQLite·가짜 Redis
const sqliteDir = tmp.dir;
runSessionStoreConformance('MemorySessionStore', (() => {
  const byShared = new Map<string, MemorySessionStore>();
  return ({ shared, ttlMs, now }) => {
    let s = byShared.get(shared);
    if (!s) {
      s = new MemorySessionStore({ ttlMs, now });
      byShared.set(shared, s);
    }
    // 같은 객체를 두 번 닫아도 되도록 close 는 비워 둔다(메모리 저장소의 "다른 핸들" 흉내)
    return { get: (id) => s!.get(id), put: (r) => s!.put(r), touch: (id) => s!.touch(id), delete: (id) => s!.delete(id), close: async () => undefined };
  };
})());
runSessionStoreConformance('SqliteSessionStore', ({ shared, ttlMs, now }) => new SqliteSessionStore({ path: join(sqliteDir, `sessions-${shared}.db`), ttlMs, now }));
runSessionStoreConformance('RedisSessionStore (practice fake Redis)', (() => {
  const fakes = new Map<string, FakeRedis>();
  return ({ shared, ttlMs, now }) => {
    let fake = fakes.get(shared);
    if (!fake) {
      fake = createFakeRedis({ now }); // 가짜 Redis 의 만료(PX)도 같은 가짜 시계로 움직인다
      fakes.set(shared, fake);
    }
    return new RedisSessionStore({ command: fake.command, ttlMs, now });
  };
})());
