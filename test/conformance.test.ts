// AC-1: 메모리·SQLite·가짜 Redis 저장소가 적합성 묶음을 통과하고, SDK 예제 저장소는 순서 검사에서 실패한다(대조군).
import assert from 'node:assert/strict';
import { after, describe, it } from 'node:test';
import { join } from 'node:path';
import { checkSameMsOrdering, runEventStoreConformance } from '../src/conformance/index.js';
import { loadSdkExampleStore } from '../src/conformance/two-instance.js';
import { MemoryEventStore, RedisEventStore, SqliteEventStore } from '../src/index.js';
import { createFakeRedis } from '../src/testing/fake-redis.js';
import { tempDir } from './helpers.js';

const tmp = tempDir('conformance');
after(tmp.cleanup);

// 메모리: 같은 프로세스 안에서 "같은 백엔드" = 같은 객체
const memory = new Map<string, MemoryEventStore>();
runEventStoreConformance('MemoryEventStore', ({ shared, retention }) => {
  let s = memory.get(shared);
  if (!s) {
    s = new MemoryEventStore(retention);
    memory.set(shared, s);
  }
  return s;
});

// SQLite: 같은 백엔드 = 같은 파일(핸들은 매번 새로 연다 — 다른 프로세스와 같은 조건)
runEventStoreConformance('SqliteEventStore', ({ shared, retention }) => new SqliteEventStore({ path: join(tmp.dir, `${shared}.db`), ...retention }));

// Redis: 연습용 가짜(mock) Redis 하나를 두고, 같은 백엔드 = 같은 키 접두사
const fake = createFakeRedis();
runEventStoreConformance(
  'RedisEventStore (practice fake Redis)',
  ({ shared, retention }) => new RedisEventStore({ command: fake.command, prefix: `t:${shared}:`, ...retention }),
);

describe('control case: the SDK example store', () => {
  it('example InMemoryEventStore reorders same-ms events', async () => {
    const Example = await loadSdkExampleStore();
    const { inOrder, got } = await checkSameMsOrdering(new Example(), 1000);
    // 예제는 "<스트림>_<ms>_<무작위>" 문자열 정렬로 재생한다 → 같은 ms 안에서는 순서가 무작위
    assert.equal(inOrder, false, `expected a shuffled order, got ${got.slice(0, 10).join(',')}...`);
  });

  it('example InMemoryEventStore cannot find the "_GET_stream" stream from its own event IDs', async () => {
    const Example = await loadSdkExampleStore();
    const store = new Example();
    const first = await store.storeEvent('_GET_stream', { jsonrpc: '2.0', method: 'a' });
    await store.storeEvent('_GET_stream', { jsonrpc: '2.0', method: 'b' });
    let sent = 0;
    // 예제는 "_" 로 잘라 스트림 ID 를 얻으므로 "_GET_stream" 은 빈 문자열이 된다
    assert.equal(await store.replayEventsAfter(first, { send: async () => void sent++ }), '');
    assert.equal(sent, 0);
  });

  it('MemoryEventStore keeps the same 1000 same-ms events in order', async () => {
    const { inOrder } = await checkSameMsOrdering(new MemoryEventStore({ now: () => 1 }), 1000);
    assert.equal(inOrder, true);
  });
});

describe('retention options are validated', () => {
  it('rejects ttlMs <= 0 and maxEventsPerStream < 1', () => {
    assert.throws(() => new MemoryEventStore({ ttlMs: 0 }), RangeError);
    assert.throws(() => new MemoryEventStore({ maxEventsPerStream: 0 }), RangeError);
  });
  it('requires a path / command', () => {
    assert.throws(() => new SqliteEventStore({ path: '' }), TypeError);
    assert.throws(() => new RedisEventStore({} as never), TypeError);
  });
  it('a closed SqliteEventStore refuses further use and close() is idempotent', async () => {
    const s = new SqliteEventStore({ path: join(tmp.dir, 'closed.db') });
    await s.close();
    await s.close();
    await assert.rejects(s.storeEvent('x', {}), /closed/);
  });
});
