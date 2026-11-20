// 공개 적합성 묶음: 어떤 EventStore 구현이든 "재개가 정말 되는가" 를 같은 기준으로 검사한다.
// node:test 의 describe/it 로 등록하므로 사용자는 자기 시험 파일에서 한 줄로 부르고 `node --test` 로 돌린다.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import type { EventStore, RetentionOptions, SessionRecord, SessionStore, StreamStore } from '../types.js';
import { hasSdk, runResumeScenario } from './two-instance.js';

export { runResumeScenario, runLeakScenario, readSse, loadSdk, hasSdk, loadSdkExampleStore } from './two-instance.js';
export type { ResumeScenarioOptions, ResumeScenarioResult, ResumeAttempt, LeakScenarioOptions, LeakScenarioResult, SdkFlavor } from './two-instance.js';

export interface ConformanceContext {
  /**
   * Names the backend. Two calls with the same `shared` value must see the same data
   * (same SQLite file, same Redis prefix, same in-process object); different values must not see each other.
   */
  shared: string;
  /** Retention options to pass to the store (includes a fake clock). */
  retention: RetentionOptions;
}

export type MakeStore = (ctx: ConformanceContext) => StreamStore | Promise<StreamStore>;

export interface ConformanceOptions {
  /** Events stored within one frozen millisecond for the ordering check. Default 1000. */
  sameMsCount?: number;
  /** Run the two-instance resume case with SDK v1 (skipped when the SDK is not installed). Default true. */
  twoInstance?: boolean;
}

/** A clock that only moves when the test says so. */
export function fakeClock(start = Date.now()) {
  let t = start;
  return {
    now: () => t,
    advance: (ms: number) => {
      t += ms;
    },
  };
}

const note = (i: number) => ({ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: i } });

/** Store `n` events in one stream and replay everything after the first. Shared by the suite and its control case. */
export async function checkSameMsOrdering(store: EventStore, n: number, streamId = `order-${crypto.randomUUID()}`) {
  const ids: string[] = [];
  for (let i = 0; i < n; i++) ids.push(await store.storeEvent(streamId, note(i)));
  const got: number[] = [];
  const gotIds: string[] = [];
  await store.replayEventsAfter(ids[0]!, {
    send: async (id, msg) => {
      got.push(msg.params.data);
      gotIds.push(id);
    },
  });
  const inOrder = got.length === n - 1 && got.every((v, i) => v === i + 1);
  return { inOrder, got, ids, gotIds };
}

export interface SessionConformanceContext {
  /** Same meaning as for EventStores: equal values share data, different values are isolated. */
  shared: string;
  ttlMs: number;
  now: () => number;
}

export type MakeSessionStore = (ctx: SessionConformanceContext) => SessionStore | Promise<SessionStore>;

/**
 * Register conformance cases for a SessionStore. Set `clockDrivesExpiry: false` for backends whose expiry
 * runs on the server clock (real Redis `PX`): the expiry case then uses a 300 ms TTL and a short real wait
 * instead of the fake clock.
 */
export function runSessionStoreConformance(name: string, make: MakeSessionStore, opts: { clockDrivesExpiry?: boolean } = {}): void {
  const run = crypto.randomUUID().slice(0, 8);
  let n = 0;
  const shared = (label: string) => `${label}-${run}-${++n}`;
  const record = (sessionId: string, at: number): SessionRecord => ({
    sessionId,
    protocolVersion: '2025-11-25',
    clientCapabilities: { sampling: {}, roots: { listChanged: true } },
    clientInfo: { name: 'conformance-client', version: '1.0.0' },
    createdAt: at,
    lastSeenAt: at,
  });

  describe(`SessionStore conformance: ${name}`, () => {
    it('put / get round-trips the record, unknown IDs are undefined', async () => {
      const clock = fakeClock();
      const store = await make({ shared: shared('rt'), ttlMs: 60_000, now: clock.now });
      try {
        const r = record(`s-${crypto.randomUUID()}`, clock.now());
        await store.put(r);
        assert.deepEqual(await store.get(r.sessionId), r);
        assert.equal(await store.get(`missing-${crypto.randomUUID()}`), undefined);
      } finally {
        await store.close();
      }
    });

    it('a second handle on the same backend sees the record (another instance)', async () => {
      const clock = fakeClock();
      const key = shared('handles');
      const a = await make({ shared: key, ttlMs: 60_000, now: clock.now });
      const b = await make({ shared: key, ttlMs: 60_000, now: clock.now });
      try {
        const r = record(`s-${crypto.randomUUID()}/with/slash`, clock.now());
        await a.put(r);
        assert.deepEqual((await b.get(r.sessionId))?.clientCapabilities, r.clientCapabilities);
      } finally {
        await a.close();
        await b.close();
      }
    });

    it('touch() moves lastSeenAt, delete() forgets the session', async () => {
      const clock = fakeClock();
      const store = await make({ shared: shared('touch'), ttlMs: 60_000, now: clock.now });
      try {
        const r = record(`s-${crypto.randomUUID()}`, clock.now());
        await store.put(r);
        clock.advance(1000);
        await store.touch(r.sessionId);
        assert.equal((await store.get(r.sessionId))?.lastSeenAt, r.lastSeenAt + 1000);
        await store.touch(`missing-${crypto.randomUUID()}`); // 모르는 ID 는 조용히 무시
        await store.delete(r.sessionId);
        assert.equal(await store.get(r.sessionId), undefined);
      } finally {
        await store.close();
      }
    });

    if (opts.clockDrivesExpiry === false) {
      // 서버 시계로 만료되는 백엔드(실제 Redis PX): 짧은 ttl 을 주고 실제로 조금 기다린다
      it('forgets sessions not seen for ttlMs (server clock, short real wait)', async () => {
        const ttlMs = 300;
        const store = await make({ shared: shared('ttl'), ttlMs, now: Date.now });
        try {
          const r = record(`s-${crypto.randomUUID()}`, Date.now());
          await store.put(r);
          assert.ok(await store.get(r.sessionId), 'alive right after put');
          await new Promise((resolve) => setTimeout(resolve, ttlMs * 3));
          assert.equal(await store.get(r.sessionId), undefined, 'expired');
        } finally {
          await store.close();
        }
      });
      return;
    }

    it('forgets sessions not seen for ttlMs (fake clock)', async () => {
      const clock = fakeClock();
      const store = await make({ shared: shared('ttl'), ttlMs: 60_000, now: clock.now });
      try {
        const r = record(`s-${crypto.randomUUID()}`, clock.now());
        await store.put(r);
        clock.advance(30_000);
        assert.ok(await store.get(r.sessionId), 'still alive halfway');
        clock.advance(60_001);
        assert.equal(await store.get(r.sessionId), undefined, 'expired');
      } finally {
        await store.close();
      }
    });
  });
}

/**
 * Register the conformance cases for one store implementation.
 *
 * ```ts
 * import { runEventStoreConformance } from 'mcp-streamstore/conformance';
 * runEventStoreConformance('my-dynamo-store', ({ shared, retention }) => new MyStore({ table: shared, ...retention }));
 * ```
 */
export function runEventStoreConformance(name: string, makeStore: MakeStore, opts: ConformanceOptions = {}): void {
  const sameMs = opts.sameMsCount ?? 1000;
  const run = crypto.randomUUID().slice(0, 8);
  let n = 0;
  const shared = (label: string) => `${label}-${run}-${++n}`;
  // 각 사례가 끝나면 연 저장소를 닫는다(파일 핸들·연결 누수 방지)
  async function withStores<T>(fn: (open: (ctx: ConformanceContext) => Promise<StreamStore>) => Promise<T>): Promise<T> {
    const opened: StreamStore[] = [];
    try {
      return await fn(async (ctx) => {
        const s = await makeStore(ctx);
        opened.push(s);
        return s;
      });
    } finally {
      for (const s of opened) await s.close().catch(() => undefined);
    }
  }

  describe(`EventStore conformance: ${name}`, () => {
    it(`replays ${sameMs} events stored in the same millisecond in order`, () =>
      withStores(async (open) => {
        const clock = fakeClock(); // 시계를 멈춰 모든 이벤트가 같은 ms 에 저장되게 한다
        const store = await open({ shared: shared('order'), retention: { now: clock.now } });
        const { inOrder, got } = await checkSameMsOrdering(store, sameMs);
        assert.equal(got.length, sameMs - 1, 'every later event is replayed');
        assert.ok(inOrder, `events must replay in storage order; first ten: ${got.slice(0, 10).join(',')}`);
      }));

    it('keeps streams apart and returns the stream ID from replay', () =>
      withStores(async (open) => {
        const store = await open({ shared: shared('streams'), retention: {} });
        const a = `a-${crypto.randomUUID()}`;
        const b = `b-${crypto.randomUUID()}`;
        const a1 = await store.storeEvent(a, note(1));
        await store.storeEvent(b, note(100));
        await store.storeEvent(a, note(2));
        await store.storeEvent(b, note(101));
        await store.storeEvent(a, note(3));
        assert.equal(await store.getStreamIdForEventId(a1), a);
        const got: number[] = [];
        const sid = await store.replayEventsAfter(a1, { send: async (_id, m) => void got.push(m.params.data) });
        assert.equal(sid, a);
        assert.deepEqual(got, [2, 3]);
      }));

    it('keeps sessions apart through forSession (shared "_GET_stream" name)', () =>
      withStores(async (open) => {
        const store = await open({ shared: shared('sessions'), retention: {} });
        const x = store.forSession(`x-${crypto.randomUUID()}`);
        const y = store.forSession(`y-${crypto.randomUUID()}`);
        const x1 = await x.storeEvent('_GET_stream', note(1));
        const y1 = await y.storeEvent('_GET_stream', note(50));
        await x.storeEvent('_GET_stream', note(2));
        await y.storeEvent('_GET_stream', note(51));
        const got: number[] = [];
        const sid = await x.replayEventsAfter(x1, { send: async (_id, m) => void got.push(m.params.data) });
        assert.equal(sid, '_GET_stream', 'the transport sees its own plain stream ID');
        assert.deepEqual(got, [2], 'only X events are replayed into X');
        assert.equal(await x.getStreamIdForEventId(y1), undefined, "Y's event ID is unknown to X");
        let sent = 0;
        assert.equal(await x.replayEventsAfter(y1, { send: async () => void sent++ }), '');
        assert.equal(sent, 0);
      }));

    it("treats unknown event IDs as unknown (undefined / '')", () =>
      withStores(async (open) => {
        const store = await open({ shared: shared('unknown'), retention: {} });
        await store.storeEvent('s', note(1));
        for (const bogus of ['', 'nope', '999999999', '0', '-1', 's_123_abc', 'x~1-0', '%E0%A4%A~1-0']) {
          assert.equal(await store.getStreamIdForEventId(bogus), undefined, `getStreamIdForEventId(${JSON.stringify(bogus)})`);
          let sent = 0;
          assert.equal(await store.replayEventsAfter(bogus, { send: async () => void sent++ }), '');
          assert.equal(sent, 0);
        }
      }));

    it('round-trips the empty priming message {}', () =>
      withStores(async (open) => {
        const store = await open({ shared: shared('priming'), retention: {} });
        const s = `p-${crypto.randomUUID()}`;
        const first = await store.storeEvent(s, note(0));
        await store.storeEvent(s, {});
        await store.storeEvent(s, note(1));
        const got: unknown[] = [];
        await store.replayEventsAfter(first, { send: async (_id, m) => void got.push(m) });
        assert.deepEqual(got, [{}, note(1)]);
      }));

    it('prune() removes events older than ttlMs (fake clock)', () =>
      withStores(async (open) => {
        const ttlMs = 60_000;
        const clock = fakeClock();
        const store = await open({ shared: shared('ttl'), retention: { ttlMs, now: clock.now } });
        const s = `t-${crypto.randomUUID()}`;
        const ids = [await store.storeEvent(s, note(1)), await store.storeEvent(s, note(2)), await store.storeEvent(s, note(3))];
        assert.equal(await store.prune(), 0, 'nothing is old yet');
        assert.equal(await store.getStreamIdForEventId(ids[0]!), s, 'fresh events survive prune()');
        // 실제 시계와 가짜 시계의 차이를 넉넉히 넘기도록 ttl + 1분을 앞당긴다
        clock.advance(ttlMs + 60_000);
        assert.ok((await store.prune()) >= 3, 'all three old events are removed');
        for (const id of ids) assert.equal(await store.getStreamIdForEventId(id), undefined);
        assert.equal(await store.replayEventsAfter(ids[0]!, { send: async () => undefined }), '');
      }));

    it('prune() keeps at most maxEventsPerStream newest events per stream', () =>
      withStores(async (open) => {
        const max = 5;
        const store = await open({ shared: shared('max'), retention: { maxEventsPerStream: max } });
        const s = `m-${crypto.randomUUID()}`;
        const ids: string[] = [];
        for (let i = 0; i < max * 3; i++) ids.push(await store.storeEvent(s, note(i)));
        await store.prune();
        assert.equal(await store.getStreamIdForEventId(ids[0]!), undefined, 'the oldest event is gone');
        const newest = ids.at(-1)!;
        assert.equal(await store.getStreamIdForEventId(newest), s, 'the newest event stays');
        // 남은 개수 확인: 저장한 ID 하나하나가 아직 알려진 ID 인지 센다
        let kept = 0;
        for (const id of ids) {
          if ((await store.getStreamIdForEventId(id)) === s) kept++;
        }
        assert.ok(kept <= max, `at most ${max} events remain (found ${kept})`);
        assert.ok(kept >= 1);
      }));

    it('deleteSession() removes only that session', () =>
      withStores(async (open) => {
        const store = await open({ shared: shared('delete'), retention: {} });
        const xs = `x-${crypto.randomUUID()}`;
        const x = store.forSession(xs);
        const y = store.forSession(`y-${crypto.randomUUID()}`);
        const x1 = await x.storeEvent('_GET_stream', note(1));
        const y1 = await y.storeEvent('_GET_stream', note(2));
        await store.deleteSession(xs);
        assert.equal(await x.getStreamIdForEventId(x1), undefined);
        assert.equal(await y.getStreamIdForEventId(y1), '_GET_stream');
      }));

    it('two handles on the same backend see each other (instance A writes, B reads)', () =>
      withStores(async (open) => {
        const key = shared('handles');
        const a = await open({ shared: key, retention: {} });
        const b = await open({ shared: key, retention: {} });
        const s = `h-${crypto.randomUUID()}`;
        const first = await a.storeEvent(s, note(1));
        await a.storeEvent(s, note(2));
        const got: number[] = [];
        assert.equal(await b.replayEventsAfter(first, { send: async (_id, m) => void got.push(m.params.data) }), s);
        assert.deepEqual(got, [2]);
      }));

    if (opts.twoInstance !== false) {
      it('resumes on instance B after disconnecting from instance A (SDK v1 transport)', async (t) => {
        if (!(await hasSdk('v1'))) {
          t.skip('@modelcontextprotocol/sdk is not installed');
          return;
        }
        await withStores(async (open) => {
          const key = shared('two-instance');
          const stores = { A: await open({ shared: key, retention: {} }), B: await open({ shared: key, retention: {} }) };
          const r = await runResumeScenario({ sdk: 'v1', eventStore: (inst) => stores[inst] });
          const attempt = r.attempts[0]!;
          assert.equal(attempt.status, 200, attempt.body);
          assert.deepEqual(
            attempt.replayed.map((e) => e.kind),
            ['notifications/message', 'notifications/message', 'result'],
            'the 2 missed notifications and the result are replayed in order',
          );
        });
      });
    }
  });
}
