// AC-2: A 에서 첫 이벤트 뒤 끊고 B 로 Last-Event-ID 재연결.
// 공유 저장소(SQLite·가짜 Redis)는 놓친 알림 2개 + 결과 1개를 순서대로 재생하고,
// 인스턴스마다 따로 둔 SDK 예제 메모리 저장소는 200·0개(조용한 실패)를 돌려준다. SDK v1·v2 양쪽에서.
import assert from 'node:assert/strict';
import { after, describe, it } from 'node:test';
import { join } from 'node:path';
import { loadSdkExampleStore, runResumeScenario, type ResumeScenarioResult, type SdkFlavor } from '../src/conformance/two-instance.js';
import { MemoryEventStore, RedisEventStore, RedisSessionStore, SqliteEventStore, type RedisCommand, type StreamStore } from '../src/index.js';
import { createFakeRedis } from '../src/testing/fake-redis.js';
import { tempDir } from './helpers.js';

const tmp = tempDir('two-instance');
const opened: StreamStore[] = [];
after(async () => {
  for (const s of opened) await s.close();
  tmp.cleanup();
});
const keep = <T extends StreamStore>(s: T): T => {
  opened.push(s);
  return s;
};

const MISSED = ['notifications/message', 'notifications/message', 'result'];

// 가짜 Redis 명령에 왕복 지연을 넣는 래퍼(새 의존성 없음).
// 실제 Redis 에서만 보였던 경쟁 상태(시나리오가 tools/call 결과 대신 initialize 결과를 보고 너무 일찍 B 로 넘어감)를
// 필수 CI(서비스 없음)에서도 잡기 위해 쓴다. 지연은 setTimeout 이라 이벤트 루프가 그 사이 다른 일을 처리한다.
//  - reads: XRANGE·GET 등 읽기 지연, writes: XADD·PEXPIRE·SET 쓰기 지연.
//  - 쓰기만 느리게 하면 "A 가 아직 도구 결과를 쓰는 중인데 B 가 벌써 재생을 읽는" 순간이 확실히 생긴다.
//    (모든 명령에 같은 지연을 주면 B 의 재생 루프가 A 의 쓰기를 우연히 따라잡아 결함이 숨을 수 있다.)
const WRITES = new Set(['XADD', 'PEXPIRE', 'SET', 'DEL', 'XTRIM']);
function delayed(command: RedisCommand, ms: { reads: number; writes: number }): RedisCommand {
  return async (args) => {
    const wait = WRITES.has((args[0] ?? '').toUpperCase()) ? ms.writes : ms.reads;
    await new Promise((resolve) => setTimeout(resolve, wait));
    return command(args);
  };
}

function assertReplayedInOrder(r: ResumeScenarioResult, numeric: boolean) {
  const a = r.attempts[0]!;
  assert.equal(a.status, 200, a.body);
  assert.deepEqual(a.replayed.map((e) => e.kind), MISSED, 'the 2 missed notifications and the result, in order');
  assert.ok(!a.replayed.some((e) => e.id === r.resumeFrom), 'the event the client already had is not sent again');
  if (numeric) {
    const ids = a.replayed.map((e) => Number(e.id));
    assert.ok(ids.every((id, i) => id > Number(r.resumeFrom) && (i === 0 || id > ids[i - 1]!)), `ids increase: ${ids}`);
  }
}

for (const sdk of ['v1', 'v2'] as SdkFlavor[]) {
  describe(`two instances, SDK ${sdk} WebStandard transport`, () => {
    it('shared SQLite file: B replays what the client missed on A', async () => {
      const path = join(tmp.dir, `shared-${sdk}.db`);
      // 인스턴스마다 같은 파일을 따로 연다(다른 프로세스와 같은 조건)
      const r = await runResumeScenario({ sdk, eventStore: () => keep(new SqliteEventStore({ path })) });
      assert.equal(r.seenOnA.length, 1, 'the client saw exactly one event on A before disconnecting');
      assert.equal(r.seenOnA[0]!.kind, 'notifications/message');
      assertReplayedInOrder(r, true);
    });

    it('shared practice fake (mock) Redis: B replays what the client missed on A', async () => {
      const fake = createFakeRedis();
      const r = await runResumeScenario({ sdk, eventStore: () => keep(new RedisEventStore({ command: fake.command })) });
      assertReplayedInOrder(r, false);
    });

    it('delayed practice fake (mock) Redis (3 ms per command): B still replays what the client missed', async () => {
      const fake = createFakeRedis();
      const command = delayed(fake.command, { reads: 3, writes: 3 });
      const r = await runResumeScenario({ sdk, eventStore: () => keep(new RedisEventStore({ command })) });
      assertReplayedInOrder(r, false);
    });

    it('slow-write practice fake (mock) Redis, stateful + rehydrate: the replay waits for the tools/call result', async () => {
      // 세션형에서는 initialize(id 1) 응답도 "result" 로 저장된다. 시나리오가 tools/call(id 2) 결과를 기다리지 않으면
      // 쓰기가 느릴 때 B 가 놓친 이벤트를 다 못 받는다(빌드 중 실제 Redis 에서 발견한 결함의 회귀 시험 —
      // 고치기 전 조건으로 되돌리면 이 시험이 200 + 0개로 실패함을 확인했다).
      const fake = createFakeRedis();
      const command = delayed(fake.command, { reads: 0, writes: 10 });
      const r = await runResumeScenario({
        sdk,
        stateful: true,
        sessions: new RedisSessionStore({ command }),
        rehydrateOnB: true,
        eventStore: (_inst, sid) => keep(new RedisEventStore({ command })).forSession(sid!),
      });
      assert.deepEqual(r.attempts.map((a) => a.status), [400, 200]);
      assert.deepEqual(r.attempts[1]!.replayed.map((e) => e.kind), MISSED, 'the 2 missed notifications and the result, in order');
    });

    it('SDK example InMemoryEventStore per instance: 200 with 0 events (silent failure)', async () => {
      const Example = await loadSdkExampleStore();
      const r = await runResumeScenario({ sdk, eventStore: () => new Example() });
      const a = r.attempts[0]!;
      assert.equal(a.status, 200);
      assert.deepEqual(a.replayed, []);
    });

    it('MemoryEventStore per instance: 400 "Invalid event ID format" (loud failure)', async () => {
      const r = await runResumeScenario({ sdk, eventStore: () => keep(new MemoryEventStore()) });
      const a = r.attempts[0]!;
      assert.equal(a.status, 400);
      assert.match(a.body ?? '', /Invalid event ID format/);
    });
  });
}
