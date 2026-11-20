// AC-2: A 에서 첫 이벤트 뒤 끊고 B 로 Last-Event-ID 재연결.
// 공유 저장소(SQLite·가짜 Redis)는 놓친 알림 2개 + 결과 1개를 순서대로 재생하고,
// 인스턴스마다 따로 둔 SDK 예제 메모리 저장소는 200·0개(조용한 실패)를 돌려준다. SDK v1·v2 양쪽에서.
import assert from 'node:assert/strict';
import { after, describe, it } from 'node:test';
import { join } from 'node:path';
import { loadSdkExampleStore, runResumeScenario, type ResumeScenarioResult, type SdkFlavor } from '../src/conformance/two-instance.js';
import { MemoryEventStore, RedisEventStore, SqliteEventStore, type StreamStore } from '../src/index.js';
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
