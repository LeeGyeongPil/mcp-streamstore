// AC-3: SDK 는 모든 세션의 독립 GET 스트림을 "_GET_stream" 이라고 부른다.
// 범위 없이 공유한 저장소에서는 세션 Y 의 알림이 X 의 재개에 섞여 재생되고(재현),
// forSession(sid) 를 쓰면 X 의 이벤트만 재생되며 Y 의 이벤트 ID 로 X 를 재개하면 400 이다.
import assert from 'node:assert/strict';
import { after, describe, it } from 'node:test';
import { join } from 'node:path';
import { runLeakScenario, type SdkFlavor } from '../src/conformance/two-instance.js';
import { MemoryEventStore, SqliteEventStore, parseScopeKey, scopeKey } from '../src/index.js';
import { tempDir } from './helpers.js';

const tmp = tempDir('scope');
const stores: Array<{ close: () => Promise<void> }> = [];
after(async () => {
  for (const s of stores) await s.close();
  tmp.cleanup();
});

for (const sdk of ['v1', 'v2'] as SdkFlavor[]) {
  describe(`session scoping, SDK ${sdk}`, () => {
    it('unscoped shared store: Y notifications leak into X resume (reproduced)', async () => {
      const store = new SqliteEventStore({ path: join(tmp.dir, `leak-${sdk}.db`) });
      stores.push(store);
      const r = await runLeakScenario({ sdk, store, scoped: false });
      assert.deepEqual(r.replayedIntoX, ['y1', 'y2-private-to-Y', 'x2']);
      assert.equal(r.crossSessionStatus, 200, "X can even resume from Y's event ID");
    });

    it('forSession(sid): only X events replay, Y event ID is rejected with 400', async () => {
      const store = new SqliteEventStore({ path: join(tmp.dir, `scoped-${sdk}.db`) });
      stores.push(store);
      const r = await runLeakScenario({ sdk, store, scoped: true });
      assert.deepEqual(r.replayedIntoX, ['x2']);
      assert.equal(r.crossSessionStatus, 400);
    });

    it('forSession(sid) also isolates sessions on the memory store', async () => {
      const store = new MemoryEventStore();
      stores.push(store);
      const r = await runLeakScenario({ sdk, store, scoped: true });
      assert.deepEqual(r.replayedIntoX, ['x2']);
      assert.equal(r.crossSessionStatus, 400);
    });
  });
}

describe('scope keys', () => {
  it('round-trip session IDs that contain "/" and other characters', () => {
    for (const sid of ['abc', 'a/b/c', 'ü 한글 ~!*', '%2F']) {
      const key = scopeKey(sid, '_GET_stream');
      assert.deepEqual(parseScopeKey(key), { sessionId: sid, streamId: '_GET_stream' });
    }
  });
  it('unscoped keys and bad encodings parse as undefined', () => {
    assert.equal(parseScopeKey('_GET_stream'), undefined);
    assert.equal(parseScopeKey('/x'), undefined);
    assert.equal(parseScopeKey('%E0%A4%A/x'), undefined);
  });
  it('forSession requires a session ID', () => {
    assert.throws(() => new MemoryEventStore().forSession(''), TypeError);
  });
});
