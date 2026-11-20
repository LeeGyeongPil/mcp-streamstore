// AC-6: v2 createMcpHandler 두 노드가 가짜 pub/sub 위의 RedisServerEventBus 를 공유할 때,
// X 의 notify.toolsChanged() 가 Y 의 subscriptions/listen 스트림에 정확히 1번, X 자신의 스트림에도 정확히 1번 도착한다.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { McpServer, createMcpHandler } from '@modelcontextprotocol/server';
import { RedisServerEventBus, type ServerEvent } from '../src/index.js';
import { createFakeRedis } from '../src/testing/fake-redis.js';
import { collectListen, listenRequest } from './helpers.js';

const TOOLS = 'notifications/tools/list_changed';
const PROMPTS = 'notifications/prompts/list_changed';
const count = (m: string[], method: string) => m.filter((x) => x === method).length;

function node(name: string, bus: RedisServerEventBus) {
  const factory = () => {
    const server = new McpServer({ name, version: '1.0.0' }, { capabilities: { tools: { listChanged: true }, prompts: { listChanged: true } } });
    server.registerTool('ping', { description: 'answers pong' }, async () => ({ content: [{ type: 'text', text: 'pong' }] }));
    return server;
  };
  return createMcpHandler(factory, { bus, keepAliveMs: 0 });
}

describe('RedisServerEventBus across two createMcpHandler nodes (practice fake pub/sub)', () => {
  it('delivers notify.toolsChanged() from X exactly once on Y and once on X', async () => {
    const fake = createFakeRedis();
    const busX = new RedisServerEventBus({ publish: fake.publish, subscribe: fake.subscribe, instanceId: 'node-x' });
    const busY = new RedisServerEventBus({ publish: fake.publish, subscribe: fake.subscribe, instanceId: 'node-y' });
    await Promise.all([busX.ready(), busY.ready()]); // 시작할 때 구독을 마쳐 두면 첫 이벤트도 놓치지 않는다
    const X = node('x', busX);
    const Y = node('y', busY);
    const onX = collectListen(await X.fetch(listenRequest('listen-x')));
    const onY = collectListen(await Y.fetch(listenRequest('listen-y')));
    try {
      await onX.until((m) => m.includes('notifications/subscriptions/acknowledged'));
      await onY.until((m) => m.includes('notifications/subscriptions/acknowledged'));

      X.notify.toolsChanged();
      await onY.until((m) => count(m, TOOLS) >= 1);
      // 끝 표시: Y 가 발행한 이벤트는 채널에서 X 의 (있다면) 자기 메아리보다 뒤에 온다 → 그 앞까지 세면 중복을 놓치지 않는다
      Y.notify.promptsChanged();
      await onX.until((m) => count(m, PROMPTS) >= 1);
      await onY.until((m) => count(m, PROMPTS) >= 1);

      assert.equal(count(onX.methods.slice(0, onX.methods.indexOf(PROMPTS)), TOOLS), 1, `X stream: ${onX.methods}`);
      assert.equal(count(onY.methods, TOOLS), 1, `Y stream: ${onY.methods}`);
      assert.equal(count(onX.methods, TOOLS), 1, 'no late self-echo on X either');
    } finally {
      await onX.close();
      await onY.close();
      await X.close();
      await Y.close();
      await busX.close();
      await busY.close();
    }
  });

  it('a bus without the instance-ID filter would echo (control): raw subscriber sees our own message', async () => {
    const fake = createFakeRedis();
    const raw: string[] = [];
    const unsub = await fake.subscribe('mcp:server-events', (m) => raw.push(m));
    const bus = new RedisServerEventBus({ publish: fake.publish, subscribe: fake.subscribe, instanceId: 'me' });
    await bus.ready();
    const local: ServerEvent[] = [];
    bus.subscribe((e) => local.push(e));
    bus.publish({ kind: 'tools_list_changed' });
    // 채널에는 내 메시지가 돌아온다(실제 Redis 와 같다). 그래도 로컬 리스너는 한 번만 받는다.
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
    assert.equal(raw.length, 1);
    assert.equal(JSON.parse(raw[0]!).from, 'me');
    assert.equal(local.length, 1);
    await unsub();
    await bus.close();
  });
});

describe('RedisServerEventBus contract', () => {
  it('unsubscribe is idempotent and close() releases the channel', async () => {
    const fake = createFakeRedis();
    let remoteSubs = 0;
    const bus = new RedisServerEventBus({
      publish: fake.publish,
      subscribe: async (channel, onMessage) => {
        remoteSubs++;
        const off = await fake.subscribe(channel, onMessage);
        return async () => {
          remoteSubs--;
          await off();
        };
      },
    });
    const seen: string[] = [];
    const off = bus.subscribe((e) => seen.push(e.kind));
    const off2 = bus.subscribe((e) => seen.push(`2:${e.kind}`));
    await bus.ready();
    assert.equal(remoteSubs, 1, 'one channel subscription per bus, however many listeners');
    assert.equal(bus.listenerCount, 2);
    off();
    off(); // 두 번째 호출도 안전
    assert.equal(bus.listenerCount, 1);
    bus.publish({ kind: 'resource_updated', uri: 'file:///a' });
    assert.deepEqual(seen, ['2:resource_updated']);
    off2();
    await bus.close();
    await bus.close(); // close 도 두 번 안전
    assert.equal(remoteSubs, 0);
    bus.publish({ kind: 'tools_list_changed' }); // 닫힌 뒤 publish 는 아무 일도 하지 않는다
    assert.deepEqual(seen, ['2:resource_updated']);
  });

  it('reports malformed messages and publish failures through onerror, and keeps going', async () => {
    const errors: string[] = [];
    let deliver!: (m: string) => void;
    const bus = new RedisServerEventBus({
      publish: async () => {
        throw new Error('publish down');
      },
      subscribe: async (_c, onMessage) => {
        deliver = onMessage;
        return async () => undefined;
      },
      instanceId: 'me',
      onerror: (e) => errors.push(e.message),
    });
    const got: string[] = [];
    bus.subscribe((e) => got.push(e.kind));
    await bus.ready();
    deliver('not json');
    deliver(JSON.stringify({ v: 1, from: 'other', event: { kind: 'nope' } }));
    deliver(JSON.stringify({ v: 1, from: 'other', event: { kind: 'prompts_list_changed' } }));
    bus.publish({ kind: 'tools_list_changed' });
    await new Promise((r) => setImmediate(r));
    assert.deepEqual(got, ['prompts_list_changed', 'tools_list_changed']);
    assert.equal(errors.length, 3, errors.join(' | '));
    assert.match(errors.join(' '), /publish down/);
    await bus.close();
  });

  it('a throwing listener does not stop delivery to the others', async () => {
    const fake = createFakeRedis();
    const bus = new RedisServerEventBus({ publish: fake.publish, subscribe: fake.subscribe, onerror: () => undefined });
    const got: string[] = [];
    bus.subscribe(() => {
      throw new Error('boom');
    });
    bus.subscribe((e) => got.push(e.kind));
    bus.publish({ kind: 'tools_list_changed' });
    assert.deepEqual(got, ['tools_list_changed']);
    await bus.close();
  });
});
