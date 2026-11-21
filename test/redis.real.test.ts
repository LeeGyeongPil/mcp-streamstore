// AC-5: 실제 Redis(6.2+) 에서 RedisEventStore·RedisSessionStore 적합성, 두 인스턴스 재개, RedisServerEventBus 전달.
// REDIS_URL 이 없으면 건너뜀을 분명히 표시하고 통과한다(로컬 샌드박스·기본 `npm test`).
// CI 에서는 GitHub Actions 의 redis:7 서비스 컨테이너로 `REDIS_URL=redis://127.0.0.1:6379 npm run test:redis` 를 돈다
// (localhost 대신 IPv4 리터럴 — 러너에서 localhost 가 ::1 로 먼저 풀려도 영향 없게).
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, describe, it } from 'node:test';
import { runEventStoreConformance, runSessionStoreConformance } from '../src/conformance/index.js';
import { runResumeScenario, type SdkFlavor } from '../src/conformance/two-instance.js';
import {
  RedisEventStore,
  RedisServerEventBus,
  RedisSessionStore,
  nodeRedisCommand,
  nodeRedisPubSub,
  type RedisCommand,
} from '../src/index.js';
import { connectFailureMessage } from '../src/cli.js';
import { globEscape } from '../src/stores/redis.js';
import { collectListen, listenRequest } from './helpers.js';

const url = process.env.REDIS_URL;

if (!url) {
  // TAP 출력에 "# SKIP skipped: REDIS_URL not set" 으로 남는다
  it('real Redis suite', { skip: 'skipped: REDIS_URL not set' }, () => undefined);
} else {
  const { createClient } = await import('redis');
  const client = createClient({ url, socket: { connectTimeout: 3000, reconnectStrategy: (n: number) => (n >= 2 ? new Error('gave up reconnecting') : 200 * (n + 1)) } });
  // 연결 실패 시 재시도 포기 표시 대신 실제 원인(ECONNREFUSED 등)을 한 줄로 남긴다(CI 로그에서 바로 보이게)
  let lastError: Error | undefined;
  client.on('error', (e: Error) => {
    lastError = e;
  });
  try {
    await client.connect();
  } catch (e) {
    client.destroy();
    throw new Error(`real Redis suite: could not connect to REDIS_URL (${new URL(url).host}): ${connectFailureMessage(lastError, e)}`);
  }
  const command: RedisCommand = nodeRedisCommand(client);
  // 시험마다 고유 접두사 → 같은 Redis 를 다른 작업과 나눠 써도 섞이지 않고, 끝나면 이 접두사만 지운다
  const prefix = `mcp-streamstore-test:${randomUUID()}:`;
  const subscribers: Array<{ close: () => Promise<void> }> = [];

  after(async () => {
    let cursor = '0';
    do {
      const [next, keys] = (await command(['SCAN', cursor, 'MATCH', `${globEscape(prefix)}*`, 'COUNT', '500'])) as [string, string[]];
      if (keys.length > 0) await command(['DEL', ...keys]);
      cursor = String(next);
    } while (cursor !== '0');
    for (const s of subscribers) await s.close().catch(() => undefined);
    await client.close();
  });

  describe('real Redis: server check', () => {
    it('answers PING and supports exclusive XRANGE (Redis >= 6.2)', async () => {
      assert.equal(await command(['PING']), 'PONG');
      const key = `${prefix}probe`;
      const first = String(await command(['XADD', key, '*', 'm', '1']));
      await command(['XADD', key, '*', 'm', '2']);
      const after = (await command(['XRANGE', key, `(${first}`, '+'])) as unknown[];
      assert.equal(after.length, 1);
    });
  });

  runEventStoreConformance('RedisEventStore (real Redis)', ({ shared, retention }) => new RedisEventStore({ command, prefix: `${prefix}${shared}:`, ...retention }));

  runSessionStoreConformance(
    'RedisSessionStore (real Redis)',
    ({ shared, ttlMs, now }) => new RedisSessionStore({ command, prefix: `${prefix}${shared}:`, ttlMs, now }),
    { clockDrivesExpiry: false }, // 만료는 Redis 서버 시계(PX)가 맡는다
  );

  for (const sdk of ['v1', 'v2'] as SdkFlavor[]) {
    describe(`real Redis: two instances, SDK ${sdk}`, () => {
      it('B replays the 2 missed notifications and the result', async () => {
        const p = `${prefix}resume-${sdk}:`;
        const r = await runResumeScenario({ sdk, eventStore: () => new RedisEventStore({ command, prefix: p }) });
        assert.equal(r.attempts[0]!.status, 200, r.attempts[0]!.body);
        assert.deepEqual(r.attempts[0]!.replayed.map((e) => e.kind), ['notifications/message', 'notifications/message', 'result']);
      });

      it('stateful: 400 before rehydrate, 200 after (RedisSessionStore)', async () => {
        const p = `${prefix}stateful-${sdk}:`;
        const caps = { sampling: {}, roots: { listChanged: true } };
        const r = await runResumeScenario({
          sdk,
          stateful: true,
          sessions: new RedisSessionStore({ command, prefix: p }),
          rehydrateOnB: true,
          clientCapabilities: caps,
          eventStore: (_inst, sid) => new RedisEventStore({ command, prefix: p }).forSession(sid!),
        });
        assert.deepEqual(r.attempts.map((a) => a.status), [400, 200]);
        assert.equal(r.attempts[1]!.replayed.length, 3);
        assert.deepEqual(r.clientCapabilitiesOnB, caps);
      });
    });
  }

  describe('real Redis: RedisServerEventBus across two createMcpHandler nodes', () => {
    it('notify.toolsChanged() on X arrives exactly once on Y and once on X', async () => {
      const { McpServer, createMcpHandler } = await import('@modelcontextprotocol/server');
      const channel = `${prefix}events`;
      // 구독한 연결은 다른 명령을 못 보내므로 노드마다 전용 연결을 하나씩 만든다
      const makeBus = async (instanceId: string) => {
        const sub = client.duplicate();
        sub.on('error', () => undefined);
        await sub.connect();
        subscribers.push({ close: () => sub.close() });
        const bus = new RedisServerEventBus({ ...nodeRedisPubSub(client, sub), channel, instanceId });
        await bus.ready();
        return bus;
      };
      const busX = await makeBus('node-x');
      const busY = await makeBus('node-y');
      const node = (name: string, bus: RedisServerEventBus) =>
        createMcpHandler(
          () => {
            const server = new McpServer({ name, version: '1.0.0' }, { capabilities: { tools: { listChanged: true }, prompts: { listChanged: true } } });
            server.registerTool('ping', { description: 'answers pong' }, async () => ({ content: [{ type: 'text', text: 'pong' }] }));
            return server;
          },
          { bus, keepAliveMs: 0 },
        );
      const X = node('x', busX);
      const Y = node('y', busY);
      const onX = collectListen(await X.fetch(listenRequest('listen-x')));
      const onY = collectListen(await Y.fetch(listenRequest('listen-y')));
      const TOOLS = 'notifications/tools/list_changed';
      const PROMPTS = 'notifications/prompts/list_changed';
      const count = (m: string[], method: string) => m.filter((x) => x === method).length;
      try {
        await onX.until((m) => m.includes('notifications/subscriptions/acknowledged'));
        await onY.until((m) => m.includes('notifications/subscriptions/acknowledged'));
        X.notify.toolsChanged();
        await onY.until((m) => count(m, TOOLS) >= 1);
        // 끝 표시: Y 가 발행한 이벤트가 X 에 도착했다면, 그보다 먼저 발행된 X 의 자기 메아리도 이미 도착했다(채널 순서)
        Y.notify.promptsChanged();
        await onX.until((m) => count(m, PROMPTS) >= 1);
        await onY.until((m) => count(m, PROMPTS) >= 1);
        assert.equal(count(onX.methods, TOOLS), 1, `X stream: ${onX.methods}`);
        assert.equal(count(onY.methods, TOOLS), 1, `Y stream: ${onY.methods}`);
      } finally {
        await onX.close();
        await onY.close();
        await X.close();
        await Y.close();
        await busX.close();
        await busY.close();
      }
    });
  });
}
