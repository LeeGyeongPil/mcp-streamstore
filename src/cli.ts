#!/usr/bin/env node
// 데모 CLI: mcp-streamstore demo [--backend all|memory|sqlite|redis] [--lang en|ko]
// 한 프로세스에 인스턴스 A·B 를 띄워 "A 에서 끊기고 B 로 재연결" 결과를 저장소별 표로 보여 준다.
// REDIS_URL 이 없으면 Redis 행은 연습용 가짜(mock) Redis 로 돈다 — 그래도 실제 SDK 전송과 실제 RedisEventStore 코드를 거친다.
import { mkdtempSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import { hasSdk, loadSdkExampleStore, runLeakScenario, runResumeScenario, type ResumeAttempt } from './conformance/two-instance.js';
import { isLang, resolveLang, translator, type Lang, type MessageKey } from './i18n/index.js';
import { nodeRedisCommand } from './redis-clients.js';
import { installedSdkVersion } from './sessions/rehydrate.js';
import { MemorySessionStore, RedisSessionStore, SqliteSessionStore } from './sessions/stores.js';
import { MemoryEventStore } from './stores/memory.js';
import { RedisEventStore, globEscape } from './stores/redis.js';
import { SqliteEventStore } from './stores/sqlite.js';
import { createFakeRedis } from './testing/fake-redis.js';
import type { RedisCommand, SessionStore, StreamStore } from './types.js';

type Backend = 'all' | 'memory' | 'sqlite' | 'redis';
const BACKENDS: Backend[] = ['all', 'memory', 'sqlite', 'redis'];

export interface CliIo {
  env: NodeJS.ProcessEnv;
  out: (line: string) => void;
  err: (line: string) => void;
}

interface Parsed {
  command?: string;
  backend: Backend;
  lang?: string;
  help: boolean;
  error?: { key: MessageKey; vars: Record<string, string> };
}

function parseArgs(argv: string[]): Parsed {
  const p: Parsed = { backend: 'all', help: false };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    const [flag, inline] = arg.startsWith('--') && arg.includes('=') ? [arg.slice(0, arg.indexOf('=')), arg.slice(arg.indexOf('=') + 1)] : [arg, undefined];
    const value = () => inline ?? argv[++i] ?? '';
    if (flag === '-h' || flag === '--help') p.help = true;
    else if (flag === '--lang') p.lang = value();
    else if (flag === '--backend') {
      const b = value();
      if ((BACKENDS as string[]).includes(b)) p.backend = b as Backend;
      else p.error ??= { key: 'error.badBackend', vars: { value: b } };
    } else if (!flag.startsWith('-') && p.command === undefined) p.command = flag;
    else p.error ??= { key: flag.startsWith('-') ? 'error.unknownOption' : 'error.unknownCommand', vars: { opt: flag, cmd: flag } };
  }
  if (p.command !== undefined && p.command !== 'demo') p.error ??= { key: 'error.unknownCommand', vars: { cmd: p.command } };
  return p;
}

/** Run the CLI and resolve with the exit code. */
export async function runCli(argv: string[], io: CliIo): Promise<number> {
  const args = parseArgs(argv);
  // 언어부터 정한다(잘못된 --lang 값도 영어로 오류를 보여 줄 수 있게)
  const lang: Lang = args.lang !== undefined && isLang(args.lang) ? args.lang : resolveLang(undefined, io.env);
  const t = translator(lang);
  const help = () => {
    for (const k of ['help.usage', 'help.summary', '', 'help.options', 'help.backend', 'help.lang', 'help.help', '', 'help.env'] as const) {
      io.out(k === '' ? '' : t(k));
    }
  };
  if (args.lang !== undefined && !isLang(args.lang)) {
    io.err(t('error.badLang', { value: args.lang }));
    help();
    return 2;
  }
  if (args.error) {
    io.err(t(args.error.key, args.error.vars));
    help();
    return 2;
  }
  if (args.help || args.command === undefined) {
    help();
    return args.help ? 0 : 1;
  }
  if (!(await hasSdk('v1'))) {
    io.err(t('error.noSdk'));
    return 2;
  }
  try {
    return await demo(args.backend, io, lang);
  } catch (e) {
    io.err(t('error.failed', { message: (e as Error).message }));
    return 1;
  }
}

// ------------------------------------------------------------------ 데모 본체

interface RedisHandle {
  command: RedisCommand;
  real: boolean;
  url?: string;
  close: () => Promise<void>;
}

/** Connection limits so an unreachable REDIS_URL fails fast instead of retrying forever. */
export const CONNECT_TIMEOUT_MS = 3000;
export const MAX_CONNECT_RETRIES = 2;
export function boundedReconnect(retries: number): number | Error {
  return retries >= MAX_CONNECT_RETRIES ? new Error('gave up reconnecting') : 200 * (retries + 1);
}

async function openRedis(io: CliIo, t: ReturnType<typeof translator>): Promise<RedisHandle | number> {
  const url = io.env.REDIS_URL;
  if (!url) {
    const fake = createFakeRedis(); // 연습용 가짜(mock) Redis
    return { command: fake.command, real: false, close: async () => undefined };
  }
  let redis: any;
  try {
    const specifier = 'redis';
    redis = await import(specifier); // 사용자가 설치한 node-redis (이 라이브러리의 의존성이 아니다)
  } catch {
    io.err(t('error.noRedisClient'));
    return 2;
  }
  // 닿지 않는 주소에서 영원히 재시도하지 않도록 연결 제한시간과 재시도 상한을 둔다
  const client = redis.createClient({ url, socket: { connectTimeout: CONNECT_TIMEOUT_MS, reconnectStrategy: boundedReconnect } });
  client.on('error', () => undefined); // 연결 오류는 아래 connect() 가 알려 준다
  try {
    await client.connect();
  } catch (e) {
    io.err(t('error.redisConnect', { message: (e as Error).message }));
    client.destroy();
    return 2;
  }
  return { command: nodeRedisCommand(client), real: true, url: redactUrl(url), close: () => client.close() };
}

function redactUrl(url: string): string {
  try {
    const u = new URL(url);
    if (u.password) u.password = '***';
    return u.toString();
  } catch {
    return 'REDIS_URL';
  }
}

async function demo(backend: Backend, io: CliIo, lang: Lang): Promise<number> {
  const t = translator(lang);
  const started = Date.now();
  const want = (b: Exclude<Backend, 'all'>) => backend === 'all' || backend === b;
  const dir = mkdtempSync(join(tmpdir(), 'mcp-streamstore-demo-'));
  const dbPath = join(dir, 'events.db');
  const opened: Array<{ close: () => Promise<void> }> = [];
  const track = <T extends { close: () => Promise<void> }>(x: T): T => {
    opened.push(x);
    return x;
  };
  let redis: RedisHandle | undefined;
  const redisPrefix = `mcp-streamstore-demo:${randomUUID()}:`;

  try {
    io.out(t('demo.title'));
    io.out(t('demo.sdk', { version: installedSdkVersion('@modelcontextprotocol/sdk') ?? '?' }));
    if (want('redis')) {
      const r = await openRedis(io, t);
      if (typeof r === 'number') return r;
      redis = r;
      io.out(redis.real ? t('demo.realRedis', { url: redis.url ?? '' }) : t('demo.mockBanner'));
    }
    io.out('');

    // ---- 1) 다른 인스턴스에서 재개
    io.out(t('demo.section.resume'));
    const rows: string[][] = [];
    const resumeRow = (label: string, a: ResumeAttempt, silentIfEmpty: boolean) => {
      let result: string;
      if (a.status !== 200) result = t('demo.result.loud', { error: errorMessage(a.body) });
      else if (a.replayed.length === 0 && silentIfEmpty) result = t('demo.result.silent');
      else result = t('demo.result.kinds', { count: a.replayed.length, kinds: a.replayed.map((e) => shortKind(e.kind)).join(', ') || '-' });
      rows.push([label, String(a.status), result]);
    };
    if (want('memory')) {
      const Example = await loadSdkExampleStore();
      const ex = await runResumeScenario({ sdk: 'v1', eventStore: () => new Example() });
      resumeRow(t('demo.store.example'), ex.attempts[0]!, true);
      const mem = await runResumeScenario({ sdk: 'v1', eventStore: () => track(new MemoryEventStore()) });
      resumeRow(t('demo.store.memory'), mem.attempts[0]!, true);
    }
    if (want('sqlite')) {
      // 인스턴스마다 같은 파일을 새로 연다(다른 프로세스라고 가정)
      const r = await runResumeScenario({ sdk: 'v1', eventStore: () => track(new SqliteEventStore({ path: dbPath })) });
      resumeRow(t('demo.store.sqlite'), r.attempts[0]!, true);
    }
    if (want('redis') && redis) {
      const cmd = redis.command;
      const r = await runResumeScenario({ sdk: 'v1', eventStore: () => track(new RedisEventStore({ command: cmd, prefix: redisPrefix })) });
      resumeRow(redis.real ? t('demo.store.redisReal') : t('demo.store.redisFake'), r.attempts[0]!, true);
    }
    printTable(io, [t('demo.col.store'), t('demo.col.status'), t('demo.col.result')], rows);
    io.out('');

    // ---- 2)·3) 에 쓸 공유 저장소 하나 고르기
    const shared = pickShared(backend, dbPath, redis, redisPrefix, t, track);

    // ---- 2) 세션 섞임
    io.out(t('demo.section.leak'));
    const leakRows: string[][] = [];
    for (const scoped of [false, true]) {
      const store = shared.makeStore();
      const r = await runLeakScenario({ sdk: 'v1', store, scoped });
      const leaked = r.replayedIntoX.some((m) => m.startsWith('y'));
      leakRows.push([
        scoped ? t('demo.leak.scoped') : t('demo.leak.unscoped'),
        (r.replayedIntoX.join(', ') || '-') + (leaked ? `  ${t('demo.leak.leaked')}` : ''),
        t('demo.http', { status: r.crossSessionStatus }),
      ]);
    }
    printTable(io, [t('demo.col.setup'), t('demo.col.received'), t('demo.col.cross')], leakRows, shared.label);
    io.out('');

    // ---- 3) 세션형 세션 다시 세우기
    io.out(t('demo.section.stateful'));
    const sessions = shared.makeSessions();
    const st = await runResumeScenario({
      sdk: 'v1',
      stateful: true,
      sessions,
      rehydrateOnB: true,
      clientCapabilities: { sampling: {}, roots: { listChanged: true } },
      eventStore: (_inst, sid) => shared.makeStore().forSession(sid!),
    });
    const [plain, re] = st.attempts as [ResumeAttempt, ResumeAttempt];
    printTable(
      io,
      [t('demo.col.step'), t('demo.col.status'), t('demo.col.result')],
      [
        [t('demo.stateful.plain'), String(plain.status), plain.status === 200 ? t('demo.result.replayed', { count: plain.replayed.length }) : errorMessage(plain.body)],
        [t('demo.stateful.rehydrated'), String(re.status), t('demo.result.kinds', { count: re.replayed.length, kinds: re.replayed.map((e) => shortKind(e.kind)).join(', ') || '-' })],
      ],
      shared.label,
    );
    io.out(t('demo.stateful.summary', { before: plain.status, after: re.status }));
    const capsOk = JSON.stringify(st.clientCapabilitiesOnB) === JSON.stringify(st.clientCapabilitiesOnA);
    io.out(t('demo.stateful.caps', { value: capsOk ? t('demo.yes') : t('demo.no') }));
    io.out('');
    io.out(t('demo.lesson'));
    io.out(t('demo.done', { ms: Date.now() - started }));
    return 0;
  } finally {
    // 연 저장소·연결·임시 파일을 모두 정리한다
    for (const x of opened.reverse()) await x.close().catch(() => undefined);
    if (redis?.real) await deleteByPrefix(redis.command, redisPrefix).catch(() => undefined);
    await redis?.close().catch(() => undefined);
    rmSync(dir, { recursive: true, force: true });
  }
}

function pickShared(
  backend: Backend,
  dbPath: string,
  redis: RedisHandle | undefined,
  prefix: string,
  t: ReturnType<typeof translator>,
  track: <T extends { close: () => Promise<void> }>(x: T) => T,
): { label: string; makeStore: () => StreamStore; makeSessions: () => SessionStore } {
  if (backend === 'memory') {
    const one = track(new MemoryEventStore());
    return { label: t('demo.store.memoryShared'), makeStore: () => one, makeSessions: () => new MemorySessionStore() };
  }
  if (backend === 'redis' && redis) {
    const command = redis.command;
    return {
      label: redis.real ? t('demo.store.redisReal') : t('demo.store.redisFake'),
      makeStore: () => track(new RedisEventStore({ command, prefix })),
      makeSessions: () => track(new RedisSessionStore({ command, prefix })),
    };
  }
  return {
    label: t('demo.store.sqlite'),
    makeStore: () => track(new SqliteEventStore({ path: dbPath })),
    makeSessions: () => track(new SqliteSessionStore({ path: dbPath })),
  };
}

async function deleteByPrefix(command: RedisCommand, prefix: string): Promise<void> {
  let cursor = '0';
  do {
    const reply = (await command(['SCAN', cursor, 'MATCH', `${globEscape(prefix)}*`, 'COUNT', '200'])) as [string, string[]];
    cursor = String(reply[0]);
    if (reply[1].length > 0) await command(['DEL', ...reply[1]]);
  } while (cursor !== '0');
}

function errorMessage(body: string | undefined): string {
  try {
    return String(JSON.parse(body ?? '').error?.message ?? body);
  } catch {
    return body ?? '';
  }
}

function shortKind(kind: string): string {
  return kind === 'notifications/message' ? 'message' : kind;
}

// 한글·한자는 터미널에서 두 칸을 차지하므로 폭을 따로 센다
function width(s: string): number {
  let w = 0;
  for (const ch of s) {
    const c = ch.codePointAt(0)!;
    w += (c >= 0x1100 && c <= 0x115f) || (c >= 0x2e80 && c <= 0xa4cf) || (c >= 0xac00 && c <= 0xd7a3) || (c >= 0xf900 && c <= 0xfaff) || (c >= 0xff00 && c <= 0xff60) ? 2 : 1;
  }
  return w;
}

function pad(s: string, n: number): string {
  return s + ' '.repeat(Math.max(0, n - width(s)));
}

function printTable(io: CliIo, header: string[], rows: string[][], caption?: string): void {
  if (caption) io.out(`  (${caption})`);
  const cols = header.length;
  const widths = Array.from({ length: cols }, (_, i) => Math.max(width(header[i] ?? ''), ...rows.map((r) => width(r[i] ?? ''))));
  // 마지막 열은 채우지 않는다(줄 끝 공백 방지)
  const line = (cells: string[]) => '  ' + cells.map((c, i) => (i === cols - 1 ? c : pad(c, widths[i]!))).join(' | ').trimEnd();
  io.out(line(header));
  io.out('  ' + widths.map((w, i) => '-'.repeat(i === cols - 1 ? Math.max(w, 3) : w)).join('-+-'));
  for (const r of rows) io.out(line(r));
}

// ------------------------------------------------------------------ 직접 실행될 때만 동작

function isMain(): boolean {
  if (!process.argv[1]) return false;
  try {
    return realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
}

if (isMain()) {
  // `... | head` 처럼 읽는 쪽이 먼저 닫히면(EPIPE) 더 쓰지 않고, 데모는 끝까지 돌아 임시 파일·연결을 정리한다
  let stdoutClosed = false;
  process.stdout.on('error', (e: NodeJS.ErrnoException) => {
    if (e.code === 'EPIPE') stdoutClosed = true;
    else throw e;
  });
  const code = await runCli(process.argv.slice(2), {
    env: process.env,
    out: (l) => {
      if (!stdoutClosed) process.stdout.write(l + '\n');
    },
    err: (l) => process.stderr.write(l + '\n'),
  });
  process.exitCode = code;
}

