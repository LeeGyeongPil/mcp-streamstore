// AC-8: 데모 CLI 두 언어. 키 집합이 같고, --lang > MCP_STREAMSTORE_LANG > LC_ALL > LC_MESSAGES > LANG 순으로 고르며,
// 한국어가 아니면 영어. 도움말·데모 출력이 실제로 바뀐다. (AC-7 의 데모 출력도 여기서 확인한다)
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { connectFailureMessage, runCli } from '../src/cli.js';
import { MESSAGES, normalize, resolveLang, translator } from '../src/i18n/index.js';

const run = promisify(execFile);
const CLI = fileURLToPath(new URL('../src/cli.js', import.meta.url));

// 부모 셸의 언어 설정이 섞이지 않도록 언어 변수를 지운 환경
function cleanEnv(extra: Record<string, string> = {}): NodeJS.ProcessEnv {
  const env = { ...process.env };
  for (const k of ['MCP_STREAMSTORE_LANG', 'LC_ALL', 'LC_MESSAGES', 'LANG', 'REDIS_URL']) delete env[k];
  return { ...env, ...extra };
}

async function cli(args: string[], env: NodeJS.ProcessEnv) {
  const out: string[] = [];
  const err: string[] = [];
  const code = await runCli(args, { env, out: (l) => out.push(l), err: (l) => err.push(l) });
  return { code, out: out.join('\n'), err: err.join('\n') };
}

describe('message tables', () => {
  it('en.json and ko.json have the same keys and no empty values', () => {
    const en = Object.keys(MESSAGES.en).sort();
    const ko = Object.keys(MESSAGES.ko).sort();
    assert.deepEqual(ko, en);
    for (const lang of ['en', 'ko'] as const) {
      for (const [k, v] of Object.entries(MESSAGES[lang])) assert.ok(v.trim() !== '', `${lang}.${k} is empty`);
    }
  });

  it('both languages use the same {placeholders} per key', () => {
    const vars = (s: string) => [...s.matchAll(/\{(\w+)\}/g)].map((m) => m[1]).sort();
    for (const k of Object.keys(MESSAGES.en)) assert.deepEqual(vars(MESSAGES.ko[k]!), vars(MESSAGES.en[k]!), k);
  });

  it('translator fills placeholders', () => {
    assert.equal(translator('en')('demo.result.replayed', { count: 3 }), 'replayed 3');
    assert.equal(translator('ko')('demo.result.replayed', { count: 3 }), '재생 3개');
  });
});

describe('language selection order', () => {
  it('--lang wins over the environment', () => {
    assert.equal(resolveLang('en', { MCP_STREAMSTORE_LANG: 'ko', LANG: 'ko_KR.UTF-8' }), 'en');
    assert.equal(resolveLang('ko', { LANG: 'en_US.UTF-8' }), 'ko');
  });
  it('MCP_STREAMSTORE_LANG, then LC_ALL, then LC_MESSAGES, then LANG', () => {
    assert.equal(resolveLang(undefined, { MCP_STREAMSTORE_LANG: 'ko', LC_ALL: 'en_US.UTF-8' }), 'ko');
    assert.equal(resolveLang(undefined, { LC_ALL: 'ko_KR.UTF-8', LANG: 'en_US.UTF-8' }), 'ko');
    assert.equal(resolveLang(undefined, { LC_ALL: 'C', LANG: 'ko_KR.UTF-8' }), 'en');
    assert.equal(resolveLang(undefined, { LC_MESSAGES: 'ko_KR', LANG: 'en_US' }), 'ko');
    assert.equal(resolveLang(undefined, { LANG: 'ko_KR.UTF-8' }), 'ko');
  });
  it('anything that is not Korean falls back to English', () => {
    assert.equal(resolveLang(undefined, {}), 'en');
    assert.equal(resolveLang(undefined, { LANG: 'fr_FR.UTF-8' }), 'en');
    assert.equal(resolveLang(undefined, { LANG: 'kok_IN' }), 'en', 'Konkani is not Korean');
    assert.equal(normalize('ko-KR'), 'ko');
    assert.equal(normalize('KO'), 'ko');
  });
});

describe('demo CLI in both languages', () => {
  it('help: --lang ko prints 사용법, --lang en prints Usage (real process)', async () => {
    const ko = await run(process.execPath, [CLI, 'demo', '--lang', 'ko', '--help'], { env: cleanEnv() });
    assert.match(ko.stdout, /사용법/);
    const en = await run(process.execPath, [CLI, 'demo', '--lang', 'en', '--help'], { env: cleanEnv() });
    assert.match(en.stdout, /Usage/);
    assert.doesNotMatch(en.stdout, /사용법/);
  });

  it('help follows MCP_STREAMSTORE_LANG and LANG=ko_KR.UTF-8 when --lang is absent', async () => {
    const byVar = await run(process.execPath, [CLI, '--help'], { env: cleanEnv({ MCP_STREAMSTORE_LANG: 'ko' }) });
    assert.match(byVar.stdout, /사용법/);
    const byLang = await run(process.execPath, [CLI, '--help'], { env: cleanEnv({ LANG: 'ko_KR.UTF-8' }) });
    assert.match(byLang.stdout, /사용법/);
    const fallback = await run(process.execPath, [CLI, '--help'], { env: cleanEnv({ LANG: 'de_DE.UTF-8' }) });
    assert.match(fallback.stdout, /Usage/);
  });

  it('AC-7: English demo prints replayed 0 / replayed 3, the mock banner and the rehydrate row', async () => {
    const started = Date.now();
    const r = await cli(['demo', '--backend', 'all', '--lang', 'en'], cleanEnv());
    assert.equal(r.code, 0, r.err);
    assert.ok(Date.now() - started < 10_000, 'finishes within 10 s');
    for (const needle of ['replayed 0', 'replayed 3', 'practice fake (mock) Redis', '400 -> 200 after rehydrate', "LEAK: Y's notifications"]) {
      assert.ok(r.out.includes(needle), `missing "${needle}" in:\n${r.out}`);
    }
    assert.doesNotMatch(r.out, /재생/);
  });

  it('Korean demo prints the same table in Korean', async () => {
    const r = await cli(['demo', '--lang', 'ko'], cleanEnv());
    assert.equal(r.code, 0, r.err);
    for (const needle of ['재생 0개', '재생 3개', '연습용 가짜(mock) Redis', 'rehydrate 후 400 -> 200']) {
      assert.ok(r.out.includes(needle), `missing "${needle}" in:\n${r.out}`);
    }
  });

  it('single backends and bad arguments', async () => {
    const mem = await cli(['demo', '--backend', 'memory', '--lang', 'en'], cleanEnv());
    assert.equal(mem.code, 0, mem.err);
    assert.match(mem.out, /replayed 0/);
    assert.doesNotMatch(mem.out, /SqliteEventStore, shared file \|/);
    const redis = await cli(['demo', '--backend=redis', '--lang=en'], cleanEnv());
    assert.equal(redis.code, 0, redis.err);
    assert.match(redis.out, /practice fake \(mock\) Redis/);
    const bad = await cli(['demo', '--backend', 'mongo', '--lang', 'ko'], cleanEnv());
    assert.equal(bad.code, 2);
    assert.match(bad.err, /알 수 없는 저장소: mongo/);
    const badLang = await cli(['demo', '--lang', 'fr'], cleanEnv());
    assert.equal(badLang.code, 2);
    assert.match(badLang.err, /Unknown language: fr/);
    const unknown = await cli(['serve'], cleanEnv());
    assert.equal(unknown.code, 2);
  });
});

describe('unreachable REDIS_URL', () => {
  // 회귀: 닿지 않는 Redis 에서 무한 재시도로 멈추던 문제. 제한시간 안에 오류 메시지와 종료코드 2 로 끝나야 한다.
  for (const [lang, needle] of [['en', 'Could not connect to REDIS_URL'], ['ko', 'REDIS_URL 에 연결하지 못했습니다']] as const) {
    it(`fails fast with a clear ${lang} message`, { timeout: 20000 }, async () => {
      const started = Date.now();
      const r = await cli(['demo', '--backend', 'redis', '--lang', lang], cleanEnv({ REDIS_URL: 'redis://127.0.0.1:1' }));
      assert.equal(r.code, 2);
      assert.ok(r.err.includes(needle), r.err);
      assert.ok(Date.now() - started < 15000, 'took too long');
      // 원인은 재시도 포기 표시가 아니라 실제 소켓 오류여야 하고, 다음 문장 앞에 마침표가 있어야 한다
      assert.doesNotMatch(r.err, /gave up reconnecting/);
      assert.match(r.err, /ECONNREFUSED|connect|refused/i);
      assert.match(r.err, lang === 'en' ? /\. Check / : /\. 서버가 /);
      assert.ok(r.err.includes('REDIS_URL'), 'mentions unsetting REDIS_URL');
      assert.ok(r.err.includes(lang === 'en' ? 'practice fake (mock) Redis' : '연습용 가짜(mock) Redis'), r.err);
    });
  }

  it('connectFailureMessage prefers the real cause and trims the trailing period', () => {
    const refused = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:1'), { code: 'ECONNREFUSED' });
    assert.equal(connectFailureMessage(refused, new Error('gave up reconnecting')), 'connect ECONNREFUSED 127.0.0.1:1');
    assert.equal(connectFailureMessage(undefined, new Error('WRONGPASS invalid username-password pair.')), 'WRONGPASS invalid username-password pair');
    const both = new AggregateError([new Error('connect ECONNREFUSED ::1:1'), new Error('connect ECONNREFUSED 127.0.0.1:1')], '');
    assert.equal(connectFailureMessage(both, undefined), 'connect ECONNREFUSED ::1:1');
    assert.equal(connectFailureMessage(undefined, undefined), 'unknown error');
  });
});
