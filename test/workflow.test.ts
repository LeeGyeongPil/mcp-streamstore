// AC-10: CI 계약 시험. 필수 CI(ci.yml)는 외부 서비스 없이 로컬과 같은 한 명령(npm run ci)만 돌고,
// 러너·Node·액션이 고정되어 있어야 한다. 실 Redis 시험(redis-real.yml)은 수동 실행 전용이어야 한다.
// YAML 파서 의존성 없이 글자(줄·들여쓰기)로 검사한다 — 이 레포의 워크플로는 아래 규칙을 지키는 단순한 모양이다.
import assert from 'node:assert/strict';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

// ---------------------------------------------------------------- 레포 루트 찾기 (src·build 어디서 돌아도)

function repoRoot(): string {
  let dir = dirname(fileURLToPath(import.meta.url));
  for (;;) {
    const pkg = join(dir, 'package.json');
    if (existsSync(pkg) && (JSON.parse(readFileSync(pkg, 'utf8')) as { name?: string }).name === 'mcp-streamstore') return dir;
    const up = dirname(dir);
    if (up === dir) throw new Error('package.json of mcp-streamstore not found above the test file');
    dir = up;
  }
}

const ROOT = repoRoot();
const WORKFLOWS = join(ROOT, '.github', 'workflows');
const read = (name: string) => readFileSync(join(WORKFLOWS, name), 'utf8');

// ---------------------------------------------------------------- 작은 줄 단위 도구

/** Lines without comments (a `#` that starts the line or follows a space) and without trailing spaces. */
function codeLines(text: string): string[] {
  return text.split(/\r?\n/).map((l) => l.replace(/(^|\s)#.*$/, '').replace(/\s+$/, ''));
}

const indentOf = (line: string) => line.length - line.trimStart().length;

/** Lines that belong to the mapping key `key:` found at `indent` (its children), or undefined if the key is missing. */
function block(lines: string[], key: string, indent = 0): string[] | undefined {
  const start = lines.findIndex((l) => indentOf(l) === indent && l.trim().startsWith(`${key}:`));
  if (start < 0) return undefined;
  const out: string[] = [];
  const inline = lines[start]!.trim().slice(key.length + 1).trim();
  if (inline) out.push(`${' '.repeat(indent + 2)}${inline}`); // `on: workflow_dispatch` 같은 한 줄 값
  for (const l of lines.slice(start + 1)) {
    if (l.trim() === '') continue;
    if (indentOf(l) <= indent) break;
    out.push(l);
  }
  return out;
}

/** Mapping keys directly under a block (the smallest indentation inside it). */
function childKeys(lines: string[]): string[] {
  const body = lines.filter((l) => l.trim() !== '');
  if (body.length === 0) return [];
  const min = Math.min(...body.map(indentOf));
  return body
    .filter((l) => indentOf(l) === min)
    .map((l) => l.trim())
    .flatMap((t) => {
      if (/^\[.*\]$/.test(t)) return t.slice(1, -1).split(',').map((s) => s.trim()); // on: [a, b]
      const m = /^([\w-]+)\s*:/.exec(t) ?? /^([\w-]+)$/.exec(t);
      return m ? [m[1]!] : [t];
    });
}

/** Every value of `key:` anywhere (step keys may follow `- `). Quotes are kept so callers can check them. */
function values(lines: string[], key: string): string[] {
  const re = new RegExp(`^\\s*(?:-\\s+)?${key}:\\s*(.*)$`);
  return lines.flatMap((l) => {
    const m = re.exec(l);
    return m ? [m[1]!] : [];
  });
}

const SHA = /^[0-9a-f]{40}$/;
const ALLOWED_ACTIONS = ['actions/checkout', 'actions/setup-node'];
const RUNNER = 'ubuntu-24.04';
const NODE = '"22.23.5"';
const DIAGNOSTIC_RUN = 'node --version && npm --version'; // 버전만 찍는 단계는 허용

/** `uses:` entries split into action name, ref and the tag written in the trailing comment. */
function usesOf(text: string) {
  return text.split(/\r?\n/).flatMap((l) => {
    const m = /^\s*(?:-\s+)?uses:\s*([^@\s]+)@(\S+)\s*(?:#\s*(\S+))?\s*$/.exec(l);
    return m ? [{ action: m[1]!, ref: m[2]!, tag: m[3] }] : [];
  });
}

/** Problems shared by both workflows: pinned runner, Node and actions. */
function pinningProblems(text: string): string[] {
  const problems: string[] = [];
  const lines = codeLines(text);
  const runsOn = values(lines, 'runs-on');
  if (runsOn.length !== 1 || runsOn[0] !== RUNNER) problems.push(`runs-on must be exactly one "${RUNNER}", got ${JSON.stringify(runsOn)}`);
  const node = values(lines, 'node-version');
  if (node.length !== 1 || node[0] !== NODE) problems.push(`node-version must be exactly ${NODE}, got ${JSON.stringify(node)}`);
  const uses = usesOf(text);
  const names = uses.map((u) => u.action).sort();
  if (JSON.stringify(names) !== JSON.stringify(ALLOWED_ACTIONS)) problems.push(`uses must be only ${ALLOWED_ACTIONS.join(' and ')}, got ${JSON.stringify(names)}`);
  for (const u of uses) {
    if (!SHA.test(u.ref)) problems.push(`${u.action} must be pinned to a 40-hex commit SHA, got @${u.ref}`);
    if (!u.tag || !/^v\d+\.\d+\.\d+$/.test(u.tag)) problems.push(`${u.action} needs a trailing "# vX.Y.Z" tag comment`);
  }
  return problems;
}

/** Every rule of the required CI (AC-10). An empty list means the workflow is fine. */
export function ciProblems(text: string): string[] {
  const problems = pinningProblems(text);
  const lines = codeLines(text);

  // 외부 서비스·캐시·예약 실행·실패 무시는 글자 자체가 없어야 한다(주석 포함)
  for (const [word, re] of [
    ['services:', /\bservices\s*:/],
    ['container:', /\bcontainer\s*:/],
    ['REDIS_URL', /REDIS_URL/],
    ['cache:', /\bcache\s*:/],
    ['schedule:', /\bschedule\s*:/],
    ['continue-on-error', /continue-on-error/],
  ] as const) {
    if (re.test(text)) problems.push(`must not contain ${word}`);
  }

  // 트리거: push(main 만)·pull_request·workflow_dispatch
  const on = block(lines, 'on');
  if (!on) problems.push('missing on:');
  else {
    const triggers = childKeys(on).sort();
    if (JSON.stringify(triggers) !== JSON.stringify(['pull_request', 'push', 'workflow_dispatch'])) {
      problems.push(`triggers must be push, pull_request, workflow_dispatch; got ${JSON.stringify(triggers)}`);
    }
    const pushIndent = on.find((l) => l.trim().startsWith('push'));
    const push = pushIndent ? block(on, 'push', indentOf(pushIndent)) : undefined;
    const pushKeys = push ? childKeys(push) : [];
    const branches = push ? values(push, 'branches') : [];
    if (JSON.stringify(pushKeys) !== JSON.stringify(['branches']) || branches[0] !== '[main]') {
      problems.push(`push must be limited to branches: [main] (no tags, no other filters); got ${JSON.stringify(push ?? [])}`);
    }
  }

  // job 은 하나
  const jobs = block(lines, 'jobs');
  const jobNames = jobs ? childKeys(jobs) : [];
  if (jobNames.length !== 1) problems.push(`must have exactly one job, got ${JSON.stringify(jobNames)}`);

  // 실행 단계: 버전 출력(선택) 다음에 npm ci --no-audit --no-fund, npm run ci 만
  const runs = values(lines, 'run');
  if (runs.some((r) => r === '|' || r === '>' || r.startsWith('|') || r.startsWith('>'))) problems.push('run steps must be single commands, not multi-line scripts');
  const real = runs.filter((r) => r !== DIAGNOSTIC_RUN);
  if (JSON.stringify(real) !== JSON.stringify(['npm ci --no-audit --no-fund', 'npm run ci'])) {
    problems.push(`run steps must be "npm ci --no-audit --no-fund" then "npm run ci", got ${JSON.stringify(runs)}`);
  }
  return problems;
}

/** Rules for the optional real-Redis workflow: manual only, same pinning, actually runs the real suite. */
export function redisRealProblems(text: string): string[] {
  const problems = pinningProblems(text);
  const lines = codeLines(text);
  const on = block(lines, 'on');
  const triggers = on ? childKeys(on) : [];
  if (JSON.stringify(triggers) !== JSON.stringify(['workflow_dispatch'])) problems.push(`trigger must be workflow_dispatch only, got ${JSON.stringify(triggers)}`);
  if (!values(lines, 'run').includes('npm run test:redis')) problems.push('must run npm run test:redis');
  if (!/REDIS_URL:\s*redis:\/\/127\.0\.0\.1:6379/.test(text)) problems.push('must set REDIS_URL to the service on 127.0.0.1:6379');
  return problems;
}

// ---------------------------------------------------------------- 실제 파일 검사

describe('required CI workflow (.github/workflows/ci.yml)', () => {
  it('follows the offline CI contract', () => {
    assert.deepEqual(ciProblems(read('ci.yml')), []);
  });

  it('npm run ci is the same command contributors run: npm test, then the demo', () => {
    const pkg = JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf8')) as { scripts: Record<string, string> };
    assert.equal(pkg.scripts.ci, 'npm test && node dist/cli.js demo --backend all --lang en');
  });
});

describe('optional real Redis workflow (.github/workflows/redis-real.yml)', () => {
  it('is manual-only and uses the same pinned runner, Node and actions', () => {
    assert.deepEqual(redisRealProblems(read('redis-real.yml')), []);
  });

  it('pins the same action SHAs as ci.yml', () => {
    const pins = (t: string) => usesOf(t).map((u) => `${u.action}@${u.ref}`).sort();
    assert.deepEqual(pins(read('redis-real.yml')), pins(read('ci.yml')));
  });
});

it('no other workflow files exist (no scheduled or extra jobs slipped back in)', () => {
  const files = readdirSync(WORKFLOWS).filter((f) => /\.ya?ml$/.test(f)).sort();
  assert.deepEqual(files, ['ci.yml', 'redis-real.yml']);
});

// ---------------------------------------------------------------- 검사기가 위반을 실제로 잡는지 (v1 워크플로로 확인)

const V1_CI = `name: ci
on:
  push:
  pull_request:
  workflow_dispatch:
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:7
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-node@v7
        with:
          node-version: "22"
      - run: npm ci --no-audit --no-fund
      - run: npm test
      - run: npm run test:redis
        env:
          REDIS_URL: redis://127.0.0.1:6379
`;

describe('the contract checker catches violations', () => {
  it('flags every problem of the v1 workflow', () => {
    const p = ciProblems(V1_CI).join('\n');
    for (const expected of [
      /runs-on must be exactly one "ubuntu-24.04"/,
      /node-version must be exactly "22.23.5"/,
      /actions\/checkout must be pinned to a 40-hex commit SHA, got @v7/,
      /actions\/setup-node must be pinned to a 40-hex commit SHA/,
      /must not contain services:/,
      /must not contain REDIS_URL/,
      /push must be limited to branches: \[main\]/,
      /run steps must be "npm ci --no-audit --no-fund" then "npm run ci"/,
    ]) {
      assert.match(p, expected);
    }
  });

  it('flags tags on push, a second job, cache and a schedule', () => {
    const good = read('ci.yml');
    assert.match(ciProblems(good.replace('branches: [main]', 'branches: [main]\n    tags: ["v*"]')).join('\n'), /push must be limited/);
    assert.match(ciProblems(good.replace('  workflow_dispatch:', '  workflow_dispatch:\n  schedule:\n    - cron: "0 3 * * *"')).join('\n'), /schedule:|triggers must be/);
    assert.match(ciProblems(good.replace('node-version: "22.23.5"', 'node-version: "22.23.5"\n          cache: npm')).join('\n'), /must not contain cache:/);
    assert.match(ciProblems(`${good}\n  extra:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: echo hi\n`).join('\n'), /exactly one job/);
  });

  it('flags a real-Redis workflow that also runs on push', () => {
    const manual = read('redis-real.yml');
    assert.match(redisRealProblems(manual.replace('on:\n  workflow_dispatch:', 'on:\n  push:\n  workflow_dispatch:')).join('\n'), /workflow_dispatch only/);
    assert.match(redisRealProblems(manual.replace('on:\n  workflow_dispatch:', 'on: [push, workflow_dispatch]')).join('\n'), /workflow_dispatch only/);
  });
});
