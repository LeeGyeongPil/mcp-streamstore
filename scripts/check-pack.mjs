#!/usr/bin/env node
// 설치 확인(AC-9): npm pack 결과물을 빈 임시 프로젝트에 설치해 "남이 처음 설치했을 때" 를 그대로 재현한다.
//   1) npm pack  2) os.tmpdir() 아래 새 프로젝트에 tarball + SDK 설치  3) 동봉된 examples/quickstart.mjs 실행
//   4) tsc --noEmit 로 exports 하위 경로(".", "./conformance", "./testing")의 타입과 TS 예제를 확인
//   5) Node 가 .ts 를 직접 돌릴 수 있으면(22.18+) TS 예제도 실행
// 사용: npm run check:pack  (--keep 을 주면 임시 폴더를 남긴다)
import { spawnSync } from 'node:child_process';
import { cpSync, mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const keep = process.argv.includes('--keep');
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const started = Date.now();

function run(cmd, args, cwd, { capture = false } = {}) {
  const r = spawnSync(cmd, args, { cwd, encoding: 'utf8', stdio: capture ? ['ignore', 'pipe', 'inherit'] : 'inherit', shell: false });
  if (r.status !== 0) throw new Error(`${cmd} ${args.join(' ')} failed (exit ${r.status})`);
  return r.stdout ?? '';
}

const work = mkdtempSync(join(tmpdir(), 'mcp-streamstore-pack-'));
try {
  // 1) 게시될 파일 묶음 만들기(빌드는 npm run check:pack 이 먼저 했다 — prepack 출력이 --json 에 섞이지 않게 스크립트는 끈다)
  const packed = JSON.parse(run(npm, ['pack', '--json', '--ignore-scripts', '--pack-destination', work], root, { capture: true }));
  const tarball = join(work, packed[0].filename);
  console.log(`packed ${packed[0].filename} (${packed[0].entryCount} files)`);

  // 2) 빈 프로젝트에 설치 — SDK·TypeScript 는 이 레포가 시험한 버전 그대로
  const app = join(work, 'app');
  cpSync(join(root, 'scripts', 'check-pack'), app, { recursive: true }); // package.json·tsconfig·types-check.ts
  const dev = pkg.devDependencies;
  run(npm, [
    'install', '--no-audit', '--no-fund', '--loglevel=error',
    tarball,
    `@modelcontextprotocol/sdk@${dev['@modelcontextprotocol/sdk']}`,
    `@modelcontextprotocol/server@${dev['@modelcontextprotocol/server']}`,
    `typescript@${dev.typescript}`,
    `@types/node@${dev['@types/node']}`,
  ], app);

  // 3) 패키지에 들어간 예제를 꺼내 실행
  const shipped = join(app, 'node_modules', 'mcp-streamstore', 'examples');
  const examples = readdirSync(shipped);
  for (const f of examples) cpSync(join(shipped, f), join(app, f));
  console.log(`examples shipped in the package: ${examples.join(', ')}`);
  const out = run(process.execPath, ['quickstart.mjs'], app, { capture: true });
  process.stdout.write(out);
  if (!/resumed 3 events/.test(out)) throw new Error('quickstart did not resume 3 events');

  // 4) 타입 확인: 세 하위 경로 + TS 예제
  run(process.execPath, [join(app, 'node_modules', 'typescript', 'bin', 'tsc'), '-p', 'tsconfig.json'], app);
  console.log('tsc --noEmit: types resolve for ".", "./conformance", "./testing" and the TypeScript examples');

  // 5) TS 예제 실행(타입 지우기를 지원하는 Node 만)
  if (process.features?.typescript) {
    for (const f of examples.filter((x) => x.endsWith('.ts'))) {
      process.stdout.write(run(process.execPath, [f], app, { capture: true }));
    }
  } else {
    console.log(`Node ${process.version} cannot run .ts files directly; skipped running the TypeScript examples`);
  }
  console.log(`check:pack ok in ${((Date.now() - started) / 1000).toFixed(1)} s`);
} finally {
  if (keep) console.log(`kept ${work}`);
  else rmSync(work, { recursive: true, force: true });
}
