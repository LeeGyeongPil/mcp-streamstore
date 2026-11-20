// 빌드 전에 출력 폴더를 비운다(지운 소스의 낡은 .js 가 패키지에 섞여 들어가지 않게).
// 사용: node scripts/clean.mjs <folder> [...]  — 레포 안의 폴더만 지운다.
import { rmSync } from 'node:fs';
import { dirname, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
for (const arg of process.argv.slice(2)) {
  const target = resolve(root, arg);
  const rel = relative(root, target);
  if (rel === '' || rel.startsWith('..')) throw new Error(`refusing to delete outside the repository: ${arg}`);
  rmSync(target, { recursive: true, force: true });
}
