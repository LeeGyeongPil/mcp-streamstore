// 연습·단위 시험용 가짜(mock) Redis. 실제 Redis 를 대체하지 않는다.
// 이 라이브러리가 쓰는 명령만 한 프로세스 메모리 안에서 흉내 낸다:
//   XADD(MAXLEN [~|=] n) · XRANGE(배타 "(" 범위, COUNT) · XTRIM(MAXLEN|MINID) · PEXPIRE · PTTL · DEL · EXISTS
//   SCAN(MATCH, COUNT 무시하고 한 번에) · SET(PX) · GET · PING · PUBLISH/SUBSCRIBE(함수)
// 실제 Redis 와 다른 점: MAXLEN "~" 도 정확히 자른다, 영속성·복제·클러스터·RESP 프로토콜이 없다,
// 만료는 접근할 때 확인한다, pub/sub 전달은 setImmediate 로 비동기지만 네트워크 지연은 없다.
import type { RedisCommand } from '../types.js';

export interface FakeRedis {
  /** Same shape as `nodeRedisCommand(client)`. */
  command: RedisCommand;
  /** Same shape as `RedisServerEventBus` `publish` option. */
  publish: (channel: string, message: string) => Promise<number>;
  /** Same shape as `RedisServerEventBus` `subscribe` option. */
  subscribe: (channel: string, onMessage: (message: string) => void) => Promise<() => Promise<void>>;
  /** Number of keys currently stored (expired keys excluded). */
  size(): number;
  /** Every command received, newest last (bounded to the last 1000). */
  readonly log: string[][];
}

interface Stream {
  type: 'stream';
  entries: Array<{ id: [number, number]; fields: string[] }>;
  last: [number, number];
  expireAt?: number;
}
interface Str {
  type: 'string';
  value: string;
  expireAt?: number;
}
type Value = Stream | Str;

const LOG_LIMIT = 1000;

export function createFakeRedis(opts: { now?: () => number } = {}): FakeRedis {
  const now = opts.now ?? Date.now;
  const data = new Map<string, Value>();
  const channels = new Map<string, Set<(message: string) => void>>();
  const log: string[][] = [];

  function live(key: string): Value | undefined {
    const v = data.get(key);
    if (v && v.expireAt !== undefined && v.expireAt <= now()) {
      data.delete(key);
      return undefined;
    }
    return v;
  }

  function stream(key: string, create: boolean): Stream | undefined {
    const v = live(key);
    if (v && v.type !== 'stream') throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
    if (!v && create) {
      const s: Stream = { type: 'stream', entries: [], last: [0, 0] };
      data.set(key, s);
      return s;
    }
    return v as Stream | undefined;
  }

  const command: RedisCommand = async (args) => {
    log.push([...args]);
    if (log.length > LOG_LIMIT) log.splice(0, log.length - LOG_LIMIT);
    const [rawName, ...a] = args;
    const name = (rawName ?? '').toUpperCase();
    switch (name) {
      case 'PING':
        return 'PONG';
      case 'XADD': {
        const key = need(a[0]);
        let i = 1;
        let maxlen: number | undefined;
        if (a[i]?.toUpperCase() === 'MAXLEN') {
          i++;
          if (a[i] === '~' || a[i] === '=') i++;
          maxlen = Number(need(a[i++]));
        }
        const idArg = need(a[i++]);
        if (idArg !== '*') throw new Error('ERR fake redis supports only "*" ids');
        const fields = a.slice(i);
        if (fields.length === 0 || fields.length % 2 !== 0) throw new Error('ERR wrong number of arguments for XADD');
        const s = stream(key, true)!;
        // Redis 와 같은 규칙: "<ms>-<seq>", 같은 ms 거나 시계가 뒤로 가면 seq 를 올린다
        const ms = Math.floor(now());
        const id: [number, number] = ms > s.last[0] ? [ms, 0] : [s.last[0], s.last[1] + 1];
        s.last = id;
        s.entries.push({ id, fields });
        if (maxlen !== undefined && s.entries.length > maxlen) s.entries.splice(0, s.entries.length - maxlen);
        return fmt(id);
      }
      case 'XRANGE': {
        const s = stream(need(a[0]), false);
        const [lo, loExcl] = bound(need(a[1]), 'low');
        const [hi, hiExcl] = bound(need(a[2]), 'high');
        let count = Infinity;
        if (a[3]?.toUpperCase() === 'COUNT') count = Number(need(a[4]));
        const out: Array<[string, string[]]> = [];
        for (const e of s?.entries ?? []) {
          if (out.length >= count) break;
          const c1 = cmp(e.id, lo);
          const c2 = cmp(e.id, hi);
          if (c1 < 0 || (loExcl && c1 === 0)) continue;
          if (c2 > 0 || (hiExcl && c2 === 0)) continue;
          out.push([fmt(e.id), [...e.fields]]);
        }
        return out;
      }
      case 'XTRIM': {
        const s = stream(need(a[0]), false);
        if (!s) return 0;
        let i = 1;
        const strategy = need(a[i++]).toUpperCase();
        if (a[i] === '~' || a[i] === '=') i++;
        const threshold = need(a[i]);
        const before = s.entries.length;
        if (strategy === 'MAXLEN') {
          const max = Number(threshold);
          if (s.entries.length > max) s.entries.splice(0, s.entries.length - max);
        } else if (strategy === 'MINID') {
          const [min] = bound(threshold, 'low');
          s.entries = s.entries.filter((e) => cmp(e.id, min) >= 0);
        } else {
          throw new Error(`ERR unknown XTRIM strategy ${strategy}`);
        }
        return before - s.entries.length;
      }
      case 'PEXPIRE': {
        const v = live(need(a[0]));
        if (!v) return 0;
        v.expireAt = now() + Number(need(a[1]));
        return 1;
      }
      case 'PTTL': {
        const v = live(need(a[0]));
        if (!v) return -2;
        return v.expireAt === undefined ? -1 : Math.max(0, v.expireAt - now());
      }
      case 'DEL': {
        let n = 0;
        for (const k of a) if (live(k) && data.delete(k)) n++;
        return n;
      }
      case 'EXISTS': {
        let n = 0;
        for (const k of a) if (live(k)) n++;
        return n;
      }
      case 'SCAN': {
        // 커서는 무시하고 한 번에 모두 돌려준다(커서 "0" = 끝)
        let pattern = '*';
        for (let i = 1; i < a.length; i += 2) if (a[i]?.toUpperCase() === 'MATCH') pattern = need(a[i + 1]);
        const re = globToRegExp(pattern);
        const keys = [...data.keys()].filter((k) => live(k) && re.test(k));
        return ['0', keys];
      }
      case 'SET': {
        const key = need(a[0]);
        const value = need(a[1]);
        const v: Str = { type: 'string', value };
        if (a[2]?.toUpperCase() === 'PX') v.expireAt = now() + Number(need(a[3]));
        data.set(key, v);
        return 'OK';
      }
      case 'GET': {
        const v = live(need(a[0]));
        if (!v) return null;
        if (v.type !== 'string') throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
        return v.value;
      }
      case 'FLUSHALL':
        data.clear();
        return 'OK';
      default:
        throw new Error(`ERR fake redis does not implement ${name}`);
    }
  };

  const publish = async (channel: string, message: string): Promise<number> => {
    const subs = channels.get(channel);
    if (!subs || subs.size === 0) return 0;
    const targets = [...subs];
    // 실제 Redis 처럼 발행 순서대로, 발행 호출이 끝난 뒤 비동기로 전달한다
    setImmediate(() => {
      for (const fn of targets) fn(message);
    });
    return targets.length;
  };

  const subscribe = async (channel: string, onMessage: (message: string) => void) => {
    let subs = channels.get(channel);
    if (!subs) {
      subs = new Set();
      channels.set(channel, subs);
    }
    // 같은 함수를 두 번 구독해도 독립된 구독이 되도록 감싼다
    const entry = (m: string) => onMessage(m);
    subs.add(entry);
    let done = false;
    return async () => {
      if (done) return;
      done = true;
      subs!.delete(entry);
      if (subs!.size === 0) channels.delete(channel);
    };
  };

  return {
    command,
    publish,
    subscribe,
    size: () => [...data.keys()].filter((k) => live(k)).length,
    log,
  };
}

function need(v: string | undefined): string {
  if (v === undefined) throw new Error('ERR wrong number of arguments');
  return v;
}

function fmt(id: [number, number]): string {
  return `${id[0]}-${id[1]}`;
}

function cmp(x: [number, number], y: [number, number]): number {
  return x[0] !== y[0] ? x[0] - y[0] : x[1] - y[1];
}

// XRANGE 경계: "-", "+", "(<id>", "<ms>" 또는 "<ms>-<seq>"
function bound(s: string, side: 'low' | 'high'): [[number, number], boolean] {
  if (s === '-') return [[0, 0], false];
  if (s === '+') return [[Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER], false];
  const excl = s.startsWith('(');
  const body = excl ? s.slice(1) : s;
  const [ms, seq] = body.split('-');
  const msN = Number(ms);
  if (!Number.isFinite(msN)) throw new Error('ERR Invalid stream ID specified as stream command argument');
  const seqN = seq === undefined ? (side === 'low' ? 0 : Number.MAX_SAFE_INTEGER) : Number(seq);
  return [[msN, seqN], excl];
}

function globToRegExp(glob: string): RegExp {
  let re = '';
  for (let i = 0; i < glob.length; i++) {
    const c = glob[i]!;
    if (c === '\\' && i + 1 < glob.length) re += escapeRe(glob[++i]!);
    else if (c === '*') re += '.*';
    else if (c === '?') re += '.';
    else re += escapeRe(c);
  }
  return new RegExp(`^${re}$`, 's');
}

function escapeRe(c: string): string {
  return c.replace(/[.*+?^${}()|[\]\\/]/g, '\\$&');
}
