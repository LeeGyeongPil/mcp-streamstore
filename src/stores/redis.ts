// Redis 저장소(Redis Streams). 여러 호스트의 인스턴스가 같은 Redis 를 보면 어느 인스턴스로 재연결해도 재생된다.
// - 스트림 하나 = Redis 키 하나: "<prefix>s:<스트림 키>"
// - 이벤트 ID = "<스트림 키 인코딩>~<XADD ID>": XADD ID("1700000000000-0") 에는 스트림 정보가 없으므로
//   (블로그 예제처럼 XADD ID 를 잘라 스트림을 알아내는 방식은 틀린다) 스트림 키를 이벤트 ID 에 직접 담는다.
// - 재생 = XRANGE key (<id> +  (괄호는 "배타 범위", Redis 6.2+)
// - 보존 = XADD MAXLEN ~ N(근사) + 키 PEXPIRE(마지막 쓰기 뒤 ttl), prune() 은 정확한 XTRIM 으로 마무리
// Redis 클라이언트는 동봉하지 않는다: 사용자가 고른 클라이언트의 "명령 하나 보내기" 함수를 받는다.
import { SessionScopedEventStore, sessionKeyPrefix } from '../scope.js';
import {
  resolveRetention,
  type EventId,
  type JSONRPCMessage,
  type RedisCommand,
  type ReplaySink,
  type RetentionOptions,
  type StreamId,
  type StreamStore,
} from '../types.js';

export interface RedisEventStoreOptions extends RetentionOptions {
  /** Sends one command and resolves with the raw reply. See `nodeRedisCommand` / `ioredisCommand`. */
  command: RedisCommand;
  /** Key prefix for everything this store writes. Default "mcp:". */
  prefix?: string;
}

const XADD_ID = /^[0-9]{1,20}-[0-9]{1,20}$/;
const ID_SEP = '~'; // encodeURIComponent 는 "~" 를 바꾸지 않지만 XADD ID 에는 "~" 가 없으므로 마지막 "~" 가 경계다
const REPLAY_BATCH = 500;

export class RedisEventStore implements StreamStore {
  private readonly retention: Required<RetentionOptions>;
  private readonly command: RedisCommand;
  private readonly prefix: string;
  private closed = false;

  constructor(opts: RedisEventStoreOptions) {
    if (!opts || typeof opts.command !== 'function') {
      throw new TypeError('RedisEventStore: options.command is required (see nodeRedisCommand / ioredisCommand)');
    }
    this.retention = resolveRetention(opts);
    this.command = opts.command;
    this.prefix = opts.prefix ?? 'mcp:';
  }

  private redisKey(streamKey: string): string {
    return `${this.prefix}s:${streamKey}`;
  }

  async storeEvent(streamId: StreamId, message: JSONRPCMessage): Promise<EventId> {
    this.assertOpen();
    const key = this.redisKey(streamId);
    const args = ['XADD', key];
    if (Number.isFinite(this.retention.maxEventsPerStream)) {
      // "~" 는 근사 정리: Redis 가 내부 노드 단위로 잘라 빠르다. 정확한 상한은 prune() 이 맞춘다.
      args.push('MAXLEN', '~', String(this.retention.maxEventsPerStream));
    }
    args.push('*', 'm', JSON.stringify(message ?? {}));
    const id = toStr(await this.command(args));
    if (!id || !XADD_ID.test(id)) throw new Error(`RedisEventStore: unexpected XADD reply ${JSON.stringify(id)}`);
    if (Number.isFinite(this.retention.ttlMs)) {
      // 마지막 쓰기 뒤 ttl 이 지나면 스트림 전체가 사라진다(끝난 스트림이 영원히 남지 않게)
      await this.command(['PEXPIRE', key, String(Math.ceil(this.retention.ttlMs))]);
    }
    return `${encodeURIComponent(streamId)}${ID_SEP}${id}`;
  }

  async getStreamIdForEventId(eventId: EventId): Promise<StreamId | undefined> {
    this.assertOpen();
    const parsed = parseEventId(eventId);
    if (!parsed) return undefined;
    // 이벤트가 아직 남아 있는지 확인한다(만료·정리된 ID 는 모르는 ID → 전송이 400 을 돌려준다)
    const reply = await this.command(['XRANGE', this.redisKey(parsed.streamKey), parsed.id, parsed.id, 'COUNT', '1']);
    return parseEntries(reply).length > 0 ? parsed.streamKey : undefined;
  }

  async replayEventsAfter(lastEventId: EventId, { send }: ReplaySink): Promise<StreamId> {
    const streamKey = await this.getStreamIdForEventId(lastEventId);
    if (streamKey === undefined) return '';
    const key = this.redisKey(streamKey);
    let cursor = parseEventId(lastEventId)!.id;
    for (;;) {
      this.assertOpen();
      const entries = parseEntries(await this.command(['XRANGE', key, `(${cursor}`, '+', 'COUNT', String(REPLAY_BATCH)]));
      for (const [id, json] of entries) {
        await send(`${encodeURIComponent(streamKey)}${ID_SEP}${id}`, JSON.parse(json));
        cursor = id;
      }
      if (entries.length < REPLAY_BATCH) break;
    }
    return streamKey;
  }

  forSession(sessionId: string) {
    return new SessionScopedEventStore(this, sessionId);
  }

  async deleteSession(sessionId: string): Promise<void> {
    this.assertOpen();
    const keys = await this.scanKeys(globEscape(this.redisKey(sessionKeyPrefix(sessionId))) + '*');
    for (let i = 0; i < keys.length; i += 100) {
      await this.command(['DEL', ...keys.slice(i, i + 100)]);
    }
  }

  async prune(): Promise<number> {
    this.assertOpen();
    let removed = 0;
    const keys = await this.scanKeys(globEscape(`${this.prefix}s:`) + '*');
    for (const key of keys) {
      if (Number.isFinite(this.retention.ttlMs)) {
        // XADD ID 의 앞부분은 Redis 서버 시각(ms)이다 → 그보다 오래된 항목을 정확히 지운다
        const minId = Math.max(0, Math.floor(this.retention.now() - this.retention.ttlMs));
        removed += toNum(await this.command(['XTRIM', key, 'MINID', String(minId)]));
      }
      if (Number.isFinite(this.retention.maxEventsPerStream)) {
        removed += toNum(await this.command(['XTRIM', key, 'MAXLEN', String(this.retention.maxEventsPerStream)]));
      }
    }
    return removed;
  }

  async close(): Promise<void> {
    // 연결은 사용자가 만든 것이므로 닫지 않는다(공유 클라이언트일 수 있다). 이후 호출만 막는다.
    this.closed = true;
  }

  private assertOpen(): void {
    if (this.closed) throw new Error('RedisEventStore is closed');
  }

  private async scanKeys(pattern: string): Promise<string[]> {
    const found = new Set<string>();
    let cursor = '0';
    do {
      const reply = await this.command(['SCAN', cursor, 'MATCH', pattern, 'COUNT', '200']);
      const [next, keys] = parseScan(reply);
      for (const k of keys) found.add(k);
      cursor = next;
    } while (cursor !== '0');
    return [...found];
  }
}

/** Split "<encoded stream key>~<xadd id>". */
export function parseEventId(eventId: string): { streamKey: string; id: string } | undefined {
  const i = eventId.lastIndexOf(ID_SEP);
  if (i <= 0) return undefined;
  const id = eventId.slice(i + 1);
  if (!XADD_ID.test(id)) return undefined;
  try {
    return { streamKey: decodeURIComponent(eventId.slice(0, i)), id };
  } catch {
    return undefined;
  }
}

/** Escape glob special characters for SCAN MATCH. */
export function globEscape(s: string): string {
  return s.replace(/[*?[\]\\]/g, (c) => `\\${c}`);
}

// --- Redis 응답 해석: node-redis·ioredis 의 RESP2 배열과 Buffer, RESP3 Map 을 모두 받는다 ---

function toStr(v: unknown): string | undefined {
  if (typeof v === 'string') return v;
  if (v instanceof Uint8Array) return Buffer.from(v).toString('utf8');
  if (typeof v === 'number') return String(v);
  return undefined;
}

function toNum(v: unknown): number {
  const n = Number(toStr(v) ?? v);
  return Number.isFinite(n) ? n : 0;
}

/** XRANGE reply → [[id, messageJson], ...] */
function parseEntries(reply: unknown): Array<[string, string]> {
  if (!Array.isArray(reply)) return [];
  const out: Array<[string, string]> = [];
  for (const entry of reply) {
    let id: string | undefined;
    let fields: unknown;
    if (Array.isArray(entry)) {
      id = toStr(entry[0]);
      fields = entry[1];
    } else if (entry && typeof entry === 'object') {
      const e = entry as { id?: unknown; message?: unknown };
      id = toStr(e.id);
      fields = e.message;
    }
    const m = fieldValue(fields, 'm');
    if (id !== undefined && m !== undefined) out.push([id, m]);
  }
  return out;
}

function fieldValue(fields: unknown, name: string): string | undefined {
  if (Array.isArray(fields)) {
    for (let i = 0; i + 1 < fields.length; i += 2) if (toStr(fields[i]) === name) return toStr(fields[i + 1]);
    return undefined;
  }
  if (fields instanceof Map) return toStr(fields.get(name));
  if (fields && typeof fields === 'object') return toStr((fields as Record<string, unknown>)[name]);
  return undefined;
}

function parseScan(reply: unknown): [string, string[]] {
  if (Array.isArray(reply)) {
    const keys = Array.isArray(reply[1]) ? reply[1].map(toStr).filter((k): k is string => k !== undefined) : [];
    return [toStr(reply[0]) ?? '0', keys];
  }
  if (reply && typeof reply === 'object') {
    const r = reply as { cursor?: unknown; keys?: unknown };
    const keys = Array.isArray(r.keys) ? r.keys.map(toStr).filter((k): k is string => k !== undefined) : [];
    return [toStr(r.cursor) ?? '0', keys];
  }
  return ['0', []];
}
