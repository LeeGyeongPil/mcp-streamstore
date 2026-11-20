// 세션 기록 저장소: "이 세션은 어떤 클라이언트가, 어떤 능력·프로토콜 버전으로 열었나" 를 인스턴스 사이에 공유한다.
// 다른 인스턴스는 모르는 Mcp-Session-Id 를 받으면 여기서 기록을 찾아 rehydrate() 로 세션을 다시 세운다.
import { DatabaseSync, type StatementSync } from 'node:sqlite';
import type { RedisCommand, SessionRecord, SessionStore } from '../types.js';

export interface SessionStoreOptions {
  /** Forget sessions not seen for this long. Default 24 hours. */
  ttlMs?: number;
  /** Clock (inject a fake one in tests). Default `Date.now`. */
  now?: () => number;
}

const DEFAULT_SESSION_TTL_MS = 24 * 60 * 60 * 1000;

function resolve(opts: SessionStoreOptions = {}) {
  const ttlMs = opts.ttlMs ?? DEFAULT_SESSION_TTL_MS;
  if (!(ttlMs > 0)) throw new RangeError(`ttlMs must be > 0 (got ${ttlMs})`);
  return { ttlMs, now: opts.now ?? Date.now };
}

function validate(record: SessionRecord): void {
  if (!record || typeof record.sessionId !== 'string' || record.sessionId === '') {
    throw new TypeError('SessionRecord.sessionId must be a non-empty string');
  }
}

/** In-process session store (single instance, tests). */
export class MemorySessionStore implements SessionStore {
  private readonly opts: { ttlMs: number; now: () => number };
  private readonly records = new Map<string, string>();

  constructor(opts?: SessionStoreOptions) {
    this.opts = resolve(opts);
  }

  async get(sessionId: string): Promise<SessionRecord | undefined> {
    const raw = this.records.get(sessionId);
    if (raw === undefined) return undefined;
    const record = JSON.parse(raw) as SessionRecord;
    if (this.opts.now() - record.lastSeenAt > this.opts.ttlMs) {
      this.records.delete(sessionId); // 만료된 기록은 읽는 순간 지운다
      return undefined;
    }
    return record;
  }

  async put(record: SessionRecord): Promise<void> {
    validate(record);
    this.records.set(record.sessionId, JSON.stringify(record));
    this.sweep();
  }

  async touch(sessionId: string): Promise<void> {
    const record = await this.get(sessionId);
    if (!record) return;
    record.lastSeenAt = this.opts.now();
    this.records.set(sessionId, JSON.stringify(record));
  }

  async delete(sessionId: string): Promise<void> {
    this.records.delete(sessionId);
  }

  async close(): Promise<void> {
    this.records.clear();
  }

  // put 할 때 만료된 기록을 함께 치워 Map 이 끝없이 커지지 않게 한다.
  // 매번 전체를 훑으면 세션이 많을 때 put 이 느려지므로 ttl 의 절반(최대 1분)에 한 번만 훑는다.
  private lastSweep = Number.NEGATIVE_INFINITY;
  private sweep(): void {
    const now = this.opts.now();
    if (now - this.lastSweep < Math.min(this.opts.ttlMs / 2, 60_000)) return;
    this.lastSweep = now;
    const cutoff = now - this.opts.ttlMs;
    for (const [id, raw] of this.records) {
      if ((JSON.parse(raw) as SessionRecord).lastSeenAt < cutoff) this.records.delete(id);
    }
  }
}

/** Session store in a SQLite file shared by the processes of one host (can share the EventStore's file). */
export class SqliteSessionStore implements SessionStore {
  private readonly opts: { ttlMs: number; now: () => number };
  private readonly db: DatabaseSync;
  private closed = false;
  private readonly q: { get: StatementSync; put: StatementSync; touch: StatementSync; del: StatementSync; sweep: StatementSync };

  constructor(opts: { path: string; busyTimeoutMs?: number } & SessionStoreOptions) {
    if (!opts || typeof opts.path !== 'string' || opts.path === '') {
      throw new TypeError('SqliteSessionStore: options.path is required');
    }
    this.opts = resolve(opts);
    this.db = new DatabaseSync(opts.path);
    this.db.exec(`PRAGMA busy_timeout = ${Math.max(0, Math.floor(opts.busyTimeoutMs ?? 5000))}`);
    this.db.exec('PRAGMA journal_mode = WAL');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS mcp_sessions (
        session_id   TEXT PRIMARY KEY,
        record       TEXT NOT NULL,
        last_seen_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS mcp_sessions_seen ON mcp_sessions (last_seen_at);
    `);
    this.q = {
      get: this.db.prepare('SELECT record, last_seen_at FROM mcp_sessions WHERE session_id = ?'),
      put: this.db.prepare(
        'INSERT INTO mcp_sessions (session_id, record, last_seen_at) VALUES (?, ?, ?) ' +
          'ON CONFLICT(session_id) DO UPDATE SET record = excluded.record, last_seen_at = excluded.last_seen_at',
      ),
      touch: this.db.prepare('UPDATE mcp_sessions SET last_seen_at = ? WHERE session_id = ?'),
      del: this.db.prepare('DELETE FROM mcp_sessions WHERE session_id = ?'),
      sweep: this.db.prepare('DELETE FROM mcp_sessions WHERE last_seen_at < ?'),
    };
  }

  async get(sessionId: string): Promise<SessionRecord | undefined> {
    this.assertOpen();
    const row = this.q.get.get(sessionId) as { record: string; last_seen_at: number } | undefined;
    if (!row) return undefined;
    if (this.opts.now() - row.last_seen_at > this.opts.ttlMs) {
      this.q.del.run(sessionId);
      return undefined;
    }
    return { ...(JSON.parse(row.record) as SessionRecord), lastSeenAt: row.last_seen_at };
  }

  async put(record: SessionRecord): Promise<void> {
    this.assertOpen();
    validate(record);
    this.q.put.run(record.sessionId, JSON.stringify(record), record.lastSeenAt);
    this.q.sweep.run(this.opts.now() - this.opts.ttlMs);
  }

  async touch(sessionId: string): Promise<void> {
    this.assertOpen();
    this.q.touch.run(this.opts.now(), sessionId);
  }

  async delete(sessionId: string): Promise<void> {
    this.assertOpen();
    this.q.del.run(sessionId);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.db.close();
  }

  private assertOpen(): void {
    if (this.closed) throw new Error('SqliteSessionStore is closed');
  }
}

/** Session store in Redis: one string key per session with a sliding expiry (`PX`). */
export class RedisSessionStore implements SessionStore {
  private readonly opts: { ttlMs: number; now: () => number };
  private readonly command: RedisCommand;
  private readonly prefix: string;

  constructor(opts: { command: RedisCommand; prefix?: string } & SessionStoreOptions) {
    if (!opts || typeof opts.command !== 'function') {
      throw new TypeError('RedisSessionStore: options.command is required (see nodeRedisCommand / ioredisCommand)');
    }
    this.opts = resolve(opts);
    this.command = opts.command;
    this.prefix = opts.prefix ?? 'mcp:';
  }

  private key(sessionId: string): string {
    return `${this.prefix}session:${encodeURIComponent(sessionId)}`;
  }

  async get(sessionId: string): Promise<SessionRecord | undefined> {
    const raw = await this.command(['GET', this.key(sessionId)]);
    const text = typeof raw === 'string' ? raw : raw instanceof Uint8Array ? Buffer.from(raw).toString('utf8') : undefined;
    return text ? (JSON.parse(text) as SessionRecord) : undefined;
  }

  async put(record: SessionRecord): Promise<void> {
    validate(record);
    // 만료는 Redis 가 맡는다(PX): 세션이 쓰이지 않으면 ttl 뒤 저절로 사라진다
    await this.command(['SET', this.key(record.sessionId), JSON.stringify(record), 'PX', String(Math.ceil(this.opts.ttlMs))]);
  }

  async touch(sessionId: string): Promise<void> {
    const record = await this.get(sessionId);
    if (!record) return;
    record.lastSeenAt = this.opts.now();
    await this.put(record);
  }

  async delete(sessionId: string): Promise<void> {
    await this.command(['DEL', this.key(sessionId)]);
  }

  async close(): Promise<void> {
    // 연결은 호출자의 것이므로 닫지 않는다
  }
}
