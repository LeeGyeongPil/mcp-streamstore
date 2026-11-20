// SQLite 저장소(node:sqlite 내장, 외부 네이티브 모듈 없음).
// 같은 호스트의 여러 프로세스가 같은 .db 파일을 열면 "공용 장부" 가 된다.
// - 이벤트 ID = AUTOINCREMENT seq: 여러 프로세스가 동시에 써도 커밋 순서대로 단조 증가한다(같은 ms 여도 순서 보장).
// - WAL + busy_timeout: 읽기는 쓰기를 막지 않고, 쓰기 충돌은 잠시 기다린다.
// 네트워크 파일시스템(NFS 등) 위의 공유 파일은 잠금이 믿을 수 없으므로 여러 호스트라면 Redis 를 쓴다.
import { DatabaseSync, type StatementSync } from 'node:sqlite';
import { SessionScopedEventStore, sessionKeyPrefix } from '../scope.js';
import {
  resolveRetention,
  type EventId,
  type JSONRPCMessage,
  type ReplaySink,
  type RetentionOptions,
  type StreamId,
  type StreamStore,
} from '../types.js';
import { parseSeq } from './memory.js';

export interface SqliteEventStoreOptions extends RetentionOptions {
  /** Database file path (shared by every process on this host). Use ':memory:' for a private store. */
  path: string;
  /** How long a writer waits for the lock held by another process. Default 5000 ms. */
  busyTimeoutMs?: number;
}

const REPLAY_BATCH = 500;

export class SqliteEventStore implements StreamStore {
  private readonly retention: Required<RetentionOptions>;
  private readonly db: DatabaseSync;
  private closed = false;
  private lastAutoPrune = 0;
  private readonly q: {
    insert: StatementSync;
    trimStream: StatementSync;
    owner: StatementSync;
    after: StatementSync;
    pruneOld: StatementSync;
    pruneExtra: StatementSync;
    deletePrefix: StatementSync;
  };

  constructor(opts: SqliteEventStoreOptions) {
    if (!opts || typeof opts.path !== 'string' || opts.path === '') {
      throw new TypeError('SqliteEventStore: options.path is required');
    }
    this.retention = resolveRetention(opts);
    this.db = new DatabaseSync(opts.path);
    this.db.exec(`PRAGMA busy_timeout = ${Math.max(0, Math.floor(opts.busyTimeoutMs ?? 5000))}`);
    this.db.exec('PRAGMA journal_mode = WAL');
    this.db.exec('PRAGMA synchronous = NORMAL');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS mcp_events (
        seq        INTEGER PRIMARY KEY AUTOINCREMENT,
        stream_key TEXT    NOT NULL,
        message    TEXT    NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS mcp_events_stream  ON mcp_events (stream_key, seq);
      CREATE INDEX IF NOT EXISTS mcp_events_created ON mcp_events (created_at);
    `);
    this.q = {
      insert: this.db.prepare('INSERT INTO mcp_events (stream_key, message, created_at) VALUES (?, ?, ?)'),
      // 이 스트림에서 최신 N 개보다 오래된 것을 지운다
      trimStream: this.db.prepare(`
        DELETE FROM mcp_events WHERE stream_key = ? AND seq <= (
          SELECT seq FROM mcp_events WHERE stream_key = ? ORDER BY seq DESC LIMIT 1 OFFSET ?
        )`),
      owner: this.db.prepare('SELECT stream_key FROM mcp_events WHERE seq = ?'),
      after: this.db.prepare('SELECT seq, message FROM mcp_events WHERE stream_key = ? AND seq > ? ORDER BY seq LIMIT ?'),
      pruneOld: this.db.prepare('DELETE FROM mcp_events WHERE created_at < ?'),
      pruneExtra: this.db.prepare(`
        DELETE FROM mcp_events WHERE seq IN (
          SELECT seq FROM (
            SELECT seq, ROW_NUMBER() OVER (PARTITION BY stream_key ORDER BY seq DESC) AS rn FROM mcp_events
          ) WHERE rn > ?
        )`),
      deletePrefix: this.db.prepare('DELETE FROM mcp_events WHERE substr(stream_key, 1, ?) = ?'),
    };
  }

  async storeEvent(streamId: StreamId, message: JSONRPCMessage): Promise<EventId> {
    this.assertOpen();
    const now = this.retention.now();
    const r = this.q.insert.run(streamId, JSON.stringify(message ?? {}), now);
    if (Number.isFinite(this.retention.maxEventsPerStream)) {
      this.q.trimStream.run(streamId, streamId, this.retention.maxEventsPerStream);
    }
    this.maybeAutoPrune(now);
    return String(r.lastInsertRowid);
  }

  async getStreamIdForEventId(eventId: EventId): Promise<StreamId | undefined> {
    this.assertOpen();
    const seq = parseSeq(eventId);
    if (seq === undefined) return undefined;
    const row = this.q.owner.get(seq) as { stream_key: string } | undefined;
    return row?.stream_key;
  }

  async replayEventsAfter(lastEventId: EventId, { send }: ReplaySink): Promise<StreamId> {
    const streamKey = await this.getStreamIdForEventId(lastEventId);
    if (streamKey === undefined) return '';
    // 큰 스트림도 메모리를 한꺼번에 쓰지 않도록 묶음 단위로 읽는다
    let cursor = Number(lastEventId);
    for (;;) {
      this.assertOpen();
      const rows = this.q.after.all(streamKey, cursor, REPLAY_BATCH) as Array<{ seq: number; message: string }>;
      for (const row of rows) {
        await send(String(row.seq), JSON.parse(row.message));
        cursor = row.seq;
      }
      if (rows.length < REPLAY_BATCH) break;
    }
    return streamKey;
  }

  forSession(sessionId: string) {
    return new SessionScopedEventStore(this, sessionId);
  }

  async deleteSession(sessionId: string): Promise<void> {
    this.assertOpen();
    const prefix = sessionKeyPrefix(sessionId);
    this.q.deletePrefix.run(prefix.length, prefix);
  }

  async prune(): Promise<number> {
    this.assertOpen();
    let removed = 0;
    if (Number.isFinite(this.retention.ttlMs)) {
      removed += Number(this.q.pruneOld.run(this.retention.now() - this.retention.ttlMs).changes);
    }
    if (Number.isFinite(this.retention.maxEventsPerStream)) {
      removed += Number(this.q.pruneExtra.run(this.retention.maxEventsPerStream).changes);
    }
    return removed;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.db.close();
  }

  private assertOpen(): void {
    if (this.closed) throw new Error('SqliteEventStore is closed');
  }

  // 저장 경로에서 TTL 의 절반마다 한 번 오래된 이벤트를 지운다(별도 타이머 없음)
  private maybeAutoPrune(now: number): void {
    if (!Number.isFinite(this.retention.ttlMs)) return;
    if (now - this.lastAutoPrune < this.retention.ttlMs / 2) return;
    this.lastAutoPrune = now;
    this.q.pruneOld.run(now - this.retention.ttlMs);
  }
}
