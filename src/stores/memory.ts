// 메모리 저장소: 한 프로세스 안에서만 쓰는 개선판.
// SDK 예제 InMemoryEventStore 와 달리 (1) 전역 단조 카운터로 순서를 보장하고 (2) 보존 정책으로 상한이 있다.
// 프로세스가 바뀌면 기록이 사라지므로 여러 인스턴스·재시작에는 SqliteEventStore / RedisEventStore 를 쓴다.
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

interface StoredEvent {
  seq: number;
  message: string; // JSON 문자열로 보관해 호출자가 원본 객체를 바꿔도 기록이 변하지 않게 한다
  at: number;
}

export class MemoryEventStore implements StreamStore {
  private readonly retention: Required<RetentionOptions>;
  private seq = 0;
  private readonly streams = new Map<string, StoredEvent[]>(); // 스트림 키 → 이벤트(오름차순)
  private readonly owner = new Map<number, string>(); // seq → 스트림 키 (이벤트 ID 역참조)
  private lastAutoPrune = 0;

  constructor(opts: RetentionOptions = {}) {
    this.retention = resolveRetention(opts);
  }

  async storeEvent(streamId: StreamId, message: JSONRPCMessage): Promise<EventId> {
    const now = this.retention.now();
    const seq = ++this.seq;
    let list = this.streams.get(streamId);
    if (!list) {
      list = [];
      this.streams.set(streamId, list);
    }
    list.push({ seq, message: JSON.stringify(message ?? {}), at: now });
    this.owner.set(seq, streamId);
    // 스트림당 최대 개수: 가장 오래된 것부터 버린다
    while (list.length > this.retention.maxEventsPerStream) {
      const dropped = list.shift();
      if (dropped) this.owner.delete(dropped.seq);
    }
    this.maybeAutoPrune(now);
    return String(seq);
  }

  async getStreamIdForEventId(eventId: EventId): Promise<StreamId | undefined> {
    const seq = parseSeq(eventId);
    return seq === undefined ? undefined : this.owner.get(seq);
  }

  async replayEventsAfter(lastEventId: EventId, { send }: ReplaySink): Promise<StreamId> {
    const seq = parseSeq(lastEventId);
    const streamId = seq === undefined ? undefined : this.owner.get(seq);
    if (seq === undefined || streamId === undefined) return '';
    // 복사본을 돌면서 보낸다: send 도중 새 이벤트가 들어와도 순회가 깨지지 않는다
    const list = [...(this.streams.get(streamId) ?? [])];
    for (const ev of list) {
      if (ev.seq > seq) await send(String(ev.seq), JSON.parse(ev.message));
    }
    return streamId;
  }

  forSession(sessionId: string) {
    return new SessionScopedEventStore(this, sessionId);
  }

  async deleteSession(sessionId: string): Promise<void> {
    const prefix = sessionKeyPrefix(sessionId);
    for (const [key, list] of this.streams) {
      if (!key.startsWith(prefix)) continue;
      for (const ev of list) this.owner.delete(ev.seq);
      this.streams.delete(key);
    }
  }

  async prune(): Promise<number> {
    const cutoff = this.retention.now() - this.retention.ttlMs;
    let removed = 0;
    for (const [key, list] of this.streams) {
      const keep = list.filter((ev) => ev.at >= cutoff);
      const extra = Math.max(0, keep.length - this.retention.maxEventsPerStream);
      const kept = keep.slice(extra);
      const keptSeqs = new Set(kept.map((ev) => ev.seq));
      for (const ev of list) {
        if (!keptSeqs.has(ev.seq)) {
          this.owner.delete(ev.seq);
          removed++;
        }
      }
      if (kept.length === 0) this.streams.delete(key);
      else this.streams.set(key, kept);
    }
    return removed;
  }

  async close(): Promise<void> {
    this.streams.clear();
    this.owner.clear();
  }

  // TTL 의 절반마다 저장 경로에서 한 번 정리한다(타이머 없이 상한 유지 — 닫을 자원이 생기지 않는다)
  private maybeAutoPrune(now: number): void {
    if (!Number.isFinite(this.retention.ttlMs)) return;
    if (now - this.lastAutoPrune < this.retention.ttlMs / 2) return;
    this.lastAutoPrune = now;
    void this.prune();
  }
}

/** Event IDs of the memory and SQLite stores are positive integers written in decimal. */
export function parseSeq(eventId: string): number | undefined {
  if (!/^[1-9][0-9]{0,15}$/.test(eventId)) return undefined;
  const n = Number(eventId);
  return Number.isSafeInteger(n) ? n : undefined;
}
