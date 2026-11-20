// 세션 범위 키: "<인코딩된 세션 ID>/<스트림 ID>".
// SDK 는 모든 세션의 독립 GET 스트림에 같은 ID "_GET_stream" 을 쓰므로(1.32.2·2.4.0 확인),
// 저장소를 여러 세션이 공유하면 스트림 ID 만으로는 세션을 구분할 수 없다. 그래서 세션 ID 를 키 앞에 붙인다.
import type { EventId, EventStore, JSONRPCMessage, ReplaySink, StreamId } from './types.js';

const SEP = '/';

/** Build the stored stream key for a session-scoped stream. */
export function scopeKey(sessionId: string, streamId: StreamId): string {
  if (!sessionId) throw new TypeError('sessionId must be a non-empty string');
  // encodeURIComponent 는 "/" 를 %2F 로 바꾸므로, 첫 "/" 가 항상 경계가 된다
  return `${encodeURIComponent(sessionId)}${SEP}${streamId}`;
}

/** Split a stored stream key back into session and stream. Returns undefined for unscoped keys. */
export function parseScopeKey(key: string): { sessionId: string; streamId: StreamId } | undefined {
  const i = key.indexOf(SEP);
  if (i <= 0) return undefined;
  try {
    return { sessionId: decodeURIComponent(key.slice(0, i)), streamId: key.slice(i + 1) };
  } catch {
    return undefined;
  }
}

/** Prefix shared by every stream key of one session (used by deleteSession). */
export function sessionKeyPrefix(sessionId: string): string {
  return `${encodeURIComponent(sessionId)}${SEP}`;
}

/** The raw operations a backend exposes; keys here are full stored stream keys. */
export interface KeyedBackend {
  storeEvent(streamKey: string, message: JSONRPCMessage): Promise<EventId>;
  getStreamIdForEventId(eventId: EventId): Promise<string | undefined>;
  replayEventsAfter(lastEventId: EventId, sink: ReplaySink): Promise<string>;
}

/**
 * A session-scoped view over a backend. The SDK transport only ever sees the plain stream ID
 * (for example "_GET_stream"), while the backend stores "<session>/_GET_stream".
 */
export class SessionScopedEventStore implements EventStore {
  constructor(
    private readonly backend: KeyedBackend,
    readonly sessionId: string,
  ) {
    if (!sessionId) throw new TypeError('sessionId must be a non-empty string');
  }

  storeEvent(streamId: StreamId, message: JSONRPCMessage): Promise<EventId> {
    return this.backend.storeEvent(scopeKey(this.sessionId, streamId), message);
  }

  async getStreamIdForEventId(eventId: EventId): Promise<StreamId | undefined> {
    const key = await this.backend.getStreamIdForEventId(eventId);
    if (key === undefined) return undefined;
    const parsed = parseScopeKey(key);
    // 다른 세션(또는 범위 없는 스트림)의 이벤트 ID 는 "모르는 ID" 로 취급한다 → 전송이 400 을 돌려준다
    if (!parsed || parsed.sessionId !== this.sessionId) return undefined;
    return parsed.streamId;
  }

  async replayEventsAfter(lastEventId: EventId, sink: ReplaySink): Promise<StreamId> {
    // 먼저 세션 소유를 확인한 다음에만 재생한다(다른 세션의 기록을 한 줄도 보내지 않기 위해)
    if ((await this.getStreamIdForEventId(lastEventId)) === undefined) return '';
    const key = await this.backend.replayEventsAfter(lastEventId, sink);
    return parseScopeKey(key)?.streamId ?? '';
  }
}
