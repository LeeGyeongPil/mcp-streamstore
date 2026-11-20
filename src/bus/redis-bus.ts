// v2 createMcpHandler 의 ServerEventBus 를 Redis pub/sub 으로 노드 사이에 잇는다.
// - publish(event): 같은 프로세스의 리스너에는 곧바로(동기) 전달하고, 다른 노드에는 채널로 발행한다.
// - 채널로 받은 메시지 중 "내가 보낸 것"(인스턴스 ID 가 같음)은 버린다 → 로컬 리스너가 같은 이벤트를 두 번 받지 않는다.
// SDK 계약: subscribe 가 돌려주는 해제 함수는 여러 번 불러도 안전해야 하고, 발행한 리스너 자신에게 되돌려 보내면 안 된다.
import { randomUUID } from 'node:crypto';

/** Same shape as the v2 SDK `ServerEvent` (kept structural so the SDK is not imported at runtime). */
export type ServerEvent = (
  | { kind: 'tools_list_changed' }
  | { kind: 'prompts_list_changed' }
  | { kind: 'resources_list_changed' }
  | { kind: 'resource_updated'; uri: string }
) & { params?: Record<string, unknown> };

/** Same shape as the v2 SDK `ServerEventBus`. */
export interface ServerEventBus {
  publish(event: ServerEvent): void;
  subscribe(listener: (event: ServerEvent) => void): () => void;
}

export interface RedisServerEventBusOptions {
  /** Publish one message on a channel (e.g. `client.publish`). */
  publish: (channel: string, message: string) => Promise<unknown>;
  /** Subscribe to a channel on a dedicated connection; resolves with an unsubscribe function. */
  subscribe: (channel: string, onMessage: (message: string) => void) => Promise<() => Promise<void>>;
  /** Channel name. Default "mcp:server-events". */
  channel?: string;
  /** This node's ID, used to drop our own messages when they come back. Default: random UUID. */
  instanceId?: string;
  /** Called for publish/subscribe failures and malformed messages. Default: ignore. */
  onerror?: (error: Error) => void;
}

interface Envelope {
  v: 1;
  from: string;
  event: ServerEvent;
}

const KINDS = new Set(['tools_list_changed', 'prompts_list_changed', 'resources_list_changed', 'resource_updated']);

export class RedisServerEventBus implements ServerEventBus {
  readonly instanceId: string;
  readonly channel: string;
  private readonly listeners = new Set<(event: ServerEvent) => void>();
  private readonly opts: RedisServerEventBusOptions;
  private readyPromise: Promise<void> | undefined;
  private unsubscribeRemote: (() => Promise<void>) | undefined;
  private closed = false;

  constructor(opts: RedisServerEventBusOptions) {
    if (!opts || typeof opts.publish !== 'function' || typeof opts.subscribe !== 'function') {
      throw new TypeError('RedisServerEventBus: options.publish and options.subscribe are required');
    }
    this.opts = opts;
    this.channel = opts.channel ?? 'mcp:server-events';
    this.instanceId = opts.instanceId ?? randomUUID();
  }

  /** Subscribe to the channel. Called automatically on the first `subscribe()`; await it at startup to not miss events. */
  ready(): Promise<void> {
    if (this.closed) return Promise.reject(new Error('RedisServerEventBus is closed'));
    this.readyPromise ??= this.opts
      .subscribe(this.channel, (message) => this.onRemote(message))
      .then(async (unsubscribe) => {
        if (this.closed) {
          await unsubscribe(); // ready 중에 close 됐다면 바로 해제
          return;
        }
        this.unsubscribeRemote = unsubscribe;
      });
    return this.readyPromise;
  }

  publish(event: ServerEvent): void {
    if (this.closed) return;
    // 1) 이 노드의 리스너: 기본 InMemoryServerEventBus 와 같이 동기 전달
    this.deliver(event);
    // 2) 다른 노드: 발행(실패는 onerror 로만 알린다 — publish 는 SDK 계약상 동기 void)
    const envelope: Envelope = { v: 1, from: this.instanceId, event };
    this.opts.publish(this.channel, JSON.stringify(envelope)).catch((e: unknown) => this.report(e));
  }

  subscribe(listener: (event: ServerEvent) => void): () => void {
    this.listeners.add(listener);
    if (!this.closed) this.ready().catch((e: unknown) => this.report(e));
    // 해제 함수는 여러 번 불러도 안전하다(Set.delete 는 멱등)
    return () => {
      this.listeners.delete(listener);
    };
  }

  /** Number of local listeners (tests/introspection). */
  get listenerCount(): number {
    return this.listeners.size;
  }

  /** Unsubscribe from the channel and drop every local listener. Safe to call twice. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.listeners.clear();
    if (this.readyPromise) await this.readyPromise.catch(() => undefined);
    const unsubscribe = this.unsubscribeRemote;
    this.unsubscribeRemote = undefined;
    if (unsubscribe) await unsubscribe();
  }

  private onRemote(message: string): void {
    if (this.closed) return;
    let envelope: Envelope;
    try {
      envelope = JSON.parse(message) as Envelope;
    } catch {
      this.report(new Error('RedisServerEventBus: ignored a malformed message'));
      return;
    }
    if (envelope?.v !== 1 || typeof envelope.from !== 'string' || !envelope.event || !KINDS.has(envelope.event.kind)) {
      this.report(new Error('RedisServerEventBus: ignored a message with an unknown shape'));
      return;
    }
    if (envelope.from === this.instanceId) return; // 내가 보낸 것: 로컬에는 publish 때 이미 전달했다
    this.deliver(envelope.event);
  }

  private deliver(event: ServerEvent): void {
    // 순회 중 리스너가 스스로 해제해도 안전하도록 복사본을 돈다. 한 리스너가 던져도 나머지는 받는다.
    for (const listener of [...this.listeners]) {
      try {
        listener(event);
      } catch (e) {
        this.report(e);
      }
    }
  }

  private report(e: unknown): void {
    const err = e instanceof Error ? e : new Error(String(e));
    try {
      this.opts.onerror?.(err);
    } catch {
      // onerror 자체의 오류는 삼킨다
    }
  }
}
