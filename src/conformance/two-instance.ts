// 한 프로세스 안에 MCP 서버 인스턴스 A·B 를 띄워 "A 에서 받다가 끊김 → B 로 Last-Event-ID 재연결" 을 실제 SDK 전송으로 재현한다.
// 네트워크 없이 웹 표준 Request/Response 를 전송의 handleRequest 에 직접 넘긴다(로드밸런서가 B 로 보낸 것과 같다).
// 적합성 묶음·테스트·데모가 모두 이 파일을 쓴다. SDK 는 선택 peer 라서 동적으로 import 한다.
import { randomUUID } from 'node:crypto';
import { captureSession, rehydrate } from '../sessions/rehydrate.js';
import type { EventStore, SessionRecord, SessionStore } from '../types.js';

export type SdkFlavor = 'v1' | 'v2';
export type InstanceName = 'A' | 'B';

const PROTOCOL = '2025-11-25'; // 프라이밍 이벤트(재개 지점)는 2025-11-25 이상 클라이언트에게만 나간다

// ---------------------------------------------------------------- SDK 불러오기

/** The pieces of one SDK major version the scenarios need. */
interface SdkKit {
  flavor: SdkFlavor;
  McpServer: new (info: { name: string; version: string }, opts: { capabilities: Record<string, unknown> }) => any;
  Transport: new (opts: Record<string, unknown>) => any;
  /** Send a notification related to the current tool call. */
  notify(extra: any, notification: { method: string; params: Record<string, unknown> }): Promise<void>;
}

const kits = new Map<SdkFlavor, Promise<SdkKit>>();

/** Load SDK v1 (`@modelcontextprotocol/sdk`) or v2 (`@modelcontextprotocol/server`). Throws a readable error if missing. */
export function loadSdk(flavor: SdkFlavor): Promise<SdkKit> {
  let p = kits.get(flavor);
  if (!p) {
    p = flavor === 'v1' ? loadV1() : loadV2();
    kits.set(flavor, p);
  }
  return p;
}

async function loadV1(): Promise<SdkKit> {
  try {
    const mcp = await import('@modelcontextprotocol/sdk/server/mcp.js');
    const web = await import('@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js');
    return {
      flavor: 'v1',
      McpServer: mcp.McpServer as any,
      Transport: web.WebStandardStreamableHTTPServerTransport as any,
      notify: (extra, n) => extra.sendNotification(n),
    };
  } catch (e) {
    throw new Error(`SDK v1 not available: install @modelcontextprotocol/sdk (${(e as Error).message})`);
  }
}

async function loadV2(): Promise<SdkKit> {
  try {
    const server = await import('@modelcontextprotocol/server');
    return {
      flavor: 'v2',
      McpServer: server.McpServer as any,
      Transport: server.WebStandardStreamableHTTPServerTransport as any,
      notify: (ctx, n) => ctx.mcpReq.notify(n),
    };
  } catch (e) {
    throw new Error(`SDK v2 not available: install @modelcontextprotocol/server (${(e as Error).message})`);
  }
}

/**
 * The SDK's own example store (`examples/shared/inMemoryEventStore.js` in @modelcontextprotocol/sdk v1),
 * used as the "before" picture in tests and the demo.
 */
export async function loadSdkExampleStore(): Promise<new () => EventStore> {
  const specifier = '@modelcontextprotocol/sdk/examples/shared/inMemoryEventStore.js';
  const mod = (await import(specifier)) as { InMemoryEventStore: new () => EventStore };
  return mod.InMemoryEventStore;
}

/** True when the given SDK can be imported. */
export async function hasSdk(flavor: SdkFlavor): Promise<boolean> {
  try {
    await loadSdk(flavor);
    return true;
  } catch {
    kits.delete(flavor);
    return false;
  }
}

// ---------------------------------------------------------------- 작은 HTTP·SSE 도구

export interface SseEvent {
  id?: string;
  /** Parsed `data:` JSON, or undefined for the empty priming event / keep-alive. */
  data?: any;
}

const BASE_HEADERS = {
  'content-type': 'application/json',
  accept: 'application/json, text/event-stream',
  'mcp-protocol-version': PROTOCOL,
};

function post(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request('http://mcp.local/mcp', { method: 'POST', headers: { ...BASE_HEADERS, ...headers }, body: JSON.stringify(body) });
}

function get(headers: Record<string, string>): Request {
  return new Request('http://mcp.local/mcp', { method: 'GET', headers: { ...BASE_HEADERS, ...headers } });
}

function parseBlock(block: string): SseEvent | undefined {
  let id: string | undefined;
  const data: string[] = [];
  for (const line of block.split('\n')) {
    if (line.startsWith('id:')) id = line.slice(3).trim();
    else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''));
  }
  if (id === undefined && data.length === 0) return undefined; // 주석(keep-alive) 블록
  const text = data.join('\n');
  return { id, data: text.trim() === '' ? undefined : JSON.parse(text) };
}

/**
 * Read an SSE response until `done(events)` is true, the stream ends, or `timeoutMs` passes; then cancel it.
 * Reading by count (not by sleeping) keeps the scenarios deterministic.
 */
export async function readSse(
  res: Response,
  done: (events: SseEvent[]) => boolean,
  timeoutMs = 5000,
  /** Stop when no new bytes arrive for this long (used after a replay, which is written before the response returns). */
  idleMs?: number,
): Promise<SseEvent[]> {
  const events: SseEvent[] = [];
  if (!res.body) return events;
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    reader.cancel().catch(() => undefined);
  }, timeoutMs);
  try {
    while (!done(events)) {
      let idleTimer: NodeJS.Timeout | undefined;
      const next = reader.read();
      const chunk = idleMs === undefined
        ? await next
        : await Promise.race([
            next,
            new Promise<'idle'>((r) => {
              idleTimer = setTimeout(() => r('idle'), idleMs);
            }),
          ]);
      clearTimeout(idleTimer);
      if (chunk === 'idle') break;
      const { done: end, value } = chunk;
      if (end) break;
      buf += decoder.decode(value, { stream: true });
      let i: number;
      while ((i = buf.indexOf('\n\n')) >= 0) {
        const ev = parseBlock(buf.slice(0, i));
        buf = buf.slice(i + 2);
        if (ev) events.push(ev);
        if (done(events)) break;
      }
    }
  } catch {
    // 시간 초과로 취소된 경우 등: 읽은 데까지 돌려준다
  } finally {
    clearTimeout(timer);
    if (!timedOut) await reader.cancel().catch(() => undefined); // 클라이언트가 연결을 끊는 것과 같다
  }
  return events;
}

const dataEvents = (evs: SseEvent[]) => evs.filter((e) => e.data !== undefined);
const kindOf = (data: any): string => (typeof data?.method === 'string' ? data.method : 'result' in (data ?? {}) ? 'result' : 'error');

async function until(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error('timed out waiting for the server');
    await new Promise((r) => setImmediate(r));
  }
}

/** Wraps an EventStore to see what instance A stored (to know when the tool's result is safely stored). */
class Recorder implements EventStore {
  readonly stored: any[] = [];
  constructor(private readonly inner: EventStore) {}
  async storeEvent(streamId: string, message: any) {
    const id = await this.inner.storeEvent(streamId, message);
    this.stored.push(message);
    return id;
  }
  getStreamIdForEventId = (id: string) => this.inner.getStreamIdForEventId?.(id) ?? Promise.resolve(undefined);
  replayEventsAfter(id: string, sink: { send: (id: string, m: any) => Promise<void> }) {
    return this.inner.replayEventsAfter(id, sink);
  }
}

// ---------------------------------------------------------------- 서버 인스턴스

interface Gate {
  wait: Promise<void>;
  open: () => void;
}
function gate(): Gate {
  let open!: () => void;
  const wait = new Promise<void>((r) => (open = r));
  return { wait, open };
}

/** An MCP server whose `work` tool sends 3 progress log notifications, pausing after the first one. */
function makeServer(kit: SdkKit, pause: Gate | undefined) {
  const server = new kit.McpServer({ name: 'streamstore-scenario', version: '0.0.0' }, { capabilities: { logging: {} } });
  server.registerTool('work', { description: 'Sends three progress notifications, then returns' }, async (...args: any[]) => {
    const extra = args[args.length - 1];
    for (let step = 1; step <= 3; step++) {
      await kit.notify(extra, { method: 'notifications/message', params: { level: 'info', data: `step ${step}` } });
      if (step === 1 && pause) await pause.wait; // 클라이언트가 첫 이벤트만 받고 끊기는 순간
    }
    return { content: [{ type: 'text', text: 'done' }] };
  });
  return server;
}

async function instance(kit: SdkKit, eventStore: EventStore, sessionId: string | undefined, pause?: Gate) {
  const transport = new kit.Transport({
    sessionIdGenerator: sessionId ? () => sessionId : undefined,
    eventStore,
  });
  const server = makeServer(kit, pause);
  await server.connect(transport);
  return { transport, server };
}

// ---------------------------------------------------------------- 시나리오 1: 재개

export interface ResumeScenarioOptions {
  sdk: SdkFlavor;
  /** Stateful (Mcp-Session-Id) or stateless serving. Default false. */
  stateful?: boolean;
  /** The EventStore each instance uses. Same backend for A and B = shared ledger. */
  eventStore: (instance: InstanceName, sessionId: string | undefined) => EventStore;
  /** Stateful only: where A saves the session record and B looks it up. */
  sessions?: SessionStore;
  /** Stateful only: after a plain resume attempt on B, rehydrate B from `sessions` and try again. */
  rehydrateOnB?: boolean;
  /** Capabilities the client declares in `initialize` (stateful). */
  clientCapabilities?: Record<string, unknown>;
}

export interface ResumeAttempt {
  label: 'plain' | 'rehydrated';
  status: number;
  /** Error body for non-200 answers. */
  body?: string;
  replayed: Array<{ id: string; kind: string }>;
}

export interface ResumeScenarioResult {
  sdk: SdkFlavor;
  sessionId?: string;
  /** Data events the client saw on A before disconnecting. */
  seenOnA: Array<{ id: string; kind: string }>;
  resumeFrom: string;
  attempts: ResumeAttempt[];
  record?: SessionRecord;
  clientCapabilitiesOnA?: unknown;
  clientCapabilitiesOnB?: unknown;
}

/**
 * A runs the `work` tool; the client reads the first notification and disconnects; the tool finishes on A
 * (its events go only to the store); then the client reconnects to B with `Last-Event-ID`.
 */
export async function runResumeScenario(opts: ResumeScenarioOptions): Promise<ResumeScenarioResult> {
  const kit = await loadSdk(opts.sdk);
  const stateful = opts.stateful ?? false;
  const sessionId = stateful ? randomUUID() : undefined;
  const sh: Record<string, string> = sessionId ? { 'mcp-session-id': sessionId } : {};
  const pause = gate();
  const toClose: Array<{ transport: any; server: any }> = [];
  const result: ResumeScenarioResult = { sdk: opts.sdk, sessionId, seenOnA: [], resumeFrom: '', attempts: [] };

  try {
    // --- 인스턴스 A
    const storeA = new Recorder(opts.eventStore('A', sessionId));
    const A = await instance(kit, storeA, sessionId, pause);
    toClose.push(A);
    if (stateful) {
      const init = await A.transport.handleRequest(
        post({
          jsonrpc: '2.0',
          id: 1,
          method: 'initialize',
          params: { protocolVersion: PROTOCOL, capabilities: opts.clientCapabilities ?? {}, clientInfo: { name: 'scenario-client', version: '1.0.0' } },
        }),
      );
      await init.text();
      await (await A.transport.handleRequest(post({ jsonrpc: '2.0', method: 'notifications/initialized' }, sh))).text();
      // 세션 기록 남기기: 다른 인스턴스가 이 세션을 다시 세울 때 쓴다
      result.record = captureSession(A.transport, A.server, { protocolVersion: PROTOCOL });
      result.clientCapabilitiesOnA = A.server.server.getClientCapabilities();
      if (opts.sessions) await opts.sessions.put(result.record);
    }
    const call = await A.transport.handleRequest(post({ jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'work', arguments: {} } }, sh));
    if (call.status !== 200) throw new Error(`tools/call on A failed: ${call.status} ${await call.text()}`);
    // 첫 알림 하나만 받고 연결을 끊는다
    const seen = dataEvents(await readSse(call, (evs) => dataEvents(evs).length >= 1));
    result.seenOnA = seen.map((e) => ({ id: e.id ?? '', kind: kindOf(e.data) }));
    result.resumeFrom = seen[0]?.id ?? '';
    if (!result.resumeFrom) throw new Error('A did not send an event with an ID (is the store returning IDs?)');
    // 끊긴 뒤 도구가 마저 실행된다 → 나머지 알림 2개와 결과는 저장소에만 남는다
    pause.open();
    // tools/call(id 2) 의 결과가 저장될 때까지 기다린다. 세션형이면 initialize(id 1) 응답도 결과로 저장되므로
    // "result 가 하나라도 있나" 로 기다리면 너무 일찍 B 로 넘어간다(느린 실제 Redis 에서 재생 2개로 드러남).
    await until(() => storeA.stored.some((m) => m && m.id === 2 && 'result' in m));

    // --- 인스턴스 B (다른 프로세스라고 가정: 새 전송·새 서버·새 저장소 핸들)
    const resumeHeaders = { ...sh, 'last-event-id': result.resumeFrom };
    const B = await instance(kit, opts.eventStore('B', sessionId), sessionId);
    toClose.push(B);
    result.attempts.push(await resume(B.transport, resumeHeaders, 'plain'));
    if (stateful && opts.rehydrateOnB) {
      const record = opts.sessions ? await opts.sessions.get(sessionId!) : result.record;
      if (!record) throw new Error('session record not found in the SessionStore');
      rehydrate(B.transport, B.server, record);
      result.clientCapabilitiesOnB = B.server.server.getClientCapabilities();
      result.attempts.push(await resume(B.transport, resumeHeaders, 'rehydrated'));
    }
    return result;
  } finally {
    pause.open();
    for (const { transport, server } of toClose) {
      await server.close().catch(() => undefined);
      await transport.close().catch(() => undefined);
    }
  }
}

async function resume(transport: any, headers: Record<string, string>, label: ResumeAttempt['label']): Promise<ResumeAttempt> {
  const res: Response = await transport.handleRequest(get(headers));
  if (res.status !== 200) return { label, status: res.status, body: await res.text(), replayed: [] };
  // 재생은 응답을 돌려주기 전에 스트림에 모두 쓰인다. 결과(result)가 오거나 더 읽을 것이 없을 때까지 읽는다.
  const evs = await readSse(res, (e) => dataEvents(e).some((x) => kindOf(x.data) === 'result'), 3000, 150);
  return { label, status: res.status, replayed: dataEvents(evs).map((e) => ({ id: e.id ?? '', kind: kindOf(e.data) })) };
}

// ---------------------------------------------------------------- 시나리오 2: 세션 섞임

export interface LeakScenarioOptions {
  sdk: SdkFlavor;
  /** Store shared by both sessions. */
  store: EventStore & { forSession?: (sessionId: string) => EventStore };
  /** Give each session `store.forSession(sessionId)` instead of the bare store. */
  scoped: boolean;
}

export interface LeakScenarioResult {
  /** Messages (log `data`) replayed into session X when it resumes its GET stream. */
  replayedIntoX: string[];
  /** Status when X resumes with an event ID that belongs to session Y. */
  crossSessionStatus: number;
}

/**
 * Two sessions X and Y on one shared store. Both get server notifications on their standalone GET stream,
 * which the SDK always names "_GET_stream". X disconnects, Y gets a notification, X resumes.
 */
export async function runLeakScenario(opts: LeakScenarioOptions): Promise<LeakScenarioResult> {
  const kit = await loadSdk(opts.sdk);
  const open: Array<{ transport: any; server: any }> = [];
  const pick = (sid: string) => {
    if (!opts.scoped) return opts.store;
    if (typeof opts.store.forSession !== 'function') throw new TypeError('store has no forSession()');
    return opts.store.forSession(sid);
  };
  const say = (server: any, text: string) =>
    server.server.notification({ method: 'notifications/message', params: { level: 'info', data: text } });

  async function openSession() {
    const sid = randomUUID();
    const node = await instance(kit, pick(sid), sid);
    open.push(node);
    const h = { 'mcp-session-id': sid };
    await (
      await node.transport.handleRequest(
        post({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: PROTOCOL, capabilities: {}, clientInfo: { name: 'c', version: '1' } } }),
      )
    ).text();
    await (await node.transport.handleRequest(post({ jsonrpc: '2.0', method: 'notifications/initialized' }, h))).text();
    return { sid, h, ...node };
  }

  try {
    const X = await openSession();
    const Y = await openSession();

    // X 와 Y 가 각자 GET 스트림을 열고 알림 하나씩 받는다(이벤트 ID 확보)
    const xGet = await X.transport.handleRequest(get(X.h));
    const xFirstP = readSse(xGet, (e) => dataEvents(e).length >= 1);
    await say(X.server, 'x1');
    const xFirst = dataEvents(await xFirstP)[0]!; // 읽은 뒤 끊김
    const yGet = await Y.transport.handleRequest(get(Y.h));
    const yFirstP = readSse(yGet, (e) => dataEvents(e).length >= 1);
    await say(Y.server, 'y1');
    const yFirst = dataEvents(await yFirstP)[0]!;

    // X 가 끊긴 동안: Y 에게 알림 하나, X 에게 알림 하나 (둘 다 "_GET_stream" 이름으로 저장된다)
    await say(Y.server, 'y2-private-to-Y');
    await say(X.server, 'x2');

    // X 재연결: 재생분은 응답 전에 스트림에 쓰인다. 이어서 "끝" 표시를 보내 그 앞까지만 읽는다.
    const resumed = await X.transport.handleRequest(get({ ...X.h, 'last-event-id': xFirst.id! }));
    const replayedP = readSse(resumed, (e) => dataEvents(e).some((x) => x.data?.params?.data === 'end'));
    await say(X.server, 'end');
    const replayedIntoX = dataEvents(await replayedP)
      .map((e) => String(e.data?.params?.data))
      .filter((t) => t !== 'end');

    // X 세션 헤더로 Y 의 이벤트 ID 를 내밀면?
    const cross = await X.transport.handleRequest(get({ ...X.h, 'last-event-id': yFirst.id! }));
    const crossSessionStatus = cross.status;
    if (cross.body) await readSse(cross, () => true, 200);
    return { replayedIntoX, crossSessionStatus };
  } finally {
    for (const { transport, server } of open) {
      await server.close().catch(() => undefined);
      await transport.close().catch(() => undefined);
    }
  }
}
