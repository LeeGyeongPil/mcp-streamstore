// 세션 다시 세우기(typescript-sdk #1658 우회).
// 상태형(세션형) 전송은 initialize 를 받은 인스턴스만 세션을 안다. 다른 인스턴스의 새 전송은
// "400 Bad Request: Server not initialized" 를 돌려주고, 이를 고칠 공개 API 가 아직 없다.
// 그래서 SDK 비공개 필드(전송의 sessionId·_initialized, 서버의 _clientCapabilities 등)를 직접 채운다.
// 비공개 필드는 SDK 업데이트로 바뀔 수 있으므로 이 파일 한곳에 가두고, 모양이 다르면 조용히 넘어가지 않고
// RehydrateUnsupportedError 를 던진다(설치된 SDK 버전을 메시지에 담는다).
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { SessionRecord } from '../types.js';

/** SDK versions this helper was tested against. */
export const TESTED_SDK_VERSIONS = { '@modelcontextprotocol/sdk': '1.32.2', '@modelcontextprotocol/server': '2.4.0' } as const;

export class RehydrateUnsupportedError extends Error {
  constructor(reason: string) {
    super(
      `rehydrate: ${reason}. Installed: ${describeInstalledSdks()}; tested with ` +
        `@modelcontextprotocol/sdk ${TESTED_SDK_VERSIONS['@modelcontextprotocol/sdk']} and ` +
        `@modelcontextprotocol/server ${TESTED_SDK_VERSIONS['@modelcontextprotocol/server']}. ` +
        'Please open an issue with the SDK version.',
    );
    this.name = 'RehydrateUnsupportedError';
  }
}

// 전송 객체에서 우리가 만지는 부분만 적은 모양
interface WebStandardTransportInternals {
  sessionId?: string;
  sessionIdGenerator?: unknown;
  _initialized: boolean;
}

// 서버(Server 또는 McpServer.server)에서 만지는 부분
interface ServerInternals {
  getClientCapabilities(): unknown;
  getClientVersion?(): unknown;
  getNegotiatedProtocolVersion?(): string | undefined;
  _clientCapabilities?: unknown;
  _clientVersion?: unknown;
  _negotiatedProtocolVersion?: string;
}

/**
 * Snapshot what another instance needs to serve this session.
 * Call it after the client finished `initialize` (for example in `server.server.oninitialized`).
 * SDK v1 does not remember the negotiated protocol version; pass it via `opts.protocolVersion` if you need it.
 */
export function captureSession(transport: unknown, server: unknown, opts: { protocolVersion?: string; now?: () => number } = {}): SessionRecord {
  const t = unwrapTransport(transport);
  const s = unwrapServer(server);
  if (typeof t.sessionId !== 'string' || t.sessionId === '') {
    throw new RehydrateUnsupportedError('transport has no sessionId yet (is it stateful, and has initialize completed?)');
  }
  const caps = s.getClientCapabilities();
  if (caps === undefined) {
    throw new RehydrateUnsupportedError('server has no client capabilities yet (call captureSession after initialize)');
  }
  const now = (opts.now ?? Date.now)();
  const negotiated = typeof s.getNegotiatedProtocolVersion === 'function' ? s.getNegotiatedProtocolVersion() : undefined;
  const clientInfo = typeof s.getClientVersion === 'function' ? s.getClientVersion() : undefined;
  return {
    sessionId: t.sessionId,
    protocolVersion: opts.protocolVersion ?? negotiated ?? 'unknown',
    clientCapabilities: structuredClone(caps) as Record<string, unknown>,
    ...(clientInfo ? { clientInfo: structuredClone(clientInfo) as Record<string, unknown> } : {}),
    createdAt: now,
    lastSeenAt: now,
  };
}

/**
 * Make a fresh stateful transport + server on this instance serve an existing session.
 * Call it after `server.connect(transport)` and before `transport.handleRequest(...)`.
 * Supported: SDK v1 `StreamableHTTPServerTransport` (Node) and `WebStandardStreamableHTTPServerTransport`,
 * SDK v2 `WebStandardStreamableHTTPServerTransport`.
 */
export function rehydrate(transport: unknown, server: unknown, record: SessionRecord): void {
  if (!record || typeof record.sessionId !== 'string' || record.sessionId === '') {
    throw new TypeError('rehydrate: record.sessionId must be a non-empty string');
  }
  const t = unwrapTransport(transport);
  const s = unwrapServer(server);
  if (typeof t.sessionIdGenerator !== 'function') {
    throw new RehydrateUnsupportedError('transport is stateless (no sessionIdGenerator); only stateful transports have sessions to rehydrate');
  }
  if (t._initialized === true && t.sessionId !== record.sessionId) {
    throw new RehydrateUnsupportedError(`transport already serves session ${String(t.sessionId)}`);
  }
  // 1) 전송: 이 세션 ID 로 이미 initialize 를 마친 것처럼 만든다
  t.sessionId = record.sessionId;
  t._initialized = true;
  // 2) 서버: 클라이언트 능력·정보를 되살린다(샘플링·엘리시테이션 같은 능력 검사가 원래대로 동작하도록)
  s._clientCapabilities = structuredClone(record.clientCapabilities);
  if (record.clientInfo !== undefined) s._clientVersion = structuredClone(record.clientInfo);
  // 3) v2 서버만 협상된 프로토콜 버전을 기억한다(코덱 선택에 쓰임)
  if ('_negotiatedProtocolVersion' in s && record.protocolVersion && record.protocolVersion !== 'unknown') {
    s._negotiatedProtocolVersion = record.protocolVersion;
  }
}

function unwrapTransport(transport: unknown): WebStandardTransportInternals {
  if (!transport || typeof transport !== 'object') throw new TypeError('rehydrate: transport must be an object');
  // v1 Node 용 StreamableHTTPServerTransport 는 웹 표준 전송을 _webStandardTransport 에 감싸 둔다
  const inner = (transport as { _webStandardTransport?: unknown })._webStandardTransport;
  const t = (inner && typeof inner === 'object' ? inner : transport) as Record<string, unknown>;
  if (!('_initialized' in t) || typeof t._initialized !== 'boolean') {
    throw new RehydrateUnsupportedError('transport has no boolean _initialized field (unsupported transport or SDK shape)');
  }
  if (!('sessionIdGenerator' in t)) {
    throw new RehydrateUnsupportedError('transport has no sessionIdGenerator field (unsupported transport or SDK shape)');
  }
  return t as unknown as WebStandardTransportInternals;
}

function unwrapServer(server: unknown): ServerInternals {
  if (!server || typeof server !== 'object') throw new TypeError('rehydrate: server must be an object');
  // McpServer 는 저수준 Server 를 .server 에 둔다
  const inner = (server as { server?: unknown }).server;
  const s = (inner && typeof inner === 'object' && typeof (inner as ServerInternals).getClientCapabilities === 'function'
    ? inner
    : server) as ServerInternals;
  if (typeof s.getClientCapabilities !== 'function') {
    throw new RehydrateUnsupportedError('server has no getClientCapabilities() (pass an McpServer or Server)');
  }
  return s;
}

// --- 설치된 SDK 버전 찾기(오류 메시지용). 패키지 진입 파일에서 위로 올라가며 package.json 을 찾는다 ---

const versionCache = new Map<string, string | undefined>();

export function installedSdkVersion(name: '@modelcontextprotocol/sdk' | '@modelcontextprotocol/server'): string | undefined {
  if (versionCache.has(name)) return versionCache.get(name);
  let version: string | undefined;
  try {
    const entry = name === '@modelcontextprotocol/sdk' ? `${name}/server/index.js` : name;
    let dir = dirname(fileURLToPath(import.meta.resolve(entry)));
    for (let i = 0; i < 8; i++) {
      try {
        const pkg = JSON.parse(readFileSync(join(dir, 'package.json'), 'utf8')) as { name?: string; version?: string };
        if (pkg.name === name) {
          version = pkg.version;
          break;
        }
      } catch {
        // 이 폴더에는 package.json 이 없다 → 한 단계 위로
      }
      const up = dirname(dir);
      if (up === dir) break;
      dir = up;
    }
  } catch {
    version = undefined; // 설치되지 않음
  }
  versionCache.set(name, version);
  return version;
}

function describeInstalledSdks(): string {
  const v1 = installedSdkVersion('@modelcontextprotocol/sdk');
  const v2 = installedSdkVersion('@modelcontextprotocol/server');
  return `@modelcontextprotocol/sdk ${v1 ?? 'not installed'}, @modelcontextprotocol/server ${v2 ?? 'not installed'}`;
}
