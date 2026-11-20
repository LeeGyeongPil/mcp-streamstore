// Stateful (Mcp-Session-Id) MCP server that any instance can serve.
//
//   node examples/stateful-multi-instance.ts          (Node >= 22.18 runs .ts files directly)
//   REDIS_URL=redis://localhost:6379 node examples/stateful-multi-instance.ts   (needs `npm install redis`)
//
// Routing rule of each instance:
//   1. no session header + initialize  -> new session; record it in the SessionStore once initialized
//   2. session this instance knows      -> its own transport
//   3. session this instance never saw  -> look it up in the SessionStore and rehydrate() a fresh transport
// Without step 3 the SDK answers "400 Bad Request: Server not initialized" (typescript-sdk #1658).
//
// Without REDIS_URL this runs on the practice fake (mock) Redis from `mcp-streamstore/testing`:
// an in-process imitation, not a real server. The routing code is the same either way.
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { WebStandardStreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js';
import {
  RedisEventStore,
  RedisSessionStore,
  captureSession,
  nodeRedisCommand,
  rehydrate,
  type RedisCommand,
  type SessionStore,
  type StreamStore,
} from 'mcp-streamstore';
import { createFakeRedis } from 'mcp-streamstore/testing';

function buildServer(instanceName: string): McpServer {
  const server = new McpServer({ name: 'stateful-example', version: '1.0.0' }, { capabilities: { logging: {} } });
  server.registerTool('whoami', { description: 'Says which instance answered' }, async () => ({
    content: [{ type: 'text', text: `served by ${instanceName}` }],
  }));
  return server;
}

/** One instance: returns a fetch-style handler you can mount on node:http, Hono, Express (via an adapter) etc. */
export function createInstance(instanceName: string, events: StreamStore, sessions: SessionStore) {
  interface Entry {
    transport: WebStandardStreamableHTTPServerTransport;
    server: McpServer;
    /** Pending write of the session record (awaited before answering, so B never looks it up too early). */
    saved?: Promise<void>;
  }
  const local = new Map<string, Entry>();

  async function open(sessionId: string | undefined): Promise<Entry> {
    const server = buildServer(instanceName);
    const transport: WebStandardStreamableHTTPServerTransport = new WebStandardStreamableHTTPServerTransport({
      sessionIdGenerator: () => sessionId ?? randomUUID(),
      // Session-scoped view: other sessions' "_GET_stream" events never replay into this one.
      eventStore: {
        storeEvent: (streamId, message) => events.forSession(transport.sessionId!).storeEvent(streamId, message),
        getStreamIdForEventId: (eventId) => events.forSession(transport.sessionId!).getStreamIdForEventId(eventId),
        replayEventsAfter: (eventId, sink) => events.forSession(transport.sessionId!).replayEventsAfter(eventId, sink),
      },
      onsessionclosed: async (id) => {
        local.delete(id);
        await sessions.delete(id);
        await events.deleteSession(id);
      },
    });
    const entry: Entry = { transport, server };
    // Record the session for the other instances once the client finished initialize.
    server.server.oninitialized = () => {
      entry.saved = sessions.put(captureSession(transport, server));
    };
    await server.connect(transport);
    return entry;
  }

  async function handle(request: Request): Promise<Response> {
    const sessionId = request.headers.get('mcp-session-id') ?? undefined;
    if (!sessionId) {
      const fresh = await open(undefined); // 1) a new session starts here
      const response = await fresh.transport.handleRequest(request);
      if (fresh.transport.sessionId) local.set(fresh.transport.sessionId, fresh);
      else await fresh.server.close(); // not an initialize request: nothing to keep
      return response;
    }
    let entry = local.get(sessionId); // 2) we already serve it
    if (!entry) {
      const record = await sessions.get(sessionId); // 3) another instance started it
      if (!record) return new Response('Session not found', { status: 404 });
      entry = await open(sessionId);
      rehydrate(entry.transport, entry.server, record);
      local.set(sessionId, entry);
    }
    await sessions.touch(sessionId);
    const response = await entry.transport.handleRequest(request);
    await entry.saved; // set while handling notifications/initialized
    return response;
  }

  async function close() {
    for (const { server } of local.values()) await server.close();
    local.clear();
  }

  return { handle, close };
}

// ------------------------------------------------------------------ self-check when run directly

async function connectRedis(): Promise<{ command: RedisCommand; label: string; close: () => Promise<void> }> {
  const url = process.env.REDIS_URL;
  if (!url) {
    const fake = createFakeRedis();
    return { command: fake.command, label: 'practice fake (mock) Redis', close: async () => undefined };
  }
  const specifier = 'redis'; // optional: npm install redis
  const { createClient } = await import(specifier);
  const client = createClient({ url, socket: { connectTimeout: 3000, reconnectStrategy: (n: number) => (n >= 2 ? new Error('gave up reconnecting') : 200 * (n + 1)) } });
  await client.connect();
  return { command: nodeRedisCommand(client), label: 'real Redis', close: () => client.close() };
}

const headers = { 'content-type': 'application/json', accept: 'application/json, text/event-stream', 'mcp-protocol-version': '2025-11-25' };
const post = (body: unknown, extra: Record<string, string> = {}) =>
  new Request('http://localhost/mcp', { method: 'POST', headers: { ...headers, ...extra }, body: JSON.stringify(body) });

async function selfCheck() {
  const redis = await connectRedis().catch((e: Error) => {
    console.error(`Could not connect to REDIS_URL: ${e.message}. Check that the server is running and the URL and password are right.`);
    process.exit(2);
  });
  const prefix = `stateful-example:${randomUUID()}:`;
  const events = new RedisEventStore({ command: redis.command, prefix });
  const sessions = new RedisSessionStore({ command: redis.command, prefix });
  const A = createInstance('instance A', events, sessions);
  const B = createInstance('instance B', events, sessions);
  try {
    console.log(`store: ${redis.label}`);
    const init = await A.handle(
      post({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-11-25', capabilities: { sampling: {} }, clientInfo: { name: 'demo', version: '1' } } }),
    );
    const sid = init.headers.get('mcp-session-id')!;
    await init.text();
    await (await A.handle(post({ jsonrpc: '2.0', method: 'notifications/initialized' }, { 'mcp-session-id': sid }))).text();
    console.log(`session ${sid.slice(0, 8)}... opened on instance A`);

    // The load balancer now sends the same session to B.
    const call = await B.handle(post({ jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'whoami', arguments: {} } }, { 'mcp-session-id': sid }));
    const text = await call.text();
    const answer = /served by [^"]+/.exec(text)?.[0] ?? text;
    console.log(`tools/call on instance B: HTTP ${call.status}, ${answer}`);
    if (call.status !== 200) process.exitCode = 1;

    // Ending the session on any instance removes its record and its events from the shared store.
    const end = await B.handle(new Request('http://localhost/mcp', { method: 'DELETE', headers: { ...headers, 'mcp-session-id': sid } }));
    await end.text();
    console.log(`DELETE on instance B: HTTP ${end.status}, record left in the store: ${(await sessions.get(sid)) ? 'yes' : 'no'}`);
  } finally {
    await A.close();
    await B.close();
    await redis.close();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await selfCheck();
