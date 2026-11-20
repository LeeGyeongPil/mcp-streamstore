// SDK v2 modern path: stateless createMcpHandler nodes that share list-changed events through Redis pub/sub.
//
//   node examples/v2-bus.ts            (Node >= 22.18 runs .ts files directly)
//   REDIS_URL=redis://localhost:6379 node examples/v2-bus.ts   (needs `npm install redis`)
//
// createMcpHandler has no eventStore; its extension point is `bus`. The default bus only reaches
// subscriptions/listen streams opened on the same process. RedisServerEventBus carries
// notify.toolsChanged() from node X to the listen stream a client holds open on node Y,
// and drops its own messages when they come back from the channel (no duplicates on X).
//
// Without REDIS_URL this uses the practice fake (mock) pub/sub from `mcp-streamstore/testing`
// (in-process imitation, not a real server).
import { pathToFileURL } from 'node:url';
import { McpServer, createMcpHandler } from '@modelcontextprotocol/server';
import { RedisServerEventBus, nodeRedisPubSub, type PubSub } from 'mcp-streamstore';
import { createFakeRedis } from 'mcp-streamstore/testing';

/** One node of the cluster. Every node builds the same handler with its own bus object. */
export function createNode(name: string, bus: RedisServerEventBus) {
  return createMcpHandler(
    () => {
      const server = new McpServer({ name, version: '1.0.0' }, { capabilities: { tools: { listChanged: true } } });
      server.registerTool('ping', { description: 'answers pong' }, async () => ({ content: [{ type: 'text', text: 'pong' }] }));
      return server;
    },
    { bus },
  );
}

// ------------------------------------------------------------------ self-check when run directly

async function connectPubSub(): Promise<{ pubsub: () => Promise<PubSub>; label: string; close: () => Promise<void> }> {
  const url = process.env.REDIS_URL;
  if (!url) {
    const fake = createFakeRedis();
    return { pubsub: async () => fake, label: 'practice fake (mock) Redis pub/sub', close: async () => undefined };
  }
  const specifier = 'redis'; // optional: npm install redis
  const { createClient } = await import(specifier);
  const publisher = createClient({ url, socket: { connectTimeout: 3000, reconnectStrategy: (n: number) => (n >= 2 ? new Error('gave up reconnecting') : 200 * (n + 1)) } });
  await publisher.connect();
  const connections = [publisher];
  return {
    label: 'real Redis pub/sub',
    // a subscribed connection cannot send other commands, so every bus gets its own
    pubsub: async () => {
      const subscriber = publisher.duplicate();
      await subscriber.connect();
      connections.push(subscriber);
      return nodeRedisPubSub(publisher, subscriber);
    },
    close: async () => {
      for (const c of connections) await c.close();
    },
  };
}

// The raw subscriptions/listen request a 2026-07-28 client sends.
function listen(id: string): Request {
  const meta = {
    'io.modelcontextprotocol/protocolVersion': '2026-07-28',
    'io.modelcontextprotocol/clientInfo': { name: 'v2-bus-example', version: '1.0.0' },
    'io.modelcontextprotocol/clientCapabilities': {},
  };
  return new Request('http://localhost/mcp', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'application/json, text/event-stream',
      'mcp-method': 'subscriptions/listen',
      'mcp-protocol-version': '2026-07-28',
    },
    body: JSON.stringify({ jsonrpc: '2.0', id, method: 'subscriptions/listen', params: { _meta: meta, notifications: { toolsListChanged: true } } }),
  });
}

// Read notification methods from a listen stream until `stop` returns true (or 5 s pass).
async function readMethods(response: Response, stop: (methods: string[]) => boolean, onMethod: (method: string) => void = () => undefined): Promise<string[]> {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  const methods: string[] = [];
  let buffer = '';
  const timer = setTimeout(() => void reader.cancel(), 5000);
  try {
    while (!stop(methods)) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      for (let end = buffer.indexOf('\n\n'); end >= 0; end = buffer.indexOf('\n\n')) {
        const data = /^data: ?(.*)$/m.exec(buffer.slice(0, end))?.[1];
        buffer = buffer.slice(end + 2);
        if (data) {
          const method: string = JSON.parse(data).method ?? 'response';
          methods.push(method);
          onMethod(method);
        }
      }
    }
  } finally {
    clearTimeout(timer);
    await reader.cancel().catch(() => undefined);
  }
  return methods;
}

async function selfCheck() {
  const redis = await connectPubSub().catch((e: Error) => {
    console.error(`Could not connect to REDIS_URL: ${e.message}. Check that the server is running and the URL and password are right.`);
    process.exit(2);
  });
  const channel = `v2-bus-example:${crypto.randomUUID()}`;
  const busX = new RedisServerEventBus({ ...(await redis.pubsub()), channel });
  const busY = new RedisServerEventBus({ ...(await redis.pubsub()), channel });
  await Promise.all([busX.ready(), busY.ready()]); // subscribe at startup so the first event is not missed
  const X = createNode('node-x', busX);
  const Y = createNode('node-y', busY);
  try {
    console.log(`bus: ${redis.label}`);
    const TOOLS = 'notifications/tools/list_changed';
    // A client holds a listen stream open on node Y ...
    let acknowledged!: () => void;
    const ready = new Promise<void>((resolve) => (acknowledged = resolve));
    const onY = readMethods(
      await Y.fetch(listen('listen-on-y')),
      (m) => m.includes(TOOLS),
      (method) => method === 'notifications/subscriptions/acknowledged' && acknowledged(),
    );
    await ready; // node Y has registered the listen stream on its bus
    // ... and a tool list change happens on node X.
    X.notify.toolsChanged();
    const methods = await onY;
    console.log(`listen stream on node Y received: ${methods.join(', ')}`);
    if (!methods.includes(TOOLS)) process.exitCode = 1;
  } finally {
    await X.close();
    await Y.close();
    await busX.close();
    await busY.close();
    await redis.close();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await selfCheck();
