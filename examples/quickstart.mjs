// Quick start: a stateless MCP server (SDK v1) whose stream survives a reconnect to another instance.
//
//   npm install mcp-streamstore @modelcontextprotocol/sdk
//   node quickstart.mjs            ->  "resumed 3 events on instance B"
//
// The only change against the SDK example is the event store:
//   - eventStore: new InMemoryEventStore(),                         // one notebook per instance
//   + eventStore: new SqliteEventStore({ path: './mcp-events.db' }), // one shared ledger
//
// Instances A and B run in this one process and get web-standard Requests directly
// (exactly what a load balancer would forward), so no ports are opened.
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { WebStandardStreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js';
import { SqliteEventStore } from 'mcp-streamstore';

const dir = mkdtempSync(join(tmpdir(), 'mcp-streamstore-quickstart-'));
const dbPath = join(dir, 'mcp-events.db'); // every instance on this host opens the same file

let finishWork; // lets the tool continue only after the client has disconnected
const workMayFinish = new Promise((resolve) => (finishWork = resolve));

// One "server process". Instance A and instance B are built the same way.
async function startInstance(name) {
  const server = new McpServer({ name, version: '1.0.0' }, { capabilities: { logging: {} } });
  server.registerTool('work', { description: 'Reports 3 progress steps, then answers' }, async (extra) => {
    for (let step = 1; step <= 3; step++) {
      await extra.sendNotification({ method: 'notifications/message', params: { level: 'info', data: `step ${step}` } });
      if (step === 1) await workMayFinish;
    }
    return { content: [{ type: 'text', text: 'done' }] };
  });
  const eventStore = new SqliteEventStore({ path: dbPath }); // <- the two-line change
  const transport = new WebStandardStreamableHTTPServerTransport({ sessionIdGenerator: undefined, eventStore });
  await server.connect(transport);
  return { transport, close: async () => { await server.close(); await eventStore.close(); } };
}

const headers = {
  'content-type': 'application/json',
  accept: 'application/json, text/event-stream',
  'mcp-protocol-version': '2025-11-25',
};

// Read SSE events ("id:" + "data:") until `enough(events)` says stop, then hang up.
async function readEvents(response, enough) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const events = [];
  let buffer = '';
  while (!enough(events)) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let end;
    while ((end = buffer.indexOf('\n\n')) >= 0) {
      const block = buffer.slice(0, end);
      buffer = buffer.slice(end + 2);
      const id = block.match(/^id: ?(.*)$/m)?.[1];
      const data = block.match(/^data: ?(.*)$/m)?.[1];
      if (data) events.push({ id, message: JSON.parse(data) });
    }
  }
  await reader.cancel(); // the client disconnects
  return events;
}

// Wait (up to 5 s) until the tool's final answer is in the shared store.
async function waitForStoredResult(afterEventId) {
  const ledger = new SqliteEventStore({ path: dbPath });
  try {
    for (let i = 0; i < 500; i++) {
      let hasResult = false;
      await ledger.replayEventsAfter(afterEventId, { send: async (_id, message) => void (hasResult ||= 'result' in message) });
      if (hasResult) return;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error('the tool did not finish on instance A');
  } finally {
    await ledger.close();
  }
}

const A = await startInstance('instance-a');
const B = await startInstance('instance-b');
try {
  // 1) The client calls the tool on A, sees the first progress event, and loses the connection.
  const call = await A.transport.handleRequest(
    new Request('http://localhost/mcp', {
      method: 'POST',
      headers,
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'work', arguments: {} } }),
    }),
  );
  const [first] = await readEvents(call, (events) => events.length >= 1);
  console.log(`instance A sent: ${first.message.params.data} (event id ${first.id}), then the connection dropped`);

  // 2) The tool keeps running on A; its remaining events go only to the store.
  finishWork();
  await waitForStoredResult(first.id);

  // 3) The load balancer sends the reconnect to B. B has never seen this stream.
  const resumed = await B.transport.handleRequest(
    new Request('http://localhost/mcp', { method: 'GET', headers: { ...headers, 'last-event-id': first.id } }),
  );
  const replayed = await readEvents(resumed, (events) => events.some((e) => 'result' in e.message));
  for (const e of replayed) console.log(`  replayed ${e.id}: ${e.message.method ?? 'result: ' + e.message.result.content[0].text}`);
  console.log(`resumed ${replayed.length} events on instance B (HTTP ${resumed.status})`);
  if (replayed.length !== 3) process.exitCode = 1;
} finally {
  await A.close();
  await B.close();
  rmSync(dir, { recursive: true, force: true });
}
