// 여러 시험 파일이 함께 쓰는 작은 도구(임시 폴더, v2 subscriptions/listen 요청).
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/** A fresh temp folder that is removed by the returned `cleanup`. */
export function tempDir(label: string): { dir: string; cleanup: () => void } {
  const dir = mkdtempSync(join(tmpdir(), `mcp-streamstore-${label}-`));
  return { dir, cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}

const MODERN = '2026-07-28';
const ENVELOPE = {
  'io.modelcontextprotocol/protocolVersion': MODERN,
  'io.modelcontextprotocol/clientInfo': { name: 'bus-test-client', version: '1.0.0' },
  'io.modelcontextprotocol/clientCapabilities': {},
};

/** Raw `subscriptions/listen` request (protocol revision 2026-07-28), the same bytes the v2 client sends. */
export function listenRequest(id: string): Request {
  return new Request('http://mcp.local/mcp', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'application/json, text/event-stream',
      'mcp-method': 'subscriptions/listen',
      'mcp-protocol-version': MODERN,
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id,
      method: 'subscriptions/listen',
      params: { _meta: ENVELOPE, notifications: { toolsListChanged: true, promptsListChanged: true } },
    }),
  });
}

/**
 * Reads one listen stream in the background and collects notification methods in arrival order,
 * so a test can wait for "the sentinel arrived" without sleeping.
 */
export function collectListen(res: Response) {
  const methods: string[] = [];
  const waiters = new Set<() => void>();
  const wake = () => {
    for (const w of [...waiters]) w();
  };
  if (!res.body) throw new Error(`listen answered without a body (HTTP ${res.status})`);
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const pump = (async () => {
    let buf = '';
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let i: number;
        while ((i = buf.indexOf('\n\n')) >= 0) {
          const block = buf.slice(0, i);
          buf = buf.slice(i + 2);
          const data = block
            .split('\n')
            .filter((l) => l.startsWith('data:'))
            .map((l) => l.slice(5).trim())
            .join('');
          if (data) {
            const msg = JSON.parse(data) as { method?: string };
            if (typeof msg.method === 'string') methods.push(msg.method);
          }
        }
        wake();
      }
    } catch {
      // 취소됨
    }
    wake();
  })();
  return {
    methods,
    /** Resolve once `pred(methods)` holds (re-checked after every chunk). */
    async until(pred: (m: string[]) => boolean, timeoutMs = 5000): Promise<void> {
      const deadline = Date.now() + timeoutMs;
      while (!pred(methods)) {
        if (Date.now() > deadline) throw new Error(`timed out; got ${JSON.stringify(methods)}`);
        await new Promise<void>((resolve) => {
          const timer = setTimeout(done, 50);
          function done() {
            clearTimeout(timer);
            waiters.delete(done);
            resolve();
          }
          waiters.add(done);
        });
      }
    },
    /** Disconnect (cancel the stream) and wait for the reader loop to end. */
    async close(): Promise<void> {
      await reader.cancel().catch(() => undefined);
      await pump;
    },
  };
}
