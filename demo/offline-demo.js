
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const TOGGLES = ['LOG_HEADERS', 'LOG_BODY', 'LOG_RESPONSE', 'REDACT_AUTH'];

const CLIENT_AUTHORIZATION = 'Bearer synthetic-demo-token-0000';
const JSON_REQUEST = {
  model: 'synthetic-model',
  messages: [{ role: 'user', content: 'Synthetic demo prompt' }],
};

const JSON_RESPONSE = {
  id: 'chatcmpl-synthetic-demo',
  object: 'chat.completion',
  choices: [
    {
      index: 0,
      message: { role: 'assistant', content: 'Synthetic demo response' },
      finish_reason: 'stop',
    },
  ],
};

const SSE_CONTENT = 'Synthetic streamed response';

const SSE_EVENTS = [
  `data: {"id":"chatcmpl-synthetic-demo-stream","choices":[{"index":0,"delta":{"role":"assistant","content":"Synthetic "}}]}\n\n`,
  `data: {"id":"chatcmpl-synthetic-demo-stream","choices":[{"index":0,"delta":{"content":"streamed "}}]}\n\n`,
  `data: {"id":"chatcmpl-synthetic-demo-stream","choices":[{"index":0,"delta":{"content":"response"}}]}\n\n`,
  'data: [DONE]\n\n',
];

const EXPECTED_LOG_ENTRIES = 4;
const LOG_WAIT_TIMEOUT_MS = 5000;

function printSection(title) {
  process.stdout.write(`\n== ${title} ==\n`);
}

function readLogEntries(logDir) {
  const files = fs.existsSync(logDir)
    ? fs.readdirSync(logDir).filter((name) => name.endsWith('.jsonl')).sort()
    : [];
  const entries = [];
  const lines = [];

  for (const name of files) {
    const text = fs.readFileSync(path.join(logDir, name), 'utf8');
    const complete = text.split('\n');
    complete.pop();

    for (const line of complete) {
      if (line.trim() === '') {
        continue;
      }
      entries.push(JSON.parse(line));
      lines.push(line);
    }
  }

  return {
    file: files.length > 0 ? path.join(logDir, files[0]) : null,
    entries,
    lines,
  };
}

async function waitForLogEntries(logDir, expected, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let found = readLogEntries(logDir);

  while (found.entries.length < expected) {
    if (Date.now() >= deadline) {
      throw new Error(
        `timed out waiting for ${expected} log entries; the log file holds ${found.entries.length} complete line(s)`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
    found = readLogEntries(logDir);
  }

  return found;
}

function removeTempDir(logDir) {
  if (!fs.existsSync(logDir)) {
    return;
  }
  try {
    fs.rmSync(logDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 50 });
  } catch {}

}

function pairEntries(entries, urlSuffix) {
  const request = entries.find((entry) => entry.type === 'request' && entry.url.endsWith(urlSuffix));
  assert.ok(request, `expected a request entry for ${urlSuffix}`);
  const response = entries.find((entry) => entry.type === 'response' && entry.id === request.id);
  assert.ok(response, `expected a response entry sharing the id of the ${urlSuffix} request`);
  return { request, response };
}

function verify({ jsonExchange, sseExchange, log }) {
  assert.strictEqual(jsonExchange.status, 200, 'JSON exchange should return 200');
  assert.deepStrictEqual(jsonExchange.json, JSON_RESPONSE, 'JSON exchange should return the upstream body');
  assert.strictEqual(sseExchange.status, 200, 'SSE exchange should return 200');
  assert.ok(sseExchange.body.includes('data: [DONE]'), 'SSE exchange should deliver the [DONE] terminator');

  assert.strictEqual(log.entries.length, EXPECTED_LOG_ENTRIES, 'log should hold one request and one response per exchange');

  const json = pairEntries(log.entries, '/chat/completions');
  assert.strictEqual(json.response.status, 200);
  assert.deepStrictEqual(json.response.body, JSON_RESPONSE);
  assert.ok(
    /REDACTED/.test(json.request.headers.authorization),
    'request log should redact the synthetic authorization value',
  );

  const sse = pairEntries(log.entries, '/stream');
  assert.strictEqual(sse.response.status, 200);
  assert.strictEqual(sse.response.reconstructed_content, SSE_CONTENT);
  assert.ok(Array.isArray(sse.response.chunks) && sse.response.chunks.length > 0, 'SSE entry should record chunks');
}

async function run(logDir) {
  for (const name of TOGGLES) {
    delete process.env[name];
  }
  process.env.LOG_DIR = logDir;

  const {
    startMockUpstream,
    sendJson,
    sendEventStream,
  } = require('../test/helpers/mock-upstream');
  const { request, startGateway } = require('../test/helpers/gateway');

  const upstream = await startMockUpstream(async (record, res) => {
    if (record.url === '/api/v1/chat/completions') {
      sendJson(res, 200, JSON_RESPONSE, { 'x-upstream': 'synthetic-demo' });
      return;
    }
    if (record.url === '/api/v1/stream') {
      await sendEventStream(res, 200, SSE_EVENTS);
      return;
    }
    sendJson(res, 404, { error: 'synthetic route not found', url: record.url });
  });

  let gateway;

  try {
    gateway = await startGateway(`${upstream.baseUrl}/api/v1`);

    process.stdout.write('llm-tap offline demonstration\n');
    process.stdout.write(`Mock upstream: ${upstream.baseUrl}\n`);
    process.stdout.write(`Gateway:       ${gateway.origin}\n`);
    process.stdout.write(`Log directory: ${logDir}\n`);

    printSection('JSON exchange');
    const jsonExchange = await request(gateway.origin, {
      method: 'POST',
      path: '/v1/chat/completions',
      headers: {
        'content-type': 'application/json',
        authorization: CLIENT_AUTHORIZATION,
      },
      body: JSON.stringify(JSON_REQUEST),
    });
    process.stdout.write(`client status: ${jsonExchange.status}\n`);
    process.stdout.write(`client body:   ${jsonExchange.body}\n`);

    printSection('SSE exchange');
    const sseExchange = await request(gateway.origin, {
      method: 'POST',
      path: '/v1/stream',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'synthetic-model', stream: true }),
    });
    process.stdout.write(`client status: ${sseExchange.status}\n`);
    process.stdout.write(`client body:\n${sseExchange.body}`);

    const log = await waitForLogEntries(logDir, EXPECTED_LOG_ENTRIES, LOG_WAIT_TIMEOUT_MS);

    printSection(`JSONL records from ${log.file}`);
    for (const line of log.lines) {
      process.stdout.write(`${line}\n`);
    }

    verify({ jsonExchange, sseExchange, log });

    process.stdout.write(`\n${log.entries.length} record(s) written to ${log.file}\n`);
    process.stdout.write('Demonstration complete: both exchanges logged as expected.\n');
  } finally {
    if (gateway) {
      await gateway.close();
    }
    await upstream.close();
  }
}

async function main() {
  const logDir = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-tap-demo-'));

  try {
    await run(logDir);
  } finally {
    removeTempDir(logDir);
    process.stdout.write(`Temporary log directory removed: ${logDir}\n`);
  }
}

main().catch((err) => {
  process.stderr.write(`${err.stack || err.message}\n`);
  process.exitCode = 1;
});
