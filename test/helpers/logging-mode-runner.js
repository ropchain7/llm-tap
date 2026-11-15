
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const TOGGLES = ['LOG_HEADERS', 'LOG_BODY', 'LOG_RESPONSE', 'REDACT_AUTH'];

const CLIENT_AUTHORIZATION = 'Bearer synthetic-access-token-0000';
const UPSTREAM_AUTHORIZATION = 'Synthetic upstream credential 9f3c';

const JSON_REQUEST_BODY = {
  model: 'synthetic-model',
  messages: [{ role: 'user', content: 'Synthetic prompt' }],
};
const TEXT_REQUEST_BODY = 'Synthetic plain-text request body';
const SSE_REQUEST_BODY = { model: 'synthetic-model', stream: true };

const UPSTREAM_PAYLOAD = {
  id: 'chatcmpl-synthetic-matrix',
  object: 'chat.completion',
  choices: [
    {
      index: 0,
      message: { role: 'assistant', content: 'Synthetic matrix response' },
      finish_reason: 'stop',
    },
  ],
};

const SSE_EVENTS = [
  'data: {"id":"chatcmpl-synthetic-matrix","choices":[{"index":0,"delta":{"role":"assistant","content":"Synthetic "}}]}\n\n',
  'data: {"id":"chatcmpl-synthetic-matrix","choices":[{"index":0,"delta":{"content":"streamed "}}]}\n\n',
  'data: {"id":"chatcmpl-synthetic-matrix","choices":[{"index":0,"delta":{"content":"response"}}]}\n\n',
  'data: [DONE]\n\n',
];
const SSE_BODY = SSE_EVENTS.join('');

function applySettings(settings) {
  for (const name of TOGGLES) {
    if (settings[name] === undefined) {
      delete process.env[name];
    } else {
      process.env[name] = settings[name];
    }
  }
}

async function startMockUpstream(handler) {
  const { startMockUpstream: start } = require('./mock-upstream');
  return start(handler);
}

async function main() {
  const settings = JSON.parse(process.argv[2] || '{}');
  const logDir = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-tap-mode-'));
  applySettings(settings);
  process.env.LOG_DIR = logDir;

  const upstreamRequests = [];
  const upstream = await startMockUpstream(async (record, res) => {
    upstreamRequests.push({
      method: record.method,
      url: record.url,
      authorization: record.headers.authorization,
      body: record.body,
    });

    if (record.url === '/api/v1/chat') {
      const { sendJson } = require('./mock-upstream');
      sendJson(res, 200, UPSTREAM_PAYLOAD, { 'x-upstream': 'synthetic-upstream-header' });
      return;
    }

    if (record.url === '/api/v1/text') {
      const { sendJson } = require('./mock-upstream');
      sendJson(res, 200, { ok: true }, { 'x-upstream': 'synthetic-text-response' });
      return;
    }

    if (record.url === '/api/v1/stream') {
      const { sendEventStream } = require('./mock-upstream');
      await sendEventStream(res, 200, SSE_EVENTS);
      return;
    }

    if (record.url === '/api/v1/auth') {
      const { sendJson } = require('./mock-upstream');
      sendJson(res, 200, { ok: true }, {
        authorization: UPSTREAM_AUTHORIZATION,
        'x-upstream': 'synthetic-auth-response',
      });
      return;
    }

    const { sendJson } = require('./mock-upstream');
    sendJson(res, 404, { error: 'synthetic route not found', url: record.url });
  });

  const { captureLogEntries, request, startGateway } = require('./gateway');
  const logs = captureLogEntries();
  const responses = {};
  let gateway;

  try {
    gateway = await startGateway(`${upstream.baseUrl}/api/v1`);

    const jsonResponse = await request(gateway.origin, {
      method: 'POST',
      path: '/v1/chat',
      headers: {
        'content-type': 'application/json',
        authorization: CLIENT_AUTHORIZATION,
        'x-matrix': 'synthetic-matrix-header',
      },
      body: JSON.stringify(JSON_REQUEST_BODY),
    });
    responses.json = { status: jsonResponse.status, body: jsonResponse.json };

    const textResponse = await request(gateway.origin, {
      method: 'POST',
      path: '/v1/text',
      headers: { 'content-type': 'text/plain' },
      body: TEXT_REQUEST_BODY,
    });
    responses.text = { status: textResponse.status };

    const streamResponse = await request(gateway.origin, {
      method: 'POST',
      path: '/v1/stream',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(SSE_REQUEST_BODY),
    });
    responses.stream = { status: streamResponse.status, body: streamResponse.body };

    const authResponse = await request(gateway.origin, {
      method: 'POST',
      path: '/v1/auth',
      headers: {
        'content-type': 'application/json',
        authorization: CLIENT_AUTHORIZATION,
      },
      body: JSON.stringify({ input: 'synthetic-auth-exchange' }),
    });
    responses.auth = {
      status: authResponse.status,
      authorization: authResponse.headers.authorization,
    };

    process.stdout.write(`${JSON.stringify({
      settings,
      entries: logs.entries,
      upstreamRequests,
      responses,
    })}\n`);
  } finally {
    if (gateway) {
      await gateway.close();
    }
    await upstream.close();
    logs.restore();
    try {
      fs.rmSync(logDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 50 });
    } catch {}

  }
}

main().catch((err) => {
  process.stderr.write(`${err.stack || err.message}\n`);
  process.exitCode = 1;
});
