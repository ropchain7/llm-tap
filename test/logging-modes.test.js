const assert = require('node:assert/strict');
const { execFile } = require('node:child_process');
const path = require('node:path');
const { promisify } = require('node:util');
const { test } = require('node:test');

const run = promisify(execFile);

const RUNNER = path.join(__dirname, 'helpers', 'logging-mode-runner.js');

const CLIENT_AUTHORIZATION = 'Bearer synthetic-access-token-0000';
const UPSTREAM_AUTHORIZATION = 'Synthetic upstream credential 9f3c';
const REDACTED_CLIENT_AUTHORIZATION = 'Bearer synth...REDACTED...0000';
const REDACTED_UPSTREAM_AUTHORIZATION = 'Synthetic up...REDACTED...9f3c';
const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const JSON_REQUEST_BODY = {
  model: 'synthetic-model',
  messages: [{ role: 'user', content: 'Synthetic prompt' }],
};
const TEXT_REQUEST_BODY = 'Synthetic plain-text request body';
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
const SSE_BODY = [
  'data: {"id":"chatcmpl-synthetic-matrix","choices":[{"index":0,"delta":{"role":"assistant","content":"Synthetic "}}]}\n\n',
  'data: {"id":"chatcmpl-synthetic-matrix","choices":[{"index":0,"delta":{"content":"streamed "}}]}\n\n',
  'data: {"id":"chatcmpl-synthetic-matrix","choices":[{"index":0,"delta":{"content":"response"}}]}\n\n',
  'data: [DONE]\n\n',
].join('');

const EXCHANGES = ['chat', 'text', 'stream', 'auth'];

async function runMode(settings) {
  const { stdout } = await run(process.execPath, [RUNNER, JSON.stringify(settings)], {
    timeout: 20000,
    encoding: 'utf8',
  });
  return JSON.parse(stdout);
}

function entriesFor(report, type, exchange) {
  const route = `/api/v1/${exchange}`;
  if (type === 'request') {
    return report.entries.filter((entry) => entry.type === 'request' && entry.url.endsWith(route));
  }
  const requests = entriesFor(report, 'request', exchange);
  return requests.map((request) => {
    const matches = report.entries.filter((entry) => entry.type === 'response' && entry.id === request.id);
    assert.ok(matches.length <= 1, `expected at most one response entry for ${route}, got ${matches.length}`);
    return matches[0];
  });
}

function singleEntry(report, type, exchange) {
  const [entry] = entriesFor(report, type, exchange);
  assert.ok(entry, `expected one ${type} entry for /api/v1/${exchange}`);
  return entry;
}

function noResponseEntry(report, exchange) {
  assert.equal(entriesFor(report, 'response', exchange).filter(Boolean).length, 0);
}

function upstreamRequest(report, exchange) {
  return report.upstreamRequests.find((record) => record.url === `/api/v1/${exchange}`);
}

test('default logging records request and response entries with headers and parsed JSON bodies', async () => {
  const report = await runMode({});

  assert.deepEqual(report.settings, {});
  assert.equal(report.responses.json.status, 200);
  assert.deepEqual(report.responses.json.body, UPSTREAM_PAYLOAD);

  const request = singleEntry(report, 'request', 'chat');
  assert.deepEqual(Object.keys(request), ['id', 'timestamp', 'type', 'method', 'url', 'headers', 'body']);
  assert.equal(request.method, 'POST');
  assert.match(request.url, /^http:\/\/127\.0\.0\.1:\d+\/api\/v1\/chat$/);
  assert.match(request.timestamp, ISO_TIMESTAMP);
  assert.equal(request.headers['x-matrix'], 'synthetic-matrix-header');
  assert.equal(typeof request.body, 'object');
  assert.deepEqual(request.body, JSON_REQUEST_BODY);

  const response = singleEntry(report, 'response', 'chat');
  assert.deepEqual(Object.keys(response), ['id', 'timestamp', 'type', 'status', 'duration_ms', 'headers', 'body']);
  assert.equal(response.id, request.id);
  assert.equal(response.status, 200);
  assert.equal(typeof response.duration_ms, 'number');
  assert.match(response.timestamp, ISO_TIMESTAMP);
  assert.equal(response.headers['x-upstream'], 'synthetic-upstream-header');
  assert.deepEqual(response.body, UPSTREAM_PAYLOAD);
});

test('default logging records a text/plain request body as the raw string', async () => {
  const report = await runMode({});

  const request = singleEntry(report, 'request', 'text');
  assert.equal(request.headers['content-type'], 'text/plain');
  assert.equal(typeof request.body, 'string');
  assert.equal(request.body, TEXT_REQUEST_BODY);
  assert.equal(upstreamRequest(report, 'text').body, TEXT_REQUEST_BODY);
});

test('with LOG_BODY=false the request and response entries omit the body and keep headers', async () => {
  const report = await runMode({ LOG_BODY: 'false' });

  for (const exchange of EXCHANGES) {
    const request = singleEntry(report, 'request', exchange);
    assert.deepEqual(Object.keys(request), ['id', 'timestamp', 'type', 'method', 'url', 'headers']);
    assert.equal('body' in request, false);
  }

  assert.equal(singleEntry(report, 'request', 'chat').headers['x-matrix'], 'synthetic-matrix-header');
  assert.equal(singleEntry(report, 'request', 'text').headers['content-type'], 'text/plain');

  for (const exchange of ['chat', 'text', 'auth']) {
    const response = singleEntry(report, 'response', exchange);
    assert.deepEqual(Object.keys(response), ['id', 'timestamp', 'type', 'status', 'duration_ms', 'headers']);
    assert.equal('body' in response, false);
  }
  assert.equal(singleEntry(report, 'response', 'chat').headers['x-upstream'], 'synthetic-upstream-header');

  assert.equal(upstreamRequest(report, 'chat').body, JSON.stringify(JSON_REQUEST_BODY));
});

test('with LOG_HEADERS=false the request and response entries omit headers and keep bodies', async () => {
  const report = await runMode({ LOG_HEADERS: 'false' });

  for (const exchange of EXCHANGES) {
    const request = singleEntry(report, 'request', exchange);
    assert.deepEqual(Object.keys(request), ['id', 'timestamp', 'type', 'method', 'url', 'body']);
    assert.equal('headers' in request, false);
  }

  assert.deepEqual(singleEntry(report, 'request', 'chat').body, JSON_REQUEST_BODY);
  assert.equal(singleEntry(report, 'request', 'text').body, TEXT_REQUEST_BODY);

  for (const exchange of ['chat', 'text', 'auth']) {
    const response = singleEntry(report, 'response', exchange);
    assert.deepEqual(Object.keys(response), ['id', 'timestamp', 'type', 'status', 'duration_ms', 'body']);
    assert.equal('headers' in response, false);
  }
  assert.deepEqual(singleEntry(report, 'response', 'chat').body, UPSTREAM_PAYLOAD);
});

test('with LOG_HEADERS=false and LOG_BODY=false the entries carry only their fixed fields', async () => {
  const report = await runMode({ LOG_HEADERS: 'false', LOG_BODY: 'false' });

  for (const exchange of EXCHANGES) {
    assert.deepEqual(Object.keys(singleEntry(report, 'request', exchange)), [
      'id', 'timestamp', 'type', 'method', 'url',
    ]);
  }
  for (const exchange of ['chat', 'text', 'auth']) {
    assert.deepEqual(Object.keys(singleEntry(report, 'response', exchange)), [
      'id', 'timestamp', 'type', 'status', 'duration_ms',
    ]);
  }
});

test('with LOG_RESPONSE=false no response entry is written but responses still reach the client', async () => {
  const report = await runMode({ LOG_RESPONSE: 'false' });

  assert.deepEqual(report.entries.map((entry) => entry.type), ['request', 'request', 'request', 'request']);
  for (const exchange of EXCHANGES) {
    noResponseEntry(report, exchange);
  }

  assert.equal(report.responses.json.status, 200);
  assert.deepEqual(report.responses.json.body, UPSTREAM_PAYLOAD);
  assert.equal(report.responses.stream.body, SSE_BODY);
});

test('with LOG_RESPONSE on a non-streaming response entry carries status, duration_ms and the parsed body', async () => {
  const report = await runMode({ LOG_RESPONSE: 'true' });

  const response = singleEntry(report, 'response', 'chat');
  assert.equal(response.type, 'response');
  assert.equal(response.status, 200);
  assert.equal(typeof response.duration_ms, 'number');
  assert.equal(response.id, singleEntry(report, 'request', 'chat').id);
  assert.deepEqual(response.body, UPSTREAM_PAYLOAD);
});

test('with LOG_RESPONSE on an SSE response entry carries chunks and reconstructed_content', async () => {
  const report = await runMode({ LOG_RESPONSE: 'true' });

  const response = singleEntry(report, 'response', 'stream');
  assert.equal(report.responses.stream.body, SSE_BODY);
  assert.deepEqual(Object.keys(response), [
    'id', 'timestamp', 'type', 'status', 'duration_ms', 'chunks', 'reconstructed_content', 'headers',
  ]);
  assert.equal(Buffer.from(response.chunks.flat()).toString('utf8'), SSE_BODY);
  assert.equal(response.reconstructed_content, 'Synthetic streamed response');
});

test('current behaviour: with LOG_RESPONSE on and LOG_BODY=false an SSE entry still carries chunks and reconstructed_content', async () => {
  const report = await runMode({ LOG_RESPONSE: 'true', LOG_BODY: 'false' });

  const response = singleEntry(report, 'response', 'stream');
  assert.deepEqual(Object.keys(response), [
    'id', 'timestamp', 'type', 'status', 'duration_ms', 'chunks', 'reconstructed_content', 'headers',
  ]);
  assert.equal('body' in response, false);
  assert.equal(Buffer.from(response.chunks.flat()).toString('utf8'), SSE_BODY);
  assert.equal(response.reconstructed_content, 'Synthetic streamed response');

  for (const exchange of ['chat', 'text', 'auth']) {
    assert.equal('body' in singleEntry(report, 'response', exchange), false);
    assert.equal('chunks' in singleEntry(report, 'response', exchange), false);
  }
});

test('with REDACT_AUTH=false the request entry records the client authorization value unredacted', async () => {
  const report = await runMode({ REDACT_AUTH: 'false' });

  const request = singleEntry(report, 'request', 'auth');
  assert.equal(request.headers.authorization, CLIENT_AUTHORIZATION);
  assert.equal(request.headers.authorization, upstreamRequest(report, 'auth').authorization);
  assert.equal(report.responses.auth.authorization, undefined);
});

test('with REDACT_AUTH on the request entry redacts the client authorization while upstream receives it unchanged', async () => {
  const report = await runMode({ REDACT_AUTH: 'true' });

  const request = singleEntry(report, 'request', 'auth');
  assert.equal(request.headers.authorization, REDACTED_CLIENT_AUTHORIZATION);
  assert.notEqual(request.headers.authorization, CLIENT_AUTHORIZATION);
  assert.equal(upstreamRequest(report, 'auth').authorization, CLIENT_AUTHORIZATION);
  assert.equal(report.responses.auth.authorization, undefined);
});

test('with REDACT_AUTH on the response log redacts an upstream authorization header that is not forwarded', async () => {
  const report = await runMode({ REDACT_AUTH: 'true' });

  const response = singleEntry(report, 'response', 'auth');
  assert.equal(response.headers.authorization, REDACTED_UPSTREAM_AUTHORIZATION);
  assert.notEqual(response.headers.authorization, UPSTREAM_AUTHORIZATION);
  assert.equal(report.responses.auth.authorization, undefined);
});
