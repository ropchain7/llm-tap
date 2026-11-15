const assert = require('node:assert/strict');
const { execFile } = require('node:child_process');
const http = require('node:http');
const path = require('node:path');
const { after, before, beforeEach, test } = require('node:test');
const { promisify } = require('node:util');

const { cleanup } = require('./helpers/env');
const { captureLogEntries, request, startGateway } = require('./helpers/gateway');
const { sendEventStream, sendJson, sendText, startMockUpstream } = require('./helpers/mock-upstream');

const run = promisify(execFile);
const CONNECTION_FAILURE_RUNNER = path.join(__dirname, 'helpers', 'connection-failure-runner.js');

const CLIENT_AUTHORIZATION = 'Bearer synthetic-access-token-0000';
const UPSTREAM_AUTHORIZATION = 'Synthetic upstream credential 9f3c';
const REDACTED_CLIENT_AUTHORIZATION = 'Bearer synth...REDACTED...0000';
const REDACTED_UPSTREAM_AUTHORIZATION = 'Synthetic up...REDACTED...9f3c';
const UPSTREAM_PAYLOAD = {
  id: 'chatcmpl-synthetic-001',
  object: 'chat.completion',
  choices: [
    {
      index: 0,
      message: { role: 'assistant', content: 'Synthetic response text' },
      finish_reason: 'stop',
    },
  ],
};

const SSE_STREAM_EVENTS = [
  'data: {"id":"chatcmpl-synthetic-sse","choices":[{"index":0,"delta":{"role":"assistant","content":"Synthetic "}}]}\n\n',
  'data: {"id":"chatcmpl-synthetic-sse","choices":[{"index":0,"delta":{"content":"streamed "}}]}\n\n',
  'data: {"id":"chatcmpl-synthetic-sse","choices":[{"index":0,"delta":{"content":"response"}}]}\n\n',
  'data: [DONE]\n\n',
];
const SSE_BODY = SSE_STREAM_EVENTS.join('');
const SSE_SPLIT_FIRST_PART =
  'data: {"id":"chatcmpl-synthetic-split","choices":[{"index":0,"delta":{"content":"Split';
const SSE_SPLIT_SECOND_PART = ' content"}}]}\n\n';
const SSE_SPLIT_INTACT_EVENT =
  'data: {"id":"chatcmpl-synthetic-split","choices":[{"index":0,"delta":{"content":"Intact"}}]}\n\n';
const SSE_REQUEST = {
  model: 'synthetic-model',
  stream: true,
  messages: [{ role: 'user', content: 'Synthetic stream prompt' }],
};

const SSE_FAILURE_FIRST_EVENT =
  'data: {"id":"chatcmpl-synthetic-failure","choices":[{"index":0,"delta":{"content":"Before failure"}}]}\n\n';

const HOP_BY_HOP_HEADERS = [
  'host',
  'content-length',
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'transfer-encoding',
  'upgrade',
];

let upstream;
let gateway;
let logs;
let startupError;
let streamProbe = {
  text: () => '',
  async waitForText() {
    return true;
  },
};
const streamGate = { observations: [] };
let releaseUpstreamFailure;
let upstreamFailureGate;

function resetUpstreamFailureGate() {
  upstreamFailureGate = new Promise((resolve) => {
    releaseUpstreamFailure = resolve;
  });
}

function openEventStream(origin, { method = 'POST', path, headers = {}, body } = {}) {
  const state = { text: '', ended: false };
  const waiters = [];

  function settleAll(result) {
    while (waiters.length > 0) {
      waiters.pop().settle(result);
    }
  }

  const probe = {
    text: () => state.text,
    waitForText(fragment, timeoutMs = 2000) {
      if (state.text.includes(fragment)) {
        return Promise.resolve(true);
      }
      if (state.ended) {
        return Promise.resolve(false);
      }
      return new Promise((resolve) => {
        const waiter = {
          fragment,
          settle(result) {
            if (waiter.settled) {
              return;
            }
            waiter.settled = true;
            clearTimeout(waiter.timer);
            resolve(result);
          },
        };
        waiter.timer = setTimeout(() => {
          const index = waiters.indexOf(waiter);
          if (index !== -1) {
            waiters.splice(index, 1);
          }
          waiter.settle(false);
        }, timeoutMs);
        waiters.push(waiter);
      });
    },
  };

  streamProbe = probe;

  probe.done = new Promise((resolve, reject) => {
    const req = http.request(`${origin}${path}`, { method, headers, agent: false }, (res) => {
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        state.text += chunk;
        for (let index = waiters.length - 1; index >= 0; index -= 1) {
          if (state.text.includes(waiters[index].fragment)) {
            waiters.splice(index, 1)[0].settle(true);
          }
        }
      });
      res.on('end', () => {
        state.ended = true;
        settleAll(false);
        resolve({ status: res.statusCode, headers: res.headers, body: state.text });
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end(body);
  });

  return probe;
}

async function gateOn(fragment) {
  const seen = await streamProbe.waitForText(fragment);
  streamGate.observations.push({ fragment, seen, partial: streamProbe.text() });
  return seen;
}

async function mockHandler(req, res) {
  if (req.method === 'HEAD' && req.url === '/api/v1/head') {
    res.writeHead(200, {
      'content-length': '123',
      'x-upstream': 'synthetic-head-header',
    });
    res.end();
    return;
  }

  if (req.url === '/api/v1/chat') {
    sendJson(res, 200, UPSTREAM_PAYLOAD, {
      'content-encoding': 'identity',
      'x-upstream': 'synthetic-upstream-header',
    });
    return;
  }

  if (req.url === '/api/v1/cookies') {
    const body = JSON.stringify({ ok: true });
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'content-length': Buffer.byteLength(body),
      'set-cookie': ['session=synthetic-a; Path=/', 'refresh=synthetic-b; Path=/'],
    });
    res.end(body);
    return;
  }

  if (req.url === '/api/v1/error/422') {
    sendJson(
      res,
      422,
      { error: { code: 'synthetic_validation_error', message: 'Synthetic invalid input' } },
      { 'x-upstream-error': 'validation' },
    );
    return;
  }

  if (req.url === '/api/v1/error/503') {
    sendText(res, 503, 'Synthetic upstream unavailable', { 'x-upstream-error': 'unavailable' });
    return;
  }

  if (req.url === '/api/v1/auth') {
    sendJson(res, 200, { ok: true }, {
      authorization: UPSTREAM_AUTHORIZATION,
      'x-upstream': 'synthetic-auth-response',
    });
    return;
  }

  if (req.url === '/api/v1/stream') {
    await sendEventStream(res, 200, [
      SSE_STREAM_EVENTS[0],
      () => gateOn('Synthetic '),
      SSE_STREAM_EVENTS[1],
      () => gateOn('streamed '),
      SSE_STREAM_EVENTS[2],
      () => gateOn('response'),
      SSE_STREAM_EVENTS[3],
    ]);
    return;
  }

  if (req.url === '/api/v1/stream-auth') {
    await sendEventStream(
      res,
      200,
      [
        SSE_STREAM_EVENTS[0],
        () => gateOn('Synthetic '),
        SSE_STREAM_EVENTS[1],
        () => gateOn('streamed '),
        SSE_STREAM_EVENTS[3],
      ],
      { authorization: UPSTREAM_AUTHORIZATION },
    );
    return;
  }

  if (req.url === '/api/v1/stream-split') {
    await sendEventStream(res, 200, [
      SSE_SPLIT_FIRST_PART,
      () => gateOn('Split'),
      SSE_SPLIT_SECOND_PART,
      () => gateOn('Split content'),
      SSE_SPLIT_INTACT_EVENT,
      'data: [DONE]\n\n',
    ]);
    return;
  }

  if (req.url === '/api/v1/stream-after-done') {
    await sendEventStream(res, 200, [
      SSE_STREAM_EVENTS[0],
      SSE_STREAM_EVENTS[3],
      () => gateOn('data: [DONE]'),
      'data: {"id":"chatcmpl-synthetic-sse","choices":[{"index":0,"delta":{"content":"After done"}}]}\n\n',
    ]);
    return;
  }

  if (req.url === '/api/v1/stream-dies') {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.flushHeaders();
    res.write(SSE_FAILURE_FIRST_EVENT);
    res.flush?.();
    await upstreamFailureGate;
    res.destroy();
    return;
  }

  if (req.url === '/api/v1/ordinary') {
    sendJson(res, 200, { ok: true }, { 'x-upstream': 'synthetic-ordinary-response' });
    return;
  }

  if (req.url === '/api/v1/filtered-head') {
    res.writeHead(200, {
      'content-length': '777',
      'content-encoding': 'identity',
      'x-upstream': 'synthetic-filtered-headers',
    });
    res.end();
    return;
  }

  if (req.url === '/api/v1/filtered-chunked') {
    res.writeHead(200, {
      'content-type': 'text/plain',
      'content-encoding': 'identity',
      'x-upstream': 'synthetic-filtered-chunked',
    });
    res.write('Synthetic chunked body');
    res.end();
    return;
  }

  sendJson(res, 404, { error: 'synthetic route not found', url: req.url });
}

before(async () => {
  process.env.LOG_HEADERS = 'true';
  process.env.LOG_BODY = 'true';
  process.env.LOG_RESPONSE = 'true';
  process.env.REDACT_AUTH = 'true';

  logs = captureLogEntries();
  try {
    upstream = await startMockUpstream(mockHandler);
    gateway = await startGateway(`${upstream.baseUrl}/api/v1`);
  } catch (err) {
    startupError = err;
  }
});

beforeEach(() => {
  logs.reset();
  streamGate.observations.length = 0;
  resetUpstreamFailureGate();
});

after(async () => {
  if (gateway) {
    await gateway.close();
  }
  if (upstream) {
    await upstream.close();
  }
  logs.restore();
  cleanup();
});

function requireStartup() {
  if (startupError) {
    assert.fail(`local test server could not start: ${startupError.code || startupError.message}`);
  }
}

test('forwards a JSON request and returns the upstream JSON response', async () => {
  requireStartup();
  const payload = {
    model: 'synthetic-model',
    messages: [{ role: 'user', content: 'Synthetic prompt' }],
  };

  const response = await request(gateway.origin, {
    method: 'POST',
    path: '/v1/chat',
    headers: {
      'content-type': 'application/json',
      authorization: CLIENT_AUTHORIZATION,
      'x-forward-me': 'synthetic-request-header',
      'proxy-authorization': 'Basic synthetic-hop-by-hop',
    },
    body: JSON.stringify(payload),
  });

  await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);
  assert.equal(response.status, 200);
  assert.deepEqual(response.json, UPSTREAM_PAYLOAD);
  assert.equal(response.headers['x-upstream'], 'synthetic-upstream-header');
  assert.equal(response.headers['content-encoding'], undefined);
  assert.match(response.headers['content-type'], /^application\/json/);
  assert.equal(response.headers['content-length'], String(Buffer.byteLength(JSON.stringify(UPSTREAM_PAYLOAD))));

  const forwarded = upstream.requests.at(-1);
  assert.equal(forwarded.method, 'POST');
  assert.equal(forwarded.url, '/api/v1/chat');
  assert.deepEqual(JSON.parse(forwarded.body), payload);
  assert.equal(forwarded.headers.authorization, CLIENT_AUTHORIZATION);
  assert.equal(forwarded.headers['x-forward-me'], 'synthetic-request-header');
  assert.equal(forwarded.headers['proxy-authorization'], undefined);
});

test('forwards multiple Set-Cookie response headers instead of collapsing them', async () => {
  requireStartup();
  const response = await request(gateway.origin, {
    method: 'GET',
    path: '/v1/cookies',
  });

  assert.equal(response.status, 200);
  const cookies = response.headers['set-cookie'];
  assert.ok(Array.isArray(cookies), `expected an array of set-cookie headers, got ${typeof cookies}`);
  assert.equal(cookies.length, 2);
  assert.match(cookies[0], /^session=synthetic-a; Path=\/$/);
  assert.match(cookies[1], /^refresh=synthetic-b; Path=\/$/);
});

test('passes through a 4xx status, body and selected headers', async () => {
  requireStartup();
  const response = await request(gateway.origin, {
    method: 'POST',
    path: '/v1/error/422',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ input: 'synthetic-invalid-input' }),
  });

  await logs.waitFor((entry) => entry.type === 'response' && entry.status === 422);
  assert.equal(response.status, 422);
  assert.deepEqual(response.json, {
    error: { code: 'synthetic_validation_error', message: 'Synthetic invalid input' },
  });
  assert.equal(response.headers['x-upstream-error'], 'validation');
});

test('passes through a 5xx status and body unchanged', async () => {
  requireStartup();
  const response = await request(gateway.origin, {
    method: 'POST',
    path: '/v1/error/503',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ input: 'synthetic-request' }),
  });

  await logs.waitFor((entry) => entry.type === 'response' && entry.status === 503);
  assert.equal(response.status, 503);
  assert.equal(response.body, 'Synthetic upstream unavailable');
  assert.equal(response.headers['x-upstream-error'], 'unavailable');
});

test('filters an upstream content-length header instead of forwarding it', async () => {
  requireStartup();
  const response = await request(gateway.origin, {
    method: 'HEAD',
    path: '/v1/head',
  });

  assert.equal(response.status, 200);
  assert.equal(response.headers['x-upstream'], 'synthetic-head-header');
  assert.notEqual(response.headers['content-length'], '123');
});

test('redacts the client authorization header in request logs and forwards it upstream', async () => {
  requireStartup();
  const response = await request(gateway.origin, {
    method: 'POST',
    path: '/v1/auth',
    headers: {
      'content-type': 'application/json',
      authorization: CLIENT_AUTHORIZATION,
    },
    body: JSON.stringify({ input: 'synthetic-auth-check' }),
  });

  await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  const requestEntry = logs.entries.find((entry) => entry.type === 'request');
  const responseEntry = logs.entries.find((entry) => entry.type === 'response');

  assert.equal(requestEntry.headers.authorization, REDACTED_CLIENT_AUTHORIZATION);
  assert.equal(upstream.requests.at(-1).headers.authorization, CLIENT_AUTHORIZATION);
  assert.equal(responseEntry.headers.authorization, REDACTED_UPSTREAM_AUTHORIZATION);
  assert.notEqual(responseEntry.headers.authorization, UPSTREAM_AUTHORIZATION);
});

test('drops an upstream authorization response header and logs it sanitized', async () => {
  requireStartup();
  const response = await request(gateway.origin, {
    method: 'POST',
    path: '/v1/auth',
    headers: {
      'content-type': 'application/json',
      authorization: CLIENT_AUTHORIZATION,
    },
    body: JSON.stringify({ input: 'synthetic-auth-response-check' }),
  });

  await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  const requestEntry = logs.entries.find((entry) => entry.type === 'request');
  const responseEntry = logs.entries.find((entry) => entry.type === 'response');

  assert.equal(response.status, 200);
  assert.equal(response.headers.authorization, undefined);
  assert.equal(requestEntry.headers.authorization, REDACTED_CLIENT_AUTHORIZATION);
  assert.equal(upstream.requests.at(-1).headers.authorization, CLIENT_AUTHORIZATION);
  assert.equal(responseEntry.headers.authorization, REDACTED_UPSTREAM_AUTHORIZATION);
  assert.notEqual(responseEntry.headers.authorization, UPSTREAM_AUTHORIZATION);
});

test('streams an SSE response chunk by chunk and delivers the full body', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(SSE_REQUEST),
  });

  const response = await stream.done;
  const responseEntry = await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  assert.equal(response.status, 200);
  assert.equal(response.body, SSE_BODY);

  assert.deepEqual(
    streamGate.observations.map((observation) => observation.fragment),
    ['Synthetic ', 'streamed ', 'response'],
  );
  for (const observation of streamGate.observations) {
    assert.equal(observation.seen, true, `client did not receive "${observation.fragment}" before the stream ended`);
  }
  assert.equal(streamGate.observations[0].partial.includes('data: [DONE]'), false);
  assert.equal(streamGate.observations[1].partial.includes('data: [DONE]'), false);

  assert.ok(responseEntry.chunks.length > 1, `expected several recorded chunks, got ${responseEntry.chunks.length}`);
  assert.equal(
    Buffer.from(responseEntry.chunks.flat()).toString('utf8'),
    SSE_BODY,
  );
});

test('delivers the data: [DONE] terminator once and ends the client stream at the upstream close', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(SSE_REQUEST),
  });

  const response = await stream.done;
  const responseEntry = await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  assert.equal(response.status, 200);
  assert.match(response.body, /data: \[DONE\]\n\n$/);
  assert.equal(response.body.split('data: [DONE]').length - 1, 1);
  assert.equal(responseEntry.reconstructed_content.includes('[DONE]'), false);
});

test('current behaviour: the SSE stream does not stop at data: [DONE]; later events are forwarded until the upstream closes', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream-after-done',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(SSE_REQUEST),
  });

  const response = await stream.done;
  const responseEntry = await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  assert.equal(streamGate.observations[0].seen, true, 'the client did not receive data: [DONE] before the stream closed');
  assert.equal(response.status, 200);
  assert.equal(response.body.includes('After done'), true);
  assert.equal(response.body.indexOf('data: [DONE]') < response.body.indexOf('After done'), true);
  assert.equal(responseEntry.reconstructed_content, 'Synthetic After done');
});

test('records reconstructed_content assembled from choices[0].delta.content across chunks', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(SSE_REQUEST),
  });

  await stream.done;
  const responseEntry = await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  assert.equal(responseEntry.reconstructed_content, 'Synthetic streamed response');
  assert.ok(responseEntry.chunks.length > 1);
});

test('records SSE chunks as arrays of byte values', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(SSE_REQUEST),
  });

  await stream.done;
  const responseEntry = await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  assert.ok(Array.isArray(responseEntry.chunks));
  for (const chunk of responseEntry.chunks) {
    assert.ok(Array.isArray(chunk));
    assert.ok(chunk.length > 0);
    for (const byte of chunk) {
      assert.equal(Number.isInteger(byte), true);
      assert.ok(byte >= 0 && byte <= 255);
    }
  }
  assert.equal(Buffer.from(responseEntry.chunks.flat()).toString('utf8'), SSE_BODY);
});

test('sets Cache-Control, Connection and X-Accel-Buffering on the streaming response', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(SSE_REQUEST),
  });

  const response = await stream.done;

  assert.equal(response.headers['cache-control'], 'no-cache');
  assert.equal(response.headers.connection, 'keep-alive');
  assert.equal(response.headers['x-accel-buffering'], 'no');
  assert.match(response.headers['content-type'], /^text\/event-stream/);
});

test('shares one id between the request entry and the SSE response entry', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(SSE_REQUEST),
  });

  await stream.done;
  await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  const requestEntry = logs.entries.find((entry) => entry.type === 'request');
  const responseEntry = logs.entries.find((entry) => entry.type === 'response');

  assert.equal(typeof requestEntry.id, 'string');
  assert.ok(requestEntry.id.length > 0);
  assert.equal(responseEntry.id, requestEntry.id);
  assert.deepEqual([...new Set(logs.entries.map((entry) => entry.id))], [requestEntry.id]);
});

test('drops an upstream authorization header from the SSE client response and logs it sanitized', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream-auth',
    headers: {
      'content-type': 'application/json',
      authorization: CLIENT_AUTHORIZATION,
    },
    body: JSON.stringify(SSE_REQUEST),
  });

  const response = await stream.done;
  await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  const requestEntry = logs.entries.find((entry) => entry.type === 'request');
  const responseEntry = logs.entries.find((entry) => entry.type === 'response');

  assert.equal(response.status, 200);
  assert.equal(response.headers.authorization, undefined);
  assert.equal(upstream.requests.at(-1).headers.authorization, CLIENT_AUTHORIZATION);
  assert.equal(requestEntry.headers.authorization, REDACTED_CLIENT_AUTHORIZATION);
  assert.equal(responseEntry.headers.authorization, REDACTED_UPSTREAM_AUTHORIZATION);
  assert.notEqual(responseEntry.headers.authorization, UPSTREAM_AUTHORIZATION);
});

test('current behaviour: an SSE data: line split across two network chunks is missing from reconstructed_content', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream-split',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(SSE_REQUEST),
  });

  const response = await stream.done;
  const responseEntry = await logs.waitFor((entry) => entry.type === 'response' && entry.status === 200);

  assert.equal(response.status, 200);
  assert.equal(response.body, `${SSE_SPLIT_FIRST_PART}${SSE_SPLIT_SECOND_PART}${SSE_SPLIT_INTACT_EVENT}data: [DONE]\n\n`);
  assert.equal(streamGate.observations[0].partial.includes(SSE_SPLIT_SECOND_PART), false);
  assert.equal(responseEntry.reconstructed_content, 'Intact');
  assert.equal(responseEntry.reconstructed_content.includes('Split content'), false);
  assert.equal(Buffer.from(responseEntry.chunks.flat()).toString('utf8'), response.body);
});

test('returns 502 with an error: "Bad Gateway" body when the upstream port is closed, then serves a follow-up request in the same process', async () => {
  const { stdout } = await run(process.execPath, [CONNECTION_FAILURE_RUNNER], {
    timeout: 20000,
    encoding: 'utf8',
  });
  const report = JSON.parse(stdout);

  assert.equal(report.failed.status, 502);
  assert.equal(report.failed.json.error, 'Bad Gateway');
  assert.equal(report.failed.json.message, 'Failed to communicate with OpenRouter API');
  assert.equal(typeof report.failed.json.details, 'string');
  assert.equal(report.followUp.status, 200);
  assert.deepEqual(report.followUp.json, { ok: true });
});

test('strips every hop-by-hop request header in the proxy removal set while ordinary headers pass through', async () => {
  requireStartup();
  const ordinaryHeaders = { 'x-ordinary': 'synthetic-ordinary-header' };

  const firstResponse = await request(gateway.origin, {
    method: 'GET',
    path: '/v1/ordinary',
    headers: {
      host: 'synthetic-client.invalid',
      'content-length': '0',
      connection: 'x-synthetic-connection',
      'keep-alive': 'timeout=99',
      ...ordinaryHeaders,
    },
  });
  const firstForwarded = upstream.requests.at(-1);
  const firstRequestEntry = logs.entries.find(
    (entry) => entry.type === 'request' && entry.url.endsWith('/api/v1/ordinary'),
  );

  const secondResponse = await request(gateway.origin, {
    method: 'GET',
    path: '/v1/ordinary',
    headers: {
      'proxy-authenticate': 'Basic synthetic-proxy-authenticate',
      'proxy-authorization': 'Basic synthetic-proxy-authorization',
      'transfer-encoding': 'chunked',
      upgrade: 'x-synthetic-upgrade',
      ...ordinaryHeaders,
    },
  });
  const secondForwarded = upstream.requests.at(-1);

  assert.equal(firstResponse.status, 200);
  assert.equal(secondResponse.status, 200);
  assert.deepEqual(HOP_BY_HOP_HEADERS, [
    'host',
    'content-length',
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'transfer-encoding',
    'upgrade',
  ]);

  assert.notEqual(firstForwarded.headers.host, 'synthetic-client.invalid');
  assert.equal(firstForwarded.headers['content-length'], undefined);
  assert.notEqual(firstForwarded.headers.connection, 'x-synthetic-connection');
  assert.equal(firstForwarded.headers['keep-alive'], undefined);
  assert.equal(firstForwarded.headers['x-ordinary'], 'synthetic-ordinary-header');
  assert.equal(firstRequestEntry.headers.host, 'synthetic-client.invalid');
  assert.equal(firstRequestEntry.headers['content-length'], '0');
  assert.equal(firstRequestEntry.headers.connection, 'x-synthetic-connection');
  assert.equal(firstRequestEntry.headers['keep-alive'], 'timeout=99');
  assert.equal(firstRequestEntry.headers['x-ordinary'], 'synthetic-ordinary-header');

  assert.equal(secondForwarded.headers['proxy-authenticate'], undefined);
  assert.equal(secondForwarded.headers['proxy-authorization'], undefined);
  assert.equal(secondForwarded.headers['transfer-encoding'], undefined);
  assert.equal(secondForwarded.headers.upgrade, undefined);
  assert.equal(secondForwarded.headers['x-ordinary'], 'synthetic-ordinary-header');
});

test('excludes upstream transfer-encoding, content-encoding and content-length while forwarding other upstream headers', async () => {
  requireStartup();

  const headResponse = await request(gateway.origin, {
    method: 'HEAD',
    path: '/v1/filtered-head',
  });
  assert.equal(headResponse.status, 200);
  assert.equal(headResponse.headers['x-upstream'], 'synthetic-filtered-headers');
  assert.notEqual(headResponse.headers['content-length'], '777');
  assert.equal(headResponse.headers['content-encoding'], undefined);

  const chunkedResponse = await request(gateway.origin, {
    method: 'POST',
    path: '/v1/filtered-chunked',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'synthetic-model' }),
  });
  assert.equal(chunkedResponse.status, 200);
  assert.equal(chunkedResponse.body, 'Synthetic chunked body');
  assert.equal(chunkedResponse.headers['x-upstream'], 'synthetic-filtered-chunked');
  assert.equal(chunkedResponse.headers['content-encoding'], undefined);
  assert.equal(chunkedResponse.headers['transfer-encoding'], undefined);
  assert.equal(
    chunkedResponse.headers['content-length'],
    String(Buffer.byteLength('Synthetic chunked body')),
  );
});

test('current behaviour: when the upstream dies mid-SSE-stream the client stream ends without a 502 and no response record is written', async () => {
  requireStartup();
  const stream = openEventStream(gateway.origin, {
    method: 'POST',
    path: '/v1/stream-dies',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(SSE_REQUEST),
  });

  assert.equal(await stream.waitForText('Before failure'), true);
  releaseUpstreamFailure();
  const response = await stream.done;

  assert.equal(response.status, 200);
  assert.equal(response.headers['content-type'], 'text/event-stream');
  assert.equal(response.body, SSE_FAILURE_FIRST_EVENT);

  await logs.waitFor((entry) => entry.type === 'request' && entry.url.endsWith('/api/v1/stream-dies'));
  assert.equal(logs.entries.filter((entry) => entry.type === 'response').length, 0);
});
