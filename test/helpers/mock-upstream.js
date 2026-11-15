const http = require('node:http');

async function startMockUpstream(handler, { listenPort = 0 } = {}) {
  const requests = [];
  const server = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }

    const record = {
      method: req.method,
      url: req.url,
      headers: req.headers,
      body: Buffer.concat(chunks).toString('utf8'),
    };
    requests.push(record);

    try {
      await handler(record, res);
    } catch (err) {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'mock handler failed', message: err.message }));
    }
  });

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(listenPort, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });
  const { port } = server.address();

  return {
    requests,
    baseUrl: `http://127.0.0.1:${port}`,
    async close() {
      server.close();
      server.closeAllConnections?.();
      await new Promise((resolve) => server.once('close', resolve));
    },
  };
}

function sendJson(res, status, payload, extraHeaders = {}) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
    ...extraHeaders,
  });
  res.end(body);
  return body;
}

function sendText(res, status, body, extraHeaders = {}) {
  res.writeHead(status, {
    'content-type': 'text/plain; charset=utf-8',
    'content-length': Buffer.byteLength(body),
    ...extraHeaders,
  });
  res.end(body);
}

async function sendEventStream(res, status, segments, extraHeaders = {}) {
  res.writeHead(status, {
    'content-type': 'text/event-stream',
    ...extraHeaders,
  });
  if (typeof res.flushHeaders === 'function') {
    res.flushHeaders();
  }

  for (const segment of segments) {
    if (typeof segment === 'function') {
      await segment(res);
      continue;
    }
    res.write(segment);
    if (typeof res.flush === 'function') {
      res.flush();
    }
  }

  res.end();
}

module.exports = {
  startMockUpstream,
  sendJson,
  sendText,
  sendEventStream,
};
