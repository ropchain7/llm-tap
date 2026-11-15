
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');

const { request, startGateway } = require('./gateway');
const { startMockUpstream } = require('./mock-upstream');

async function reserveClosedPort() {
  const probe = http.createServer();
  await new Promise((resolve, reject) => {
    probe.once('error', reject);
    probe.listen(0, '127.0.0.1', () => {
      probe.off('error', reject);
      resolve();
    });
  });
  const { port } = probe.address();
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

async function main() {
  const logDir = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-tap-connection-failure-'));
  process.env.LOG_DIR = logDir;

  const port = await reserveClosedPort();
  const gateway = await startGateway(`http://127.0.0.1:${port}/api/v1`);
  let upstream;

  try {
    const failed = await request(gateway.origin, {
      method: 'POST',
      path: '/v1/chat',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'synthetic-model' }),
    });

    upstream = await startMockUpstream(async (record, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    }, { listenPort: port });

    const followUp = await request(gateway.origin, {
      method: 'GET',
      path: '/v1/ordinary',
    });

    process.stdout.write(`${JSON.stringify({
      failed: { status: failed.status, json: failed.json },
      followUp: { status: followUp.status, json: followUp.json },
    })}\n`);
  } finally {
    if (upstream) {
      await upstream.close();
    }
    await gateway.close();
    try {
      fs.rmSync(logDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 50 });
    } catch {}

  }
}

main().catch((err) => {
  process.stderr.write(`${err.stack || err.message}\n`);
  process.exitCode = 1;
});
