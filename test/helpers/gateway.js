const fs = require('node:fs');
const http = require('node:http');

function request(origin, { method = 'GET', path = '/', headers = {}, body } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request(`${origin}${path}`, { method, headers, agent: false }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let json;
        try {
          json = JSON.parse(text);
        } catch {
          json = undefined;
        }
        resolve({ status: res.statusCode, headers: res.headers, body: text, json });
      });
    });

    req.on('error', reject);
    if (body !== undefined) {
      req.end(body);
    } else {
      req.end();
    }
  });
}

async function startGateway(targetBaseUrl) {
  process.env.TARGET_BASE_URL = targetBaseUrl;
  const express = require('express');
  const proxyRouter = require('../../src/proxy');

  const app = express();
  app.use(express.json({ limit: '10mb' }));
  app.use(express.text({ type: 'text/*', limit: '10mb' }));
  app.use(proxyRouter);

  const server = http.createServer(app);
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });
  const { port } = server.address();

  return {
    origin: `http://127.0.0.1:${port}`,
    async close() {
      server.close();
      server.closeAllConnections?.();
      await new Promise((resolve) => server.once('close', resolve));
    },
  };
}

function captureLogEntries() {
  const entries = [];
  const waiters = [];
  const originalAppendFile = fs.appendFile;

  fs.appendFile = (filePath, data, callback) => {
    const entry = JSON.parse(data);
    entries.push(entry);

    for (let index = waiters.length - 1; index >= 0; index -= 1) {
      if (waiters[index].predicate(entry)) {
        const [{ resolve }] = waiters.splice(index, 1);
        resolve(entry);
      }
    }

    if (typeof callback === 'function') {
      callback(null);
    }
  };

  return {
    entries,
    reset() {
      entries.length = 0;
    },
    waitFor(predicate, timeoutMs = 2000) {
      const existing = entries.find(predicate);
      if (existing) {
        return Promise.resolve(existing);
      }
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const index = waiters.findIndex((waiter) => waiter.resolve === onEntry);
          if (index !== -1) {
            waiters.splice(index, 1);
          }
          reject(new Error('timed out waiting for a matching log entry'));
        }, timeoutMs);

        function onEntry(entry) {
          clearTimeout(timer);
          resolve(entry);
        }

        waiters.push({ predicate, resolve: onEntry });
      });
    },
    restore() {
      fs.appendFile = originalAppendFile;
    },
  };
}

module.exports = {
  request,
  startGateway,
  captureLogEntries,
};
