const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const logDir = fs.mkdtempSync(path.join(os.tmpdir(), 'llm-tap-test-'));
process.env.LOG_DIR = logDir;

let cleaned = false;

function cleanup() {
  if (cleaned) {
    return;
  }
  cleaned = true;

  if (!fs.existsSync(logDir)) {
    return;
  }

  try {
    fs.rmSync(logDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 50 });
  } catch {}

}

module.exports = {
  logDir,
  cleanup,
};
