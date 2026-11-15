const fs = require('fs');
const path = require('path');

const LOG_DIR = process.env.LOG_DIR || './logs';
const LOG_HEADERS = process.env.LOG_HEADERS !== 'false';
const LOG_BODY = process.env.LOG_BODY !== 'false';
const LOG_RESPONSE = process.env.LOG_RESPONSE !== 'false';
const REDACT_AUTH = process.env.REDACT_AUTH !== 'false';

try {
  fs.mkdirSync(LOG_DIR, { recursive: true });
} catch (err) {
  console.error('Failed to create log directory:', err.message);
}

function redactAuth(headerValue) {
  if (!headerValue || typeof headerValue !== 'string') {
    return '[REDACTED]';
  }
  
  if (headerValue.length < 20) {
    return '[REDACTED]';
  }
  
  const first12 = headerValue.substring(0, 12);
  const last4 = headerValue.substring(headerValue.length - 4);
  
  return `${first12}...REDACTED...${last4}`;
}

function sanitizeHeaders(headersObj, redact = true) {
  if (!headersObj || typeof headersObj !== 'object') {
    return {};
  }
  
  const sanitized = {};
  
  for (const [key, value] of Object.entries(headersObj)) {
    if (key.toLowerCase() === 'authorization' && redact && REDACT_AUTH) {
      sanitized[key] = redactAuth(value);
    } else {
      sanitized[key] = value;
    }
  }
  
  return sanitized;
}

function getLogFilename() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}.jsonl`;
}

function logEntry(entry) {
  if (!LOG_RESPONSE && entry.type && entry.type.startsWith('response')) {
    return;
  }
  
  const logPath = path.join(LOG_DIR, getLogFilename());
  const logLine = JSON.stringify(entry) + '\n';

  fs.appendFile(logPath, logLine, (err) => {
    if (err) {
      console.error('Failed to write log entry:', err.message);
    }
  });
}

module.exports = {
  logEntry,
  redactAuth,
  sanitizeHeaders,
  LOG_DIR,
  LOG_HEADERS,
  LOG_BODY,
  LOG_RESPONSE,
  REDACT_AUTH
};
