const assert = require('node:assert/strict');
const { after, test } = require('node:test');

const { cleanup } = require('./helpers/env');

const { redactAuth, sanitizeHeaders } = require('../src/logger');

after(() => {
  cleanup();
});

test('redacts a long authorization value while retaining its prefix and suffix', () => {
  assert.equal(
    redactAuth('Bearer synthetic-access-token-0000'),
    'Bearer synth...REDACTED...0000',
  );
});

test('redacts short authorization values completely', () => {
  assert.equal(redactAuth('Bearer short'), '[REDACTED]');
  assert.equal(redactAuth(undefined), '[REDACTED]');
});

test('sanitizes authorization headers case-insensitively and preserves other headers', () => {
  const sanitized = sanitizeHeaders({
    Authorization: 'Bearer synthetic-access-token-0000',
    'x-request-id': 'synthetic-request-id',
  });

  assert.equal(sanitized.Authorization, 'Bearer synth...REDACTED...0000');
  assert.equal(sanitized['x-request-id'], 'synthetic-request-id');
});
