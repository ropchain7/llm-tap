const express = require('express');
const { randomUUID } = require('crypto');
const { TextDecoder } = require('util');
const { logEntry, sanitizeHeaders } = require('./logger');

const router = express.Router();

const TARGET_BASE_URL = process.env.TARGET_BASE_URL || 'https://openrouter.ai/api/v1';
const LOG_HEADERS = process.env.LOG_HEADERS !== 'false';
const LOG_BODY = process.env.LOG_BODY !== 'false';
const LOG_RESPONSE = process.env.LOG_RESPONSE !== 'false';

function getHostFromUrl(urlString) {
  try {
    const url = new URL(urlString);
    return url.host;
  } catch {
    return 'openrouter.ai';
  }
}

function buildForwardHeaders(clientHeaders) {
  const forward = { ...clientHeaders };
  const hopByHop = [
    'host',
    'content-length',
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'transfer-encoding',
    'upgrade',
  ];
  
  hopByHop.forEach(h => delete forward[h]);
  forward['host'] = getHostFromUrl(TARGET_BASE_URL);
  
  return forward;
}

function copyResponseHeaders(upstreamHeaders, clientRes) {
  const exclude = new Set(['transfer-encoding', 'content-encoding', 'content-length', 'authorization']);

  const collected = new Map();
  for (const [key, value] of upstreamHeaders.entries()) {
    const lower = key.toLowerCase();
    if (exclude.has(lower)) {
      continue;
    }
    if (!collected.has(lower)) {
      collected.set(lower, { name: key, values: [] });
    }
    collected.get(lower).values.push(value);
  }

  const cookies = typeof upstreamHeaders.getSetCookie === 'function'
    ? upstreamHeaders.getSetCookie()
    : (collected.get('set-cookie')?.values ?? []);
  if (cookies.length > 0) {
    collected.set('set-cookie', { name: 'Set-Cookie', values: cookies });
  }

  for (const { name, values } of collected.values()) {
    clientRes.setHeader(name, values.length === 1 ? values[0] : values);
  }
}

router.all('*', async (req, res, next) => {
  const requestId = randomUUID();
  const startTime = Date.now();
  
  const targetPath = req.path.replace(/^\/v1/, '');
  const targetUrl = `${TARGET_BASE_URL}${targetPath}`;

  try {
    const requestLog = {
      id: requestId,
      timestamp: new Date().toISOString(),
      type: 'request',
      method: req.method,
      url: targetUrl,
    };
    
    if (LOG_HEADERS) {
      requestLog.headers = sanitizeHeaders(req.headers);
    }
    
    if (LOG_BODY && req.body) {
      requestLog.body = req.body;
    }
    
    logEntry(requestLog);
  } catch (err) {
    console.error(`[Proxy] Failed to log the request (ignored): ${err.message}`);
  }

  try {
    const forwardHeaders = buildForwardHeaders(req.headers);
    const fetchOptions = {
      method: req.method,
      headers: forwardHeaders,
      body: ['GET', 'HEAD'].includes(req.method) ? undefined : 
            (typeof req.body === 'string' ? req.body : JSON.stringify(req.body)),
    };

    const upstreamResponse = await fetch(targetUrl, fetchOptions);
    const contentType = upstreamResponse.headers.get('content-type') || '';
    const isSSE = contentType.includes('text/event-stream');

    res.status(upstreamResponse.status);
    copyResponseHeaders(upstreamResponse.headers, res);

    if (isSSE) {
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('X-Accel-Buffering', 'no');
      res.flushHeaders();

      const reader = upstreamResponse.body.getReader();
      const decoder = new TextDecoder('utf-8');
      const chunks = [];
      let reconstructed = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        res.write(value);
        if (typeof res.flush === 'function') {
          res.flush();
        }

        chunks.push(Array.from(value));
        const textChunk = decoder.decode(value, { stream: true });
        
        const lines = textChunk.split('\n');
        for (const line of lines) {
          if (line.startsWith('data: ') && line.trim() !== 'data: [DONE]') {
            try {
              const dataObj = JSON.parse(line.substring(6));
              if (dataObj.choices && dataObj.choices[0]?.delta?.content) {
                reconstructed += dataObj.choices[0].delta.content;
              }
            } catch (e) {
            }
          }
        }
      }
      res.end();

      try {
        if (LOG_RESPONSE) {
          const responseLog = {
            id: requestId,
            timestamp: new Date().toISOString(),
            type: 'response',
            status: upstreamResponse.status,
            duration_ms: Date.now() - startTime,
            chunks: chunks,
            reconstructed_content: reconstructed
          };
          if (LOG_HEADERS) {
            responseLog.headers = sanitizeHeaders(Object.fromEntries(upstreamResponse.headers.entries()));
          }
          logEntry(responseLog);
        }
      } catch (err) {
        console.error(`[Proxy] Failed to log the SSE response: ${err.message}`);
      }

    } else {
      const responseText = await upstreamResponse.text();
      res.send(responseText);

      try {
        if (LOG_RESPONSE) {
          const responseLog = {
            id: requestId,
            timestamp: new Date().toISOString(),
            type: 'response',
            status: upstreamResponse.status,
            duration_ms: Date.now() - startTime
          };
          
          if (LOG_HEADERS) {
            responseLog.headers = sanitizeHeaders(Object.fromEntries(upstreamResponse.headers.entries()));
          }
          
          if (LOG_BODY) {
            try {
              responseLog.body = JSON.parse(responseText);
            } catch {
              responseLog.body = responseText;
            }
          }
          logEntry(responseLog);
        }
      } catch (err) {
        console.error(`[Proxy] Failed to log the response: ${err.message}`);
      }
    }
    
  } catch (err) {
    console.error(`[Proxy] Critical error during forwarding: ${err.message}`);
    
    if (!res.headersSent) {
      res.status(502).json({
        error: 'Bad Gateway',
        message: 'Failed to communicate with OpenRouter API',
        details: err.message
      });
    } else {
      res.end();
    }
  }
});

module.exports = router;
