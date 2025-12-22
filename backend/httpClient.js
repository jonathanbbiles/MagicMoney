const { alpacaLimiter, quoteLimiter } = require('./limiters');

const REQUEST_TIMEOUT_MS = Number(process.env.HTTP_REQUEST_TIMEOUT_MS || 2000);

function getLimiterForUrl(url) {
  const host = new URL(url).hostname;
  if (host.includes('data.alpaca.markets')) {
    return quoteLimiter;
  }
  if (host.includes('alpaca.markets')) {
    return alpacaLimiter;
  }
  return null;
}

async function executeFetch(url, options, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });

    const text = await response.text();

    if (!response.ok) {
      const snippet = text.slice(0, 200);
      throw new Error(
        `HTTP ${response.status} ${response.statusText} for ${url}: ${snippet}`,
      );
    }

    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch (err) {
      return text;
    }
  } catch (err) {
    const causeCode = err?.cause?.code;
    const causeText = causeCode ? ` (cause: ${causeCode})` : '';
    const name = err?.name || 'Error';
    const message = err?.message || 'Unknown error';
    throw new Error(`Fetch error for ${url}: ${name}: ${message}${causeText}`);
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchJson(method, url, { headers, body, timeoutMs = REQUEST_TIMEOUT_MS } = {}) {
  const limiter = getLimiterForUrl(url);
  const options = {
    method,
    headers,
    body,
  };

  if (limiter) {
    return limiter.schedule(() => executeFetch(url, options, timeoutMs));
  }

  return executeFetch(url, options, timeoutMs);
}

async function httpGetJson(url, { headers, timeoutMs } = {}) {
  return fetchJson('GET', url, { headers, timeoutMs });
}

async function httpPostJson(url, body, { headers, timeoutMs } = {}) {
  return fetchJson('POST', url, {
    headers: {
      'Content-Type': 'application/json',
      ...(headers || {}),
    },
    body: body == null ? undefined : JSON.stringify(body),
    timeoutMs,
  });
}

async function httpDeleteJson(url, { headers, timeoutMs } = {}) {
  return fetchJson('DELETE', url, { headers, timeoutMs });
}

module.exports = {
  httpGetJson,
  httpPostJson,
  httpDeleteJson,
};
