const { alpacaLimiter, quoteLimiter } = require('./limiters');

const DEFAULT_TIMEOUT_MS = 10000;
const DEFAULT_RETRIES = 2;

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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetry(error) {
  if (!error) return false;
  if (error.isTimeout || error.isNetworkError) return true;
  if (Number.isFinite(error.statusCode) && error.statusCode === 429) return true;
  if (Number.isFinite(error.statusCode) && error.statusCode >= 500) return true;
  return false;
}

function getBackoffDelayMs(attempt, error) {
  const baseDelays = [250, 750, 1750, 3750];
  const base = baseDelays[Math.min(attempt, baseDelays.length - 1)];
  const jitter = Math.floor(Math.random() * 120);
  if (Number.isFinite(error?.statusCode) && error.statusCode === 429) {
    const extra = 1000 + Math.floor(Math.random() * 1200);
    return base + jitter + extra;
  }
  return base + jitter;
}

async function executeFetch({ method, url, headers, body, timeoutMs }) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      method,
      headers,
      body,
      signal: controller.signal,
    });

    const text = await response.text();
    const snippet = text ? text.slice(0, 200) : '';

    if (!response.ok) {
      return {
        data: null,
        error: {
          url,
          method,
          statusCode: response.status,
          errorMessage: `HTTP ${response.status} ${response.statusText}`,
          responseSnippet200: snippet,
          isTimeout: false,
          isNetworkError: false,
        },
        statusCode: response.status,
      };
    }

    if (!text) {
      return { data: null, error: null, statusCode: response.status };
    }

    try {
      return { data: JSON.parse(text), error: null, statusCode: response.status };
    } catch (err) {
      return {
        data: null,
        error: {
          url,
          method,
          statusCode: response.status,
          errorMessage: 'parse_error',
          responseSnippet200: snippet,
          isTimeout: false,
          isNetworkError: false,
          parse_error: true,
        },
        statusCode: response.status,
      };
    }
  } catch (err) {
    const isTimeout = err?.name === 'AbortError';
    const message = err?.message || 'Network error';
    return {
      data: null,
      error: {
        url,
        method,
        statusCode: null,
        errorMessage: message,
        responseSnippet200: '',
        isTimeout,
        isNetworkError: !isTimeout,
      },
      statusCode: null,
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function httpJson({
  method,
  url,
  headers,
  body,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  retries = DEFAULT_RETRIES,
} = {}) {
  const limiter = getLimiterForUrl(url);

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const runner = () => executeFetch({ method, url, headers, body, timeoutMs });
    const result = limiter ? await limiter.schedule(runner) : await runner();

    if (!result.error) {
      return result;
    }

    if (attempt < retries && shouldRetry(result.error)) {
      await sleep(getBackoffDelayMs(attempt, result.error));
      continue;
    }

    return result;
  }

  return {
    data: null,
    error: {
      url,
      method,
      statusCode: null,
      errorMessage: 'Unknown HTTP error',
      responseSnippet200: '',
      isTimeout: false,
      isNetworkError: false,
    },
  };
}

module.exports = {
  httpJson,
};
