const { Agent } = require('undici');
const { alpacaLimiter, quoteLimiter } = require('./limiters');
const { isCooling, recordFailure, recordSuccess } = require('./symbolFailures');

const CONNECT_TIMEOUT_MS = Number(process.env.HTTP_CONNECT_TIMEOUT_MS || 1000);
const REQUEST_TIMEOUT_MS = Number(process.env.HTTP_REQUEST_TIMEOUT_MS || 2000);
const TOTAL_RETRY_TIMEOUT_MS = Number(process.env.HTTP_TOTAL_RETRY_TIMEOUT_MS || 3000);

// Retry only transient HTTP status codes (429/5xx).
const retryStatusCodes = new Set([429, 500, 502, 503, 504]);
// Retry only transient network errors/timeouts.
const retryErrorCodes = new Set([
  'ECONNRESET',
  'ETIMEDOUT',
  'EAI_AGAIN',
  'ENOTFOUND',
  'ECONNREFUSED',
]);

const backoffScheduleMs = [250, 500, 1000];

const dispatcher = new Agent({
  connect: { timeout: CONNECT_TIMEOUT_MS },
  keepAliveTimeout: 10_000,
  keepAliveMaxTimeout: 60_000,
});

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

function extractErrorCode(err) {
  return err?.code || err?.cause?.code || err?.name || null;
}

function buildError({
  message,
  statusCode,
  errorCode,
  responseSnippet,
  attempt,
  totalAttempts,
  url,
  method,
  purpose,
  symbol,
  isTransient,
  cause,
}) {
  const error = new Error(message);
  error.statusCode = statusCode || null;
  error.errorCode = errorCode || null;
  error.responseSnippet = responseSnippet || '';
  error.attempt = attempt;
  error.totalAttempts = totalAttempts;
  error.url = url;
  error.method = method;
  error.purpose = purpose;
  error.symbol = symbol;
  error.isTransient = Boolean(isTransient);
  if (cause) {
    error.cause = cause;
  }
  return error;
}

function logRequestFailure(error) {
  console.warn('http_request_failed', {
    symbol: error.symbol,
    purpose: error.purpose,
    url: error.url,
    method: error.method,
    statusCode: error.statusCode,
    errorCode: error.errorCode,
    errorMessage: error.message,
    responseSnippet: error.responseSnippet,
    attempt: error.attempt,
    totalAttempts: error.totalAttempts,
  });
}

function parseBody(text, contentType) {
  if (!text) return null;
  if (contentType && contentType.includes('application/json')) {
    try {
      return JSON.parse(text);
    } catch (err) {
      return text;
    }
  }
  return text;
}

async function request({
  method,
  url,
  headers,
  body,
  purpose,
  symbol,
  timeoutMs = REQUEST_TIMEOUT_MS,
  totalRetryMs = TOTAL_RETRY_TIMEOUT_MS,
  allowRetry = false,
}) {
  if (symbol && isCooling(symbol)) {
    const cooldownError = buildError({
      message: `Cooldown active for ${symbol}`,
      errorCode: 'COOLDOWN',
      attempt: 0,
      totalAttempts: 0,
      url,
      method,
      purpose,
      symbol,
      isTransient: false,
    });
    throw cooldownError;
  }

  // Only allow retries for explicitly idempotent requests (allowRetry=true).
  const totalAttempts = allowRetry ? backoffScheduleMs.length + 1 : 1;
  const startTime = Date.now();

  const attemptRequest = async (attempt) => {
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort(new Error('Request timeout'));
    }, timeoutMs);

    try {
      const response = await fetch(url, {
        method,
        headers,
        body,
        dispatcher,
        signal: controller.signal,
      });
      clearTimeout(timeout);

      const responseText = await response.text();
      const responseSnippet = responseText ? responseText.slice(0, 200) : '';
      const data = parseBody(responseText, response.headers.get('content-type'));

      if (!response.ok) {
        const statusCode = response.status;
        const isTransient = retryStatusCodes.has(statusCode);
        const error = buildError({
          message: `HTTP ${statusCode}`,
          statusCode,
          errorCode: String(statusCode),
          responseSnippet,
          attempt,
          totalAttempts,
          url,
          method,
          purpose,
          symbol,
          isTransient,
        });
        logRequestFailure(error);
        if (symbol && isTransient) {
          recordFailure(symbol, {
            statusCode,
            errorCode: String(statusCode),
          });
        }
        return { error, isTransient, data };
      }

      if (symbol) {
        recordSuccess(symbol);
      }
      return { data };
    } catch (err) {
      clearTimeout(timeout);
      const errorCode = extractErrorCode(err);
      const isTransient = retryErrorCodes.has(errorCode) || err?.name === 'AbortError';
      const error = buildError({
        message: err?.message || 'Network request failed',
        errorCode,
        responseSnippet: '',
        attempt,
        totalAttempts,
        url,
        method,
        purpose,
        symbol,
        isTransient,
        cause: err,
      });
      logRequestFailure(error);
      if (symbol && isTransient) {
        recordFailure(symbol, {
          errorCode,
        });
      }
      return { error, isTransient };
    }
  };

  const limiter = getLimiterForUrl(url);
  for (let attempt = 1; attempt <= totalAttempts; attempt += 1) {
    const { error, isTransient, data } = limiter
      ? await limiter.schedule(() => attemptRequest(attempt))
      : await attemptRequest(attempt);

    if (!error) {
      return data;
    }

    const elapsed = Date.now() - startTime;
    const backoffIndex = attempt - 1;
    if (!allowRetry || !isTransient || backoffIndex >= backoffScheduleMs.length) {
      throw error;
    }

    const delay = backoffScheduleMs[backoffIndex] + Math.floor(Math.random() * 200);
    if (elapsed + delay > totalRetryMs) {
      throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  throw buildError({
    message: 'Retry limit exceeded',
    attempt: totalAttempts,
    totalAttempts,
    url,
    method,
    purpose,
    symbol,
    isTransient: false,
  });
}

function serializeJsonBody(payload) {
  if (payload == null) return undefined;
  return JSON.stringify(payload);
}

async function get(url, options = {}) {
  return request({
    method: 'GET',
    url,
    headers: options.headers,
    purpose: options.purpose,
    symbol: options.symbol,
    allowRetry: true,
  });
}

async function post(url, payload, options = {}) {
  return request({
    method: 'POST',
    url,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    body: serializeJsonBody(payload),
    purpose: options.purpose,
    symbol: options.symbol,
    allowRetry: Boolean(options.allowRetry),
  });
}

async function del(url, options = {}) {
  return request({
    method: 'DELETE',
    url,
    headers: options.headers,
    purpose: options.purpose,
    symbol: options.symbol,
    allowRetry: true,
  });
}

module.exports = {
  get,
  post,
  del,
};
