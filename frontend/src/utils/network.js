export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let tokens = 180;
let lastRefill = Date.now();
const REFILL_RATE = 180 / 60000; // tokens/ms
const MAX_TOKENS = 180;
const RESPONSE_CACHE_TTL_MS = 750;
const responseCache = new Map();
const backoffUntil = new Map();

const buildCacheKey = (url, opts = {}) => {
  const method = String(opts?.method || 'GET').toUpperCase();
  return `${method}:${url}`;
};

const buildCachedResponse = (cached) => {
  return new Response(cached.body, {
    status: cached.status,
    statusText: cached.statusText,
    headers: cached.headers,
  });
};

const readRetryAfter = (res) => {
  const header = res?.headers?.get?.('Retry-After') || res?.headers?.get?.('retry-after');
  const value = Number(header);
  if (!Number.isFinite(value)) return null;
  return Math.max(0, value);
};

const buildBackoffResponse = (retryAfterSec) => {
  const payload = JSON.stringify({ ok: false, error: 'rate_limited', retryAfterSec });
  return new Response(payload, {
    status: 429,
    headers: {
      'Content-Type': 'application/json',
      'Retry-After': String(retryAfterSec),
    },
  });
};

async function takeToken() {
  const now = Date.now();
  const elapsed = now - lastRefill;
  if (elapsed > 0) {
    tokens = Math.min(MAX_TOKENS, tokens + elapsed * REFILL_RATE);
    lastRefill = now;
  }
  if (tokens >= 1) {
    tokens -= 1;
    return;
  }
  const waitMs = Math.ceil((1 - tokens) / REFILL_RATE);
  await sleep(Math.min(1500, Math.max(50, waitMs)));
  return takeToken();
}

export async function rateLimitedFetch(url, opts = {}, timeoutMs = 12000, retries = 3) {
  let lastErr = null;
  const method = String(opts?.method || 'GET').toUpperCase();
  const cacheKey = buildCacheKey(url, opts);
  if (method === 'GET') {
    const blockedUntil = backoffUntil.get(cacheKey);
    if (blockedUntil && Date.now() < blockedUntil) {
      const retryAfterSec = Math.ceil((blockedUntil - Date.now()) / 1000);
      return buildBackoffResponse(retryAfterSec);
    }
    const cached = responseCache.get(cacheKey);
    if (cached && Date.now() - cached.ts < RESPONSE_CACHE_TTL_MS) {
      return buildCachedResponse(cached);
    }
  }
  for (let i = 0; i <= retries; i++) {
    await takeToken();
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...opts, signal: ac.signal });
      clearTimeout(timer);
      if (method === 'GET') {
        const clone = res.clone();
        clone.text()
          .then((body) => {
            responseCache.set(cacheKey, {
              ts: Date.now(),
              status: res.status,
              statusText: res.statusText,
              headers: Array.from(clone.headers.entries()),
              body,
            });
          })
          .catch(() => {});
      }
      if (res.status === 429 || res.status >= 500) {
        if (res.status === 429 && method === 'GET') {
          const retryAfter = readRetryAfter(res);
          if (Number.isFinite(retryAfter)) {
            backoffUntil.set(cacheKey, Date.now() + retryAfter * 1000);
          }
        }
        if (i === retries) return res;
        const extra = res.status === 429 ? 1200 + Math.floor(Math.random() * 800) : 0;
        await sleep(400 * Math.pow(2, i) + Math.floor(Math.random() * 300) + extra);
        continue;
      }
      return res;
    } catch (err) {
      clearTimeout(timer);
      lastErr = err;
      if (i === retries) throw err;
      await sleep(300 * Math.pow(2, i) + Math.floor(Math.random() * 250));
    }
  }
  throw lastErr || new Error('fetch failed');
}
