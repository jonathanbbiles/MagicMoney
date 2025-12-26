export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let tokens = 180;
let lastRefill = Date.now();
const REFILL_RATE = 180 / 60000; // tokens/ms
const MAX_TOKENS = 180;

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
  for (let i = 0; i <= retries; i++) {
    await takeToken();
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...opts, signal: ac.signal });
      clearTimeout(timer);
      if (res.status === 429 || res.status >= 500) {
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
