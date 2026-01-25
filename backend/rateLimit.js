const getWindowMs = () => {
  const value = Number(process.env.RATE_LIMIT_WINDOW_MS);
  return Number.isFinite(value) && value > 0 ? value : 60000;
};

const getMax = () => {
  const value = Number(process.env.RATE_LIMIT_MAX);
  return Number.isFinite(value) && value > 0 ? value : 120;
};

const buckets = new Map();

const rateLimit = (req, res, next) => {
  const windowMs = getWindowMs();
  const max = getMax();
  const now = Date.now();
  const ip = req.ip || 'unknown';

  const bucket = buckets.get(ip);
  if (!bucket || bucket.resetAt <= now) {
    buckets.set(ip, { count: 1, resetAt: now + windowMs });
    return next();
  }

  if (bucket.count >= max) {
    const retryAfterMs = Math.max(0, bucket.resetAt - now);
    const retryAfterSec = Math.ceil(retryAfterMs / 1000);
    res.set('Retry-After', String(retryAfterSec));
    res.set('x-rate-limit-window-ms', String(windowMs));
    res.set('x-rate-limit-max', String(max));
    return res.status(429).json({ ok: false, error: 'rate_limited', retryAfterSec });
  }

  bucket.count += 1;
  return next();
};

module.exports = { rateLimit };
