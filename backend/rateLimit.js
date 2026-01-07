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
    return res.status(429).json({ ok: false, error: 'rate_limited' });
  }

  bucket.count += 1;
  return next();
};

module.exports = { rateLimit };
