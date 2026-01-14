const ENV =
  (typeof process !== 'undefined' && process && process.env) ? process.env : {};
const MAX_QUOTE_AGE_MS = Number(ENV.MAX_QUOTE_AGE_MS || 30000);
const ABSURD_AGE_MS = Number(ENV.ABSURD_AGE_MS || 86400 * 1000);
const MAX_CLOCK_SKEW_MS = 5000;

function normalizeEpochNumber(rawTs) {
  if (!Number.isFinite(rawTs)) return null;
  const abs = Math.abs(rawTs);
  let tsMs = abs;
  if (abs < 2e10) {
    tsMs = abs * 1000;
  } else if (abs < 2e13) {
    tsMs = abs;
  } else if (abs < 2e16) {
    tsMs = Math.floor(abs / 1000);
  } else {
    tsMs = Math.floor(abs / 1e6);
  }
  return Number.isFinite(tsMs) ? tsMs : null;
}

function normalizeQuoteTsMs(rawTs) {
  if (rawTs == null) return null;
  if (rawTs instanceof Date) {
    const ts = rawTs.getTime();
    return Number.isFinite(ts) ? ts : null;
  }

  if (typeof rawTs === 'string') {
    const trimmed = rawTs.trim();
    if (!trimmed) return null;
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
      return normalizeEpochNumber(numeric);
    }
    const parsed = Date.parse(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }

  if (typeof rawTs === 'number') {
    return normalizeEpochNumber(rawTs);
  }

  return null;
}

function computeQuoteAgeMs({ nowMs, tsMs }) {
  if (!Number.isFinite(nowMs) || !Number.isFinite(tsMs)) return null;
  let ageMs = nowMs - tsMs;
  if (!Number.isFinite(ageMs)) return null;
  if (ageMs < -MAX_CLOCK_SKEW_MS) {
    ageMs = 0;
  }
  return ageMs;
}

function normalizeQuoteAgeMs(ageMs) {
  if (!Number.isFinite(ageMs)) return null;
  if (ageMs > ABSURD_AGE_MS) return null;
  return ageMs;
}

function isStaleQuoteAge(ageMs) {
  return Number.isFinite(ageMs) && ageMs > MAX_QUOTE_AGE_MS;
}

function isFresh(tsMs, ttlMs) {
  return Number.isFinite(tsMs) && Date.now() - tsMs <= ttlMs;
}

const exportsObject = {
  MAX_QUOTE_AGE_MS,
  ABSURD_AGE_MS,
  MAX_CLOCK_SKEW_MS,
  normalizeQuoteTsMs,
  computeQuoteAgeMs,
  normalizeQuoteAgeMs,
  isStaleQuoteAge,
  isFresh,
};

module.exports = exportsObject;
