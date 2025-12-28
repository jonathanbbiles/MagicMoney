const MAX_STALE_AGE_SECONDS = 7 * 24 * 3600;
const MAX_CLOCK_SKEW_SECONDS = 5;

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

function normalizeEpochNumber(rawTs) {
  if (!Number.isFinite(rawTs)) return null;
  const abs = Math.abs(rawTs);
  const tsMs = abs < 2e10 ? abs * 1000 : abs;
  return Number.isFinite(tsMs) ? tsMs : null;
}

function computeQuoteAgeSeconds({ nowMs, tsMs }) {
  if (!Number.isFinite(nowMs) || !Number.isFinite(tsMs)) return null;
  let ageSec = (nowMs - tsMs) / 1000;
  if (!Number.isFinite(ageSec)) return null;
  if (ageSec < -MAX_CLOCK_SKEW_SECONDS) {
    ageSec = 0;
  }
  return ageSec;
}

function isStaleQuoteAge(ageSec) {
  return Number.isFinite(ageSec) && ageSec > MAX_STALE_AGE_SECONDS;
}

module.exports = {
  MAX_STALE_AGE_SECONDS,
  normalizeQuoteTsMs,
  computeQuoteAgeSeconds,
  isStaleQuoteAge,
};
