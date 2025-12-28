export const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));

export const fmtUSD = (n) =>
  Number.isFinite(n)
    ? `$ ${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
    : '—';

export const fmtPct = (n) => (Number.isFinite(n) ? `${n.toFixed(2)}%` : '—');

export const normalizeQuoteTsMs = (rawTs) => {
  if (rawTs == null) return null;
  if (rawTs instanceof Date) {
    const ts = rawTs.getTime();
    return Number.isFinite(ts) ? ts : null;
  }
  if (typeof rawTs === 'string') {
    const trimmed = rawTs.trim();
    if (!trimmed) return null;
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) return normalizeEpochNumber(numeric);
    const parsed = Date.parse(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (typeof rawTs === 'number') {
    return normalizeEpochNumber(rawTs);
  }
  return null;
};

const normalizeEpochNumber = (rawTs) => {
  if (!Number.isFinite(rawTs)) return null;
  const abs = Math.abs(rawTs);
  return abs < 2e10 ? abs * 1000 : abs;
};

export const parseTsMs = normalizeQuoteTsMs;

export const isFresh = (tsMs, ttlMs) => Number.isFinite(tsMs) && Date.now() - tsMs <= ttlMs;

export const emaArr = (arr, span) => {
  if (!arr?.length) return [];
  const k = 2 / (span + 1);
  let prev = arr[0];
  const out = [prev];
  for (let i = 1; i < arr.length; i++) {
    prev = arr[i] * k + prev * (1 - k);
    out.push(prev);
  }
  return out;
};

export const roundToTick = (px, tick) => Math.ceil(px / tick) * tick;

export const isFractionalQty = (q) => Math.abs(q - Math.round(q)) > 1e-6;

export const isoDaysAgo = (n) => new Date(Date.now() - n * 864e5).toISOString();
