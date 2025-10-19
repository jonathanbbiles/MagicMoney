export const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));

export const fmtUSD = (n) =>
  Number.isFinite(n)
    ? `$ ${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
    : '—';

export const fmtPct = (n) => (Number.isFinite(n) ? `${n.toFixed(2)}%` : '—');

export const parseTsMs = (t) => {
  if (t == null) return NaN;
  if (typeof t === 'string') {
    const ms = Date.parse(t);
    if (Number.isFinite(ms)) return ms;
    const n = +t;
    if (Number.isFinite(n)) {
      if (n > 1e15) return Math.floor(n / 1e6);
      if (n > 1e12) return Math.floor(n / 1e3);
      if (n > 1e10) return n;
      if (n > 1e9) return n * 1000;
    }
    return NaN;
  }
  if (typeof t === 'number') {
    if (t > 1e15) return Math.floor(t / 1e6);
    if (t > 1e12) return Math.floor(t / 1e3);
    if (t > 1e10) return t;
    if (t > 1e9) return t * 1000;
    return t;
  }
  return NaN;
};

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
