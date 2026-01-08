const quoteUtils = require('shared/quoteUtils');
const normalizeQuoteTsMs = quoteUtils.normalizeQuoteTsMs;
const isFreshQuote = quoteUtils.isFresh;

export const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));

export const fmtUSD = (n) =>
  Number.isFinite(n)
    ? `$ ${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
    : '—';

export const fmtPct = (n) => (Number.isFinite(n) ? `${n.toFixed(2)}%` : '—');

export { normalizeQuoteTsMs };

export const parseTsMs = normalizeQuoteTsMs;

export const isFresh = isFreshQuote;

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
