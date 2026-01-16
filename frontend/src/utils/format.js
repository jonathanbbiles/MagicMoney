export function formatUsd(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return '—';
  const abs = Math.abs(x);
  const digits = abs >= 1000 ? 0 : abs >= 100 ? 2 : 2;
  return x.toLocaleString(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: digits });
}

export function formatNum(n, { max = 8 } = {}) {
  const x = Number(n);
  if (!Number.isFinite(x)) return '—';
  const abs = Math.abs(x);
  const digits = abs >= 1000 ? 2 : abs >= 1 ? 6 : 8;
  return x.toLocaleString(undefined, { maximumFractionDigits: Math.min(digits, max) });
}

export function formatPct(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return '—';
  return `${(x * 100).toFixed(2)}%`;
}

export function formatAgo(iso) {
  if (!iso) return '—';
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return '—';
  const sec = Math.max(0, Math.floor((Date.now() - t) / 1000));
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  return `${hr}h ago`;
}

export function pickTradePrice(tradeObj) {
  if (!tradeObj) return null;
  // Alpaca "latest trade" tends to use `p` for price, `t` for timestamp
  const p = tradeObj.p ?? tradeObj.price ?? tradeObj.ap ?? null;
  const x = Number(p);
  return Number.isFinite(x) ? x : null;
}

export function safeUpper(s) {
  return String(s || '').trim().toUpperCase();
}
