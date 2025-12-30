const SYMBOL_CANONICAL_MAP = new Map([
  ['BCH/USD', 'BCHUSD'],
  ['BCH-USD', 'BCHUSD'],
  ['UNI/USD', 'UNIUSD'],
  ['UNI-USD', 'UNIUSD'],
  ['LTC/USD', 'LTCUSD'],
  ['LTC-USD', 'LTCUSD'],
  ['XRP/USD', 'XRPUSD'],
  ['XRP-USD', 'XRPUSD'],
  ['BTC/USD', 'BTCUSD'],
  ['BTC-USD', 'BTCUSD'],
  ['ETH/USD', 'ETHUSD'],
  ['ETH-USD', 'ETHUSD'],
  ['SOL/USD', 'SOLUSD'],
  ['SOL-USD', 'SOLUSD'],
  ['AAVE/USD', 'AAVEUSD'],
  ['AAVE-USD', 'AAVEUSD'],
]);

export function toInternalSymbol(sym) {
  if (!sym) return sym;
  const t = String(sym).trim().toUpperCase();
  if (SYMBOL_CANONICAL_MAP.has(t)) return SYMBOL_CANONICAL_MAP.get(t);
  if (t.includes('/')) return t.replace(/\//g, '');
  if (t.endsWith('-USD')) return t.replace(/-USD$/, 'USD');
  if (t.endsWith('/USD')) return t.replace(/\/USD$/, 'USD');
  return t;
}

export function toAlpacaCryptoSymbol(sym) {
  if (!sym) return sym;
  const internal = toInternalSymbol(sym);
  if (internal.includes('/')) return internal;
  if (internal.endsWith('USD')) {
    const base = internal.slice(0, -3);
    if (!base || base.toUpperCase().endsWith('USD')) return `${internal}/USD`;
    return `${base}/USD`;
  }
  return internal;
}

export function normalizeCryptoSymbol(sym) {
  if (!sym) return sym;
  return toAlpacaCryptoSymbol(sym);
}

export function toTradeSymbol(sym) {
  return toInternalSymbol(sym);
}

export function toDataSymbol(sym) {
  return toAlpacaCryptoSymbol(sym);
}

export function isCrypto(sym) {
  const normalized = toInternalSymbol(sym);
  return /USD$/.test(normalized || '');
}

export function isStock(sym) {
  return !isCrypto(sym);
}
