const SYMBOL_CANONICAL_MAP = new Map([
  ['BCH/USD', 'BCH/USD'],
  ['BCH-USD', 'BCH/USD'],
  ['UNI/USD', 'UNI/USD'],
  ['UNI-USD', 'UNI/USD'],
  ['LTC/USD', 'LTC/USD'],
  ['LTC-USD', 'LTC/USD'],
  ['XRP/USD', 'XRP/USD'],
  ['XRP-USD', 'XRP/USD'],
  ['BTC/USD', 'BTC/USD'],
  ['BTC-USD', 'BTC/USD'],
  ['ETH/USD', 'ETH/USD'],
  ['ETH-USD', 'ETH/USD'],
  ['SOL/USD', 'SOL/USD'],
  ['SOL-USD', 'SOL/USD'],
  ['AAVE/USD', 'AAVE/USD'],
  ['AAVE-USD', 'AAVE/USD'],
]);

export function normalizePair(sym) {
  if (!sym) return sym;
  const t = String(sym).trim().toUpperCase();
  if (SYMBOL_CANONICAL_MAP.has(t)) return SYMBOL_CANONICAL_MAP.get(t);
  if (t.includes('/')) {
    const [base, quote] = t.split('/');
    if (!base) return t;
    return `${base}/${quote || 'USD'}`;
  }
  if (t.includes('-')) {
    const [base, quote] = t.split('-');
    if (!base) return t;
    return `${base}/${quote || 'USD'}`;
  }
  if (t.endsWith('USD') && t.length > 3) {
    return `${t.slice(0, -3)}/USD`;
  }
  return t;
}

export function alpacaSymbol(pair) {
  if (!pair) return pair;
  const normalized = normalizePair(pair);
  return normalized ? normalized.replace('/', '') : normalized;
}

export function toInternalSymbol(sym) {
  return normalizePair(sym);
}

export function toAlpacaCryptoSymbol(sym) {
  return normalizePair(sym);
}

export function normalizeCryptoSymbol(sym) {
  return normalizePair(sym);
}

export function toTradeSymbol(sym) {
  return alpacaSymbol(sym);
}

export function toDataSymbol(sym) {
  return normalizePair(sym);
}

export function isCrypto(sym) {
  const normalized = normalizePair(sym);
  return /\/USD$/.test(normalized || '');
}

export function isStock(sym) {
  return !isCrypto(sym);
}
