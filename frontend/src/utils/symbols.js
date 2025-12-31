export function normalizePair(sym) {
  if (!sym) return sym;
  const t = String(sym).trim().toUpperCase();
  if (!t) return t;
  if (t.includes('/')) return t;
  if (t.endsWith('USD') && t.length > 3 && !t.includes('-')) {
    return `${t.slice(0, -3)}/USD`;
  }
  return t;
}

export function toAlpacaSymbol(pair) {
  if (!pair) return pair;
  const normalized = normalizePair(pair);
  return normalized ? normalized.replace('/', '') : normalized;
}

export function alpacaSymbol(pair) {
  return toAlpacaSymbol(pair);
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
