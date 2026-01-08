function normalizePair(rawSymbol) {
  if (!rawSymbol) return rawSymbol;
  const symbol = String(rawSymbol).trim().toUpperCase();
  if (!symbol) return symbol;
  if (symbol.includes('/')) {
    return symbol;
  }
  if (symbol.endsWith('USD') && symbol.length > 3 && !symbol.includes('-')) {
    return `${symbol.slice(0, -3)}/USD`;
  }
  return symbol;
}

function toAlpacaSymbol(pair) {
  if (!pair) return pair;
  const normalized = normalizePair(pair);
  return normalized ? normalized.replace('/', '') : normalized;
}

function alpacaSymbol(pair) {
  return toAlpacaSymbol(pair);
}

function canonicalPair(rawSymbol) {
  return normalizePair(rawSymbol);
}

function canonicalAsset(rawSymbol) {
  return toAlpacaSymbol(rawSymbol);
}

function toInternalSymbol(rawSymbol) {
  return normalizePair(rawSymbol);
}

function toAlpacaCryptoSymbol(rawSymbol) {
  return normalizePair(rawSymbol);
}

function normalizeCryptoSymbol(rawSymbol) {
  return normalizePair(rawSymbol);
}

function toTradeSymbol(rawSymbol) {
  return toAlpacaSymbol(rawSymbol);
}

function toDataSymbol(rawSymbol) {
  return normalizePair(rawSymbol);
}

function isCrypto(sym) {
  const normalized = normalizePair(sym);
  return /\/USD$/.test(normalized || '');
}

function isStock(sym) {
  return !isCrypto(sym);
}

const exportsObject = {
  canonicalPair,
  canonicalAsset,
  normalizePair,
  toAlpacaSymbol,
  alpacaSymbol,
  toInternalSymbol,
  toAlpacaCryptoSymbol,
  normalizeCryptoSymbol,
  toTradeSymbol,
  toDataSymbol,
  isCrypto,
  isStock,
};

module.exports = exportsObject;
