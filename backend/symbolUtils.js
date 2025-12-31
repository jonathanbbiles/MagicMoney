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

module.exports = {
  canonicalPair,
  canonicalAsset,
  normalizePair,
  toAlpacaSymbol,
  alpacaSymbol,
};
