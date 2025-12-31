function normalizePair(rawSymbol) {
  if (!rawSymbol) return rawSymbol;
  const symbol = String(rawSymbol).trim().toUpperCase();
  if (!symbol) return symbol;
  if (symbol.includes('/')) {
    const [base, quote] = symbol.split('/');
    if (!base) return symbol;
    return `${base}/${quote || 'USD'}`;
  }
  if (symbol.includes('-')) {
    const [base, quote] = symbol.split('-');
    if (!base) return symbol;
    return `${base}/${quote || 'USD'}`;
  }
  if (symbol.endsWith('USD') && symbol.length > 3) {
    return `${symbol.slice(0, -3)}/USD`;
  }
  return symbol;
}

function alpacaSymbol(pair) {
  if (!pair) return pair;
  const normalized = normalizePair(pair);
  return normalized ? normalized.replace('/', '') : normalized;
}

function canonicalPair(rawSymbol) {
  return normalizePair(rawSymbol);
}

function canonicalAsset(rawSymbol) {
  return alpacaSymbol(rawSymbol);
}

module.exports = {
  canonicalPair,
  canonicalAsset,
  normalizePair,
  alpacaSymbol,
};
