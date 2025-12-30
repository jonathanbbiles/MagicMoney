function canonicalPair(rawSymbol) {
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

function canonicalAsset(rawSymbol) {
  if (!rawSymbol) return rawSymbol;
  const pair = canonicalPair(rawSymbol);
  if (!pair) return pair;
  return pair.replace('/', '');
}

module.exports = {
  canonicalPair,
  canonicalAsset,
};
