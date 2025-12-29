export function toTradeSymbol(sym) {
  if (!sym) return sym;
  const t = String(sym).trim().toUpperCase();
  if (t.includes('/')) return t.replace(/\//g, '');
  if (t.endsWith('-USD')) return t.replace(/-USD$/, 'USD');
  return t;
}

export function toDataSymbol(sym) {
  if (!sym) return sym;
  const normalized = toTradeSymbol(sym);
  if (normalized.includes('/')) return normalized;
  if (normalized.endsWith('USD')) {
    const base = normalized.slice(0, -3);
    if (!base || base.toUpperCase().endsWith('USD')) return `${normalized}/USD`;
    return `${base}/USD`;
  }
  return normalized;
}

export function isCrypto(sym) {
  return /USD$/.test(sym);
}

export function isStock(sym) {
  return !isCrypto(sym);
}
