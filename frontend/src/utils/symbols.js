export function toDataSymbol(sym) {
  if (!sym) return sym;
  if (sym.includes('/')) return sym;
  if (sym.endsWith('USD')) {
    const base = sym.slice(0, -3);
    if (!base || base.toUpperCase().endsWith('USD')) return `${sym}/USD`;
    return `${base}/USD`;
  }
  return sym;
}

export function isCrypto(sym) {
  return /USD$/.test(sym);
}

export function isStock(sym) {
  return !isCrypto(sym);
}
