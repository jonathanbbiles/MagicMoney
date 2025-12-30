export function toInternalSymbol(sym) {
  if (!sym) return "";
  return String(sym).replace("/", "").toUpperCase().trim();
}

export function toAlpacaCryptoSymbol(sym) {
  const s = toInternalSymbol(sym);
  if (s.length <= 3) return String(sym || "");
  const base = s.slice(0, s.length - 3);
  const quote = s.slice(-3);
  return `${base}/${quote}`;
}

export function normalizeCryptoSymbol(sym) {
  return toAlpacaCryptoSymbol(sym);
}

export function isCrypto(sym) {
  const normalized = toInternalSymbol(sym);
  return /USD$/.test(normalized || "");
}

export function isStock(sym) {
  return !isCrypto(sym);
}
