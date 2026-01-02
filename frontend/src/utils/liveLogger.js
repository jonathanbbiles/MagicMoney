const LEVELS = ['ERROR', 'WARN', 'INFO', 'DEBUG'];

const cleanValue = (value) => {
  if (value == null) return null;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return String(value);
    return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(4)));
  }
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'string') return value.replace(/\s+/g, '_');
  return JSON.stringify(value);
};

const formatFields = (fields = {}) => {
  const parts = [];
  for (const [key, raw] of Object.entries(fields)) {
    const val = cleanValue(raw);
    if (val == null || val === '') continue;
    parts.push(`${key}=${val}`);
  }
  return parts.join(' ');
};

export const createLogger = ({
  getScanId = () => 0,
  getDebugEnabled = () => false,
  throttleMs = 4000,
} = {}) => {
  const throttles = new Map();

  const shouldLog = ({ level, event, symbol, throttleKey, throttleOverride }) => {
    if (level === 'DEBUG' && !getDebugEnabled()) return false;
    const ms = Number.isFinite(throttleOverride) ? throttleOverride : throttleMs;
    if (!(ms > 0)) return true;
    const key = throttleKey || `${event}|${symbol}`;
    const now = Date.now();
    const last = throttles.get(key) || 0;
    if (now - last < ms) return false;
    throttles.set(key, now);
    return true;
  };

  const log = ({
    level = 'INFO',
    event = 'EVENT',
    symbol = 'GLOBAL',
    scanId,
    fields = {},
    throttleKey,
    throttleOverride,
  } = {}) => {
    const lvl = LEVELS.includes(level) ? level : 'INFO';
    const scan_id = Number.isFinite(scanId) ? scanId : getScanId();
    const sym = symbol || 'GLOBAL';
    if (!shouldLog({ level: lvl, event, symbol: sym, throttleKey, throttleOverride })) return null;
    const timestamp = new Date().toISOString();
    const fieldText = formatFields(fields);
    const text = `${timestamp} ${lvl} ${event} scan_id=${scan_id} symbol=${sym}${fieldText ? ` ${fieldText}` : ''}`;
    return {
      timestamp,
      level: lvl,
      event,
      scan_id,
      symbol: sym,
      text,
      ...fields,
    };
  };

  return { log };
};
