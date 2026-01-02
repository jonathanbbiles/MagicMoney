// src/utils/liveLogger.js
const LEVELS = { ERROR: 0, WARN: 1, INFO: 2, DEBUG: 3 };
let minLevel = 'INFO';
let debugEnabled = false;

export function setLogLevel(level) {
  if (LEVELS[level] == null) return;
  minLevel = level;
}
export function setDebugEnabled(enabled) {
  debugEnabled = !!enabled;
}

function shouldLog(level) {
  if (level === 'DEBUG') return debugEnabled;
  return LEVELS[level] <= LEVELS[minLevel];
}

function fmt(fields) {
  if (!fields) return '';
  return Object.entries(fields).map(([k, v]) => `${k}=${String(v)}`).join(' ');
}

function emit(level, code, fields) {
  if (!shouldLog(level)) return;
  const ts = new Date().toISOString();
  const tail = fmt(fields);
  console.log(`${ts} ${level} ${code}${tail ? ' ' + tail : ''}`);
}

export const LiveLog = {
  error: (code, fields) => emit('ERROR', code, fields),
  warn:  (code, fields) => emit('WARN', code, fields),
  info:  (code, fields) => emit('INFO', code, fields),
  debug: (code, fields) => emit('DEBUG', code, fields),
};
