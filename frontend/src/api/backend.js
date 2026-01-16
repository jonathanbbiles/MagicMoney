import Constants from 'expo-constants';

const EX = Constants?.expoConfig?.extra || Constants?.manifest?.extra || {};
const API_TOKEN = String(EX.API_TOKEN || '').trim();

const RENDER_BACKEND_URL = 'https://magicmoney.onrender.com';
const normalizeBaseUrl = (value) => String(value || '').trim().replace(/\/+$/, '');
const BACKEND_BASE_URL = normalizeBaseUrl(EX.BACKEND_BASE_URL || RENDER_BACKEND_URL);

const DEFAULT_TIMEOUT_MS = 10000;

function withTimeout(promise, ms = DEFAULT_TIMEOUT_MS) {
  let timer = null;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error('timeout')), ms);
  });
  return Promise.race([promise, timeout]).finally(() => timer && clearTimeout(timer));
}

function headers() {
  return {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...(API_TOKEN ? { Authorization: `Bearer ${API_TOKEN}` } : {}),
  };
}

export function getBaseUrl() {
  return BACKEND_BASE_URL;
}

export async function apiGet(path, { timeoutMs } = {}) {
  const url = `${BACKEND_BASE_URL}${path.startsWith('/') ? '' : '/'}${path}`;
  const res = await withTimeout(fetch(url, { method: 'GET', headers: headers() }), timeoutMs);
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch (_) { json = null; }
  if (!res.ok) {
    const msg = json?.error || json?.message || `HTTP ${res.status}`;
    const err = new Error(msg);
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

export function buildSymbolsParam(symbols) {
  return encodeURIComponent((symbols || []).filter(Boolean).join(','));
}
