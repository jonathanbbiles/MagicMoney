const { randomUUID } = require('crypto');

const { httpJson } = require('./httpClient');
const {
  MAX_QUOTE_AGE_MS,
  ABSURD_AGE_MS,
  normalizeQuoteTsMs,
  computeQuoteAgeMs,
  normalizeQuoteAgeMs,
  isStaleQuoteAge,
} = require('./quoteUtils');
const { canonicalAsset, normalizePair, alpacaSymbol } = require('./symbolUtils');

const RAW_TRADE_BASE = process.env.TRADE_BASE || process.env.ALPACA_API_BASE || 'https://api.alpaca.markets';
const RAW_DATA_BASE = process.env.DATA_BASE || 'https://data.alpaca.markets';

function normalizeTradeBase(baseUrl) {
  if (!baseUrl) return 'https://api.alpaca.markets';
  const trimmed = baseUrl.replace(/\/+$/, '');
  try {
    const parsed = new URL(trimmed);
    if (parsed.hostname.includes('data.alpaca.markets')) {
      console.warn('trade_base_invalid_host', { host: parsed.hostname });
      return 'https://api.alpaca.markets';
    }
  } catch (err) {
    console.warn('trade_base_parse_failed', { baseUrl: trimmed });
  }
  return trimmed.replace(/\/v2$/, '');
}

function normalizeDataBase(baseUrl) {
  if (!baseUrl) return 'https://data.alpaca.markets';
  let trimmed = baseUrl.replace(/\/+$/, '');
  try {
    const parsed = new URL(trimmed);
    if (parsed.hostname.includes('api.alpaca.markets') || parsed.hostname.includes('paper-api.alpaca.markets')) {
      console.warn('data_base_invalid_host', { host: parsed.hostname });
      return 'https://data.alpaca.markets';
    }
  } catch (err) {
    console.warn('data_base_parse_failed', { baseUrl: trimmed });
  }
  trimmed = trimmed.replace(/\/v1beta2$/, '');
  trimmed = trimmed.replace(/\/v1beta3$/, '');
  trimmed = trimmed.replace(/\/v2\/stocks$/, '');
  trimmed = trimmed.replace(/\/v2$/, '');
  return trimmed;
}

const TRADE_BASE = normalizeTradeBase(RAW_TRADE_BASE);
const DATA_BASE = normalizeDataBase(RAW_DATA_BASE);
const ALPACA_BASE_URL = `${TRADE_BASE}/v2`;
const DATA_URL = `${DATA_BASE}/v1beta3`;
const STOCKS_DATA_URL = `${DATA_BASE}/v2/stocks`;
const CRYPTO_DATA_URL = `${DATA_URL}/crypto`;

const ALPACA_KEY_ENV_VARS = ['APCA_API_KEY_ID', 'ALPACA_KEY_ID', 'ALPACA_API_KEY_ID', 'ALPACA_API_KEY'];
const ALPACA_SECRET_ENV_VARS = ['APCA_API_SECRET_KEY', 'ALPACA_SECRET_KEY', 'ALPACA_API_SECRET_KEY'];

const resolvedAlpacaAuth = (() => {
  const envStatus = {
    ALPACA_KEY_ID: Boolean(process.env.ALPACA_KEY_ID),
    ALPACA_SECRET_KEY: Boolean(process.env.ALPACA_SECRET_KEY),
    APCA_API_KEY_ID: Boolean(process.env.APCA_API_KEY_ID),
    APCA_API_SECRET_KEY: Boolean(process.env.APCA_API_SECRET_KEY),
    ALPACA_API_KEY_ID: Boolean(process.env.ALPACA_API_KEY_ID),
    ALPACA_API_KEY: Boolean(process.env.ALPACA_API_KEY),
  };
  console.log('alpaca_auth_env', envStatus);
  const keyId =
    process.env.APCA_API_KEY_ID ||
    process.env.ALPACA_KEY_ID ||
    process.env.ALPACA_API_KEY_ID ||
    process.env.ALPACA_API_KEY ||
    '';
  const secretKey =
    process.env.APCA_API_SECRET_KEY ||
    process.env.ALPACA_SECRET_KEY ||
    process.env.ALPACA_API_SECRET_KEY ||
    '';
  if (!keyId || !secretKey) {
    const missing = [];
    if (!keyId) missing.push('key id');
    if (!secretKey) missing.push('secret key');
    throw new Error(
      `Missing Alpaca ${missing.join(' and ')}. Checked env vars: key id -> ${ALPACA_KEY_ENV_VARS.join(
        ', '
      )}; secret -> ${ALPACA_SECRET_ENV_VARS.join(', ')}.`
    );
  }
  const alpacaKeyIdPresent = Boolean(keyId);
  const alpacaAuthOk = Boolean(keyId && secretKey);
  return {
    keyId,
    secretKey,
    alpacaKeyIdPresent,
    alpacaAuthOk,
  };
})();

function alpacaHeaders() {
  const headers = { Accept: 'application/json' };
  if (resolvedAlpacaAuth.keyId) {
    headers['APCA-API-KEY-ID'] = resolvedAlpacaAuth.keyId;
  }
  if (resolvedAlpacaAuth.secretKey) {
    headers['APCA-API-SECRET-KEY'] = resolvedAlpacaAuth.secretKey;
  }
  return headers;
}

function alpacaJsonHeaders() {
  return {
    'Content-Type': 'application/json',
    Accept: 'application/json',
    ...alpacaHeaders(),
  };
}

 

const MIN_ORDER_NOTIONAL_USD = Number(process.env.MIN_ORDER_NOTIONAL_USD || 15);
const MIN_TRADE_QTY = Number(process.env.MIN_TRADE_QTY || 1e-6);
const MARKET_DATA_TIMEOUT_MS = Number(process.env.MARKET_DATA_TIMEOUT_MS || 9000);
const MARKET_DATA_RETRIES = Number(process.env.MARKET_DATA_RETRIES || 2);
const MARKET_DATA_FAILURE_LIMIT = Number(process.env.MARKET_DATA_FAILURE_LIMIT || 5);
const MARKET_DATA_COOLDOWN_MS = Number(process.env.MARKET_DATA_COOLDOWN_MS || 60000);

const USER_MIN_PROFIT_BPS = Number(process.env.USER_MIN_PROFIT_BPS || 5);

const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS || 10);

const BUFFER_BPS = Number(process.env.BUFFER_BPS || 15);

const FEE_BPS_MAKER = Number(process.env.FEE_BPS_MAKER || 10);
const FEE_BPS_TAKER = Number(process.env.FEE_BPS_TAKER || 20);
const PROFIT_BUFFER_BPS = Number(process.env.PROFIT_BUFFER_BPS || 2);
const TAKER_EXIT_ON_TOUCH = readEnvFlag('TAKER_EXIT_ON_TOUCH', false);
const REPLACE_THRESHOLD_BPS = Number(process.env.REPLACE_THRESHOLD_BPS || 8);
const ORDER_TTL_MS = Number(process.env.ORDER_TTL_MS || 45000);
const SELL_ORDER_TTL_MS = Number(process.env.SELL_ORDER_TTL_MS || 45000);
const MIN_REPRICE_INTERVAL_MS = Number(process.env.MIN_REPRICE_INTERVAL_MS || 10000);
const REPRICE_IF_AWAY_BPS = Number(process.env.REPRICE_IF_AWAY_BPS || 8);
const MAX_SPREAD_BPS_TO_TRADE = Number(process.env.MAX_SPREAD_BPS_TO_TRADE || 60);

const MAX_HOLD_SECONDS = Number(process.env.MAX_HOLD_SECONDS || 300);
const MAX_HOLD_MS = Number(process.env.MAX_HOLD_MS || MAX_HOLD_SECONDS * 1000);

const REPRICE_EVERY_SECONDS = Number(process.env.REPRICE_EVERY_SECONDS || 20);

const FORCE_EXIT_SECONDS = Number(process.env.FORCE_EXIT_SECONDS || 600);

const PRICE_TICK = Number(process.env.PRICE_TICK || 0.01);
const MAX_CONCURRENT_POSITIONS = Number(process.env.MAX_CONCURRENT_POSITIONS || 100);
const MIN_POSITION_QTY = Number(process.env.MIN_POSITION_QTY || 1e-6);
const POSITIONS_SNAPSHOT_TTL_MS = Number(process.env.POSITIONS_SNAPSHOT_TTL_MS || 5000);
const QUOTE_CACHE_MAX_AGE_MS = MAX_QUOTE_AGE_MS;
const MAX_LOGGED_QUOTE_AGE_SECONDS = 9999;
const DEBUG_QUOTE_TS = ['1', 'true', 'yes'].includes(String(process.env.DEBUG_QUOTE_TS || '').toLowerCase());
const quoteTsDebugLogged = new Set();

const inventoryState = new Map();

const exitState = new Map();
const desiredExitBpsBySymbol = new Map();
const symbolLocks = new Map();
const lastActionAt = new Map();
const lastCancelReplaceAt = new Map();

const cfeeCache = { ts: 0, items: [] };
const quoteCache = new Map();
const lastQuoteAt = new Map();
const scanState = { lastScanAt: null };
let exitManagerRunning = false;
const positionsSnapshot = {
  tsMs: 0,
  mapBySymbol: new Map(),
  mapByRaw: new Map(),
  loggedNoneSymbols: new Set(),
  pending: null,
};
let lastHttpError = null;
const marketDataState = {
  consecutiveFailures: 0,
  cooldownUntil: 0,
  cooldownLoggedAt: 0,
};
let dataDegradedUntil = 0;

 

function sleep(ms) {

  return new Promise((resolve) => setTimeout(resolve, ms));

}

function logSkip(reason, details = {}) {

  console.log(`Skip — ${reason}`, details);

}

function logNetworkError({ type, symbol, attempts, context }) {
  console.warn(`Network error (${type})`, {
    symbol,
    attempts,
    context: context || null,
  });
}

function isNetworkError(err) {
  return Boolean(err?.isNetworkError || err?.isTimeout || err?.errorCode === 'NETWORK');
}

function buildAlpacaUrl({ baseUrl, path, params, label }) {
  const base = String(baseUrl || '').replace(/\/+$/, '');
  const cleanPath = String(path || '').replace(/^\/+/, '');
  const url = new URL(`${base}/${cleanPath}`);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value == null) return;
      url.searchParams.set(key, String(value));
    });
  }
  const finalUrl = url.toString();
  console.log('alpaca_request_url', { label, url: finalUrl });
  return finalUrl;
}

function toTradeSymbol(rawSymbol) {
  return normalizePair(rawSymbol);
}

function toDataSymbol(rawSymbol) {
  return normalizePair(rawSymbol);
}

const supportedCryptoPairsState = {
  loaded: false,
  pairs: new Set(),
  lastUpdated: null,
};

async function loadSupportedCryptoPairs({ force = false } = {}) {
  if (supportedCryptoPairsState.loaded && !force) return supportedCryptoPairsState.pairs;
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'assets',
    params: { asset_class: 'crypto' },
    label: 'crypto_assets',
  });
  try {
    const data = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
    const nextPairs = new Set();
    (Array.isArray(data) ? data : []).forEach((asset) => {
      if (!asset?.tradable || asset?.status !== 'active') return;
      const normalized = toDataSymbol(asset.symbol);
      if (normalized && normalized.endsWith('/USD')) {
        nextPairs.add(normalized);
      }
    });
    supportedCryptoPairsState.pairs = nextPairs;
    supportedCryptoPairsState.loaded = true;
    supportedCryptoPairsState.lastUpdated = new Date().toISOString();
  } catch (err) {
    console.warn('supported_pairs_fetch_failed', err?.errorMessage || err?.message || err);
  }
  return supportedCryptoPairsState.pairs;
}

function getSupportedCryptoPairsSnapshot() {
  return {
    pairs: Array.from(supportedCryptoPairsState.pairs),
    lastUpdated: supportedCryptoPairsState.lastUpdated,
  };
}

function filterSupportedCryptoSymbols(symbols = []) {
  if (!supportedCryptoPairsState.pairs.size) return symbols;
  return symbols.filter((sym) => supportedCryptoPairsState.pairs.has(toDataSymbol(sym)));
}

function isMarketDataCooldown() {
  return Date.now() < marketDataState.cooldownUntil;
}

function isDataDegraded() {
  return Date.now() < dataDegradedUntil;
}

function markDataDegraded() {
  dataDegradedUntil = Math.max(dataDegradedUntil, Date.now() + 2000);
}

function markMarketDataFailure(statusCode) {
  if (statusCode !== 429) {
    return;
  }
  marketDataState.consecutiveFailures += 1;
  if (marketDataState.consecutiveFailures >= MARKET_DATA_FAILURE_LIMIT && !isMarketDataCooldown()) {
    marketDataState.cooldownUntil = Date.now() + MARKET_DATA_COOLDOWN_MS;
    marketDataState.cooldownLoggedAt = Date.now();
    console.error('DATA DOWN — rate limit, pausing scans 60s');
  }
}

function markMarketDataSuccess() {
  marketDataState.consecutiveFailures = 0;
}

function formatLogUrl(url) {
  try {
    const parsed = new URL(url);
    return `${parsed.host}${parsed.pathname}${parsed.search}`;
  } catch (err) {
    return url;
  }
}

function getMarketDataLabel(type) {
  const normalized = String(type || '').toUpperCase();
  if (normalized === 'QUOTE' || normalized === 'QUOTES') return 'quotes';
  if (normalized === 'TRADE' || normalized === 'TRADES') return 'trades';
  if (normalized === 'BAR' || normalized === 'BARS') return 'bars';
  return normalized.toLowerCase() || 'marketdata';
}

function logMarketDataDiagnostics({ type, url, statusCode, snippet, errorType, requestId, urlHost, urlPath }) {
  const label = getMarketDataLabel(type);
  console.log('alpaca_marketdata', {
    label,
    type,
    url: formatLogUrl(url),
    urlHost: urlHost || null,
    urlPath: urlPath || null,
    requestId: requestId || null,
    statusCode,
    errorType,
    snippet,
  });
}

function logHttpError({ symbol, label, url, error }) {
  const axiosStatus = error?.response?.status;
  const statusCode = error?.statusCode ?? axiosStatus ?? null;
  const axiosData = error?.response?.data;
  const axiosSnippet =
    typeof axiosData === 'string'
      ? axiosData.slice(0, 200)
      : axiosData
        ? JSON.stringify(axiosData).slice(0, 200)
        : '';
  const errorMessage = error?.errorMessage || error?.message || `HTTP ${statusCode ?? 'NA'}`;
  const snippet = error?.responseSnippet200 || error?.responseSnippet || axiosSnippet || '';
  const method = error?.method || null;
  const requestId = error?.requestId || null;
  const urlHost = error?.urlHost || null;
  const urlPath = error?.urlPath || null;
  const errorType = error?.isNetworkError || error?.isTimeout ? 'network' : 'http';
  console.error('alpaca_http_error', {
    symbol,
    label,
    method,
    url: formatLogUrl(url),
    urlHost,
    urlPath,
    requestId,
    statusCode,
    errorType,
    errorMessage,
    snippet,
  });
  if (statusCode === 401 || statusCode === 403) {
    console.error('AUTH_ERROR: check Render env vars');
  }
}

function logPositionNoneOnce(symbol, statusCode = 404) {
  const normalized = normalizeSymbol(symbol);
  if (positionsSnapshot.loggedNoneSymbols.has(normalized)) return;
  positionsSnapshot.loggedNoneSymbols.add(normalized);
  console.log('POS_NONE', { symbol: normalized, status: statusCode });
}

function logPositionError({ symbol, statusCode, snippet, level = 'error', extra = {} }) {
  const normalized = normalizeSymbol(symbol);
  const payload = {
    symbol: normalized,
    status: statusCode ?? null,
    snippet: snippet || '',
    ...extra,
  };
  if (level === 'warn') {
    console.warn('POS_ERR', payload);
  } else {
    console.error('POS_ERR', payload);
  }
}

function updatePositionsSnapshot(positions = []) {
  const mapBySymbol = new Map();
  const mapByRaw = new Map();
  for (const pos of positions) {
    const rawSymbol = pos.rawSymbol ?? pos.symbol;
    const normalizedSymbol = normalizeSymbol(rawSymbol);
    if (rawSymbol) {
      mapByRaw.set(rawSymbol, pos);
    }
    if (normalizedSymbol) {
      mapBySymbol.set(normalizedSymbol, pos);
    }
  }
  positionsSnapshot.mapBySymbol = mapBySymbol;
  positionsSnapshot.mapByRaw = mapByRaw;
  positionsSnapshot.tsMs = Date.now();
  positionsSnapshot.loggedNoneSymbols.clear();
}

async function fetchPositionsSnapshot({ force = false } = {}) {
  const nowMs = Date.now();
  if (!force && positionsSnapshot.mapBySymbol.size > 0 && nowMs - positionsSnapshot.tsMs < POSITIONS_SNAPSHOT_TTL_MS) {
    return positionsSnapshot;
  }
  if (positionsSnapshot.pending) {
    return positionsSnapshot.pending;
  }
  positionsSnapshot.pending = (async () => {
    const positions = await fetchPositions();
    updatePositionsSnapshot(positions);
    return positionsSnapshot;
  })();
  try {
    return await positionsSnapshot.pending;
  } finally {
    positionsSnapshot.pending = null;
  }
}

function logOrderPayload({ payload }) {
  if (!payload) return;
  const symbolRaw = payload.symbol ?? '';
  const symbol = normalizePair(symbolRaw);
  const qty = payload.qty ?? 'NA';
  const limit = payload.limit_price ?? 'NA';
  let notional = payload.notional ?? null;
  if (!Number.isFinite(Number(notional)) && Number.isFinite(Number(qty)) && Number.isFinite(Number(limit))) {
    notional = Number(qty) * Number(limit);
  }
  const notionalLogged = notional ?? 'NA';
  console.log('order_submit', {
    symbol_raw: symbolRaw,
    symbol,
    side: payload.side,
    type: payload.type,
    tif: payload.time_in_force,
    qty,
    notional: notionalLogged,
    limit,
  });
}

function logOrderIntent({ label, payload, reason }) {
  if (!payload) return;
  const symbolRaw = payload.symbol ?? '';
  const symbol = normalizePair(symbolRaw);
  console.log('order_intent', {
    label: label || null,
    symbol_raw: symbolRaw,
    symbol,
    side: payload.side,
    type: payload.type,
    tif: payload.time_in_force,
    qty: payload.qty ?? payload.notional ?? 'NA',
    limit: payload.limit_price ?? 'NA',
    reason: reason || null,
  });
}

function logOrderResponse({ payload, response, error }) {
  const symbolRaw = payload?.symbol ?? '';
  const symbol = normalizePair(symbolRaw);
  if (response?.id) {
    console.log('order_ok', {
      id: response.id,
      status: response.status || response.order_status || 'accepted',
      symbol_raw: symbolRaw,
      symbol,
    });
    return;
  }
  if (response) {
    const body = JSON.stringify(response);
    console.warn('order_fail', {
      http: 'NA',
      code: 'NA',
      message: 'invalid_order_response',
      body,
      symbol_raw: symbolRaw,
      symbol,
    });
    return;
  }
  if (error) {
    const httpStatus = error?.statusCode ?? 'NA';
    const code = error?.errorCode ?? 'NA';
    const message = error?.errorMessage || error?.message || 'Unknown error';
    const body = error?.responseSnippet200 || error?.responseSnippet || '';
    console.warn('order_fail', {
      http: httpStatus,
      code,
      message,
      body,
      symbol_raw: symbolRaw,
      symbol,
    });
  }
}

async function placeOrderUnified({
  symbol,
  url,
  payload,
  label,
  reason,
  context,
}) {
  logOrderIntent({ label, payload, reason });
  logOrderPayload({ label, payload });
  let response;
  try {
    response = await requestJson({
      method: 'POST',
      url,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify(payload),
    });
    logOrderResponse({ label, payload, response });
  } catch (err) {
    logHttpError({ symbol, label: 'orders', url, error: err });
    logOrderResponse({ label, payload, error: err });
    if (isNetworkError(err)) {
      logNetworkError({
        type: 'order',
        symbol,
        attempts: err.attempts ?? 1,
        context: context || label || null,
      });
    }
    throw err;
  }
  return response;
}

async function requestJson({
  method,
  url,
  headers,
  body,
  timeoutMs,
  retries,
}) {
  const result = await httpJson({
    method,
    url,
    headers,
    body,
    timeoutMs,
    retries,
  });

  if (result.error) {
    lastHttpError = result.error;
    throw result.error;
  }

  return result.data;
}

async function requestMarketDataJson({ type, url, symbol }) {
  if (isMarketDataCooldown()) {
    const err = new Error('Market data cooldown active');
    err.errorCode = 'COOLDOWN';
    logMarketDataDiagnostics({
      type,
      url,
      statusCode: null,
      snippet: '',
      errorType: 'cooldown',
    });
    throw err;
  }
  if (isDataDegraded()) {
    const err = new Error('Market data degraded');
    err.errorCode = 'DEGRADED';
    logMarketDataDiagnostics({
      type,
      url,
      statusCode: null,
      snippet: '',
      errorType: 'degraded',
    });
    throw err;
  }

  const result = await httpJson({
    method: 'GET',
    url,
    headers: alpacaHeaders(),
    timeoutMs: MARKET_DATA_TIMEOUT_MS,
    retries: MARKET_DATA_RETRIES,
  });

  if (result.error) {
    const errorType = result.error.isNetworkError || result.error.isTimeout ? 'network' : 'http';
    logMarketDataDiagnostics({
      type,
      url,
      statusCode: result.error.statusCode ?? null,
      snippet: result.error.responseSnippet200 || '',
      errorType,
      requestId: result.error.requestId || null,
      urlHost: result.error.urlHost || null,
      urlPath: result.error.urlPath || null,
    });
    markMarketDataFailure(result.error.statusCode ?? null);
    markDataDegraded();
    lastHttpError = result.error;
    const err = new Error(result.error.errorMessage || 'Market data request failed');
    err.errorCode = errorType === 'network' ? 'NETWORK' : 'HTTP_ERROR';
    if (errorType === 'network') {
      err.attempts = result.error.attempts ?? MARKET_DATA_RETRIES + 1;
      logNetworkError({ type: String(type || 'QUOTE').toLowerCase(), symbol, attempts: err.attempts });
    }
    err.statusCode = result.error.statusCode ?? null;
    err.responseSnippet200 = result.error.responseSnippet200 || '';
    err.requestId = result.error.requestId || null;
    err.urlHost = result.error.urlHost || null;
    err.urlPath = result.error.urlPath || null;
    if (err.statusCode === 429 && result.error.rateLimit) {
      console.warn('marketdata_rate_limit', {
        type,
        limit: result.error.rateLimit.limit,
        remaining: result.error.rateLimit.remaining,
        reset: result.error.rateLimit.reset,
      });
    }
    throw err;
  }

  logMarketDataDiagnostics({
    type,
    url,
    statusCode: result.statusCode ?? 200,
    snippet: '',
    errorType: 'ok',
    requestId: result.requestId || null,
    urlHost: result.urlHost || null,
    urlPath: result.urlPath || null,
  });
  markMarketDataSuccess();
  return result.data;
}

function parseQuoteTimestamp({ quote, symbol, source }) {
  const raw = quote?.t ?? quote?.timestamp ?? quote?.time ?? quote?.ts;
  if (symbol) {
    logQuoteTimestampDebug({ symbol, rawTs: raw, source });
  }
  return normalizeQuoteTsMs(raw);
}

function recordLastQuoteAt(symbol, { tsMs, source, reason }) {
  const normalizedSymbol = normalizeSymbol(symbol);
  const entry = {
    tsMs: Number.isFinite(tsMs) ? tsMs : null,
    source,
    reason: reason || null,
  };
  lastQuoteAt.set(normalizedSymbol, entry);
}

function logQuoteAgeWarning({ symbol, ageMs, source, tsMs }) {
  if (!DEBUG_QUOTE_TS) return;
  if (!Number.isFinite(ageMs) || ageMs <= ABSURD_AGE_MS) return;
  console.warn('quote_age_warning', {
    symbol,
    ageSeconds: Math.round(ageMs / 1000),
    source: source || null,
    tsMs: Number.isFinite(tsMs) ? tsMs : null,
  });
}

function isDustQty(qty) {
  return Number.isFinite(qty) && Math.abs(qty) <= MIN_POSITION_QTY;
}

function normalizeSymbol(rawSymbol) {
  if (!rawSymbol) return rawSymbol;
  return normalizePair(rawSymbol);
}

function logQuoteTimestampDebug({ symbol, rawTs, source }) {
  if (!DEBUG_QUOTE_TS) return;
  const key = `${symbol}:${source || 'unknown'}`;
  if (quoteTsDebugLogged.has(key)) return;
  quoteTsDebugLogged.add(key);
  console.warn('quote_ts_debug', {
    symbol,
    source: source || null,
    rawTs,
  });
}

function formatLoggedAgeSeconds(ageMs) {
  if (!Number.isFinite(ageMs)) return null;
  return Math.min(Math.round(ageMs / 1000), MAX_LOGGED_QUOTE_AGE_SECONDS);
}

function buildUrlWithParams(baseUrl, params = {}) {
  const searchParams = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value == null) return;
    searchParams.append(key, String(value));
  });
  const query = searchParams.toString();
  return query ? `${baseUrl}?${query}` : baseUrl;
}

function buildClientOrderId(symbol, purpose) {
  const normalized = canonicalAsset(symbol) || 'UNKNOWN';
  return `${normalized}-${purpose}-${randomUUID()}`;
}

function getOrderIntentBucket() {
  const ttl = Number.isFinite(ORDER_TTL_MS) && ORDER_TTL_MS > 0 ? ORDER_TTL_MS : 45000;
  return Math.floor(Date.now() / ttl);
}

function buildIntentClientOrderId({ symbol, side, intent, ref }) {
  const normalized = canonicalAsset(symbol) || 'UNKNOWN';
  const safeSide = String(side || '').toUpperCase();
  const safeIntent = String(intent || '').toUpperCase();
  const suffix = ref || getOrderIntentBucket();
  return `BOT:${normalized}:${safeSide}:${safeIntent}:${suffix}`;
}

function buildEntryClientOrderId(symbol) {
  return buildIntentClientOrderId({ symbol, side: 'BUY', intent: 'ENTRY' });
}

function buildExitClientOrderId(symbol) {
  return buildIntentClientOrderId({ symbol, side: 'SELL', intent: 'EXIT' });
}

function buildTpClientOrderId(symbol, ref) {
  return buildIntentClientOrderId({ symbol, side: 'SELL', intent: 'TP', ref });
}

function buildIntentPrefix({ symbol, side, intent }) {
  const normalized = canonicalAsset(symbol) || 'UNKNOWN';
  const safeSide = String(side || '').toUpperCase();
  const safeIntent = String(intent || '').toUpperCase();
  return `BOT:${normalized}:${safeSide}:${safeIntent}`;
}

function getOrderAgeMs(order) {
  const rawTs = order?.submitted_at || order?.submittedAt || order?.created_at || order?.createdAt;
  if (!rawTs) return null;
  const tsMs = Date.parse(rawTs);
  return Number.isFinite(tsMs) ? Date.now() - tsMs : null;
}

function hasOpenOrderForIntent(openOrders, { symbol, side, intent }) {
  const prefix = buildIntentPrefix({ symbol, side, intent });
  return (Array.isArray(openOrders) ? openOrders : []).some((order) => {
    const orderSymbol = normalizePair(order.symbol || order.rawSymbol);
    const orderSide = String(order.side || '').toUpperCase();
    const clientOrderId = String(order.client_order_id || order.clientOrderId || '');
    return orderSymbol === normalizePair(symbol) && orderSide === String(side || '').toUpperCase() && clientOrderId.startsWith(prefix);
  });
}

function shouldReplaceOrder({ side, currentPrice, nextPrice }) {
  const cur = Number(currentPrice);
  const next = Number(nextPrice);
  if (!Number.isFinite(cur) || !Number.isFinite(next) || cur <= 0) return false;
  const deltaBps = ((next - cur) / cur) * 10000;
  if (String(side || '').toLowerCase() === 'buy') {
    return -deltaBps >= REPLACE_THRESHOLD_BPS;
  }
  return deltaBps >= REPLACE_THRESHOLD_BPS;
}

function logBuyDecision(symbol, computedNotionalUsd, decision) {

  console.log('buy_gate', {

    symbol,

    computedNotionalUsd,

    minOrderNotionalUsd: MIN_ORDER_NOTIONAL_USD,

    decision,

  });

}

function logExitDecision({
  symbol,
  heldSeconds,
  entryPrice,
  targetPrice,
  bid,
  ask,
  minNetProfitBps,
  actionTaken,
}) {
  console.log('exit_state', {
    symbol,
    heldSeconds,
    entryPrice,
    targetPrice,
    bid,
    ask,
    minNetProfitBps,
    actionTaken,
  });
}

function logExitRepairDecision({
  symbol,
  qty,
  avgEntryPrice,
  costBasis,
  bid,
  ask,
  targetPrice,
  timeInForce,
  orderType,
  hasOpenSell,
  gates,
  decision,
}) {
  console.log('exit_repair_decision', {
    symbol,
    qty,
    avgEntryPrice,
    costBasis,
    bid,
    ask,
    targetPrice,
    timeInForce,
    orderType,
    hasOpenSell,
    gates,
    decision,
  });
}

function readEnvFlag(name, defaultValue = true) {
  const raw = process.env[name];
  if (raw == null || raw === '') return defaultValue;
  return String(raw).toLowerCase() === 'true';
}

function updateInventoryFromBuy(symbol, qty, price) {

  const normalizedSymbol = normalizeSymbol(symbol);

  const qtyNum = Number(qty);

  const priceNum = Number(price);

  if (!Number.isFinite(qtyNum) || !Number.isFinite(priceNum) || qtyNum <= 0 || priceNum <= 0) {

    return;

  }

  const current = inventoryState.get(normalizedSymbol) || { qty: 0, costBasis: 0, avgPrice: 0 };

  const newQty = current.qty + qtyNum;

  const newCost = current.costBasis + qtyNum * priceNum;

  const avgPrice = newQty > 0 ? newCost / newQty : 0;

  inventoryState.set(normalizedSymbol, { qty: newQty, costBasis: newCost, avgPrice });

}

async function initializeInventoryFromPositions() {
  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'positions', label: 'positions_init' });
  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ label: 'positions', url, error: err });
    throw err;
  }

  const positions = Array.isArray(res) ? res : [];

  inventoryState.clear();

  for (const pos of positions) {

    const symbol = normalizeSymbol(pos.symbol);

    const qty = Number(pos.qty ?? pos.quantity ?? 0);

    const avgPrice = Number(pos.avg_entry_price ?? pos.avgEntryPrice ?? 0);

    if (!Number.isFinite(qty) || !Number.isFinite(avgPrice) || qty <= 0 || avgPrice <= 0) {

      continue;

    }

    if (isDustQty(qty)) {

      continue;

    }

    inventoryState.set(symbol, { qty, costBasis: qty * avgPrice, avgPrice });

  }

  return inventoryState;

}

async function fetchRecentCfeeEntries(limit = 25) {

  const now = Date.now();

  if (now - cfeeCache.ts < 60000 && cfeeCache.items.length) {

    return cfeeCache.items;

  }

  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'account/activities',
    params: {
      activity_types: 'CFEE',
      direction: 'desc',
      page_size: String(limit),
    },
    label: 'account_activities_cfee',
  });
  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ label: 'account', url, error: err });
    throw err;
  }

  const items = Array.isArray(res)
    ? res.map((entry) => ({
      ...entry,
      symbol: normalizeSymbol(entry.symbol),
    }))
    : [];

  cfeeCache.ts = now;

  cfeeCache.items = items;

  return items;

}

function parseCashFlowUsd(entry) {

  const raw =

    entry.cashflow_usd ??

    entry.cashflowUSD ??

    entry.cash_flow_usd ??

    entry.cash_flow ??

    entry.net_amount ??

    entry.amount;

  const val = Number(raw);

  return Number.isFinite(val) ? val : null;

}

async function feeAwareMinProfitBps(symbol, notionalUsd) {

  const normalizedSymbol = normalizeSymbol(symbol);

  if (!Number.isFinite(notionalUsd) || notionalUsd <= 0) {

    return USER_MIN_PROFIT_BPS;

  }

  let feeUsd = 0;

  let entries = [];

  try {

    entries = await fetchRecentCfeeEntries();

  } catch (err) {

    console.warn('CFEE fetch failed, falling back to user min profit', err?.message || err);

  }

  for (const entry of entries) {

    const cashFlowUsd = parseCashFlowUsd(entry);

    if (cashFlowUsd != null && cashFlowUsd < 0) {

      feeUsd += Math.abs(cashFlowUsd);

    }

    const qty = Number(entry.qty ?? entry.quantity ?? 0);

    const price = Number(entry.price ?? entry.fill_price ?? 0);

    if (Number.isFinite(qty) && Number.isFinite(price) && qty < 0 && price > 0) {

      feeUsd += Math.abs(qty) * price;

    }

  }

  const feeBps = (feeUsd / notionalUsd) * 10000;

  const feeFloor = feeBps + SLIPPAGE_BPS + BUFFER_BPS;

  const minBps = Math.max(USER_MIN_PROFIT_BPS, feeFloor);

  console.log('feeAwareMinProfitBps', {
    symbol: normalizedSymbol,
    notionalUsd,
    feeUsd,
    feeBps,
    minBps,
  });

  return minBps;

}

 

// Places a limit buy order first, then a limit sell after the buy is filled.

async function placeLimitBuyThenSell(symbol, qty, limitPrice) {

  const normalizedSymbol = normalizeSymbol(symbol);
  const openOrders = await fetchOrders({ status: 'open' });
  if (hasOpenOrderForIntent(openOrders, { symbol: normalizedSymbol, side: 'BUY', intent: 'ENTRY' })) {
    console.log('hold_existing_order', { symbol: normalizedSymbol, side: 'buy', reason: 'existing_entry_intent' });
    return { skipped: true, reason: 'existing_entry_intent' };
  }
  let bid = null;
  let ask = null;
  let spreadBps = null;
  try {
    const quote = await getLatestQuote(normalizedSymbol);
    bid = quote.bid;
    ask = quote.ask;
    if (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0) {
      spreadBps = ((ask - bid) / bid) * 10000;
    }
  } catch (err) {
    console.warn('entry_quote_failed', { symbol: normalizedSymbol, error: err?.message || err });
  }
  if (Number.isFinite(spreadBps) && spreadBps > MAX_SPREAD_BPS_TO_TRADE) {
    logSkip('spread_too_wide', { symbol: normalizedSymbol, bid, ask, spreadBps });
    return { skipped: true, reason: 'spread_too_wide', spreadBps };
  }

  const intendedNotional = Number(qty) * Number(limitPrice);

  const decision = Number.isFinite(intendedNotional) && intendedNotional >= MIN_ORDER_NOTIONAL_USD ? 'BUY' : 'SKIP';

  logBuyDecision(normalizedSymbol, intendedNotional, decision);

  if (decision === 'SKIP') {

    logSkip('notional_too_small', {

      symbol: normalizedSymbol,

      intendedNotional,

      minNotionalUsd: MIN_ORDER_NOTIONAL_USD,

    });

    return { skipped: true, reason: 'notional_too_small', notionalUsd: intendedNotional };

  }

  const sizeGuard = guardTradeSize({
    symbol: normalizedSymbol,
    qty,
    notional: intendedNotional,
    price: Number(limitPrice),
    side: 'buy',
    context: 'limit_buy',
  });
  if (sizeGuard.skip) {
    return { skipped: true, reason: 'below_min_trade', notionalUsd: sizeGuard.notional };
  }
  const finalQty = sizeGuard.qty ?? adjustedQty;

  // submit the limit buy order

  const buyOrderUrl = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'orders',
    label: 'orders_limit_buy',
  });
  const buyPayload = {
    symbol: toTradeSymbol(normalizedSymbol),
    qty: finalQty,
    side: 'buy',
    type: 'limit',
    // crypto orders must be GTC
    time_in_force: 'gtc',
    limit_price: limitPrice,
    client_order_id: buildEntryClientOrderId(normalizedSymbol),
  };
  const buyOrder = await placeOrderUnified({
    symbol: normalizedSymbol,
    url: buyOrderUrl,
    payload: buyPayload,
    label: 'orders_limit_buy',
    reason: 'limit_buy',
    context: 'limit_buy',
  });

 

  // poll until the order is filled

  let filledOrder = buyOrder;

  for (let i = 0; i < 20; i++) {

    const checkUrl = buildAlpacaUrl({
      baseUrl: ALPACA_BASE_URL,
      path: `orders/${buyOrder.id}`,
      label: 'orders_limit_buy_check',
    });
    let check;
    try {
      check = await requestJson({
        method: 'GET',
        url: checkUrl,
        headers: alpacaHeaders(),
      });
    } catch (err) {
      logHttpError({
        symbol: normalizedSymbol,
        label: 'orders',
        url: checkUrl,
        error: err,
      });
      throw err;
    }

    filledOrder = check;

    if (filledOrder.status === 'filled') break;

    await sleep(3000);

  }

 

  if (filledOrder.status !== 'filled') {

    throw new Error('Buy order not filled in time');

  }

 

  const avgPrice = parseFloat(filledOrder.filled_avg_price);

  updateInventoryFromBuy(normalizedSymbol, filledOrder.filled_qty, avgPrice);

  const inventory = inventoryState.get(normalizedSymbol);

  if (!inventory || inventory.qty <= 0) {

    logSkip('no_inventory_for_sell', { symbol: normalizedSymbol, qty: filledOrder.filled_qty });

    return { buy: filledOrder, sell: null, sellError: 'No inventory to sell' };

  }

  const sellOrder = await handleBuyFill({

    symbol: normalizedSymbol,

    qty: filledOrder.filled_qty,

    entryPrice: avgPrice,
    entryOrderId: filledOrder.id || buyOrder?.id,

  });

  return { buy: filledOrder, sell: sellOrder };

}

 

// Fetch latest trade price for a symbol

function isCryptoSymbol(symbol) {
  return Boolean(symbol && normalizePair(symbol).endsWith('/USD'));
}

async function getLatestPrice(symbol) {

  if (isCryptoSymbol(symbol)) {
    const dataSymbol = toDataSymbol(symbol);
    const url = buildAlpacaUrl({
      baseUrl: CRYPTO_DATA_URL,
      path: 'us/latest/trades',
      params: { symbols: dataSymbol },
      label: 'crypto_latest_trades',
    });
    let res;
    try {
      res = await requestMarketDataJson({ type: 'TRADE', url, symbol });
    } catch (err) {
      logHttpError({ symbol, label: 'trades', url, error: err });
      logSkip('no_quote', { symbol, reason: err?.errorCode === 'COOLDOWN' ? 'cooldown' : 'request_failed' });
      throw err;
    }

    const trade = res.trades && res.trades[dataSymbol];

    if (!trade) {
      markMarketDataFailure(null);
      logSkip('no_quote', { symbol, reason: 'no_data' });
      throw new Error(`Price not available for ${symbol}`);
    }

    return parseFloat(trade.p);

  }

  const url = buildAlpacaUrl({
    baseUrl: STOCKS_DATA_URL,
    path: 'trades/latest',
    params: { symbols: symbol },
    label: 'stock_latest_trades',
  });
  let res;
  try {
    res = await requestMarketDataJson({ type: 'TRADE', url, symbol });
  } catch (err) {
    logHttpError({ symbol, label: 'trades', url, error: err });
    logSkip('no_quote', { symbol, reason: err?.errorCode === 'COOLDOWN' ? 'cooldown' : 'request_failed' });
    throw err;
  }

  const trade = res.trades && res.trades[symbol];

  if (!trade) {
    markMarketDataFailure(null);
    logSkip('no_quote', { symbol, reason: 'no_data' });
    throw new Error(`Price not available for ${symbol}`);
  }

  return parseFloat(trade.p ?? trade.price);

}

 

// Get portfolio value and buying power from the Alpaca account

async function getAccountInfo() {
  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'account', label: 'account' });
  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ label: 'account', url, error: err });
    throw err;
  }

  const portfolioValue = parseFloat(res.portfolio_value);

  const buyingPower = parseFloat(res.buying_power);

  return {

    portfolioValue: isNaN(portfolioValue) ? 0 : portfolioValue,

    buyingPower: isNaN(buyingPower) ? 0 : buyingPower,

  };

}

async function fetchAccount() {
  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'account', label: 'account_raw' });
  return requestJson({
    method: 'GET',
    url,
    headers: alpacaHeaders(),
  });
}

async function fetchPortfolioHistory(params = {}) {
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'account/portfolio/history',
    params,
    label: 'portfolio_history',
  });
  return requestJson({
    method: 'GET',
    url,
    headers: alpacaHeaders(),
  });
}

async function fetchActivities(params = {}) {
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'account/activities',
    params,
    label: 'account_activities',
  });
  const items = await requestJson({
    method: 'GET',
    url,
    headers: alpacaHeaders(),
  });
  return {
    items: Array.isArray(items) ? items : [],
    nextPageToken: null,
  };
}

async function fetchClock() {
  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'clock', label: 'market_clock' });
  return requestJson({
    method: 'GET',
    url,
    headers: alpacaHeaders(),
  });
}

async function fetchPositions() {
  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'positions', label: 'positions' });
  const res = await requestJson({
    method: 'GET',
    url,
    headers: alpacaHeaders(),
  });
  const positions = Array.isArray(res) ? res : [];
  const normalized = positions.map((pos) => ({
    ...pos,
    rawSymbol: pos.symbol,
    pairSymbol: normalizeSymbol(pos.symbol),
    symbol: normalizeSymbol(pos.symbol),
  }));
  updatePositionsSnapshot(normalized);
  return normalized;
}

async function fetchPosition(symbol) {
  const normalized = toTradeSymbol(symbol);
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: `positions/${encodeURIComponent(normalized)}`,
    label: 'positions_single',
  });
  try {
    return await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    const statusCode = err?.statusCode ?? err?.response?.status ?? null;
    const axiosData = err?.response?.data;
    const axiosSnippet =
      typeof axiosData === 'string'
        ? axiosData.slice(0, 200)
        : axiosData
          ? JSON.stringify(axiosData).slice(0, 200)
          : '';
    const snippet = err?.responseSnippet200 || err?.responseSnippet || axiosSnippet || '';
    if (statusCode === 404) {
      logPositionNoneOnce(symbol, statusCode);
      return null;
    }
    if (statusCode === 429) {
      logPositionError({
        symbol,
        statusCode,
        snippet,
        level: 'warn',
        extra: {
          rateLimit: err?.rateLimit ?? null,
        },
      });
      throw err;
    }
    if (statusCode === 401 || statusCode === 403) {
      logPositionError({ symbol, statusCode, snippet, level: 'error' });
      throw err;
    }
    if (Number.isFinite(statusCode) && statusCode >= 500) {
      logPositionError({ symbol, statusCode, snippet, level: 'error' });
      throw err;
    }
    logPositionError({ symbol, statusCode, snippet, level: 'error' });
    throw err;
  }
}

async function fetchAsset(symbol) {
  const normalized = toTradeSymbol(symbol);
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: `assets/${encodeURIComponent(normalized)}`,
    label: 'asset',
  });
  return requestJson({
    method: 'GET',
    url,
    headers: alpacaHeaders(),
  });
}

async function getAvailablePositionQty(symbol) {
  const normalized = normalizeSymbol(symbol);
  try {
    const snapshot = await fetchPositionsSnapshot();
    const pos = snapshot.mapBySymbol.get(normalized);
    if (!pos) {
      logPositionNoneOnce(normalized, 404);
      return 0;
    }
    const qty = Number(pos?.qty_available ?? pos?.available ?? pos?.qty ?? pos?.quantity ?? 0);
    return Number.isFinite(qty) ? qty : 0;
  } catch (err) {
    if (err?.statusCode === 404) {
      logPositionNoneOnce(normalized, 404);
      return 0;
    }
    throw err;
  }
}

 

// Round quantities to Alpaca's supported crypto precision

function roundQty(qty) {

  return parseFloat(Number(qty).toFixed(9));

}

function roundNotional(notional) {
  return parseFloat(Number(notional).toFixed(2));
}

function guardTradeSize({ symbol, qty, notional, price, side, context }) {
  const qtyNum = Number(qty);
  const notionalNum = Number(notional);
  const roundedQty = Number.isFinite(qtyNum) ? roundQty(qtyNum) : null;
  const roundedNotional = Number.isFinite(notionalNum) ? roundNotional(notionalNum) : null;
  const sideLower = String(side || '').toLowerCase();
  let computedNotional = roundedNotional;
  if (!Number.isFinite(computedNotional) && Number.isFinite(roundedQty) && Number.isFinite(price)) {
    computedNotional = roundNotional(roundedQty * price);
  }

  if (Number.isFinite(roundedQty) && roundedQty > 0 && roundedQty < MIN_TRADE_QTY) {
    if (sideLower === 'sell') {
      console.log(`${symbol} — Sell allowed despite below_min_order_size`, {
        qty: roundedQty,
        minQty: MIN_TRADE_QTY,
        context,
      });
    } else {
      logSkip('below_min_trade', {
        symbol,
        side,
        qty: roundedQty,
        minQty: MIN_TRADE_QTY,
        context,
      });
      return { skip: true, qty: roundedQty, notional: computedNotional };
    }
  }

  if (Number.isFinite(computedNotional) && computedNotional < MIN_ORDER_NOTIONAL_USD) {
    if (sideLower === 'sell') {
      console.log(`${symbol} — Sell allowed despite below_min_notional`, {
        notionalUsd: computedNotional,
        minNotionalUsd: MIN_ORDER_NOTIONAL_USD,
        context,
      });
    } else {
      logSkip('below_min_trade', {
        symbol,
        side,
        notionalUsd: computedNotional,
        minNotionalUsd: MIN_ORDER_NOTIONAL_USD,
        context,
      });
      return { skip: true, qty: roundedQty, notional: computedNotional };
    }
  }

  return { skip: false, qty: roundedQty ?? qty, notional: roundedNotional ?? notional, computedNotional };
}

 

// Round prices to two decimals

function roundPrice(price) {

  return parseFloat(Number(price).toFixed(2));

}

function getTickSize({ symbol, price }) {
  if (isCryptoSymbol(symbol)) {
    const priceNum = Number(price);
    if (Number.isFinite(priceNum) && priceNum < 1) {
      return 0.0001;
    }
    return 0.01;
  }
  return Number.isFinite(PRICE_TICK) && PRICE_TICK > 0 ? PRICE_TICK : 0.01;
}

function roundToTick(price, symbolOrTick = PRICE_TICK, direction = 'up') {
  if (!Number.isFinite(price)) return price;
  const tickSize =
    typeof symbolOrTick === 'number'
      ? (Number.isFinite(symbolOrTick) && symbolOrTick > 0 ? symbolOrTick : 0.01)
      : getTickSize({ symbol: symbolOrTick, price });
  if (!Number.isFinite(tickSize) || tickSize <= 0) return price;
  if (direction === 'down') {
    return Math.floor(price / tickSize) * tickSize;
  }
  return Math.ceil(price / tickSize) * tickSize;
}

function getFeeBps({ orderType, isMaker }) {
  const typeLower = String(orderType || '').toLowerCase();
  if (typeLower === 'market') {
    return FEE_BPS_TAKER;
  }
  return isMaker ? FEE_BPS_MAKER : FEE_BPS_TAKER;
}

function resolveRequiredExitBps({ desiredNetExitBps, feeBpsRoundTrip, profitBufferBps }) {
  const desired = Number(desiredNetExitBps);
  if (Number.isFinite(desired) && desired > 0) return desired;
  const feeBps = Number.isFinite(feeBpsRoundTrip) ? feeBpsRoundTrip : 0;
  const bufferBps = Number.isFinite(profitBufferBps) ? profitBufferBps : 0;
  return feeBps + bufferBps;
}

function computeMinNetProfitBps({ feeBpsRoundTrip, profitBufferBps, desiredNetExitBps }) {
  return resolveRequiredExitBps({ desiredNetExitBps, feeBpsRoundTrip, profitBufferBps });
}

// requiredExitBps is the total net move (fees + buffer or desiredNetExitBps) above entry.
function computeTargetSellPrice(entryPrice, requiredExitBps, tickSize) {
  const minBps = Number.isFinite(requiredExitBps) ? requiredExitBps : 0;
  const rawTarget = Number(entryPrice) * (1 + minBps / 10000);
  if (!Number.isFinite(tickSize) || tickSize <= 0) {
    return rawTarget;
  }
  return Math.ceil(rawTarget / tickSize) * tickSize;
}

function computeBreakevenPrice(entryPrice, minNetProfitBps) {
  return Number(entryPrice) * (1 + Number(minNetProfitBps) / 10000);
}

function normalizeOrderType(orderType) {
  return String(orderType || '').toLowerCase();
}

function inferEntryFeeBps({ symbol, orderType, postOnly }) {
  const typeLower = normalizeOrderType(orderType);
  const isMarket = typeLower === 'market';
  if (isMarket) return FEE_BPS_TAKER;
  if (isCryptoSymbol(symbol) && typeLower === 'limit' && postOnly === false) {
    return FEE_BPS_TAKER;
  }
  return FEE_BPS_MAKER;
}

function inferExitFeeBps({ takerExitOnTouch }) {
  return takerExitOnTouch ? FEE_BPS_TAKER : FEE_BPS_MAKER;
}

function computeExitFloorBps({ exitFeeBps }) {
  const entryFeeBps = FEE_BPS_MAKER;
  const exitFee = Number.isFinite(exitFeeBps) ? exitFeeBps : FEE_BPS_MAKER;
  return entryFeeBps + exitFee;
}

function normalizeOrderLimitPrice(order) {
  const raw = order?.limit_price ?? order?.limitPrice ?? order?.price;
  const num = Number(raw);
  return Number.isFinite(num) ? num : null;
}

function normalizeOrderQty(order) {
  const raw = order?.qty ?? order?.quantity ?? order?.qty_available ?? order?.remaining_qty ?? order?.remainingQty;
  const num = Number(raw);
  return Number.isFinite(num) ? num : null;
}

function hasExitIntentOrder(order, symbol) {
  const clientOrderId = String(order?.client_order_id ?? order?.clientOrderId ?? '');
  if (!clientOrderId) return false;
  const tpPrefix = buildIntentPrefix({ symbol, side: 'SELL', intent: 'TP' });
  const exitPrefix = buildIntentPrefix({ symbol, side: 'SELL', intent: 'EXIT' });
  return clientOrderId.startsWith(tpPrefix) || clientOrderId.startsWith(exitPrefix);
}

function normalizeFilledQty(order) {
  const raw = order?.filled_qty ?? order?.filledQty ?? order?.filled_quantity ?? order?.filledQuantity;
  const num = Number(raw);
  return Number.isFinite(num) ? num : 0;
}

function isProfitableExit(entryPrice, exitPrice, feeBpsRoundTrip, profitBufferBps) {
  const entry = Number(entryPrice);
  const exit = Number(exitPrice);
  if (!Number.isFinite(entry) || !Number.isFinite(exit) || entry <= 0) return false;
  const netBps = ((exit - entry) / entry) * 10000;
  return netBps >= computeMinNetProfitBps({ feeBpsRoundTrip, profitBufferBps });
}

async function fetchFallbackTradeQuote(symbol, nowMs) {
  const isCrypto = isCryptoSymbol(symbol);
  const dataSymbol = isCrypto ? toDataSymbol(symbol) : symbol;
  const url = isCrypto
    ? buildAlpacaUrl({
      baseUrl: CRYPTO_DATA_URL,
      path: 'us/latest/trades',
      params: { symbols: dataSymbol },
      label: 'crypto_latest_trades_fallback',
    })
    : buildAlpacaUrl({
      baseUrl: STOCKS_DATA_URL,
      path: 'trades/latest',
      params: { symbols: symbol },
      label: 'stock_latest_trades_fallback',
    });

  let res;
  try {
    res = await requestMarketDataJson({ type: 'TRADE', url, symbol });
  } catch (err) {
    logHttpError({ symbol, label: 'trades_fallback', url, error: err });
    return null;
  }

  const tradeKey = isCrypto ? dataSymbol : symbol;
  const trade = res.trades && res.trades[tradeKey];
  if (!trade) {
    return null;
  }

  const price = Number(trade.p ?? trade.price);
  const rawTs = trade.t ?? trade.timestamp ?? trade.time ?? trade.ts;
  logQuoteTimestampDebug({ symbol, rawTs, source: 'trade_fallback' });
  const tsMs = normalizeQuoteTsMs(rawTs);
  if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(tsMs)) {
    return null;
  }

  const rawAgeMs = computeQuoteAgeMs({ nowMs, tsMs });
  const ageMs = normalizeQuoteAgeMs(rawAgeMs);
  if (Number.isFinite(rawAgeMs)) {
    logQuoteAgeWarning({ symbol, ageMs: rawAgeMs, source: 'trade_fallback', tsMs });
  }
  if (Number.isFinite(rawAgeMs) && !Number.isFinite(ageMs)) {
    logSkip('stale_quote', { symbol, ageSeconds: formatLoggedAgeSeconds(rawAgeMs) });
    return null;
  }
  if (isStaleQuoteAge(ageMs)) {
    logSkip('stale_quote', { symbol, ageSeconds: formatLoggedAgeSeconds(ageMs) });
    return null;
  }

  return {
    bid: price,
    ask: price,
    mid: price,
    tsMs,
    receivedAtMs: nowMs,
    source: 'trade_fallback',
  };
}

async function getLatestQuote(rawSymbol) {

  const symbol = normalizeSymbol(rawSymbol);

  const nowMs = Date.now();
  const cached = quoteCache.get(symbol);
  const cachedTsMs = cached && Number.isFinite(cached.tsMs) ? cached.tsMs : null;
  const cachedAgeMsRaw = Number.isFinite(cachedTsMs)
    ? computeQuoteAgeMs({ nowMs, tsMs: cachedTsMs })
    : null;
  const cachedAgeMs = normalizeQuoteAgeMs(cachedAgeMsRaw);
  if (Number.isFinite(cachedAgeMsRaw)) {
    logQuoteAgeWarning({ symbol, ageMs: cachedAgeMsRaw, source: cached?.source || 'cache', tsMs: cachedTsMs });
  }
  if (Number.isFinite(cachedAgeMs) && cachedAgeMs <= QUOTE_CACHE_MAX_AGE_MS) {
    recordLastQuoteAt(symbol, { tsMs: cachedTsMs, source: 'cache' });
    return {
      bid: cached.bid,
      ask: cached.ask,
      tsMs: cachedTsMs,
    };
  }

  if (cached) {
    quoteCache.delete(symbol);
  }

  const isCrypto = isCryptoSymbol(symbol);
  const dataSymbol = isCrypto ? toDataSymbol(symbol) : symbol;
  const url = isCrypto
    ? buildAlpacaUrl({
      baseUrl: CRYPTO_DATA_URL,
      path: 'us/latest/quotes',
      params: { symbols: dataSymbol },
      label: 'crypto_latest_quotes',
    })
    : buildAlpacaUrl({
      baseUrl: STOCKS_DATA_URL,
      path: 'quotes/latest',
      params: { symbols: symbol },
      label: 'stock_latest_quotes',
    });

  let res;
  let primaryError = null;
  try {
    res = await requestMarketDataJson({ type: 'QUOTE', url, symbol });
  } catch (err) {
    primaryError = err;
    logHttpError({ symbol, label: 'quotes', url, error: err });
  }

  const tryFallbackTradeQuote = async () => {
    const fallback = await fetchFallbackTradeQuote(symbol, nowMs);
    if (!fallback) return null;
    quoteCache.set(symbol, fallback);
    recordLastQuoteAt(symbol, { tsMs: fallback.tsMs, source: fallback.source });
    return {
      bid: fallback.bid,
      ask: fallback.ask,
      tsMs: fallback.tsMs,
    };
  };

  if (primaryError) {
    const fallbackQuote = await tryFallbackTradeQuote();
    if (fallbackQuote) {
      return fallbackQuote;
    }
    if (primaryError?.errorCode === 'COOLDOWN') {
      logSkip('no_quote', { symbol, reason: 'cooldown' });
    } else {
      logSkip('no_quote', { symbol, reason: 'request_failed' });
    }
    recordLastQuoteAt(symbol, { tsMs: cachedTsMs, source: 'error', reason: 'request_failed' });
    throw primaryError;
  }

  const quoteKey = isCrypto ? dataSymbol : symbol;
  const quote = res.quotes && res.quotes[quoteKey];
  if (!quote) {
    const fallbackQuote = await tryFallbackTradeQuote();
    if (fallbackQuote) {
      return fallbackQuote;
    }
    const reason = cached ? 'stale_cache' : 'no_data';
    if (cached && Number.isFinite(cachedTsMs)) {
      const lastSeenAge = Number.isFinite(cachedAgeMs)
        ? cachedAgeMs
        : cachedAgeMsRaw;
      logSkip('stale_quote', { symbol, lastSeenAgeSeconds: formatLoggedAgeSeconds(lastSeenAge) });
    } else {
      logSkip('no_quote', { symbol, reason });
    }
    markMarketDataFailure(null);
    recordLastQuoteAt(symbol, {
      tsMs: cached ? cachedTsMs : null,
      source: cached ? 'stale' : 'error',
      reason,
    });
    throw new Error(`Quote not available for ${symbol}`);
  }

  const tsMs = parseQuoteTimestamp({ quote, symbol, source: 'alpaca_quote' });
  const bid = Number(quote.bp ?? quote.bid_price ?? quote.bid);
  const ask = Number(quote.ap ?? quote.ask_price ?? quote.ask);
  const normalizedBid = Number.isFinite(bid) ? bid : null;
  const normalizedAsk = Number.isFinite(ask) ? ask : null;
  const rawAgeMs = Number.isFinite(tsMs) ? computeQuoteAgeMs({ nowMs, tsMs }) : null;
  const ageMs = normalizeQuoteAgeMs(rawAgeMs);
  if (Number.isFinite(rawAgeMs)) {
    logQuoteAgeWarning({ symbol, ageMs: rawAgeMs, source: 'alpaca', tsMs });
  }

  if (!Number.isFinite(normalizedBid) || !Number.isFinite(normalizedAsk) || normalizedBid <= 0 || normalizedAsk <= 0) {
    const fallbackQuote = await tryFallbackTradeQuote();
    if (fallbackQuote) {
      return fallbackQuote;
    }
    if (cached && Number.isFinite(cachedTsMs)) {
      const lastSeenAge = Number.isFinite(cachedAgeMs)
        ? cachedAgeMs
        : cachedAgeMsRaw;
      logSkip('stale_quote', { symbol, lastSeenAgeSeconds: formatLoggedAgeSeconds(lastSeenAge) });
    } else {
      logSkip('no_quote', { symbol, reason: 'invalid_bid_ask' });
    }
    recordLastQuoteAt(symbol, { tsMs: Number.isFinite(tsMs) ? tsMs : null, source: 'error', reason: 'invalid_bid_ask' });
    throw new Error(`Quote bid/ask missing for ${symbol}`);
  }

  if (!Number.isFinite(tsMs)) {
    const fallbackQuote = await tryFallbackTradeQuote();
    if (fallbackQuote) {
      return fallbackQuote;
    }
    recordLastQuoteAt(symbol, { tsMs: null, source: 'error', reason: 'missing_timestamp' });
    logSkip('no_quote', { symbol, reason: 'missing_timestamp' });
    throw new Error(`Quote timestamp missing for ${symbol}`);
  }

  if (Number.isFinite(rawAgeMs) && !Number.isFinite(ageMs)) {
    const fallbackQuote = await tryFallbackTradeQuote();
    if (fallbackQuote) {
      return fallbackQuote;
    }
    logSkip('stale_quote', { symbol, ageSeconds: formatLoggedAgeSeconds(rawAgeMs) });
    recordLastQuoteAt(symbol, { tsMs: null, source: 'stale', reason: 'absurd_age' });
    throw new Error(`Quote age absurd for ${symbol}`);
  }

  if (isStaleQuoteAge(ageMs)) {
    const fallbackQuote = await tryFallbackTradeQuote();
    if (fallbackQuote) {
      return fallbackQuote;
    }
    logSkip('stale_quote', { symbol, ageSeconds: formatLoggedAgeSeconds(ageMs) });
    recordLastQuoteAt(symbol, { tsMs, source: 'stale', reason: 'stale_quote' });
    throw new Error(`Quote stale for ${symbol}`);
  }

  const normalizedQuote = {
    bid: normalizedBid,
    ask: normalizedAsk,
    mid: (normalizedBid + normalizedAsk) / 2,
    tsMs,
    receivedAtMs: nowMs,
    source: 'alpaca',
  };
  quoteCache.set(symbol, normalizedQuote);
  recordLastQuoteAt(symbol, { tsMs, source: 'fresh' });
  return normalizedQuote;

}

function normalizeSymbolsParam(rawSymbols) {
  if (!rawSymbols) return [];
  if (Array.isArray(rawSymbols)) return rawSymbols.map((s) => String(s).trim()).filter(Boolean);
  return String(rawSymbols)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}

async function fetchCryptoQuotes({ symbols, location = 'us' }) {
  const dataSymbols = symbols.map((s) => toDataSymbol(s));
  const url = buildAlpacaUrl({
    baseUrl: CRYPTO_DATA_URL,
    path: `${location}/latest/quotes`,
    params: { symbols: dataSymbols.join(',') },
    label: 'crypto_latest_quotes_batch',
  });
  return requestMarketDataJson({ type: 'QUOTE', url, symbol: dataSymbols.join(',') });
}

async function fetchCryptoTrades({ symbols, location = 'us' }) {
  const dataSymbols = symbols.map((s) => toDataSymbol(s));
  const url = buildAlpacaUrl({
    baseUrl: CRYPTO_DATA_URL,
    path: `${location}/latest/trades`,
    params: { symbols: dataSymbols.join(',') },
    label: 'crypto_latest_trades_batch',
  });
  return requestMarketDataJson({ type: 'TRADE', url, symbol: dataSymbols.join(',') });
}

async function fetchCryptoBars({ symbols, location = 'us', limit = 6, timeframe = '1Min' }) {
  const dataSymbols = symbols.map((s) => toDataSymbol(s));
  const url = buildAlpacaUrl({
    baseUrl: CRYPTO_DATA_URL,
    path: `${location}/bars`,
    params: { symbols: dataSymbols.join(','), limit: String(limit), timeframe },
    label: 'crypto_bars_batch',
  });
  return requestMarketDataJson({ type: 'BARS', url, symbol: dataSymbols.join(',') });
}

async function fetchStockQuotes({ symbols }) {
  const url = buildAlpacaUrl({
    baseUrl: STOCKS_DATA_URL,
    path: 'quotes/latest',
    params: { symbols: symbols.join(',') },
    label: 'stocks_latest_quotes_batch',
  });
  return requestMarketDataJson({ type: 'QUOTE', url, symbol: symbols.join(',') });
}

async function fetchStockTrades({ symbols }) {
  const url = buildAlpacaUrl({
    baseUrl: STOCKS_DATA_URL,
    path: 'trades/latest',
    params: { symbols: symbols.join(',') },
    label: 'stocks_latest_trades_batch',
  });
  return requestMarketDataJson({ type: 'TRADE', url, symbol: symbols.join(',') });
}

async function fetchStockBars({ symbols, limit = 6, timeframe = '1Min' }) {
  const url = buildAlpacaUrl({
    baseUrl: STOCKS_DATA_URL,
    path: 'bars',
    params: { symbols: symbols.join(','), limit: String(limit), timeframe },
    label: 'stocks_bars_batch',
  });
  return requestMarketDataJson({ type: 'BARS', url, symbol: symbols.join(',') });
}

async function fetchOrderById(orderId) {
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: `orders/${orderId}`,
    label: 'orders_get',
  });
  let response;
  try {
    response = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ label: 'orders', url, error: err });
    throw err;
  }

  return response;

}

async function cancelOrderSafe(orderId) {

  try {

    await cancelOrder(orderId);

    return true;

  } catch (err) {

    console.warn('cancel_order_failed', {
      orderId,
      error: err?.responseSnippet200 || err?.errorMessage || err.message,
    });

    return false;

  }

}

async function submitLimitSell({

  symbol,

  qty,

  limitPrice,

  reason,
  intentRef,
  openOrders,
  postOnly = true,

}) {

  const open = openOrders || (await fetchOrders({ status: 'open' }));
  const availableQty = await getAvailablePositionQty(symbol);
  if (!(availableQty > 0)) {
    logSkip('no_position_qty', { symbol, qty, availableQty, context: 'limit_sell' });
    return { skipped: true, reason: 'no_position_qty' };
  }
  const qtyNum = Number(qty);
  const adjustedQty = Number.isFinite(qtyNum) && qtyNum > 0 ? Math.min(qtyNum, availableQty) : availableQty;

  const roundedLimit = roundToTick(Number(limitPrice), symbol, 'up');
  const openList = Array.isArray(open) ? open : [];
  const normalizedSymbol = normalizePair(symbol);
  const requiredQty = Number.isFinite(adjustedQty) && adjustedQty > 0 ? adjustedQty : availableQty;
  const openSellCandidates = openList.filter((order) => {
    const orderSymbol = normalizePair(order.symbol || order.rawSymbol);
    const side = String(order.side || '').toLowerCase();
    if (orderSymbol !== normalizedSymbol || side !== 'sell') return false;
    const orderQty = normalizeOrderQty(order);
    return Number.isFinite(orderQty) && orderQty >= requiredQty;
  });
  if (openSellCandidates.length) {
    const taggedCandidates = openSellCandidates.filter((order) => hasExitIntentOrder(order, normalizedSymbol));
    const desiredLimit = roundedLimit;
    const pool = taggedCandidates.length ? taggedCandidates : openSellCandidates;
    const bestOrder = pool.reduce((best, candidate) => {
      const bestPrice = normalizeOrderLimitPrice(best);
      const candidatePrice = normalizeOrderLimitPrice(candidate);
      const bestDiff = Number.isFinite(bestPrice) ? Math.abs(bestPrice - desiredLimit) : Number.POSITIVE_INFINITY;
      const candidateDiff = Number.isFinite(candidatePrice)
        ? Math.abs(candidatePrice - desiredLimit)
        : Number.POSITIVE_INFINITY;
      return candidateDiff < bestDiff ? candidate : best;
    }, pool[0]);
    const adoptedId = bestOrder?.id || bestOrder?.order_id || null;
    const adoptedLimit = normalizeOrderLimitPrice(bestOrder);
    console.log('adopt_existing_sell', {
      symbol,
      orderId: adoptedId,
      limitPrice: adoptedLimit,
      matchedQty: requiredQty,
      intentTagged: hasExitIntentOrder(bestOrder, normalizedSymbol),
    });
    return {
      id: adoptedId,
      limitPrice: adoptedLimit,
      submittedAt: bestOrder?.submitted_at || bestOrder?.submittedAt || bestOrder?.created_at || bestOrder?.createdAt,
      adopted: true,
    };
  }

  const sizeGuard = guardTradeSize({
    symbol,
    qty: adjustedQty,
    price: roundedLimit,
    side: 'sell',
    context: 'limit_sell',
  });
  if (sizeGuard.skip) {
    return { skipped: true, reason: 'below_min_trade' };
  }
  const finalQty = sizeGuard.qty ?? qty;

  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'orders', label: 'orders_limit_sell' });
  const clientOrderId = buildTpClientOrderId(symbol, intentRef);
  const payload = {
    symbol: toTradeSymbol(symbol),
    qty: finalQty,
    side: 'sell',
    type: 'limit',
    time_in_force: 'gtc',
    limit_price: roundedLimit,
    client_order_id: clientOrderId,
  };
  if (postOnly && isCryptoSymbol(symbol)) {
    payload.post_only = true;
  }
  console.log('tp_sell_attempt', {
    symbol,
    qty: finalQty,
    limitPrice: roundedLimit,
    tif: payload.time_in_force,
    client_order_id: clientOrderId,
    sell_reason: 'TP_ATTACH',
  });
  let response;
  try {
    response = await placeOrderUnified({
      symbol,
      url,
      payload,
      label: 'orders_limit_sell',
      reason,
      context: 'limit_sell',
    });
  } catch (err) {
    const status = err?.statusCode ?? err?.response?.status ?? null;
    const body = err?.response?.data ?? err?.responseSnippet200 ?? err?.responseSnippet ?? err?.message ?? null;
    const bodyText = typeof body === 'string' ? body : JSON.stringify(body);
    console.error('tp_sell_error', { symbol, status, body: bodyText });
    throw err;
  }

  console.log('submit_limit_sell', { symbol, qty, limitPrice: roundedLimit, reason, orderId: response?.id });

  try {
    const openList = openOrders || open;
    const hasOpenSell = (Array.isArray(openList) ? openList : []).some((order) => {
      const orderSymbol = normalizePair(order.symbol);
      const side = String(order.side || '').toLowerCase();
      return orderSymbol === normalizePair(symbol) && side === 'sell';
    });
    if (!hasOpenSell) {
      console.warn('TP missing after buy', { symbol });
    }
  } catch (err) {
    console.warn('tp_open_check_failed', { symbol, error: err?.message || err });
  }

  return response;

}

async function submitMarketSell({

  symbol,

  qty,

  reason,

}) {

  const availableQty = await getAvailablePositionQty(symbol);
  if (!(availableQty > 0)) {
    logSkip('no_position_qty', { symbol, qty, availableQty, context: 'market_sell' });
    return { skipped: true, reason: 'no_position_qty' };
  }
  const qtyNum = Number(qty);
  const adjustedQty = Number.isFinite(qtyNum) && qtyNum > 0 ? Math.min(qtyNum, availableQty) : availableQty;

  const sizeGuard = guardTradeSize({
    symbol,
    qty: adjustedQty,
    side: 'sell',
    context: 'market_sell',
  });
  if (sizeGuard.skip) {
    return { skipped: true, reason: 'below_min_trade' };
  }
  const finalQty = sizeGuard.qty ?? adjustedQty;

  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'orders', label: 'orders_market_sell' });
  const payload = {
    symbol: toTradeSymbol(symbol),
    qty: finalQty,
    side: 'sell',
    type: 'market',
    time_in_force: 'gtc',
    client_order_id: buildExitClientOrderId(symbol),
  };
  const response = await placeOrderUnified({
    symbol,
    url,
    payload,
    label: 'orders_market_sell',
    reason,
    context: 'market_sell',
  });

  console.log('submit_market_sell', { symbol, qty, reason, exit_reason: reason, orderId: response?.id });

  return response;

}

async function submitIocLimitSell({
  symbol,
  qty,
  limitPrice,
  reason,
}) {
  const availableQty = await getAvailablePositionQty(symbol);
  if (!(availableQty > 0)) {
    logSkip('no_position_qty', { symbol, qty, availableQty, context: 'ioc_limit_sell' });
    return { skipped: true, reason: 'no_position_qty' };
  }
  const qtyNum = Number(qty);
  const adjustedQty = Number.isFinite(qtyNum) && qtyNum > 0 ? Math.min(qtyNum, availableQty) : availableQty;

  const roundedLimit = roundToTick(Number(limitPrice), symbol, 'down');
  const sizeGuard = guardTradeSize({
    symbol,
    qty: adjustedQty,
    price: roundedLimit,
    side: 'sell',
    context: 'ioc_limit_sell',
  });
  if (sizeGuard.skip) {
    return { skipped: true, reason: 'below_min_trade' };
  }
  const finalQty = sizeGuard.qty ?? adjustedQty;

  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'orders', label: 'orders_ioc_limit_sell' });
  const payload = {
    symbol: toTradeSymbol(symbol),
    qty: finalQty,
    side: 'sell',
    type: 'limit',
    time_in_force: 'ioc',
    limit_price: roundedLimit,
    client_order_id: buildExitClientOrderId(symbol),
  };
  const response = await placeOrderUnified({
    symbol,
    url,
    payload,
    label: 'orders_ioc_limit_sell',
    reason,
    context: 'ioc_limit_sell',
  });

  console.log('submit_ioc_limit_sell', { symbol, qty: finalQty, limitPrice: roundedLimit, reason, orderId: response?.id });

  return { order: response, requestedQty: finalQty };
}

async function handleBuyFill({

  symbol: rawSymbol,

  qty,

  entryPrice,
  entryOrderId,
  desiredNetExitBps,

}) {

  const symbol = normalizeSymbol(rawSymbol);

  const entryPriceNum = Number(entryPrice);

  const qtyNum = Number(qty);

  const notionalUsd = qtyNum * entryPriceNum;

  let entryOrderType = null;
  let entryPostOnly = null;
  if (entryOrderId) {
    try {
      const entryOrder = await fetchOrderById(entryOrderId);
      entryOrderType = entryOrder?.type ?? entryOrder?.order_type ?? null;
      entryPostOnly = entryOrder?.post_only ?? entryOrder?.postOnly ?? null;
    } catch (err) {
      console.warn('entry_order_fetch_failed', { symbol, entryOrderId, error: err?.message || err });
    }
  }
  const entryFeeBps = inferEntryFeeBps({
    symbol,
    orderType: entryOrderType,
    postOnly: entryPostOnly,
  });
  const exitFeeBps = inferExitFeeBps({ takerExitOnTouch: TAKER_EXIT_ON_TOUCH });
  const feeBpsRoundTrip = entryFeeBps + exitFeeBps;
  const desiredNetExitBpsValue = Number.isFinite(desiredNetExitBps)
    ? Number(desiredNetExitBps)
    : (Number.isFinite(desiredExitBpsBySymbol.get(symbol)) ? desiredExitBpsBySymbol.get(symbol) : null);
  if (desiredNetExitBpsValue != null) {
    desiredExitBpsBySymbol.delete(symbol);
  }
  const profitBufferBps = PROFIT_BUFFER_BPS;
  const minNetProfitBps = computeMinNetProfitBps({
    feeBpsRoundTrip,
    profitBufferBps,
    desiredNetExitBps: desiredNetExitBpsValue,
  });
  const tickSize = getTickSize({ symbol, price: entryPriceNum });
  const requiredExitBps = resolveRequiredExitBps({
    desiredNetExitBps: desiredNetExitBpsValue,
    feeBpsRoundTrip,
    profitBufferBps,
  });
  const targetPrice = computeTargetSellPrice(entryPriceNum, requiredExitBps, tickSize);
  const postOnly = true;

  console.log('tp_attach_plan', {
    symbol,
    entryPrice: entryPriceNum,
    entryFeeBps,
    exitFeeBps,
    feeBpsRoundTrip,
    desiredNetExitBps: desiredNetExitBpsValue,
    targetPrice,
    takerExitOnTouch: TAKER_EXIT_ON_TOUCH,
    postOnly,
  });

  const sellOrder = await submitLimitSell({

    symbol,

    qty: qtyNum,

    limitPrice: targetPrice,

    reason: 'initial_target',
    intentRef: entryOrderId || getOrderIntentBucket(),
    postOnly,

  });

  const now = Date.now();
  const sellOrderId = sellOrder?.id || sellOrder?.order_id || null;
  const sellOrderLimit = normalizeOrderLimitPrice(sellOrder) ?? targetPrice;
  const sellOrderSubmittedAtRaw = sellOrder?.submittedAt || sellOrder?.submitted_at || null;
  const sellOrderSubmittedAt =
    typeof sellOrderSubmittedAtRaw === 'string' ? Date.parse(sellOrderSubmittedAtRaw) : sellOrderSubmittedAtRaw;

  exitState.set(symbol, {

    symbol,

    qty: qtyNum,

    entryPrice: entryPriceNum,

    entryTime: now,

    notionalUsd,

    minNetProfitBps,

    targetPrice,
    feeBpsRoundTrip,
    profitBufferBps,
    desiredNetExitBps: desiredNetExitBpsValue,
    entryFeeBps,
    exitFeeBps,
    entryOrderId: entryOrderId || null,

    sellOrderId,

    sellOrderSubmittedAt: Number.isFinite(sellOrderSubmittedAt) ? sellOrderSubmittedAt : now,

    sellOrderLimit,

    takerAttempted: false,
  });

  logExitDecision({

    symbol,

    heldSeconds: 0,

    entryPrice: entryPriceNum,

    targetPrice,

    bid: null,

    ask: null,

    minNetProfitBps,

    actionTaken: 'placed_initial_limit_sell',

  });

  return sellOrder;

}

async function repairOrphanExits() {
  const autoTradeEnabled = readEnvFlag('AUTO_TRADE', true);
  const autoSellEnabled = readEnvFlag('AUTO_SELL', true);
  const exitsEnabled = readEnvFlag('EXITS_ENABLED', true);
  const liveMode = readEnvFlag('LIVE', readEnvFlag('LIVE_MODE', readEnvFlag('LIVE_TRADING', true)));
  const gateFlags = { autoTradeEnabled, autoSellEnabled, exitsEnabled, liveMode };
  let positions = [];
  let openOrders = [];

  try {
    [positions, openOrders] = await Promise.all([fetchPositions(), fetchOrders({ status: 'open' })]);
  } catch (err) {
    console.warn('exit_repair_fetch_failed', { error: err?.message || err });
    return { placed: 0, skipped: 0, failed: 0 };
  }

  const openSellsBySymbol = (Array.isArray(openOrders) ? openOrders : []).reduce((acc, order) => {
    const side = String(order.side || '').toLowerCase();
    const status = String(order.status || '').toLowerCase();
    if (side !== 'sell') {
      return acc;
    }
    if (['filled', 'canceled', 'expired', 'rejected'].includes(status)) {
      return acc;
    }
    const symbol = normalizeSymbol(order.symbol || order.rawSymbol);
    if (!acc.has(symbol)) {
      acc.set(symbol, []);
    }
    acc.get(symbol).push(order);
    return acc;
  }, new Map());
  const openSellSymbols = new Set(
    (Array.isArray(openOrders) ? openOrders : [])
      .filter((order) => String(order.side || '').toLowerCase() === 'sell')
      .map((order) => normalizeSymbol(order.symbol || order.rawSymbol))
  );
  const positionsBySymbol = new Map(
    (Array.isArray(positions) ? positions : []).map((pos) => [
      normalizeSymbol(pos.symbol || pos.rawSymbol),
      Number(pos.qty ?? pos.quantity ?? 0),
    ])
  );
  let placed = 0;
  let skipped = 0;
  let failed = 0;

  console.log('exit_repair_pass_start', {
    positions: Array.isArray(positions) ? positions.length : 0,
    openSell: openSellSymbols.size,
    openOrdersCount: Array.isArray(openOrders) ? openOrders.length : 0,
    openSellSample: Array.from(openSellSymbols).slice(0, 3),
  });

  for (const [symbol, sellOrders] of openSellsBySymbol.entries()) {
    const qty = positionsBySymbol.get(symbol);
    if (!Number.isFinite(qty) || qty <= 0 || isDustQty(qty)) {
      let canceled = 0;
      for (const order of sellOrders) {
        const orderId = order?.id || order?.order_id;
        if (orderId) {
          await cancelOrderSafe(orderId);
          canceled += 1;
        }
      }
      const hadTracked = exitState.has(symbol);
      exitState.delete(symbol);
      console.log('exit_orphan_cleanup', {
        symbol,
        canceled,
        clearedTracked: hadTracked,
      });
    }
  }

  for (const pos of positions) {
    const symbol = normalizeSymbol(pos.symbol);
    const qty = Number(pos.qty ?? pos.quantity ?? 0);
    const avgEntryPrice = Number(pos.avg_entry_price ?? pos.avgEntryPrice ?? 0);
    const costBasis = Number(pos.cost_basis ?? pos.costBasis ?? 0);
    const orderType = 'limit';
    const timeInForce = 'gtc';
    let bid = null;
    let ask = null;

    try {
      const quote = await getLatestQuote(symbol);
      bid = quote.bid;
      ask = quote.ask;
    } catch (err) {
      console.warn('exit_repair_quote_failed', { symbol, error: err?.message || err });
    }

    const hasOpenSell = openSellSymbols.has(symbol);
    const hasTrackedExit = exitState.has(symbol);
    let decision = 'SKIP:unknown';
    let targetPrice = null;

    if (!Number.isFinite(qty) || qty <= 0) {
      decision = 'SKIP:non_positive_qty';
      skipped += 1;
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
      continue;
    }

    if (isDustQty(qty)) {
      decision = 'SKIP:dust_qty';
      skipped += 1;
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
      continue;
    }

    if (hasTrackedExit) {
      if (hasOpenSell) {
        decision = 'OK:tracked_and_has_open_sell';
        skipped += 1;
        logExitRepairDecision({
          symbol,
          qty,
          avgEntryPrice,
          costBasis,
          bid,
          ask,
          targetPrice,
          timeInForce,
          orderType,
          hasOpenSell,
          gates: gateFlags,
          decision,
        });
        continue;
      }
      exitState.delete(symbol);
      decision = 'RESET:tracked_missing_open_sell';
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
    }

    if (hasOpenSell) {
      decision = 'SKIP:open_sell';
      skipped += 1;
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
      continue;
    }

    console.warn('exit_orphan_detected', { symbol, qty });

    if (!Number.isFinite(avgEntryPrice) || avgEntryPrice <= 0) {
      decision = 'SKIP:missing_cost_basis';
      skipped += 1;
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
      continue;
    }

    if (!autoTradeEnabled) {
      decision = 'SKIP:auto_trade_disabled';
      skipped += 1;
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
      continue;
    }

    if (!autoSellEnabled) {
      decision = 'SKIP:auto_sell_disabled';
      skipped += 1;
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
      continue;
    }

    if (!exitsEnabled) {
      decision = 'SKIP:exits_disabled';
      skipped += 1;
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
      continue;
    }

    if (!liveMode) {
      decision = 'SKIP:live_mode_disabled';
      skipped += 1;
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
      continue;
    }

    const notionalUsd = qty * avgEntryPrice;
    const entryFeeBps = inferEntryFeeBps({ symbol, orderType, postOnly: true });
    const exitFeeBps = inferExitFeeBps({ takerExitOnTouch: TAKER_EXIT_ON_TOUCH });
    const feeBpsRoundTrip = entryFeeBps + exitFeeBps;
    const exitFloorBps = computeExitFloorBps({ exitFeeBps });
    const slippageBps = Number.isFinite(SLIPPAGE_BPS) ? SLIPPAGE_BPS : null;
    const desiredNetExitBps = Number.isFinite(desiredExitBpsBySymbol.get(symbol))
      ? desiredExitBpsBySymbol.get(symbol)
      : Math.max(
        USER_MIN_PROFIT_BPS,
        Number.isFinite(exitFloorBps) ? exitFloorBps + (Number.isFinite(slippageBps) ? slippageBps : 0) : USER_MIN_PROFIT_BPS
      );
    const profitBufferBps = PROFIT_BUFFER_BPS;
    const minNetProfitBps = computeMinNetProfitBps({
      feeBpsRoundTrip,
      profitBufferBps,
      desiredNetExitBps,
    });
    const tickSize = getTickSize({ symbol, price: avgEntryPrice });
    const requiredExitBps = resolveRequiredExitBps({
      desiredNetExitBps,
      feeBpsRoundTrip,
      profitBufferBps,
    });
    targetPrice = computeTargetSellPrice(avgEntryPrice, requiredExitBps, tickSize);
    const postOnly = true;

    console.log('tp_attach_plan', {
      symbol,
      entryPrice: avgEntryPrice,
      entryFeeBps,
      exitFeeBps,
      feeBpsRoundTrip,
      desiredNetExitBps,
      targetPrice,
      takerExitOnTouch: TAKER_EXIT_ON_TOUCH,
      postOnly,
    });

    try {
      const repairOrder = await submitLimitSell({
        symbol,
        qty,
        limitPrice: targetPrice,
        reason: 'exit_repair_orphan',
        intentRef: getOrderIntentBucket(),
        openOrders,
        postOnly,
      });
      if (repairOrder?.skipped) {
        decision = `SKIP:${repairOrder.reason || 'submit_skipped'}`;
        skipped += 1;
        logExitRepairDecision({
          symbol,
          qty,
          avgEntryPrice,
          costBasis,
          bid,
          ask,
          targetPrice,
          timeInForce,
          orderType,
          hasOpenSell,
          gates: gateFlags,
          decision,
        });
        continue;
      }
      if (!repairOrder?.id) {
        decision = 'FAIL:invalid_order_response';
        failed += 1;
        logExitRepairDecision({
          symbol,
          qty,
          avgEntryPrice,
          costBasis,
          bid,
          ask,
          targetPrice,
          timeInForce,
          orderType,
          hasOpenSell,
          gates: gateFlags,
          decision,
        });
        continue;
      }
      const now = Date.now();
      exitState.set(symbol, {
        symbol,
        qty,
        entryPrice: avgEntryPrice,
        entryTime: now,
        notionalUsd,
        minNetProfitBps,
        targetPrice,
        feeBpsRoundTrip,
        profitBufferBps,
        desiredNetExitBps,
        entryFeeBps,
        exitFeeBps,
        sellOrderId: repairOrder.id,
        sellOrderSubmittedAt: now,
        sellOrderLimit: targetPrice,
        takerAttempted: false,
      });
      desiredExitBpsBySymbol.delete(symbol);
      placed += 1;
      decision = 'PLACE_EXIT';
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
    } catch (err) {
      failed += 1;
      decision = `FAIL:${err?.message || err}`;
      logExitRepairDecision({
        symbol,
        qty,
        avgEntryPrice,
        costBasis,
        bid,
        ask,
        targetPrice,
        timeInForce,
        orderType,
        hasOpenSell,
        gates: gateFlags,
        decision,
      });
    }
  }

  console.log('exit_repair_pass_done', { placed, skipped, failed });
  return { placed, skipped, failed };
}

async function manageExitStates() {

  if (exitManagerRunning) {
    console.warn('exit_manager_skip_concurrent');
    return;
  }
  exitManagerRunning = true;

  try {
    const now = Date.now();

    await repairOrphanExits();
    let openOrders = [];
    try {
      openOrders = await fetchOrders({ status: 'open' });
    } catch (err) {
      console.warn('exit_manager_open_orders_failed', { error: err?.message || err });
    }
    const openOrdersList = Array.isArray(openOrders) ? openOrders : [];
    const openOrdersBySymbol = openOrdersList.reduce((acc, order) => {
      const symbol = normalizePair(order.symbol || order.rawSymbol);
      if (!acc.has(symbol)) {
        acc.set(symbol, []);
      }
      acc.get(symbol).push(order);
      return acc;
    }, new Map());
    const maxHoldMs = Number.isFinite(MAX_HOLD_MS) && MAX_HOLD_MS > 0 ? MAX_HOLD_MS : MAX_HOLD_SECONDS * 1000;

    for (const [symbol, state] of exitState.entries()) {
      if (symbolLocks.get(symbol)) {
        console.log('exit_manager_symbol_locked', { symbol });
        continue;
      }
      symbolLocks.set(symbol, true);
      try {
        const heldMs = now - state.entryTime;
        const heldSeconds = heldMs / 1000;
        const symbolOrders = openOrdersBySymbol.get(normalizePair(symbol)) || [];
        const openBuyCount = symbolOrders.filter((order) => String(order.side || '').toLowerCase() === 'buy').length;
        const openSellCount = symbolOrders.filter((order) => String(order.side || '').toLowerCase() === 'sell').length;

        let bid = null;

        let ask = null;
        let quoteFetchFailed = false;

        try {

          const quote = await getLatestQuote(symbol);

          bid = quote.bid;

          ask = quote.ask;

        } catch (err) {

          console.warn('quote_fetch_failed', { symbol, error: err?.message || err });
          quoteFetchFailed = isNetworkError(err);

        }

        if (quoteFetchFailed) {
          console.warn('exit_manager_skip_orders', { symbol, reason: 'quote_network_error' });
          continue;
        }

        let actionTaken = 'none';
        let reasonCode = 'hold';
        const spreadBps =
          Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 ? ((ask - bid) / bid) * 10000 : null;
        const entryFeeBps = Number.isFinite(state.entryFeeBps)
          ? state.entryFeeBps
          : inferEntryFeeBps({ symbol, orderType: 'limit', postOnly: true });
        const exitFeeBps = Number.isFinite(state.exitFeeBps)
          ? state.exitFeeBps
          : inferExitFeeBps({ takerExitOnTouch: TAKER_EXIT_ON_TOUCH });
        const feeBpsRoundTrip = Number.isFinite(state.feeBpsRoundTrip)
          ? state.feeBpsRoundTrip
          : entryFeeBps + exitFeeBps;
        const profitBufferBps = Number.isFinite(state.profitBufferBps) ? state.profitBufferBps : PROFIT_BUFFER_BPS;
        const desiredNetExitBps = Number.isFinite(state.desiredNetExitBps) ? state.desiredNetExitBps : null;
        const minNetProfitBps = computeMinNetProfitBps({
          feeBpsRoundTrip,
          profitBufferBps,
          desiredNetExitBps,
        });
        const tickSize = getTickSize({ symbol, price: state.entryPrice });
        const requiredExitBps = resolveRequiredExitBps({
          desiredNetExitBps,
          feeBpsRoundTrip,
          profitBufferBps,
        });
        const targetPrice = computeTargetSellPrice(state.entryPrice, requiredExitBps, tickSize);
        state.targetPrice = targetPrice;
        state.minNetProfitBps = minNetProfitBps;
        state.feeBpsRoundTrip = feeBpsRoundTrip;
        state.profitBufferBps = profitBufferBps;
        state.desiredNetExitBps = desiredNetExitBps;
        state.entryFeeBps = entryFeeBps;
        state.exitFeeBps = exitFeeBps;
        const breakevenPrice = computeBreakevenPrice(state.entryPrice, minNetProfitBps);
        const roundedBreakeven = roundToTick(breakevenPrice, tickSize, 'up');
        const bidMeetsBreakeven = Number.isFinite(bid) && bid >= breakevenPrice;
        const askMeetsBreakeven = Number.isFinite(ask) && ask >= breakevenPrice;
        const roundedAsk = Number.isFinite(ask) ? roundToTick(ask, tickSize, 'down') : null;
        const askMinusTick = Number.isFinite(ask) ? roundToTick(ask - tickSize, tickSize, 'down') : null;
        const makerDesiredLimit =
          askMeetsBreakeven &&
          Number.isFinite(roundedBreakeven) &&
          Number.isFinite(roundedAsk) &&
          roundedBreakeven <= roundedAsk
            ? Math.min(roundedAsk, Math.max(roundedBreakeven, askMinusTick))
            : null;
        const desiredLimit = Number.isFinite(makerDesiredLimit) ? makerDesiredLimit : roundedBreakeven;
        const lastCancelReplaceAtMs = lastCancelReplaceAt.get(symbol) || null;
        const lastRepriceAgeMs = Number.isFinite(lastCancelReplaceAtMs) ? now - lastCancelReplaceAtMs : null;
        const openSellOrders = symbolOrders.filter((order) => String(order.side || '').toLowerCase() === 'sell');
        if (!state.sellOrderId && openSellOrders.length === 1) {
          const onlyOrder = openSellOrders[0];
          state.sellOrderId = onlyOrder?.id || onlyOrder?.order_id || null;
          const limitPrice = normalizeOrderLimitPrice(onlyOrder);
          state.sellOrderLimit = Number.isFinite(limitPrice) ? limitPrice : state.sellOrderLimit;
          const orderAgeMs = getOrderAgeMs(onlyOrder);
          const normalizedOrderAgeMs = Number.isFinite(orderAgeMs) ? Math.max(0, orderAgeMs) : null;
          if (!state.sellOrderSubmittedAt && Number.isFinite(normalizedOrderAgeMs)) {
            state.sellOrderSubmittedAt = now - normalizedOrderAgeMs;
          }
        }
        if (openSellOrders.length > 1) {
          const desiredForSelect = Number.isFinite(desiredLimit) ? desiredLimit : state.sellOrderLimit;
          let keepOrder = openSellOrders[0];
          if (Number.isFinite(desiredForSelect)) {
            keepOrder = openSellOrders.reduce((best, candidate) => {
              const bestPrice = normalizeOrderLimitPrice(best);
              const candidatePrice = normalizeOrderLimitPrice(candidate);
              const bestDiff = Number.isFinite(bestPrice) ? Math.abs(bestPrice - desiredForSelect) : Number.POSITIVE_INFINITY;
              const candidateDiff = Number.isFinite(candidatePrice)
                ? Math.abs(candidatePrice - desiredForSelect)
                : Number.POSITIVE_INFINITY;
              return candidateDiff < bestDiff ? candidate : best;
            }, keepOrder);
          }
          const keepId = keepOrder?.id || keepOrder?.order_id;
          for (const order of openSellOrders) {
            const orderId = order?.id || order?.order_id;
            if (orderId && orderId !== keepId) {
              await cancelOrderSafe(orderId);
            }
          }
          if (keepId) {
            state.sellOrderId = keepId;
            const limitPrice = normalizeOrderLimitPrice(keepOrder);
            state.sellOrderLimit = Number.isFinite(limitPrice) ? limitPrice : state.sellOrderLimit;
            const keepOrderAgeMs = getOrderAgeMs(keepOrder);
            const normalizedKeepOrderAgeMs = Number.isFinite(keepOrderAgeMs) ? Math.max(0, keepOrderAgeMs) : null;
            state.sellOrderSubmittedAt =
              state.sellOrderSubmittedAt ||
              (Number.isFinite(normalizedKeepOrderAgeMs) ? now - normalizedKeepOrderAgeMs : null);
          }
          lastCancelReplaceAt.set(symbol, now);
        }

        if (!state.sellOrderId && state.qty > 0) {
          const replacement = await submitLimitSell({
            symbol,
            qty: state.qty,
            limitPrice: targetPrice,
            reason: 'missing_sell_order',
            intentRef: state.entryOrderId || getOrderIntentBucket(),
            postOnly: true,
            openOrders: symbolOrders,
          });
          if (!replacement?.skipped && replacement?.id) {
            state.sellOrderId = replacement.id;
            state.sellOrderSubmittedAt = Date.now();
            state.sellOrderLimit = normalizeOrderLimitPrice(replacement) ?? targetPrice;
            actionTaken = 'recreate_limit_sell';
            reasonCode = 'missing_sell_order';
            lastActionAt.set(symbol, now);
          }
        }


      if (heldSeconds >= FORCE_EXIT_SECONDS) {

        if (state.sellOrderId) {

          await cancelOrderSafe(state.sellOrderId);

        }

        await submitMarketSell({ symbol, qty: state.qty, reason: 'kill_switch' });

        exitState.delete(symbol);

        actionTaken = 'forced_exit_timeout';
        reasonCode = 'kill_switch';
        lastActionAt.set(symbol, now);
        const decisionPath = bidMeetsBreakeven ? 'taker_ioc' : (askMeetsBreakeven ? 'maker_post_only' : 'hold_not_profitable');
        const loggedLastCancelReplaceAt = lastCancelReplaceAt.get(symbol) || lastCancelReplaceAtMs;
        const loggedLastRepriceAgeMs = Number.isFinite(loggedLastCancelReplaceAt) ? now - loggedLastCancelReplaceAt : null;

        logExitDecision({

          symbol,

          heldSeconds,

          entryPrice: state.entryPrice,

          targetPrice,

          bid,

          ask,

          minNetProfitBps,

          actionTaken,

        });

        console.log('exit_scan', {
          symbol,
          heldQty: state.qty,
          entryPrice: state.entryPrice,
          bid,
          ask,
          spreadBps,
          openBuyCount,
          openSellCount,
          existingOrderAgeMs: null,
          feeBpsRoundTrip,
          profitBufferBps,
          minNetProfitBps,
          targetPrice,
          breakevenPrice,
          desiredLimit,
          decisionPath,
          lastRepriceAgeMs: loggedLastRepriceAgeMs,
          lastCancelReplaceAt: loggedLastCancelReplaceAt,
          actionTaken,
          reasonCode,
          iocRequestedQty: null,
          iocFilledQty: null,
          iocFallbackReason: null,
        });

        continue;

      }


      if (heldMs >= maxHoldMs && Number.isFinite(bid)) {

        const netProfitBps = ((bid - state.entryPrice) / state.entryPrice) * 10000;

        if (netProfitBps >= minNetProfitBps) {

          if (state.sellOrderId) {

            await cancelOrderSafe(state.sellOrderId);

          }

          await submitMarketSell({ symbol, qty: state.qty, reason: 'max_hold' });

          exitState.delete(symbol);

          actionTaken = 'max_hold_exit';
          reasonCode = 'max_hold';
          lastActionAt.set(symbol, now);
          const decisionPath = bidMeetsBreakeven ? 'taker_ioc' : (askMeetsBreakeven ? 'maker_post_only' : 'hold_not_profitable');
          const loggedLastCancelReplaceAt = lastCancelReplaceAt.get(symbol) || lastCancelReplaceAtMs;
          const loggedLastRepriceAgeMs = Number.isFinite(loggedLastCancelReplaceAt) ? now - loggedLastCancelReplaceAt : null;

          logExitDecision({

            symbol,

            heldSeconds,

            entryPrice: state.entryPrice,

            targetPrice,

            bid,

            ask,

            minNetProfitBps,

            actionTaken,

          });

          console.log('exit_scan', {
            symbol,
            heldQty: state.qty,
            entryPrice: state.entryPrice,
            bid,
            ask,
            spreadBps,
            openBuyCount,
            openSellCount,
            existingOrderAgeMs: null,
            feeBpsRoundTrip,
            profitBufferBps,
            minNetProfitBps,
            targetPrice,
            breakevenPrice,
            desiredLimit,
            decisionPath,
            lastRepriceAgeMs: loggedLastRepriceAgeMs,
            lastCancelReplaceAt: loggedLastCancelReplaceAt,
            actionTaken,
            reasonCode,
            iocRequestedQty: null,
            iocFilledQty: null,
            iocFallbackReason: null,
          });

          continue;

        }

      }
      let decisionPath = 'hold_within_band';
      let iocRequestedQty = null;
      let iocFilledQty = null;
      let iocFallbackReason = null;
      const repriceCooldownActive =
        Number.isFinite(lastRepriceAgeMs) && lastRepriceAgeMs < MIN_REPRICE_INTERVAL_MS;

      if (bidMeetsBreakeven && Number.isFinite(bid)) {
        if (state.sellOrderId) {
          await cancelOrderSafe(state.sellOrderId);
          lastCancelReplaceAt.set(symbol, now);
        }
        const iocPrice = roundToTick(bid, tickSize, 'down');
        const iocResult = await submitIocLimitSell({
          symbol,
          qty: state.qty,
          limitPrice: iocPrice,
          reason: 'taker_ioc',
        });
        if (!iocResult?.skipped) {
          iocRequestedQty = iocResult.requestedQty;
          iocFilledQty = normalizeFilledQty(iocResult.order);
          const remainingQty =
            Number.isFinite(iocRequestedQty) && Number.isFinite(iocFilledQty)
              ? Math.max(iocRequestedQty - iocFilledQty, 0)
              : null;
          if (remainingQty && remainingQty > 0) {
            iocFallbackReason = 'ioc_partial_fill';
            await submitMarketSell({ symbol, qty: remainingQty, reason: 'ioc_fallback' });
          }
          exitState.delete(symbol);
          actionTaken = 'taker_ioc_exit';
          reasonCode = 'taker_ioc';
          decisionPath = 'taker_ioc';
          lastActionAt.set(symbol, now);
        } else {
          actionTaken = 'hold_existing_order';
          reasonCode = iocResult?.reason || 'taker_ioc_skipped';
          decisionPath = 'taker_ioc';
        }
      } else if (askMeetsBreakeven && Number.isFinite(makerDesiredLimit)) {
        let order;
        if (state.sellOrderId) {
          try {
            order = await fetchOrderById(state.sellOrderId);
          } catch (err) {
            console.warn('order_fetch_failed', { symbol, orderId: state.sellOrderId, error: err?.message || err });
          }
        }

        if (order && ['canceled', 'expired', 'rejected'].includes(order.status)) {
          state.sellOrderId = null;
          state.sellOrderSubmittedAt = null;
          state.sellOrderLimit = null;
        }

        if (!state.sellOrderId) {
          const replacement = await submitLimitSell({
            symbol,
            qty: state.qty,
            limitPrice: makerDesiredLimit,
            reason: 'missing_sell_order',
            intentRef: state.entryOrderId || getOrderIntentBucket(),
            postOnly: true,
          });
          if (!replacement?.skipped && replacement?.id) {
            state.sellOrderId = replacement.id;
            state.sellOrderSubmittedAt = Date.now();
            state.sellOrderLimit = makerDesiredLimit;
            actionTaken = 'recreate_limit_sell';
            reasonCode = 'maker_post_only';
            decisionPath = 'maker_post_only';
            lastActionAt.set(symbol, now);
          } else {
            actionTaken = 'hold_existing_order';
            reasonCode = replacement?.reason || 'missing_sell_order_skipped';
            decisionPath = 'maker_post_only';
          }
        } else if (order && order.status === 'filled') {
          exitState.delete(symbol);
          actionTaken = 'sell_filled';
          reasonCode = 'tp_maker';
          decisionPath = 'maker_post_only';
          logExitDecision({
            symbol,
            heldSeconds,
            entryPrice: state.entryPrice,
            targetPrice,
            bid,
            ask,
            minNetProfitBps,
            actionTaken,
          });
        } else {
          const existingOrderAgeMs =
            (order && Number.isFinite(getOrderAgeMs(order)) ? Math.max(0, getOrderAgeMs(order)) : null) ||
            (state.sellOrderSubmittedAt ? Math.max(0, Date.now() - state.sellOrderSubmittedAt) : null);
          const currentLimit = normalizeOrderLimitPrice(order) ?? state.sellOrderLimit;
          if (Number.isFinite(currentLimit)) {
            state.sellOrderLimit = currentLimit;
          }
          const awayBps =
            Number.isFinite(currentLimit) && Number.isFinite(makerDesiredLimit) && makerDesiredLimit > 0
              ? ((currentLimit - makerDesiredLimit) / makerDesiredLimit) * 10000
              : null;

          if (existingOrderAgeMs != null && existingOrderAgeMs > SELL_ORDER_TTL_MS) {
            await cancelOrderSafe(state.sellOrderId);
            const replacement = await submitLimitSell({
              symbol,
              qty: state.qty,
              limitPrice: makerDesiredLimit,
              reason: 'reprice_ttl',
              intentRef: state.entryOrderId || getOrderIntentBucket(),
              postOnly: true,
            });
            if (replacement?.id) {
              state.sellOrderId = replacement.id;
              state.sellOrderSubmittedAt = Date.now();
              state.sellOrderLimit = makerDesiredLimit;
              actionTaken = 'reprice_cancel_replace';
              reasonCode = 'reprice_ttl';
              decisionPath = 'reprice_ttl';
              lastActionAt.set(symbol, now);
              lastCancelReplaceAt.set(symbol, now);
            } else {
              actionTaken = 'hold_existing_order';
              reasonCode = replacement?.reason || 'reprice_ttl_skipped';
              decisionPath = 'reprice_ttl';
            }
          } else if (Number.isFinite(awayBps) && awayBps > REPRICE_IF_AWAY_BPS && !repriceCooldownActive) {
            await cancelOrderSafe(state.sellOrderId);
            const replacement = await submitLimitSell({
              symbol,
              qty: state.qty,
              limitPrice: makerDesiredLimit,
              reason: 'reprice_away',
              intentRef: state.entryOrderId || getOrderIntentBucket(),
              postOnly: true,
            });
            if (replacement?.id) {
              state.sellOrderId = replacement.id;
              state.sellOrderSubmittedAt = Date.now();
              state.sellOrderLimit = makerDesiredLimit;
              actionTaken = 'reprice_cancel_replace';
              reasonCode = 'reprice_away';
              decisionPath = 'reprice_away';
              lastActionAt.set(symbol, now);
              lastCancelReplaceAt.set(symbol, now);
            } else {
              actionTaken = 'hold_existing_order';
              reasonCode = replacement?.reason || 'reprice_away_skipped';
              decisionPath = 'hold_within_band';
            }
          } else if (Number.isFinite(awayBps) && awayBps > REPRICE_IF_AWAY_BPS && repriceCooldownActive) {
            actionTaken = 'hold_existing_order';
            reasonCode = 'reprice_cooldown';
            decisionPath = 'hold_within_band';
          } else {
            actionTaken = 'hold_existing_order';
            reasonCode = 'no_reprice_needed';
            decisionPath = 'hold_within_band';
          }
        }
      } else if (!askMeetsBreakeven && !bidMeetsBreakeven) {
        decisionPath = 'hold_not_profitable';
        if (state.sellOrderId) {
          let order;
          try {
            order = await fetchOrderById(state.sellOrderId);
          } catch (err) {
            console.warn('order_fetch_failed', { symbol, orderId: state.sellOrderId, error: err?.message || err });
          }
          if (order && ['canceled', 'expired', 'rejected'].includes(order.status)) {
            state.sellOrderId = null;
            state.sellOrderSubmittedAt = null;
            state.sellOrderLimit = null;
          }
          if (order && order.status === 'filled') {
            exitState.delete(symbol);
            actionTaken = 'sell_filled';
            reasonCode = 'tp_maker';
          } else if (state.sellOrderId && Number.isFinite(desiredLimit)) {
            const existingOrderAgeMs =
              (order && Number.isFinite(getOrderAgeMs(order)) ? Math.max(0, getOrderAgeMs(order)) : null) ||
              (state.sellOrderSubmittedAt ? Math.max(0, Date.now() - state.sellOrderSubmittedAt) : null);
            const currentLimit = normalizeOrderLimitPrice(order) ?? state.sellOrderLimit;
            if (Number.isFinite(currentLimit)) {
              state.sellOrderLimit = currentLimit;
            }
            const awayBps =
              Number.isFinite(currentLimit) && desiredLimit > 0
                ? ((currentLimit - desiredLimit) / desiredLimit) * 10000
                : null;
            if (existingOrderAgeMs != null && existingOrderAgeMs > SELL_ORDER_TTL_MS) {
              await cancelOrderSafe(state.sellOrderId);
              const replacement = await submitLimitSell({
                symbol,
                qty: state.qty,
                limitPrice: desiredLimit,
                reason: 'reprice_ttl',
                intentRef: state.entryOrderId || getOrderIntentBucket(),
                postOnly: true,
              });
              if (replacement?.id) {
                state.sellOrderId = replacement.id;
                state.sellOrderSubmittedAt = Date.now();
                state.sellOrderLimit = desiredLimit;
                actionTaken = 'reprice_cancel_replace';
                reasonCode = 'reprice_ttl';
                decisionPath = 'reprice_ttl';
                lastActionAt.set(symbol, now);
                lastCancelReplaceAt.set(symbol, now);
              } else {
                actionTaken = 'hold_existing_order';
                reasonCode = replacement?.reason || 'reprice_ttl_skipped';
              }
            } else if (Number.isFinite(awayBps) && awayBps > REPRICE_IF_AWAY_BPS && !repriceCooldownActive) {
              await cancelOrderSafe(state.sellOrderId);
              const replacement = await submitLimitSell({
                symbol,
                qty: state.qty,
                limitPrice: desiredLimit,
                reason: 'reprice_away',
                intentRef: state.entryOrderId || getOrderIntentBucket(),
                postOnly: true,
              });
              if (replacement?.id) {
                state.sellOrderId = replacement.id;
                state.sellOrderSubmittedAt = Date.now();
                state.sellOrderLimit = desiredLimit;
                actionTaken = 'reprice_cancel_replace';
                reasonCode = 'reprice_away';
                decisionPath = 'reprice_away';
                lastActionAt.set(symbol, now);
                lastCancelReplaceAt.set(symbol, now);
              } else {
                actionTaken = 'hold_existing_order';
                reasonCode = replacement?.reason || 'reprice_away_skipped';
                decisionPath = 'hold_not_profitable';
              }
            } else if (Number.isFinite(awayBps) && awayBps > REPRICE_IF_AWAY_BPS && repriceCooldownActive) {
              actionTaken = 'hold_existing_order';
              reasonCode = 'reprice_cooldown';
              decisionPath = 'hold_not_profitable';
            } else {
              actionTaken = 'hold_existing_order';
              reasonCode = 'no_reprice_needed';
              decisionPath = 'hold_not_profitable';
            }
          } else {
            actionTaken = 'hold_existing_order';
            reasonCode = 'no_sell_order';
          }
        } else {
          actionTaken = 'hold_existing_order';
          reasonCode = 'not_profitable_no_order';
        }
      } else {
        actionTaken = 'hold_existing_order';
        reasonCode = 'hold_within_band';
        decisionPath = 'hold_within_band';
      }

      const rawAge = state.sellOrderSubmittedAt ? Date.now() - state.sellOrderSubmittedAt : null;
      const existingOrderAgeMs = Number.isFinite(rawAge) ? Math.max(0, rawAge) : null;
      const loggedLastCancelReplaceAt = lastCancelReplaceAt.get(symbol) || lastCancelReplaceAtMs;
      const loggedLastRepriceAgeMs = Number.isFinite(loggedLastCancelReplaceAt) ? now - loggedLastCancelReplaceAt : null;

      logExitDecision({

        symbol,

        heldSeconds,

        entryPrice: state.entryPrice,

        targetPrice,

        bid,

        ask,

        minNetProfitBps,

        actionTaken,

      });

      console.log('exit_scan', {
        symbol,
        heldQty: state.qty,
        entryPrice: state.entryPrice,
        bid,
        ask,
        spreadBps,
        openBuyCount,
        openSellCount,
        existingOrderAgeMs,
        feeBpsRoundTrip,
        profitBufferBps,
        minNetProfitBps,
        targetPrice,
        breakevenPrice,
        desiredLimit,
        decisionPath,
        lastRepriceAgeMs: loggedLastRepriceAgeMs,
        lastCancelReplaceAt: loggedLastCancelReplaceAt,
        actionTaken,
        reasonCode,
        iocRequestedQty,
        iocFilledQty,
        iocFallbackReason,
      });
      } finally {
        symbolLocks.delete(symbol);
      }

    }
  } finally {
    exitManagerRunning = false;
  }
}

function startExitManager() {

  setInterval(() => {

    manageExitStates().catch((err) => {

      console.error('exit_manager_failed', err?.message || err);

    });

  }, REPRICE_EVERY_SECONDS * 1000);

  console.log('exit_manager_started', { intervalSeconds: REPRICE_EVERY_SECONDS });

}

 

// Market buy using 10% of portfolio value then place a limit sell with markup

// covering taker fees and profit target

async function placeMarketBuyThenSell(symbol) {

  const normalizedSymbol = normalizeSymbol(symbol);
  const openOrders = await fetchOrders({ status: 'open' });
  if (hasOpenOrderForIntent(openOrders, { symbol: normalizedSymbol, side: 'BUY', intent: 'ENTRY' })) {
    console.log('hold_existing_order', { symbol: normalizedSymbol, side: 'buy', reason: 'existing_entry_intent' });
    return { skipped: true, reason: 'existing_entry_intent' };
  }
  let bid = null;
  let ask = null;
  let spreadBps = null;
  try {
    const quote = await getLatestQuote(normalizedSymbol);
    bid = quote.bid;
    ask = quote.ask;
    if (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0) {
      spreadBps = ((ask - bid) / bid) * 10000;
    }
  } catch (err) {
    console.warn('entry_quote_failed', { symbol: normalizedSymbol, error: err?.message || err });
  }
  if (Number.isFinite(spreadBps) && spreadBps > MAX_SPREAD_BPS_TO_TRADE) {
    logSkip('spread_too_wide', { symbol: normalizedSymbol, bid, ask, spreadBps });
    return { skipped: true, reason: 'spread_too_wide', spreadBps };
  }

  const [price, account] = await Promise.all([

    getLatestPrice(normalizedSymbol),

    getAccountInfo(),

  ]);

 

  const portfolioValue = account.portfolioValue;

  const buyingPower = account.buyingPower;

 

  const targetTradeAmount = portfolioValue * 0.1;

  const amountToSpend = Math.min(targetTradeAmount, buyingPower);

  const decision = Number.isFinite(amountToSpend) && amountToSpend >= MIN_ORDER_NOTIONAL_USD ? 'BUY' : 'SKIP';

  logBuyDecision(normalizedSymbol, amountToSpend, decision);

  if (decision === 'SKIP') {

    logSkip('notional_too_small', {

      symbol: normalizedSymbol,

      intendedNotional: amountToSpend,

      minNotionalUsd: MIN_ORDER_NOTIONAL_USD,

    });

    return { skipped: true, reason: 'notional_too_small', notionalUsd: amountToSpend };

  }

  if (amountToSpend < 10) {

    throw new Error('Insufficient buying power for trade');

  }

 

  const qty = roundQty(amountToSpend / price);

  if (qty <= 0) {

    throw new Error('Insufficient buying power for trade');

  }

  const sizeGuard = guardTradeSize({
    symbol: normalizedSymbol,
    qty,
    notional: amountToSpend,
    price,
    side: 'buy',
    context: 'market_buy',
  });
  if (sizeGuard.skip) {
    return { skipped: true, reason: 'below_min_trade', notionalUsd: sizeGuard.notional };
  }
  const finalQty = sizeGuard.qty ?? qty;

  const buyOrderUrl = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'orders',
    label: 'orders_market_buy',
  });
  const buyPayload = {
    symbol: toTradeSymbol(normalizedSymbol),
    qty: finalQty,
    side: 'buy',
    type: 'market',
    time_in_force: 'gtc',
    client_order_id: buildEntryClientOrderId(normalizedSymbol),
  };
  const buyOrder = await placeOrderUnified({
    symbol: normalizedSymbol,
    url: buyOrderUrl,
    payload: buyPayload,
    label: 'orders_market_buy',
    reason: 'market_buy',
    context: 'market_buy',
  });

 

  // Wait for fill

  let filled = buyOrder;

  for (let i = 0; i < 20; i++) {

    const checkUrl = buildAlpacaUrl({
      baseUrl: ALPACA_BASE_URL,
      path: `orders/${buyOrder.id}`,
      label: 'orders_market_buy_check',
    });
    let chk;
    try {
      chk = await requestJson({
        method: 'GET',
        url: checkUrl,
        headers: alpacaHeaders(),
      });
    } catch (err) {
      logHttpError({
        symbol: normalizedSymbol,
        label: 'orders',
        url: checkUrl,
        error: err,
      });
      throw err;
    }

    filled = chk;

    if (filled.status === 'filled') break;

    await sleep(3000);

  }

 

  if (filled.status !== 'filled') {

    throw new Error('Buy order not filled in time');

  }

  updateInventoryFromBuy(normalizedSymbol, filled.filled_qty, filled.filled_avg_price);

  const inventory = inventoryState.get(normalizedSymbol);

  if (!inventory || inventory.qty <= 0) {

    logSkip('no_inventory_for_sell', { symbol: normalizedSymbol, qty: filled.filled_qty });

    return { buy: filled, sell: null, sellError: 'No inventory to sell' };

  }

 

  const avgPrice = parseFloat(filled.filled_avg_price);

  try {

    const sellOrder = await handleBuyFill({

      symbol: normalizedSymbol,

      qty: filled.filled_qty,

      entryPrice: avgPrice,
      entryOrderId: filled.id || buyOrder?.id,

    });

    return { buy: filled, sell: sellOrder };

  } catch (err) {

    console.error('Sell order failed:', err?.responseSnippet200 || err?.errorMessage || err.message);

    return { buy: filled, sell: null, sellError: err.message };

  }

}

async function submitOrder(order = {}) {

  const {

    symbol: rawSymbol,

    qty,

    side,

    type,

    time_in_force,

    limit_price,

    notional,

    client_order_id,

    reason,
    desiredNetExitBps,

  } = order;

  const normalizedSymbol = normalizeSymbol(rawSymbol);
  const isCrypto = isCryptoSymbol(normalizedSymbol);
  const sideLower = String(side || '').toLowerCase();
  const typeLower = String(type || '').toLowerCase();
  const allowedCryptoTypes = new Set(['market', 'limit', 'stop_limit']);
  const finalType = isCrypto && !allowedCryptoTypes.has(typeLower) ? 'market' : (typeLower || 'market');
  const rawTif = String(time_in_force || '').toLowerCase();
  const allowedCryptoTifs = new Set(['gtc', 'ioc', 'fok']);
  const defaultCryptoTif = sideLower === 'sell' ? 'ioc' : 'gtc';
  const finalTif = isCrypto ? (allowedCryptoTifs.has(rawTif) ? rawTif : defaultCryptoTif) : (rawTif || time_in_force);
  let qtyNum = Number(qty);
  const limitPriceNum = Number(limit_price);

  let computedNotionalUsd = Number(notional);

  if (!Number.isFinite(computedNotionalUsd) || computedNotionalUsd <= 0) {

    if (Number.isFinite(qtyNum) && qtyNum > 0 && Number.isFinite(limitPriceNum) && limitPriceNum > 0) {

      computedNotionalUsd = qtyNum * limitPriceNum;

    } else if (Number.isFinite(qtyNum) && qtyNum > 0 && sideLower === 'buy') {

      const price = await getLatestPrice(normalizedSymbol);

      computedNotionalUsd = qtyNum * price;

    }

  }

  if (sideLower === 'sell') {
    const availableQty = await getAvailablePositionQty(normalizedSymbol);
    if (!(availableQty > 0)) {
      logSkip('no_position_qty', {
        symbol: normalizedSymbol,
        qty: qtyNum,
        availableQty,
        context: 'submit_order',
      });
      return { skipped: true, reason: 'no_position_qty' };
    }
    const openOrders = await fetchOrders({ status: 'open' });
    const hasOpenSell = (Array.isArray(openOrders) ? openOrders : []).some((order) => {
      const orderSymbol = normalizePair(order.symbol || order.rawSymbol);
      const side = String(order.side || '').toLowerCase();
      return orderSymbol === normalizePair(normalizedSymbol) && side === 'sell';
    });
    if (hasOpenSell) {
      console.log('hold_existing_order', { symbol: normalizedSymbol, side: 'sell', reason: 'existing_sell_open' });
      return { skipped: true, reason: 'existing_sell_open' };
    }
    if (!Number.isFinite(qtyNum) || qtyNum <= 0) {
      qtyNum = availableQty;
    } else {
      qtyNum = Math.min(qtyNum, availableQty);
    }
    if (Number.isFinite(qtyNum) && qtyNum > 0 && Number.isFinite(limitPriceNum) && limitPriceNum > 0) {
      computedNotionalUsd = qtyNum * limitPriceNum;
    }
  }

  if (sideLower === 'buy') {
    const desiredNetExitBpsNum = Number(desiredNetExitBps);
    if (Number.isFinite(desiredNetExitBpsNum)) {
      desiredExitBpsBySymbol.set(normalizedSymbol, desiredNetExitBpsNum);
    }

    const decision =

      Number.isFinite(computedNotionalUsd) && computedNotionalUsd >= MIN_ORDER_NOTIONAL_USD ? 'BUY' : 'SKIP';

    logBuyDecision(normalizedSymbol, computedNotionalUsd, decision);

    if (decision === 'SKIP') {

      logSkip('notional_too_small', {

        symbol: normalizedSymbol,

        intendedNotional: computedNotionalUsd,

        minNotionalUsd: MIN_ORDER_NOTIONAL_USD,

      });

      return { skipped: true, reason: 'notional_too_small', notionalUsd: computedNotionalUsd };

    }

  }

  if (sideLower === 'buy') {
    const openOrders = await fetchOrders({ status: 'open' });
    if (hasOpenOrderForIntent(openOrders, { symbol: normalizedSymbol, side: 'BUY', intent: 'ENTRY' })) {
      console.log('hold_existing_order', { symbol: normalizedSymbol, side: 'buy', reason: 'existing_entry_intent' });
      return { skipped: true, reason: 'existing_entry_intent' };
    }
  }

  const sizeGuard = guardTradeSize({
    symbol: normalizedSymbol,
    qty: qtyNum,
    notional: Number.isFinite(computedNotionalUsd) ? computedNotionalUsd : notional,
    price: Number.isFinite(limitPriceNum) ? limitPriceNum : null,
    side: sideLower,
    context: 'submit_order',
  });
  if (sizeGuard.skip) {
    return { skipped: true, reason: 'below_min_trade', notionalUsd: sizeGuard.notional };
  }
  const finalQty = sizeGuard.qty ?? qtyNum ?? qty;
  const finalNotional = sizeGuard.notional ?? notional;
  const hasQty = Number.isFinite(Number(finalQty)) && Number(finalQty) > 0;
  const hasNotional = Number.isFinite(Number(finalNotional)) && Number(finalNotional) > 0;
  const useQty = hasQty;
  const useNotional = !useQty && hasNotional;

  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'orders', label: 'orders_submit' });
  const defaultClientOrderId =
    sideLower === 'buy' ? buildEntryClientOrderId(normalizedSymbol) : buildClientOrderId(normalizedSymbol, 'order');
  const payload = {
    symbol: toTradeSymbol(normalizedSymbol),
    side: sideLower,
    type: finalType,
    time_in_force: finalTif,
    limit_price: Number.isFinite(limitPriceNum) ? limitPriceNum : undefined,
    qty: useQty ? finalQty : undefined,
    notional: useNotional ? finalNotional : undefined,
    client_order_id: client_order_id || defaultClientOrderId,
  };
  return placeOrderUnified({
    symbol: normalizedSymbol,
    url,
    payload,
    label: 'orders_submit',
    reason,
    context: 'submit_order',
  });

}

async function replaceOrder(orderId, payload = {}) {
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: `orders/${orderId}`,
    label: 'orders_replace',
  });
  try {
    const response = await requestJson({
      method: 'PATCH',
      url,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify(payload),
    });
    return response;
  } catch (err) {
    logHttpError({ label: 'orders_replace', url, error: err });
    throw err;
  }
}

async function fetchOrders(params = {}) {
  const resolvedParams = { ...(params || {}) };
  if (String(resolvedParams.status || '').toLowerCase() === 'open') {
    delete resolvedParams.symbol;
  }
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'orders',
    params: resolvedParams,
    label: 'orders_list',
  });
  let response;
  try {
    response = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ label: 'orders', url, error: err });
    throw err;
  }

  if (Array.isArray(response)) {

    return response.map((order) => ({

      ...order,

      rawSymbol: order.symbol,
      pairSymbol: normalizeSymbol(order.symbol),
      symbol: normalizeSymbol(order.symbol),

    }));

  }

  return response;

}

async function fetchOpenPositions() {
  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'positions', label: 'positions_list' });
  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ label: 'positions', url, error: err });
    throw err;
  }
  const positions = Array.isArray(res) ? res : [];
  updatePositionsSnapshot(
    positions.map((pos) => ({
      ...pos,
      rawSymbol: pos.symbol,
      pairSymbol: normalizeSymbol(pos.symbol),
      symbol: normalizeSymbol(pos.symbol),
    }))
  );
  return positions
    .map((pos) => {
      const qty = Number(pos.qty ?? pos.quantity ?? 0);
      const pairSymbol = normalizeSymbol(pos.symbol);
      return {
        rawSymbol: pos.symbol,
        pairSymbol,
        symbol: pairSymbol,
        qty,
        isDust: isDustQty(qty),
      };
    })
    .filter((pos) => Number.isFinite(pos.qty) && pos.qty !== 0);
}

async function fetchOpenOrders() {
  const orders = await fetchOrders({ status: 'open' });
  const list = Array.isArray(orders) ? orders : [];
  return list.map((order) => ({
    id: order.id || order.order_id,
    client_order_id: order.client_order_id,
    rawSymbol: order.rawSymbol ?? order.symbol,
    pairSymbol: normalizeSymbol(order.symbol),
    symbol: normalizeSymbol(order.symbol),
    side: order.side,
    status: order.status,
    limit_price: order.limit_price,
    submitted_at: order.submitted_at,
    created_at: order.created_at,
  }));
}

async function getConcurrencyGuardStatus() {
  scanState.lastScanAt = new Date().toISOString();
  const [openPositions, openOrders] = await Promise.all([
    fetchOpenPositions(),
    fetchOpenOrders(),
  ]);
  const nonDustPositions = openPositions.filter((pos) => !pos.isDust);
  const positionSymbols = new Set(nonDustPositions.map((pos) => pos.symbol));
  const orderSymbols = new Set(openOrders.map((order) => order.symbol));
  const activeSymbols = new Set([...positionSymbols, ...orderSymbols]);
  const activeSlotsUsed = activeSymbols.size;
  const positionsCount = positionSymbols.size;
  const ordersCount = orderSymbols.size;
  console.log(
    `Concurrency guard: used=${activeSlotsUsed} cap=${MAX_CONCURRENT_POSITIONS} positions=${positionsCount} orders=${ordersCount}`
  );
  return {
    openPositions,
    openOrders,
    activeSlotsUsed,
    capMax: MAX_CONCURRENT_POSITIONS,
    lastScanAt: scanState.lastScanAt,
  };
}

function getLastQuoteSnapshot() {
  const nowMs = Date.now();
  const snapshot = {};
  for (const [symbol, entry] of lastQuoteAt.entries()) {
    const tsMs = entry?.tsMs;
    if (Number.isFinite(tsMs)) {
      const rawAgeMs = computeQuoteAgeMs({ nowMs, tsMs });
      const ageMs = normalizeQuoteAgeMs(rawAgeMs);
      if (Number.isFinite(rawAgeMs)) {
        logQuoteAgeWarning({ symbol, ageMs: rawAgeMs, source: entry.source, tsMs });
      }
      snapshot[symbol] = {
        ts: new Date(tsMs).toISOString(),
        ageSeconds: Number.isFinite(ageMs) ? ageMs / 1000 : null,
        source: entry.source,
        reason: entry.reason ?? undefined,
      };
    } else {
      snapshot[symbol] = {
        ts: null,
        ageSeconds: null,
        source: entry?.source,
        reason: entry?.reason ?? undefined,
      };
    }
  }
  return snapshot;
}

async function runDustCleanup() {
  const dustCleanupEnabled = String(process.env.DUST_CLEANUP || '').toLowerCase() === 'true';
  if (!dustCleanupEnabled) {
    return;
  }
  const autoSellEnabled = String(process.env.AUTO_SELL_DUST || '').toLowerCase() === 'true';
  let positions = [];
  try {
    positions = await fetchOpenPositions();
  } catch (err) {
    console.warn('dust_cleanup_fetch_failed', err?.message || err);
    return;
  }
  const dustPositions = positions.filter((pos) => pos.isDust);
  if (!dustPositions.length) {
    console.log('dust_cleanup', { detected: 0 });
    return;
  }

  for (const dust of dustPositions) {
    console.log('dust_position_detected', { symbol: dust.symbol, qty: dust.qty });
    if (!autoSellEnabled) {
      continue;
    }
    if (!Number.isFinite(dust.qty) || dust.qty <= 0) {
      console.log('dust_auto_sell_skipped', { symbol: dust.symbol, qty: dust.qty, reason: 'non_positive_qty' });
      continue;
    }
    try {
      const result = await submitMarketSell({
        symbol: dust.symbol,
        qty: dust.qty,
        reason: 'dust_cleanup',
      });
      console.log('dust_auto_sell_submitted', { symbol: dust.symbol, qty: dust.qty, orderId: result?.id });
    } catch (err) {
      console.warn('dust_auto_sell_failed', {
        symbol: dust.symbol,
        qty: dust.qty,
        error: err?.responseSnippet200 || err?.errorMessage || err?.message || err,
      });
    }
  }
}

function getAlpacaAuthStatus() {
  return {
    alpacaAuthOk: resolvedAlpacaAuth.alpacaAuthOk,
    alpacaKeyIdPresent: resolvedAlpacaAuth.alpacaKeyIdPresent,
  };
}

function getLastHttpError() {
  return lastHttpError;
}

async function cancelOrder(orderId) {
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: `orders/${orderId}`,
    label: 'orders_cancel',
  });
  let response;
  try {
    response = await requestJson({
      method: 'DELETE',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ label: 'orders', url, error: err });
    throw err;
  }

  return response;

}

async function getAlpacaConnectivityStatus() {
  const hasAuth = resolvedAlpacaAuth.alpacaAuthOk;
  const tradeUrl = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'account', label: 'account_health' });
  const dataSymbol = 'AAPL';
  const dataUrl = buildAlpacaUrl({
    baseUrl: STOCKS_DATA_URL,
    path: 'quotes/latest',
    params: { symbols: dataSymbol },
    label: 'stocks_health_quote',
  });

  const tradeResult = await httpJson({
    method: 'GET',
    url: tradeUrl,
    headers: alpacaHeaders(),
  });
  if (tradeResult.error) {
    logHttpError({ label: 'account', url: tradeUrl, error: tradeResult.error });
  }

  const dataResult = await httpJson({
    method: 'GET',
    url: dataUrl,
    headers: alpacaHeaders(),
  });
  if (dataResult.error) {
    logHttpError({ label: 'quotes', url: dataUrl, error: dataResult.error });
  }

  const tradeErrorMessage = tradeResult.error
    ? tradeResult.error.errorMessage || tradeResult.error.message || 'Unknown error'
    : null;
  const dataErrorMessage = dataResult.error
    ? dataResult.error.errorMessage || dataResult.error.message || 'Unknown error'
    : null;
  const errors = [tradeErrorMessage ? `trade: ${tradeErrorMessage}` : null, dataErrorMessage ? `data: ${dataErrorMessage}` : null]
    .filter(Boolean)
    .join('; ') || null;

  return {
    auth: {
      hasAuth,
      alpacaAuthOk: resolvedAlpacaAuth.alpacaAuthOk,
      alpacaKeyIdPresent: resolvedAlpacaAuth.alpacaKeyIdPresent,
    },
    tradeAccountOk: !tradeResult.error,
    tradeStatus: tradeResult.error ? tradeResult.error.statusCode ?? null : tradeResult.statusCode ?? 200,
    tradeSnippet: tradeResult.error
      ? tradeResult.error.responseSnippet200 || ''
      : tradeResult.responseSnippet200 || '',
    tradeRequestId: tradeResult.error ? tradeResult.error.requestId || null : tradeResult.requestId || null,
    dataQuoteOk: !dataResult.error,
    dataStatus: dataResult.error ? dataResult.error.statusCode ?? null : dataResult.statusCode ?? 200,
    dataSnippet: dataResult.error
      ? dataResult.error.responseSnippet200 || ''
      : dataResult.responseSnippet200 || '',
    dataRequestId: dataResult.error ? dataResult.error.requestId || null : dataResult.requestId || null,
    baseUrls: {
      tradeBase: TRADE_BASE,
      dataBase: DATA_BASE,
    },
    resolvedUrls: {
      tradeBaseUrl: ALPACA_BASE_URL,
      cryptoDataUrl: CRYPTO_DATA_URL,
      stocksDataUrl: STOCKS_DATA_URL,
      tradeAccountUrl: tradeUrl,
      dataQuoteUrl: dataUrl,
    },
    error: errors,
  };
}

module.exports = {

  placeLimitBuyThenSell,

  placeMarketBuyThenSell,

  initializeInventoryFromPositions,

  submitOrder,

  fetchOrders,
  fetchOrderById,
  replaceOrder,

  cancelOrder,

  normalizeSymbol,
  normalizeSymbolsParam,
  getLatestQuote,
  getLatestPrice,
  fetchCryptoQuotes,
  fetchCryptoTrades,
  fetchCryptoBars,
  fetchStockQuotes,
  fetchStockTrades,
  fetchStockBars,
  fetchAccount,
  fetchPortfolioHistory,
  fetchActivities,
  fetchClock,
  fetchPositions,
  fetchPosition,
  fetchAsset,
  loadSupportedCryptoPairs,
  getSupportedCryptoPairsSnapshot,
  filterSupportedCryptoSymbols,

  startExitManager,
  getConcurrencyGuardStatus,
  getLastQuoteSnapshot,
  getAlpacaAuthStatus,
  getLastHttpError,
  getAlpacaConnectivityStatus,
  runDustCleanup,

};
