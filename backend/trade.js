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

const MAX_HOLD_SECONDS = Number(process.env.MAX_HOLD_SECONDS || 300);

const REPRICE_EVERY_SECONDS = Number(process.env.REPRICE_EVERY_SECONDS || 20);

const FORCE_EXIT_SECONDS = Number(process.env.FORCE_EXIT_SECONDS || 600);

const PRICE_TICK = Number(process.env.PRICE_TICK || 0.01);
const MAX_CONCURRENT_POSITIONS = Number(process.env.MAX_CONCURRENT_POSITIONS || 100);
const MIN_POSITION_QTY = Number(process.env.MIN_POSITION_QTY || 1e-6);
const QUOTE_CACHE_MAX_AGE_MS = 60000;
const MAX_LOGGED_QUOTE_AGE_SECONDS = 9999;
const DEBUG_QUOTE_TS = ['1', 'true', 'yes'].includes(String(process.env.DEBUG_QUOTE_TS || '').toLowerCase());
const quoteTsDebugLogged = new Set();

const inventoryState = new Map();

const exitState = new Map();

const cfeeCache = { ts: 0, items: [] };
const quoteCache = new Map();
const lastQuoteAt = new Map();
const scanState = { lastScanAt: null };
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
  return alpacaSymbol(rawSymbol);
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
  const statusCode = error?.statusCode ?? null;
  const errorMessage = error?.errorMessage || error?.message || 'Unknown error';
  const snippet = error?.responseSnippet200 || '';
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
    client_order_id: buildClientOrderId(normalizedSymbol, 'limit-buy'),
  };
  logOrderPayload({ label: 'orders_limit_buy', payload: buyPayload });
  let buyOrder;
  try {
    buyOrder = await requestJson({
      method: 'POST',
      url: buyOrderUrl,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify(buyPayload),
    });
    logOrderResponse({ label: 'orders_limit_buy', payload: buyPayload, response: buyOrder });
  } catch (err) {
    logHttpError({
      symbol: normalizedSymbol,
      label: 'orders',
      url: buyOrderUrl,
      error: err,
    });
    logOrderResponse({ label: 'orders_limit_buy', payload: buyPayload, error: err });
    if (isNetworkError(err)) {
      logNetworkError({
        type: 'order',
        symbol: normalizedSymbol,
        attempts: err.attempts ?? 1,
        context: 'limit_buy',
      });
    }
    throw err;
  }

 

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
  return positions.map((pos) => ({
    ...pos,
    rawSymbol: pos.symbol,
    pairSymbol: normalizeSymbol(pos.symbol),
    symbol: normalizeSymbol(pos.symbol),
  }));
}

async function fetchPosition(symbol) {
  const normalized = toTradeSymbol(symbol);
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: `positions/${encodeURIComponent(normalized)}`,
    label: 'positions_single',
  });
  return requestJson({
    method: 'GET',
    url,
    headers: alpacaHeaders(),
  });
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
  try {
    const pos = await fetchPosition(symbol);
    const qty = Number(pos?.qty_available ?? pos?.available ?? pos?.qty ?? pos?.quantity ?? 0);
    return Number.isFinite(qty) ? qty : 0;
  } catch (err) {
    if (err?.statusCode === 404) return 0;
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
  let computedNotional = roundedNotional;
  if (!Number.isFinite(computedNotional) && Number.isFinite(roundedQty) && Number.isFinite(price)) {
    computedNotional = roundNotional(roundedQty * price);
  }

  if (Number.isFinite(roundedQty) && roundedQty > 0 && roundedQty < MIN_TRADE_QTY) {
    const reason = String(side || '').toLowerCase() === 'sell' ? 'below_min_order_size' : 'below_min_trade';
    logSkip(reason, {
      symbol,
      side,
      qty: roundedQty,
      minQty: MIN_TRADE_QTY,
      context,
    });
    return { skip: true, qty: roundedQty, notional: computedNotional };
  }

  if (Number.isFinite(computedNotional) && computedNotional < MIN_ORDER_NOTIONAL_USD) {
    const reason = String(side || '').toLowerCase() === 'sell' ? 'below_min_order_size' : 'below_min_trade';
    logSkip(reason, {
      symbol,
      side,
      notionalUsd: computedNotional,
      minNotionalUsd: MIN_ORDER_NOTIONAL_USD,
      context,
    });
    return { skip: true, qty: roundedQty, notional: computedNotional };
  }

  return { skip: false, qty: roundedQty ?? qty, notional: roundedNotional ?? notional, computedNotional };
}

 

// Round prices to two decimals

function roundPrice(price) {

  return parseFloat(Number(price).toFixed(2));

}

function roundToTick(price, tick = PRICE_TICK) {
  if (!Number.isFinite(price)) return price;
  const tickSize = Number.isFinite(tick) && tick > 0 ? tick : 0.01;
  return Math.ceil(price / tickSize) * tickSize;
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

}) {

  const availableQty = await getAvailablePositionQty(symbol);
  if (!(availableQty > 0)) {
    logSkip('no_position_qty', { symbol, qty, availableQty, context: 'limit_sell' });
    return { skipped: true, reason: 'no_position_qty' };
  }
  const qtyNum = Number(qty);
  const adjustedQty = Number.isFinite(qtyNum) && qtyNum > 0 ? Math.min(qtyNum, availableQty) : availableQty;

  const sizeGuard = guardTradeSize({
    symbol,
    qty: adjustedQty,
    price: Number(limitPrice),
    side: 'sell',
    context: 'limit_sell',
  });
  if (sizeGuard.skip) {
    return { skipped: true, reason: 'below_min_trade' };
  }
  const finalQty = sizeGuard.qty ?? qty;

  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'orders', label: 'orders_limit_sell' });
  const payload = {
    symbol: toTradeSymbol(symbol),
    qty: finalQty,
    side: 'sell',
    type: 'limit',
    time_in_force: 'gtc',
    limit_price: limitPrice,
    client_order_id: buildClientOrderId(symbol, 'limit-sell'),
  };
  logOrderPayload({ label: 'orders_limit_sell', payload });
  let response;
  try {
    response = await requestJson({
      method: 'POST',
      url,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify(payload),
    });
    logOrderResponse({ label: 'orders_limit_sell', payload, response });
  } catch (err) {
    logHttpError({ symbol, label: 'orders', url, error: err });
    logOrderResponse({ label: 'orders_limit_sell', payload, error: err });
    if (isNetworkError(err)) {
      logNetworkError({
        type: 'order',
        symbol,
        attempts: err.attempts ?? 1,
        context: 'limit_sell',
      });
    }
    throw err;
  }

  console.log('submit_limit_sell', { symbol, qty, limitPrice, reason, orderId: response?.id });

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
    client_order_id: buildClientOrderId(symbol, 'market-sell'),
  };
  logOrderPayload({ label: 'orders_market_sell', payload });
  let response;
  try {
    response = await requestJson({
      method: 'POST',
      url,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify(payload),
    });
    logOrderResponse({ label: 'orders_market_sell', payload, response });
  } catch (err) {
    logHttpError({ symbol, label: 'orders', url, error: err });
    logOrderResponse({ label: 'orders_market_sell', payload, error: err });
    if (isNetworkError(err)) {
      logNetworkError({
        type: 'order',
        symbol,
        attempts: err.attempts ?? 1,
        context: 'market_sell',
      });
    }
    throw err;
  }

  console.log('submit_market_sell', { symbol, qty, reason, orderId: response?.id });

  return response;

}

async function handleBuyFill({

  symbol: rawSymbol,

  qty,

  entryPrice,

}) {

  const symbol = normalizeSymbol(rawSymbol);

  const entryPriceNum = Number(entryPrice);

  const qtyNum = Number(qty);

  const notionalUsd = qtyNum * entryPriceNum;

  const minNetProfitBps = await feeAwareMinProfitBps(symbol, notionalUsd);

  const targetPrice = roundToTick(entryPriceNum * (1 + minNetProfitBps / 10000));

  const sellOrder = await submitLimitSell({

    symbol,

    qty: qtyNum,

    limitPrice: targetPrice,

    reason: 'initial_target',

  });

  const now = Date.now();

  exitState.set(symbol, {

    symbol,

    qty: qtyNum,

    entryPrice: entryPriceNum,

    entryTime: now,

    notionalUsd,

    minNetProfitBps,

    targetPrice,

    sellOrderId: sellOrder.id,

    sellOrderSubmittedAt: now,

    sellOrderLimit: targetPrice,

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

async function manageExitStates() {

  const now = Date.now();

  for (const [symbol, state] of exitState.entries()) {

    const heldSeconds = (now - state.entryTime) / 1000;

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


    if (heldSeconds >= FORCE_EXIT_SECONDS) {

      if (state.sellOrderId) {

        await cancelOrderSafe(state.sellOrderId);

      }

      await submitMarketSell({ symbol, qty: state.qty, reason: 'forced_exit_timeout' });

      exitState.delete(symbol);

      logExitDecision({

        symbol,

        heldSeconds,

        entryPrice: state.entryPrice,

        targetPrice: state.targetPrice,

        bid,

        ask,

        minNetProfitBps: state.minNetProfitBps,

        actionTaken: 'forced_exit_timeout',

      });

      continue;

    }


    if (heldSeconds >= MAX_HOLD_SECONDS && Number.isFinite(bid)) {

      const netProfitBps = ((bid - state.entryPrice) / state.entryPrice) * 10000;

      if (netProfitBps >= state.minNetProfitBps) {

        if (state.sellOrderId) {

          await cancelOrderSafe(state.sellOrderId);

        }

        await submitMarketSell({ symbol, qty: state.qty, reason: 'profit_exit' });

        exitState.delete(symbol);

        logExitDecision({

          symbol,

          heldSeconds,

          entryPrice: state.entryPrice,

          targetPrice: state.targetPrice,

          bid,

          ask,

          minNetProfitBps: state.minNetProfitBps,

          actionTaken: 'profit_exit_market',

        });

        continue;

      }

    }


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

      if (!state.sellOrderId) {

        const replacement = await submitLimitSell({

          symbol,

          qty: state.qty,

          limitPrice: state.targetPrice,

          reason: 'missing_sell_order',

        });

        state.sellOrderId = replacement.id;

        state.sellOrderSubmittedAt = Date.now();

        state.sellOrderLimit = state.targetPrice;

        actionTaken = 'recreate_limit_sell';

      } else if (order && order.status === 'filled') {

        exitState.delete(symbol);

        actionTaken = 'sell_filled';

        logExitDecision({

          symbol,

          heldSeconds,

          entryPrice: state.entryPrice,

          targetPrice: state.targetPrice,

          bid,

          ask,

          minNetProfitBps: state.minNetProfitBps,

          actionTaken,

        });

        continue;

      } else {

        const orderAgeSeconds = state.sellOrderSubmittedAt ? (now - state.sellOrderSubmittedAt) / 1000 : 0;

        if (orderAgeSeconds >= REPRICE_EVERY_SECONDS && Number.isFinite(ask)) {

          if (ask < state.sellOrderLimit - PRICE_TICK) {

            await cancelOrderSafe(state.sellOrderId);

            const newLimit = roundToTick(Math.max(state.targetPrice, ask - PRICE_TICK));

            const replacement = await submitLimitSell({

              symbol,

              qty: state.qty,

              limitPrice: newLimit,

              reason: 'reprice_lower_ask',

            });

            state.sellOrderId = replacement.id;

            state.sellOrderSubmittedAt = Date.now();

            state.sellOrderLimit = newLimit;

            actionTaken = 'reprice_cancel_replace';

          }

        }

      }

    } else {

      const replacement = await submitLimitSell({

        symbol,

        qty: state.qty,

        limitPrice: state.targetPrice,

        reason: 'missing_sell_order',

      });

      state.sellOrderId = replacement.id;

      state.sellOrderSubmittedAt = Date.now();

      state.sellOrderLimit = state.targetPrice;

      actionTaken = 'recreate_limit_sell';

    }


    logExitDecision({

      symbol,

      heldSeconds,

      entryPrice: state.entryPrice,

      targetPrice: state.targetPrice,

      bid,

      ask,

      minNetProfitBps: state.minNetProfitBps,

      actionTaken,

    });

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
    client_order_id: buildClientOrderId(normalizedSymbol, 'market-buy'),
  };
  logOrderPayload({ label: 'orders_market_buy', payload: buyPayload });
  let buyOrder;
  try {
    buyOrder = await requestJson({
      method: 'POST',
      url: buyOrderUrl,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify(buyPayload),
    });
    logOrderResponse({ label: 'orders_market_buy', payload: buyPayload, response: buyOrder });
  } catch (err) {
    logHttpError({
      symbol: normalizedSymbol,
      label: 'orders',
      url: buyOrderUrl,
      error: err,
    });
    logOrderResponse({ label: 'orders_market_buy', payload: buyPayload, error: err });
    throw err;
  }

 

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

  } = order;

  const normalizedSymbol = normalizeSymbol(rawSymbol);
  const isCrypto = isCryptoSymbol(normalizedSymbol);
  const sideLower = String(side || '').toLowerCase();
  const typeLower = String(type || '').toLowerCase();
  const allowedCryptoTypes = new Set(['market', 'limit', 'stop_limit']);
  const finalType = isCrypto && !allowedCryptoTypes.has(typeLower) ? 'market' : (typeLower || 'market');
  const rawTif = String(time_in_force || '').toLowerCase();
  const allowedCryptoTifs = new Set(['gtc', 'ioc', 'fok']);
  const finalTif = isCrypto ? (allowedCryptoTifs.has(rawTif) ? rawTif : 'gtc') : (rawTif || time_in_force);
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
  const payload = {
    symbol: toTradeSymbol(normalizedSymbol),
    side: sideLower,
    type: finalType,
    time_in_force: finalTif,
    limit_price: Number.isFinite(limitPriceNum) ? limitPriceNum : undefined,
    qty: useQty ? finalQty : undefined,
    notional: useNotional ? finalNotional : undefined,
    client_order_id: buildClientOrderId(normalizedSymbol, 'order'),
  };
  logOrderPayload({ label: 'orders_submit', payload });
  let response;
  try {
    response = await requestJson({
      method: 'POST',
      url,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify(payload),
    });
    logOrderResponse({ label: 'orders_submit', payload, response });
  } catch (err) {
    logHttpError({ symbol: normalizedSymbol, label: 'orders', url, error: err });
    logOrderResponse({ label: 'orders_submit', payload, error: err });
    if (isNetworkError(err)) {
      logNetworkError({
        type: 'order',
        symbol: normalizedSymbol,
        attempts: err.attempts ?? 1,
        context: 'submit_order',
      });
    }
    throw err;
  }

  return response;

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
  return list
    .map((order) => ({
      rawSymbol: order.symbol,
      pairSymbol: normalizeSymbol(order.symbol),
      symbol: normalizeSymbol(order.symbol),
      side: order.side,
      status: order.status,
    }))
    .filter((order) => String(order.status || '').toLowerCase() === 'open');
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
