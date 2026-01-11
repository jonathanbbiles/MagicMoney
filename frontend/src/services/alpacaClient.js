import {
  BACKEND_BASE_URL,
  DATA_LOCATIONS,
  getBackendHeaders,
} from '../config/alpaca';
import { getSettings } from '../state/settingsStore';
import { isoDaysAgo, isFresh, parseTsMs } from '../utils/format';
import { rateLimitedFetch as fetchWithBudget, sleep } from '../utils/network';
import { isStock, normalizePair, toAlpacaCryptoSymbol, toInternalSymbol } from '../utils/symbols';

let logHandler = () => {};
export const registerAlpacaLogger = (fn) => {
  logHandler = typeof fn === 'function' ? fn : () => {};
};
const log = (type, symbol, details) => {
  try {
    logHandler(type, symbol, details);
  } catch (err) {
    console.warn('Failed to log trade action', err);
  }
};

const MARKET_DATA_TIMEOUT_MS = 9000;
const MARKET_DATA_RETRIES = 3;
const MARKET_DATA_FAILURE_LIMIT = 5;
const MARKET_DATA_COOLDOWN_MS = 60000;
const marketDataState = {
  consecutiveFailures: 0,
  cooldownUntil: 0,
  cooldownLoggedAt: 0,
};

const buildBackendUrl = ({ path, params, label }) => {
  const base = String(BACKEND_BASE_URL || '').replace(/\/+$/, '');
  const cleanPath = String(path || '').replace(/^\/+/, '');
  const url = new URL(`${base}/${cleanPath}`);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value == null) return;
      url.searchParams.set(key, String(value));
    });
  }
  const finalUrl = url.toString();
  console.log('backend_request_url', { label, url: finalUrl });
  return finalUrl;
};

const isMarketDataCooldown = () => Date.now() < marketDataState.cooldownUntil;
const markMarketDataFailure = () => {
  marketDataState.consecutiveFailures += 1;
  if (marketDataState.consecutiveFailures >= MARKET_DATA_FAILURE_LIMIT && !isMarketDataCooldown()) {
    marketDataState.cooldownUntil = Date.now() + MARKET_DATA_COOLDOWN_MS;
    marketDataState.cooldownLoggedAt = Date.now();
    console.warn('DATA DOWN — pausing scans 60s');
  }
};
const markMarketDataSuccess = () => {
  marketDataState.consecutiveFailures = 0;
};

const logMarketDataDiagnostics = ({ type, url, statusCode, snippet, errorType }) => {
  console.log('alpaca_marketdata', {
    type,
    url,
    statusCode,
    errorType,
    snippet,
  });
  log('market_data', type, { url, statusCode, errorType, snippet });
};

const fetchMarketData = async (type, url) => {
  if (isMarketDataCooldown()) {
    logMarketDataDiagnostics({ type, url, statusCode: null, snippet: '', errorType: 'cooldown' });
    const err = new Error('Market data cooldown active');
    err.code = 'COOLDOWN';
    throw err;
  }

  try {
    const headers = getBackendHeaders();
    const res = await fetchWithBudget(url, { headers }, MARKET_DATA_TIMEOUT_MS, MARKET_DATA_RETRIES);
    const status = res?.status;
    if (!res?.ok) {
      const body = await res.text().catch(() => '');
      logMarketDataDiagnostics({
        type,
        url,
        statusCode: status,
        errorType: 'http',
        snippet: body?.slice?.(0, 200),
      });
      markMarketDataFailure();
      const err = new Error(`HTTP ${status}`);
      err.code = 'HTTP_ERROR';
      err.statusCode = status;
      err.responseSnippet = body?.slice?.(0, 200);
      throw err;
    }
    const text = await res.text().catch(() => '');
    logMarketDataDiagnostics({ type, url, statusCode: status, errorType: 'ok', snippet: '' });
    markMarketDataSuccess();
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch (err) {
      logMarketDataDiagnostics({ type, url, statusCode: status, errorType: 'parse', snippet: text.slice(0, 200) });
      markMarketDataFailure();
      const parseErr = new Error('parse_error');
      parseErr.code = 'PARSE_ERROR';
      throw parseErr;
    }
  } catch (err) {
    if (err?.name === 'AbortError' || err?.message?.includes?.('Network') || err?.message?.includes?.('fetch')) {
      logMarketDataDiagnostics({ type, url, statusCode: null, errorType: 'network', snippet: err?.message || '' });
      markMarketDataFailure();
    }
    throw err;
  }
};

const quoteCache = new Map();
const unsupportedSymbols = new Map();
const barsCache = new Map();
const barsCacheTTL = 30000;

const STOCK_CLOCK_CACHE = { value: { is_open: false }, ts: 0 };
let openOrdersCache = { ts: 0, items: [] };
let positionsCache = { ts: 0, items: [] };

export const invalidateOpenOrdersCache = () => { openOrdersCache = { ts: 0, items: [] }; };
export const invalidatePositionsCache = () => { positionsCache = { ts: 0, items: [] }; };

const isUnsupported = (sym) => {
  const normalized = toInternalSymbol(sym);
  const u = unsupportedSymbols.get(normalized);
  if (!u) return false;
  if (Date.now() > u) {
    unsupportedSymbols.delete(normalized);
    return false;
  }
  return true;
};

export const markUnsupported = (sym, mins = 120) => {
  const normalized = toInternalSymbol(sym);
  if (!normalized) return;
  unsupportedSymbols.set(normalized, Date.now() + mins * 60000);
};

export const clearQuoteCache = () => quoteCache.clear();

const buildURLCrypto = (loc, what, symbols = [], params = {}) => {
  const normalized = symbols.map((s) => toAlpacaCryptoSymbol(s)).join(',');
  return buildBackendUrl({
    path: `market/crypto/${what}`,
    params: { symbols: normalized, location: loc, ...params },
    label: `crypto_${what}`,
  });
};

export const getPortfolioHistory = async ({ period = '1M', timeframe = '1D' } = {}) => {
  const url = buildBackendUrl({
    path: 'account/portfolio/history',
    params: { period, timeframe, extended_hours: true },
    label: 'portfolio_history',
  });
  const headers = getBackendHeaders();
  const res = await fetchWithBudget(url, { headers });
  if (!res.ok) return null;
  return res.json().catch(() => null);
};

export const getActivities = async ({ afterISO, untilISO, pageToken, types } = {}) => {
  const params = new URLSearchParams({
    activity_types: types || 'FILL,CFEE,FEE,PTC',
    direction: 'desc',
    page_size: '100',
  });
  if (afterISO) params.set('after', afterISO);
  if (untilISO) params.set('until', untilISO);
  if (pageToken) params.set('page_token', pageToken);

  const url = buildBackendUrl({
    path: 'account/activities',
    params: Object.fromEntries(params.entries()),
    label: 'activities',
  });
  const headers = getBackendHeaders();
  const res = await fetchWithBudget(url, { headers });
  let items = [];
  let next = res.headers?.get?.('x-next-page-token') || null;
  try {
    const body = await res.json();
    items = body?.items ?? body;
    if (!next) {
      next = body?.nextPageToken ?? null;
    }
  } catch (err) {
    log('activities_parse_error', 'ACTIVITIES', { message: err?.message });
  }
  return { items: Array.isArray(items) ? items : [], next };
};

export const getPnLAndFeesSnapshot = async () => {
  const hist1M = await getPortfolioHistory({ period: '1M', timeframe: '1D' });
  let last7Sum = null;
  let last7DownDays = null;
  let last7UpDays = null;
  let last30Sum = null;
  if (hist1M?.profit_loss) {
    const pl = hist1M.profit_loss.map(Number).filter(Number.isFinite);
    const last7 = pl.slice(-7);
    const last30 = pl.slice(-30);
    last7Sum = last7.reduce((a, b) => a + b, 0);
    last30Sum = last30.reduce((a, b) => a + b, 0);
    last7UpDays = last7.filter((x) => x > 0).length;
    last7DownDays = last7.filter((x) => x < 0).length;
  }

  let fees30 = 0;
  let fillsCount30 = 0;
  const afterISO = isoDaysAgo(30);
  const untilISO = new Date().toISOString();
  let token = null;
  for (let i = 0; i < 10; i++) {
    const { items, next } = await getActivities({ afterISO, untilISO, pageToken: token });
    for (const it of items) {
      const t = (it?.activity_type || it?.activityType || '').toUpperCase();
      if (t === 'CFEE' || t === 'FEE' || t === 'PTC') {
        const raw =
          it.net_amount ??
          it.amount ??
          it.price ??
          (Number(it.per_share_amount) * Number(it.qty) || NaN);
        const amt = Number(raw);
        if (Number.isFinite(amt)) fees30 += amt;
      } else if (t === 'FILL') fillsCount30 += 1;
    }
    if (!next) break;
    token = next;
  }
  return { last7Sum, last7UpDays, last7DownDays, last30Sum, fees30, fillsCount30 };
};

export const getStockClock = async () => {
  try {
    const headers = getBackendHeaders();
    const res = await fetchWithBudget(`${BACKEND_BASE_URL}/clock`, { headers });
    if (!res.ok) return { is_open: false };
    const body = await res.json();
    return { is_open: !!body.is_open, next_open: body.next_open, next_close: body.next_close };
  } catch (err) {
    log('clock_error', 'CLOCK', { message: err?.message });
    return { is_open: false };
  }
};

export const getStockClockCached = async (ttlMs = 30000) => {
  const now = Date.now();
  if (now - STOCK_CLOCK_CACHE.ts < ttlMs) return STOCK_CLOCK_CACHE.value;
  const v = await getStockClock();
  STOCK_CLOCK_CACHE.value = v;
  STOCK_CLOCK_CACHE.ts = now;
  return v;
};

export const getCryptoQuotesBatch = async (symbols = []) => {
  const internalSymbols = symbols.map((s) => toInternalSymbol(s)).filter(Boolean);
  if (!internalSymbols.length) return new Map();
  const dataSymbols = internalSymbols.map((s) => toAlpacaCryptoSymbol(s));
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildURLCrypto(loc, 'quotes', dataSymbols);
      const payload = await fetchMarketData('QUOTE', url);
      const raw = payload?.quotes || {};
      const out = new Map();
      for (const symbol of internalSymbols) {
        const dataSymbol = toAlpacaCryptoSymbol(symbol);
        const q = Array.isArray(raw[dataSymbol]) ? raw[dataSymbol][0] : raw[dataSymbol];
        if (!q) {
          log('no_quote', symbol, { reason: 'no_data', requestType: 'QUOTE' });
          continue;
        }
        const bid = Number(q.bp ?? q.bid_price);
        const ask = Number(q.ap ?? q.ask_price);
        const bs = Number(q.bs ?? q.bid_size);
        const as = Number(q.as ?? q.ask_size);
        const tms = parseTsMs(q.t);
        if (bid > 0 && ask > 0) {
          out.set(symbol, {
            bid,
            ask,
            bs: Number.isFinite(bs) ? bs : null,
            as: Number.isFinite(as) ? as : null,
            tms,
          });
        }
      }
      if (out.size) return out;
    } catch (err) {
      log('quote_http_error', 'QUOTE', { status: err?.statusCode || err?.code || 'exception', loc, body: err?.responseSnippet || err?.message || '' });
      for (const symbol of internalSymbols) {
        log('no_quote', symbol, { reason: err?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'QUOTE' });
      }
    }
  }
  return new Map();
};

export const getCryptoTradesBatch = async (symbols = []) => {
  const internalSymbols = symbols.map((s) => toInternalSymbol(s)).filter(Boolean);
  if (!internalSymbols.length) return new Map();
  const dataSymbols = internalSymbols.map((s) => toAlpacaCryptoSymbol(s));
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildURLCrypto(loc, 'trades', dataSymbols);
      const payload = await fetchMarketData('TRADE', url);
      const raw = payload?.trades || {};
      const out = new Map();
      for (const symbol of internalSymbols) {
        const dataSymbol = toAlpacaCryptoSymbol(symbol);
        const t = Array.isArray(raw[dataSymbol]) ? raw[dataSymbol][0] : raw[dataSymbol];
        const price = Number(t?.p ?? t?.price);
        const tms = parseTsMs(t?.t);
        if (Number.isFinite(price) && price > 0) out.set(symbol, { price, tms });
      }
      if (out.size) return out;
    } catch (err) {
      log('trade_http_error', 'TRADE', { status: err?.statusCode || err?.code || 'exception', loc, body: err?.responseSnippet || err?.message || '' });
      for (const symbol of internalSymbols) {
        log('no_quote', symbol, { reason: err?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'TRADE' });
      }
    }
  }
  return new Map();
};

export const getCryptoBars1m = async (symbol, limit = 6) => {
  const internalSymbol = toInternalSymbol(symbol);
  const dsym = toAlpacaCryptoSymbol(internalSymbol);
  const cached = barsCache.get(internalSymbol);
  const now = Date.now();
  if (cached && now - cached.ts < barsCacheTTL) {
    return cached.bars.slice(0, limit);
  }
  const settings = getSettings();
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildBackendUrl({
        path: 'market/crypto/bars',
        params: { timeframe: '1Min', limit: String(limit), symbols: dsym, location: loc },
        label: 'crypto_bars',
      });
      const payload = await fetchMarketData('BARS', url);
      const arr = payload?.bars?.[dsym];
      if (Array.isArray(arr) && arr.length) {
        const bars = arr
          .map((b) => ({
            open: Number(b.o ?? b.open),
            high: Number(b.h ?? b.high),
            low: Number(b.l ?? b.low),
            close: Number(b.c ?? b.close),
            vol: Number(b.v ?? b.volume ?? 0),
            tms: parseTsMs(b.t),
          }))
          .filter((x) => Number.isFinite(x.close) && x.close > 0);
        barsCache.set(internalSymbol, { ts: now, bars: bars.slice() });
        return bars.slice(0, limit);
      }
      log('no_quote', internalSymbol, { reason: 'no_data', requestType: 'BARS' });
    } catch (err) {
      log('quote_http_error', 'BARS', { status: err?.statusCode || err?.code || 'exception', loc, body: err?.responseSnippet || err?.message || '' });
      log('no_quote', internalSymbol, { reason: err?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'BARS' });
    }
  }
  return [];
};

export const getCryptoBars1mBatch = async (symbols = [], limit = 6) => {
  const uniqSyms = Array.from(new Set(symbols.map((s) => toInternalSymbol(s)).filter(Boolean)));
  if (!uniqSyms.length) return new Map();
  const dsymList = uniqSyms.map((s) => toAlpacaCryptoSymbol(s));
  const out = new Map();
  const now = Date.now();

  const missing = [];
  for (const sym of uniqSyms) {
    const cached = barsCache.get(sym);
    if (cached && now - cached.ts < barsCacheTTL) {
      out.set(sym, cached.bars.slice(0, limit));
    } else {
      missing.push(sym);
    }
  }
  if (!missing.length) return out;

  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildBackendUrl({
        path: 'market/crypto/bars',
        params: { timeframe: '1Min', limit: String(limit), symbols: missing.map((s) => toAlpacaCryptoSymbol(s)).join(','), location: loc },
        label: 'crypto_bars_batch',
      });
      const payload = await fetchMarketData('BARS', url);
      const raw = payload?.bars || {};
      for (const sym of missing) {
        const dataSymbol = toAlpacaCryptoSymbol(sym);
        const arr = raw[dataSymbol];
        if (Array.isArray(arr) && arr.length) {
          const bars = arr
            .map((b) => ({
              open: Number(b.o ?? b.open),
              high: Number(b.h ?? b.high),
              low: Number(b.l ?? b.low),
              close: Number(b.c ?? b.close),
              vol: Number(b.v ?? b.volume ?? 0),
              tms: parseTsMs(b.t),
            }))
            .filter((x) => Number.isFinite(x.close) && x.close > 0);
          barsCache.set(sym, { ts: now, bars: bars.slice() });
          out.set(sym, bars.slice(0, limit));
        } else {
          log('no_quote', sym, { reason: 'no_data', requestType: 'BARS' });
        }
      }
      break;
    } catch (err) {
      log('quote_http_error', 'BARS', { status: err?.statusCode || err?.code || 'exception', loc, body: err?.responseSnippet || err?.message || '' });
      for (const sym of missing) {
        log('no_quote', sym, { reason: err?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'BARS' });
      }
    }
  }
  return out;
};

export const stocksLatestQuotesBatch = async (symbols = []) => {
  if (!symbols.length) return new Map();
  const csv = symbols.join(',');
  try {
    const url = buildBackendUrl({
      path: 'market/stocks/quotes',
      params: { symbols: csv },
      label: 'stocks_latest_quotes',
    });
    const body = await fetchMarketData('QUOTE', url);
    const out = new Map();
    for (const sym of symbols) {
      const qraw = body?.quotes?.[sym];
      const q = Array.isArray(qraw) ? qraw[0] : qraw;
      if (!q) {
        log('no_quote', sym, { reason: 'no_data', requestType: 'QUOTE' });
        continue;
      }
      const bid = Number(q.bp ?? q.bid_price);
      const ask = Number(q.ap ?? q.ask_price);
      const bs = Number(q.bs ?? q.bid_size);
      const as = Number(q.as ?? q.ask_size);
      const tms = parseTsMs(q.t);
      if (bid > 0 && ask > 0) {
        out.set(sym, {
          bid,
          ask,
          bs: Number.isFinite(bs) ? bs : null,
          as: Number.isFinite(as) ? as : null,
          tms,
        });
      }
    }
    return out;
  } catch (err) {
    log('quote_http_error', 'STOCK_QUOTE', { status: err?.statusCode || err?.code || 'exception', body: err?.responseSnippet || err?.message || '' });
    for (const sym of symbols) {
      log('no_quote', sym, { reason: err?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'QUOTE' });
    }
    return new Map();
  }
};

export const getQuoteSmart = async (symbol, preloadedMap = null) => {
  try {
    const normalizedSymbol = toInternalSymbol(symbol);
    if (isStock(normalizedSymbol)) {
      markUnsupported(normalizedSymbol, 60);
      return null;
    }
    if (isUnsupported(normalizedSymbol)) return null;

    const settings = getSettings();
    const cached = quoteCache.get(normalizedSymbol);
    if (cached && Date.now() - cached.ts < settings.quoteTtlMs) return cached.q;

    if (preloadedMap && preloadedMap.has(normalizedSymbol)) {
      const q = preloadedMap.get(normalizedSymbol);
      if (q && isFresh(q.tms, settings.liveFreshMsCrypto)) return q;
    }

    const m = await getCryptoQuotesBatch([normalizedSymbol]);
    const q0 = m.get(normalizedSymbol);
    if (q0 && isFresh(q0.tms, settings.liveFreshMsCrypto)) {
      const qObj = { bid: q0.bid, ask: q0.ask, bs: q0.bs, as: q0.as, tms: q0.tms };
      quoteCache.set(normalizedSymbol, { ts: Date.now(), q: qObj });
      return qObj;
    }

    if (!settings.liveRequireQuote) {
      const tm = await getCryptoTradesBatch([normalizedSymbol]);
      const t = tm.get(normalizedSymbol);
      if (t && isFresh(t.tms, settings.liveFreshTradeMsCrypto)) {
        const spread = settings.syntheticTradeSpreadBps;
        const price = t.price;
        if (price > 0) {
          const half = price * (spread / 20000);
          const synth = { bid: price - half, ask: price + half, bs: null, as: null, tms: Date.now() };
          quoteCache.set(normalizedSymbol, { ts: Date.now(), q: synth });
          return synth;
        }
      }
    }
    return null;
  } catch (err) {
    log('quote_exception', symbol, { error: err?.message });
    return null;
  }
};

export const getAccountSummaryRaw = async () => {
  const headers = getBackendHeaders();
  const res = await fetchWithBudget(`${BACKEND_BASE_URL}/account`, { headers });
  if (!res.ok) throw new Error(`Account ${res.status}`);
  const body = await res.json();
  const num = (x) => {
    const n = parseFloat(x);
    return Number.isFinite(n) ? n : NaN;
  };

  const equity = num(body.equity ?? body.portfolio_value);
  const stockBP = num(body.buying_power);
  const cryptoBP = num(body.crypto_buying_power);
  const nmbp = num(body.non_marginable_buying_power);
  const cash = num(body.cash);
  const dtbp = num(body.daytrade_buying_power);

  const cashish = Number.isFinite(nmbp)
    ? nmbp
    : Number.isFinite(cash)
    ? cash
    : Number.isFinite(cryptoBP)
    ? cryptoBP
    : NaN;

  const buyingPowerDisplay = Number.isFinite(cashish) ? cashish : stockBP;

  const prevClose = num(body.equity_previous_close);
  const lastEq = num(body.last_equity);
  const ref = Number.isFinite(prevClose) ? prevClose : lastEq;
  const changeUsd = Number.isFinite(equity) && Number.isFinite(ref) ? equity - ref : NaN;
  const changePct = Number.isFinite(changeUsd) && ref > 0 ? (changeUsd / ref) * 100 : NaN;

  const patternDayTrader = !!body.pattern_day_trader;
  const daytradeCount = Number.isFinite(+body.daytrade_count) ? +body.daytrade_count : null;

  return {
    equity,
    buyingPower: buyingPowerDisplay,
    changeUsd,
    changePct,
    patternDayTrader,
    daytradeCount,
    cryptoBuyingPower: cashish,
    stockBuyingPower: Number.isFinite(stockBP) ? stockBP : cashish,
    daytradeBuyingPower: dtbp,
    cash,
  };
};

export const getAllPositions = async () => {
  try {
    const headers = getBackendHeaders();
    const res = await fetchWithBudget(`${BACKEND_BASE_URL}/positions`, { headers });
    if (!res.ok) return [];
    const arr = await res.json();
    return Array.isArray(arr)
      ? arr.map((pos) => ({
        ...pos,
        rawSymbol: pos.rawSymbol ?? pos.symbol,
        pairSymbol: normalizePair(pos.rawSymbol ?? pos.symbol),
        symbol: normalizePair(pos.rawSymbol ?? pos.symbol),
      }))
      : [];
  } catch (err) {
    log('positions_error', 'POSITIONS', { message: err?.message });
    return [];
  }
};

export const getAllPositionsCached = async (ttlMs = 2000) => {
  const now = Date.now();
  if (now - positionsCache.ts < ttlMs) return positionsCache.items.slice();
  const items = await getAllPositions();
  positionsCache = { ts: now, items };
  return items.slice();
};

export const getOpenOrders = async () => {
  try {
    const res = await fetchWithBudget(`${BACKEND_BASE_URL}/orders?status=open&nested=true&limit=100`, {
      headers: getBackendHeaders(),
    });
    if (!res.ok) return [];
    const arr = await res.json();
    return Array.isArray(arr)
      ? arr.map((order) => ({
        ...order,
        rawSymbol: order.rawSymbol ?? order.symbol,
        pairSymbol: normalizePair(order.rawSymbol ?? order.symbol),
        symbol: normalizePair(order.rawSymbol ?? order.symbol),
      }))
      : [];
  } catch (err) {
    log('orders_error', 'ORDERS', { message: err?.message });
    return [];
  }
};

export const getOpenOrdersCached = async (ttlMs = 2000) => {
  const now = Date.now();
  if (now - openOrdersCache.ts < ttlMs) return openOrdersCache.items.slice();
  const items = await getOpenOrders();
  openOrdersCache = { ts: now, items };
  return items.slice();
};

export const cancelOpenOrdersForSymbol = async (symbol, side = null) => {
  try {
    const normalizedSymbol = normalizePair(symbol);
    const open = await getOpenOrdersCached();
    const targets = (open || []).filter(
      (o) =>
        normalizePair(o.symbol) === normalizedSymbol &&
        (!side || (o.side || '').toLowerCase() === String(side).toLowerCase())
    );
    await Promise.all(
      targets.map((o) =>
        fetchWithBudget(`${BACKEND_BASE_URL}/orders/${o.id}`, {
          method: 'DELETE',
          headers: getBackendHeaders(),
        }).catch(() => null)
      )
    );
    openOrdersCache = { ts: 0, items: [] };
  } catch (err) {
    log('order_cancel_error', symbol, { message: err?.message });
  }
};

export const cancelAllOrders = async () => {
  try {
    const orders = await getOpenOrdersCached();
    await Promise.all(
      (orders || []).map((o) =>
        fetchWithBudget(`${BACKEND_BASE_URL}/orders/${o.id}`, { method: 'DELETE', headers: getBackendHeaders() }).catch(() => null)
      )
    );
    openOrdersCache = { ts: 0, items: [] };
  } catch (err) {
    log('order_cancel_error', 'ALL', { message: err?.message });
  }
};

export const getUsableBuyingPower = async ({ forCrypto = true } = {}) => {
  const settings = getSettings();
  const account = await getAccountSummaryRaw();
  let base = forCrypto
    ? Number.isFinite(account.cryptoBuyingPower)
      ? account.cryptoBuyingPower
      : Number.isFinite(account.cash)
      ? account.cash
      : account.buyingPower
    : Number.isFinite(account.stockBuyingPower)
    ? account.stockBuyingPower
    : account.buyingPower;
  base = Number.isFinite(base) ? base : 0;

  let pending = 0;
  try {
    const open = await getOpenOrdersCached();
    for (const o of open || []) {
      const side = String(o.side || '').toLowerCase();
      if (side !== 'buy') continue;
      const sym = o.symbol || '';
      const isCryptoSym = /USD$/.test(normalizePair(sym) || '');
      if (forCrypto !== isCryptoSym) continue;
      const qty = +o.qty || +o.quantity || NaN;
      const lim = +o.limit_price || +o.limitPrice || NaN;
      const notional = +o.notional || NaN;
      if (Number.isFinite(notional) && notional > 0) {
        pending += notional;
      } else if (Number.isFinite(qty) && qty > 0 && Number.isFinite(lim) && lim > 0) {
        pending += qty * lim;
      }
    }
  } catch (err) {
    log('bp_pending_error', 'ORDERS', { message: err?.message });
  }

  const usable = Math.max(0, base - pending);
  return { usable, base, pending, snapshot: account, settings };
};

export const wait = sleep;
