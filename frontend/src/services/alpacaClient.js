import {
  ALPACA_BASE_URL,
  BACKEND_BASE_URL,
  BACKEND_HEADERS,
  DATA_LOCATIONS,
  DATA_ROOT_CRYPTO,
  DATA_ROOT_STOCKS_V2,
  HEADERS,
} from '../config/alpaca';
import { getSettings } from '../state/settingsStore';
import { isoDaysAgo, isFresh, parseTsMs } from '../utils/format';
import { rateLimitedFetch as fetchWithBudget, sleep } from '../utils/network';
import { isStock, toDataSymbol } from '../utils/symbols';

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
  const u = unsupportedSymbols.get(sym);
  if (!u) return false;
  if (Date.now() > u) {
    unsupportedSymbols.delete(sym);
    return false;
  }
  return true;
};

export const markUnsupported = (sym, mins = 120) => {
  unsupportedSymbols.set(sym, Date.now() + mins * 60000);
};

export const clearQuoteCache = () => quoteCache.clear();

const buildURLCrypto = (loc, what, symbolsCSV, params = {}) => {
  const encoded = symbolsCSV.split(',').map((s) => encodeURIComponent(s)).join(',');
  const sp = new URLSearchParams();
  Object.entries(params || {}).forEach(([k, v]) => {
    if (v != null) sp.set(k, v);
  });
  const qs = sp.toString();
  return `${DATA_ROOT_CRYPTO}/${loc}/latest/${what}?symbols=${encoded}${qs ? `&${qs}` : ''}`;
};

export const getPortfolioHistory = async ({ period = '1M', timeframe = '1D' } = {}) => {
  const url = `${ALPACA_BASE_URL}/account/portfolio/history?period=${encodeURIComponent(period)}&timeframe=${encodeURIComponent(timeframe)}&extended_hours=true`;
  const res = await fetchWithBudget(url, { headers: HEADERS });
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

  const url = `${ALPACA_BASE_URL}/account/activities?${params.toString()}`;
  const res = await fetchWithBudget(url, { headers: HEADERS });
  let items = [];
  try {
    items = await res.json();
  } catch (err) {
    log('activities_parse_error', 'ACTIVITIES', { message: err?.message });
  }
  const next = res.headers?.get?.('x-next-page-token') || null;
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
    const res = await fetchWithBudget(`${ALPACA_BASE_URL}/clock`, { headers: HEADERS });
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

export const getCryptoQuotesBatch = async (dsyms = []) => {
  if (!dsyms.length) return new Map();
  const settings = getSettings();
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildURLCrypto(loc, 'quotes', dsyms.join(','));
      const res = await fetchWithBudget(url, { headers: HEADERS });
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        log('quote_http_error', 'QUOTE', { status: res.status, loc, body: body?.slice?.(0, 120) });
        continue;
      }
      const payload = await res.json().catch(() => null);
      const raw = payload?.quotes || {};
      const out = new Map();
      for (const dsym of dsyms) {
        const q = Array.isArray(raw[dsym]) ? raw[dsym][0] : raw[dsym];
        if (!q) continue;
        const bid = Number(q.bp ?? q.bid_price);
        const ask = Number(q.ap ?? q.ask_price);
        const bs = Number(q.bs ?? q.bid_size);
        const as = Number(q.as ?? q.ask_size);
        const tms = parseTsMs(q.t);
        if (bid > 0 && ask > 0) {
          out.set(dsym, {
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
      log('quote_http_error', 'QUOTE', { status: 'exception', loc, body: err?.message || '' });
    }
  }
  return new Map();
};

export const getCryptoTradesBatch = async (dsyms = []) => {
  if (!dsyms.length) return new Map();
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildURLCrypto(loc, 'trades', dsyms.join(','));
      const res = await fetchWithBudget(url, { headers: HEADERS });
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        log('trade_http_error', 'TRADE', { status: res.status, loc, body: body?.slice?.(0, 120) });
        continue;
      }
      const payload = await res.json().catch(() => null);
      const raw = payload?.trades || {};
      const out = new Map();
      for (const dsym of dsyms) {
        const t = Array.isArray(raw[dsym]) ? raw[dsym][0] : raw[dsym];
        const price = Number(t?.p ?? t?.price);
        const tms = parseTsMs(t?.t);
        if (Number.isFinite(price) && price > 0) out.set(dsym, { price, tms });
      }
      if (out.size) return out;
    } catch (err) {
      log('trade_http_error', 'TRADE', { status: 'exception', loc, body: err?.message || '' });
    }
  }
  return new Map();
};

export const getCryptoBars1m = async (symbol, limit = 6) => {
  const dsym = toDataSymbol(symbol);
  const cached = barsCache.get(dsym);
  const now = Date.now();
  if (cached && now - cached.ts < barsCacheTTL) {
    return cached.bars.slice(0, limit);
  }
  const settings = getSettings();
  for (const loc of DATA_LOCATIONS) {
    try {
      const sp = new URLSearchParams({ timeframe: '1Min', limit: String(limit), symbols: dsym });
      const url = `${DATA_ROOT_CRYPTO}/${loc}/bars?${sp.toString()}`;
      const res = await fetchWithBudget(url, { headers: HEADERS });
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        log('quote_http_error', 'BARS', { status: res.status, loc, body: body?.slice?.(0, 120) });
        continue;
      }
      const payload = await res.json().catch(() => null);
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
        barsCache.set(dsym, { ts: now, bars: bars.slice() });
        return bars.slice(0, limit);
      }
    } catch (err) {
      log('quote_http_error', 'BARS', { status: 'exception', loc, body: err?.message || '' });
    }
  }
  return [];
};

export const getCryptoBars1mBatch = async (symbols = [], limit = 6) => {
  const uniqSyms = Array.from(new Set(symbols.filter(Boolean)));
  if (!uniqSyms.length) return new Map();
  const dsymList = uniqSyms.map((s) => toDataSymbol(s));
  const out = new Map();
  const now = Date.now();

  const missing = [];
  for (const dsym of dsymList) {
    const cached = barsCache.get(dsym);
    if (cached && now - cached.ts < barsCacheTTL) {
      out.set(dsym.replace('/', ''), cached.bars.slice(0, limit));
    } else {
      missing.push(dsym);
    }
  }
  if (!missing.length) return out;

  for (const loc of DATA_LOCATIONS) {
    try {
      const sp = new URLSearchParams({ timeframe: '1Min', limit: String(limit), symbols: missing.join(',') });
      const url = `${DATA_ROOT_CRYPTO}/${loc}/bars?${sp.toString()}`;
      const res = await fetchWithBudget(url, { headers: HEADERS });
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        log('quote_http_error', 'BARS', { status: res.status, loc, body: body?.slice?.(0, 120) });
        continue;
      }
      const payload = await res.json().catch(() => null);
      const raw = payload?.bars || {};
      for (const dsym of missing) {
        const arr = raw[dsym];
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
          barsCache.set(dsym, { ts: now, bars: bars.slice() });
          out.set(dsym.replace('/', ''), bars.slice(0, limit));
        }
      }
      break;
    } catch (err) {
      log('quote_http_error', 'BARS', { status: 'exception', loc, body: err?.message || '' });
    }
  }
  return out;
};

export const stocksLatestQuotesBatch = async (symbols = []) => {
  if (!symbols.length) return new Map();
  const csv = symbols.join(',');
  try {
    const res = await fetchWithBudget(`${DATA_ROOT_STOCKS_V2}/quotes/latest?symbols=${encodeURIComponent(csv)}`, {
      headers: HEADERS,
    });
    if (!res.ok) return new Map();
    const body = await res.json().catch(() => null);
    const out = new Map();
    for (const sym of symbols) {
      const qraw = body?.quotes?.[sym];
      const q = Array.isArray(qraw) ? qraw[0] : qraw;
      if (!q) continue;
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
    log('quote_http_error', 'STOCK_QUOTE', { message: err?.message });
    return new Map();
  }
};

export const getQuoteSmart = async (symbol, preloadedMap = null) => {
  try {
    if (isStock(symbol)) {
      markUnsupported(symbol, 60);
      return null;
    }
    if (isUnsupported(symbol)) return null;

    const settings = getSettings();
    const cached = quoteCache.get(symbol);
    if (cached && Date.now() - cached.ts < settings.quoteTtlMs) return cached.q;

    if (preloadedMap && preloadedMap.has(symbol)) {
      const q = preloadedMap.get(symbol);
      if (q && isFresh(q.tms, settings.liveFreshMsCrypto)) return q;
    }

    const dsym = toDataSymbol(symbol);
    const m = await getCryptoQuotesBatch([dsym]);
    const q0 = m.get(dsym);
    if (q0 && isFresh(q0.tms, settings.liveFreshMsCrypto)) {
      const qObj = { bid: q0.bid, ask: q0.ask, bs: q0.bs, as: q0.as, tms: q0.tms };
      quoteCache.set(symbol, { ts: Date.now(), q: qObj });
      return qObj;
    }

    if (!settings.liveRequireQuote) {
      const tm = await getCryptoTradesBatch([dsym]);
      const t = tm.get(dsym);
      if (t && isFresh(t.tms, settings.liveFreshTradeMsCrypto)) {
        const spread = settings.syntheticTradeSpreadBps;
        const price = t.price;
        if (price > 0) {
          const half = price * (spread / 20000);
          const synth = { bid: price - half, ask: price + half, bs: null, as: null, tms: Date.now() };
          quoteCache.set(symbol, { ts: Date.now(), q: synth });
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
  const res = await fetchWithBudget(`${ALPACA_BASE_URL}/account`, { headers: HEADERS });
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
    const res = await fetchWithBudget(`${ALPACA_BASE_URL}/positions`, { headers: HEADERS });
    if (!res.ok) return [];
    const arr = await res.json();
    return Array.isArray(arr) ? arr : [];
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
      headers: BACKEND_HEADERS,
    });
    if (!res.ok) return [];
    const arr = await res.json();
    return Array.isArray(arr) ? arr : [];
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
    const open = await getOpenOrdersCached();
    const targets = (open || []).filter(
      (o) => o.symbol === symbol && (!side || (o.side || '').toLowerCase() === String(side).toLowerCase())
    );
    await Promise.all(
      targets.map((o) =>
        fetchWithBudget(`${BACKEND_BASE_URL}/orders/${o.id}`, {
          method: 'DELETE',
          headers: BACKEND_HEADERS,
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
        fetchWithBudget(`${BACKEND_BASE_URL}/orders/${o.id}`, { method: 'DELETE', headers: BACKEND_HEADERS }).catch(() => null)
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
      const isCryptoSym = /USD$/.test(sym);
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
