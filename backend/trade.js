const { randomUUID } = require('crypto');

const { httpJson } = require('./httpClient');

const RAW_TRADE_BASE = process.env.TRADE_BASE || process.env.ALPACA_API_BASE || 'https://api.alpaca.markets';
const RAW_DATA_BASE = process.env.DATA_BASE || 'https://data.alpaca.markets';

function normalizeTradeBase(baseUrl) {
  if (!baseUrl) return 'https://api.alpaca.markets';
  const trimmed = baseUrl.replace(/\/+$/, '');
  return trimmed.replace(/\/v2$/, '');
}

function normalizeDataBase(baseUrl) {
  if (!baseUrl) return 'https://data.alpaca.markets';
  let trimmed = baseUrl.replace(/\/+$/, '');
  trimmed = trimmed.replace(/\/v1beta2$/, '');
  trimmed = trimmed.replace(/\/v2\/stocks$/, '');
  trimmed = trimmed.replace(/\/v2$/, '');
  return trimmed;
}

const TRADE_BASE = normalizeTradeBase(RAW_TRADE_BASE);
const DATA_BASE = normalizeDataBase(RAW_DATA_BASE);
const ALPACA_BASE_URL = `${TRADE_BASE}/v2`;
const DATA_URL = `${DATA_BASE}/v1beta2`;
const STOCKS_DATA_URL = `${DATA_BASE}/v2/stocks`;

const resolvedAlpacaAuth = (() => {
  const envStatus = {
    ALPACA_KEY_ID: Boolean(process.env.ALPACA_KEY_ID),
    ALPACA_SECRET_KEY: Boolean(process.env.ALPACA_SECRET_KEY),
    APCA_API_KEY_ID: Boolean(process.env.APCA_API_KEY_ID),
    APCA_API_SECRET_KEY: Boolean(process.env.APCA_API_SECRET_KEY),
    ALPACA_API_KEY: Boolean(process.env.ALPACA_API_KEY),
  };
  console.log('alpaca_auth_env', envStatus);
  const keyId =
    process.env.ALPACA_KEY_ID ||
    process.env.APCA_API_KEY_ID ||
    process.env.ALPACA_API_KEY ||
    '';
  const secretKey = process.env.ALPACA_SECRET_KEY || process.env.APCA_API_SECRET_KEY || '';
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
  const headers = {};
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
    ...alpacaHeaders(),
  };
}

 

const MIN_ORDER_NOTIONAL_USD = Number(process.env.MIN_ORDER_NOTIONAL_USD || 15);

const USER_MIN_PROFIT_BPS = Number(process.env.USER_MIN_PROFIT_BPS || 5);

const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS || 10);

const BUFFER_BPS = Number(process.env.BUFFER_BPS || 15);

const MAX_HOLD_SECONDS = Number(process.env.MAX_HOLD_SECONDS || 300);

const REPRICE_EVERY_SECONDS = Number(process.env.REPRICE_EVERY_SECONDS || 20);

const FORCE_EXIT_SECONDS = Number(process.env.FORCE_EXIT_SECONDS || 600);

const PRICE_TICK = Number(process.env.PRICE_TICK || 0.01);
const MAX_CONCURRENT_POSITIONS = Number(process.env.MAX_CONCURRENT_POSITIONS || 8);
const MIN_POSITION_QTY = Number(process.env.MIN_POSITION_QTY || 1e-6);
const QUOTE_CACHE_MAX_AGE_SECONDS = 60;

const inventoryState = new Map();

const exitState = new Map();

const cfeeCache = { ts: 0, items: [] };
const quoteCache = new Map();
const lastQuoteAt = new Map();
const scanState = { lastScanAt: null };
let lastHttpError = null;

 

function sleep(ms) {

  return new Promise((resolve) => setTimeout(resolve, ms));

}

function logSkip(reason, details = {}) {

  console.log(`Skip — ${reason}`, details);

}

function logHttpError({ symbol, phase, url, error }) {
  const statusCode = error?.statusCode ?? null;
  const errorMessage = error?.errorMessage || error?.message || 'Unknown error';
  const snippet = error?.responseSnippet200 || '';
  const method = error?.method || null;
  console.error('alpaca_http_error', {
    symbol,
    phase,
    method,
    url,
    statusCode,
    errorMessage,
    snippet,
  });
  if (statusCode === 401 || statusCode === 403) {
    console.error('AUTH_ERROR: check Render env vars');
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

function parseQuoteTimestamp(quote) {
  const raw = quote?.t ?? quote?.timestamp ?? quote?.time ?? quote?.ts;
  if (!raw) return null;
  const parsed = new Date(raw).getTime();
  return Number.isFinite(parsed) ? parsed : null;
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

function isDustQty(qty) {
  return Number.isFinite(qty) && Math.abs(qty) <= MIN_POSITION_QTY;
}

function normalizeSymbol(rawSymbol) {

  if (!rawSymbol) return rawSymbol;

  const symbol = String(rawSymbol).trim().toUpperCase();

  return symbol.includes('/') ? symbol.replace(/\//g, '') : symbol;

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
  const normalized = normalizeSymbol(symbol) || 'UNKNOWN';
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
  const url = `${ALPACA_BASE_URL}/positions`;
  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ phase: 'positions', url, error: err });
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

  const url = buildUrlWithParams(`${ALPACA_BASE_URL}/account/activities`, {
    activity_types: 'CFEE',
    direction: 'desc',
    page_size: String(limit),
  });
  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ phase: 'orders', url, error: err });
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

  // submit the limit buy order

  const buyOrderUrl = `${ALPACA_BASE_URL}/orders`;
  let buyOrder;
  try {
    buyOrder = await requestJson({
      method: 'POST',
      url: buyOrderUrl,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify({
        symbol: normalizedSymbol,
        qty,
        side: 'buy',
        type: 'limit',
        // crypto orders must be GTC
        time_in_force: 'gtc',
        limit_price: limitPrice,
        client_order_id: buildClientOrderId(normalizedSymbol, 'limit-buy'),
      }),
    });
  } catch (err) {
    logHttpError({
      symbol: normalizedSymbol,
      phase: 'order',
      url: buyOrderUrl,
      error: err,
    });
    throw err;
  }

 

  // poll until the order is filled

  let filledOrder = buyOrder;

  for (let i = 0; i < 20; i++) {

    const checkUrl = `${ALPACA_BASE_URL}/orders/${buyOrder.id}`;
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
        phase: 'orders',
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

  return /USD$/.test(symbol);

}

async function getLatestPrice(symbol) {

  if (isCryptoSymbol(symbol)) {
    const url = `${DATA_URL}/crypto/latest/trades?symbols=${symbol}`;
    let res;
    try {
      res = await requestJson({
        method: 'GET',
        url,
        headers: alpacaHeaders(),
      });
    } catch (err) {
      logHttpError({ symbol, phase: 'quote', url, error: err });
      throw err;
    }

    const trade = res.trades && res.trades[symbol];

    if (!trade) throw new Error(`Price not available for ${symbol}`);

    return parseFloat(trade.p);

  }

  const url = `${STOCKS_DATA_URL}/trades/latest?symbols=${encodeURIComponent(symbol)}`;
  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ symbol, phase: 'quote', url, error: err });
    throw err;
  }

  const trade = res.trades && res.trades[symbol];

  if (!trade) throw new Error(`Price not available for ${symbol}`);

  return parseFloat(trade.p ?? trade.price);

}

 

// Get portfolio value and buying power from the Alpaca account

async function getAccountInfo() {
  const url = `${ALPACA_BASE_URL}/account`;
  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ phase: 'orders', url, error: err });
    throw err;
  }

  const portfolioValue = parseFloat(res.portfolio_value);

  const buyingPower = parseFloat(res.buying_power);

  return {

    portfolioValue: isNaN(portfolioValue) ? 0 : portfolioValue,

    buyingPower: isNaN(buyingPower) ? 0 : buyingPower,

  };

}

 

// Round quantities to Alpaca's supported crypto precision

function roundQty(qty) {

  return parseFloat(Number(qty).toFixed(8));

}

 

// Round prices to two decimals

function roundPrice(price) {

  return parseFloat(Number(price).toFixed(2));

}

async function getLatestQuote(rawSymbol) {

  const symbol = normalizeSymbol(rawSymbol);

  const now = Date.now();
  const cached = quoteCache.get(symbol);
  let staleCache = false;
  const cachedTsMs = cached && Number.isFinite(cached.tsMs) ? cached.tsMs : null;
  if (cached && Number.isFinite(cached.tsMs)) {
    const ageSeconds = (now - cached.tsMs) / 1000;
    if (ageSeconds <= QUOTE_CACHE_MAX_AGE_SECONDS) {
      recordLastQuoteAt(symbol, { tsMs: cached.tsMs, source: 'cache' });
      return {
        bid: cached.bid,
        ask: cached.ask,
        tsMs: cached.tsMs,
      };
    }
    staleCache = true;
  } else if (cached) {
    staleCache = true;
  }

  if (staleCache) {
    quoteCache.delete(symbol);
  }

  const isCrypto = isCryptoSymbol(symbol);
  const url = isCrypto
    ? `${DATA_URL}/crypto/latest/quotes?symbols=${symbol}`
    : `${STOCKS_DATA_URL}/quotes/latest?symbols=${encodeURIComponent(symbol)}`;

  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ symbol, phase: 'quote', url, error: err });
    if (err?.errorCode === 'COOLDOWN') {
      logSkip('no_quote', { symbol, reason: 'cooldown' });
    } else {
      logSkip('no_quote', { symbol, reason: 'api_error' });
    }
    recordLastQuoteAt(symbol, { tsMs: cachedTsMs, source: 'error', reason: 'api_error' });
    throw err;
  }

  const quote = res.quotes && res.quotes[symbol];
  if (!quote) {
    const reason = staleCache ? 'stale_cache' : 'no_data';
    logSkip('no_quote', { symbol, reason });
    recordLastQuoteAt(symbol, {
      tsMs: staleCache ? cachedTsMs : null,
      source: staleCache ? 'stale' : 'error',
      reason,
    });
    throw new Error(`Quote not available for ${symbol}`);
  }

  const tsMs = parseQuoteTimestamp(quote);
  if (!Number.isFinite(tsMs)) {
    recordLastQuoteAt(symbol, { tsMs: null, source: 'error', reason: 'parse_error' });
    logSkip('no_quote', { symbol, reason: 'parse_error' });
    throw new Error(`Quote timestamp missing for ${symbol}`);
  }

  const bid = Number(quote.bp ?? quote.bid_price ?? quote.bid);
  const ask = Number(quote.ap ?? quote.ask_price ?? quote.ask);
  const normalizedQuote = {
    bid: Number.isFinite(bid) ? bid : null,
    ask: Number.isFinite(ask) ? ask : null,
    tsMs,
  };
  quoteCache.set(symbol, normalizedQuote);
  recordLastQuoteAt(symbol, { tsMs, source: 'fresh' });
  return normalizedQuote;

}

async function fetchOrderById(orderId) {
  const url = `${ALPACA_BASE_URL}/orders/${orderId}`;
  let response;
  try {
    response = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ phase: 'orders', url, error: err });
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

  const url = `${ALPACA_BASE_URL}/orders`;
  let response;
  try {
    response = await requestJson({
      method: 'POST',
      url,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify({
        symbol,
        qty,
        side: 'sell',
        type: 'limit',
        time_in_force: 'gtc',
        limit_price: limitPrice,
        client_order_id: buildClientOrderId(symbol, 'limit-sell'),
      }),
    });
  } catch (err) {
    logHttpError({ symbol, phase: 'order', url, error: err });
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

  const url = `${ALPACA_BASE_URL}/orders`;
  let response;
  try {
    response = await requestJson({
      method: 'POST',
      url,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify({
        symbol,
        qty,
        side: 'sell',
        type: 'market',
        time_in_force: 'gtc',
        client_order_id: buildClientOrderId(symbol, 'market-sell'),
      }),
    });
  } catch (err) {
    logHttpError({ symbol, phase: 'order', url, error: err });
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

  const targetPrice = roundPrice(entryPriceNum * (1 + minNetProfitBps / 10000));

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

    try {

      const quote = await getLatestQuote(symbol);

      bid = quote.bid;

      ask = quote.ask;

    } catch (err) {

      console.warn('quote_fetch_failed', { symbol, error: err?.message || err });

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

            const newLimit = roundPrice(Math.max(state.targetPrice, ask - PRICE_TICK));

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

 

  const buyOrderUrl = `${ALPACA_BASE_URL}/orders`;
  let buyOrder;
  try {
    buyOrder = await requestJson({
      method: 'POST',
      url: buyOrderUrl,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify({
        symbol: normalizedSymbol,
        qty,
        side: 'buy',
        type: 'market',
        time_in_force: 'gtc',
        client_order_id: buildClientOrderId(normalizedSymbol, 'market-buy'),
      }),
    });
  } catch (err) {
    logHttpError({
      symbol: normalizedSymbol,
      phase: 'order',
      url: buyOrderUrl,
      error: err,
    });
    throw err;
  }

 

  // Wait for fill

  let filled = buyOrder;

  for (let i = 0; i < 20; i++) {

    const checkUrl = `${ALPACA_BASE_URL}/orders/${buyOrder.id}`;
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
        phase: 'orders',
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

  const sideLower = String(side || '').toLowerCase();

  let computedNotionalUsd = Number(notional);

  if (!Number.isFinite(computedNotionalUsd) || computedNotionalUsd <= 0) {

    const qtyNum = Number(qty);

    const limitPriceNum = Number(limit_price);

    if (Number.isFinite(qtyNum) && qtyNum > 0 && Number.isFinite(limitPriceNum) && limitPriceNum > 0) {

      computedNotionalUsd = qtyNum * limitPriceNum;

    } else if (Number.isFinite(qtyNum) && qtyNum > 0 && sideLower === 'buy') {

      const price = await getLatestPrice(normalizedSymbol);

      computedNotionalUsd = qtyNum * price;

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

  const url = `${ALPACA_BASE_URL}/orders`;
  let response;
  try {
    response = await requestJson({
      method: 'POST',
      url,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify({
        symbol: normalizedSymbol,
        qty,
        side,
        type,
        time_in_force,
        limit_price,
        notional,
        client_order_id: buildClientOrderId(normalizedSymbol, 'order'),
      }),
    });
  } catch (err) {
    logHttpError({ symbol: normalizedSymbol, phase: 'order', url, error: err });
    throw err;
  }

  return response;

}

async function fetchOrders(params = {}) {
  const url = buildUrlWithParams(`${ALPACA_BASE_URL}/orders`, params);
  let response;
  try {
    response = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ phase: 'orders', url, error: err });
    throw err;
  }

  if (Array.isArray(response)) {

    return response.map((order) => ({

      ...order,

      symbol: normalizeSymbol(order.symbol),

    }));

  }

  return response;

}

async function fetchOpenPositions() {
  const url = `${ALPACA_BASE_URL}/positions`;
  let res;
  try {
    res = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ phase: 'positions', url, error: err });
    throw err;
  }
  const positions = Array.isArray(res) ? res : [];
  return positions
    .map((pos) => {
      const qty = Number(pos.qty ?? pos.quantity ?? 0);
      return {
        symbol: normalizeSymbol(pos.symbol),
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
  const now = Date.now();
  const snapshot = {};
  for (const [symbol, entry] of lastQuoteAt.entries()) {
    const tsMs = entry?.tsMs;
    if (Number.isFinite(tsMs)) {
      snapshot[symbol] = {
        ts: new Date(tsMs).toISOString(),
        ageSeconds: (now - tsMs) / 1000,
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
  const url = `${ALPACA_BASE_URL}/orders/${orderId}`;
  let response;
  try {
    response = await requestJson({
      method: 'DELETE',
      url,
      headers: alpacaHeaders(),
    });
  } catch (err) {
    logHttpError({ phase: 'orders', url, error: err });
    throw err;
  }

  return response;

}

async function getAlpacaConnectivityStatus() {
  const hasAuth = resolvedAlpacaAuth.alpacaAuthOk;
  const tradeUrl = `${ALPACA_BASE_URL}/account`;
  const dataSymbol = 'AAPL';
  const dataUrl = `${STOCKS_DATA_URL}/quotes/latest?symbols=${encodeURIComponent(dataSymbol)}`;

  const tradeResult = await httpJson({
    method: 'GET',
    url: tradeUrl,
    headers: alpacaHeaders(),
  });
  if (tradeResult.error) {
    logHttpError({ phase: 'account', url: tradeUrl, error: tradeResult.error });
  }

  const dataResult = await httpJson({
    method: 'GET',
    url: dataUrl,
    headers: alpacaHeaders(),
  });
  if (dataResult.error) {
    logHttpError({ phase: 'quote', url: dataUrl, error: dataResult.error });
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
    hasAuth,
    tradeAccountOk: !tradeResult.error,
    tradeStatus: tradeResult.error ? tradeResult.error.statusCode ?? null : 200,
    dataQuoteOk: !dataResult.error,
    dataStatus: dataResult.error ? dataResult.error.statusCode ?? null : 200,
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

  startExitManager,
  getConcurrencyGuardStatus,
  getLastQuoteSnapshot,
  getAlpacaAuthStatus,
  getLastHttpError,
  getAlpacaConnectivityStatus,
  runDustCleanup,

};
