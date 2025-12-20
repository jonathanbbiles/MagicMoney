const axios = require('axios');

 

const ALPACA_BASE_URL = process.env.ALPACA_API_BASE || 'https://api.alpaca.markets/v2';

const DATA_URL = 'https://data.alpaca.markets/v1beta2';
const STOCKS_DATA_URL = 'https://data.alpaca.markets/v2/stocks';

const API_KEY = process.env.ALPACA_API_KEY;

const SECRET_KEY = process.env.ALPACA_SECRET_KEY;

 

const HEADERS = {

  'APCA-API-KEY-ID': API_KEY,

  'APCA-API-SECRET-KEY': SECRET_KEY,

};

 

const MIN_ORDER_NOTIONAL_USD = Number(process.env.MIN_ORDER_NOTIONAL_USD || 15);

const USER_MIN_PROFIT_BPS = Number(process.env.USER_MIN_PROFIT_BPS || 5);

const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS || 10);

const BUFFER_BPS = Number(process.env.BUFFER_BPS || 15);

const MAX_HOLD_SECONDS = Number(process.env.MAX_HOLD_SECONDS || 300);

const REPRICE_EVERY_SECONDS = Number(process.env.REPRICE_EVERY_SECONDS || 20);

const FORCE_EXIT_SECONDS = Number(process.env.FORCE_EXIT_SECONDS || 600);

const PRICE_TICK = Number(process.env.PRICE_TICK || 0.01);

const inventoryState = new Map();

const exitState = new Map();

const cfeeCache = { ts: 0, items: [] };

 

function sleep(ms) {

  return new Promise((resolve) => setTimeout(resolve, ms));

}

function logSkip(reason, details = {}) {

  console.log(`Skip — ${reason}`, details);

}

function normalizeSymbol(rawSymbol) {

  if (!rawSymbol) return rawSymbol;

  const symbol = String(rawSymbol).trim().toUpperCase();

  return symbol.includes('/') ? symbol.replace(/\//g, '') : symbol;

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

  const res = await axios.get(`${ALPACA_BASE_URL}/positions`, { headers: HEADERS });

  const positions = Array.isArray(res.data) ? res.data : [];

  inventoryState.clear();

  for (const pos of positions) {

    const symbol = normalizeSymbol(pos.symbol);

    const qty = Number(pos.qty ?? pos.quantity ?? 0);

    const avgPrice = Number(pos.avg_entry_price ?? pos.avgEntryPrice ?? 0);

    if (!Number.isFinite(qty) || !Number.isFinite(avgPrice) || qty <= 0 || avgPrice <= 0) {

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

  const params = new URLSearchParams({

    activity_types: 'CFEE',

    direction: 'desc',

    page_size: String(limit),

  });

  const res = await axios.get(`${ALPACA_BASE_URL}/account/activities?${params.toString()}`, {

    headers: HEADERS,

  });

  const items = Array.isArray(res.data)
    ? res.data.map((entry) => ({
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

  const buyRes = await axios.post(

    `${ALPACA_BASE_URL}/orders`,

    {

      symbol: normalizedSymbol,

      qty,

      side: 'buy',

      type: 'limit',

      // crypto orders must be GTC

      time_in_force: 'gtc',

      limit_price: limitPrice,

    },

    { headers: HEADERS }

  );

 

  const buyOrder = buyRes.data;

 

  // poll until the order is filled

  let filledOrder = buyOrder;

  for (let i = 0; i < 20; i++) {

    const check = await axios.get(`${ALPACA_BASE_URL}/orders/${buyOrder.id}`, {

      headers: HEADERS,

    });

    filledOrder = check.data;

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

    const res = await axios.get(

      `${DATA_URL}/crypto/latest/trades?symbols=${symbol}`,

      { headers: HEADERS }

    );

    const trade = res.data.trades && res.data.trades[symbol];

    if (!trade) throw new Error(`Price not available for ${symbol}`);

    return parseFloat(trade.p);

  }

  const res = await axios.get(

    `${STOCKS_DATA_URL}/trades/latest?symbols=${encodeURIComponent(symbol)}`,

    { headers: HEADERS }

  );

  const trade = res.data.trades && res.data.trades[symbol];

  if (!trade) throw new Error(`Price not available for ${symbol}`);

  return parseFloat(trade.p ?? trade.price);

}

 

// Get portfolio value and buying power from the Alpaca account

async function getAccountInfo() {

  const res = await axios.get(`${ALPACA_BASE_URL}/account`, { headers: HEADERS });

  const portfolioValue = parseFloat(res.data.portfolio_value);

  const buyingPower = parseFloat(res.data.buying_power);

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

  if (isCryptoSymbol(symbol)) {

    const res = await axios.get(

      `${DATA_URL}/crypto/latest/quotes?symbols=${symbol}`,

      { headers: HEADERS }

    );

    const quote = res.data.quotes && res.data.quotes[symbol];

    if (!quote) throw new Error(`Quote not available for ${symbol}`);

    const bid = Number(quote.bp ?? quote.bid_price ?? quote.bid);

    const ask = Number(quote.ap ?? quote.ask_price ?? quote.ask);

    return {

      bid: Number.isFinite(bid) ? bid : null,

      ask: Number.isFinite(ask) ? ask : null,

    };

  }

  const res = await axios.get(

    `${STOCKS_DATA_URL}/quotes/latest?symbols=${encodeURIComponent(symbol)}`,

    { headers: HEADERS }

  );

  const quote = res.data.quotes && res.data.quotes[symbol];

  if (!quote) throw new Error(`Quote not available for ${symbol}`);

  const bid = Number(quote.bp ?? quote.bid_price ?? quote.bid);

  const ask = Number(quote.ap ?? quote.ask_price ?? quote.ask);

  return {

    bid: Number.isFinite(bid) ? bid : null,

    ask: Number.isFinite(ask) ? ask : null,

  };

}

async function fetchOrderById(orderId) {

  const response = await axios.get(`${ALPACA_BASE_URL}/orders/${orderId}`, { headers: HEADERS });

  return response.data;

}

async function cancelOrderSafe(orderId) {

  try {

    await cancelOrder(orderId);

    return true;

  } catch (err) {

    console.warn('cancel_order_failed', { orderId, error: err?.response?.data || err.message });

    return false;

  }

}

async function submitLimitSell({

  symbol,

  qty,

  limitPrice,

  reason,

}) {

  const response = await axios.post(

    `${ALPACA_BASE_URL}/orders`,

    {

      symbol,

      qty,

      side: 'sell',

      type: 'limit',

      time_in_force: 'gtc',

      limit_price: limitPrice,

    },

    { headers: HEADERS }

  );

  console.log('submit_limit_sell', { symbol, qty, limitPrice, reason, orderId: response.data?.id });

  return response.data;

}

async function submitMarketSell({

  symbol,

  qty,

  reason,

}) {

  const response = await axios.post(

    `${ALPACA_BASE_URL}/orders`,

    {

      symbol,

      qty,

      side: 'sell',

      type: 'market',

      time_in_force: 'gtc',

    },

    { headers: HEADERS }

  );

  console.log('submit_market_sell', { symbol, qty, reason, orderId: response.data?.id });

  return response.data;

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

 

  const buyRes = await axios.post(

    `${ALPACA_BASE_URL}/orders`,

    {

      symbol: normalizedSymbol,

      qty,

      side: 'buy',

      type: 'market',

      time_in_force: 'gtc',

    },

    { headers: HEADERS }

  );

 

  const buyOrder = buyRes.data;

 

  // Wait for fill

  let filled = buyOrder;

  for (let i = 0; i < 20; i++) {

    const chk = await axios.get(`${ALPACA_BASE_URL}/orders/${buyOrder.id}`, {

      headers: HEADERS,

    });

    filled = chk.data;

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

    console.error('Sell order failed:', err?.response?.data || err.message);

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

  const response = await axios.post(

    `${ALPACA_BASE_URL}/orders`,

    {

      symbol: normalizedSymbol,

      qty,

      side,

      type,

      time_in_force,

      limit_price,

      notional,

    },

    { headers: HEADERS }

  );

  return response.data;

}

async function fetchOrders(params = {}) {

  const response = await axios.get(`${ALPACA_BASE_URL}/orders`, { headers: HEADERS, params });

  if (Array.isArray(response.data)) {

    return response.data.map((order) => ({

      ...order,

      symbol: normalizeSymbol(order.symbol),

    }));

  }

  return response.data;

}

async function cancelOrder(orderId) {

  const response = await axios.delete(`${ALPACA_BASE_URL}/orders/${orderId}`, { headers: HEADERS });

  return response.data;

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

};
