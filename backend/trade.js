const axios = require('axios');

 

const ALPACA_BASE_URL = process.env.ALPACA_API_BASE || 'https://api.alpaca.markets/v2';

const DATA_URL = 'https://data.alpaca.markets/v1beta2';

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

const inventoryState = new Map();

const cfeeCache = { ts: 0, items: [] };

 

function sleep(ms) {

  return new Promise((resolve) => setTimeout(resolve, ms));

}

function logSkip(reason, details = {}) {

  console.log(`Skip — ${reason}`, details);

}

function updateInventoryFromBuy(symbol, qty, price) {

  const qtyNum = Number(qty);

  const priceNum = Number(price);

  if (!Number.isFinite(qtyNum) || !Number.isFinite(priceNum) || qtyNum <= 0 || priceNum <= 0) {

    return;

  }

  const current = inventoryState.get(symbol) || { qty: 0, costBasis: 0, avgPrice: 0 };

  const newQty = current.qty + qtyNum;

  const newCost = current.costBasis + qtyNum * priceNum;

  const avgPrice = newQty > 0 ? newCost / newQty : 0;

  inventoryState.set(symbol, { qty: newQty, costBasis: newCost, avgPrice });

}

async function initializeInventoryFromPositions() {

  const res = await axios.get(`${ALPACA_BASE_URL}/positions`, { headers: HEADERS });

  const positions = Array.isArray(res.data) ? res.data : [];

  inventoryState.clear();

  for (const pos of positions) {

    const qty = Number(pos.qty ?? pos.quantity ?? 0);

    const avgPrice = Number(pos.avg_entry_price ?? pos.avgEntryPrice ?? 0);

    if (!Number.isFinite(qty) || !Number.isFinite(avgPrice) || qty <= 0 || avgPrice <= 0) {

      continue;

    }

    inventoryState.set(pos.symbol, { qty, costBasis: qty * avgPrice, avgPrice });

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

  const items = Array.isArray(res.data) ? res.data : [];

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

  console.log('feeAwareMinProfitBps', { symbol, notionalUsd, feeUsd, feeBps, minBps });

  return minBps;

}

 

// Places a limit buy order first, then a limit sell after the buy is filled.

async function placeLimitBuyThenSell(symbol, qty, limitPrice) {

  const intendedNotional = Number(qty) * Number(limitPrice);

  if (!Number.isFinite(intendedNotional) || intendedNotional < MIN_ORDER_NOTIONAL_USD) {

    logSkip('notional_too_small', { symbol, intendedNotional, minNotionalUsd: MIN_ORDER_NOTIONAL_USD });

    return { skipped: true, reason: 'notional_too_small', notionalUsd: intendedNotional };

  }

  // submit the limit buy order

  const buyRes = await axios.post(

    `${ALPACA_BASE_URL}/orders`,

    {

      symbol,

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

  updateInventoryFromBuy(symbol, filledOrder.filled_qty, avgPrice);

  const inventory = inventoryState.get(symbol);

  if (!inventory || inventory.qty <= 0) {

    logSkip('no_inventory_for_sell', { symbol, qty: filledOrder.filled_qty });

    return { buy: filledOrder, sell: null, sellError: 'No inventory to sell' };

  }

  const notionalUsd = Number(filledOrder.filled_qty) * avgPrice;

  const minProfitBps = await feeAwareMinProfitBps(symbol, notionalUsd);

  // Mark up sell price to cover fees, slippage, and desired profit

  const sellPrice = roundPrice(avgPrice * (1 + minProfitBps / 10000));

 

  const sellRes = await axios.post(

    `${ALPACA_BASE_URL}/orders`,

    {

      symbol,

      qty: filledOrder.filled_qty,

      side: 'sell',

      type: 'limit',

      // match the buy order's time in force

      time_in_force: 'gtc',

      limit_price: sellPrice,

    },

    { headers: HEADERS }

  );

 

  return { buy: filledOrder, sell: sellRes.data };

}

 

// Fetch latest trade price for a symbol

async function getLatestPrice(symbol) {

  const res = await axios.get(

    `${DATA_URL}/crypto/latest/trades?symbols=${symbol}`,

    { headers: HEADERS }

  );

  const trade = res.data.trades && res.data.trades[symbol];

  if (!trade) throw new Error(`Price not available for ${symbol}`);

  return parseFloat(trade.p);

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

 

// Market buy using 10% of portfolio value then place a limit sell with markup

// covering taker fees and profit target

async function placeMarketBuyThenSell(symbol) {

  const [price, account] = await Promise.all([

    getLatestPrice(symbol),

    getAccountInfo(),

  ]);

 

  const portfolioValue = account.portfolioValue;

  const buyingPower = account.buyingPower;

 

  const targetTradeAmount = portfolioValue * 0.1;

  const amountToSpend = Math.min(targetTradeAmount, buyingPower);

  if (!Number.isFinite(amountToSpend) || amountToSpend < MIN_ORDER_NOTIONAL_USD) {

    logSkip('notional_too_small', { symbol, intendedNotional: amountToSpend, minNotionalUsd: MIN_ORDER_NOTIONAL_USD });

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

      symbol,

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

  updateInventoryFromBuy(symbol, filled.filled_qty, filled.filled_avg_price);

  const inventory = inventoryState.get(symbol);

  if (!inventory || inventory.qty <= 0) {

    logSkip('no_inventory_for_sell', { symbol, qty: filled.filled_qty });

    return { buy: filled, sell: null, sellError: 'No inventory to sell' };

  }

 

  // Wait 10 seconds before selling

  await sleep(10000);

 

  const avgPrice = parseFloat(filled.filled_avg_price);

  const notionalUsd = Number(filled.filled_qty) * avgPrice;

  const minProfitBps = await feeAwareMinProfitBps(symbol, notionalUsd);

  // Mark up sell price to cover fees, slippage, and preserve desired profit margin

  const limitPrice = roundPrice(avgPrice * (1 + minProfitBps / 10000));

 

  try {

    const sellRes = await axios.post(

      `${ALPACA_BASE_URL}/orders`,

      {

        symbol,

        qty: filled.filled_qty,

        side: 'sell',

        type: 'limit',

        time_in_force: 'gtc',

        limit_price: limitPrice,

      },

      { headers: HEADERS }

    );

    return { buy: filled, sell: sellRes.data };

  } catch (err) {

    console.error('Sell order failed:', err?.response?.data || err.message);

    return { buy: filled, sell: null, sellError: err.message };

  }

}

 

module.exports = {

  placeLimitBuyThenSell,

  placeMarketBuyThenSell,

  initializeInventoryFromPositions,

};
