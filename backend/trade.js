const { randomUUID, randomBytes } = require('crypto');

const { httpJson } = require('./httpClient');
const {
  MAX_QUOTE_AGE_MS,
  ABSURD_AGE_MS,
  normalizeQuoteTsMs,
  computeQuoteAgeMs,
  normalizeQuoteAgeMs,
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

const TRADE_PORTFOLIO_PCT = Number(process.env.TRADE_PORTFOLIO_PCT || 0.10);
const MIN_ORDER_NOTIONAL_USD = Number(process.env.MIN_ORDER_NOTIONAL_USD || 1);
const MIN_TRADE_QTY = Number(process.env.MIN_TRADE_QTY || 1e-6);
const MARKET_DATA_TIMEOUT_MS = Number(process.env.MARKET_DATA_TIMEOUT_MS || 9000);
const MARKET_DATA_RETRIES = Number(process.env.MARKET_DATA_RETRIES || 2);
const MARKET_DATA_FAILURE_LIMIT = Number(process.env.MARKET_DATA_FAILURE_LIMIT || 5);
const MARKET_DATA_COOLDOWN_MS = Number(process.env.MARKET_DATA_COOLDOWN_MS || 60000);

const USER_MIN_PROFIT_BPS = Number(process.env.USER_MIN_PROFIT_BPS || 5);
const DESIRED_NET_PROFIT_BASIS_POINTS = readNumber('DESIRED_NET_PROFIT_BASIS_POINTS', 100);
const MAX_GROSS_TAKE_PROFIT_BASIS_POINTS = readNumber('MAX_GROSS_TAKE_PROFIT_BASIS_POINTS', 150);
const MIN_GROSS_TAKE_PROFIT_BASIS_POINTS = readNumber('MIN_GROSS_TAKE_PROFIT_BASIS_POINTS', 60);

const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS || 0);

const BUFFER_BPS = Number(process.env.BUFFER_BPS || 0);

const FEE_BPS_MAKER = Number(process.env.FEE_BPS_MAKER || 10);
const FEE_BPS_TAKER = Number(process.env.FEE_BPS_TAKER || 20);
const PROFIT_BUFFER_BPS = Number(process.env.PROFIT_BUFFER_BPS || 0);
const EXIT_POLICY_LOCKED = readEnvFlag('EXIT_POLICY_LOCKED', true);
const EXIT_NET_PROFIT_AFTER_FEES_BPS = readNumber('EXIT_NET_PROFIT_AFTER_FEES_BPS', 5);
const EXIT_CANCELS_ENABLED = readEnvFlag('EXIT_CANCELS_ENABLED', false);
const SELL_REPRICE_ENABLED = readEnvFlag('SELL_REPRICE_ENABLED', false);
const EXIT_TAKER_ON_TOUCH_ENABLED = readEnvFlag('EXIT_TAKER_ON_TOUCH_ENABLED', false);
const EXIT_MARKET_EXITS_ENABLED = readEnvFlag('EXIT_MARKET_EXITS_ENABLED', false);
const DISABLE_IOC_EXITS = readEnvFlag('DISABLE_IOC_EXITS', true);
const EXIT_LIMIT_SELL_TIF = String(process.env.EXIT_LIMIT_SELL_TIF || 'gtc').trim().toLowerCase();
const EXIT_LIMIT_SELL_TIF_SAFE = ['gtc', 'ioc', 'fok'].includes(EXIT_LIMIT_SELL_TIF) ? EXIT_LIMIT_SELL_TIF : 'gtc';
const TAKER_EXIT_ON_TOUCH = EXIT_POLICY_LOCKED ? false : EXIT_TAKER_ON_TOUCH_ENABLED;
const REPLACE_THRESHOLD_BPS = Number(process.env.REPLACE_THRESHOLD_BPS || 8);
const ORDER_TTL_MS = Number(process.env.ORDER_TTL_MS || 45000);
const SELL_ORDER_TTL_MS = readNumber('SELL_ORDER_TTL_MS', 12000);
const ORDER_FETCH_THROTTLE_MS = 1000;
const MIN_REPRICE_INTERVAL_MS = Number(process.env.MIN_REPRICE_INTERVAL_MS || 20000);
const REPRICE_IF_AWAY_BPS = Number(process.env.REPRICE_IF_AWAY_BPS || 8);
const MAX_SPREAD_BPS_TO_TRADE = readNumber('MAX_SPREAD_BPS_TO_TRADE', 25);
const STOP_LOSS_BPS = readNumber('STOP_LOSS_BPS', 60);
const VOL_HALF_LIFE_MIN = readNumber('VOL_HALF_LIFE_MIN', 6);
const STOP_VOL_MULT = readNumber('STOP_VOL_MULT', 2.5);
const TP_VOL_SCALE = readNumber('TP_VOL_SCALE', 1.0);
const EV_GUARD_ENABLED = readFlag('EV_GUARD_ENABLED', true);
const EV_MIN_BPS = readNumber('EV_MIN_BPS', -1);
const RISK_LEVEL = readNumber('RISK_LEVEL', 2);
const ENTRY_SCAN_INTERVAL_MS = readNumber('ENTRY_SCAN_INTERVAL_MS', 4000);
const DEBUG_ENTRY = readFlag('DEBUG_ENTRY', false);

const MAX_HOLD_SECONDS = readNumber('MAX_HOLD_SECONDS', 180);
const MAX_HOLD_MS = Number(process.env.MAX_HOLD_MS || MAX_HOLD_SECONDS * 1000);

const REPRICE_EVERY_SECONDS = readNumber('REPRICE_EVERY_SECONDS', 5);

const EXIT_MODE_RAW = String(process.env.EXIT_MODE || 'robust').trim().toLowerCase();
const EXIT_MODE = EXIT_POLICY_LOCKED ? 'net_after_fees' : EXIT_MODE_RAW;
const FORCE_EXIT_SECONDS = readNumber('FORCE_EXIT_SECONDS', 0);
const FORCE_EXIT_ALLOW_LOSS = readFlag('FORCE_EXIT_ALLOW_LOSS', false);
const ENTRY_FILL_TIMEOUT_SECONDS = readNumber('ENTRY_FILL_TIMEOUT_SECONDS', 30);
const ENTRY_INTENT_TTL_MS = readNumber('ENTRY_INTENT_TTL_MS', 45000);
const ENTRY_BUY_TIF = String(process.env.ENTRY_BUY_TIF || 'ioc').trim().toLowerCase();
const ENTRY_BUY_TIF_SAFE = ['gtc', 'ioc', 'fok'].includes(ENTRY_BUY_TIF) ? ENTRY_BUY_TIF : 'ioc';
const POST_ONLY_BUY = readFlag('POST_ONLY_BUY', true);
const ENTRY_FALLBACK_MARKET = readFlag('ENTRY_FALLBACK_MARKET', false);
const ALLOW_TAKER_BEFORE_TARGET = readFlag('ALLOW_TAKER_BEFORE_TARGET', false);
const TAKER_TOUCH_MIN_INTERVAL_MS = readNumber('TAKER_TOUCH_MIN_INTERVAL_MS', 5000);

const SIMPLE_SCALPER_ENABLED = readFlag('SIMPLE_SCALPER', false);
const MAX_SPREAD_BPS_SIMPLE_DEFAULT = Number.isFinite(Number(process.env.MAX_SPREAD_BPS_TO_TRADE))
  ? Number(process.env.MAX_SPREAD_BPS_TO_TRADE)
  : 60;
const MAX_SPREAD_BPS_SIMPLE = readNumber('MAX_SPREAD_BPS_SIMPLE', MAX_SPREAD_BPS_SIMPLE_DEFAULT);
const PROFIT_NET_BPS = readNumber('PROFIT_NET_BPS', 100);
const FEE_BPS_EST = readNumber('FEE_BPS_EST', 25);
const BUYING_POWER_RESERVE_USD = readNumber('BUYING_POWER_RESERVE_USD', 0);
const ORDERBOOK_GUARD_ENABLED = readFlag('ORDERBOOK_GUARD_ENABLED', true);
const ORDERBOOK_MAX_AGE_MS = readNumber('ORDERBOOK_MAX_AGE_MS', 3000);
const ORDERBOOK_BAND_BPS = readNumber('ORDERBOOK_BAND_BPS', 10);
const ORDERBOOK_MIN_DEPTH_USD = readNumber('ORDERBOOK_MIN_DEPTH_USD', 250);
const ORDERBOOK_IMPACT_NOTIONAL_USD = readNumber('ORDERBOOK_IMPACT_NOTIONAL_USD', 100);
const ORDERBOOK_MAX_IMPACT_BPS = readNumber('ORDERBOOK_MAX_IMPACT_BPS', 6);
const ORDERBOOK_IMBALANCE_BIAS_SCALE = readNumber('ORDERBOOK_IMBALANCE_BIAS_SCALE', 0.04);

const PRICE_TICK = Number(process.env.PRICE_TICK || 0.01);
const MAX_CONCURRENT_POSITIONS = Number(process.env.MAX_CONCURRENT_POSITIONS || 100);
const MIN_POSITION_QTY = Number(process.env.MIN_POSITION_QTY || 1e-6);
const POSITIONS_SNAPSHOT_TTL_MS = Number(process.env.POSITIONS_SNAPSHOT_TTL_MS || 5000);
const OPEN_POSITIONS_CACHE_TTL_MS = 1500;
const OPEN_ORDERS_CACHE_TTL_MS = 1500;
const LIVE_ORDERS_CACHE_TTL_MS = 1500;
const ACCOUNT_CACHE_TTL_MS = 2000;
const EXIT_QUOTE_MAX_AGE_MS = readNumber('EXIT_QUOTE_MAX_AGE_MS', 120000);
const EXIT_STALE_QUOTE_MAX_AGE_MS = readNumber('EXIT_STALE_QUOTE_MAX_AGE_MS', 15000);
const EXIT_REPAIR_INTERVAL_MS = readNumber('EXIT_REPAIR_INTERVAL_MS', 60000);
const SELL_QTY_MATCH_EPSILON = Number(process.env.SELL_QTY_MATCH_EPSILON || 1e-9);
const ENTRY_QUOTE_MAX_AGE_MS = readNumber('ENTRY_QUOTE_MAX_AGE_MS', 120000);
const CRYPTO_QUOTE_MAX_AGE_MS = readNumber('CRYPTO_QUOTE_MAX_AGE_MS', 600000);
const MAX_LOGGED_QUOTE_AGE_SECONDS = 9999;
const DEBUG_QUOTE_TS = ['1', 'true', 'yes'].includes(String(process.env.DEBUG_QUOTE_TS || '').toLowerCase());
const quoteTsDebugLogged = new Set();
const quoteKeyMissingLogged = new Set();
const cryptoQuoteTtlOverrideLogged = new Set();
const HALT_ON_ORPHANS = readEnvFlag('HALT_ON_ORPHANS', false);
const ORPHAN_AUTO_ATTACH_TP = readEnvFlag('ORPHAN_AUTO_ATTACH_TP', true);
const ORPHAN_REPAIR_BEFORE_HALT = readEnvFlag('ORPHAN_REPAIR_BEFORE_HALT', true);
const ORPHAN_SCAN_TTL_MS = readNumber('ORPHAN_SCAN_TTL_MS', 15000);
let tradingHaltedReason = null;
let lastOrphanScan = { tsMs: 0, orphans: [], positionsCount: 0, openOrdersCount: 0, openSellSymbols: [] };

// ───────────────────────── ENTRY SIGNAL (RESTORED FROM v3) ─────────────────────────
const symStats = Object.create(null);
const sigmaEwmaBySymbol = new Map();
const spreadEwmaBySymbol = new Map();
const slipEwmaBySymbol = new Map();

/* 10) STATIC UNIVERSES */
const ORIGINAL_TOKENS = [
  'BTC/USD',
  'ETH/USD',
  'SOL/USD',
  'AVAX/USD',
  'DOGE/USD',
  'ADA/USD',
  'XRP/USD',
  'DOT/USD',
  'LINK/USD',
  'MATIC/USD',
  'LTC/USD',
  'BCH/USD',
  'UNI/USD',
  'AAVE/USD',
];

const CRYPTO_CORE_TRACKED = ORIGINAL_TOKENS.filter((sym) => !String(sym).includes('USD/USD'));

const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));

const emaArr = (arr, span) => {
  if (!arr?.length) return [];
  const k = 2 / (span + 1);
  let prev = arr[0];
  const out = [prev];
  for (let i = 1; i < arr.length; i++) {
    prev = arr[i] * k + prev * (1 - k);
    out.push(prev);
  }
  return out;
};

const LIVE_ORDER_STATUSES = new Set(['new', 'accepted', 'pending_new', 'partially_filled', 'pending_replace']);
const NON_LIVE_ORDER_STATUSES = new Set(['filled', 'canceled', 'expired', 'rejected']);
const OPEN_LIKE_ORDER_STATUSES = new Set([
  'new',
  'accepted',
  'partially_filled',
  'pending_new',
  'pending_replace',
  'held',
  'queued',
  'replaced',
]);

function isLiveOrderStatus(status) {
  const lowered = String(status || '').toLowerCase();
  if (!lowered) return false;
  if (LIVE_ORDER_STATUSES.has(lowered)) return true;
  if (NON_LIVE_ORDER_STATUSES.has(lowered)) return false;
  return false;
}

function isTerminalOrderStatus(status) {
  const lowered = String(status || '').toLowerCase();
  if (!lowered) return false;
  return NON_LIVE_ORDER_STATUSES.has(lowered);
}

function isOpenLikeOrderStatus(status) {
  const lowered = String(status || '').toLowerCase();
  if (!lowered) return false;
  return OPEN_LIKE_ORDER_STATUSES.has(lowered);
}

function expandNestedOrders(orders) {
  const list = Array.isArray(orders) ? orders : [];
  return list.reduce((acc, order) => {
    acc.push(order);
    if (Array.isArray(order?.legs)) {
      acc.push(...order.legs);
    }
    return acc;
  }, []);
}

function filterLiveOrders(orders) {
  const list = Array.isArray(orders) ? orders : [];
  return list.filter((order) => isLiveOrderStatus(order?.status));
}

/* 14) FEE / PNL MODEL */
const BPS = 10000;

function feeBpsRoundTrip() {
  return FEE_BPS_MAKER + (TAKER_EXIT_ON_TOUCH ? FEE_BPS_TAKER : FEE_BPS_MAKER);
}

function expectedValueBps({ pUp, winBps, loseBps, feeBps, spreadBps, slippageBps }) {
  const win = Number.isFinite(winBps) ? winBps : 0;
  const lose = Number.isFinite(loseBps) ? loseBps : 0;
  const fees = Number.isFinite(feeBps) ? feeBps : 0;
  const spread = Number.isFinite(spreadBps) ? spreadBps : 0;
  const slip = Number.isFinite(slippageBps) ? slippageBps : 0;
  const p = clamp(Number.isFinite(pUp) ? pUp : 0.5, 0, 1);
  return p * win - (1 - p) * lose - fees - spread - slip;
}

function requiredProfitBpsForSymbol({ slippageBps, feeBps, desiredNetExitBps }) {
  const desiredNet = Number.isFinite(desiredNetExitBps) ? desiredNetExitBps : DESIRED_NET_PROFIT_BASIS_POINTS;
  const slip = Number.isFinite(slippageBps) ? slippageBps : SLIPPAGE_BPS;
  const fees = Number.isFinite(feeBps) ? feeBps : feeBpsRoundTrip();
  return resolveRequiredExitBps({
    desiredNetExitBps: desiredNet,
    feeBpsRoundTrip: fees,
    slippageBps: slip,
    spreadBufferBps: BUFFER_BPS,
    profitBufferBps: PROFIT_BUFFER_BPS,
    maxGrossTakeProfitBps: MAX_GROSS_TAKE_PROFIT_BASIS_POINTS,
  });
}

/* 18) SIGNAL / ENTRY MATH */
function ewmaSigmaFromCloses(closes, halfLifeMin = 6) {
  if (!Array.isArray(closes) || closes.length < 2) return 0;
  const hl = Math.max(1, halfLifeMin);
  const alpha = 1 - Math.exp(Math.log(0.5) / hl);
  let variance = 0;
  for (let i = 1; i < closes.length; i++) {
    const prev = Number(closes[i - 1]);
    const next = Number(closes[i]);
    if (!Number.isFinite(prev) || !Number.isFinite(next) || prev <= 0 || next <= 0) continue;
    const r = Math.log(next / prev);
    const r2 = r * r;
    variance = alpha * r2 + (1 - alpha) * variance;
  }
  return Math.sqrt(Math.max(variance, 0)) * BPS;
}

function barrierPTouchUpDriftless(distUpBps, distDownBps) {
  const up = Math.max(1, Number(distUpBps) || 0);
  const down = Math.max(1, Number(distDownBps) || 0);
  return clamp(down / (up + down), 0.05, 0.95);
}

function microMetrics({ mid, prevMid, spreadBps }) {
  const deltaBps = Number.isFinite(prevMid) && prevMid > 0 ? ((mid - prevMid) / prevMid) * BPS : 0;
  const spreadNorm = Number.isFinite(spreadBps) ? spreadBps : 0;
  const microBias = clamp(deltaBps / Math.max(spreadNorm, 1) * 0.08, -0.08, 0.08);
  return {
    deltaBps,
    microBias,
  };
}

async function computeEntrySignal(symbol) {
  const asset = { symbol: normalizeSymbol(symbol) };
  const riskLvl = clamp(Math.round(RISK_LEVEL), 0, 4);
  let quote;
  try {
    quote = await getLatestQuote(asset.symbol, { maxAgeMs: ENTRY_QUOTE_MAX_AGE_MS });
  } catch (err) {
    return { entryReady: false, why: 'stale_quote', meta: { symbol: asset.symbol, error: err?.message || err } };
  }

  const bid = Number(quote.bid);
  const ask = Number(quote.ask);
  const mid = Number.isFinite(bid) && Number.isFinite(ask) ? (bid + ask) / 2 : Number(quote.mid || bid || ask);
  if (!Number.isFinite(bid) || !Number.isFinite(ask) || bid <= 0 || ask <= 0 || !Number.isFinite(mid)) {
    return { entryReady: false, why: 'invalid_quote', meta: { symbol: asset.symbol, bid, ask } };
  }

  const spreadBps = ((ask - bid) / mid) * BPS;
  if (SIMPLE_SCALPER_ENABLED) {
    if (Number.isFinite(spreadBps) && spreadBps > MAX_SPREAD_BPS_SIMPLE) {
      return { entryReady: false, why: 'spread_gate', meta: { symbol: asset.symbol, spreadBps } };
    }
    return {
      entryReady: true,
      symbol: asset.symbol,
      spreadBps,
      meta: { spreadBps },
    };
  }

  if (Number.isFinite(spreadBps) && spreadBps > MAX_SPREAD_BPS_TO_TRADE) {
    return { entryReady: false, why: 'spread_gate', meta: { symbol: asset.symbol, spreadBps } };
  }

  let orderbookMeta = null;
  let obBias = 0;
  let obImpactBpsBuy = null;

  if (ORDERBOOK_GUARD_ENABLED) {
    try {
      const ob = await getLatestOrderbook(asset.symbol, { maxAgeMs: ORDERBOOK_MAX_AGE_MS });
      const m = computeOrderbookMetrics(ob, { bid, ask });
      orderbookMeta = m;
      obBias = m.obBias || 0;
      obImpactBpsBuy = m.impactBpsBuy;

      if (!m.ok) {
        return {
          entryReady: false,
          why: 'orderbook_liquidity_gate',
          meta: { symbol: asset.symbol, spreadBps, ...m },
        };
      }
    } catch (err) {
      return {
        entryReady: false,
        why: 'orderbook_unavailable',
        meta: { symbol: asset.symbol, spreadBps, error: err?.message || String(err) },
      };
    }
  }

  let bars;
  try {
    bars = await fetchCryptoBars({ symbols: [asset.symbol], limit: 16, timeframe: '1Min' });
  } catch (err) {
    return { entryReady: false, why: 'bars_unavailable', meta: { symbol: asset.symbol, error: err?.message || err } };
  }

  const barKey = normalizeSymbol(asset.symbol);
  const barSeries = bars?.bars?.[barKey] || bars?.bars?.[normalizePair(barKey)] || [];
  const closes = (Array.isArray(barSeries) ? barSeries : []).map((bar) =>
    Number(bar.c ?? bar.close ?? bar.close_price ?? bar.price ?? bar.vwap)
  ).filter((value) => Number.isFinite(value) && value > 0);

  if (closes.length < 3) {
    return { entryReady: false, why: 'insufficient_bars', meta: { symbol: asset.symbol, barCount: closes.length } };
  }

  const sigmaRawBps = ewmaSigmaFromCloses(closes, VOL_HALF_LIFE_MIN);
  const sigmaAlpha = 2 / (Math.max(2, VOL_HALF_LIFE_MIN) + 1);
  const prevSigma = sigmaEwmaBySymbol.get(asset.symbol) ?? sigmaRawBps;
  const sigmaEwma = sigmaAlpha * sigmaRawBps + (1 - sigmaAlpha) * prevSigma;
  sigmaEwmaBySymbol.set(asset.symbol, sigmaEwma);

  const spreadAlpha = 0.2;
  const prevSpread = spreadEwmaBySymbol.get(asset.symbol) ?? spreadBps;
  const spreadEwma = spreadAlpha * spreadBps + (1 - spreadAlpha) * prevSpread;
  spreadEwmaBySymbol.set(asset.symbol, spreadEwma);

  const rawSlip = Number.isFinite(spreadBps)
    ? Math.max(SLIPPAGE_BPS, Math.min(spreadBps, 250))
    : SLIPPAGE_BPS;
  const prevSlip = slipEwmaBySymbol.get(asset.symbol) ?? rawSlip;
  const slipEwma = spreadAlpha * rawSlip + (1 - spreadAlpha) * prevSlip;
  slipEwmaBySymbol.set(asset.symbol, slipEwma);

  const prevMid = symStats[asset.symbol]?.lastMid;
  const micro = microMetrics({ mid, prevMid, spreadBps: spreadEwma });
  const closesEma = emaArr(closes.slice(-8), 5);
  const emaTail = closesEma[closesEma.length - 1];
  const momentumBps = Number.isFinite(emaTail) ? ((closes[closes.length - 1] - emaTail) / emaTail) * BPS : 0;
  const momentumPenaltyBps = momentumBps < 0 ? Math.abs(momentumBps) * 0.35 : 0;
  const momBias = clamp(momentumBps / Math.max(sigmaEwma, 1) * 0.15, -0.15, 0.15);

  const stopBps = Math.max(STOP_LOSS_BPS, sigmaEwma * STOP_VOL_MULT);
  const needBpsVol = Math.max(1, sigmaEwma * TP_VOL_SCALE);
  const desiredNetExitBps = DESIRED_NET_PROFIT_BASIS_POINTS;
  const obSlip = Number.isFinite(obImpactBpsBuy) ? obImpactBpsBuy : 0;
  const slippageBpsForExit = Math.max(SLIPPAGE_BPS, obSlip, Number.isFinite(slipEwma) ? slipEwma : 0);
  const requiredGrossExitBps = requiredProfitBpsForSymbol({
    slippageBps: slippageBpsForExit,
    feeBps: feeBpsRoundTrip(),
    desiredNetExitBps,
  });
  const riskScale = [1.25, 1.1, 1.0, 0.9, 0.8][riskLvl] ?? 1.0;
  const needDyn = Math.max(requiredGrossExitBps, needBpsVol * riskScale) + momentumPenaltyBps;
  const pUpBarrier = barrierPTouchUpDriftless(requiredGrossExitBps, stopBps);
  let pUp = 0.5 + micro.microBias + momBias + obBias + (pUpBarrier - 0.5) * 0.65;
  pUp = clamp(pUp, 0.05, 0.95);

  const expectedBps = expectedValueBps({
    pUp,
    winBps: requiredGrossExitBps,
    loseBps: stopBps,
    feeBps: feeBpsRoundTrip(),
    spreadBps: spreadEwma,
    slippageBps: slippageBpsForExit,
  });

  if (EV_GUARD_ENABLED && expectedBps < EV_MIN_BPS) {
    return {
      entryReady: false,
      why: 'ev_guard',
      meta: { symbol: asset.symbol, expectedBps, pUp, requiredGrossExitBps, stopBps },
    };
  }

  const desiredNetExitBpsForV22 = desiredNetExitBps;

  symStats[asset.symbol] = {
    lastMid: mid,
    lastTs: quote.tsMs,
    sigmaBps: sigmaEwma,
    spreadBps: spreadEwma,
    slipBps: slipEwma,
  };

  return {
    entryReady: true,
    symbol: asset.symbol,
    grossTpBps: requiredGrossExitBps,
    desiredNetExitBpsForV22,
    stopBps,
    spreadBps: spreadEwma,
    slippageBps: slipEwma,
    sigmaBps: sigmaEwma,
    pUp,
    expectedBps,
    meta: {
      riskLvl,
      microDeltaBps: micro.deltaBps,
      momentumBps,
      needBpsVol,
      needDyn,
      requiredGrossExitBps,
      feeBps: feeBpsRoundTrip(),
      orderbookAskDepthUsd: orderbookMeta?.askDepthUsd,
      orderbookBidDepthUsd: orderbookMeta?.bidDepthUsd,
      orderbookImpactBpsBuy: orderbookMeta?.impactBpsBuy,
      orderbookImbalance: orderbookMeta?.imbalance,
    },
  };
}

const inventoryState = new Map();

const exitState = new Map();
const desiredExitBpsBySymbol = new Map();
const entrySpreadOverridesBySymbol = new Map();
const symbolLocks = new Map();
const lastActionAt = new Map();
const lastCancelReplaceAt = new Map();
const lastOrderFetchAt = new Map();
const lastOrderSnapshotBySymbol = new Map();
const ENTRY_SUBMISSION_COOLDOWN_MS = Number(process.env.ENTRY_SUBMISSION_COOLDOWN_MS || 60000);
const recentEntrySubmissions = new Map(); // symbol -> { atMs, orderId }
const SIMPLE_SCALPER_ENTRY_TIMEOUT_MS = 30000;
const SIMPLE_SCALPER_RETRY_COOLDOWN_MS = 120000;
const inFlightBySymbol = new Map();

const cfeeCache = { ts: 0, items: [] };
const quoteCache = new Map();
const orderbookCache = new Map(); // symbol -> { tsMs, receivedAtMs, asks, bids }
const QUOTE_FAILURE_WINDOW_MS = 120000;
const QUOTE_FAILURE_THRESHOLD = 3;
const QUOTE_COOLDOWN_MS = 300000;
const quoteFailureState = new Map();
const lastQuoteAt = new Map();
const scanState = { lastScanAt: null };
let exitManagerRunning = false;
let exitRepairIntervalId = null;
let exitRepairRunning = false;
let lastExitRepairAtMs = 0;
const positionsSnapshot = {
  tsMs: 0,
  mapBySymbol: new Map(),
  mapByRaw: new Map(),
  loggedNoneSymbols: new Set(),
  pending: null,
};
const openOrdersCache = {
  tsMs: 0,
  data: null,
  pending: null,
};
const liveOrdersCache = {
  tsMs: 0,
  data: null,
  pending: null,
};
const positionsListCache = {
  tsMs: 0,
  data: null,
  pending: null,
};
let entryManagerRunning = false;
let entryScanRunning = false;
const openPositionsCache = {
  tsMs: 0,
  data: null,
  pending: null,
};
const accountCache = {
  tsMs: 0,
  data: null,
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

function logSimpleScalperSkip(symbol, reason, details = {}) {
  console.log('simple_scalper_skip', { symbol, reason, ...details });
}

function getInFlightStatus(symbol) {
  const normalized = normalizeSymbol(symbol);
  const entry = inFlightBySymbol.get(normalized);
  if (!entry) return null;
  const untilMs = entry.untilMs;
  if (Number.isFinite(untilMs) && Date.now() > untilMs) {
    inFlightBySymbol.delete(normalized);
    return null;
  }
  return entry;
}

function setInFlightStatus(symbol, entry) {
  const normalized = normalizeSymbol(symbol);
  inFlightBySymbol.set(normalized, entry);
}

function markRecentEntry(symbol, orderId) {
  recentEntrySubmissions.set(symbol, { atMs: Date.now(), orderId });
}

function hasRecentEntry(symbol) {
  const value = recentEntrySubmissions.get(symbol);
  if (!value) return false;
  if (Date.now() - value.atMs > ENTRY_SUBMISSION_COOLDOWN_MS) {
    recentEntrySubmissions.delete(symbol);
    return false;
  }
  return true;
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

function isStaleQuoteError(err) {
  const message = String(err?.message || err || '').toLowerCase();
  return message.includes('stale') || message.includes('absurd') || message.includes('timestamp');
}

function getQuoteFailureState(symbol) {
  if (!quoteFailureState.has(symbol)) {
    quoteFailureState.set(symbol, {
      failures: [],
      cooldownUntil: 0,
      lastReason: null,
    });
  }
  return quoteFailureState.get(symbol);
}

function isQuoteCooling(symbol) {
  const state = getQuoteFailureState(symbol);
  return Number.isFinite(state.cooldownUntil) && state.cooldownUntil > Date.now();
}

function recordQuoteFailure(symbol, reason) {
  if (!symbol) return;
  const state = getQuoteFailureState(symbol);
  state.lastReason = reason || state.lastReason;
  if (reason === 'stale_quote') return;
  const now = Date.now();
  state.failures = state.failures.filter((ts) => now - ts <= QUOTE_FAILURE_WINDOW_MS);
  state.failures.push(now);
  if (state.failures.length >= QUOTE_FAILURE_THRESHOLD) {
    state.failures = [];
    state.cooldownUntil = now + QUOTE_COOLDOWN_MS;
    console.warn('quote_cooldown', { symbol, reason: state.lastReason, cooldownMs: QUOTE_COOLDOWN_MS });
  }
}

function recordQuoteSuccess(symbol) {
  if (!symbol) return;
  const state = getQuoteFailureState(symbol);
  state.failures = [];
  state.cooldownUntil = 0;
  state.lastReason = null;
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
  return `${normalized}-${purpose}-${generateOrderNonce()}`;
}

function generateOrderNonce() {
  if (typeof randomUUID === 'function') return randomUUID();
  return randomBytes(6).toString('hex');
}

function getOrderIntentBucket() {
  const ttl = Number.isFinite(ORDER_TTL_MS) && ORDER_TTL_MS > 0 ? ORDER_TTL_MS : 45000;
  return Math.floor(Date.now() / ttl);
}

function buildIntentClientOrderId({ symbol, side, intent, ref }) {
  const normalized = canonicalAsset(symbol) || 'UNKNOWN';
  const safeSide = String(side || '').toUpperCase();
  const safeIntent = String(intent || '').toUpperCase();
  const bucket = ref ?? getOrderIntentBucket();
  const nonce = generateOrderNonce();
  return `BOT:${normalized}:${safeSide}:${safeIntent}:${bucket}:${nonce}`;
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

// Backwards-compatible env boolean parser.
// Accepts: true/false, 1/0, yes/no, on/off (case-insensitive).
function readEnvFlag(name, defaultValue = true) {
  const raw = String(process.env[name] ?? '').trim().toLowerCase();
  if (!raw) return defaultValue;
  if (raw === 'true' || raw === '1' || raw === 'yes' || raw === 'y' || raw === 'on') return true;
  if (raw === 'false' || raw === '0' || raw === 'no' || raw === 'n' || raw === 'off') return false;
  return defaultValue;
}

function readFlag(name, defaultValue = false) {
  const v = String(process.env[name] ?? '').trim().toLowerCase();
  if (v === 'true' || v === '1' || v === 'yes') return true;
  if (v === 'false' || v === '0' || v === 'no') return false;
  return defaultValue;
}

function readNumber(name, fallback) {
  const n = Number(process.env[name]);
  return Number.isFinite(n) ? n : fallback;
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

    return DESIRED_NET_PROFIT_BASIS_POINTS;

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

  const minBps = Math.max(DESIRED_NET_PROFIT_BASIS_POINTS, feeFloor);

  console.log('feeAwareMinProfitBasisPoints', {
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
    const quote = await getLatestQuote(normalizedSymbol, { maxAgeMs: ENTRY_QUOTE_MAX_AGE_MS });
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

  const qtyNum = Number(qty);
  if (!Number.isFinite(qtyNum) || qtyNum <= 0) {
    logSkip('invalid_qty', { symbol: normalizedSymbol, qty });
    return { skipped: true, reason: 'invalid_qty' };
  }
  const intendedNotional = qtyNum * Number(limitPrice);

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
    qty: qtyNum,
    notional: intendedNotional,
    price: Number(limitPrice),
    side: 'buy',
    context: 'limit_buy',
  });
  if (sizeGuard.skip) {
    return { skipped: true, reason: 'below_min_trade', notionalUsd: sizeGuard.notional };
  }
  const finalQty = sizeGuard.qty ?? qtyNum;

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
    entryBid: bid,
    entryAsk: ask,
    entrySpreadBps: spreadBps,

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
  let res;
  try {
    res = await fetchAccount();
  } catch (err) {
    const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'account', label: 'account' });
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
  const nowMs = Date.now();
  if (accountCache.data && nowMs - accountCache.tsMs < ACCOUNT_CACHE_TTL_MS) {
    return accountCache.data;
  }
  if (accountCache.pending) {
    return accountCache.pending;
  }
  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'account', label: 'account_raw' });
  accountCache.pending = (async () => {
    const data = await requestJson({
      method: 'GET',
      url,
      headers: alpacaHeaders(),
    });
    accountCache.data = data;
    accountCache.tsMs = Date.now();
    return data;
  })();
  try {
    return await accountCache.pending;
  } finally {
    accountCache.pending = null;
  }
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

async function fetchMaxFillPriceForOrder({ symbol, orderId, lookback = 100 }) {
  if (!orderId) return null;
  try {
    const { items } = await fetchActivities({
      activity_types: 'FILL',
      direction: 'desc',
      page_size: String(lookback),
    });
    const normalizedSymbol = normalizeSymbol(symbol);
    const prices = items
      .filter((item) => {
        const itemOrderId = item?.order_id || item?.orderId || null;
        if (!itemOrderId || String(itemOrderId) !== String(orderId)) {
          return false;
        }
        const itemSymbol = normalizeSymbol(item?.symbol || '');
        return itemSymbol === normalizedSymbol;
      })
      .map((item) => Number(item?.price ?? item?.fill_price ?? item?.transaction_price))
      .filter((price) => Number.isFinite(price) && price > 0);
    if (!prices.length) {
      return null;
    }
    return Math.max(...prices);
  } catch (err) {
    console.warn('fill_activity_fetch_failed', { symbol, orderId, error: err?.message || err });
    return null;
  }
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
  const nowMs = Date.now();
  if (positionsListCache.data && nowMs - positionsListCache.tsMs < OPEN_POSITIONS_CACHE_TTL_MS) {
    return positionsListCache.data;
  }
  if (positionsListCache.pending) {
    return positionsListCache.pending;
  }
  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'positions', label: 'positions' });
  positionsListCache.pending = (async () => {
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
    positionsListCache.data = normalized;
    positionsListCache.tsMs = Date.now();
    return normalized;
  })();
  try {
    return await positionsListCache.pending;
  } finally {
    positionsListCache.pending = null;
  }
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
    // Avoid huge bps rounding on mid-priced coins.
    if (Number.isFinite(priceNum)) {
      if (priceNum < 0.01) return 0.00000001;
      if (priceNum < 0.1) return 0.000001;
      if (priceNum < 1000) return 0.0001;
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

function roundDownToTick(price, symbolOrTick = PRICE_TICK) {
  return roundToTick(price, symbolOrTick, 'down');
}

function getFeeBps({ orderType, isMaker }) {
  const typeLower = String(orderType || '').toLowerCase();
  if (typeLower === 'market') {
    return FEE_BPS_TAKER;
  }
  return isMaker ? FEE_BPS_MAKER : FEE_BPS_TAKER;
}

function computeRequiredExitBpsForNetAfterFees({ entryFeeBps, exitFeeBps, netAfterFeesBps }) {
  const fBuy = Number(entryFeeBps) / 10000;
  const fSell = Number(exitFeeBps) / 10000;
  const r = Number(netAfterFeesBps) / 10000;
  const denom = (1 - fBuy) * (1 - fSell);
  if (!Number.isFinite(denom) || denom <= 0) {
    return 0;
  }
  const g = (1 + r) / denom - 1;
  const requiredExitBps = g * 10000;
  if (!Number.isFinite(requiredExitBps)) {
    return 0;
  }
  return Math.max(0, requiredExitBps);
}

function computeExitPlanNetAfterFees({
  symbol,
  entryPrice,
  entryFeeBps,
  exitFeeBps,
  effectiveEntryPriceOverride,
}) {
  const netAfterFeesBps = EXIT_NET_PROFIT_AFTER_FEES_BPS;
  const effectiveEntryPrice = effectiveEntryPriceOverride ?? entryPrice;
  const requiredExitBps = computeRequiredExitBpsForNetAfterFees({
    entryFeeBps,
    exitFeeBps,
    netAfterFeesBps,
  });
  const tickSize = getTickSize({ symbol, price: effectiveEntryPrice });
  const targetPrice = computeTargetSellPrice(effectiveEntryPrice, requiredExitBps, tickSize);
  const breakevenPrice = Number(effectiveEntryPrice) * (1 + requiredExitBps / 10000);
  return { netAfterFeesBps, effectiveEntryPrice, requiredExitBps, targetPrice, breakevenPrice };
}

function computeSpreadAwareExitBps({ baseRequiredExitBps, spreadBps }) {
  const enabled = (process.env.EXIT_SPREAD_AWARE_ENABLED ?? 'true').toLowerCase() === 'true';
  if (!enabled) return baseRequiredExitBps;

  const mult = Number(process.env.EXIT_SPREAD_BPS_MULTIPLIER ?? 1.0);
  const add = Number(process.env.EXIT_SPREAD_BPS_ADD ?? 0);
  const cap = Number(process.env.EXIT_SPREAD_BPS_CAP ?? 250);
  const floor = Number(process.env.EXIT_SPREAD_BPS_FLOOR ?? 0);

  if (!Number.isFinite(spreadBps)) return baseRequiredExitBps;
  const s = Math.max(floor, Math.min(cap, spreadBps));
  const spreadAllowance = s * mult + add;
  return Math.max(baseRequiredExitBps, spreadAllowance);
}

function resolveRequiredExitBps({
  desiredNetExitBps,
  feeBpsRoundTrip,
  slippageBps,
  spreadBufferBps,
  profitBufferBps,
  maxGrossTakeProfitBps,
}) {
  const desired = Number.isFinite(desiredNetExitBps) ? desiredNetExitBps : DESIRED_NET_PROFIT_BASIS_POINTS;
  const feeBps = Number.isFinite(feeBpsRoundTrip) ? feeBpsRoundTrip : 0;
  const slipBps = Number.isFinite(slippageBps) ? slippageBps : SLIPPAGE_BPS;
  const spreadBuffer = Number.isFinite(spreadBufferBps) ? spreadBufferBps : BUFFER_BPS;
  const bufferBps = Number.isFinite(profitBufferBps) ? profitBufferBps : PROFIT_BUFFER_BPS;
  const rawRequired = Math.max(0, desired) + feeBps + slipBps + spreadBuffer + bufferBps;
  const cap = Number.isFinite(maxGrossTakeProfitBps) ? maxGrossTakeProfitBps : MAX_GROSS_TAKE_PROFIT_BASIS_POINTS;
  const safetyFloor = feeBps + slipBps + spreadBuffer + bufferBps;
  let capped = rawRequired;
  if (Number.isFinite(cap) && cap > 0 && cap < capped) {
    if (cap >= safetyFloor) {
      capped = cap;
    }
  }
  const minGross = MIN_GROSS_TAKE_PROFIT_BASIS_POINTS;
  if (Number.isFinite(minGross) && minGross > 0) {
    capped = Math.max(capped, minGross);
  }
  return capped;
}

function computeMinNetProfitBps({
  feeBpsRoundTrip,
  profitBufferBps,
  desiredNetExitBps,
  slippageBps,
  spreadBufferBps,
  maxGrossTakeProfitBps,
}) {
  return resolveRequiredExitBps({
    desiredNetExitBps,
    feeBpsRoundTrip,
    slippageBps,
    spreadBufferBps,
    profitBufferBps,
    maxGrossTakeProfitBps,
  });
}

// requiredExitBps is the total move above entry (fees + slippage + spread buffer + profit buffer + desired net profit).
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

function resolveOrderQty(order) {
  const normalizedQty = normalizeOrderQty(order);
  if (Number.isFinite(normalizedQty)) return normalizedQty;
  const fallback = Number(order?.qty ?? order?.quantity ?? order?.qty_requested ?? order?.order_qty ?? 0);
  return Number.isFinite(fallback) ? fallback : null;
}

function orderHasValidLimit(order) {
  const limitPrice = normalizeOrderLimitPrice(order) ?? Number(order?.limit);
  if (Number.isFinite(limitPrice) && limitPrice > 0) return true;
  const orderType = String(order?.type ?? order?.order_type ?? '').toLowerCase();
  if (orderType === 'stop_limit') {
    const stopLimitPrice = Number(order?.stop_limit_price ?? order?.stop_limit ?? order?.stopLimitPrice);
    if (Number.isFinite(stopLimitPrice) && stopLimitPrice > 0) return true;
  }
  const nestedLimit = Number(order?.order_type?.limit_price ?? order?.order_type?.limitPrice ?? order?.order_type?.limit);
  if (Number.isFinite(nestedLimit) && nestedLimit > 0) return true;
  const orderClass = String(order?.order_class ?? order?.orderClass ?? '').toLowerCase();
  if (orderClass === 'oco' && Array.isArray(order?.legs)) {
    return order.legs.some((leg) => {
      const side = String(leg?.side || '').toLowerCase();
      if (side !== 'sell') return false;
      return orderHasValidLimit(leg);
    });
  }
  return false;
}

function orderQtyMeetsRequired(orderQty, requiredQty) {
  if (!Number.isFinite(orderQty) || !Number.isFinite(requiredQty)) return false;
  const tol = Math.max(SELL_QTY_MATCH_EPSILON, requiredQty * 1e-6);
  return orderQty + tol >= requiredQty;
}

function hasExitIntentOrder(order, symbol) {
  const clientOrderId = String(order?.client_order_id ?? order?.clientOrderId ?? '');
  if (!clientOrderId) return false;
  const tpPrefix = buildIntentPrefix({ symbol, side: 'SELL', intent: 'TP' });
  const exitPrefix = buildIntentPrefix({ symbol, side: 'SELL', intent: 'EXIT' });
  return (
    clientOrderId.startsWith(tpPrefix) ||
    clientOrderId.startsWith(exitPrefix) ||
    clientOrderId.startsWith('TP_') ||
    clientOrderId.startsWith('EXIT-')
  );
}

function normalizeFilledQty(order) {
  const raw = order?.filled_qty ?? order?.filledQty ?? order?.filled_quantity ?? order?.filledQuantity;
  const num = Number(raw);
  return Number.isFinite(num) ? num : 0;
}

async function waitForFilledOrder({ orderId, timeoutMs = 10000, pollIntervalMs = 1000 }) {
  if (!orderId) return { order: null, filledQty: null, filledAvgPrice: null };
  const start = Date.now();
  let lastOrder = null;
  let filledQty = null;
  let filledAvgPrice = null;
  while (Date.now() - start < timeoutMs) {
    try {
      lastOrder = await fetchOrderById(orderId);
    } catch (err) {
      console.warn('exit_order_fetch_failed', { orderId, error: err?.message || err });
      await sleep(pollIntervalMs);
      continue;
    }
    const qty = normalizeFilledQty(lastOrder);
    const avgPrice = Number(lastOrder?.filled_avg_price ?? lastOrder?.filledAvgPrice ?? lastOrder?.filled_price);
    if (Number.isFinite(qty) && qty > 0) {
      filledQty = qty;
    }
    if (Number.isFinite(avgPrice) && avgPrice > 0) {
      filledAvgPrice = avgPrice;
    }
    if (Number.isFinite(filledQty) && Number.isFinite(filledAvgPrice)) {
      break;
    }
    await sleep(pollIntervalMs);
  }
  return { order: lastOrder, filledQty, filledAvgPrice };
}

async function logExitRealized({
  symbol,
  entryPrice,
  feeBpsRoundTrip,
  entrySpreadBpsUsed,
  heldSeconds,
  reasonCode,
  orderId,
}) {
  if (!orderId) {
    console.warn('exit_realized_missing_order', { symbol, reasonCode });
    return;
  }
  const { filledQty, filledAvgPrice } = await waitForFilledOrder({ orderId });
  const qtyFilled = Number.isFinite(filledQty) ? filledQty : null;
  const exitPrice = Number.isFinite(filledAvgPrice) ? filledAvgPrice : null;
  const entryPriceNum = Number(entryPrice);
  const hasCalcInputs =
    Number.isFinite(entryPriceNum) &&
    entryPriceNum > 0 &&
    Number.isFinite(exitPrice) &&
    exitPrice > 0 &&
    Number.isFinite(qtyFilled) &&
    qtyFilled > 0;
  const grossPnlUsd = hasCalcInputs ? (exitPrice - entryPriceNum) * qtyFilled : null;
  const grossPnlBps = hasCalcInputs ? ((exitPrice - entryPriceNum) / entryPriceNum) * 10000 : null;
  const feeBps = Number.isFinite(feeBpsRoundTrip) ? feeBpsRoundTrip : 0;
  const spreadBps = Number.isFinite(entrySpreadBpsUsed) ? entrySpreadBpsUsed : 0;
  const feeEstimateUsd = hasCalcInputs ? (feeBps / 10000) * entryPriceNum * qtyFilled : null;
  const spreadEstimateUsd = hasCalcInputs ? (spreadBps / 10000) * entryPriceNum * qtyFilled : null;
  const netPnlEstimateUsd =
    hasCalcInputs && Number.isFinite(feeEstimateUsd) && Number.isFinite(spreadEstimateUsd)
      ? grossPnlUsd - feeEstimateUsd - spreadEstimateUsd
      : null;

  console.log('exit_realized', {
    symbol,
    entryPrice: Number.isFinite(entryPriceNum) ? entryPriceNum : null,
    exitPrice,
    qtyFilled,
    grossPnlUsd,
    grossPnlBps,
    feeEstimateUsd,
    spreadEstimateUsd,
    netPnlEstimateUsd,
    heldSeconds,
    reasonCode,
  });
}

function isProfitableExit(entryPrice, exitPrice, feeBpsRoundTrip, profitBufferBps) {
  const entry = Number(entryPrice);
  const exit = Number(exitPrice);
  if (!Number.isFinite(entry) || !Number.isFinite(exit) || entry <= 0) return false;
  const netBps = ((exit - entry) / entry) * 10000;
  return netBps >= computeMinNetProfitBps({ feeBpsRoundTrip, profitBufferBps });
}

function pickSymbolKey(map, primaryKey) {
  if (!map || !primaryKey) return null;
  if (map[primaryKey]) return primaryKey;
  const alt = String(primaryKey).replace('/', '');
  if (map[alt]) return alt;
  return null;
}

function applyCryptoQuoteMaxAgeOverride({ symbol, isCrypto, effectiveMaxAgeMs }) {
  const effectiveMaxAgeMsFinal = isCrypto ? Math.max(effectiveMaxAgeMs, CRYPTO_QUOTE_MAX_AGE_MS) : effectiveMaxAgeMs;
  if (isCrypto && effectiveMaxAgeMsFinal !== effectiveMaxAgeMs && !cryptoQuoteTtlOverrideLogged.has(symbol)) {
    console.log('crypto_quote_ttl_override', { symbol, maxAgeMs: effectiveMaxAgeMsFinal });
    cryptoQuoteTtlOverrideLogged.add(symbol);
  }
  return effectiveMaxAgeMsFinal;
}

async function fetchFallbackTradeQuote(symbol, nowMs, opts = {}) {
  const effectiveMaxAgeMs = Number.isFinite(opts.maxAgeMs) ? opts.maxAgeMs : MAX_QUOTE_AGE_MS;
  const isCrypto = isCryptoSymbol(symbol);
  const effectiveMaxAgeMsFinal = applyCryptoQuoteMaxAgeOverride({ symbol, isCrypto, effectiveMaxAgeMs });
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
  const tKey = pickSymbolKey(res.trades, tradeKey);
  const trade = tKey ? res.trades[tKey] : null;
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
  if (Number.isFinite(ageMs) && ageMs > effectiveMaxAgeMsFinal) {
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

async function getLatestQuote(rawSymbol, opts = {}) {

  const symbol = normalizeSymbol(rawSymbol);
  const effectiveMaxAgeMs = Number.isFinite(opts.maxAgeMs) ? opts.maxAgeMs : MAX_QUOTE_AGE_MS;
  const isCrypto = isCryptoSymbol(symbol);
  const effectiveMaxAgeMsFinal = applyCryptoQuoteMaxAgeOverride({ symbol, isCrypto, effectiveMaxAgeMs });

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
  if (Number.isFinite(cachedAgeMs) && cachedAgeMs <= effectiveMaxAgeMsFinal) {
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

  if (isQuoteCooling(symbol)) {
    logSkip('no_quote', { symbol, reason: 'quote_cooldown' });
    const err = new Error(`Quote cooldown for ${symbol}`);
    err.errorCode = 'QUOTE_COOLDOWN';
    throw err;
  }

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
    const fallback = await fetchFallbackTradeQuote(symbol, nowMs, { maxAgeMs: effectiveMaxAgeMsFinal });
    if (!fallback) return null;
    quoteCache.set(symbol, fallback);
    recordLastQuoteAt(symbol, { tsMs: fallback.tsMs, source: fallback.source });
    recordQuoteSuccess(symbol);
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
  const qKey = pickSymbolKey(res.quotes, quoteKey);
  const quote = qKey ? res.quotes[qKey] : null;
  if (!quote) {
    const fallbackQuote = await tryFallbackTradeQuote();
    if (fallbackQuote) {
      return fallbackQuote;
    }
    if (!quoteKeyMissingLogged.has(symbol)) {
      console.warn('quote_key_missing', { symbol, expectedKeys: [quoteKey, quoteKey.replace('/', '')] });
      quoteKeyMissingLogged.add(symbol);
    }
    const reason = cached ? 'stale_cache' : 'no_data';
    if (cached && Number.isFinite(cachedTsMs)) {
      const lastSeenAge = Number.isFinite(cachedAgeMs)
        ? cachedAgeMs
        : cachedAgeMsRaw;
      logSkip('stale_quote', { symbol, lastSeenAgeSeconds: formatLoggedAgeSeconds(lastSeenAge) });
      recordQuoteFailure(symbol, 'stale_quote');
    } else {
      logSkip('no_quote', { symbol, reason });
      recordQuoteFailure(symbol, 'no_data');
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
    recordQuoteFailure(symbol, 'invalid_bid_ask');
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
    recordQuoteFailure(symbol, 'stale_quote');
    throw new Error(`Quote timestamp missing for ${symbol}`);
  }

  if (Number.isFinite(rawAgeMs) && !Number.isFinite(ageMs)) {
    const fallbackQuote = await tryFallbackTradeQuote();
    if (fallbackQuote) {
      return fallbackQuote;
    }
    logSkip('stale_quote', { symbol, ageSeconds: formatLoggedAgeSeconds(rawAgeMs) });
    recordLastQuoteAt(symbol, { tsMs: null, source: 'stale', reason: 'absurd_age' });
    recordQuoteFailure(symbol, 'stale_quote');
    throw new Error(`Quote age absurd for ${symbol}`);
  }

  if (Number.isFinite(ageMs) && ageMs > effectiveMaxAgeMsFinal) {
    const fallbackQuote = await tryFallbackTradeQuote();
    if (fallbackQuote) {
      return fallbackQuote;
    }
    logSkip('stale_quote', { symbol, ageSeconds: formatLoggedAgeSeconds(ageMs) });
    recordLastQuoteAt(symbol, { tsMs, source: 'stale', reason: 'stale_quote' });
    recordQuoteFailure(symbol, 'stale_quote');
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
  recordQuoteSuccess(symbol);
  return normalizedQuote;

}

async function getLatestQuoteFromQuotesOnly(rawSymbol, opts = {}) {
  const symbol = normalizeSymbol(rawSymbol);
  const effectiveMaxAgeMs = Number.isFinite(opts.maxAgeMs) ? opts.maxAgeMs : MAX_QUOTE_AGE_MS;
  const nowMs = Date.now();
  const isCrypto = isCryptoSymbol(symbol);
  const effectiveMaxAgeMsFinal = applyCryptoQuoteMaxAgeOverride({ symbol, isCrypto, effectiveMaxAgeMs });
  const dataSymbol = isCrypto ? toDataSymbol(symbol) : symbol;
  const url = isCrypto
    ? buildAlpacaUrl({
      baseUrl: CRYPTO_DATA_URL,
      path: 'us/latest/quotes',
      params: { symbols: dataSymbol },
      label: 'crypto_latest_quotes_direct',
    })
    : buildAlpacaUrl({
      baseUrl: STOCKS_DATA_URL,
      path: 'quotes/latest',
      params: { symbols: symbol },
      label: 'stock_latest_quotes_direct',
    });

  let res;
  try {
    res = await requestMarketDataJson({ type: 'QUOTE', url, symbol });
  } catch (err) {
    logHttpError({ symbol, label: 'quotes_direct', url, error: err });
    throw err;
  }

  const quoteKey = isCrypto ? dataSymbol : symbol;
  const quote = res.quotes && res.quotes[quoteKey];
  if (!quote) {
    throw new Error(`Quote not available for ${symbol}`);
  }

  const tsMs = parseQuoteTimestamp({ quote, symbol, source: 'alpaca_quote_direct' });
  const bid = Number(quote.bp ?? quote.bid_price ?? quote.bid);
  const ask = Number(quote.ap ?? quote.ask_price ?? quote.ask);
  const normalizedBid = Number.isFinite(bid) ? bid : null;
  const normalizedAsk = Number.isFinite(ask) ? ask : null;
  const rawAgeMs = Number.isFinite(tsMs) ? computeQuoteAgeMs({ nowMs, tsMs }) : null;
  const ageMs = normalizeQuoteAgeMs(rawAgeMs);
  if (Number.isFinite(rawAgeMs)) {
    logQuoteAgeWarning({ symbol, ageMs: rawAgeMs, source: 'alpaca_direct', tsMs });
  }

  if (!Number.isFinite(normalizedBid) || !Number.isFinite(normalizedAsk) || normalizedBid <= 0 || normalizedAsk <= 0) {
    throw new Error(`Quote bid/ask missing for ${symbol}`);
  }

  if (!Number.isFinite(tsMs)) {
    throw new Error(`Quote timestamp missing for ${symbol}`);
  }

  if (Number.isFinite(rawAgeMs) && !Number.isFinite(ageMs)) {
    throw new Error(`Quote age absurd for ${symbol}`);
  }

  if (Number.isFinite(ageMs) && ageMs > effectiveMaxAgeMsFinal) {
    throw new Error(`Quote stale for ${symbol}`);
  }

  const normalizedQuote = {
    bid: normalizedBid,
    ask: normalizedAsk,
    mid: (normalizedBid + normalizedAsk) / 2,
    tsMs,
    receivedAtMs: nowMs,
    source: 'alpaca_direct',
  };
  quoteCache.set(symbol, normalizedQuote);
  recordLastQuoteAt(symbol, { tsMs, source: 'alpaca_direct' });
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

function parseIsoTsMs(value) {
  if (!value) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function fetchCryptoOrderbooks({ symbols, location = 'us' }) {
  const dataSymbols = symbols.map((s) => toDataSymbol(s));
  const url = buildAlpacaUrl({
    baseUrl: CRYPTO_DATA_URL,
    path: `${location}/latest/orderbooks`,
    params: { symbols: dataSymbols.join(',') },
    label: 'crypto_latest_orderbooks_batch',
  });
  return requestMarketDataJson({ type: 'ORDERBOOK', url, symbol: dataSymbols.join(',') });
}

async function getLatestOrderbook(symbol, { maxAgeMs }) {
  const now = Date.now();
  const cached = orderbookCache.get(symbol);
  if (
    cached &&
    Number.isFinite(cached.receivedAtMs) &&
    (now - cached.receivedAtMs) <= Math.max(250, maxAgeMs)
  ) {
    return cached;
  }

  const resp = await fetchCryptoOrderbooks({ symbols: [symbol], limit: undefined });
  const key = toDataSymbol(symbol);
  const book =
    resp?.orderbooks?.[key] ||
    resp?.orderbooks?.[normalizePair(key)] ||
    resp?.orderbooks?.[symbol] ||
    null;

  const asks = Array.isArray(book?.a) ? book.a : [];
  const bids = Array.isArray(book?.b) ? book.b : [];
  const tsMs = Number.isFinite(book?.t) ? Number(book.t) : parseIsoTsMs(book?.t);

  if (!asks.length || !bids.length) throw new Error(`Orderbook missing levels for ${symbol}`);
  if (!Number.isFinite(tsMs)) throw new Error(`Orderbook timestamp missing for ${symbol}`);

  if (now - tsMs > maxAgeMs) throw new Error(`Orderbook stale for ${symbol}`);

  const normalized = { asks, bids, tsMs, receivedAtMs: now };
  orderbookCache.set(symbol, normalized);
  return normalized;
}

function sumDepthUsdWithinBand(levels, bestPrice, bandBps, side) {
  const band = Math.max(1, bandBps) / 10000;
  const limit = side === 'ask' ? bestPrice * (1 + band) : bestPrice * (1 - band);
  let total = 0;
  for (const lvl of levels) {
    const p = Number(lvl?.p ?? lvl?.price);
    const s = Number(lvl?.s ?? lvl?.size ?? lvl?.q ?? lvl?.qty);
    if (!Number.isFinite(p) || !Number.isFinite(s) || p <= 0 || s <= 0) continue;
    if (side === 'ask') {
      if (p > limit) continue;
    } else if (p < limit) {
      continue;
    }
    total += p * s;
  }
  return total;
}

function estimateBuyImpactBps(asks, bestAsk, notionalUsd) {
  const target = Math.max(1, Number(notionalUsd) || 0);
  let remaining = target;
  let cost = 0;
  let qty = 0;
  for (const lvl of asks) {
    const p = Number(lvl?.p ?? lvl?.price);
    const s = Number(lvl?.s ?? lvl?.size ?? lvl?.q ?? lvl?.qty);
    if (!Number.isFinite(p) || !Number.isFinite(s) || p <= 0 || s <= 0) continue;
    const lvlNotional = p * s;
    const takeNotional = Math.min(remaining, lvlNotional);
    const takeQty = takeNotional / p;
    cost += takeNotional;
    qty += takeQty;
    remaining -= takeNotional;
    if (remaining <= 0) break;
  }
  if (remaining > 0 || qty <= 0) return Infinity;
  const vwap = cost / qty;
  return ((vwap - bestAsk) / bestAsk) * 10000;
}

function computeOrderbookMetrics({ asks, bids }, { bid, ask }) {
  const askDepthUsd = sumDepthUsdWithinBand(asks, ask, ORDERBOOK_BAND_BPS, 'ask');
  const bidDepthUsd = sumDepthUsdWithinBand(bids, bid, ORDERBOOK_BAND_BPS, 'bid');
  const impactBpsBuy = estimateBuyImpactBps(asks, ask, ORDERBOOK_IMPACT_NOTIONAL_USD);

  const denom = Math.max(1, bidDepthUsd + askDepthUsd);
  const imbalance = (bidDepthUsd - askDepthUsd) / denom;
  const obBias = clamp(imbalance * ORDERBOOK_IMBALANCE_BIAS_SCALE, -0.05, 0.05);

  const okDepth = askDepthUsd >= ORDERBOOK_MIN_DEPTH_USD && bidDepthUsd >= (ORDERBOOK_MIN_DEPTH_USD * 0.5);
  const okImpact = Number.isFinite(impactBpsBuy) && impactBpsBuy <= ORDERBOOK_MAX_IMPACT_BPS;

  return { askDepthUsd, bidDepthUsd, impactBpsBuy, imbalance, obBias, ok: okDepth && okImpact };
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

async function fetchOrderByIdThrottled({ symbol, orderId }) {
  if (!orderId) return null;
  const now = Date.now();
  const lastFetchAt = lastOrderFetchAt.get(symbol);
  if (Number.isFinite(lastFetchAt) && now - lastFetchAt < ORDER_FETCH_THROTTLE_MS) {
    return lastOrderSnapshotBySymbol.get(symbol) || null;
  }
  const order = await fetchOrderById(orderId);
  lastOrderFetchAt.set(symbol, now);
  if (order) {
    lastOrderSnapshotBySymbol.set(symbol, order);
  }
  return order;
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

function shouldCancelExitSell() {
  return false; // POLICY: never cancel TP sells
}

async function maybeCancelExitSell({ symbol, orderId, reason }) {
  if (!orderId) return false;
  console.log('exit_cancel_blocked_policy', { symbol, orderId, reason });
  return false;
}

async function submitOcoExit({

  symbol,
  qty,
  entryPrice,
  targetPrice,
  clientOrderId,

}) {

  const stopBps = readNumber('STOP_LOSS_BPS', 60);
  const offBps = readNumber('STOP_LIMIT_OFFSET_BPS', 10);

  const stopPrice = Number(entryPrice) * (1 - stopBps / 10000);
  const stopLimit = stopPrice * (1 - offBps / 10000);

  const payload = {
    side: 'sell',
    symbol: toTradeSymbol(symbol),
    type: 'limit',
    qty: String(qty),
    time_in_force: 'gtc',
    order_class: 'oco',
    client_order_id: clientOrderId,
    take_profit: { limit_price: roundToTick(targetPrice, symbol, 'up') },
    stop_loss: {
      stop_price: roundToTick(stopPrice, symbol, 'down'),
      limit_price: roundToTick(stopLimit, symbol, 'down'),
    },
  };

  if (!payload.client_order_id) {
    delete payload.client_order_id;
  }

  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'orders', label: 'orders_oco_exit' });

  try {
    const res = await requestJson({
      method: 'POST',
      url,
      headers: alpacaJsonHeaders(),
      body: JSON.stringify(payload),
    });
    const id = res?.id || res?.data?.id;
    if (!id) {
      console.warn('oco_exit_rejected', { symbol, reason: 'missing_order_id' });
      return null;
    }
    return res;
  } catch (err) {
    console.warn('oco_exit_rejected', { symbol, err: err?.response?.data || err?.message });
    return null;
  }

}

async function submitLimitSell({

  symbol,

  qty,

  limitPrice,

  reason,
  intentRef,
  openOrders,

}) {

  const open = openOrders || (await fetchLiveOrders());
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
    const status = String(order.status || '').toLowerCase();
    if (orderSymbol !== normalizedSymbol || side !== 'sell') return false;

    // IMPORTANT: only adopt sells that are actually OPEN-like
    if (!isOpenLikeOrderStatus(status)) return false;

    // Must be a real limit TP (avoid adopting market sells / weird records)
    const type = String(order.type || order.order_type || '').toLowerCase();
    if (type && type !== 'limit') return false;

    // Must have a valid limit price
    if (!orderHasValidLimit(order)) return false;

    return true;
  });
  const anySellHistory = openList.some((order) => {
    const orderSymbol = normalizePair(order.symbol || order.rawSymbol);
    const side = String(order.side || '').toLowerCase();
    return orderSymbol === normalizedSymbol && side === 'sell';
  });
  if (anySellHistory && openSellCandidates.length === 0) {
    console.log('sell_history_present_but_no_open_sell', { symbol });
  }
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
      status: String(bestOrder?.status || '').toLowerCase(),
      type: String(bestOrder?.type || bestOrder?.order_type || '').toLowerCase(),
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
  console.log('tp_sell_attempt', {
    symbol,
    qty: finalQty,
    limit_price: roundedLimit,
    tif: payload.time_in_force,
    client_order_id: clientOrderId,
    post_only: false,
    post_only_disabled: true,
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

  const responseStatus = String(response?.status || response?.order_status || '').toLowerCase();
  if (responseStatus === 'rejected' || responseStatus === 'canceled' || responseStatus === 'cancelled') {
    console.error('tp_sell_error', { symbol, status: responseStatus, body: JSON.stringify(response) });
  }

  if (!response?.id) {
    console.warn('tp_sell_missing_id', { symbol });
  }

  if (response?.id) {
    const rawSymbol = response.symbol || symbol;
    const normalizedSymbol = normalizeSymbol(rawSymbol);
    const cachedOrder = {
      id: response.id,
      order_id: response.order_id,
      client_order_id: response.client_order_id || clientOrderId,
      rawSymbol,
      pairSymbol: normalizedSymbol,
      symbol: normalizedSymbol,
      side: response.side || 'sell',
      status: response.status || 'new',
      limit_price: response.limit_price ?? roundedLimit,
      submitted_at: response.submitted_at || new Date().toISOString(),
      created_at: response.created_at,
    };
    const upsertCacheOrder = (cache) => {
      if (!Array.isArray(cache?.data)) return;
      const idx = cache.data.findIndex((order) => (order?.id || order?.order_id) === response.id);
      if (idx >= 0) {
        cache.data[idx] = { ...cache.data[idx], ...cachedOrder };
      } else {
        cache.data.push(cachedOrder);
      }
      cache.tsMs = Date.now();
    };
    upsertCacheOrder(liveOrdersCache);
    upsertCacheOrder(openOrdersCache);
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

  if (DISABLE_IOC_EXITS) {
    console.log('ioc_disabled_market_sell', { symbol, qty: finalQty, reason });
    const order = await submitMarketSell({ symbol, qty: finalQty, reason: `${reason}_market` });
    return { order, requestedQty: finalQty };
  }

  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'orders', label: 'orders_exit_limit_sell' });
  const payload = {
    symbol: toTradeSymbol(symbol),
    qty: finalQty,
    side: 'sell',
    type: 'limit',
    time_in_force: EXIT_LIMIT_SELL_TIF_SAFE,
    limit_price: roundedLimit,
    client_order_id: buildExitClientOrderId(symbol),
  };
  const response = await placeOrderUnified({
    symbol,
    url,
    payload,
    label: 'orders_exit_limit_sell',
    reason,
    context: 'ioc_limit_sell',
  });

  console.log('submit_exit_limit_sell', { symbol, qty: finalQty, limitPrice: roundedLimit, reason, orderId: response?.id });

  return { order: response, requestedQty: finalQty };
}

async function attachInitialExitLimit({ symbol: rawSymbol, qty, entryPrice, entryOrderId = null, maxFill = null }) {
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

  let entrySpreadBpsUsed = Number(entrySpreadOverridesBySymbol.get(symbol));
  if (Number.isFinite(entrySpreadBpsUsed)) {
    entrySpreadOverridesBySymbol.delete(symbol);
  } else {
    try {
      const quote = await getLatestQuote(symbol, { maxAgeMs: EXIT_QUOTE_MAX_AGE_MS });
      const bidNum = Number(quote.bid);
      const askNum = Number(quote.ask);
      if (Number.isFinite(bidNum) && Number.isFinite(askNum) && bidNum > 0) {
        entrySpreadBpsUsed = ((askNum - bidNum) / bidNum) * 10000;
      }
    } catch (err) {
      console.warn('entry_spread_fetch_failed', { symbol, error: err?.message || err });
    }
  }
  if (!Number.isFinite(entrySpreadBpsUsed)) {
    entrySpreadBpsUsed = 0;
    console.warn('entry_spread_unknown', { symbol });
  }

  const entryFeeBps = inferEntryFeeBps({
    symbol,
    orderType: entryOrderType,
    postOnly: entryPostOnly,
  });
  const exitFeeBps = inferExitFeeBps({ takerExitOnTouch: TAKER_EXIT_ON_TOUCH });
  const effectiveEntryPrice = Number.isFinite(maxFill) ? maxFill : entryPriceNum;
  const feeBpsRoundTrip = entryFeeBps + exitFeeBps;
  let desiredNetExitBpsValue = Number.isFinite(desiredExitBpsBySymbol.get(symbol))
    ? desiredExitBpsBySymbol.get(symbol)
    : null;
  if (desiredNetExitBpsValue != null) {
    desiredExitBpsBySymbol.delete(symbol);
  }
  const profitBufferBps = PROFIT_BUFFER_BPS;
  const slippageBpsUsed = SLIPPAGE_BPS;
  const spreadBufferBps = BUFFER_BPS;
  if (!Number.isFinite(desiredNetExitBpsValue)) {
    desiredNetExitBpsValue = DESIRED_NET_PROFIT_BASIS_POINTS;
  }
  if (Number.isFinite(desiredNetExitBpsValue) && desiredNetExitBpsValue < 0) {
    console.log('desired_exit_basis_points_raised', {
      symbol,
      desiredNetExitBasisPoints: desiredNetExitBpsValue,
      floorBasisPoints: 0,
    });
    desiredNetExitBpsValue = 0;
  }
  let baseRequiredExitBps = null;
  let requiredExitBpsFinal = null;
  let netAfterFeesBps = null;
  if (EXIT_MODE === 'net_after_fees') {
    netAfterFeesBps = EXIT_NET_PROFIT_AFTER_FEES_BPS;
    const requiredExitBps = computeRequiredExitBpsForNetAfterFees({
      entryFeeBps,
      exitFeeBps,
      netAfterFeesBps,
    });
    baseRequiredExitBps = requiredExitBps;
    requiredExitBpsFinal = requiredExitBps;
  } else {
    baseRequiredExitBps = resolveRequiredExitBps({
      desiredNetExitBps: desiredNetExitBpsValue,
      feeBpsRoundTrip,
      slippageBps: slippageBpsUsed,
      spreadBufferBps,
      profitBufferBps,
      maxGrossTakeProfitBps: MAX_GROSS_TAKE_PROFIT_BASIS_POINTS,
    });
    requiredExitBpsFinal = baseRequiredExitBps;
  }
  if (Number.isFinite(entrySpreadBpsUsed)) {
    requiredExitBpsFinal = computeSpreadAwareExitBps({
      baseRequiredExitBps: requiredExitBpsFinal,
      spreadBps: entrySpreadBpsUsed,
    });
  }
  const minNetProfitBps = requiredExitBpsFinal;
  const tickSize = getTickSize({ symbol, price: effectiveEntryPrice });
  const targetPrice = computeTargetSellPrice(effectiveEntryPrice, requiredExitBpsFinal, tickSize);
  const breakevenPrice = computeBreakevenPrice(effectiveEntryPrice, minNetProfitBps);
  const postOnly = true;
  const wantOco = !EXIT_POLICY_LOCKED && (process.env.EXIT_ORDER_CLASS || '').toLowerCase() === 'oco';

  console.log('tp_attach_plan', {
    symbol,
    entryPrice: entryPriceNum,
    effectiveEntryPrice,
    maxFillPriceUsed: Number.isFinite(maxFill) ? maxFill : null,
    entryFeeBps,
    exitFeeBps,
    feeBpsRoundTrip,
    desiredNetExitBps: desiredNetExitBpsValue,
    netAfterFeesBps,
    entrySpreadBpsUsed,
    slippageBpsUsed,
    spreadBufferBps,
    baseRequiredExitBps,
    requiredExitBps: requiredExitBpsFinal,
    maxGrossTakeProfitBps: MAX_GROSS_TAKE_PROFIT_BASIS_POINTS,
    breakevenPrice,
    targetPrice,
    takerExitOnTouch: TAKER_EXIT_ON_TOUCH,
    postOnly,
  });

  if (wantOco) {
    const oco = await submitOcoExit({
      symbol,
      qty: qtyNum,
      entryPrice: entryPriceNum,
      targetPrice,
      clientOrderId: buildIntentClientOrderId({
        symbol,
        side: 'SELL',
        intent: 'EXIT_OCO',
        ref: entryOrderId || getOrderIntentBucket(),
      }),
    });
    if (oco && (oco.id || oco.client_order_id)) {
      console.log('oco_exit_attached', { symbol, tp: targetPrice, sl_basis_points: readNumber('STOP_LOSS_BPS', 60) });
      return oco;
    }
    console.warn('oco_exit_fallback_to_legacy', { symbol });
  }

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
    effectiveEntryPrice,
    entryTime: now,
    notionalUsd,
    minNetProfitBps,
    targetPrice,
    feeBpsRoundTrip,
    profitBufferBps,
    desiredNetExitBps: desiredNetExitBpsValue,
    entrySpreadBpsUsed,
    slippageBpsUsed,
    spreadBufferBps,
    requiredExitBps: requiredExitBpsFinal,
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

async function handleBuyFill({

  symbol: rawSymbol,

  qty,

  entryPrice,
  entryOrderId,
  desiredNetExitBps,
  entryBid,
  entryAsk,
  entrySpreadBps,

}) {

  const symbol = normalizeSymbol(rawSymbol);

  const entryPriceNum = Number(entryPrice);

  const qtyNum = Number(qty);

  const notionalUsd = qtyNum * entryPriceNum;

  if (Number.isFinite(Number(desiredNetExitBps))) {
    desiredExitBpsBySymbol.set(symbol, Number(desiredNetExitBps));
  }

  let entrySpreadBpsUsed = Number(entrySpreadBps);
  if (!Number.isFinite(entrySpreadBpsUsed)) {
    const bidNum = Number(entryBid);
    const askNum = Number(entryAsk);
    if (Number.isFinite(bidNum) && Number.isFinite(askNum) && bidNum > 0) {
      entrySpreadBpsUsed = ((askNum - bidNum) / bidNum) * 10000;
    }
  }
  if (!Number.isFinite(entrySpreadBpsUsed)) {
    try {
      const quote = await getLatestQuote(symbol, { maxAgeMs: EXIT_QUOTE_MAX_AGE_MS });
      const bidNum = Number(quote.bid);
      const askNum = Number(quote.ask);
      if (Number.isFinite(bidNum) && Number.isFinite(askNum) && bidNum > 0) {
        entrySpreadBpsUsed = ((askNum - bidNum) / bidNum) * 10000;
      }
    } catch (err) {
      console.warn('entry_spread_fetch_failed', { symbol, error: err?.message || err });
    }
  }
  if (!Number.isFinite(entrySpreadBpsUsed)) {
    entrySpreadBpsUsed = 0;
    console.warn('entry_spread_unknown', { symbol });
  }
  entrySpreadOverridesBySymbol.set(symbol, entrySpreadBpsUsed);

  const maxFill = await fetchMaxFillPriceForOrder({ symbol, orderId: entryOrderId });
  const sellOrder = await attachInitialExitLimit({
    symbol,
    qty: qtyNum,
    entryPrice: entryPriceNum,
    entryOrderId,
    maxFill,
  });

  return sellOrder;

}

async function scanOrphanPositions() {
  let positions = [];
  let openOrders = [];
  try {
    [positions, openOrders] = await Promise.all([
      fetchPositions(),
      fetchOrders({ status: 'open', nested: true, limit: 500 }),
    ]);
  } catch (err) {
    console.warn('orphan_scan_failed', { error: err?.message || err });
    return {
      orphans: [],
      positionsCount: 0,
      openOrdersCount: 0,
      openSellSymbols: [],
    };
  }

  const expandedOrders = expandNestedOrders(openOrders);
  const openSellsBySymbol = expandedOrders.reduce((acc, order) => {
    const side = String(order.side || '').toLowerCase();
    const status = String(order.status || '').toLowerCase();
    if (side !== 'sell' || !isOpenLikeOrderStatus(status)) {
      return acc;
    }
    const orderQty = resolveOrderQty(order);
    if (!Number.isFinite(orderQty) || orderQty <= 0) {
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
    expandedOrders
      .filter((order) => {
        const side = String(order.side || '').toLowerCase();
        const status = String(order.status || '').toLowerCase();
        return side === 'sell' && isOpenLikeOrderStatus(status);
      })
      .map((order) => normalizeSymbol(order.symbol || order.rawSymbol))
  );

  const orphans = [];
  for (const pos of Array.isArray(positions) ? positions : []) {
    const symbol = normalizeSymbol(pos.symbol);
    const qty = Number(pos.qty ?? pos.quantity ?? 0);
    if (!Number.isFinite(qty) || qty <= 0 || isDustQty(qty)) {
      continue;
    }
    const avgEntryPrice = Number(pos.avg_entry_price ?? pos.avgEntryPrice ?? 0);
    const openSellOrders = openSellsBySymbol.get(symbol) || [];
    if (openSellOrders.length === 0) {
      orphans.push({
        symbol,
        qty,
        avgEntryPrice,
        reason: 'no_open_sell',
      });
    }
  }

  return {
    orphans,
    positionsCount: Array.isArray(positions) ? positions.length : 0,
    openOrdersCount: Array.isArray(openOrders) ? openOrders.length : 0,
    openSellSymbols: Array.from(openSellSymbols),
  };
}

async function getCachedOrphanScan() {
  const now = Date.now();
  if (lastOrphanScan.tsMs && now - lastOrphanScan.tsMs < ORPHAN_SCAN_TTL_MS) {
    return lastOrphanScan;
  }
  const report = await scanOrphanPositions();
  lastOrphanScan = { tsMs: now, ...report };
  return lastOrphanScan;
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
    [positions, openOrders] = await Promise.all([
      fetchPositions(),
      fetchOrders({ status: 'open', nested: true, limit: 500 }),
    ]);
  } catch (err) {
    console.warn('exit_repair_fetch_failed', { error: err?.message || err });
    return { placed: 0, skipped: 0, failed: 0 };
  }

  const expandedOrders = expandNestedOrders(openOrders);
  const openSellsBySymbol = expandedOrders.reduce((acc, order) => {
    const side = String(order.side || '').toLowerCase();
    const status = String(order.status || '').toLowerCase();
    if (side !== 'sell' || !isOpenLikeOrderStatus(status)) {
      return acc;
    }
    const orderQty = resolveOrderQty(order);
    if (!Number.isFinite(orderQty) || orderQty <= 0) {
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
    expandedOrders
      .filter((order) => {
        const side = String(order.side || '').toLowerCase();
        const status = String(order.status || '').toLowerCase();
        return side === 'sell' && isOpenLikeOrderStatus(status);
      })
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
  let adopted = 0;
  let positionsChecked = 0;
  let orphansFound = 0;
  const exitsSkippedReasons = new Map();

  console.log('exit_repair_pass_start', {
    positionsChecked: Array.isArray(positions) ? positions.length : 0,
    openSell: openSellSymbols.size,
    openOrdersCount: Array.isArray(openOrders) ? openOrders.length : 0,
    openSellSample: Array.from(openSellSymbols).slice(0, 3),
  });

  for (const [symbol, sellOrders] of openSellsBySymbol.entries()) {
    const qty = positionsBySymbol.get(symbol);
    if (!Number.isFinite(qty) || qty <= 0 || isDustQty(qty)) {
      const hadTracked = exitState.has(symbol);
      exitState.delete(symbol);
      console.log('exit_orphan_cleanup_suppressed', {
        symbol,
        openSellCount: sellOrders.length,
        clearedTracked: hadTracked,
      });
    }
  }

  for (const pos of positions) {
    positionsChecked += 1;
    const symbol = normalizeSymbol(pos.symbol);
    const qty = Number(pos.qty ?? pos.quantity ?? 0);
    const avgEntryPrice = Number(pos.avg_entry_price ?? pos.avgEntryPrice ?? 0);
    const costBasis = Number(pos.cost_basis ?? pos.costBasis ?? 0);
    const orderType = 'limit';
    const timeInForce = 'gtc';
    let bid = null;
    let ask = null;

    try {
      const quote = await getLatestQuote(symbol, { maxAgeMs: EXIT_QUOTE_MAX_AGE_MS });
      bid = quote.bid;
      ask = quote.ask;
    } catch (err) {
      console.warn('exit_repair_quote_failed', { symbol, error: err?.message || err });
    }

    let openSellOrders = openSellsBySymbol.get(symbol) || [];
    let hasOpenSell = openSellOrders.length > 0;
    const hasTrackedExit = exitState.has(symbol);
    let decision = 'SKIP:unknown';
    let targetPrice = null;

    if (hasOpenSell) {
      const openSellQty = openSellOrders.reduce((sum, order) => {
        const orderQty = resolveOrderQty(order);
        return Number.isFinite(orderQty) ? sum + orderQty : sum;
      }, 0);
      const hasValidLimit = openSellOrders.some((order) => orderHasValidLimit(order));
      if (
        !Number.isFinite(openSellQty) ||
        openSellQty <= 0 ||
        isDustQty(openSellQty) ||
        !hasValidLimit
      ) {
        console.log('open_sell_unusable_but_retained', {
          symbol,
          openSellCount: openSellOrders.length,
        });
      }
    }

    if (!Number.isFinite(qty) || qty <= 0) {
      decision = 'SKIP:non_positive_qty';
      skipped += 1;
      exitsSkippedReasons.set('non_positive_qty', (exitsSkippedReasons.get('non_positive_qty') || 0) + 1);
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
      exitsSkippedReasons.set('dust_qty', (exitsSkippedReasons.get('dust_qty') || 0) + 1);
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

    if (!Number.isFinite(avgEntryPrice) || avgEntryPrice <= 0) {
      decision = 'SKIP:missing_cost_basis';
      skipped += 1;
      exitsSkippedReasons.set('missing_cost_basis', (exitsSkippedReasons.get('missing_cost_basis') || 0) + 1);
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
    const slippageBps = Number.isFinite(SLIPPAGE_BPS) ? SLIPPAGE_BPS : null;
    const desiredNetExitBps = Number.isFinite(desiredExitBpsBySymbol.get(symbol))
      ? desiredExitBpsBySymbol.get(symbol)
      : DESIRED_NET_PROFIT_BASIS_POINTS;
    const profitBufferBps = PROFIT_BUFFER_BPS;
    const spreadBufferBps = BUFFER_BPS;
    const spreadBps =
      Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 ? ((ask - bid) / bid) * 10000 : null;
    let requiredExitBps = null;
    let minNetProfitBps = null;
    let netAfterFeesBps = null;
    if (EXIT_MODE === 'net_after_fees') {
      const plan = computeExitPlanNetAfterFees({
        symbol,
        entryPrice: avgEntryPrice,
        entryFeeBps,
        exitFeeBps,
      });
      netAfterFeesBps = plan.netAfterFeesBps;
      requiredExitBps = plan.requiredExitBps;
      minNetProfitBps = plan.requiredExitBps;
      targetPrice = plan.targetPrice;
    } else {
      const baseRequiredExitBps = resolveRequiredExitBps({
        desiredNetExitBps,
        feeBpsRoundTrip,
        slippageBps,
        spreadBufferBps,
        profitBufferBps,
        maxGrossTakeProfitBps: MAX_GROSS_TAKE_PROFIT_BASIS_POINTS,
      });
      requiredExitBps = Number.isFinite(spreadBps)
        ? computeSpreadAwareExitBps({ baseRequiredExitBps, spreadBps })
        : baseRequiredExitBps;
      minNetProfitBps = requiredExitBps;
      const tickSize = getTickSize({ symbol, price: avgEntryPrice });
      targetPrice = computeTargetSellPrice(avgEntryPrice, requiredExitBps, tickSize);
    }
    const postOnly = true;

    if (hasTrackedExit) {
      if (hasOpenSell) {
        const trackedState = exitState.get(symbol) || {};
        exitState.set(symbol, {
          ...trackedState,
          effectiveEntryPrice: avgEntryPrice,
          requiredExitBps,
          minNetProfitBps,
          netAfterFeesBps,
          targetPrice,
        });
        decision = 'OK:tracked_and_has_open_sell';
        skipped += 1;
        exitsSkippedReasons.set('tracked_and_has_open_sell', (exitsSkippedReasons.get('tracked_and_has_open_sell') || 0) + 1);
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

    if (hasOpenSell && !hasTrackedExit) {
      const bestOrder = openSellOrders
        .map((order) => {
          const orderQty = resolveOrderQty(order);
          const limitPrice = normalizeOrderLimitPrice(order);
          return { order, orderQty, limitPrice };
        })
        .filter((item) => Number.isFinite(item.orderQty) && item.orderQty > 0 && Number.isFinite(item.limitPrice))
        .reduce((best, current) => {
          if (!best) return current;
          if (current.orderQty > best.orderQty) return current;
          if (current.orderQty < best.orderQty) return best;
          if (!Number.isFinite(targetPrice)) return best;
          const bestDiff = Math.abs(best.limitPrice - targetPrice);
          const currentDiff = Math.abs(current.limitPrice - targetPrice);
          return currentDiff < bestDiff ? current : best;
        }, null);
      if (bestOrder) {
        const adoptedOrder = bestOrder.order;
        const adoptedOrderId = adoptedOrder?.id || adoptedOrder?.order_id || null;
        const sellOrderSubmittedAtRaw =
          adoptedOrder?.submitted_at ||
          adoptedOrder?.submittedAt ||
          adoptedOrder?.created_at ||
          adoptedOrder?.createdAt ||
          null;
        const sellOrderSubmittedAt = sellOrderSubmittedAtRaw ? Date.parse(sellOrderSubmittedAtRaw) : null;
        const now = Date.now();
        const sellOrderSubmittedAtMs = Number.isFinite(sellOrderSubmittedAt) ? sellOrderSubmittedAt : now;
        const entryTime = Number.isFinite(sellOrderSubmittedAt) ? sellOrderSubmittedAt : now;
        exitState.set(symbol, {
          symbol,
          qty,
          entryPrice: avgEntryPrice,
          effectiveEntryPrice: avgEntryPrice,
          entryTime,
          notionalUsd,
          minNetProfitBps,
          targetPrice,
          feeBpsRoundTrip,
          profitBufferBps,
          desiredNetExitBps,
          slippageBpsUsed: slippageBps,
          spreadBufferBps,
          entryFeeBps,
          exitFeeBps,
          requiredExitBps,
          netAfterFeesBps,
          sellOrderId: adoptedOrderId,
          sellOrderSubmittedAt: sellOrderSubmittedAtMs,
          sellOrderLimit: bestOrder.limitPrice,
          takerAttempted: false,
          entryOrderId: null,
        });
        desiredExitBpsBySymbol.delete(symbol);
        console.log('exit_repair_adopt_open_sell', {
          symbol,
          adoptedOrderId,
          adoptedLimitPrice: bestOrder.limitPrice,
          qty,
          entryPrice: avgEntryPrice,
          targetPrice,
        });
        decision = 'ADOPT:open_sell_untracked';
        adopted += 1;
        exitsSkippedReasons.set('adopt_open_sell_untracked', (exitsSkippedReasons.get('adopt_open_sell_untracked') || 0) + 1);
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
    }

    if (hasOpenSell) {
      decision = 'SKIP:open_sell';
      skipped += 1;
      exitsSkippedReasons.set('open_sell', (exitsSkippedReasons.get('open_sell') || 0) + 1);
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
      exitsSkippedReasons.set('auto_sell_disabled', (exitsSkippedReasons.get('auto_sell_disabled') || 0) + 1);
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
      exitsSkippedReasons.set('exits_disabled', (exitsSkippedReasons.get('exits_disabled') || 0) + 1);
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
      exitsSkippedReasons.set('live_mode_disabled', (exitsSkippedReasons.get('live_mode_disabled') || 0) + 1);
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

    orphansFound += 1;
    console.warn('exit_orphan_detected', { symbol, qty, avg_entry_price: avgEntryPrice });
    console.warn('exit_orphan_gates', { symbol, gates: gateFlags });

    if (ORPHAN_AUTO_ATTACH_TP && autoSellEnabled && exitsEnabled && liveMode) {
      try {
        const sellOrder = await attachInitialExitLimit({
          symbol,
          qty,
          entryPrice: avgEntryPrice,
          entryOrderId: null,
          maxFill: null,
        });
        const orderId = sellOrder?.id || sellOrder?.order_id || null;
        const exitSnapshot = exitState.get(symbol) || {};
        const loggedTargetPrice = normalizeOrderLimitPrice(sellOrder) ?? exitSnapshot.targetPrice ?? null;
        placed += 1;
        decision = 'PLACED:repair_attached_tp';
        console.log('exit_orphan_repaired', { symbol, qty, targetPrice: loggedTargetPrice, orderId });
      } catch (err) {
        failed += 1;
        decision = 'FAILED:repair_attach_tp';
        console.warn('exit_orphan_repair_failed', { symbol, error: err?.message || err });
      }
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

    console.warn('exit_orphan_action_required', { symbol, qty, targetPrice, note: 'manual_sell_required' });
    decision = 'SKIP:manual_sell_required';
    skipped += 1;
    exitsSkippedReasons.set('manual_sell_required', (exitsSkippedReasons.get('manual_sell_required') || 0) + 1);
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

  const exitSkipSummary = Array.from(exitsSkippedReasons.entries())
    .sort((a, b) => b[1] - a[1])
    .reduce((acc, [reason, count]) => {
      acc[reason] = count;
      return acc;
    }, {});
  console.log('exit_repair_pass_done', {
    positionsChecked,
    orphansFound,
    exitsPlaced: placed,
    exitsSkippedReasons: exitSkipSummary,
    skipped,
    failed,
    adopted,
  });
  return { placed, skipped, failed, adopted };
}

async function repairOrphanExitsSafe() {
  if (exitRepairRunning) {
    return;
  }
  exitRepairRunning = true;
  try {
    await repairOrphanExits();
  } finally {
    exitRepairRunning = false;
  }
}

async function manageExitStates() {

  if (exitManagerRunning) {
    console.warn('exit_manager_skip_concurrent');
    return;
  }
  exitManagerRunning = true;

  try {
    const nowMs = Date.now();
    if (nowMs - lastExitRepairAtMs >= EXIT_REPAIR_INTERVAL_MS) {
      await repairOrphanExitsSafe();
      lastExitRepairAtMs = nowMs;
    } else {
      console.log('exit_repair_skip_interval', {
        nextInMs: EXIT_REPAIR_INTERVAL_MS - (nowMs - lastExitRepairAtMs),
      });
    }
    const now = nowMs;
    let openOrders = [];
    try {
      openOrders = await fetchLiveOrders();
    } catch (err) {
      console.warn('exit_manager_open_orders_failed', { error: err?.message || err });
    }
    const openOrdersList = Array.isArray(openOrders) ? openOrders : [];
    if (openOrdersList.length) {
      for (const [symbol, state] of exitState.entries()) {
        if (state?.sellOrderId) continue;
        const normalizedSymbol = normalizePair(symbol);
        const requiredQty = Number(state?.qty ?? 0);
        if (!Number.isFinite(requiredQty) || requiredQty <= 0) {
          continue;
        }
        const candidates = openOrdersList.filter((order) => {
          const orderSymbol = normalizePair(order.symbol || order.rawSymbol);
          const side = String(order.side || '').toLowerCase();
          if (orderSymbol !== normalizedSymbol || side !== 'sell') return false;
          const orderQty = normalizeOrderQty(order);
          return orderQtyMeetsRequired(orderQty, requiredQty);
        });
        if (!candidates.length) continue;
        const tpPrefix = buildIntentPrefix({ symbol: normalizedSymbol, side: 'SELL', intent: 'TP' });
        const exitPrefix = buildIntentPrefix({ symbol: normalizedSymbol, side: 'SELL', intent: 'EXIT' });
        const preferred = candidates.filter((order) => {
          const clientOrderId = String(order?.client_order_id ?? order?.clientOrderId ?? '');
          return clientOrderId.startsWith(tpPrefix) || clientOrderId.startsWith(exitPrefix);
        });
        const chosen = (preferred.length ? preferred : candidates)[0];
        const adoptedOrderId = chosen?.id || chosen?.order_id || null;
        if (!adoptedOrderId) continue;
        const submittedAtRaw =
          chosen?.submitted_at || chosen?.submittedAt || chosen?.created_at || chosen?.createdAt || null;
        const submittedAt = submittedAtRaw ? Date.parse(submittedAtRaw) : null;
        const submittedAtMs = Number.isFinite(submittedAt) ? submittedAt : Date.now();
        const limitPrice = normalizeOrderLimitPrice(chosen);
        state.sellOrderId = adoptedOrderId;
        state.sellOrderSubmittedAt = submittedAtMs;
        state.sellOrderLimit = limitPrice;
        console.log('adopt_existing_sell_on_restart', {
          symbol,
          orderId: adoptedOrderId,
          limitPrice,
          matchedQty: requiredQty,
          intentTagged: preferred.length > 0,
        });
      }
    }
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
        const heldMs = Math.max(0, now - state.entryTime);
        const heldSeconds = heldMs / 1000;
        const symbolOrders = openOrdersBySymbol.get(normalizePair(symbol)) || [];
        const openBuyCount = symbolOrders.filter((order) => String(order.side || '').toLowerCase() === 'buy').length;
        const openSellCount = symbolOrders.filter((order) => String(order.side || '').toLowerCase() === 'sell').length;

        let bid = null;

        let ask = null;
        let quoteFetchFailed = false;
        let quoteStale = false;

        try {

          const quote = await getLatestQuote(symbol, { maxAgeMs: EXIT_QUOTE_MAX_AGE_MS });

          bid = quote.bid;

          ask = quote.ask;
          state.lastBid = Number.isFinite(bid) ? bid : state.lastBid;
          state.lastAsk = Number.isFinite(ask) ? ask : state.lastAsk;
          if (Number.isFinite(bid) && Number.isFinite(ask)) {
            state.lastMid = (bid + ask) / 2;
          }
          state.lastQuoteTsMs = Number.isFinite(quote.tsMs) ? quote.tsMs : state.lastQuoteTsMs;
          state.lastQuoteSource = quote.source || state.lastQuoteSource;
          state.staleQuoteSkipAt = null;

        } catch (err) {

          console.warn('quote_fetch_failed', { symbol, error: err?.message || err });
          quoteFetchFailed = isNetworkError(err);
          quoteStale = !quoteFetchFailed && isStaleQuoteError(err);

        }

        if (quoteFetchFailed) {
          console.warn('exit_manager_skip_orders', { symbol, reason: 'quote_network_error' });
          continue;
        }

        let actionTaken = 'none';
        let reasonCode = 'hold';
        let spreadBps =
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
        const slippageBps = Number.isFinite(state.slippageBpsUsed) ? state.slippageBpsUsed : SLIPPAGE_BPS;
        const spreadBufferBps = Number.isFinite(state.spreadBufferBps) ? state.spreadBufferBps : BUFFER_BPS;
        const desiredNetExitBps = Number.isFinite(state.desiredNetExitBps) ? state.desiredNetExitBps : null;
        const useNetAfterFeesMode = EXIT_MODE === 'net_after_fees';
        let exitEntryPrice = Number.isFinite(state.effectiveEntryPrice) ? state.effectiveEntryPrice : state.entryPrice;
        let requiredExitBps = Number.isFinite(state.requiredExitBps) ? state.requiredExitBps : null;
        let minNetProfitBps = Number.isFinite(state.requiredExitBps) ? state.requiredExitBps : null;
        let targetPrice = null;
        let baseRequiredExitBps = requiredExitBps;
        let requiredExitBpsFinal = requiredExitBps;
        if (useNetAfterFeesMode) {
          const plan = computeExitPlanNetAfterFees({
            symbol,
            entryPrice: state.entryPrice,
            entryFeeBps,
            exitFeeBps,
            effectiveEntryPriceOverride: Number.isFinite(state.effectiveEntryPrice)
              ? state.effectiveEntryPrice
              : state.entryPrice,
          });
          state.netAfterFeesBps = plan.netAfterFeesBps;
          state.effectiveEntryPrice = plan.effectiveEntryPrice;
          exitEntryPrice = plan.effectiveEntryPrice;
          baseRequiredExitBps = plan.requiredExitBps;
          requiredExitBpsFinal = baseRequiredExitBps;
          if (Number.isFinite(bid) && Number.isFinite(ask)) {
            requiredExitBpsFinal = computeSpreadAwareExitBps({ baseRequiredExitBps, spreadBps });
          }
          requiredExitBps = requiredExitBpsFinal;
          minNetProfitBps = requiredExitBpsFinal;
        } else {
          state.netAfterFeesBps = null;
          requiredExitBps = Number.isFinite(requiredExitBps)
            ? requiredExitBps
            : resolveRequiredExitBps({
              desiredNetExitBps,
              feeBpsRoundTrip,
              slippageBps,
              spreadBufferBps,
              profitBufferBps,
              maxGrossTakeProfitBps: MAX_GROSS_TAKE_PROFIT_BASIS_POINTS,
            });
          minNetProfitBps = Number.isFinite(minNetProfitBps)
            ? minNetProfitBps
            : computeMinNetProfitBps({
              feeBpsRoundTrip,
              profitBufferBps,
              desiredNetExitBps,
              slippageBps,
              spreadBufferBps,
              maxGrossTakeProfitBps: MAX_GROSS_TAKE_PROFIT_BASIS_POINTS,
            });
          baseRequiredExitBps = Number.isFinite(baseRequiredExitBps) ? baseRequiredExitBps : requiredExitBps;
          requiredExitBpsFinal = baseRequiredExitBps;
          if (Number.isFinite(bid) && Number.isFinite(ask)) {
            requiredExitBpsFinal = computeSpreadAwareExitBps({ baseRequiredExitBps, spreadBps });
          }
          requiredExitBps = requiredExitBpsFinal;
          minNetProfitBps = requiredExitBpsFinal;
        }
        baseRequiredExitBps = Number.isFinite(baseRequiredExitBps) ? baseRequiredExitBps : requiredExitBps;
        requiredExitBpsFinal = Number.isFinite(requiredExitBpsFinal) ? requiredExitBpsFinal : requiredExitBps;
        const tickSize = getTickSize({ symbol, price: exitEntryPrice });
        targetPrice = computeTargetSellPrice(exitEntryPrice, requiredExitBpsFinal, tickSize);
        state.targetPrice = targetPrice;
        state.minNetProfitBps = minNetProfitBps;
        state.feeBpsRoundTrip = feeBpsRoundTrip;
        state.profitBufferBps = profitBufferBps;
        state.slippageBpsUsed = slippageBps;
        state.spreadBufferBps = spreadBufferBps;
        state.desiredNetExitBps = desiredNetExitBps;
        state.requiredExitBps = requiredExitBpsFinal;
        state.entryFeeBps = entryFeeBps;
        state.exitFeeBps = exitFeeBps;
        state.effectiveEntryPrice = exitEntryPrice;
        const breakevenPrice = computeBreakevenPrice(exitEntryPrice, minNetProfitBps);
        state.breakevenPrice = breakevenPrice;
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
        const hasOpenSell = openSellOrders.length > 0 || Boolean(state.sellOrderId);

        if (quoteStale) {
          const lastKnownBid = Number.isFinite(state.lastBid) ? state.lastBid : null;
          const lastKnownAsk = Number.isFinite(state.lastAsk) ? state.lastAsk : null;
          const lastKnownMid = Number.isFinite(state.lastMid)
            ? state.lastMid
            : (Number.isFinite(lastKnownBid) && Number.isFinite(lastKnownAsk) ? (lastKnownBid + lastKnownAsk) / 2 : null);
          let fallbackBase = Number.isFinite(lastKnownAsk) ? lastKnownAsk : lastKnownMid;

          // Backfill bid/ask for logging + spread math when quote is stale
          if (!Number.isFinite(bid) && Number.isFinite(state.lastBid)) bid = state.lastBid;
          if (!Number.isFinite(ask) && Number.isFinite(state.lastAsk)) ask = state.lastAsk;

          // If still missing, try a quotes-only fetch once using the relaxed exit TTL
          if (!Number.isFinite(bid) || !Number.isFinite(ask)) {
            try {
              const q2 = await getLatestQuoteFromQuotesOnly(symbol, { maxAgeMs: EXIT_QUOTE_MAX_AGE_MS });
              if (Number.isFinite(q2?.bid)) bid = q2.bid;
              if (Number.isFinite(q2?.ask)) ask = q2.ask;
              if (Number.isFinite(bid)) state.lastBid = bid;
              if (Number.isFinite(ask)) state.lastAsk = ask;
              if (Number.isFinite(bid) && Number.isFinite(ask)) state.lastMid = (bid + ask) / 2;
            } catch (_) {}
          }

          // Recompute spreadBps if bid/ask are now available
          spreadBps =
            Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 ? ((ask - bid) / bid) * 10000 : null;

          if (hasOpenSell) {
            actionTaken = 'hold_existing_order';
            reasonCode = 'stale_quote_keep_order';
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
              baseRequiredExitBps,
              requiredExitBpsFinal,
              openBuyCount,
              openSellCount,
              existingOrderAgeMs: null,
              feeBpsRoundTrip,
              profitBufferBps,
              minNetProfitBps,
              targetPrice,
              breakevenPrice,
              desiredLimit,
              decisionPath: 'stale_quote_keep_order',
              lastRepriceAgeMs: lastRepriceAgeMs,
              lastCancelReplaceAt: lastCancelReplaceAtMs,
              actionTaken,
              reasonCode,
              iocRequestedQty: null,
              iocFilledQty: null,
              iocFallbackReason: null,
            });
            continue;
          }

          if (!Number.isFinite(fallbackBase) && state.staleQuoteSkipAt) {
            try {
              const directQuote = await getLatestQuoteFromQuotesOnly(symbol, { maxAgeMs: EXIT_QUOTE_MAX_AGE_MS });
              bid = directQuote.bid;
              ask = directQuote.ask;
              state.lastBid = Number.isFinite(bid) ? bid : state.lastBid;
              state.lastAsk = Number.isFinite(ask) ? ask : state.lastAsk;
              if (Number.isFinite(bid) && Number.isFinite(ask)) {
                state.lastMid = (bid + ask) / 2;
              }
              state.lastQuoteTsMs = Number.isFinite(directQuote.tsMs) ? directQuote.tsMs : state.lastQuoteTsMs;
              state.lastQuoteSource = directQuote.source || state.lastQuoteSource;
              fallbackBase = Number.isFinite(ask) ? ask : state.lastMid;
            } catch (err) {
              console.warn('exit_stale_quote_retry_failed', { symbol, error: err?.message || err });
            }
          }

          if (Number.isFinite(fallbackBase)) {
            const conservativeLimit = roundToTick(fallbackBase, tickSize, 'up');
            const replacement = await submitLimitSell({
              symbol,
              qty: state.qty,
              limitPrice: conservativeLimit,
              reason: 'stale_quote_fallback',
              intentRef: state.entryOrderId || getOrderIntentBucket(),
              postOnly: true,
              openOrders: symbolOrders,
            });
            if (!replacement?.skipped && replacement?.id) {
              state.sellOrderId = replacement.id;
              state.sellOrderSubmittedAt = Date.now();
              state.sellOrderLimit = normalizeOrderLimitPrice(replacement) ?? conservativeLimit;
              actionTaken = 'placed_stale_fallback';
              reasonCode = 'stale_quote_fallback';
              lastActionAt.set(symbol, now);
            } else {
              actionTaken = 'hold_existing_order';
              reasonCode = replacement?.reason || 'stale_quote_fallback_skipped';
            }
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
              baseRequiredExitBps,
              requiredExitBpsFinal,
              openBuyCount,
              openSellCount,
              existingOrderAgeMs: null,
              feeBpsRoundTrip,
              profitBufferBps,
              minNetProfitBps,
              targetPrice,
              breakevenPrice,
              desiredLimit,
              decisionPath: 'stale_quote_fallback',
              lastRepriceAgeMs: lastRepriceAgeMs,
              lastCancelReplaceAt: lastCancelReplaceAtMs,
              actionTaken,
              reasonCode,
              iocRequestedQty: null,
              iocFilledQty: null,
              iocFallbackReason: null,
            });
            state.staleQuoteSkipAt = null;
            continue;
          }

          if (!state.staleQuoteSkipAt) {
            state.staleQuoteSkipAt = now;
            console.warn('exit_stale_quote_skip', {
              symbol,
              reason: 'no_last_price',
              retry: 'next_cycle_latest_quotes',
            });
          }
          actionTaken = 'hold_existing_order';
          reasonCode = 'stale_quote_no_price';
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
            baseRequiredExitBps,
            requiredExitBpsFinal,
            openBuyCount,
            openSellCount,
            existingOrderAgeMs: null,
            feeBpsRoundTrip,
            profitBufferBps,
            minNetProfitBps,
            targetPrice,
            breakevenPrice,
            desiredLimit,
            decisionPath: 'stale_quote_no_price',
            lastRepriceAgeMs: lastRepriceAgeMs,
            lastCancelReplaceAt: lastCancelReplaceAtMs,
            actionTaken,
            reasonCode,
            iocRequestedQty: null,
            iocFilledQty: null,
            iocFallbackReason: null,
          });
          continue;
        }
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
          console.log('multiple_open_sells_detected', {
            symbol,
            keptId: keepId,
            count: openSellOrders.length,
          });
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

        const takerOnTouch = EXIT_POLICY_LOCKED ? false : EXIT_TAKER_ON_TOUCH_ENABLED;
        const lastActionAtMs = lastActionAt.get(symbol);
        const takerTouchCooldownActive =
          Number.isFinite(TAKER_TOUCH_MIN_INTERVAL_MS) &&
          TAKER_TOUCH_MIN_INTERVAL_MS > 0 &&
          Number.isFinite(lastActionAtMs) &&
          now - lastActionAtMs < TAKER_TOUCH_MIN_INTERVAL_MS;

        if (takerOnTouch && Number.isFinite(bid) && bid >= targetPrice && !takerTouchCooldownActive) {
          if (state.sellOrderId) {
            await maybeCancelExitSell({
              symbol,
              orderId: state.sellOrderId,
              reason: 'target_touch_taker',
            });
          }
          const iocLimitPrice = roundDownToTick(bid, symbol);
          const iocResult = await submitIocLimitSell({
            symbol,
            qty: state.qty,
            limitPrice: iocLimitPrice,
            reason: 'target_touch',
          });
          console.log('target_touch_taker', { symbol, targetPrice, bid, qty: state.qty, iocLimitPrice });
          if (!iocResult?.skipped) {
            const requestedQty = iocResult.requestedQty;
            const filledQty = normalizeFilledQty(iocResult.order);
            const remainingQty =
              Number.isFinite(requestedQty) && Number.isFinite(filledQty)
                ? Math.max(requestedQty - filledQty, 0)
                : null;
            let realizedOrderId = iocResult?.order?.id || iocResult?.order?.order_id || null;
            if (remainingQty && remainingQty > 0) {
              const marketOrder = await submitMarketSell({ symbol, qty: remainingQty, reason: 'target_touch_fallback' });
              realizedOrderId = marketOrder?.id || marketOrder?.order_id || realizedOrderId;
            }
            exitState.delete(symbol);
            actionTaken = 'target_touch_taker';
            reasonCode = 'target_touch_taker';
            lastActionAt.set(symbol, now);
            await logExitRealized({
              symbol,
              entryPrice: state.entryPrice,
              feeBpsRoundTrip,
              entrySpreadBpsUsed: state.entrySpreadBpsUsed,
              heldSeconds,
              reasonCode,
              orderId: realizedOrderId,
            });
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
              baseRequiredExitBps,
              requiredExitBpsFinal,
              openBuyCount,
              openSellCount,
              existingOrderAgeMs: null,
              feeBpsRoundTrip,
              profitBufferBps,
              minNetProfitBps,
              targetPrice,
              breakevenPrice,
              desiredLimit,
              decisionPath: 'target_touch_taker',
              lastRepriceAgeMs: lastRepriceAgeMs,
              lastCancelReplaceAt: lastCancelReplaceAtMs,
              actionTaken,
              reasonCode,
              iocRequestedQty: requestedQty ?? null,
              iocFilledQty: filledQty ?? null,
              iocFallbackReason: remainingQty && remainingQty > 0 ? 'target_touch_fallback' : null,
            });
            continue;
          }
        }

        const slBps = STOP_LOSS_BPS;
        if (EXIT_MARKET_EXITS_ENABLED && slBps > 0 && Number.isFinite(state.entryPrice) && Number.isFinite(bid)) {
          const stopTrigger = state.entryPrice * (1 - slBps / 10000);
          if (bid <= stopTrigger) {
            console.log('hard_stop_trigger', { symbol, entryPrice: state.entryPrice, bid, slBps });
            if (state.sellOrderId) {
              await maybeCancelExitSell({
                symbol,
                orderId: state.sellOrderId,
                reason: 'hard_stop_trigger',
              });
            }
            const ioc = await submitIocLimitSell({
              symbol,
              qty: state.qty,
              limitPrice: bid,
              reason: 'hard_stop',
            });
            const iocStatus = String(ioc?.order?.status || '').toLowerCase();
            const requestedQty = ioc?.requestedQty;
            const filledQty = normalizeFilledQty(ioc?.order);
            const remainingQty =
              Number.isFinite(requestedQty) && Number.isFinite(filledQty)
                ? Math.max(requestedQty - filledQty, 0)
                : null;
            let realizedOrderId = ioc?.order?.id || ioc?.order?.order_id || null;
            if (
              ioc?.skipped ||
              remainingQty == null ||
              remainingQty > 0 ||
              ['canceled', 'expired', 'rejected'].includes(iocStatus)
            ) {
              const marketOrder = await submitMarketSell({
                symbol,
                qty: Number.isFinite(remainingQty) && remainingQty > 0 ? remainingQty : state.qty,
                reason: 'hard_stop_market',
              });
              realizedOrderId = marketOrder?.id || marketOrder?.order_id || realizedOrderId;
            }
            exitState.delete(symbol);
            actionTaken = 'hard_stop_exit';
            reasonCode = 'hard_stop';
            lastActionAt.set(symbol, now);
            await logExitRealized({
              symbol,
              entryPrice: state.entryPrice,
              feeBpsRoundTrip,
              entrySpreadBpsUsed: state.entrySpreadBpsUsed,
              heldSeconds,
              reasonCode,
              orderId: realizedOrderId,
            });
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
              baseRequiredExitBps,
              requiredExitBpsFinal,
              openBuyCount,
              openSellCount,
              existingOrderAgeMs: null,
              feeBpsRoundTrip,
              profitBufferBps,
              minNetProfitBps,
              targetPrice,
              breakevenPrice,
              desiredLimit,
              decisionPath: 'hard_stop',
              lastRepriceAgeMs: lastRepriceAgeMs,
              lastCancelReplaceAt: lastCancelReplaceAtMs,
              actionTaken,
              reasonCode,
              iocRequestedQty: requestedQty ?? null,
              iocFilledQty: filledQty ?? null,
              iocFallbackReason: remainingQty && remainingQty > 0 ? 'hard_stop_market' : null,
            });
            continue;
          }
        }

      if (EXIT_MARKET_EXITS_ENABLED && FORCE_EXIT_SECONDS > 0 && heldSeconds >= FORCE_EXIT_SECONDS) {
        const allowLossExit = FORCE_EXIT_ALLOW_LOSS;
        const canExitProfitably = Number.isFinite(bid) && bid >= breakevenPrice;
        const wantOco = !EXIT_POLICY_LOCKED && (process.env.EXIT_ORDER_CLASS || '').toLowerCase() === 'oco';

        if (allowLossExit || canExitProfitably) {
          if (state.sellOrderId) {
            await maybeCancelExitSell({
              symbol,
              orderId: state.sellOrderId,
              reason: 'force_exit_timeout',
            });
          }

          const marketOrder = await submitMarketSell({
            symbol,
            qty: state.qty,
            reason: allowLossExit ? 'kill_switch' : 'timeout_exit',
          });

          exitState.delete(symbol);

          actionTaken = allowLossExit ? 'forced_exit_timeout' : 'timeout_exit_profit';
          reasonCode = allowLossExit ? 'kill_switch' : 'timeout_exit';
          lastActionAt.set(symbol, now);
          await logExitRealized({
            symbol,
            entryPrice: state.entryPrice,
            feeBpsRoundTrip,
            entrySpreadBpsUsed: state.entrySpreadBpsUsed,
            heldSeconds,
            reasonCode,
            orderId: marketOrder?.id || marketOrder?.order_id || null,
          });
        } else if (wantOco && openSellCount === 0 && !state.sellOrderId) {
          const oco = await submitOcoExit({
            symbol,
            qty: state.qty,
            entryPrice: state.entryPrice,
            targetPrice,
            clientOrderId: buildIntentClientOrderId({
              symbol,
              side: 'SELL',
              intent: 'EXIT_OCO',
              ref: state.entryOrderId || getOrderIntentBucket(),
            }),
          });
          if (oco?.id || oco?.order_id) {
            state.sellOrderId = oco.id || oco.order_id;
            state.sellOrderSubmittedAt = Date.now();
          }
          actionTaken = oco?.id || oco?.order_id ? 'oco_exit_attached' : 'timeout_exit_hold';
          reasonCode = oco?.id || oco?.order_id ? 'timeout_exit_oco' : 'timeout_exit_not_profitable';
        } else {
          actionTaken = 'timeout_exit_hold';
          reasonCode = 'timeout_exit_not_profitable';
        }

        console.log('forced_exit_elapsed', { symbol, heldSeconds, limitSeconds: FORCE_EXIT_SECONDS });
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
          baseRequiredExitBps,
          requiredExitBpsFinal,
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


      if (EXIT_MARKET_EXITS_ENABLED && heldMs >= maxHoldMs && Number.isFinite(bid)) {

        const netProfitBps = ((bid - state.entryPrice) / state.entryPrice) * 10000;

        if (netProfitBps >= minNetProfitBps) {

          if (state.sellOrderId) {
            await maybeCancelExitSell({
              symbol,
              orderId: state.sellOrderId,
              reason: 'max_hold_exit',
            });
          }

          const marketOrder = await submitMarketSell({ symbol, qty: state.qty, reason: 'max_hold' });

          exitState.delete(symbol);

          actionTaken = 'max_hold_exit';
          reasonCode = 'max_hold';
          lastActionAt.set(symbol, now);
          await logExitRealized({
            symbol,
            entryPrice: state.entryPrice,
            feeBpsRoundTrip,
            entrySpreadBpsUsed: state.entrySpreadBpsUsed,
            heldSeconds,
            reasonCode,
            orderId: marketOrder?.id || marketOrder?.order_id || null,
          });
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
            baseRequiredExitBps,
            requiredExitBpsFinal,
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

      if (EXIT_MARKET_EXITS_ENABLED && ALLOW_TAKER_BEFORE_TARGET && bidMeetsBreakeven && Number.isFinite(bid)) {
        if (state.sellOrderId) {
          await maybeCancelExitSell({
            symbol,
            orderId: state.sellOrderId,
            reason: 'taker_before_target',
          });
          if (shouldCancelExitSell()) {
            lastCancelReplaceAt.set(symbol, now);
          }
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
          await logExitRealized({
            symbol,
            entryPrice: state.entryPrice,
            feeBpsRoundTrip,
            entrySpreadBpsUsed: state.entrySpreadBpsUsed,
            heldSeconds,
            reasonCode,
            orderId: iocResult?.order?.id || iocResult?.order?.order_id || null,
          });
        } else {
          actionTaken = 'hold_existing_order';
          reasonCode = iocResult?.reason || 'taker_ioc_skipped';
          decisionPath = 'taker_ioc';
        }
      } else if (askMeetsBreakeven && Number.isFinite(makerDesiredLimit)) {
        let order;
        if (state.sellOrderId) {
          try {
            order = await fetchOrderByIdThrottled({ symbol, orderId: state.sellOrderId });
          } catch (err) {
            console.warn('order_fetch_failed', { symbol, orderId: state.sellOrderId, error: err?.message || err });
          }
        }

        const st = String(order?.status || '').toLowerCase();
        if (order && ['canceled', 'expired', 'rejected'].includes(st)) {
          console.warn('tp_order_became_terminal', {
            symbol,
            orderId: state.sellOrderId,
            status: st,
            canceled_at: order.canceled_at || null,
            failed_at: order.failed_at || null,
            expired_at: order.expired_at || null,
            replaced_at: order.replaced_at || null,
            replaced_by: order.replaced_by || null,
            client_order_id: order.client_order_id || null,
            limit_price: order.limit_price || null,
            qty: order.qty || null,
            filled_qty: order.filled_qty || null,
            time_in_force: order.time_in_force || null,
            type: order.type || null,
            post_only: order.post_only ?? null,
            note: 'Bot cancel is hard-disabled; terminal status is broker-side or external.',
          });
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

          if (SELL_REPRICE_ENABLED) {
            if (!shouldCancelExitSell()) {
              actionTaken = 'hold_existing_order';
              reasonCode = 'policy_no_cancel_no_reprice';
              decisionPath = 'policy_lock';
            } else if (
              existingOrderAgeMs != null &&
              existingOrderAgeMs > SELL_ORDER_TTL_MS &&
              Number.isFinite(awayBps) &&
              awayBps > REPRICE_IF_AWAY_BPS &&
              !repriceCooldownActive
            ) {
              await maybeCancelExitSell({
                symbol,
                orderId: state.sellOrderId,
                reason: 'reprice_ttl',
              });
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
                if (shouldCancelExitSell()) {
                  lastCancelReplaceAt.set(symbol, now);
                }
              } else {
                actionTaken = 'hold_existing_order';
                reasonCode = replacement?.reason || 'reprice_ttl_skipped';
                decisionPath = 'reprice_ttl';
              }
            } else if (existingOrderAgeMs != null && existingOrderAgeMs > SELL_ORDER_TTL_MS && repriceCooldownActive) {
              actionTaken = 'hold_existing_order';
              reasonCode = 'reprice_ttl_cooldown';
              decisionPath = 'reprice_ttl';
            } else if (existingOrderAgeMs != null && existingOrderAgeMs > SELL_ORDER_TTL_MS) {
              actionTaken = 'hold_existing_order';
              reasonCode = 'reprice_ttl_within_band';
              decisionPath = 'reprice_ttl';
            } else if (Number.isFinite(awayBps) && awayBps > REPRICE_IF_AWAY_BPS && !repriceCooldownActive) {
              await maybeCancelExitSell({
                symbol,
                orderId: state.sellOrderId,
                reason: 'reprice_away',
              });
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
                if (shouldCancelExitSell()) {
                  lastCancelReplaceAt.set(symbol, now);
                }
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
            order = await fetchOrderByIdThrottled({ symbol, orderId: state.sellOrderId });
          } catch (err) {
            console.warn('order_fetch_failed', { symbol, orderId: state.sellOrderId, error: err?.message || err });
          }
          const st = String(order?.status || '').toLowerCase();
          if (order && ['canceled', 'expired', 'rejected'].includes(st)) {
            console.warn('tp_order_became_terminal', {
              symbol,
              orderId: state.sellOrderId,
              status: st,
              canceled_at: order.canceled_at || null,
              failed_at: order.failed_at || null,
              expired_at: order.expired_at || null,
              replaced_at: order.replaced_at || null,
              replaced_by: order.replaced_by || null,
              client_order_id: order.client_order_id || null,
              limit_price: order.limit_price || null,
              qty: order.qty || null,
              filled_qty: order.filled_qty || null,
              time_in_force: order.time_in_force || null,
              type: order.type || null,
              post_only: order.post_only ?? null,
              note: 'Bot cancel is hard-disabled; terminal status is broker-side or external.',
            });
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
            if (SELL_REPRICE_ENABLED) {
              if (!shouldCancelExitSell()) {
                actionTaken = 'hold_existing_order';
                reasonCode = 'policy_no_cancel_no_reprice';
                decisionPath = 'policy_lock';
              } else if (
                existingOrderAgeMs != null &&
                existingOrderAgeMs > SELL_ORDER_TTL_MS &&
                Number.isFinite(awayBps) &&
                awayBps > REPRICE_IF_AWAY_BPS &&
                !repriceCooldownActive
              ) {
                await maybeCancelExitSell({
                  symbol,
                  orderId: state.sellOrderId,
                  reason: 'reprice_ttl',
                });
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
                  if (shouldCancelExitSell()) {
                    lastCancelReplaceAt.set(symbol, now);
                  }
                } else {
                  actionTaken = 'hold_existing_order';
                  reasonCode = replacement?.reason || 'reprice_ttl_skipped';
                }
              } else if (existingOrderAgeMs != null && existingOrderAgeMs > SELL_ORDER_TTL_MS && repriceCooldownActive) {
                actionTaken = 'hold_existing_order';
                reasonCode = 'reprice_ttl_cooldown';
              } else if (existingOrderAgeMs != null && existingOrderAgeMs > SELL_ORDER_TTL_MS) {
                actionTaken = 'hold_existing_order';
                reasonCode = 'reprice_ttl_within_band';
              } else if (Number.isFinite(awayBps) && awayBps > REPRICE_IF_AWAY_BPS && !repriceCooldownActive) {
                await maybeCancelExitSell({
                  symbol,
                  orderId: state.sellOrderId,
                  reason: 'reprice_away',
                });
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
                  if (shouldCancelExitSell()) {
                    lastCancelReplaceAt.set(symbol, now);
                  }
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
        baseRequiredExitBps,
        requiredExitBpsFinal,
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

async function runEntryScanOnce() {
  if (entryScanRunning) return;
  entryScanRunning = true;
  try {
    const startMs = Date.now();
    const MAX_ATTEMPTS = Number(process.env.SIMPLE_SCALPER_MAX_ENTRY_ATTEMPTS_PER_SCAN ?? 5);
    const maxAttemptsPerScan = Number.isFinite(MAX_ATTEMPTS) && MAX_ATTEMPTS > 0 ? MAX_ATTEMPTS : 5;
    const autoTradeEnabled = readEnvFlag('AUTO_TRADE', true);
    const liveMode = readEnvFlag('LIVE', readEnvFlag('LIVE_MODE', readEnvFlag('LIVE_TRADING', true)));
    if (!autoTradeEnabled || !liveMode) {
      return;
    }
    if (HALT_ON_ORPHANS) {
      const orphanReport1 = await getCachedOrphanScan();
      const orphans1 = Array.isArray(orphanReport1?.orphans) ? orphanReport1.orphans : [];
      if (orphans1.length > 0 && ORPHAN_REPAIR_BEFORE_HALT) {
        await repairOrphanExitsSafe();
        lastOrphanScan.tsMs = 0;
      }
      const orphanReport2 = await scanOrphanPositions();
      const orphans2 = Array.isArray(orphanReport2?.orphans) ? orphanReport2.orphans : [];
      if (orphans2.length > 0) {
        tradingHaltedReason = 'orphans_present';
        console.warn('HALT_TRADING_ORPHANS', { count: orphans2.length, symbols: orphans2.map((orphan) => orphan.symbol) });
        const endMs = Date.now();
        console.log('entry_scan', {
          startMs,
          endMs,
          durationMs: endMs - startMs,
          scanned: 0,
          placed: 0,
          skipped: 1,
          topSkipReasons: { halted_orphans: 1 },
        });
        return;
      }
      tradingHaltedReason = null;
    }

    const envSymbols = normalizeSymbolsParam(process.env.AUTO_SCAN_SYMBOLS);
    const stableSymbols = new Set(['USDC/USD', 'USDT/USD', 'BUSD/USD', 'DAI/USD']);
    let universe = [];
    if (SIMPLE_SCALPER_ENABLED) {
      await loadSupportedCryptoPairs();
      universe = Array.from(supportedCryptoPairsState.pairs);
    } else if (envSymbols.length) {
      universe = envSymbols;
    } else {
      await loadSupportedCryptoPairs();
      universe = Array.from(supportedCryptoPairsState.pairs);
      if (!universe.length) {
        universe = CRYPTO_CORE_TRACKED;
      }
    }
    const scanSymbols = universe
      .map((sym) => normalizeSymbol(sym))
      .filter((sym) => sym && !stableSymbols.has(sym));

    let positions = [];
    let openOrders = [];
    try {
      const ordersStatus = SIMPLE_SCALPER_ENABLED ? 'all' : 'open';
      [positions, openOrders] = await Promise.all([fetchPositions(), fetchOrders({ status: ordersStatus })]);
    } catch (err) {
      console.warn('entry_scan_fetch_failed', err?.message || err);
      return;
    }

    const heldSymbols = new Set();
    (Array.isArray(positions) ? positions : []).forEach((pos) => {
      const qty = Number(pos.qty ?? pos.quantity ?? pos.position_qty ?? pos.available);
      if (Number.isFinite(qty) && qty > 0) {
        heldSymbols.add(normalizeSymbol(pos.symbol || pos.asset_id || pos.id || ''));
      }
    });

    const openBuySymbols = new Set();
    (Array.isArray(openOrders) ? openOrders : []).forEach((order) => {
      if (SIMPLE_SCALPER_ENABLED && isTerminalOrderStatus(order?.status)) return;
      const orderSymbol = normalizeSymbol(order.symbol || order.rawSymbol || '');
      if (!orderSymbol) return;
      if (SIMPLE_SCALPER_ENABLED) {
        openBuySymbols.add(orderSymbol);
        return;
      }
      const side = String(order.side || '').toLowerCase();
      if (side !== 'buy') return;
      openBuySymbols.add(orderSymbol);
    });

    let placed = 0;
    let scanned = 0;
    let skipped = 0;
    let attempts = 0;
    const skipCounts = new Map();

    for (const symbol of scanSymbols) {
      if (attempts >= maxAttemptsPerScan) {
        break;
      }
      scanned += 1;
      if (heldSymbols.has(symbol)) {
        skipped += 1;
        skipCounts.set('held_position', (skipCounts.get('held_position') || 0) + 1);
        if (SIMPLE_SCALPER_ENABLED) {
          logSimpleScalperSkip(symbol, 'held_position');
        }
        continue;
      }
      if (openBuySymbols.has(symbol)) {
        skipped += 1;
        const reason = SIMPLE_SCALPER_ENABLED ? 'open_order' : 'open_buy';
        skipCounts.set(reason, (skipCounts.get(reason) || 0) + 1);
        if (SIMPLE_SCALPER_ENABLED) {
          logSimpleScalperSkip(symbol, 'open_order');
        }
        continue;
      }
      if (SIMPLE_SCALPER_ENABLED) {
        const inFlight = getInFlightStatus(symbol);
        if (inFlight) {
          skipped += 1;
          const reason = inFlight.reason || 'in_flight';
          skipCounts.set(reason, (skipCounts.get(reason) || 0) + 1);
          logSimpleScalperSkip(symbol, reason, { untilMs: inFlight.untilMs ?? null });
          continue;
        }
      }

      const signal = await computeEntrySignal(symbol);
      if (DEBUG_ENTRY) {
        console.log('entry_signal', { symbol, entryReady: signal.entryReady, why: signal.why, meta: signal.meta });
      }
      if (!signal.entryReady) {
        skipped += 1;
        skipCounts.set(signal.why || 'signal_skip', (skipCounts.get(signal.why || 'signal_skip') || 0) + 1);
        if (SIMPLE_SCALPER_ENABLED) {
          logSimpleScalperSkip(symbol, signal.why || 'signal_skip', signal.meta || {});
        }
        continue;
      }

      let result = null;
      if (SIMPLE_SCALPER_ENABLED) {
        result = await placeSimpleScalperEntry(symbol);
      } else {
        desiredExitBpsBySymbol.set(symbol, signal.desiredNetExitBpsForV22);
        result = await placeMakerLimitBuyThenSell(symbol);
      }
      attempts += 1;
      if (result?.submitted) {
        placed += 1;
        break;
      }
      if (result?.skipped || result?.failed) {
        skipped += 1;
        const reason = result.reason || (result.failed ? 'attempt_failed' : 'attempt_skipped');
        skipCounts.set(reason, (skipCounts.get(reason) || 0) + 1);
      }
    }

    const topSkipReasons = Array.from(skipCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .reduce((acc, [reason, count]) => {
        acc[reason] = count;
        return acc;
      }, {});

    const endMs = Date.now();
    console.log('entry_scan', {
      startMs,
      endMs,
      durationMs: endMs - startMs,
      scanned,
      placed,
      skipped,
      topSkipReasons,
    });
  } finally {
    entryScanRunning = false;
  }
}

function startExitManager() {
  if (!exitRepairIntervalId) {
    try {
      exitRepairIntervalId = setInterval(() => {
        repairOrphanExitsSafe().catch((err) => {
          console.error('exit_repair_scheduler_failed', err?.message || err);
        });
      }, EXIT_REPAIR_INTERVAL_MS);
      setTimeout(() => {
        repairOrphanExitsSafe().catch((err) => {
          console.error('exit_repair_scheduler_failed', err?.message || err);
        });
      }, 0);
      console.log('exit_repair_scheduler_started', { intervalMs: EXIT_REPAIR_INTERVAL_MS });
    } catch (err) {
      console.error('exit_repair_scheduler_failed', err?.message || err);
    }
  }
  if (SIMPLE_SCALPER_ENABLED) {
    return;
  }

  setInterval(() => {

    manageExitStates().catch((err) => {

      console.error('exit_manager_failed', err?.message || err);

    });

  }, REPRICE_EVERY_SECONDS * 1000);

  console.log('exit_manager_started', { intervalSeconds: REPRICE_EVERY_SECONDS });
  setTimeout(() => {
    repairOrphanExitsSafe().catch((err) => {
      console.error('exit_repair_start_failed', err?.message || err);
    });
  }, 0);

}

function startEntryManager() {
  if (entryManagerRunning) return;
  entryManagerRunning = true;
  setInterval(() => {
    runEntryScanOnce().catch((err) => {
      console.error('entry_manager_failed', err?.message || err);
    });
  }, ENTRY_SCAN_INTERVAL_MS);
  console.log('entry_manager_started', { intervalMs: ENTRY_SCAN_INTERVAL_MS });
}

function monitorSimpleScalperTpFill({ symbol, orderId, maxMs = 600000, intervalMs = 5000 }) {
  if (!orderId) return;
  const normalizedSymbol = normalizeSymbol(symbol);
  const startMs = Date.now();
  const poll = async () => {
    try {
      const order = await fetchOrderById(orderId);
      const status = String(order?.status || '').toLowerCase();
      if (status === 'filled') {
        console.log('simple_scalper_tp_fill', {
          symbol: normalizedSymbol,
          filledQty: order?.filled_qty ?? null,
          avgPrice: order?.filled_avg_price ?? null,
        });
        return;
      }
      if (isTerminalOrderStatus(status)) {
        return;
      }
    } catch (err) {
      console.warn('simple_scalper_tp_fill_check_failed', {
        symbol: normalizedSymbol,
        orderId,
        error: err?.message || err,
      });
    }
    if (Date.now() - startMs < maxMs) {
      setTimeout(poll, intervalMs);
    }
  };
  setTimeout(poll, intervalMs);
}

async function waitForOrderFill({ symbol, orderId, timeoutMs, intervalMs = 1000 }) {
  const startMs = Date.now();
  let order = null;
  let status = null;
  while (Date.now() - startMs < timeoutMs) {
    order = await fetchOrderById(orderId);
    status = String(order?.status || '').toLowerCase();
    if (status === 'filled') {
      return { filled: true, order };
    }
    if (isTerminalOrderStatus(status)) {
      return { filled: false, terminalStatus: status, order };
    }
    await sleep(intervalMs);
  }
  return { filled: false, timeout: true, order };
}

async function placeSimpleScalperEntry(symbol) {
  const normalizedSymbol = normalizeSymbol(symbol);
  if (HALT_ON_ORPHANS) {
    const orphanReport1 = await getCachedOrphanScan();
    const orphans1 = Array.isArray(orphanReport1?.orphans) ? orphanReport1.orphans : [];
    if (orphans1.length > 0 && ORPHAN_REPAIR_BEFORE_HALT) {
      await repairOrphanExitsSafe();
      lastOrphanScan.tsMs = 0;
    }
    const orphanReport2 = await scanOrphanPositions();
    const orphans2 = Array.isArray(orphanReport2?.orphans) ? orphanReport2.orphans : [];
    if (orphans2.length > 0) {
      tradingHaltedReason = 'orphans_present';
      console.warn('HALT_TRADING_ORPHANS', { count: orphans2.length, symbols: orphans2.map((orphan) => orphan.symbol) });
      logSimpleScalperSkip(normalizedSymbol, 'halted_orphans');
      return { skipped: true, reason: 'halted_orphans' };
    }
    tradingHaltedReason = null;
  }
  const inflight = getInFlightStatus(normalizedSymbol);
  if (inflight) {
    logSimpleScalperSkip(normalizedSymbol, inflight.reason || 'in_flight');
    return { skipped: true, reason: inflight.reason || 'in_flight' };
  }

  const openOrders = await fetchOrders({ status: 'all' });
  const hasOpenOrder = (Array.isArray(openOrders) ? openOrders : []).some((order) => {
    if (isTerminalOrderStatus(order?.status)) return false;
    const orderSymbol = normalizeSymbol(order.symbol || order.rawSymbol || '');
    return orderSymbol === normalizedSymbol;
  });
  if (hasOpenOrder) {
    logSimpleScalperSkip(normalizedSymbol, 'open_order');
    return { skipped: true, reason: 'open_order' };
  }

  let quote;
  try {
    quote = await getLatestQuote(normalizedSymbol, { maxAgeMs: ENTRY_QUOTE_MAX_AGE_MS });
  } catch (err) {
    logSimpleScalperSkip(normalizedSymbol, 'stale_quote', { error: err?.message || err });
    return { skipped: true, reason: 'stale_quote' };
  }
  const bid = Number(quote.bid);
  const ask = Number(quote.ask);
  const mid = Number.isFinite(bid) && Number.isFinite(ask) ? (bid + ask) / 2 : Number(quote.mid || bid || ask);
  if (!Number.isFinite(bid) || !Number.isFinite(ask) || bid <= 0 || ask <= 0 || !Number.isFinite(mid)) {
    logSimpleScalperSkip(normalizedSymbol, 'invalid_quote', { bid, ask });
    return { skipped: true, reason: 'invalid_quote' };
  }
  const spreadBps = ((ask - bid) / mid) * BPS;
  if (Number.isFinite(spreadBps) && spreadBps > MAX_SPREAD_BPS_SIMPLE) {
    logSimpleScalperSkip(normalizedSymbol, 'spread_gate', { spreadBps });
    return { skipped: true, reason: 'spread_gate', spreadBps };
  }

  const account = await getAccountInfo();
  const portfolioValue = account.portfolioValue;
  const buyingPower = account.buyingPower;
  if (!Number.isFinite(portfolioValue) || portfolioValue <= 0 || !Number.isFinite(buyingPower)) {
    logSimpleScalperSkip(normalizedSymbol, 'invalid_account_values', { portfolioValue, buyingPower });
    return { skipped: true, reason: 'invalid_account_values' };
  }
  if (buyingPower <= 0) {
    logSimpleScalperSkip(normalizedSymbol, 'no_buying_power', { portfolioValue, buyingPower });
    return { skipped: true, reason: 'no_buying_power' };
  }
  const reserveUsd = Math.max(0, BUYING_POWER_RESERVE_USD);
  const buyingPowerAvailable = buyingPower - reserveUsd;
  const notionalUsd = Math.min(portfolioValue * 0.10, buyingPowerAvailable);
  if (!Number.isFinite(notionalUsd) || notionalUsd < MIN_ORDER_NOTIONAL_USD) {
    logSimpleScalperSkip(normalizedSymbol, 'notional_too_small', {
      notionalUsd,
      minNotionalUsd: MIN_ORDER_NOTIONAL_USD,
    });
    return { skipped: true, reason: 'notional_too_small', notionalUsd };
  }
  const qty = roundQty(notionalUsd / ask);
  if (!Number.isFinite(qty) || qty <= 0) {
    logSimpleScalperSkip(normalizedSymbol, 'invalid_qty', { qty });
    return { skipped: true, reason: 'invalid_qty' };
  }
  const sizeGuard = guardTradeSize({
    symbol: normalizedSymbol,
    qty,
    notional: notionalUsd,
    price: ask,
    side: 'buy',
    context: 'simple_scalper_entry',
  });
  if (sizeGuard.skip) {
    logSimpleScalperSkip(normalizedSymbol, 'below_min_trade', { notionalUsd: sizeGuard.notional });
    return { skipped: true, reason: 'below_min_trade', notionalUsd: sizeGuard.notional };
  }
  const finalQty = sizeGuard.qty ?? qty;

  const buyOrderUrl = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'orders',
    label: 'orders_simple_scalper_buy',
  });
  let buyOrder = null;
  let buyType = 'market';
  let limitPrice = null;
  setInFlightStatus(normalizedSymbol, {
    reason: 'entry_in_flight',
    untilMs: Date.now() + SIMPLE_SCALPER_ENTRY_TIMEOUT_MS,
  });
  try {
    const payload = {
      symbol: toTradeSymbol(normalizedSymbol),
      qty: finalQty,
      side: 'buy',
      type: 'market',
      time_in_force: isCryptoSymbol(normalizedSymbol) ? ENTRY_BUY_TIF_SAFE : 'gtc',
      client_order_id: buildEntryClientOrderId(normalizedSymbol),
    };
    buyOrder = await placeOrderUnified({
      symbol: normalizedSymbol,
      url: buyOrderUrl,
      payload,
      label: 'orders_simple_scalper_buy',
      reason: 'simple_scalper_market_buy',
      context: 'simple_scalper_market_buy',
    });
  } catch (err) {
    console.warn('simple_scalper_market_buy_failed', {
      symbol: normalizedSymbol,
      error: err?.errorMessage || err?.message || err,
    });
    try {
      buyType = 'limit';
      const roundedAsk = roundToTick(ask, normalizedSymbol, 'up');
      limitPrice = roundedAsk;
      const payload = {
        symbol: toTradeSymbol(normalizedSymbol),
        qty: finalQty,
        side: 'buy',
        type: 'limit',
        time_in_force: 'gtc',
        limit_price: roundedAsk,
        client_order_id: buildEntryClientOrderId(normalizedSymbol),
      };
      buyOrder = await placeOrderUnified({
        symbol: normalizedSymbol,
        url: buyOrderUrl,
        payload,
        label: 'orders_simple_scalper_buy',
        reason: 'simple_scalper_limit_buy',
        context: 'simple_scalper_limit_buy',
      });
    } catch (submitErr) {
      setInFlightStatus(normalizedSymbol, {
        reason: 'submit_failed',
        untilMs: Date.now() + SIMPLE_SCALPER_RETRY_COOLDOWN_MS,
      });
      return {
        failed: true,
        reason: 'submit_failed',
        error: submitErr?.message || submitErr,
      };
    }
  }

  console.log('simple_scalper_buy_submit', {
    symbol: normalizedSymbol,
    qty: finalQty,
    notionalUsd,
    type: buyType,
    limitPrice,
  });

  const buyOrderId = buyOrder?.id;
  if (!buyOrderId) {
    setInFlightStatus(normalizedSymbol, { reason: 'entry_not_submitted', untilMs: Date.now() + SIMPLE_SCALPER_RETRY_COOLDOWN_MS });
    logSimpleScalperSkip(normalizedSymbol, 'entry_not_submitted');
    return { skipped: true, reason: 'entry_not_submitted' };
  }

  const fillResult = await waitForOrderFill({
    symbol: normalizedSymbol,
    orderId: buyOrderId,
    timeoutMs: SIMPLE_SCALPER_ENTRY_TIMEOUT_MS,
  });
  if (!fillResult.filled) {
    await cancelOrderSafe(buyOrderId);
    setInFlightStatus(normalizedSymbol, {
      reason: 'entry_not_filled',
      untilMs: Date.now() + SIMPLE_SCALPER_RETRY_COOLDOWN_MS,
    });
    logSimpleScalperSkip(normalizedSymbol, 'entry_not_filled', { status: fillResult.terminalStatus || null });
    return { submitted: true, skipped: true, reason: 'entry_not_filled' };
  }

  inFlightBySymbol.delete(normalizedSymbol);
  const filledOrder = fillResult.order;
  const filledQty = Number(filledOrder?.filled_qty || 0);
  const avgPrice = Number(filledOrder?.filled_avg_price || 0);
  console.log('simple_scalper_buy_fill', { symbol: normalizedSymbol, filledQty, avgPrice });
  if (Number.isFinite(filledQty) && Number.isFinite(avgPrice) && filledQty > 0 && avgPrice > 0) {
    updateInventoryFromBuy(normalizedSymbol, filledQty, avgPrice);
  }

  const targetRaw = avgPrice * (1 + PROFIT_NET_BPS / 10000 + FEE_BPS_EST / 10000);
  const targetPrice = roundToTick(targetRaw, normalizedSymbol, 'up');
  const sellOrderUrl = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'orders',
    label: 'orders_simple_scalper_tp',
  });
  const sellPayload = {
    symbol: toTradeSymbol(normalizedSymbol),
    qty: filledQty,
    side: 'sell',
    type: 'limit',
    time_in_force: 'gtc',
    limit_price: targetPrice,
    client_order_id: buildTpClientOrderId(normalizedSymbol, buyOrderId),
  };
  const sellOrder = await placeOrderUnified({
    symbol: normalizedSymbol,
    url: sellOrderUrl,
    payload: sellPayload,
    label: 'orders_simple_scalper_tp',
    reason: 'simple_scalper_tp',
    context: 'simple_scalper_tp',
  });
  console.log('simple_scalper_tp_submit', { symbol: normalizedSymbol, qty: filledQty, targetPrice });
  monitorSimpleScalperTpFill({ symbol: normalizedSymbol, orderId: sellOrder?.id });

  return { submitted: true, buy: filledOrder, sell: sellOrder };
}

// Market buy using 10% of portfolio value then place a limit sell with markup

// covering taker fees and profit target

async function placeMakerLimitBuyThenSell(symbol) {
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
    const quote = await getLatestQuote(normalizedSymbol, { maxAgeMs: ENTRY_QUOTE_MAX_AGE_MS });
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

  const account = await getAccountInfo();
  const portfolioValue = account.portfolioValue;
  const buyingPower = account.buyingPower;
  if (
    !Number.isFinite(portfolioValue) ||
    !Number.isFinite(buyingPower) ||
    portfolioValue <= 0 ||
    buyingPower <= 0
  ) {
    logSkip('invalid_account_values', { symbol: normalizedSymbol, portfolioValue, buyingPower });
    return { skipped: true, reason: 'invalid_account_values' };
  }
  const targetTradeAmount = portfolioValue * TRADE_PORTFOLIO_PCT;
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

  const tickSize = getTickSize({ symbol: normalizedSymbol, price: bid });
  const limitBuyPrice = roundToTick(Number(bid), tickSize, 'down');
  if (!Number.isFinite(limitBuyPrice) || limitBuyPrice <= 0) {
    logSkip('invalid_quote', { symbol: normalizedSymbol, bid, ask });
    return { skipped: true, reason: 'invalid_quote' };
  }

  const qty = roundQty(amountToSpend / limitBuyPrice);
  if (!Number.isFinite(qty) || qty <= 0) {
    logSkip('invalid_qty', { symbol: normalizedSymbol, qty });
    return { skipped: true, reason: 'invalid_qty' };
  }

  const sizeGuard = guardTradeSize({
    symbol: normalizedSymbol,
    qty,
    notional: amountToSpend,
    price: limitBuyPrice,
    side: 'buy',
    context: 'maker_limit_buy',
  });
  if (sizeGuard.skip) {
    return { skipped: true, reason: 'below_min_trade', notionalUsd: sizeGuard.notional };
  }
  const finalQty = sizeGuard.qty ?? qty;

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
    time_in_force: 'gtc',
    limit_price: limitBuyPrice,
    client_order_id: buildEntryClientOrderId(normalizedSymbol),
  };
  if (POST_ONLY_BUY && isCryptoSymbol(normalizedSymbol)) {
    buyPayload.post_only = true;
  }
  const buyOrder = await placeOrderUnified({
    symbol: normalizedSymbol,
    url: buyOrderUrl,
    payload: buyPayload,
    label: 'orders_limit_buy',
    reason: 'maker_limit_buy',
    context: 'maker_limit_buy',
  });
  const submitted = Boolean(buyOrder?.id);

  const timeoutMs = ENTRY_FILL_TIMEOUT_SECONDS * 1000;
  const start = Date.now();
  let filledOrder = buyOrder;
  while (Date.now() - start < timeoutMs) {
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
    if (['canceled', 'expired', 'rejected'].includes(String(filledOrder.status || '').toLowerCase())) {
      break;
    }
    await sleep(1000);
  }

  if (filledOrder.status !== 'filled') {
    if (buyOrder?.id) {
      await cancelOrderSafe(buyOrder.id);
    }
    if (ENTRY_FALLBACK_MARKET && Number.isFinite(spreadBps) && spreadBps <= MAX_SPREAD_BPS_TO_TRADE) {
      console.log('entry_fallback_market', { symbol: normalizedSymbol, spreadBps });
      return placeMarketBuyThenSell(normalizedSymbol);
    }
    return { skipped: true, reason: 'entry_not_filled', submitted };
  }

  const avgPrice = parseFloat(filledOrder.filled_avg_price);
  updateInventoryFromBuy(normalizedSymbol, filledOrder.filled_qty, avgPrice);
  const inventory = inventoryState.get(normalizedSymbol);
  if (!inventory || inventory.qty <= 0) {
    logSkip('no_inventory_for_sell', { symbol: normalizedSymbol, qty: filledOrder.filled_qty });
    return { buy: filledOrder, sell: null, sellError: 'No inventory to sell', submitted };
  }

  const sellOrder = await handleBuyFill({
    symbol: normalizedSymbol,
    qty: filledOrder.filled_qty,
    entryPrice: avgPrice,
    entryOrderId: filledOrder.id || buyOrder?.id,
    entryBid: bid,
    entryAsk: ask,
    entrySpreadBps: spreadBps,
  });

  return { buy: filledOrder, sell: sellOrder, submitted };
}

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
    const quote = await getLatestQuote(normalizedSymbol, { maxAgeMs: ENTRY_QUOTE_MAX_AGE_MS });
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

  if (
    !Number.isFinite(portfolioValue) ||
    !Number.isFinite(buyingPower) ||
    portfolioValue <= 0 ||
    buyingPower <= 0
  ) {
    logSkip('invalid_account_values', { symbol: normalizedSymbol, portfolioValue, buyingPower });
    return { skipped: true, reason: 'invalid_account_values' };
  }

  const targetTradeAmount = portfolioValue * TRADE_PORTFOLIO_PCT;

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
    time_in_force: isCryptoSymbol(normalizedSymbol) ? ENTRY_BUY_TIF_SAFE : 'gtc',
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
  const submitted = Boolean(buyOrder?.id);
  if (buyOrder?.id) {
    markRecentEntry(normalizedSymbol, buyOrder?.id || null);
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

    return { buy: filled, sell: null, sellError: 'No inventory to sell', submitted };

  }

 

  const avgPrice = parseFloat(filled.filled_avg_price);

  try {

    const sellOrder = await handleBuyFill({

      symbol: normalizedSymbol,

      qty: filled.filled_qty,

      entryPrice: avgPrice,
      entryOrderId: filled.id || buyOrder?.id,
      entryBid: bid,
      entryAsk: ask,
      entrySpreadBps: spreadBps,

    });

    return { buy: filled, sell: sellOrder, submitted };

  } catch (err) {

    console.error('Sell order failed:', err?.responseSnippet200 || err?.errorMessage || err.message);

    return { buy: filled, sell: null, sellError: err.message, submitted };

  }

}

async function submitManagedEntryBuy({
  symbol,
  qty,
  type,
  time_in_force,
  limit_price,
  desiredNetExitBps,
  notional,
}) {
  const normalizedSymbol = normalizeSymbol(symbol);
  let bid = null;
  let ask = null;
  let spreadBps = null;
  const quote = await getLatestQuote(normalizedSymbol, { maxAgeMs: ENTRY_QUOTE_MAX_AGE_MS }).catch(() => null);
  if (quote) {
    bid = quote.bid;
    ask = quote.ask;
    if (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0) {
      spreadBps = ((ask - bid) / bid) * 10000;
    }
  }

  const url = buildAlpacaUrl({ baseUrl: ALPACA_BASE_URL, path: 'orders', label: 'orders_entry_buy' });
  const limitPriceNum = Number(limit_price);
  const payload = {
    symbol: toTradeSymbol(normalizedSymbol),
    side: 'buy',
    type,
    time_in_force: time_in_force || undefined,
    limit_price: Number.isFinite(limitPriceNum) ? limitPriceNum : undefined,
    qty: qty ?? undefined,
    notional: notional ?? undefined,
    client_order_id: buildEntryClientOrderId(normalizedSymbol),
  };
  const buyOrder = await placeOrderUnified({
    symbol: normalizedSymbol,
    url,
    payload,
    label: 'orders_entry_buy',
    reason: 'managed_entry_buy',
    context: 'managed_entry_buy',
  });
  if (buyOrder?.id) {
    markRecentEntry(normalizedSymbol, buyOrder?.id || null);
  }

  let filled = buyOrder;
  let lastStatus = String(filled?.status || '').toLowerCase();
  const terminalStatuses = new Set(['canceled', 'expired', 'rejected']);
  const timeoutMs = ENTRY_FILL_TIMEOUT_SECONDS * 1000;
  const startMs = Date.now();

  while (Date.now() - startMs < timeoutMs && lastStatus !== 'filled' && !terminalStatuses.has(lastStatus)) {
    await sleep(1000);
    filled = await fetchOrderById(buyOrder.id);
    lastStatus = String(filled?.status || '').toLowerCase();
  }

  if (lastStatus !== 'filled') {
    if (terminalStatuses.has(lastStatus)) {
      return { ok: false, skipped: true, reason: 'entry_terminal', orderId: buyOrder.id, status: lastStatus };
    }
    if (Date.now() - startMs >= timeoutMs) {
      console.log('entry_buy_timeout_cancel', {
        symbol: normalizedSymbol,
        orderId: buyOrder.id,
        timeoutSeconds: ENTRY_FILL_TIMEOUT_SECONDS,
      });
      await cancelOrderSafe(buyOrder.id);
      return { ok: false, skipped: true, reason: 'entry_not_filled', orderId: buyOrder.id, status: lastStatus };
    }
  }

  const avgPriceRaw = Number(filled?.filled_avg_price);
  const avgPrice = Number.isFinite(avgPriceRaw)
    ? avgPriceRaw
    : (Number.isFinite(limitPriceNum) ? limitPriceNum : 0);
  updateInventoryFromBuy(normalizedSymbol, filled.filled_qty, avgPrice);
  const sellOrder = await handleBuyFill({
    symbol: normalizedSymbol,
    qty: filled.filled_qty,
    entryPrice: avgPrice,
    entryOrderId: filled.id,
    desiredNetExitBps,
    entryBid: bid,
    entryAsk: ask,
    entrySpreadBps: spreadBps,
  });

  return { ok: true, buy: filled, sell: sellOrder || null };
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
    raw = false,

  } = order;

  const normalizedSymbol = normalizeSymbol(rawSymbol);
  const isCrypto = isCryptoSymbol(normalizedSymbol);
  const sideLower = String(side || '').toLowerCase();
  const intent = sideLower === 'buy' ? 'ENTRY' : null;
  const typeLower = String(type || '').toLowerCase();
  const allowedCryptoTypes = new Set(['market', 'limit', 'stop_limit']);
  const finalType = isCrypto && !allowedCryptoTypes.has(typeLower) ? 'market' : (typeLower || 'market');
  const rawTif = String(time_in_force || '').toLowerCase();
  const allowedCryptoTifs = new Set(['gtc', 'ioc', 'fok']);
  const defaultCryptoTif = sideLower === 'sell' ? 'ioc' : 'gtc';
  const entryBuyTif = ENTRY_BUY_TIF_SAFE;
  const resolvedCryptoTif =
    sideLower === 'buy' && intent === 'ENTRY' ? entryBuyTif : (allowedCryptoTifs.has(rawTif) ? rawTif : defaultCryptoTif);
  const finalTif = isCrypto ? resolvedCryptoTif : (rawTif || time_in_force);
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
    const openOrders = await fetchLiveOrders();
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
    if (intent === 'ENTRY' && hasRecentEntry(normalizedSymbol)) {
      const recent = recentEntrySubmissions.get(normalizedSymbol);
      const ageMs = recent ? Date.now() - recent.atMs : null;
      console.log('hold_existing_order', {
        symbol: normalizedSymbol,
        side: 'buy',
        reason: 'recent_entry_submission',
        ageMs,
      });
      return { ok: true, hold: true, reason: 'recent_entry_submission' };
    }
    const openOrders = await fetchOrders({
      status: 'all',
      after: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
      direction: 'desc',
      limit: 500,
      nested: true,
    });
    const entryIntentPrefix = buildIntentPrefix({ symbol: normalizedSymbol, side: 'BUY', intent: 'ENTRY' });
    const entryIntentOrders = (Array.isArray(openOrders) ? openOrders : []).filter((order) => {
      const orderSymbol = normalizePair(order.symbol || order.rawSymbol);
      const orderSide = String(order.side || '').toUpperCase();
      const clientOrderId = String(order.client_order_id || order.clientOrderId || '');
      return (
        orderSymbol === normalizePair(normalizedSymbol) &&
        orderSide === 'BUY' &&
        clientOrderId.startsWith(entryIntentPrefix)
      );
    });
    const activeEntryOrders = entryIntentOrders.filter((order) => {
      const status = String(order.status || '').toLowerCase();
      return !NON_LIVE_ORDER_STATUSES.has(status);
    });
    if (activeEntryOrders.length) {
      const ttlMs = Number.isFinite(ENTRY_INTENT_TTL_MS) ? ENTRY_INTENT_TTL_MS : 45000;
      const expiredOrders = activeEntryOrders.filter((order) => {
        const ageMs = getOrderAgeMs(order);
        return Number.isFinite(ageMs) && ageMs > ttlMs;
      });
      if (expiredOrders.length) {
        for (const order of expiredOrders) {
          const orderId = order?.id || order?.order_id;
          if (!orderId) continue;
          const canceled = await cancelOrderSafe(orderId);
          console.log('entry_intent_cancel', {
            symbol: normalizedSymbol,
            orderId,
            ageMs: getOrderAgeMs(order),
            ttlMs,
            canceled,
          });
        }
      } else {
        console.log('hold_existing_order', { symbol: normalizedSymbol, side: 'buy', reason: 'existing_entry_intent' });
        return { skipped: true, reason: 'existing_entry_intent' };
      }
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

  if (sideLower === 'buy' && !raw) {
    return submitManagedEntryBuy({
      symbol: normalizedSymbol,
      qty: useQty ? finalQty : undefined,
      type: finalType,
      time_in_force: finalTif,
      limit_price: Number.isFinite(limitPriceNum) ? limitPriceNum : undefined,
      desiredNetExitBps,
      notional: useNotional ? finalNotional : undefined,
    });
  }

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
  const orderOk = await placeOrderUnified({
    symbol: normalizedSymbol,
    url,
    payload,
    label: 'orders_submit',
    reason,
    context: 'submit_order',
  });
  if (orderOk?.id && sideLower === 'buy' && intent === 'ENTRY') {
    markRecentEntry(normalizedSymbol, orderOk?.id || null);
  }
  return orderOk;

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
  if (resolvedParams.limit != null && !Number.isFinite(Number(resolvedParams.limit))) {
    delete resolvedParams.limit;
  }
  if (resolvedParams.nested != null) {
    resolvedParams.nested = Boolean(resolvedParams.nested);
  }
  const isOpenStatus = String(resolvedParams.status || '').toLowerCase() === 'open';
  if (isOpenStatus) {
    delete resolvedParams.symbol;
  }
  if (isOpenStatus) {
    const nowMs = Date.now();
    if (openOrdersCache.data && nowMs - openOrdersCache.tsMs < OPEN_ORDERS_CACHE_TTL_MS) {
      return openOrdersCache.data;
    }
    if (openOrdersCache.pending) {
      return openOrdersCache.pending;
    }
  }
  const url = buildAlpacaUrl({
    baseUrl: ALPACA_BASE_URL,
    path: 'orders',
    params: resolvedParams,
    label: 'orders_list',
  });
  const fetcher = async () => {
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
      const normalized = response.map((order) => ({
        ...order,
        rawSymbol: order.symbol,
        pairSymbol: normalizeSymbol(order.symbol),
        symbol: normalizeSymbol(order.symbol),
      }));
      if (isOpenStatus) {
        openOrdersCache.data = normalized;
        openOrdersCache.tsMs = Date.now();
      }
      return normalized;
    }

    if (isOpenStatus) {
      openOrdersCache.data = response;
      openOrdersCache.tsMs = Date.now();
    }
    return response;
  };

  if (!isOpenStatus) {
    return fetcher();
  }

  openOrdersCache.pending = fetcher();
  try {
    return await openOrdersCache.pending;
  } finally {
    openOrdersCache.pending = null;
  }

}

async function fetchLiveOrders() {
  const nowMs = Date.now();
  if (liveOrdersCache.data && nowMs - liveOrdersCache.tsMs < LIVE_ORDERS_CACHE_TTL_MS) {
    return liveOrdersCache.data;
  }
  if (liveOrdersCache.pending) {
    return liveOrdersCache.pending;
  }
  const afterIso = new Date(nowMs - 15 * 60 * 1000).toISOString();
  const fetcher = async () => {
    try {
      const response = await fetchOrders({
        status: 'all',
        after: afterIso,
        direction: 'desc',
        limit: 500,
      });
      const normalized = filterLiveOrders(Array.isArray(response) ? response : []).map((order) => {
        const rawSymbol = order.rawSymbol ?? order.symbol;
        const normalizedSymbol = normalizeSymbol(rawSymbol);
        return {
          ...order,
          rawSymbol,
          pairSymbol: normalizedSymbol,
          symbol: normalizedSymbol,
        };
      });
      liveOrdersCache.data = normalized;
      liveOrdersCache.tsMs = Date.now();
      return normalized;
    } catch (err) {
      const fallback = await fetchOrders({ status: 'open' });
      const list = Array.isArray(fallback) ? fallback : [];
      liveOrdersCache.data = list;
      liveOrdersCache.tsMs = Date.now();
      return list;
    }
  };
  liveOrdersCache.pending = fetcher();
  try {
    return await liveOrdersCache.pending;
  } finally {
    liveOrdersCache.pending = null;
  }
}

async function fetchOpenPositions() {
  const nowMs = Date.now();
  if (openPositionsCache.data && nowMs - openPositionsCache.tsMs < OPEN_POSITIONS_CACHE_TTL_MS) {
    return openPositionsCache.data;
  }
  if (openPositionsCache.pending) {
    return openPositionsCache.pending;
  }
  openPositionsCache.pending = (async () => {
    const positions = await fetchPositions();
    const normalized = (Array.isArray(positions) ? positions : [])
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
    openPositionsCache.data = normalized;
    openPositionsCache.tsMs = Date.now();
    return normalized;
  })();
  try {
    return await openPositionsCache.pending;
  } finally {
    openPositionsCache.pending = null;
  }
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

  placeMakerLimitBuyThenSell,

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
  scanOrphanPositions,

  startEntryManager,
  startExitManager,
  getConcurrencyGuardStatus,
  getLastQuoteSnapshot,
  getAlpacaAuthStatus,
  getLastHttpError,
  getAlpacaConnectivityStatus,
  runDustCleanup,

};
