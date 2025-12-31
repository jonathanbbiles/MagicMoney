// app.js
// Supported pair filter:
// - Builds a runtime set from Alpaca assets (USD crypto pairs only).
// - Cached in AsyncStorage under `supported_crypto_pairs_v1` and refreshed at most every 24h.
// - Used only for supported-pair lookups and early skips (no trading symbol changes).
import React, { useState, useEffect, useMemo, useRef } from 'react';
import {
  View,
  Text,
  ScrollView,
  StyleSheet,
  RefreshControl,
  TouchableOpacity,
  SafeAreaView,
  ActivityIndicator,
  TextInput,
  Platform,
} from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';
import Constants from 'expo-constants';
const normalizePair = (sym) => {
  if (!sym) return '';
  const raw = String(sym).trim().toUpperCase();
  if (!raw) return '';
  if (raw.includes('/')) return raw;
  if (raw.endsWith('USD') && raw.length > 3 && !raw.includes('-')) {
    return `${raw.slice(0, -3)}/USD`;
  }
  return raw;
};

const toAlpacaSymbol = (sym) => {
  if (!sym) return '';
  const pair = normalizePair(sym);
  return pair ? pair.replace('/', '') : pair;
};

const canonicalAsset = (sym) => toAlpacaSymbol(sym);

const toInternalSymbol = (sym) => normalizePair(sym);
const toAlpacaCryptoSymbol = (sym) => normalizePair(sym);
const normalizeCryptoSymbol = (sym) => normalizePair(sym);
const isCrypto = (sym) => normalizePair(sym).endsWith('/USD');
const isStock = (sym) => !isCrypto(sym);

// SAFETY PATCH NOTES:
// - Moved import-time connectivity call into a mount-only effect.
// - Added AbortControllers to prevent overlapping monitorOutcome loops.
// - Buffered log UI updates to reduce render thrash.
// - Centralized timer cleanup/guards to avoid runaway loops.
// - Mirrored halt state for UI consistency without altering logic.

/* ──────────────────────────────── 1) VERSION / CONFIG ──────────────────────────────── */
const VERSION = 'v1';
const EX = Constants?.expoConfig?.extra || Constants?.manifest?.extra || {};

const RENDER_BACKEND_URL = 'https://magicmoney.onrender.com';
// Dev-only override for physical devices: set to your LAN IP (e.g., 'http://192.168.x.x:10000').
const DEV_LAN_IP = '';
const normalizeBaseUrl = (value) => String(value || '').trim().replace(/\/+$/, '');
const BACKEND_BASE_URL = normalizeBaseUrl(
  __DEV__
    ? (EX.BACKEND_BASE_URL || DEV_LAN_IP || 'http://localhost:3000')
    : (EX.BACKEND_BASE_URL || RENDER_BACKEND_URL)
);

// IMPORTANT: your account supports 'us' for crypto data. Do not call 'global' to avoid 400s.
const DATA_LOCATIONS = ['us'];
const BACKEND_HEADERS = {
  Accept: 'application/json',
  'Content-Type': 'application/json',
};
const DRY_RUN_STOPS = false; // Set true to log stop actions without sending orders
const MIN_ORDER_NOTIONAL_USD = 5;
const AUTO_BUMP_MIN_NOTIONAL = false;
const FORCE_ONE_TEST_BUY = ['1', 'true', 'yes'].includes(String(EX.FORCE_ONE_TEST_BUY || '').toLowerCase());
const FORCE_ONE_TEST_BUY_NOTIONAL = Number(EX.FORCE_ONE_TEST_BUY_NOTIONAL || 2);
let forceTestBuyUsed = false;
console.log('[Backend ENV]', { base: BACKEND_BASE_URL });

function bootConnectivityCheck(setStatusFn) {
  let active = true;
  (async () => {
    const startedAt = Date.now();
    try {
      const res = await f(`${BACKEND_BASE_URL}/health`, { headers: BACKEND_HEADERS }, 8000, 1);
      const ms = Date.now() - startedAt;
      const ok = !!res?.ok;
      const data = await res.json().catch(() => null);
      console.log(`BACKEND health ok=${ok} ms=${ms}`);
      if (!active) return;
      if (!ok) {
        const message = data?.error || data?.message || `HTTP ${res?.status ?? 'NA'}`;
        console.log(`BACKEND_UNREACHABLE ${message}`);
        setStatusFn?.({ ok: false, checkedAt: new Date().toISOString(), error: message });
        return;
      }
      setStatusFn?.({ ok: true, checkedAt: new Date().toISOString(), error: null });
    } catch (e) {
      const ms = Date.now() - startedAt;
      const message = e?.message || String(e);
      console.log(`BACKEND health ok=false ms=${ms}`);
      console.log(`BACKEND_UNREACHABLE ${message}`);
      if (!active) return;
      setStatusFn?.({ ok: false, checkedAt: new Date().toISOString(), error: message });
    }
  })();
  return () => { active = false; };
}

/* ───────────────────────────── 2) CORE CONSTANTS / STRATEGY ───────────────────────────── */
// Fee constants retained for compatibility but no longer used for gating logic.
const FEE_BPS_MAKER = 15;
const FEE_BPS_TAKER = 25;
const EQUITY_SEC_FEE_BPS = 0.35;
const EQUITY_TAF_PER_SHARE = 0.000145;
const EQUITY_TAF_CAP = 7.27;
const EQUITY_COMMISSION_PER_TRADE_USD = 0.0;

// Legacy guard constants retained but superseded by settings (see DEFAULT_SETTINGS).
const SLIP_BUFFER_BPS_BY_RISK = [1, 2, 3, 4, 5];
const STABLES = new Set(['USDT/USD', 'USDC/USD']);
// You can remove SHIB/USD from blacklist if you want to include it despite tiny tick size.
const BLACKLIST = new Set(['SHIB/USD']);
const MIN_PRICE_FOR_TICK_SANE_USD = 0.001; // keep low, but still skip micro-price assets
const DUST_FLATTEN_MAX_USD = 0.75;
const DUST_SWEEP_MINUTES = 12;
const MIN_BID_SIZE_LOOSE = 1;
const MIN_BID_NOTIONAL_LOOSE_USD = 5; // gate by ~$ value, not raw size
const MIN_TRADE_QTY = 0.000001;

const MAX_EQUITIES = 400;
const MAX_CRYPTOS = 400;

const QUOTE_TTL_MS = 4000;

/* Fee-aware gates */
const DYNAMIC_MIN_PROFIT_BPS = 60; // ~0.60% target floor to cover fees + edge
const EXTRA_OVER_FEES_BPS = 10;
const SPREAD_OVER_FEES_MIN_BPS = 5;

/* ────────────────────────────── 3) LIVE SETTINGS (UI-MUTABLE) ────────────────────────────── */
/**
 * Safer defaults: enforce momentum & spread-over-fees; maker-first; dynamic stops with grace.
 */
const DEFAULT_SETTINGS = {
  // Risk / scan pacing
  riskLevel: 1,
  scanMs: 5000,
  stockPageSize: 12,

  // Position sizing
  maxPosPctEquity: 10,
  absMaxNotionalUSD: 2000000,
  maxConcurrentPositions: 8,

  // Entry gates
  spreadMaxBps: 160,        // allow wider spreads → more candidates
  spreadOverFeesMinBps: 1,  // small guard if user enables the toggle later
  dynamicMinProfitBps: 22,  // lower TP floor to make EV positive
  extraOverFeesBps: 2,      // minimal cushion over fees
  netMinProfitBps: 0.0,     // no extra absolute floor
  minPriceUsd: 0.001,
  slipBpsByRisk: [1, 2, 3, 4, 5],

  // Quote handling
  liveRequireQuote: true, // live quotes only; no synthetic fallback unless user disables this
  quoteTtlMs: 30000,
  liveFreshMsCrypto: 60000,
  liveFreshMsStock: 15000,
  liveFreshTradeMsCrypto: 180000,
  syntheticTradeSpreadBps: 12,

  // Momentum filter
  enforceMomentum: false,   // start OFF for fills; you can re-enable later

  // Entry / exit behavior
  enableTakerFlip: false,
  // Exit behavior: prefer maker exits by default (lower fees → lower floor)
  takerExitOnTouch: false,  // was true; set false by default
  takerExitGuard: 'min',
  makerCampSec: 18,
  touchTicksRequired: 2,
  touchFlipTimeoutSec: 8,
  maxHoldMin: 20,
  maxTimeLossUSD: -5.0,

  // Stops / trailing
  enableStops: true,
  stopLossPct: 2.0,
  stopLossBps: 50,          // tighter stop improves EV
  hardStopLossPct: 1.8,
  stopGraceSec: 10, // NEW
  enableTrailing: true,
  trailStartPct: 1.0,
  trailDropPct: 1.0,
  trailStartBps: 20,
  trailingStopBps: 10,

  // Daily halts
  haltOnDailyLoss: true,
  dailyMaxLossPct: 5.0,
  haltOnDailyProfit: false,
  dailyProfitTargetPct: 8.0,

  // Fees (crypto)
  feeBpsMaker: 15,
  feeBpsTaker: 25,

  // Housekeeping / dust handling
  dustFlattenMaxUsd: 0.75,
  dustSweepMinutes: 12,

  // Misc / compatibility
  netMinProfitUSD: 0.01,
  netMinProfitUSDBase: 0.0,
  netMinProfitPct: 0.02,
  avoidPDT: false,
  pdtEquityThresholdUSD: 10000,

  // Gates
  requireSpreadOverFees: false, // keep OFF; small guard value above only used if toggled ON

  // Auto‑tune settings
  autoTuneEnabled: false,
  autoTuneWindowMin: 2,
  autoTuneThreshold: 2,
  autoTuneCooldownSec: 45,
  autoTunePerSweepMaxSymbols: 5,
  autoTuneSpreadStepBps: 10,
  autoTuneFeesGuardStepBps: 5,
  autoTuneNetMinStepBps: 0.5,
  autoTuneMaxSpreadBps: 180,
  autoTuneMinSpreadOverFeesBps: 0,
  autoTuneMinNetMinBps: 0.0, // don’t force netMin back up if auto-tune is enabled

  // ==== New math knobs (volatility & EV) ====
  // EV guard (new)
  evGuardEnabled: false,       // OFF = fail-open (no EV veto)
  evMinBps: -1.0,              // allow slightly negative EV in bps
  evMinUSD: -0.02,             // allow tiny negative EV per trade in USD
  evShowDebug: true,           // log EV math to Live logs

  volHalfLifeMin: 10,          // EWMA half-life (minutes) for realized vol on 1m bars
  tpVolScale: 1.0,             // kTP: target scales with σ (in bps)
  stopVolMult: 2.5,            // soft stop = max(user stop, stopVolMult * σ_bps)
  trailArmVolMult: 0.8,        // trail arms at max(user bps, trailArmVolMult * σ_bps)
  sellEpsMinTicks: 2,          // taker touch epsilon at least N ticks
  sellEpsVolFrac: 0.15,        // also ≥ 15% of σ_bps
  makerMinFillProb: 0.15,      // skip maker if fill probability below this
  pTouchHorizonMin: 8,         // minutes for barrier touch horizon
  kellyFraction: 0.5,          // half-Kelly sizing factor [0..1]
  kellyEnabled: true,          // enable Kelly-lite sizing
};
let SETTINGS = { ...DEFAULT_SETTINGS };
try {
  if ((SETTINGS.quoteTtlMs || 0) > (SETTINGS.liveFreshMsCrypto || 0)) {
    console.warn('[Quotes] quoteTtlMs > liveFreshMsCrypto — cached quotes may fail freshness on read');
  }
} catch {}

const SETTINGS_STORAGE_KEY = `${VERSION}:settings:v2`;
function migrateSettings(raw) {
  const base = { ...(raw || {}) };
  return {
    ...DEFAULT_SETTINGS,
    ...base,
    enableStops: base.enableStops ?? DEFAULT_SETTINGS.enableStops,
    stopLossPct: Number.isFinite(+base.stopLossPct) ? +base.stopLossPct : DEFAULT_SETTINGS.stopLossPct,
    enableTrailing: base.enableTrailing ?? DEFAULT_SETTINGS.enableTrailing,
    trailStartPct: Number.isFinite(+base.trailStartPct) ? +base.trailStartPct : DEFAULT_SETTINGS.trailStartPct,
    trailDropPct: Number.isFinite(+base.trailDropPct) ? +base.trailDropPct : DEFAULT_SETTINGS.trailDropPct,
  };
}

// Minimal UI switch: show only Gate settings inside Settings panel.
const SIMPLE_SETTINGS_ONLY = true;

// Per-symbol overrides live here. Example: { SOLUSD: { spreadMaxBps: 130 } }
let SETTINGS_OVERRIDES = {};

function getEffectiveSettings(localSettings) {
  return localSettings || SETTINGS;
}

// Effective setting helper: tries per-symbol override, falls back to global setting
function eff(symbol, key) {
  const o = SETTINGS_OVERRIDES?.[symbol];
  const v = o && Object.prototype.hasOwnProperty.call(o, key) ? o[key] : undefined;
  return v != null ? v : SETTINGS[key];
}

/* ─────────────────────────────── 4) UTILITIES / HTTP ─────────────────────────────── */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sleepWithSignal = (ms, signal) =>
  new Promise((resolve) => {
    if (!signal) {
      sleep(ms).then(() => resolve(true));
      return;
    }
    if (signal.aborted) {
      resolve(false);
      return;
    }
    let t = null;
    const onAbort = () => {
      if (t) clearTimeout(t);
      resolve(false);
    };
    t = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve(true);
    }, ms);
    signal.addEventListener('abort', onAbort, { once: true });
  });

function safeJsonParse(raw, fallback = null, context = 'unknown') {
  if (!raw) return fallback;
  try {
    return JSON.parse(raw);
  } catch (err) {
    console.warn('JSON parse failed', { context, error: err?.message || err });
    return fallback;
  }
}

// --- Global request rate limiter (simple token bucket) ---
let __TOKENS = 180; // ~180 req/min default budget
let __LAST_REFILL = Date.now();
const __REFILL_RATE = 180 / 60000; // tokens/ms
const __MAX_TOKENS = 180;
const RATE_LIMIT_BACKOFF_MS = [250, 500, 1000, 2000, 5000];
async function __takeToken() {
  const now = Date.now();
  const elapsed = now - __LAST_REFILL;
  if (elapsed > 0) {
    __TOKENS = Math.min(__MAX_TOKENS, __TOKENS + elapsed * __REFILL_RATE);
    __LAST_REFILL = now;
  }
  if (__TOKENS >= 1) {
    __TOKENS -= 1;
    return;
  }
  const waitMs = Math.ceil((1 - __TOKENS) / __REFILL_RATE);
  await sleep(Math.min(1500, Math.max(50, waitMs)));
  return __takeToken();
}

function readRateLimitHeaders(res) {
  return {
    limit: res?.headers?.get?.('x-ratelimit-limit') || null,
    remaining: res?.headers?.get?.('x-ratelimit-remaining') || null,
    reset: res?.headers?.get?.('x-ratelimit-reset') || null,
  };
}

function computeRateLimitBackoff(attempt, headers = {}) {
  const base = RATE_LIMIT_BACKOFF_MS[Math.min(attempt, RATE_LIMIT_BACKOFF_MS.length - 1)];
  const resetRaw = headers.reset ? Number(headers.reset) : null;
  if (Number.isFinite(resetRaw) && resetRaw > 0) {
    const nowSec = Date.now() / 1000;
    const waitMs = Math.max(0, Math.ceil((resetRaw - nowSec) * 1000));
    return Math.max(base, Math.min(waitMs, 5000));
  }
  return base;
}

async function f(url, opts = {}, timeoutMs = 12000, retries = 3) {
  let lastErr = null;
  for (let i = 0; i <= retries; i++) {
    await __takeToken();
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...opts, signal: ac.signal });
      clearTimeout(t);
      if (res.status === 429 || res.status >= 500) {
        if (i === retries) return res;
        const headers = res.status === 429 ? readRateLimitHeaders(res) : null;
        const extra = res.status === 429 ? computeRateLimitBackoff(i, headers || {}) : 0;
        await sleep(400 * Math.pow(2, i) + Math.floor(Math.random() * 300) + extra);
        continue;
      }
      return res;
    } catch (e) {
      clearTimeout(t);
      lastErr = e;
      if (i === retries) throw e;
      await sleep(300 * Math.pow(2, i) + Math.floor(Math.random() * 250));
    }
  }
  throw lastErr || new Error('fetch failed');
}

const MARKET_DATA_TIMEOUT_MS = 9000;
const MARKET_DATA_RETRIES = 3;
const MARKET_DATA_FAILURE_LIMIT = 5;
const MARKET_DATA_COOLDOWN_MS = 60000;
const marketDataState = {
  consecutiveFailures: 0,
  cooldownUntil: 0,
  cooldownLoggedAt: 0,
};
let dataDegradedUntil = 0;

function buildBackendUrl({ path, params, label }) {
  const base = BACKEND_BASE_URL;
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
}

const isMarketDataCooldown = () => Date.now() < marketDataState.cooldownUntil;
const isDataDegraded = () => Date.now() < dataDegradedUntil;
const markDataDegraded = () => {
  dataDegradedUntil = Math.max(dataDegradedUntil, Date.now() + 2000);
};

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
};

async function fetchMarketData(type, url, opts = {}) {
  if (isMarketDataCooldown()) {
    logMarketDataDiagnostics({ type, url, statusCode: null, snippet: '', errorType: 'cooldown' });
    const err = new Error('Market data cooldown active');
    err.code = 'COOLDOWN';
    throw err;
  }
  if (isDataDegraded()) {
    logMarketDataDiagnostics({ type, url, statusCode: null, snippet: '', errorType: 'degraded' });
    const err = new Error('Market data degraded');
    err.code = 'DEGRADED';
    throw err;
  }

  try {
    const res = await f(url, opts, MARKET_DATA_TIMEOUT_MS, MARKET_DATA_RETRIES);
    const status = res?.status;
    if (!res?.ok) {
      const body = await res.text().catch(() => '');
      if (status === 429) {
        const rl = readRateLimitHeaders(res);
        markDataDegraded();
        logMarketDataDiagnostics({
          type,
          url,
          statusCode: status,
          snippet: body?.slice?.(0, 200),
          errorType: 'rate_limit',
        });
        const err = new Error(`HTTP ${status}`);
        err.code = 'RATE_LIMIT';
        err.statusCode = status;
        err.responseSnippet = body?.slice?.(0, 200);
        err.rateLimit = rl;
        throw err;
      }
      logMarketDataDiagnostics({
        type,
        url,
        statusCode: status,
        snippet: body?.slice?.(0, 200),
        errorType: 'http',
      });
      markMarketDataFailure();
      markDataDegraded();
      const err = new Error(`HTTP ${status}`);
      err.code = 'HTTP_ERROR';
      err.statusCode = status;
      err.responseSnippet = body?.slice?.(0, 200);
      throw err;
    }

    const text = await res.text().catch(() => '');
    logMarketDataDiagnostics({ type, url, statusCode: status, snippet: '', errorType: 'ok' });
    markMarketDataSuccess();
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch (err) {
      logMarketDataDiagnostics({
        type,
        url,
        statusCode: status,
        snippet: text.slice(0, 200),
        errorType: 'parse',
      });
      markMarketDataFailure();
      markDataDegraded();
      const parseErr = new Error('parse_error');
      parseErr.code = 'PARSE_ERROR';
      throw parseErr;
    }
  } catch (err) {
    if (err?.name === 'AbortError' || err?.message?.includes?.('Network') || err?.message?.includes?.('fetch')) {
      logMarketDataDiagnostics({
        type,
        url,
        statusCode: null,
        snippet: err?.message || '',
        errorType: 'network',
      });
      markMarketDataFailure();
      markDataDegraded();
    }
    throw err;
  }
}

/* ─────────────────────────── 5) TIME / PARSE / FORMATTING ─────────────────────────── */
const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const fmtUSD = (n) =>
  Number.isFinite(n)
    ? `$ ${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
    : '—';
const fmtPct = (n) => (Number.isFinite(n) ? `${n.toFixed(2)}%` : '—');
const MAX_QUOTE_AGE_MS = Number(EX.MAX_QUOTE_AGE_MS || SETTINGS.quoteTtlMs || 30000);
const ABSURD_AGE_MS = 86400 * 1000;
const MAX_CLOCK_SKEW_MS = 5000;
const DEBUG_QUOTE_TS = ['1', 'true', 'yes'].includes(String(EX.DEBUG_QUOTE_TS || '').toLowerCase());
const quoteTsDebugLogged = new Set();
const quoteBadTsLogged = new Set();

const normalizeQuoteTsMs = (rawTs) => {
  if (rawTs == null) return null;
  if (rawTs instanceof Date) {
    const ts = rawTs.getTime();
    return Number.isFinite(ts) ? ts : null;
  }
  if (typeof rawTs === 'string') {
    const trimmed = rawTs.trim();
    if (!trimmed) return null;
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) return normalizeEpochNumber(numeric);
    const parsed = Date.parse(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (typeof rawTs === 'number') {
    return normalizeEpochNumber(rawTs);
  }
  return null;
};

const normalizeEpochNumber = (rawTs) => {
  if (!Number.isFinite(rawTs)) return null;
  const abs = Math.abs(rawTs);
  return abs < 2e10 ? abs * 1000 : abs;
};

const parseTsMs = normalizeQuoteTsMs;
const isFresh = (tsMs, ttlMs) => Number.isFinite(tsMs) && (Date.now() - tsMs <= ttlMs);
const computeQuoteAgeMs = ({ nowMs, tsMs }) => {
  if (!Number.isFinite(nowMs) || !Number.isFinite(tsMs)) return null;
  let ageMs = nowMs - tsMs;
  if (!Number.isFinite(ageMs)) return null;
  if (ageMs < -MAX_CLOCK_SKEW_MS) ageMs = 0;
  if (ageMs > ABSURD_AGE_MS) return null;
  return ageMs;
};
const formatLoggedAgeSeconds = (ageMs) => (Number.isFinite(ageMs) ? Math.round(ageMs / 1000) : null);

const logQuoteTimestampDebug = ({ symbol, rawFields, source }) => {
  if (!DEBUG_QUOTE_TS) return;
  const key = `${symbol}:${source || 'unknown'}`;
  if (quoteTsDebugLogged.has(key)) return;
  quoteTsDebugLogged.add(key);
  console.warn('quote_ts_debug', {
    symbol,
    source: source || null,
    rawTs: rawFields,
  });
};

const logBadQuoteTimestamp = ({ symbol, rawFields }) => {
  if (!symbol) return;
  const key = symbol;
  if (quoteBadTsLogged.has(key)) return;
  quoteBadTsLogged.add(key);
  console.warn(`QUOTE BAD_TS — ${symbol} — raw=${JSON.stringify(rawFields)}`);
};

const parseQuoteTimestampMs = ({ symbol, rawFields, source }) => {
  const rawTs = rawFields?.t ?? rawFields?.timestamp ?? rawFields?.time ?? rawFields?.ts;
  if (symbol) {
    logQuoteTimestampDebug({ symbol, rawFields, source });
  }
  const tsMs = normalizeQuoteTsMs(rawTs);
  if (!Number.isFinite(tsMs)) {
    logBadQuoteTimestamp({ symbol, rawFields });
    return null;
  }
  return tsMs;
};

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
const roundToTick = (px, tick) => Math.ceil(px / tick) * tick;
const isFractionalQty = (q) => Math.abs(q - Math.round(q)) > 1e-6;
const isoDaysAgo = (n) => new Date(Date.now() - n * 864e5).toISOString();

/* ──────────────────────────── 6) SYMBOL HELPERS ──────────────────────────── */
function synthQuoteFromTrade(price, bps = SETTINGS.syntheticTradeSpreadBps) {
  if (!(price > 0)) return null;
  const half = price * (bps / 20000);
  return { bid: price - half, ask: price + half, bs: null, as: null, tms: Date.now() };
}

/* ───────────────────────── 7) ACCOUNT / HISTORY / ACTIVITIES ───────────────────────── */
async function getPortfolioHistory({
  period = '1D',
  timeframe = '5Min',
  intraday_reporting = 'continuous', // crypto-friendly
  pnl_reset = 'no_reset',            // continuous P&L
  extended_hours = true,             // harmless toggle for equities; ignored for crypto
} = {}) {
  const sp = new URLSearchParams({
    period: String(period),
    timeframe: String(timeframe),
    intraday_reporting: String(intraday_reporting),
    pnl_reset: String(pnl_reset),
    extended_hours: String(!!extended_hours),
  });
  const url = `${BACKEND_BASE_URL}/account/portfolio/history?${sp.toString()}`;
  const res = await f(url, { headers: BACKEND_HEADERS });
  if (!res.ok) return null;
  try { return await res.json(); } catch { return null; }
}

async function getActivities({ afterISO, untilISO, pageToken, types } = {}) {
  const params = new URLSearchParams({
    activity_types: types || 'FILL,CFEE,FEE,PTC',
    direction: 'desc',
    page_size: '100',
  });
  if (afterISO) params.set('after', afterISO);
  if (untilISO) params.set('until', untilISO);
  if (pageToken) params.set('page_token', pageToken);

  const url = `${BACKEND_BASE_URL}/account/activities?${params.toString()}`;
  const res = await f(url, { headers: BACKEND_HEADERS });
  let items = [];
  let payload = null;
  try { payload = await res.json(); } catch {}
  if (Array.isArray(payload)) {
    items = payload;
  } else if (payload && Array.isArray(payload.items)) {
    items = payload.items;
  }
  const next = res.headers?.get?.('x-next-page-token') || null;
  const nextJson = payload?.nextPageToken || payload?.next_page_token || null;
  return { items: Array.isArray(items) ? items : [], next: next || nextJson };
}

async function getPnLAndFeesSnapshot() {
  const hist1M = await getPortfolioHistory({ period: '1M', timeframe: '1D' });
  let last7Sum = null, last7DownDays = null, last7UpDays = null, last30Sum = null;
  if (hist1M?.profit_loss) {
    const pl = hist1M.profit_loss.map(Number). filter(Number.isFinite);
    const last7 = pl.slice(-7), last30 = pl.slice(-30);
    last7Sum = last7.reduce((a,b)=>a+b,0);
    last30Sum = last30.reduce((a,b)=>a+b,0);
    last7UpDays = last7.filter((x)=>x>0).length;
    last7DownDays = last7.filter((x)=>x<0).length;
  }

  let fees30 = 0, fillsCount30 = 0;
  const afterISO = isoDaysAgo(30), untilISO = new Date().toISOString();
  let token = null;
  for (let i = 0; i < 10; i++) {
    const { items, next } = await getActivities({ afterISO, untilISO, pageToken: token });
    for (const it of items) {
      const t = (it?.activity_type || it?.activityType || '').toUpperCase();
      if (t === 'CFEE' || t === 'FEE' || t === 'PTC') {
        const raw = it.net_amount ?? it.amount ?? it.price ?? (Number(it.per_share_amount) * Number(it.qty) || NaN);
        const amt = Number(raw);
        if (Number.isFinite(amt)) fees30 += amt;
      } else if (t === 'FILL') fillsCount30 += 1;
    }
    if (!next) break;
    token = next;
  }
  return { last7Sum, last7UpDays, last7DownDays, last30Sum, fees30, fillsCount30 };
}

/* ───────────────────────────── 8) MARKET CLOCK (STOCKS) ───────────────────────────── */
async function getStockClock() {
  try {
    const r = await f(`${BACKEND_BASE_URL}/clock`, { headers: BACKEND_HEADERS });
    if (!r.ok) return { is_open: false };
    const j = await r.json();
    return { is_open: !!j.is_open, next_open: j.next_open, next_close: j.next_close };
  } catch {
    return { is_open: false };
  }
}
let STOCK_CLOCK_CACHE = { value: { is_open: false }, ts: 0 };
async function getStockClockCached(ttlMs = 30000) {
  const now = Date.now();
  if (now - STOCK_CLOCK_CACHE.ts < ttlMs) return STOCK_CLOCK_CACHE.value;
  const v = await getStockClock();
  STOCK_CLOCK_CACHE = { value: v, ts: now };
  return v;
}

/* ──────────────────────── 9) TRANSACTION HISTORY → CSV VIEWER ──────────────────────── */
const TxnHistoryCSVViewer = () => {
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState('');
  const [csv, setCsv] = useState('');
  const csvRef = useRef(null);
  const [collapsed, setCollapsed] = useState(false);

  async function fetchActivities({ days = 7, types = 'FILL,CFEE,FEE,TRANS,PTC', max = 1000 } = {}) {
    const untilISO = new Date().toISOString();
    const afterISO = new Date(Date.now() - days * 864e5).toISOString();
    let token = null;
    let all = [];
    for (let i = 0; i < 20; i++) {
      const { items, next } = await getActivities({ afterISO, untilISO, pageToken: token, types });
      all = all.concat(items);
      if (!next || all.length >= max) break;
      token = next;
    }
    return all.slice(0, max);
  }

  function toCsv(rows) {
    const header = ['DateTime', 'Type', 'Side', 'Symbol', 'Qty', 'Price', 'CashFlowUSD', 'OrderID', 'ActivityID'];
    const escape = (v) => {
      if (v == null) return '';
      const s = String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const lines = [header.join(',')];
    for (const r of rows) {
      const dtISO = r.transaction_time || r.date || '';
      const local = dtISO ? new Date(dtISO).toLocaleString() : '';
      const side = r.side || '';
      const symbol = r.symbol || '';
      const qty = r.qty || r.cum_qty || '';
      const price = r.price || '';
      let cash = '';
      if ((r.activity_type || '').toUpperCase() === 'FILL') {
        const q = parseFloat(qty ?? '0');
        const p = parseFloat(price ?? '0');
        if (Number.isFinite(q) && Number.isFinite(p)) {
          const signed = q * p * (side === 'buy' ? -1 : 1);
          cash = signed.toFixed(2);
        }
      } else {
        const net = parseFloat(r.net_amount ?? r.amount ?? '');
        cash = Number.isFinite(net) ? net.toFixed(2) : '';
      }
      const row = [local, r.activity_type, side, symbol, qty, price, cash, r.order_id || '', r.id || ''];
      lines.push(row.map(escape).join(','));
    }
    return lines.join('\n');
  }

  const buildRange = async (days) => {
    try {
      setBusy(true);
      setStatus('Fetching…');
      setCsv('');
      setCollapsed(false);
      const acts = await fetchActivities({ days });
      if (!acts.length) {
        setStatus('No activities found in range.');
        return;
      }
      const out = toCsv(acts);
      setCsv(out);
      setStatus(`Built ${acts.length} activities (${days}d). Tap the box → Select All → Copy.`);
      setTimeout(() => {
        try {
          csvRef.current?.focus?.();
          csvRef.current?.setNativeProps?.({ selection: { start: 0, end: out.length } });
        } catch {}
      }, 150);
    } catch (err) {
      setStatus(`Error: ${err.message}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <View style={styles.txnBox}>
      <Text style={styles.txnTitle}>Transaction History → CSV</Text>
      <View style={styles.txnBtnRow}>
        <TouchableOpacity style={styles.txnBtn} onPress={() => buildRange(1)} disabled={busy}>
          <Text style={styles.txnBtnText}>Build 24h CSV</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles.txnBtn} onPress={() => buildRange(7)} disabled={busy}>
          <Text style={styles.txnBtnText}>Build 7d CSV</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles.txnBtn} onPress={() => buildRange(30)} disabled={busy}>
          <Text style={styles.txnBtnText}>Build 30d CSV</Text>
        </TouchableOpacity>
      </View>
      {busy ? <ActivityIndicator /> : null}
      <Text style={styles.txnStatus}>{status}</Text>
      {csv ? (
        <>
          {!collapsed ? (
            <View style={{ marginTop: 8 }}>
              <Text style={styles.csvHelp}>Tap the box → Select All → Copy</Text>
              <TouchableOpacity onPress={() => setCollapsed(true)} style={styles.chip}>
                <Text style={styles.chipText}>Minimize</Text>
              </TouchableOpacity>
              <TextInput
                ref={csvRef}
                style={styles.csvBox}
                value={csv}
                editable={false}
                multiline
                selectTextOnFocus
                scrollEnabled
                textBreakStrategy="highQuality"
              />
            </View>
          ) : (
            <View style={{ marginTop: 8 }}>
              <TouchableOpacity onPress={() => setCollapsed(false)} style={styles.chip}>
                <Text style={styles.chipText}>Show CSV</Text>
              </TouchableOpacity>
            </View>
          )}
        </>
      ) : null}
    </View>
  );
};

/* ─────────────────────────── 9b) LIVE LOGS → COPY VIEWER ─────────────────────────── */
const LiveLogsCopyViewer = ({ logs = [] }) => {
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState('');
  const [txt, setTxt] = useState('');
  const txtRef = useRef(null);
  const [collapsed, setCollapsed] = useState(false);

  const build = async () => {
    try {
      setBusy(true);
      setStatus('Building snapshot…');
      setTxt('');
      setCollapsed(false);
      const lines = (logs || []).map((l) => {
        const ts = new Date(l.ts).toLocaleString();
        return `${ts} • ${l.text}`;
      });
      const out = lines.join('\n');
      setTxt(out);
      setStatus(`Built ${lines.length} lines. Tap the box → Select All → Copy.`);
      setTimeout(() => {
        try {
          txtRef.current?.focus?.();
          txtRef.current?.setNativeProps?.({ selection: { start: 0, end: out.length } });
        } catch {}
      }, 150);
    } catch (e) {
      setStatus(`Error: ${e?.message || e}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <View style={styles.txnBox}>
      <Text style={styles.txnTitle}>Live Logs → Copy</Text>
      <View style={styles.txnBtnRow}>
        <TouchableOpacity style={styles.txnBtn} onPress={build} disabled={busy}>
          <Text style={styles.txnBtnText}>Build Snapshot</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles.txnBtn} onPress={() => setCollapsed((v) => !v)} disabled={busy}>
          <Text style={styles.txnBtnText}>{collapsed ? 'Show' : 'Minimize'}</Text>
        </TouchableOpacity>
      </View>
      {busy ? <ActivityIndicator /> : null}
      <Text style={styles.txnStatus}>{status}</Text>
      {txt && !collapsed ? (
        <TextInput
          ref={txtRef}
          style={styles.csvBox}
          value={txt}
          editable={false}
          multiline
          selectTextOnFocus
          scrollEnabled
          textBreakStrategy="highQuality"
        />
      ) : null}
    </View>
  );
};

/* ─────────────────────────── 9c) PnL & Benchmark Scoreboard ─────────────────────────── */
// Build minimal events from Alpaca activities, then compute FIFO realized P&L and fees.
function buildEventsFromActivities(acts = []) {
  const evs = [];
  for (const a of acts) {
    const t = String(a.activity_type || a.activityType || '').toUpperCase();
    const sym = toInternalSymbol(a.symbol || a.symbol_id || null);
    const side = String(a.side || '').toLowerCase();
    const qty = Number(a.qty ?? a.cum_qty ?? a.quantity ?? 0);
    const price = Number(a.price ?? a.fill_price ?? a.avg_price ?? 0);
    const tsRaw = a.transaction_time || a.date || a.timestamp || a.processed_at || a.created_at || a.time || '';
    const tsMs = parseTsMs(tsRaw);

    if (t === 'FILL') {
      const q = Math.abs(Number(qty) || 0);
      if (!(q > 0) || !(price > 0) || !sym) continue;
      const cashUsd = q * price * (side === 'buy' ? -1 : 1);
      evs.push({ tsMs, type: 'FILL', side, symbol: sym, qty: q, price, cashUsd });
    } else if (t === 'CFEE' || t === 'FEE' || t === 'PTC') {
      const net = Number(a.net_amount ?? a.amount ?? a.price ?? 0);
      if (sym && Number(qty) < 0 && price > 0) {
        // Asset-denominated fee: negative qty at a price → remove from lots & treat as USD fee at that valuation
        evs.push({ tsMs, type: 'CFEE_ASSET', symbol: sym, qty: Math.abs(Number(qty) || 0), price, cashUsd: 0 });
      } else {
        // Pure USD fee
        const usd = Math.abs(Number(net) || 0);
        if (usd > 0) evs.push({ tsMs, type: 'CFEE_USD', symbol: null, qty: 0, price: 0, cashUsd: -usd });
      }
    }
  }
  return evs.sort((a, b) => (a.tsMs || 0) - (b.tsMs || 0));
}

function fifoPnlFromEvents(events = []) {
  const book = new Map(); // sym -> [{qty, cost}]
  let realized = 0;
  let feesUsd = 0;
  const cashFlows = []; // [{ts, cash}]

  const lotsOf = (sym) => {
    if (!book.has(sym)) book.set(sym, []);
    return book.get(sym);
  };
  const removeQtyFIFO = (sym, qToRemove, pxForPnl = null, isSell = false) => {
    const lots = lotsOf(sym);
    let remain = qToRemove;
    while (remain > 1e-12 && lots.length) {
      const lot = lots[0];
      const take = Math.min(lot.qty, remain);
      if (isSell && pxForPnl != null) {
        realized += (pxForPnl - lot.cost) * take;
      }
      lot.qty -= take;
      remain -= take;
      if (lot.qty <= 1e-12) lots.shift();
    }
    return qToRemove - remain; // matched amount
  };

  for (const e of events) {
    if (e.type === 'FILL') {
      cashFlows.push({ ts: e.tsMs, cash: e.cashUsd });
      if (e.side === 'buy') {
        const lots = book.get(e.symbol) || [];
        lots.push({ qty: e.qty, cost: e.price });
        book.set(e.symbol, lots);
      } else if (e.side === 'sell') {
        const matched = removeQtyFIFO(e.symbol, e.qty, e.price, true);
        // If a sell exceeds in-window lots, we ignore the unmatched portion (came from pre-window inventory).
        if (matched < e.qty) {
          // Partial info only — safe fallback (do nothing for the remainder).
        }
      }
    } else if (e.type === 'CFEE_USD') {
      feesUsd += Math.abs(e.cashUsd || 0);
    } else if (e.type === 'CFEE_ASSET') {
      // Remove the asset qty from FIFO and count USD value of the removed qty as a fee
      const matched = removeQtyFIFO(e.symbol, e.qty, null, false);
      feesUsd += matched * (e.price || 0);
    }
  }
  return { book, realizedUsd: realized, feesUsd, cashFlows };
}

async function markToMarketBookPrices(book) {
  const syms = Array.from(book.keys());
  if (!syms.length) return { unrealizedUsd: 0, pricesAt: {} };
  const internalSyms = syms.map((s) => toInternalSymbol(s));
  const qmap = await getCryptoQuotesBatch(internalSyms);
  let unreal = 0;
  const pricesAt = {};
  for (const s of syms) {
    const q = qmap.get(toInternalSymbol(s));
    const mid = q && Number.isFinite(q.bid) && Number.isFinite(q.ask) ? 0.5 * (q.bid + q.ask) : (q?.bid || q?.ask || 0);
    if (mid > 0) pricesAt[s] = mid;
    const lots = book.get(s) || [];
    for (const lot of lots) {
      if (mid > 0) unreal += (mid - lot.cost) * lot.qty;
    }
  }
  return { unrealizedUsd: unreal, pricesAt };
}

function computePeakOutflow(cashFlows = []) {
  // Track cumulative cash (negative = invested). Peak outflow = min cumulative (absolute).
  let cum = 0, minCum = 0;
  for (const cf of cashFlows.sort((a, b) => (a.ts || 0) - (b.ts || 0))) {
    cum += Number(cf.cash || 0);
    if (cum < minCum) minCum = cum;
  }
  return Math.abs(minCum);
}

// Lightweight activities fetch (mirrors TxnHistoryCSVViewer internal)
async function fetchActivitiesRange({ days = 7, types = 'FILL,CFEE,FEE,TRANS,PTC', max = 1000 } = {}) {
  const untilISO = new Date().toISOString();
  const afterISO = new Date(Date.now() - days * 864e5).toISOString();
  let token = null;
  let all = [];
  for (let i = 0; i < 20; i++) {
    const { items, next } = await getActivities({ afterISO, untilISO, pageToken: token, types });
    all = all.concat(items);
    if (!next || all.length >= max) break;
    token = next;
  }
  return all.slice(0, max);
}

const PnlScoreboard = ({ days = 7 }) => {
  const [busy, setBusy] = useState(false);
  const [score, setScore] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    (async () => {
      setBusy(true);
      setError(null);
      try {
        const acts = await fetchActivitiesRange({ days });
        const events = buildEventsFromActivities(acts);
        const { book, realizedUsd, feesUsd, cashFlows } = fifoPnlFromEvents(events);
        const { unrealizedUsd } = await markToMarketBookPrices(book);
        const realizedNet = realizedUsd - feesUsd;
        const totalNet = realizedNet + unrealizedUsd;
        const peakOut = computePeakOutflow(cashFlows);
        const { spyRet, btcRet } = await computeBenchReturns(days);
        const spyHypo = peakOut * spyRet;
        const btcHypo = peakOut * btcRet;
        setScore({
          realizedUsd, feesUsd, realizedNet,
          unrealizedUsd, totalNet, peakOut,
          spyRet, btcRet, spyHypo, btcHypo,
          updatedAt: new Date().toISOString(),
        });
      } catch (e) {
        setError(e?.message || String(e));
      } finally {
        setBusy(false);
      }
    })();
  }, [days]);

  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>P&L (last {days}d) — Realized vs Unrealized + Benchmarks</Text>
      {busy && <ActivityIndicator />}
      {error && <Text style={styles.sevError}>Error: {error}</Text>}
      {score && (
        <View style={{ gap: 6 }}>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Realized (gross)</Text>
            <Text style={styles.value}>{fmtUSD(score.realizedUsd)}</Text>
          </View>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Fees</Text>
            <Text style={styles.value}>{fmtUSD(-Math.abs(score.feesUsd))}</Text>
          </View>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Realized (net)</Text>
            <Text style={styles.value}>{fmtUSD(score.realizedNet)}</Text>
          </View>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Unrealized (open lots)</Text>
            <Text style={[styles.value, { color: '#355070' }]}>{fmtUSD(score.unrealizedUsd)}</Text>
          </View>
          <View style={styles.line} />
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Total (net + unrealized)</Text>
            <Text style={styles.value}>{fmtUSD(score.totalNet)}</Text>
          </View>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Peak net cash deployed</Text>
            <Text style={styles.value}>{fmtUSD(score.peakOut)}</Text>
          </View>
          <View style={styles.line} />
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>SPY return over window</Text>
            <Text style={styles.value}>{fmtPct(score.spyRet * 100)}</Text>
          </View>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>BTC return over window</Text>
            <Text style={styles.value}>{fmtPct(score.btcRet * 100)}</Text>
          </View>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Hypothetical $ on SPY (peakOut × ret)</Text>
            <Text style={styles.value}>{fmtUSD(score.spyHypo)}</Text>
          </View>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Hypothetical $ on BTC (peakOut × ret)</Text>
            <Text style={styles.value}>{fmtUSD(score.btcHypo)}</Text>
          </View>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Alpha vs SPY (realized net − SPY $)</Text>
            <Text style={styles.value}>{fmtUSD(score.realizedNet - score.spyHypo)}</Text>
          </View>
          <View style={styles.legendRow}>
            <Text style={styles.subtle}>Alpha vs BTC (realized net − BTC $)</Text>
            <Text style={styles.value}>{fmtUSD(score.realizedNet - score.btcHypo)}</Text>
          </View>
          <Text style={styles.smallNote}>Updated {new Date(score.updatedAt).toLocaleTimeString()}</Text>
        </View>
      )}
    </View>
  );
};

/* ───────────────────────────── 10) STATIC UNIVERSES ───────────────────────────── */
const ORIGINAL_TOKENS = [
  { name: 'ETH/USD',  symbol: 'ETH/USD',  cc: 'ETH'  },
  { name: 'AAVE/USD', symbol: 'AAVE/USD', cc: 'AAVE' },
  { name: 'LTC/USD',  symbol: 'LTC/USD',  cc: 'LTC'  },
  { name: 'LINK/USD', symbol: 'LINK/USD', cc: 'LINK' },
  { name: 'UNI/USD',  symbol: 'UNI/USD',  cc: 'UNI'  },
  { name: 'SOL/USD',  symbol: 'SOL/USD',  cc: 'SOL'  },
  { name: 'BTC/USD',  symbol: 'BTC/USD',  cc: 'BTC'  },
  { name: 'AVAX/USD', symbol: 'AVAX/USD', cc: 'AVAX' },
  { name: 'ADA/USD',  symbol: 'ADA/USD',  cc: 'ADA'  },
  { name: 'MATIC/USD',symbol: 'MATIC/USD',cc: 'MATIC'},
  { name: 'XRP/USD',  symbol: 'XRP/USD',  cc: 'XRP'  },
  { name: 'SHIB/USD', symbol: 'SHIB/USD', cc: 'SHIB' },
  { name: 'BCH/USD',  symbol: 'BCH/USD',  cc: 'BCH'  },
  { name: 'ETC/USD',  symbol: 'ETC/USD',  cc: 'ETC'  },
  { name: 'TRX/USD',  symbol: 'TRX/USD',  cc: 'TRX'  },
  { name: 'USDT/USD', symbol: 'USDT/USD', cc: 'USDT' },
  { name: 'USDC/USD', symbol: 'USDC/USD', cc: 'USDC' },
];
const CRYPTO_CORE_TRACKED = ORIGINAL_TOKENS.filter(t => !STABLES.has(t.symbol));

const TRAD_100 = [];
const CRYPTO_STOCKS_100 = [];
const STATIC_UNIVERSE = Array.from(
  new Map([...TRAD_100, ...CRYPTO_STOCKS_100].map((s) => [s, { name: s, symbol: s, cc: null }])).values()
);

/* ───────────────────────── 11) QUOTE CACHE / SUPPORT FLAGS ───────────────────────── */
const SUPPORTED_CRYPTO_CACHE_KEY = 'supported_crypto_pairs_v1';
const SUPPORTED_CRYPTO_CACHE_TS_KEY = 'supported_crypto_pairs_last_refresh_v1';
const SUPPORTED_CRYPTO_REFRESH_MS = 24 * 60 * 60 * 1000;
const quoteCache = new Map();
const lastQuoteBatchMissing = new Map();
const unsupportedSymbols = new Map();
const unsupportedLocalSymbols = new Set();
const unsupportedLogTimestamps = new Map();
const getQuoteLastSeenMs = (quote) => {
  const tsMs = Number(quote?.tsMs ?? 0);
  const recvMs = Number(quote?.receivedAtMs ?? 0);
  return Math.max(tsMs || 0, recvMs || 0);
};
const isStaleQuoteEntry = (quote) => {
  const lastSeenMs = getQuoteLastSeenMs(quote);
  if (!Number.isFinite(lastSeenMs) || lastSeenMs <= 0) return true;
  return (Date.now() - lastSeenMs) > MAX_QUOTE_AGE_MS;
};
const logStaleQuote = (symbol, quote, context = {}) => {
  const lastSeenMs = getQuoteLastSeenMs(quote);
  const lastSeenIso = Number.isFinite(lastSeenMs) && lastSeenMs > 0 ? new Date(lastSeenMs).toISOString() : null;
  const ageMs = Number.isFinite(lastSeenMs) && lastSeenMs > 0 ? Date.now() - lastSeenMs : null;
  const ageSec = Number.isFinite(ageMs) ? Math.round(ageMs / 1000) : 'NA';
  console.warn(`QUOTE_STALE symbol=${symbol} age_s=${ageSec}`);
  logTradeAction('stale_quote', symbol, {
    lastSeenIso,
    lastSeenAgeSec: formatLoggedAgeSeconds(ageMs),
    lastError: quote?.lastError ?? null,
    source: quote?.source ?? null,
    ...context,
  });
};
const isUnsupported = (sym) => {
  const normalized = toInternalSymbol(sym);
  const u = unsupportedSymbols.get(normalized);
  if (!u) return false;
  if (Date.now() > u) { unsupportedSymbols.delete(normalized); return false; }
  return true;
};
function markUnsupported(sym, mins = 120) {
  const normalized = toInternalSymbol(sym);
  if (!normalized) return;
  unsupportedSymbols.set(normalized, Date.now() + mins * 60000);
}
const isUnsupportedLocal = (sym) => {
  const normalized = toInternalSymbol(sym);
  return isUnsupported(normalized) || unsupportedLocalSymbols.has(normalized);
};
const logUnsupportedOnce = (sym, meta = {}) => {
  const normalized = toInternalSymbol(sym);
  if (!normalized) return;
  const now = Date.now();
  const last = unsupportedLogTimestamps.get(normalized) || 0;
  if (now - last < 3600 * 1000) return;
  unsupportedLogTimestamps.set(normalized, now);
  console.warn('unsupported_symbol', { symbol: normalized, ...meta });
};
const markUnsupportedLocal = (sym, mins = 120) => {
  const normalized = toInternalSymbol(sym);
  if (!normalized) return;
  unsupportedLocalSymbols.add(normalized);
  markUnsupported(normalized, mins);
  logUnsupportedOnce(normalized);
};
const isUnsupportedAssetError = (err) => {
  if (err?.statusCode === 404) return true;
  const body = String(err?.responseSnippet || err?.message || '').toLowerCase();
  return body.includes('not supported') || body.includes('unsupported') || body.includes('asset') && body.includes('not');
};
const isSupportedCryptoSymbol = (sym, supportedSet) => {
  if (!supportedSet || supportedSet.size === 0) return true;
  const normalized = normalizeCryptoSymbol(sym);
  if (!normalized) return true;
  return supportedSet.has(normalized);
};

async function fetchSupportedCryptoPairs({ force = false } = {}) {
  const now = Date.now();
  let cachedSet = new Set();
  try {
    const [cachedRaw, cachedTsRaw] = await Promise.all([
      AsyncStorage.getItem(SUPPORTED_CRYPTO_CACHE_KEY),
      AsyncStorage.getItem(SUPPORTED_CRYPTO_CACHE_TS_KEY),
    ]);
    const cachedTs = Number(cachedTsRaw || 0);
    const cachedArr = cachedRaw ? safeJsonParse(cachedRaw, [], 'supported_pairs_cache') : [];
    if (Array.isArray(cachedArr)) {
      cachedSet = new Set(cachedArr.map((s) => normalizeCryptoSymbol(s)));
    }
    const shouldRefresh = force || !cachedTs || now - cachedTs > SUPPORTED_CRYPTO_REFRESH_MS;
    if (!shouldRefresh) return cachedSet;
  } catch {
    // fall through to network fetch
  }

  try {
    const url = `${BACKEND_BASE_URL}/crypto/supported`;
    const res = await f(url, { headers: BACKEND_HEADERS }, 12000, 2);
    if (!res?.ok) throw new Error(`HTTP ${res?.status || 'unknown'}`);
    const data = await res.json().catch(() => ({}));
    const pairs = Array.isArray(data?.pairs) ? data.pairs : [];
    const supported = new Set(
      pairs
        .map((pair) => normalizeCryptoSymbol(pair))
        .filter((pair) => pair && pair.endsWith('/USD'))
    );
    const arr = Array.from(supported);
    await Promise.all([
      AsyncStorage.setItem(SUPPORTED_CRYPTO_CACHE_KEY, JSON.stringify(arr)),
      AsyncStorage.setItem(SUPPORTED_CRYPTO_CACHE_TS_KEY, String(now)),
    ]);
    return supported;
  } catch (err) {
    console.warn('Supported crypto pairs fetch failed', err?.message || err);
    return cachedSet;
  }
}

// -------------------------------------------------------------------------
// BAR CACHE
const barsCache = new Map();
const barsCacheTTL = 30000; // 30 seconds

/* ─────────────────────────────── 12) CRYPTO DATA API ─────────────────────────────── */
const buildURLCrypto = (loc, what, symbols = [], params = {}) => {
  const normalized = symbols.map((s) => normalizePair(s)).join(',');
  return buildBackendUrl({
    path: `market/crypto/${what}`,
    params: { symbols: normalized, location: loc, ...params },
    label: `crypto_${what}`,
  });
};

async function getCryptoQuotesBatch(symbols = []) {
  const internalSymbols = symbols.map((s) => toInternalSymbol(s)).filter(Boolean);
  if (!internalSymbols.length) return new Map();
  const dataSymbols = internalSymbols.map((s) => normalizePair(s));
  const now = Date.now();
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildURLCrypto(loc, 'quotes', dataSymbols);
      const j = await fetchMarketData('QUOTE', url, { headers: BACKEND_HEADERS });
      const raw = j?.quotes || {};
      const out = new Map();
      for (const symbol of internalSymbols) {
        const dataSymbol = normalizePair(symbol);
        const q = Array.isArray(raw[dataSymbol]) ? raw[dataSymbol][0] : raw[dataSymbol];
        if (!q) {
          console.warn(`NO_QUOTE symbol=${symbol}`);
          logTradeAction('no_quote', symbol, { reason: 'no_data', requestType: 'QUOTE' });
          lastQuoteBatchMissing.set(symbol, now);
          quoteCache.delete(symbol);
          continue;
        }
        const bid = Number(q.bp ?? q.bid_price);
        const ask = Number(q.ap ?? q.ask_price);
        const bs = Number(q.bs ?? q.bid_size);
        const as = Number(q.as ?? q.ask_size);
        const parsedTsMs = parseQuoteTimestampMs({ symbol, rawFields: q, source: 'crypto_quote' });
        const tsMs = Number.isFinite(parsedTsMs) ? parsedTsMs : 0;
        if (bid > 0 && ask > 0) {
          const receivedAtMs = now;
          const mid = (bid + ask) / 2;
          const spreadBps = mid > 0 ? ((ask - bid) / mid) * 10000 : 0;
          const source = tsMs > 0 ? 'quote_ts' : 'recv_ts';
          const quote = {
            bid,
            ask,
            bs: Number.isFinite(bs) ? bs : null,
            as: Number.isFinite(as) ? as : null,
            mid,
            spreadBps,
            tsMs,
            receivedAtMs,
            source,
            lastError: null,
          };
          out.set(symbol, quote);
          quoteCache.set(symbol, quote);
          lastQuoteBatchMissing.delete(symbol);
        } else {
          console.warn(`NO_QUOTE symbol=${symbol}`);
          logTradeAction('no_quote', symbol, { reason: 'invalid_bid_ask', requestType: 'QUOTE' });
          lastQuoteBatchMissing.set(symbol, now);
          quoteCache.delete(symbol);
        }
      }
      if (out.size) return out;
    } catch (e) {
      logTradeAction('quote_http_error', 'QUOTE', { status: e?.statusCode || e?.code || 'exception', loc, body: e?.responseSnippet || e?.message || '' });
      const isUnsupportedErr = isUnsupportedAssetError(e);
      for (const symbol of internalSymbols) {
        if (isUnsupportedErr) {
          markUnsupportedLocal(symbol, 240);
          continue;
        }
        console.warn(`NO_QUOTE symbol=${symbol}`);
        logTradeAction('no_quote', symbol, { reason: e?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'QUOTE' });
        const cached = quoteCache.get(symbol);
        if (cached) {
          cached.lastError = e?.message || e?.code || 'request_failed';
          quoteCache.delete(symbol);
        }
        lastQuoteBatchMissing.set(symbol, now);
      }
    }
  }
  return new Map();
}
async function getCryptoTradesBatch(symbols = []) {
  const internalSymbols = symbols.map((s) => toInternalSymbol(s)).filter(Boolean);
  if (!internalSymbols.length) return new Map();
  const dataSymbols = internalSymbols.map((s) => normalizePair(s));
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildURLCrypto(loc, 'trades', dataSymbols);
      const j = await fetchMarketData('TRADE', url, { headers: BACKEND_HEADERS });
      const raw = j?.trades || {};
      const out = new Map();
      for (const symbol of internalSymbols) {
        const dataSymbol = normalizePair(symbol);
        const t = Array.isArray(raw[dataSymbol]) ? raw[dataSymbol][0] : raw[dataSymbol];
        const p = Number(t?.p ?? t?.price);
        const tms = parseQuoteTimestampMs({ symbol, rawFields: t, source: 'crypto_trade' });
        if (Number.isFinite(p) && p > 0 && Number.isFinite(tms)) out.set(symbol, { price: p, tms });
      }
      if (out.size) return out;
    } catch (e) {
      logTradeAction('trade_http_error', 'TRADE', { status: e?.statusCode || e?.code || 'exception', loc, body: e?.responseSnippet || e?.message || '' });
      for (const symbol of internalSymbols) {
        logTradeAction('no_quote', symbol, { reason: e?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'TRADE' });
      }
    }
  }
  return new Map();
}
async function getCryptoBars1m(symbol, limit = 6) {
  const internalSymbol = toInternalSymbol(symbol);
  const dataSymbol = normalizePair(internalSymbol);
  const cached = barsCache.get(internalSymbol);
  const now = Date.now();
  if (cached && (now - cached.ts) < barsCacheTTL) {
    return cached.bars.slice(0, limit);
  }
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildBackendUrl({
        path: 'market/crypto/bars',
        params: { timeframe: '1Min', limit: String(limit), symbols: dataSymbol, location: loc },
        label: 'crypto_bars',
      });
      const j = await fetchMarketData('BARS', url, { headers: BACKEND_HEADERS });
      const arr = j?.bars?.[dataSymbol];
      if (Array.isArray(arr) && arr.length) {
        const bars = arr.map((b) => ({
          open: Number(b.o ?? b.open),
          high: Number(b.h ?? b.high),
          low: Number(b.l ?? b.low),
          close: Number(b.c ?? b.close),
          vol: Number(b.v ?? b.volume ?? 0),
          tms: parseTsMs(b.t),
        })).filter((x) => Number.isFinite(x.close) && x.close > 0);
        barsCache.set(internalSymbol, { ts: now, bars: bars.slice() });
        return bars;
      }
      logTradeAction('no_quote', internalSymbol, { reason: 'no_data', requestType: 'BARS' });
    } catch (e) {
      logTradeAction('quote_http_error', 'BARS', { status: e?.statusCode || e?.code || 'exception', loc, body: e?.responseSnippet || e?.message || '' });
      logTradeAction('no_quote', internalSymbol, { reason: e?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'BARS' });
    }
  }
  return [];
}

async function getCryptoBars1mBatch(symbols = [], limit = 6) {
  const uniqSyms = Array.from(new Set(symbols.map((s) => toInternalSymbol(s)).filter(Boolean)));
  if (!uniqSyms.length) return new Map();
  const dsymList = uniqSyms.map((s) => normalizePair(s));
  const out = new Map();
  const now = Date.now();

  const missing = [];
  for (const symbol of uniqSyms) {
    const cached = barsCache.get(symbol);
    if (cached && (now - cached.ts) < barsCacheTTL) {
      out.set(symbol, cached.bars.slice(0, limit));
    } else {
      missing.push(symbol);
    }
  }
  if (!missing.length) return out;

  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildBackendUrl({
        path: 'market/crypto/bars',
        params: { timeframe: '1Min', limit: String(limit), symbols: missing.map((s) => normalizePair(s)).join(','), location: loc },
        label: 'crypto_bars_batch',
      });
      const j = await fetchMarketData('BARS', url, { headers: BACKEND_HEADERS });
      const raw = j?.bars || {};
      for (const symbol of missing) {
        const dataSymbol = normalizePair(symbol);
        const arr = raw[dataSymbol];
        if (Array.isArray(arr) && arr.length) {
          const bars = arr.map((b) => ({
            open: Number(b.o ?? b.open),
            high: Number(b.h ?? b.high),
            low: Number(b.l ?? b.low),
            close: Number(b.c ?? b.close),
            vol: Number(b.v ?? b.volume ?? 0),
            tms: parseTsMs(b.t),
          })).filter((x) => Number.isFinite(x.close) && x.close > 0);
          barsCache.set(symbol, { ts: now, bars: bars.slice() });
          out.set(symbol, bars.slice(0, limit));
        } else {
          logTradeAction('no_quote', symbol, { reason: 'no_data', requestType: 'BARS' });
        }
      }
      break;
    } catch (e) {
      logTradeAction('quote_http_error', 'BARS', { status: e?.statusCode || e?.code || 'exception', loc, body: e?.responseSnippet || e?.message || '' });
      for (const symbol of missing) {
        logTradeAction('no_quote', symbol, { reason: e?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'BARS' });
      }
    }
  }
  return out;
}

/* ──────────────────────────────── 13) STOCKS DATA API ─────────────────────────────── */
async function stocksLatestQuotesBatch(symbols = []) {
  if (!symbols.length) return new Map();
  const csv = symbols.join(',');
  try {
    const url = buildBackendUrl({
      path: 'market/stocks/quotes',
      params: { symbols: csv },
      label: 'stocks_latest_quotes',
    });
    const j = await fetchMarketData('QUOTE', url, { headers: BACKEND_HEADERS });
    const out = new Map();
    for (const sym of symbols) {
      const qraw = j?.quotes?.[sym];
      const q = Array.isArray(qraw) ? qraw[0] : qraw;
      if (!q) {
        logTradeAction('no_quote', sym, { reason: 'no_data', requestType: 'QUOTE' });
        continue;
      }
      const bid = Number(q.bp ?? q.bid_price);
      const ask = Number(q.ap ?? q.ask_price);
      const bs = Number(q.bs ?? q.bid_size);
      const as = Number(q.as ?? q.ask_size);
      const tms = parseTsMs(q.t);
      if (bid > 0 && ask > 0) out.set(sym, { bid, ask, bs: Number.isFinite(bs) ? bs : null, as: Number.isFinite(as) ? as : null, tms });
    }
    return out;
  } catch (e) {
    logTradeAction('quote_http_error', 'QUOTE', { status: e?.statusCode || e?.code || 'exception', loc: 'stocks', body: e?.responseSnippet || e?.message || '' });
    for (const sym of symbols) {
      logTradeAction('no_quote', sym, { reason: e?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'QUOTE' });
    }
    return new Map();
  }
}

// --- Daily bars helpers (stocks & crypto) for simple benchmark returns ---
async function stocksDailyBars(symbols = [], limit = 10) {
  if (!symbols.length) return new Map();
  const csv = symbols.join(',');
  try {
    const url = buildBackendUrl({
      path: 'market/stocks/bars',
      params: {
        timeframe: '1Day',
        symbols: csv,
        limit: String(limit),
        adjustment: 'raw',
      },
      label: 'stocks_daily_bars',
    });
    const j = await fetchMarketData('BARS', url, { headers: BACKEND_HEADERS });
    const raw = j?.bars || {};
    const out = new Map();
    for (const sym of symbols) {
      const arr = raw[sym];
      if (Array.isArray(arr) && arr.length) {
        out.set(sym, arr.map(b => ({
          tms: parseTsMs(b.t),
          open: Number(b.o ?? b.open),
          high: Number(b.h ?? b.high),
          low:  Number(b.l ?? b.low),
          close:Number(b.c ?? b.close),
          vol:  Number(b.v ?? b.volume ?? 0),
        })).filter(x => Number.isFinite(x.close) && x.close > 0));
      }
    }
    return out;
  } catch (e) {
    logTradeAction('quote_http_error', 'BARS', { status: e?.statusCode || e?.code || 'exception', loc: 'stocks', body: e?.responseSnippet || e?.message || '' });
    for (const sym of symbols) {
      logTradeAction('no_quote', sym, { reason: e?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'BARS' });
    }
    return new Map();
  }
}

async function cryptoDailyBars(symbols = [], limit = 10) {
  const uniq = Array.from(new Set(symbols.map((s) => toInternalSymbol(s)).filter(Boolean)));
  if (!uniq.length) return new Map();
  const out = new Map();
  for (const loc of DATA_LOCATIONS) {
    try {
      const ds = uniq.map((s) => normalizePair(s)).join(',');
      const url = buildBackendUrl({
        path: 'market/crypto/bars',
        params: { timeframe: '1Day', limit: String(limit), symbols: ds, location: loc },
        label: 'crypto_daily_bars',
      });
      const j = await fetchMarketData('BARS', url, { headers: BACKEND_HEADERS });
      const raw = j?.bars || {};
      for (const sym of uniq) {
        const dataSymbol = normalizePair(sym);
        const arr = raw[dataSymbol];
        if (Array.isArray(arr) && arr.length) {
          out.set(sym, arr.map(b => ({
            tms: parseTsMs(b.t),
            open: Number(b.o ?? b.open),
            high: Number(b.h ?? b.high),
            low:  Number(b.l ?? b.low),
            close:Number(b.c ?? b.close),
            vol:  Number(b.v ?? b.volume ?? 0),
          })).filter(x => Number.isFinite(x.close) && x.close > 0));
        }
      }
      break; // success from this location
    } catch (e) {
      logTradeAction('quote_http_error', 'BARS', { status: e?.statusCode || e?.code || 'exception', loc, body: e?.responseSnippet || e?.message || '' });
      for (const sym of uniq) {
        logTradeAction('no_quote', sym, { reason: e?.code === 'COOLDOWN' ? 'cooldown' : 'request_failed', requestType: 'BARS' });
      }
    }
  }
  return out;
}

async function computeBenchReturns(days = 7) {
  // Use SPY for stocks proxy; BTCUSD for crypto proxy.
  const spyBarsMap = await stocksDailyBars(['SPY'], days + 2);
  const btcBarsMap = await cryptoDailyBars(['BTC/USD'], days + 2);
  const spy = (spyBarsMap.get('SPY') || []).slice(-(days + 1));
  const btc = (btcBarsMap.get('BTC/USD') || []).slice(-(days + 1));
  const pct = (arr) => (arr.length >= 2 && arr[0].close > 0)
    ? (arr[arr.length - 1].close / arr[0].close) - 1
    : 0;
  return {
    spyRet: pct(spy),
    btcRet: pct(btc),
    spySeries: spy,
    btcSeries: btc,
  };
}

async function stocksLatestTrade(symbol) { return null; }
async function stocksBars1m(symbols = [], limit = 6) { return new Map(); }

/* ───────────────────────────── 14) FEE / PNL MODEL ───────────────────────────── */
/**
 * Dynamic fee model: exit/TP calculations use the *actual* buy fee (maker/taker) observed on entry.
 */
function feeModelFor(symbol) {
  if (isStock(symbol)) {
    return {
      cls: 'equity',
      buyBps: 0,
      sellBps: EQUITY_SEC_FEE_BPS,
      tafPerShare: EQUITY_TAF_PER_SHARE,
      tafCap: EQUITY_TAF_CAP,
      commissionUSD: EQUITY_COMMISSION_PER_TRADE_USD,
      tick: 0.01,
    };
  }
  return {
    cls: 'crypto',
    buyBps: SETTINGS.feeBpsMaker,
    sellBps: SETTINGS.feeBpsTaker,
    tafPerShare: 0,
    tafCap: 0,
    commissionUSD: 0,
    tick: 1e-5,
  };
}
function perShareFixedOnSell(qty, model) {
  if (model.cls !== 'equity') return 0;
  const fixed = Math.min(model.tafPerShare * qty, model.tafCap) + model.commissionUSD;
  return fixed / Math.max(1, qty);
}

function midFromQuote(q) {
  return (q && Number.isFinite(q.bid) && Number.isFinite(q.ask)) ? 0.5 * (q.bid + q.ask) : null;
}

function stopBpsForSymbol(symbol) {
  // Use configured stopLossBps as the loss distance "a"
  return Math.max(1, SETTINGS.stopLossBps || 80);
}

/**
 * Very lightweight EV model (units = bps of entry).
 * p_up is nudged by short-term slope; if momentum filter is OFF, use mild tilt only.
 */
function expectedValueBps({ symbol, q, tpBps, buyBpsOverride }) {
  const m = midFromQuote(q) || q?.bid || 0;
  if (!(m > 0) || !(tpBps > 0)) return -1e9;

  const a = stopBpsForSymbol(symbol);           // loss distance in bps
  const b = tpBps;                               // gain distance in bps
  const slip = symStats[symbol]?.slipEwmaBps ?? (SETTINGS.slipBpsByRisk?.[SETTINGS.riskLevel] ?? 1);

  // Fees: use dynamic round-trip with actual planned sell side
  const fees = roundTripFeeBpsEstimateWithBuy(symbol, buyBpsOverride) + slip;

  // Tiny momentum tilt: use last two closes if available
  const closes = PRICE_HIST.get(symbol) || [];
  let tilt = 0;
  if (closes.length >= 2) {
    const v0 = closes[closes.length - 1] - closes[closes.length - 2];
    tilt = Number.isFinite(v0) ? Math.sign(v0) * 0.02 : 0; // ±2% tilt to p_up (very mild)
  }
  // Base gambler's-ruin p_up = a/(a+b) when drift=0; tilt around 0.5
  let p_up = 0.5 + tilt;
  p_up = Math.max(0.05, Math.min(0.95, p_up));

  // EV in bps (gross minus fees)
  const evGross = p_up * b - (1 - p_up) * a;
  const evNet = evGross - fees;
  return evNet;
}

// ---- dynamic-fee variants
function roundTripFeeBpsEstimateWithBuy(symbol, buyBpsOverride) {
  const m = feeModelFor(symbol);
  const buyBps = Number.isFinite(buyBpsOverride) ? buyBpsOverride : (m.buyBps || 0);
  const sellBps = isCrypto(symbol)
    ? (SETTINGS.takerExitOnTouch ? SETTINGS.feeBpsTaker : SETTINGS.feeBpsMaker)
    : (m.sellBps || 0);
  return buyBps + sellBps;
}
function dynamicMinProfitPerShare({ symbol, entryPx, buyBpsOverride }) {
  const feesBps = roundTripFeeBpsEstimateWithBuy(symbol, buyBpsOverride);
  const floorBps = Math.max(
    eff(symbol, 'dynamicMinProfitBps'),
    feesBps + eff(symbol, 'extraOverFeesBps')
  );
  return (floorBps / 10000) * entryPx;
}
function minExitPriceFeeAwareDynamic({ symbol, entryPx, qty, buyBpsOverride }) {
  const model = feeModelFor(symbol);
  const buyBps = Number.isFinite(buyBpsOverride) ? buyBpsOverride : (model.buyBps || 0);
  const buyFeePS = entryPx * (buyBps / 10000);
  const fixedSellPS = perShareFixedOnSell(qty, model);
  const sellBps = isCrypto(symbol)
    ? (SETTINGS.takerExitOnTouch ? SETTINGS.feeBpsTaker : SETTINGS.feeBpsMaker)
    : (model.sellBps || 0);
  const sellBpsFrac = sellBps / 10000;
  const minNetPerShare = dynamicMinProfitPerShare({ symbol, entryPx, buyBpsOverride });
  const raw = (entryPx + buyFeePS + fixedSellPS + minNetPerShare) / Math.max(1e-9, 1 - sellBpsFrac);
  return roundToTick(raw, model.tick);
}
function projectedNetPnlUSDWithBuy({ symbol, entryPx, qty, sellPx, buyBpsOverride }) {
  const m = feeModelFor(symbol);
  const buyBps = Number.isFinite(buyBpsOverride) ? buyBpsOverride : (m.buyBps || 0);
  const buyFeesUSD = qty * entryPx * (buyBps / 10000);
  const sellBps = isCrypto(symbol)
    ? (SETTINGS.takerExitOnTouch ? SETTINGS.feeBpsTaker : SETTINGS.feeBpsMaker)
    : (m.sellBps || 0);
  const sellFeesUSD =
    qty * sellPx * (sellBps / 10000) +
    (m.cls === 'equity'
      ? Math.min(m.tafPerShare * qty, m.tafCap) + m.commissionUSD
      : 0);
  return sellPx * qty - sellFeesUSD - entryPx * qty - buyFeesUSD;
}
function meetsMinProfitWithBuy({ symbol, entryPx, qty, sellPx, buyBpsOverride }) {
  if (!(entryPx > 0) || !(qty > 0) || !(sellPx > 0)) return false;
  const net = projectedNetPnlUSDWithBuy({ symbol, entryPx, qty, sellPx, buyBpsOverride });
  const targetUSD = dynamicMinProfitPerShare({ symbol, entryPx, buyBpsOverride }) * qty;
  const feeFloor  = minExitPriceFeeAwareDynamic({ symbol, entryPx, qty, buyBpsOverride });
  return net >= targetUSD && sellPx >= feeFloor * (1 - 1e-6);
}

// ---- legacy wrappers (compat)
function roundTripFeeBpsEstimate(symbol) {
  return roundTripFeeBpsEstimateWithBuy(symbol, undefined);
}
function minExitPriceFeeAware({ symbol, entryPx, qty }) {
  return minExitPriceFeeAwareDynamic({ symbol, entryPx, qty, buyBpsOverride: undefined });
}
function projectedNetPnlUSD({ symbol, entryPx, qty, sellPx }) {
  return projectedNetPnlUSDWithBuy({ symbol, entryPx, qty, sellPx, buyBpsOverride: undefined });
}
function meetsMinProfit({ symbol, entryPx, qty, sellPx }) {
  return meetsMinProfitWithBuy({ symbol, entryPx, qty, sellPx, buyBpsOverride: undefined });
}

/* ──────────────────────────────── 15) LOGGING ──────────────────────────────── */
let logSubscriber = null, logBuffer = [];
const MAX_LOGS = 5000;
const RISK_LEVELS = ['🐢','🐇','🦊','🦺','🦁'];

function fmtSkipDetail(reason, d = {}) {
  try {
    switch (reason) {
      case 'no_quote': {
        return ' (fresh=NA)';
      }
      case 'stale_quote': {
        if (Number.isFinite(d.ageSec)) return ` (age=${formatLoggedAgeSeconds(d.ageSec)}s)`;
        if (Number.isFinite(d.lastSeenAgeSec)) {
          return ` (last_seen_age=${formatLoggedAgeSeconds(d.lastSeenAgeSec)}s)`;
        }
        return ' (age=NA)';
      }
      case 'spread': {
        const sb = Number(d.spreadBps)?.toFixed?.(1);
        return ` (spread ${sb}bps > max ${d.max}bps)`;
      }
      case 'spread_fee_gate': {
        const sb = Number(d.spreadBps)?.toFixed?.(1);
        const need = Number((d.feeBps || 0) + SETTINGS.spreadOverFeesMinBps).toFixed(1);
        return ` (spread ${sb}bps < fee+guard ${need}bps)`;
      }
      case 'tiny_price': {
        const m = Number(d.mid)?.toPrecision?.(4);
        return ` (mid≈${m} < min ${SETTINGS.minPriceUsd})`;
      }
      case 'illiquid': {
        return ` (bidSize ${d.bs} < ${d.min})`;
      }
      case 'edge_negative': {
        const bps = Number(d.tpBps)?.toFixed?.(1);
        return ` (need≥${bps}bps)`;
      }
      case 'nomomo': {
        const v0 = Number(d.v0);
        const slope = Number((d.emaLast ?? 0) - (d.emaPrev ?? 0));
        const sStr = Number.isFinite(slope) ? slope.toPrecision(3) : 'n/a';
        return ` (v0=${Number.isFinite(v0)?v0.toPrecision(3):'n/a'}, slope=${sStr})`;
      }
      case 'blacklist': return ' (blacklisted)';
      case 'market_closed': return ' (market closed)';
      default: return '';
    }
  } catch { return ''; }
}

export const registerLogSubscriber = (fn) => { logSubscriber = fn; };

const lastLogTimestamps = new Map();

const logTradeAction = async (type, symbol, details = {}) => {
  const now = Date.now();
  if (type === 'quote_ok') {
    const batchId = details?.batchId ?? details?.batch ?? 'na';
    const key = `${symbol || ''}|${type}|${batchId}`;
    const last = lastLogTimestamps.get(key) || 0;
    if (now - last < 250) return;
    lastLogTimestamps.set(key, now);
  }
  const timestamp = new Date(now).toISOString();
  const entry = { timestamp, type, symbol, ...details };
  logBuffer.push(entry);
  if (logBuffer.length > MAX_LOGS) logBuffer.shift();
  if (typeof logSubscriber === 'function') {
    try { logSubscriber(entry); } catch {}
  }
};
const FRIENDLY = {
  quote_ok: { sev: 'info', msg: (d) => `Quote OK (${(d.spreadBps ?? 0).toFixed(1)} bps)` },
  ev_debug: { sev: 'info', msg: (d) => `EV debug (ev=${d.evBps ?? 'n/a'}bps, tp=${d.tpBps ?? 'n/a'}bps)` },
  quote_stale: { sev: 'warn', msg: (d) => `Quote STALE (${(d.spreadBps ?? 0).toFixed(1)} bps)` },
  quote_http_error: { sev: 'warn', msg: (d) => `Alpaca ${d.symbol || 'quotes'} ${d.status}${d.loc ? ' • ' + d.loc : ''}${d.body ? ' • ' + d.body : ''}` },
  quote_exception:  { sev: 'error', msg: (d) => `Quote/Order exception: ${d?.error ?? ''}` },
  trade_http_error: { sev: 'warn', msg: (d) => `Alpaca ${d.symbol || 'trades'} ${d.status}${d.loc ? ' • ' + d.loc : ''}${d.body ? ' • ' + d.body : ''}` },
  unsupported_symbol: { sev: 'warn', msg: (d) => `Unsupported symbol: ${d.sym}` },
  buy_camped: { sev: 'info', msg: (d) => `Camping bid @ ${d.limit}` },
  buy_replaced: { sev: 'info', msg: (d) => `Replaced bid → ${d.limit}` },
  buy_success: { sev: 'success', msg: (d) => `BUY filled qty ${d.qty} @≤${d.limit}` },
  buy_unfilled_canceled: { sev: 'warn', msg: () => `BUY unfilled — canceled bid` },
  tp_limit_set: { sev: 'success', msg: (d) => `TP set @ ${d.limit}` },
  taker_force_flip: { sev: 'warn', msg: (d) => `TAKER force flip @~${d?.limit ?? ''}` },
  tp_limit_error: { sev: 'error', msg: (d) => `TP set error: ${d.error}` },
  scan_start: { sev: 'info', msg: (d) => `Scan start (batch ${d.batch})` },
  scan_summary: {
    sev: 'info',
    msg: (d) =>
      `Scan: ready ${d.readyCount} / attempts_sent ${d.attemptsSent} / attempts_failed ${d.attemptsFailed} / open ${d.ordersOpen} / fills ${d.fillsCount}`,
  },
  scan_error: { sev: 'error', msg: (d) => `Scan error: ${d.error}` },
  skip_wide_spread: { sev: 'warn', msg: (d) => `Skip: spread ${d.spreadBps} bps > max` },
  skip_small_order: {
    sev: 'warn',
    msg: (d) => {
      const reason = d?.reason || 'below_min_notional';
      if (reason === 'insufficient_funding') {
        const bp = Number.isFinite(d?.availableUsd) ? d.availableUsd.toFixed(2)
          : Number.isFinite(d?.buyingPower) ? d.buyingPower.toFixed(2)
          : d?.buyingPower;
        const req = Number.isFinite(d?.requiredNotional) ? d.requiredNotional.toFixed(2) : d?.requiredNotional;
        const reserve = Number.isFinite(d?.reserve) ? d.reserve.toFixed(2) : d?.reserve;
        const openHold = Number.isFinite(d?.openOrderHold) ? d.openOrderHold.toFixed(2) : d?.openOrderHold;
        const cash = Number.isFinite(d?.cash) ? d.cash.toFixed(2) : d?.cash;
        const rawBp = Number.isFinite(d?.buying_power) ? d.buying_power.toFixed(2) : d?.buying_power;
        const nmbp = Number.isFinite(d?.non_marginable_buying_power) ? d.non_marginable_buying_power.toFixed(2) : d?.non_marginable_buying_power;
        return `Skip: insufficient_funding (availableUsd=${bp}, requiredNotional=${req}, cash=${cash}, buying_power=${rawBp}, non_marginable_buying_power=${nmbp}` +
          `${reserve != null ? `, reserve=${reserve}` : ''}${openHold != null ? `, openOrderHold=${openHold}` : ''})`;
      }
      if (reason === 'below_min_notional') {
        const comp = Number.isFinite(d?.computedNotional) ? d.computedNotional.toFixed(2) : d?.computedNotional;
        const min = Number.isFinite(d?.minNotional) ? d.minNotional.toFixed(2) : d?.minNotional;
        return `Skip: below_min_notional (computedNotional=${comp}, minNotional=${min})`;
      }
      return `Skip: ${reason}`;
    },
  },
  bump_qty_to_min_notional: {
    sev: 'info',
    msg: (d) =>
      `BUMP_QTY_TO_MIN_NOTIONAL oldQty=${d.oldQty} newQty=${d.newQty} oldNotional=${d.oldNotional} newNotional=${d.newNotional}`,
  },
  entry_skipped: { sev: 'info', msg: (d) => `Skip — ${d.reason}${fmtSkipDetail(d.reason, d)}` },
  risk_changed: {
    sev: 'info',
    msg: (d) => `SETTINGS — Risk→${d.level} (source=${d.source || 'UI'}, reason=${d.reason || 'manual'})`,
  },
  concurrency_guard: { sev: 'warn', msg: (d) => `Concurrency guard: cap ${d.cap} @ avg ${d.avg?.toFixed?.(1) ?? d.avg} bps` },
  skip_blacklist: { sev: 'warn', msg: () => `Skip: blacklisted` },
  coarse_tick_skip: { sev: 'warn', msg: () => `Skip: coarse-tick/sub-$0.05` },
  dust_flattened: { sev: 'info', msg: (d) => `Dust flattened (${d.usd?.toFixed?.(2) ?? d.usd} USD)` },
  tp_touch_tick: { sev: 'info', msg: (d) => `Touch tick ${d.count}/${SETTINGS.touchTicksRequired} @bid≈${d.bid?.toFixed?.(5) ?? d.bid}` },
  tp_fee_floor: { sev: 'info', msg: (d) => `FeeGuard raised TP → ${d.limit}` },
  taker_blocked_fee: { sev: 'warn', msg: () => `Blocked taker exit (profit floor unmet)` },
  stop_arm: { sev: 'info', msg: (d) => `Stop armed @ ${d.stopPx.toFixed?.(5) ?? d.stopPx}${d.hard ? ' (HARD)' : ''}` },
  stop_update: { sev: 'info', msg: (d) => `Stop update → ${d.stopPx.toFixed?.(5) ?? d.stopPx}` },
  stop_exit: { sev: 'warn', msg: (d) => `STOP EXIT @~${d.atPx?.toFixed?.(5) ?? d.atPx}` },
  trail_start: { sev: 'info', msg: (d) => `Trail start ≥ ${d.startPx.toFixed?.(5) ?? d.startPx}` },
  trail_peak: { sev: 'info', msg: (d) => `Trail peak → ${d.peakPx.toFixed?.(5) ?? d.peakPx}` },
  trail_exit: { sev: 'success', msg: (d) => `TRAIL EXIT @~${d.atPx?.toFixed?.(5) ?? d.atPx}` },
  daily_halt: { sev: 'error', msg: (d) => `TRADING HALTED — ${d.reason}` },
  pdt_guard: { sev: 'warn', msg: (d) => `PDT guard: ${d.reason || 'equity_scan_disabled'} (eq=${d.eq ?? '?'}, trades=${d.dt ?? '?'})` },
  health_ok: { sev: 'success', msg: (d) => `Health OK (${d.section})` },
  health_warn: { sev: 'warn', msg: (d) => `Health WARN (${d.section}) — ${d.note || ''}` },
  health_err: { sev: 'error', msg: (d) => `Health ERROR (${d.section}) — ${d.note || ''}` },
};
const GATE_HELP = {
  spread: {
    icon: '🪟',
    title: 'Spread too wide',
    expl: (e) => {
      const b = Number(e?.spreadBps)?.toFixed?.(1);
      const cap = Number(e?.max ?? SETTINGS.spreadMaxBps);
      return `Measured spread = ${b ?? '?'} bps, cap = ${cap} bps. Widen the cap to allow more assets in, or keep it tighter to avoid slippage.`;
    },
    knobs: [
      { key: 'spreadMaxBps', deltas: [-5, +5, +10], min: 3, max: 300, label: 'Spread cap (bps)' },
    ],
  },
  spread_fee_gate: {
    icon: '💸',
    title: 'Spread not high enough over fees',
    expl: (e) => {
      const b = Number(e?.spreadBps)?.toFixed?.(1);
      const fee = Number(e?.feeBps)?.toFixed?.(1);
      const guard = SETTINGS.spreadOverFeesMinBps;
      return `Spread = ${b ?? '?'} bps, fees ≈ ${fee ?? '?'} bps. We require spread ≥ fees + ${guard} bps. Lower the guard to trigger more entries.`;
    },
    knobs: [
      { key: 'spreadOverFeesMinBps', deltas: [-1, -2, +1], min: 0, max: 50, label: 'Over‑fees guard (bps)' },
    ],
  },
  tiny_price: {
    icon: '🪙',
    title: 'Price too small',
    expl: (e) => {
      const mid = Number(e?.mid)?.toPrecision?.(4);
      return `Mid ≈ ${mid ?? '?'} < min ${SETTINGS.minPriceUsd}. Lower the min price to include micro‑priced coins.`;
    },
    knobs: [
      { key: 'minPriceUsd', deltas: [-0.0005, +0.0005], min: 0, max: 10, label: 'Min tradable price (USD)' },
    ],
  },
  nomomo: {
    icon: '📉',
    title: 'Momentum filter blocked entry',
    expl: (e) => {
      const v0 = Number(e?.v0)?.toPrecision?.(3);
      return `Short‑term momentum did not pass. You can relax or disable the momentum gate.`;
    },
    knobs: [
      { key: 'enforceMomentum', toggle: true, label: 'Require momentum' },
    ],
  },
  edge_negative: {
    icon: '🎯',
    title: 'Required profit not met',
    expl: (e) => {
      const need = Number(e?.tpBps)?.toFixed?.(1);
      return `Target edge ≈ ${need ?? '?'} bps not achievable at the moment. Lower your profit floors to enter more often.`;
    },
    knobs: [
      { key: 'dynamicMinProfitBps', deltas: [-5, -10, +5], min: 0, max: 500, label: 'Dynamic floor (bps)' },
      { key: 'netMinProfitBps', deltas: [-0.5, +0.5], min: 0, max: 100, label: 'Absolute floor (bps)' },
    ],
  },
  no_quote: {
    icon: '📭',
    title: 'No fresh quote',
    expl: () => `No recent bid/ask was available. Allow trade fallback or relax freshness.`,
    knobs: [
      { key: 'liveRequireQuote', toggle: true, label: 'Require live quote to enter' },
      { key: 'liveFreshMsCrypto', deltas: [+1000, -1000], min: 1000, max: 120000, label: 'Quote freshness (ms)' },
      { key: 'quoteTtlMs', deltas: [+500, -500], min: 0, max: 120000, label: 'Quote cache TTL (ms)' },
    ],
  },
  taker_blocked_fee: {
    icon: '🏁',
    title: 'Blocked taker exit (profit floor)',
    expl: () => `We touched the target but exit was blocked by the fee/guard check. Switch guard or relax profit floors.`,
    knobs: [
      { key: 'takerExitGuard', cycle: ['fee','min'], label: 'Taker exit guard' },
      { key: 'dynamicMinProfitBps', deltas: [-5, +5], min: 0, max: 500, label: 'Dynamic floor (bps)' },
    ],
  },
  concurrency_guard: {
    icon: '🚦',
    title: 'Too many concurrent positions',
    expl: (e) => {
      const cap = e?.cap ?? SETTINGS.maxConcurrentPositions;
      return `Open positions reached cap = ${cap}. Increase it to allow more entries.`;
    },
    knobs: [
      { key: 'maxConcurrentPositions', deltas: [+1, +2], min: 1, max: 50, label: 'Max concurrent' },
    ],
  },
  held: {
    icon: '🔁',
    title: 'Already holding this symbol',
    expl: () => `We skip new entries when already holding. (No setting to change; it’s a safety.)`,
    knobs: [],
  },
  blacklist: {
    icon: '⛔',
    title: 'Symbol is blacklisted',
    expl: () => `This asset is blacklisted in code (see BLACKLIST). Remove it to allow entries.`,
    knobs: [],
  },
};
function friendlyLog(entry) {
  const meta = FRIENDLY[entry.type];
  if (!meta)
    return { sev: 'info', text: `${entry.type}${entry.symbol ? ' ' + entry.symbol : ''}`, hint: null };
  const text = typeof meta.msg === 'function' ? meta.msg(entry) : meta.msg;
  return { sev: meta.sev, text: `${entry.symbol ? entry.symbol + ' — ' : ''}${text}`, hint: null };
}

/* ─────────────────────────── 16) QUOTES / BATCHING (LIVE) ─────────────────────────── */
const PRICE_HIST = new Map();
function pushPriceHist(sym, mid, max = 6) {
  if (!Number.isFinite(mid)) return;
  const arr = PRICE_HIST.get(sym) || [];
  arr.push(mid);
  if (arr.length > max) arr.shift();
  PRICE_HIST.set(sym, arr);
}

async function getQuotesBatch(symbols) {
  const normalizedSymbols = symbols.map((s) => toInternalSymbol(s)).filter(Boolean);
  const cryptos = normalizedSymbols.filter((s) => isCrypto(s));
  const stocks = normalizedSymbols.filter((s) => isStock(s));
  const out = new Map();
  const now = Date.now();

  if (cryptos.length) {
    const internalSymbols = Array.from(new Set(cryptos)).filter((sym) => !isUnsupportedLocal(sym));
    const missing = [];
    const qmap = await getCryptoQuotesBatch(internalSymbols);
    for (const symbol of internalSymbols) {
      const q = qmap.get(symbol);
      if (!q) {
        missing.push(symbol);
        quoteCache.delete(symbol);
        continue;
      }
      if (isStaleQuoteEntry(q)) {
        missing.push(symbol);
        logStaleQuote(symbol, q, { reason: 'stale_quote' });
        quoteCache.delete(symbol);
        continue;
      }
      out.set(symbol, q);
      lastQuoteBatchMissing.delete(symbol);
    }
    if (missing.length) {
      for (const symbol of missing) {
        lastQuoteBatchMissing.set(symbol, now);
      }
    }
    const gotCount = internalSymbols.length - missing.length;
    console.log(`QUOTE_BATCH got=${gotCount} missing=${missing.length} missingSymbols=${JSON.stringify(missing)}`);
  }

  if (stocks.length) {
    // no-op (crypto-only build keeps structure intact)
  }
  return out;
}

/* ─────────────────────────────── 17) SMART QUOTE ─────────────────────────────── */
async function getQuoteSmart(symbol, preloadedMap = null) {
  try {
    const normalizedSymbol = toInternalSymbol(symbol);
    // This build is crypto-first. Do not hit crypto data API with equities.
    if (isStock(normalizedSymbol)) { markUnsupported(normalizedSymbol, 60); return null; }
    if (isUnsupportedLocal(normalizedSymbol)) return null;

    // Always use cached real quotes if within staleness window
    {
      const c = quoteCache.get(normalizedSymbol);
      if (c && !isStaleQuoteEntry(c)) return c;
      if (c && isStaleQuoteEntry(c)) {
        logStaleQuote(normalizedSymbol, c, { reason: 'stale_quote' });
        quoteCache.delete(normalizedSymbol);
      }
    }

    // Use preloaded map if available and fresh
    if (preloadedMap && preloadedMap.has(normalizedSymbol)) {
      const q = preloadedMap.get(normalizedSymbol);
      if (q && !isStaleQuoteEntry(q)) {
        quoteCache.set(normalizedSymbol, q);
        return q;
      }
      if (q && isStaleQuoteEntry(q)) {
        logStaleQuote(normalizedSymbol, q, { reason: 'stale_quote' });
        quoteCache.delete(normalizedSymbol);
      }
    }

    // Try to fetch a real quote
    const m = await getCryptoQuotesBatch([normalizedSymbol]);
    const q0 = m.get(normalizedSymbol);
    if (q0 && !isStaleQuoteEntry(q0)) {
      quoteCache.set(normalizedSymbol, q0);
      return q0;
    }
    if (q0 && isStaleQuoteEntry(q0)) {
      logStaleQuote(normalizedSymbol, q0, { reason: 'stale_quote' });
      quoteCache.delete(normalizedSymbol);
    }
    if (!q0) {
      quoteCache.delete(normalizedSymbol);
      lastQuoteBatchMissing.set(normalizedSymbol, Date.now());
    }

    // Fallback: synthesize a quote from last trade if allowed
    if (!SETTINGS.liveRequireQuote) {
      const tm = await getCryptoTradesBatch([normalizedSymbol]);
      const t = tm.get(normalizedSymbol);
      if (t && isFresh(t.tms, SETTINGS.liveFreshTradeMsCrypto)) {
        const synth = synthQuoteFromTrade(t.price, SETTINGS.syntheticTradeSpreadBps);
        if (synth) {
          const receivedAtMs = Date.now();
          const quote = {
            bid: synth.bid,
            ask: synth.ask,
            bs: null,
            as: null,
            mid: synth.bid && synth.ask ? 0.5 * (synth.bid + synth.ask) : synth.bid,
            spreadBps: 0,
            tsMs: Number.isFinite(synth.tms) ? synth.tms : 0,
            receivedAtMs,
            source: 'trade_fallback',
            lastError: null,
          };
          quoteCache.set(normalizedSymbol, quote);
          return quote;
        }
      }
    }
    return null;
  } catch {
    return null;
  }
}

/* ─────────────────────────────── 18) SIGNAL / ENTRY MATH ─────────────────────────────── */
const SPREAD_EPS_BPS = 0.3;
/* ───────── 18a) VOL/EV HELPERS (no deps) ───────── */

// EWMA realized volatility of 1-minute log returns; returns {sigma, sigmaBps}
function ewmaSigmaFromCloses(closes = [], halfLifeMin = 10) {
  const n = closes.length;
  if (n < 3) return { sigma: 0, sigmaBps: 0 };
  const rets = [];
  for (let i = 1; i < n; i++) {
    const c0 = closes[i - 1], c1 = closes[i];
    if (!(c0 > 0 && c1 > 0)) continue;
    rets.push(Math.log(c1 / c0));
  }
  if (!rets.length) return { sigma: 0, sigmaBps: 0 };
  const alpha = 1 - Math.pow(2, -1 / Math.max(1, halfLifeMin));
  let v = 0;
  for (const r of rets) v = (1 - alpha) * v + alpha * r * r;
  const sigma = Math.sqrt(v);                // per minute
  const sigmaBps = 10000 * sigma;            // bps per minute
  return { sigma, sigmaBps };
}

// Microprice + imbalance from a quote with sizes; returns {micro, imbalance, microDrift}
function microMetrics(q) {
  const bid = +q?.bid, ask = +q?.ask, bs = +q?.bs || 0, as = +q?.as || 0;
  if (!(bid > 0 && ask > 0) || !(bs > 0 && as > 0)) {
    const mid = (bid > 0 && ask > 0) ? 0.5 * (bid + ask) : NaN;
    return { micro: mid, imbalance: 0, microDrift: 0 };
  }
  const micro = (ask * bs + bid * as) / (bs + as);
  const mid = 0.5 * (bid + ask);
  const imbalance = (bs - as) / (bs + as);
  const microDrift = micro - mid; // >0 → pressure up
  return { micro, imbalance, microDrift };
}

// Driftless Brownian barrier touch probability: P(hit +b before -a) = a/(a+b)
function barrierPTouchUpDriftless(aPrice, bPrice) {
  if (!(aPrice > 0 && bPrice > 0)) return 0.5;
  return aPrice / (aPrice + bPrice);
}

// Expected value per share using pUp, distances (price), fees/slippage; returns EV per share (USD)
function expectedValuePerShare({ pUp, aPrice, bPrice, entryPx, sellFeeBps, buyFeeBps, slippageBps }) {
  const buyFeePS  = entryPx * (Math.max(0, buyFeeBps)  / 10000);
  const sellFeePS = (entryPx + bPrice) * (Math.max(0, sellFeeBps) / 10000);
  const slipPS    = entryPx * (Math.max(0, slippageBps) / 10000);
  return pUp * (bPrice - sellFeePS) - (1 - pUp) * (aPrice + buyFeePS) - slipPS;
}

// Dynamic SELL epsilon (bps) ≥ max( sellEpsVolFrac*σ_bps, sellEpsMinTicks*tick_bps, 0.2 )
function dynamicSellEpsBps({ symbol, price, tick, sigmaBps, settings }) {
  const tickBps = (tick > 0 && price > 0) ? (tick / price) * 10000 : 0.02;
  const vPart = (settings.sellEpsVolFrac || 0.15) * (sigmaBps || 0);
  const tPart = (settings.sellEpsMinTicks || 2) * tickBps;
  return Math.max(0.2, vPart, tPart);
}

// Crude maker fill probability estimate using last bar volume as intensity proxy
// Qahead (units) ~ q.bs, lambdaSell ≈ 0.5 * volPerSec (half of trades hit bid)
function makerFillProb({ bsUnits, lastBarVolUnits, campSec }) {
  const Qahead = Math.max(1e-9, bsUnits || 0);
  const volPerSec = Math.max(0, (lastBarVolUnits || 0) / 60);
  const lambdaHit = 0.5 * volPerSec; // assume ~half of prints are sell-initiated
  const q = 1 - Math.exp(- (lambdaHit * Math.max(0, campSec || 0)) / Qahead);
  return clamp(q, 0, 1);
}

// Robust runway using median MFE observed in symStats (fallback to Brownian expectation)
function robustRunwayUSD(sym, entryPx, sigma, horizonMin, stats) {
  const s = stats[sym] || {};
  const hist = Array.isArray(s.mfeHist) ? s.mfeHist.slice(-60) : [];
  if (hist.length >= 6) {
    const sorted = hist.slice().sort((a, b) => a - b);
    const med = sorted[Math.floor(sorted.length / 2)];
    return Math.max(0, med);
  }
  // Brownian expected max excursion: E[max X_t] ≈ σ * sqrt(2H/π)
  const H = Math.max(1, horizonMin);
  const emx = sigma * Math.sqrt((2 * H) / Math.PI);
  return Math.max(0, emx * entryPx); // σ is in log space per-minute; scale by price
}

// Exit floor uses the intended exit liquidity:
// - If we plan to flip to taker on touch, assume taker fees on sell.
// - Otherwise assume maker fees on sell (resting limit TP).
const exitFloorBps = (symbol) => {
  if (isStock(symbol)) return 1.0;
  const sellBps = SETTINGS.takerExitOnTouch ? SETTINGS.feeBpsTaker : SETTINGS.feeBpsMaker;
  return SETTINGS.feeBpsMaker + sellBps; // buy (maker) + planned sell
};
function requiredProfitBpsForSymbol(symbol, riskLevel) {
  const arr = SETTINGS.slipBpsByRisk || [];
  const slip = Number.isFinite(arr[riskLevel]) ? arr[riskLevel] : (arr[0] ?? 1);
  const base = exitFloorBps(symbol) + 0.5 + slip;
  const dynamicFloor = Math.max(
    eff(symbol, 'dynamicMinProfitBps'),
    roundTripFeeBpsEstimate(symbol) + eff(symbol, 'extraOverFeesBps')
  );
  return Math.max(base, dynamicFloor);
}

/* ───────────────────────────── 19) ACCOUNT / ORDERS ───────────────────────────── */
const logOrderPayload = (context, order) => {
  if (!order) return;
  const symbol = toInternalSymbol(order.symbol);
  const notional = order.notional ?? 'NA';
  const qty = order.qty ?? 'NA';
  const limit = order.limit_price ?? 'NA';
  console.log(
    `ORDER_SUBMIT symbol=${symbol} side=${order.side} type=${order.type} tif=${order.time_in_force} notional=${notional} qty=${qty} limit=${limit}`
  );
};

const logOrderResponse = (context, order, res, data) => {
  const ok = Boolean(res?.ok && data?.ok && data?.orderId);
  const symbol = toInternalSymbol(order?.symbol);
  if (ok) {
    console.log(`ORDER_OK id=${data.orderId} status=${data.status || 'accepted'} symbol=${symbol}`);
    return { ok: true, orderId: data.orderId, status: data.status || 'accepted', err: 'NA' };
  }
  const httpStatus = res?.status ?? 'NA';
  const code = data?.error?.code ?? data?.error?.status ?? data?.code ?? 'NA';
  const message = data?.error?.message || data?.error || data?.message || 'unknown_error';
  const body = data?.raw || data?.error?.raw || JSON.stringify(data || {});
  console.warn(`ORDER_FAIL http=${httpStatus} code=${code} message=${message} body=${body}`);
  return { ok: false, orderId: 'NA', status: 'NA', err: message };
};

const logOrderError = (context, order, error = null, res = null, data = null) => {
  const httpStatus = res?.status ?? 'NA';
  const code = error?.code || error?.status || 'NA';
  const message = data?.message || data?.error || error?.message || error?.name || 'unknown_error';
  const body = data?.raw || error?.message || '';
  console.warn(`ORDER_FAIL http=${httpStatus} code=${code} message=${message} body=${body}`);
};

const orderFailureEvents = [];
const ORDER_FAILURE_WINDOW_MS = 60000;
const categorizeOrderFailure = ({ status, error }) => {
  if (status === 401) return '401';
  if (status === 403) return '403';
  if (status === 422) return '422';
  if (status === 429) return '429';
  if (Number.isFinite(status) && status >= 500) return '5xx';
  const msg = error?.message || '';
  if (error?.name === 'TypeError' || msg.includes('Network') || msg.includes('fetch')) return 'network';
  return 'unknown';
};
const recordOrderFailure = (code) => {
  const now = Date.now();
  orderFailureEvents.push({ code, ts: now });
  const cutoff = now - ORDER_FAILURE_WINDOW_MS;
  while (orderFailureEvents.length && orderFailureEvents[0].ts < cutoff) {
    orderFailureEvents.shift();
  }
  const summary = { 401: 0, 403: 0, 422: 0, 429: 0, '5xx': 0, network: 0, unknown: 0 };
  for (const ev of orderFailureEvents) {
    if (summary[ev.code] == null) summary[ev.code] = 0;
    summary[ev.code] += 1;
  }
  console.warn('order_failures_60s', { failures_by_code: summary });
};
const logOrderFailure = ({ order, endpoint, status, body, error }) => {
  const code = categorizeOrderFailure({ status, error });
  const payload = {
    symbol: toInternalSymbol(order?.symbol),
    side: order?.side,
    notional: order?.notional ?? null,
    qty: order?.qty ?? null,
    order_type: order?.type,
    limit_price: order?.limit_price ?? null,
    endpoint,
    status,
    response_body: body ?? null,
    error_code: code,
  };
  console.warn('order_submit_failed', payload);
  recordOrderFailure(code);
};

const RECENT_ORDER_TTL_MS = 10 * 60 * 1000;
const recentOrders = new Map();

const recordRecentOrder = ({ id, symbol, status, submittedAt }) => {
  if (!id) return;
  const now = Date.now();
  const normalizedStatus = String(status || '').toLowerCase();
  recentOrders.set(id, {
    id,
    symbol: toInternalSymbol(symbol),
    status: normalizedStatus || null,
    submittedAt: submittedAt || new Date(now).toISOString(),
    lastSeenAt: now,
    countedFill: false,
    countedReject: false,
  });
};

const pruneRecentOrders = () => {
  const cutoff = Date.now() - RECENT_ORDER_TTL_MS;
  for (const [id, order] of recentOrders.entries()) {
    const ts = order?.lastSeenAt || Date.parse(order?.submittedAt || 0) || 0;
    if (ts < cutoff) {
      recentOrders.delete(id);
    }
  }
};

const getPositionInfo = async (symbol) => {
  try {
    const res = await f(`${BACKEND_BASE_URL}/positions/${encodeURIComponent(canonicalAsset(symbol))}`, { headers: BACKEND_HEADERS });
    if (!res.ok) return null;
    const info = await res.json();
    const qty = parseFloat(info.qty ?? '0');
    const available = parseFloat(info.qty_available ?? info.available ?? info.qty ?? '0');
    const marketValue = parseFloat(info.market_value ?? info.marketValue ?? 'NaN');
    const markFromMV = Number.isFinite(marketValue) && qty > 0 ? marketValue / qty : NaN;
    const markFallback = parseFloat(info.current_price ?? info.asset_current_price ?? 'NaN');
    const mark = Number.isFinite(markFromMV) ? markFromMV : Number.isFinite(markFallback) ? markFallback : NaN;
    const basis = parseFloat(info.avg_entry_price ?? 'NaN');
    return {
      qty: +(qty || 0),
      available: +(available || 0),
      basis: Number.isFinite(basis) ? basis : null,
      mark: Number.isFinite(mark) ? mark : null,
      marketValue: Number.isFinite(marketValue) ? marketValue : 0,
    };
  } catch { return null; }
};
const getAllPositions = async () => {
  try {
    const r = await f(`${BACKEND_BASE_URL}/positions`, { headers: BACKEND_HEADERS });
    if (!r.ok) return [];
    const arr = await r.json();
    return Array.isArray(arr)
      ? arr.map((pos) => ({
        ...pos,
        rawSymbol: pos.rawSymbol ?? pos.symbol,
        pairSymbol: normalizePair(pos.rawSymbol ?? pos.symbol),
        symbol: normalizePair(pos.rawSymbol ?? pos.symbol),
      }))
      : [];
  } catch { return []; }
};
const getOrdersByStatus = async (status = 'open') => {
  try {
    const r = await f(`${BACKEND_BASE_URL}/orders?status=${encodeURIComponent(status)}&nested=true&limit=100`, { headers: BACKEND_HEADERS });
    if (!r.ok) return [];
    const arr = await r.json();
    return Array.isArray(arr)
      ? arr.map((order) => ({
        ...order,
        rawSymbol: order.rawSymbol ?? order.symbol,
        pairSymbol: normalizePair(order.rawSymbol ?? order.symbol),
        symbol: normalizePair(order.rawSymbol ?? order.symbol),
      }))
      : [];
  } catch { return []; }
};
const getOpenOrders = async () => getOrdersByStatus('open');
let __openOrdersCache = { ts: 0, items: [] };
async function getOpenOrdersCached(ttlMs = 2000) {
  const now = Date.now();
  if (now - __openOrdersCache.ts < ttlMs) return __openOrdersCache.items.slice();
  const items = await getOpenOrders();
  __openOrdersCache = { ts: now, items };
  return items.slice();
}

const logOrderState = (order) => {
  if (!order?.id) return;
  const status = String(order.status || order.order_status || '').toLowerCase();
  const filledQty = Number(order.filled_qty ?? order.filledQty ?? 0);
  const qty = Number(order.qty ?? order.quantity ?? 0);
  const remaining = Number.isFinite(qty) ? Math.max(0, qty - filledQty) : null;
  const reason =
    status === 'rejected' || status === 'canceled' || status === 'expired'
      ? (order.reject_reason || order.rejectReason || order.cancel_reason || order.canceled_reason || order.reason || '')
      : '';
  const reasonText = reason ? ` reason=${reason}` : '';
  console.log(`ORDER_STATE id=${order.id} status=${status || 'unknown'} filled_qty=${filledQty} remaining=${remaining}${reasonText}`);
};

async function pollRecentOrderStates() {
  pruneRecentOrders();
  if (!recentOrders.size) return { ordersOpen: 0, fillsCount: 0 };
  const openOrders = await getOrdersByStatus('open');
  const openMap = new Map((openOrders || []).map((o) => [o.id, o]));
  let ordersOpen = 0;
  let fillsCount = 0;

  let allMap = null;
  const recentIds = Array.from(recentOrders.keys());
  if (recentIds.length) {
    const allOrders = await getOrdersByStatus('all');
    allMap = new Map((allOrders || []).map((o) => [o.id, o]));
  }

  for (const [id, order] of recentOrders.entries()) {
    const openOrder = openMap.get(id);
    const resolvedOrder = openOrder || (allMap && allMap.get(id)) || null;
    const status = String(resolvedOrder?.status || resolvedOrder?.order_status || order.status || '').toLowerCase();
    const filledQty = Number(resolvedOrder?.filled_qty ?? resolvedOrder?.filledQty ?? 0);

    if (openOrder) {
      ordersOpen += 1;
      logOrderState(openOrder);
      order.lastSeenAt = Date.now();
    }

    if (resolvedOrder && !openOrder) {
      logOrderState(resolvedOrder);
      order.lastSeenAt = Date.now();
    }

    if ((filledQty > 0 || status === 'filled') && !order.countedFill) {
      fillsCount += 1;
      order.countedFill = true;
    }

    if (status === 'rejected' && !order.countedReject) {
      order.countedReject = true;
    }
  }
  return { ordersOpen, fillsCount };
}

let __positionsCache = { ts: 0, items: [] };
async function getAllPositionsCached(ttlMs = 2000) {
  const now = Date.now();
  if (now - __positionsCache.ts < ttlMs) return __positionsCache.items.slice();
  const items = await getAllPositions();
  __positionsCache = { ts: now, items };
  return items.slice();
}

// ---- Usable Buying Power (fresh snapshot minus pending BUY notional) ----
async function getUsableBuyingPower({ forCrypto = true } = {}) {
  // 1) fresh account
  const a = await getAccountSummaryRaw();

  // 2) start from the correct bucket
  let base = forCrypto
    ? (Number.isFinite(a.cryptoBuyingPower) ? a.cryptoBuyingPower : (Number.isFinite(a.cash) ? a.cash : a.buyingPower))
    : (Number.isFinite(a.stockBuyingPower) ? a.stockBuyingPower : a.buyingPower);

  base = Number.isFinite(base) ? base : 0;

  // 3) subtract pending BUY notional (open orders) to avoid double-spending
  let pending = 0;
  try {
    const open = await getOpenOrdersCached();
    for (const o of open || []) {
      const side = String(o.side || '').toLowerCase();
      if (side !== 'buy') continue;

      const sym = o.symbol || '';
      const isCryptoSym = normalizePair(sym).endsWith('/USD');
      if (forCrypto !== isCryptoSym) continue;

      const qty  = +o.qty || +o.quantity || NaN;
      const lim  = +o.limit_price || +o.limitPrice || NaN;
      const notl = +o.notional || NaN;

      if (Number.isFinite(notl) && notl > 0) {
        pending += notl;
      } else if (Number.isFinite(qty) && qty > 0 && Number.isFinite(lim) && lim > 0) {
        pending += qty * lim;
      }
      // market BUY with no notional: skip (cannot reserve safely)
    }
  } catch {}

  const usable = Math.max(0, base - pending);
  return { usable, base, pending, snapshot: a };
}

const cancelOpenOrdersForSymbol = async (symbol, side = null) => {
  try {
    const open = await getOpenOrdersCached();
    const normalizedSymbol = normalizePair(symbol);
    const targets = (open || []).filter(
      (o) =>
        (o.pairSymbol ?? normalizePair(o.symbol)) === normalizedSymbol &&
        (!side || (o.side || '').toLowerCase() === String(side).toLowerCase())
    );
    await Promise.all(
      targets.map((o) =>
        f(`${BACKEND_BASE_URL}/orders/${o.id}`, { method: 'DELETE', headers: BACKEND_HEADERS }).catch(() => null)
      )
    );
    __openOrdersCache = { ts: 0, items: [] };
  } catch {}
};
const cancelAllOrders = async () => {
  try {
    const orders = await getOpenOrdersCached();
    await Promise.all((orders || []).map((o) => f(`${BACKEND_BASE_URL}/orders/${o.id}`, { method: 'DELETE', headers: BACKEND_HEADERS }).catch(() => null)));
    __openOrdersCache = { ts: 0, items: [] };
  } catch {}
};

async function getOpenSellOrderBySymbol(symbol, cached = null) {
  const open = cached || (await getOpenOrdersCached());
  const normalizedSymbol = normalizePair(symbol);
  return (
    (open || []).find((o) => {
      const side = (o.side || '').toLowerCase();
      if (side !== 'sell') return false;
      const orderSymbol = o.pairSymbol ?? normalizePair(o.symbol);
      if (orderSymbol !== normalizedSymbol) return false;
      return true;
    }) || null
  );
}

async function cancelOrder(orderId) {
  if (!orderId) return false;
  try {
    await f(`${BACKEND_BASE_URL}/orders/${orderId}`, { method: 'DELETE', headers: BACKEND_HEADERS });
    __openOrdersCache = { ts: 0, items: [] };
    return true;
  } catch (err) {
    console.warn('Cancel error', err?.message || err);
    return false;
  }
}

const RISK_EXIT_COOLDOWN_MS = 60000;
function computeRiskDecision({ entryPrice, currentPrice, peakPrice, trailingActive, settings }) {
  if (!(entryPrice > 0) || !(currentPrice > 0)) return { trigger: false };
  const s = settings || SETTINGS;
  const profitPct = ((currentPrice - entryPrice) / entryPrice) * 100;
  const dropFromEntryPct = ((entryPrice - currentPrice) / entryPrice) * 100;
  const peakEff = trailingActive ? Math.max(peakPrice || 0, currentPrice) : peakPrice;
  const dropFromPeakPct = trailingActive && peakEff > 0 ? ((peakEff - currentPrice) / peakEff) * 100 : 0;
  let trigger = false;
  let reason = null;
  if (s.enableStops && dropFromEntryPct >= (s.stopLossPct || 0)) {
    trigger = true;
    reason = 'fixed_stop';
  }
  if (!trigger && s.enableTrailing && trailingActive && dropFromPeakPct >= (s.trailDropPct || 0)) {
    trigger = true;
    reason = 'trailing_stop';
  }
  return { trigger, reason, profitPct, dropFromEntryPct, dropFromPeakPct, peakEff };
}

async function ensureRiskExitsForPosition(pos, ctx = {}) {
  if (!SETTINGS.enableStops) return false;
  const symbol = pos.symbol;
  const qtyAvailable = Number(pos.qty_available ?? pos.available ?? pos.qty ?? 0);
  const entryPrice = Number(pos.avg_entry_price ?? pos.basis ?? 0);
  if (!(qtyAvailable > 0) || !(entryPrice > 0)) return false;

  const now = Date.now();
  const last = recentRiskExitRef.current.get(symbol);
  if (last && now - last < RISK_EXIT_COOLDOWN_MS) return false;

  let currentPrice = NaN;
  try {
    const q = await getQuoteSmart(symbol, ctx.preQuoteMap);
    if (q && Number.isFinite(q.bid) && Number.isFinite(q.ask) && q.bid > 0 && q.ask > 0) {
      currentPrice = 0.5 * (q.bid + q.ask);
    }
  } catch {}
  if (!(currentPrice > 0)) {
    const mv = Number(pos.market_value ?? pos.marketValue ?? 0);
    if (mv > 0 && qtyAvailable > 0) currentPrice = mv / qtyAvailable;
    if (!(currentPrice > 0)) {
      const px = Number(pos.current_price ?? pos.asset_current_price ?? 0);
      if (px > 0) currentPrice = px;
    }
  }
  if (!(currentPrice > 0)) return false;

  const riskState = riskTrailStateRef.current;
  const state = riskState.get(symbol) || { peakPrice: entryPrice, trailingActive: false };
  const settings = SETTINGS;
  const profitPct = ((currentPrice - entryPrice) / entryPrice) * 100;
  const shouldArmTrail = settings.enableTrailing && profitPct >= (settings.trailStartPct || 0);
  const trailingActive = state.trailingActive || shouldArmTrail;
  const peakPrice = trailingActive ? Math.max(state.peakPrice || entryPrice, currentPrice) : state.peakPrice || entryPrice;

  const decision = computeRiskDecision({ entryPrice, currentPrice, peakPrice, trailingActive, settings });
  riskState.set(symbol, { peakPrice, trailingActive });

  if (!decision.trigger) return false;

  const openSell = await getOpenSellOrderBySymbol(symbol, ctx.openOrders);
  if (openSell) await cancelOrder(openSell.id);

  const payload = {
    qty: qtyAvailable,
    entryPrice,
    currentPrice,
    peakPrice,
    profitPct,
    dropEntry: decision.dropFromEntryPct,
    dropPeak: decision.dropFromPeakPct,
    reason: decision.reason,
  };
  logTradeAction('risk_stop', symbol, payload);
  console.log(
    `STOP SELL ${symbol} qty=${qtyAvailable} entry=${entryPrice} current=${currentPrice} peak=${peakPrice} ` +
      `profitPct=${profitPct.toFixed?.(2)} dropEntry=${decision.dropFromEntryPct?.toFixed?.(2)} dropPeak=${decision.dropFromPeakPct?.toFixed?.(2)} reason=${decision.reason}${DRY_RUN_STOPS ? ' DRY-RUN' : ''}`
  );

  try {
    if (!DRY_RUN_STOPS) await marketSell(symbol, qtyAvailable);
    recentRiskExitRef.current.set(symbol, Date.now());
    riskState.delete(symbol);
    return true;
  } catch (err) {
    console.error('Stop sell error', err?.message || err);
    return false;
  }
}

function simulateRiskPath({ entryPrice, prices = [], settings }) {
  const s = settings || SETTINGS;
  let state = { peakPrice: entryPrice, trailingActive: false };
  let last = null;
  for (const p of prices) {
    const profitPct = ((p - entryPrice) / entryPrice) * 100;
    const shouldArmTrail = s.enableTrailing && profitPct >= (s.trailStartPct || 0);
    const trailingActive = state.trailingActive || shouldArmTrail;
    const peakPrice = trailingActive ? Math.max(state.peakPrice || entryPrice, p) : state.peakPrice || entryPrice;
    last = computeRiskDecision({ entryPrice, currentPrice: p, peakPrice, trailingActive, settings: s });
    state = { peakPrice, trailingActive };
  }
  return last;
}

function runRiskHarness() {
  const base = migrateSettings(SETTINGS);
  const fixed = simulateRiskPath({ entryPrice: 100, prices: [98], settings: base });
  const trailing = simulateRiskPath({ entryPrice: 100, prices: [103, 101.97], settings: base });
  console.log('Risk Harness — fixed stop', fixed);
  console.log('Risk Harness — trailing stop', trailing);
}

if (typeof globalThis !== 'undefined') {
  globalThis.runRiskHarness = runRiskHarness;
}

async function getAccountSummaryRaw() {
  const res = await f(`${BACKEND_BASE_URL}/account`, { headers: BACKEND_HEADERS });
  if (!res.ok) throw new Error(`Account ${res.status}`);
  const a = await res.json();
  const num = (x) => { const n = parseFloat(x); return Number.isFinite(n) ? n : NaN; };

  const equity = num(a.equity ?? a.portfolio_value);
  const portfolioValue = num(a.portfolio_value ?? a.equity);
  const accountStatus = a.status ?? a.account_status ?? a.accountStatus ?? null;

  // Buckets from Alpaca
  const stockBP  = num(a.buying_power);
  const cryptoBP = num(a.crypto_buying_power);
  const nmbp     = num(a.non_marginable_buying_power);
  const cash     = num(a.cash);
  const dtbp     = num(a.daytrade_buying_power);

  // Prefer NMBP, then cash, then cryptoBP
  const cashish = Number.isFinite(nmbp) ? nmbp
                : Number.isFinite(cash) ? cash
                : Number.isFinite(cryptoBP) ? cryptoBP
                : NaN;

  // For display, show cash-like funds as buying power
  const buyingPowerDisplay = Number.isFinite(cashish) ? cashish : stockBP;

  const prevClose = num(a.equity_previous_close);
  const lastEq = num(a.last_equity);
  const ref = Number.isFinite(prevClose) ? prevClose : lastEq;
  const changeUsd = Number.isFinite(equity) && Number.isFinite(ref) ? equity - ref : NaN;
  const changePct = Number.isFinite(changeUsd) && ref > 0 ? (changeUsd / ref) * 100 : NaN;

  const patternDayTrader = !!a.pattern_day_trader;
  const daytradeCount = Number.isFinite(+a.daytrade_count) ? +a.daytrade_count : null;

  return {
    equity,
    portfolioValue,
    buyingPower: buyingPowerDisplay,
    changeUsd, changePct,
    patternDayTrader, daytradeCount,
    cryptoBuyingPower: cashish,
    stockBuyingPower: Number.isFinite(stockBP) ? stockBP : cashish,
    daytradeBuyingPower: dtbp,
    cash,
    buyingPowerRaw: stockBP,
    nonMarginableBuyingPower: nmbp,
    accountStatus,
  };
}

const preflightState = {
  lastCheckedMs: 0,
  blocked: false,
  reason: null,
  account: null,
};
async function preflightAccountCheck() {
  const now = Date.now();
  if (now - preflightState.lastCheckedMs < 60000) return preflightState;
  preflightState.lastCheckedMs = now;
  try {
    const res = await f(`${BACKEND_BASE_URL}/account`, { headers: BACKEND_HEADERS });
    const status = res.status;
    const account = await res.json().catch(() => ({}));
    const tradingBlocked = Boolean(account.trading_blocked ?? account.tradingBlocked);
    const accountBlocked = Boolean(account.account_blocked ?? account.accountBlocked);
    const cryptoStatus = account.crypto_status ?? account.cryptoStatus ?? null;
    const blocked = tradingBlocked || accountBlocked || (cryptoStatus && cryptoStatus !== 'ACTIVE');
    preflightState.blocked = blocked;
    preflightState.reason = blocked
      ? `blocked${tradingBlocked ? ':trading' : ''}${accountBlocked ? ':account' : ''}${cryptoStatus && cryptoStatus !== 'ACTIVE' ? `:crypto_status=${cryptoStatus}` : ''}`
      : null;
    preflightState.account = account;
    console.log('preflight_account', {
      status,
      crypto_status: cryptoStatus,
      trading_blocked: tradingBlocked,
      account_blocked: accountBlocked,
    });
    if (blocked) {
      console.warn('preflight_blocked', { reason: preflightState.reason });
    }
    return preflightState;
  } catch (e) {
    console.warn('preflight_account_failed', { error: e?.message || e });
    preflightState.blocked = false;
    preflightState.reason = null;
    return preflightState;
  }
}
function capNotional(symbol, proposed, equity) {
  const hardCap = SETTINGS.absMaxNotionalUSD;
  const perSymbolDynCap = (SETTINGS.maxPosPctEquity / 100) * equity;
  return Math.max(0, Math.min(proposed, hardCap, perSymbolDynCap));
}
async function cleanupStaleBuyOrders(maxAgeSec = 30) {
  try {
    const [open, positions] = await Promise.all([getOpenOrdersCached(), getAllPositionsCached()]);
    const held = new Set((positions || []).map((p) => p.symbol));
    const now = Date.now();
    const tooOld = (o) => {
      const t = Date.parse(o.submitted_at || o.created_at || o.updated_at || '');
      if (!Number.isFinite(t)) return false;
      return (now - t) / 1000 > maxAgeSec;
    };
    const stale = (open || []).filter(
      (o) => (o.side || '').toLowerCase() === 'buy' && !held.has(o.symbol) && tooOld(o)
    );
    await Promise.all(
      stale.map(async (o) => {
        await f(`${BACKEND_BASE_URL}/orders/${o.id}`, { method: 'DELETE', headers: BACKEND_HEADERS }).catch(() => null);
      })
    );
    if (stale.length) {
      __openOrdersCache = { ts: 0, items: [] };
    }
  } catch {}
}

/* ───────────────────────── 20) STATS / EWMA / HALT STATE ───────────────────────── */
const symStats = {};
const ewma = (prev, x, a = 0.2) => (Number.isFinite(prev) ? a * x + (1 - a) * prev : x);
function pushMFE(sym, mfe, maxKeep = 120) {
  const s = symStats[sym] || (symStats[sym] = { mfeHist: [], hitByHour: Array.from({ length: 24 }, () => ({ h: 0, t: 0 })) });
  s.mfeHist.push(mfe);
  if (s.mfeHist.length > maxKeep) s.mfeHist.shift();
}
let TRADING_HALTED = false;
let HALT_REASON = '';
function syncHaltState(setHaltState) {
  if (typeof setHaltState === 'function') {
    setHaltState({ halted: TRADING_HALTED, reason: HALT_REASON });
  }
}
function shouldHaltTrading(changePct) {
  if (!Number.isFinite(changePct)) return false;
  if (SETTINGS.haltOnDailyLoss && changePct <= -Math.abs(SETTINGS.dailyMaxLossPct)) {
    HALT_REASON = `Daily loss ${changePct.toFixed(2)}% ≤ -${Math.abs(SETTINGS.dailyMaxLossPct)}%`;
    return true;
  }
  if (SETTINGS.haltOnDailyProfit && changePct >= Math.abs(SETTINGS.dailyProfitTargetPct)) {
    HALT_REASON = `Daily profit ${changePct.toFixed(2)}% ≥ ${Math.abs(SETTINGS.dailyProfitTargetPct)}%`;
    return true;
  }
  return false;
}

/* ────────────────────────── 21) DYNAMIC CRYPTO UNIVERSE ────────────────────────── */
async function fetchCryptoUniverseFromAssets() {
  return CRYPTO_CORE_TRACKED;
}

/* ─────────────────────────── Reusable mini line chart (no new deps) ─────────────────────────── */
const MiniLineChart = ({ series, valueKey = 'val', height = 100, colorMode = 'bySign', showZero = false }) => {
  const [size, setSize] = useState({ w: 0, h: height });

  if (!series || !series.length) {
    return <View style={{ height, borderRadius: 8, backgroundColor: '#121212' }} />;
  }

  const vals = series
    .map((p) => Number(p?.[valueKey]))
    .filter((x) => Number.isFinite(x));
  let yMin = Math.min(...vals);
  let yMax = Math.max(...vals);

  if (showZero) {
    yMin = Math.min(yMin, 0);
    yMax = Math.max(yMax, 0);
  }

  if (!Number.isFinite(yMin) || !Number.isFinite(yMax) || Math.abs(yMax - yMin) < 1e-9) {
    yMin = (vals[0] ?? 0) - 1;
    yMax = (vals[0] ?? 0) + 1;
  }

  const mapY = (v) => {
    const t = (v - yMin) / (yMax - yMin);
    return size.h - t * size.h;
  };

  const xStep = series.length > 1 ? size.w / (series.length - 1) : size.w;

  const pts = series.map((p, i) => {
    const v = Number(p?.[valueKey]) || 0;
    return { x: i * xStep, y: mapY(v), v };
  });

  const segs = [];
  for (let i = 1; i < pts.length; i++) {
    const p0 = pts[i - 1];
    const p1 = pts[i];
    let color = '#7fd180';
    if (colorMode === 'bySign') {
      color = p1.v >= 0 ? '#7fd180' : '#f37b7b';
    } else if (colorMode === 'byDelta') {
      color = p1.v >= pts[i - 1].v ? '#7fd180' : '#f37b7b';
    }
    const dx = p1.x - p0.x;
    const dy = p1.y - p0.y;
    const length = Math.sqrt(dx * dx + dy * dy) || 0;
    const angle = Math.atan2(dy, dx);
    segs.push({ key: `seg-${i}`, x: p0.x, y: p0.y, length, angle, color });
  }

  const zeroY = showZero ? mapY(0) : null;

  return (
    <View
      style={{ height, backgroundColor: '#121212', borderRadius: 8, overflow: 'hidden', position: 'relative' }}
      onLayout={(e) => setSize({ w: e.nativeEvent.layout.width, h: e.nativeEvent.layout.height })}
    >
      {showZero && Number.isFinite(zeroY) && (
        <View style={{ position: 'absolute', left: 0, right: 0, top: zeroY - 0.5, height: 1, backgroundColor: '#cde6f3' }} />
      )}
      {size.w > 0 &&
        segs.map((s) => (
          <View
            key={s.key}
            style={{
              position: 'absolute',
              left: s.x,
              top: s.y,
              width: s.length,
              height: 2,
              backgroundColor: s.color,
              transform: [{ rotateZ: `${s.angle}rad` }],
            }}
          />
        ))}
    </View>
  );
};

/* ─────────────────────────── 22) CHARTS: PORTFOLIO CHANGE ─────────────────────────── */
const PortfolioChangeChart = ({ acctSummary }) => {
  const [pts, setPts] = useState([]);
  const baseRef = useRef(null);   // history base_value
  const seededRef = useRef(false);

  // Seed from intraday, continuous history (single baseline)
  useEffect(() => {
    (async () => {
      if (seededRef.current) return;
      seededRef.current = true;

      const hist = await getPortfolioHistory({
        period: '1D',
        timeframe: '5Min',
        intraday_reporting: 'continuous',
        pnl_reset: 'no_reset',
        extended_hours: true,
      });
      if (!hist) return;

      // Prefer equity + base_value to avoid rounding artifacts
      const ts = (hist.timestamp || hist.timestamps || []).map((t) => parseTsMs(t)).filter(Number.isFinite);
      const eq = Array.isArray(hist.equity) ? hist.equity.map((x) => +x).filter(Number.isFinite) : [];
      let base = Number.isFinite(+hist.base_value) ? +hist.base_value : (eq.length ? eq[0] : NaN);
      if (!Number.isFinite(base) || base <= 0) {
        base = (eq.length ? eq[0] : 1);
      }
      baseRef.current = base;

      let seeded = [];
      if (eq.length && ts.length) {
        seeded = eq.map((e, i) => ({
          t: ts[i] || (Date.now() - (eq.length - i) * 300000),
          pct: ((e / base) - 1) * 100, // percent vs same base
        }));
      } else if (Array.isArray(hist.profit_loss_pct)) {
        // Fallback if equity missing: hist.profit_loss_pct is a fraction
        const plp = hist.profit_loss_pct.map((v) => (+v) * 100);
        seeded = plp.map((p, i) => ({
          t: ts[i] || (Date.now() - (plp.length - i) * 300000),
          pct: p,
        }));
      }

      if (seeded.length) setPts(seeded.slice(-200));
    })();
  }, []);

  // Live update: recompute last point from SAME base (no dailyChangePct mixing)
  useEffect(() => {
    const base = baseRef.current;
    const curEq = Number(acctSummary?.portfolioValue);
    if (!Number.isFinite(base) || !Number.isFinite(curEq) || base <= 0) return;

    const pctNow = ((curEq / base) - 1) * 100;
    setPts((prev) => {
      const next = prev && prev.length ? prev.slice() : [];
      const now = Date.now();
      if (!next.length) return [{ t: now, pct: pctNow }];
      // Replace last if same 5-min bucket, else append
      const last = next[next.length - 1];
      const sameBucket = Math.floor((last.t || 0) / 300000) === Math.floor(now / 300000);
      if (sameBucket) next[next.length - 1] = { t: now, pct: pctNow };
      else next.push({ t: now, pct: pctNow });
      return next.slice(-200);
    });
  }, [acctSummary?.portfolioValue]);

  if (!pts.length) {
    return (
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Portfolio Percentage</Text>
        <View style={{ height: 100, borderRadius: 8, backgroundColor: '#e0f8ff' }} />
      </View>
    );
  }

  const last = pts[pts.length - 1];

  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>Portfolio Percentage</Text>
      <MiniLineChart
        series={pts.map((p) => ({ val: p.pct }))}
        valueKey="val"
        height={100}
        colorMode="bySign"
        showZero
      />
      <View style={styles.legendRow}>
        <Text style={styles.subtle}>
          Now: <Text style={styles.value}>{Number.isFinite(last.pct) ? `${last.pct.toFixed(2)}%` : '—'}</Text>
        </Text>
        <Text style={styles.subtle}>{new Date(last.t).toLocaleTimeString()}</Text>
      </View>
    </View>
  );
};

/* ─────────────────────────── 23) CHART: DAILY PORTFOLIO VALUE ─────────────────────────── */
const DailyPortfolioValueChart = ({ acctSummary }) => {
  const [pts, setPts] = useState([]);
  const seededRef = useRef(false);

  useEffect(() => {
    (async () => {
      if (seededRef.current) return;
      seededRef.current = true;
      try {
        const hist = await getPortfolioHistory({ period: '1M', timeframe: '1D' });
        if (!hist) return;
        const ts = (hist.timestamp || hist.timestamps || []).map((t) => parseTsMs(t)).filter(Number.isFinite);
        let values = [];
        if (Array.isArray(hist.equity)) {
          values = hist.equity.map((x) => +x).filter(Number.isFinite);
        } else if (Array.isArray(hist.profit_loss)) {
          const pl = hist.profit_loss.map((x) => +x).filter(Number.isFinite);
          const base = Number.isFinite(+hist.base_value) ? +hist.base_value : (pl[0] || 0);
          values = pl.map((v) => base + v);
        }
        const ptsOut = values.map((v, i) => ({
          t: ts[i] || (Date.now() - (values.length - i) * 86400000),
          val: v,
        }));
        setPts(ptsOut.slice(-30));
      } catch {}
    })();
  }, []);

  // NEW: keep last point in sync with current equity
  useEffect(() => {
    const v = Number(acctSummary?.portfolioValue);
    if (!Number.isFinite(v)) return;
    setPts((prev) => {
      if (!prev.length) return [{ t: Date.now(), val: v }];
      const now = Date.now();
      const last = prev[prev.length - 1];
      const sameDay = new Date(last.t).toDateString() === new Date(now).toDateString();
      const next = sameDay ? prev.slice(0, -1).concat({ t: now, val: v })
                           : prev.concat({ t: now, val: v });
      return next.slice(-30);
    });
  }, [acctSummary?.portfolioValue]);

  if (!pts.length) {
    return (
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Portfolio Value</Text>
      </View>
    );
  }

  const last = pts[pts.length - 1];

  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>Portfolio Value</Text>
      <MiniLineChart
        series={pts}
        valueKey="val"
        height={100}
        colorMode="byDelta"
        showZero={false}
      />
      <View style={styles.legendRow}>
        <Text style={styles.subtle}>
          Now: <Text style={styles.value}>{Number.isFinite(last.val) ? fmtUSD(last.val) : '—'}</Text>
        </Text>
        <Text style={styles.subtle}>{new Date(last.t).toLocaleDateString()}</Text>
      </View>
    </View>
  );
};

/* ─────────────────────────── 23b) CHART: HOLDINGS PERCENTAGE (LOSS SHARE) ─────────────────────────── */
const HoldingsChangeBarChart = () => {
  const [rows, setRows] = useState([]);
  const [lastAt, setLastAt] = useState(null);
  const [totalValueUSD, setTotalValueUSD] = useState(0);

  // Palette pulled from existing app colors to keep style cohesion.
  const ACCENT_COLORS = ['#7fd180', '#f7b801', '#9f8ed7', '#b29cd4', '#355070', '#f37b7b', '#c5dbee', '#ead5ff', '#dcd3f7'];
  const colorFor = (sym = '') => {
    let h = 0;
    for (let i = 0; i < sym.length; i++) h = (h * 31 + sym.charCodeAt(i)) >>> 0;
    return ACCENT_COLORS[h % ACCENT_COLORS.length];
  };

  useEffect(() => {
    let stopped = false;
    const poll = async () => {
      try {
        const positions = await getAllPositionsCached();
        const totalsBySymbol = new Map();
        for (const p of positions || []) {
          const symbol = p.pairSymbol ?? normalizePair(p.symbol);
          const qty = +p.qty || 0;
          const mv = +(p.market_value ?? p.marketValue ?? 0);
          if (!(qty > 0) || !(mv > 0)) continue;
          totalsBySymbol.set(symbol, (totalsBySymbol.get(symbol) || 0) + mv);
        }

        const totalValue = Array.from(totalsBySymbol.values()).reduce((s, v) => s + v, 0);
        const rowsOut = Array.from(totalsBySymbol.entries())
          .map(([symbol, mv]) => ({ symbol, mv, pct: totalValue > 0 ? (mv / totalValue) * 100 : 0, color: colorFor(symbol) }))
          .sort((a, b) => (b.pct - a.pct));

        setTotalValueUSD(totalValue);
        setRows(rowsOut);
        setLastAt(new Date().toISOString());
      } catch {}
      if (!stopped) setTimeout(poll, 4000);
    };
    poll();
    return () => { stopped = true; };
  }, []);

  if (!rows.length) {
    return (
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Holdings Percentage</Text>
        <Text style={styles.subtle}>No current holdings detected.</Text>
      </View>
    );
  }

  // Display as left→right bars scaled to 100% of total current loss
  const maxPct = Math.max(100, ...rows.map((r) => Number(r.pct) || 0));

  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>Holdings Percentage</Text>
      <View style={{ gap: 8 }}>
        {rows.map((r) => {
          const pct = Math.max(0, Number(r.pct) || 0);
          const widthFrac = Math.min(1, pct / maxPct);
          return (
            <View key={r.symbol} style={styles.holdRow}>
              <View style={[styles.legendSwatch, { backgroundColor: r.color }]} />
              <Text style={styles.holdLabel}>{r.symbol}</Text>
              <View style={styles.holdBarWrap}>
                <View style={[styles.holdFill, { flex: widthFrac, backgroundColor: r.color }]} />
              </View>
              <Text style={styles.holdPct}>{Number.isFinite(pct) ? `${pct.toFixed(1)}%` : '—'}</Text>
            </View>
          );
        })}
      </View>
      {!!lastAt && (
        <View style={styles.legendRow}>
          <Text style={styles.subtle}>Total holdings: {fmtUSD(totalValueUSD)}</Text>
          <Text style={styles.subtle}>{new Date(lastAt).toLocaleTimeString()}</Text>
        </View>
      )}
    </View>
  );
};

/* ─────────────────────────── 24) ENTRY / ORDERING / EXITS ─────────────────────────── */
async function fetchAssetMeta(symbol) {
  try {
    const r = await f(`${BACKEND_BASE_URL}/assets/${encodeURIComponent(canonicalAsset(symbol))}`, { headers: BACKEND_HEADERS });
    if (!r.ok) return null;
    const a = await r.json();
    if (a && typeof a === 'object') {
      const num = (v, fallback) => {
        const n = Number(v);
        return Number.isFinite(n) && n > 0 ? n : fallback;
      };
      const priceInc = num(a.price_increment ?? a.min_price_increment ?? 1e-5, 1e-5);
      const qtyInc = num(a.min_trade_increment ?? a.min_order_size ?? 0.000001, 0.000001);
      const minNotionalRaw = Number(a.min_order_notional ?? 0);
      a._price_inc = priceInc;
      a._qty_inc = qtyInc;
      a._fractionable = a.fractionable !== false;
      a._min_notional = Number.isFinite(minNotionalRaw) && minNotionalRaw > 0 ? minNotionalRaw : 0;
    }
    return a;
  } catch { return null; }
}
function createQtyQuantizer(symbol, meta) {
  const rawQtyInc = meta?._qty_inc ?? 0.000001;
  const QINC = Number.isFinite(rawQtyInc) && rawQtyInc > 0 ? rawQtyInc : 0.000001;
  const qtyStepDecimals = Math.min(9, (QINC.toString().split('.')[1] || '').length || 6);
  return (value) => {
    if (!(value > 0) || !(QINC > 0)) return 0;
    const scaled = Math.floor(value / QINC + 1e-9);
    if (!(scaled > 0)) return 0;
    let quantized = Number((scaled * QINC).toFixed(qtyStepDecimals));
    if (isStock(symbol) && meta && meta._fractionable === false) {
      quantized = Math.floor(quantized);
    }
    const scaledFinal = Math.floor(quantized / QINC + 1e-9);
    if (!(scaledFinal > 0)) return 0;
    return Number((scaledFinal * QINC).toFixed(qtyStepDecimals));
  };
}
/**
 * Decision flow: compute notional → enforce funding (priority) → enforce min notional
 * with optional bump (if allowed) → otherwise ATTEMPT.
 * Priority order for skips: insufficient_funding → below_min_notional.
 */
function validateOrderCandidate({
  symbol,
  side,
  qty,
  price,
  computedNotional,
  minNotional,
  buyingPower,
  cash,
  reserve,
  maxPositions,
  currentOpenPositions,
  maxNotional,
  quantizeQty,
  autoBumpMinNotional = AUTO_BUMP_MIN_NOTIONAL,
}) {
  const sideLower = String(side || '').toLowerCase();
  const resolvedMinNotional = Number.isFinite(minNotional) && minNotional > 0 ? minNotional : MIN_ORDER_NOTIONAL_USD;
  let resolvedNotional = Number.isFinite(computedNotional) ? computedNotional : null;
  if (!Number.isFinite(resolvedNotional) && Number.isFinite(qty) && qty > 0 && Number.isFinite(price) && price > 0) {
    resolvedNotional = qty * price;
  }
  const requiredNotional = Number.isFinite(resolvedNotional) ? resolvedNotional : null;
  const fundingNotional = Number.isFinite(resolvedNotional)
    ? Math.max(resolvedNotional, resolvedMinNotional)
    : resolvedMinNotional;
  if (sideLower === 'buy') {
    const hasFunding = Number.isFinite(buyingPower)
      ? (Number.isFinite(fundingNotional) ? buyingPower >= fundingNotional : buyingPower > 0)
      : true;

    if (!hasFunding) {
      logTradeAction('skip_small_order', symbol, {
        reason: 'insufficient_funding',
        buyingPower,
        cash,
        requiredNotional: fundingNotional,
        reserve,
      });
      console.log(`${symbol} — Skip: insufficient_funding`, { buyingPower, requiredNotional: fundingNotional, reserve });
      return {
        decision: 'SKIP_insufficient_funding',
        reason: 'insufficient_funding',
        qty,
        computedNotional: resolvedNotional,
        minNotional: resolvedMinNotional,
      };
    }
  }

  if (Number.isFinite(requiredNotional) && requiredNotional < resolvedMinNotional) {
    const canBump =
      autoBumpMinNotional &&
      typeof quantizeQty === 'function' &&
      Number.isFinite(price) &&
      price > 0 &&
      (!Number.isFinite(maxNotional) || resolvedMinNotional <= maxNotional) &&
      (!Number.isFinite(buyingPower) || buyingPower >= resolvedMinNotional);
    if (canBump) {
      const oldQty = qty;
      const oldNotional = requiredNotional;
      const rawQty = resolvedMinNotional / price;
      const bumpedQty = quantizeQty(rawQty);
      const bumpedNotional = bumpedQty > 0 ? bumpedQty * price : 0;
      if (bumpedQty > 0 && bumpedNotional >= resolvedMinNotional) {
        logTradeAction('bump_qty_to_min_notional', symbol, {
          oldQty,
          newQty: bumpedQty,
          oldNotional,
          newNotional: bumpedNotional,
        });
        console.log('BUMP_QTY_TO_MIN_NOTIONAL', {
          symbol,
          oldQty,
          newQty: bumpedQty,
          oldNotional,
          newNotional: bumpedNotional,
        });
        return {
          decision: 'ATTEMPT',
          reason: null,
          qty: bumpedQty,
          computedNotional: bumpedNotional,
          minNotional: resolvedMinNotional,
        };
      }
    }
    logTradeAction('skip_small_order', symbol, {
      reason: 'below_min_notional',
      computedNotional: requiredNotional,
      minNotional: resolvedMinNotional,
      bumpAllowed: canBump,
    });
    console.log(`${symbol} — Skip: below_min_notional`, {
      computedNotional: requiredNotional,
      minNotional: resolvedMinNotional,
      bumpAllowed: canBump,
    });
    return {
      decision: 'SKIP_below_min_notional',
      reason: 'below_min_notional',
      qty,
      computedNotional: requiredNotional,
      minNotional: resolvedMinNotional,
    };
  }

  return {
    decision: 'ATTEMPT',
    reason: null,
    qty,
    computedNotional: resolvedNotional,
    minNotional: resolvedMinNotional,
  };
}
async function placeMakerThenMaybeTakerBuy(symbol, qty, preQuoteMap = null, usableBP = null, decisionContext = null) {
  const normalizedSymbol = toInternalSymbol(symbol);
  const emitDecisionSnapshot = typeof decisionContext?.emitDecisionSnapshot === 'function'
    ? decisionContext.emitDecisionSnapshot
    : null;
  let attempted = false;
  let attemptsSent = 0;
  let attemptsFailed = 0;
  let ordersOpen = 0;
  let fillsCount = 0;
  await cancelOpenOrdersForSymbol(normalizedSymbol, 'buy');
  const meta = await fetchAssetMeta(normalizedSymbol);
  const rawTick = meta?._price_inc ?? (isStock(symbol) ? 0.01 : 1e-5);
  const TICK = Number.isFinite(rawTick) && rawTick > 0 ? rawTick : (isStock(symbol) ? 0.01 : 1e-5);
  const rawQtyInc = meta?._qty_inc ?? 0.000001;
  const QINC = Number.isFinite(rawQtyInc) && rawQtyInc > 0 ? rawQtyInc : 0.000001;
  const quantizeQty = createQtyQuantizer(symbol, meta);
  let plannedQty = quantizeQty(qty);
  if (!(plannedQty > 0)) {
    emitDecisionSnapshot?.('SKIP_below_min_trade', { qty: plannedQty, computedNotional: null });
    return { filled: false, attempted, attemptsSent, attemptsFailed, ordersOpen, fillsCount };
  }
  if (plannedQty < MIN_TRADE_QTY) {
    emitDecisionSnapshot?.('SKIP_below_min_trade', { qty: plannedQty, computedNotional: null });
    logTradeAction('skip_small_order', normalizedSymbol, { reason: 'below_min_trade', qty: plannedQty, minQty: MIN_TRADE_QTY });
    return { filled: false, attempted, attemptsSent, attemptsFailed, ordersOpen, fillsCount };
  }
  qty = plannedQty;
  const minNotional = meta?._min_notional > 0 ? meta._min_notional : MIN_ORDER_NOTIONAL_USD;

  let lastOrderId = null, placedLimit = null, lastReplaceAt = 0;
  const t0 = Date.now(), CAMP_SEC = SETTINGS.makerCampSec;
  const tickDecimals = (() => {
    const frac = (TICK.toString().split('.')[1] || '');
    if (!frac.length) return isStock(symbol) ? 2 : 5;
    return Math.min(6, Math.max(frac.length, isStock(symbol) ? 2 : 5));
  })();
  const formatLimit = (px) => Number(px).toFixed(tickDecimals);

  while ((Date.now() - t0) / 1000 < CAMP_SEC) {
    const q = await getQuoteSmart(normalizedSymbol, preQuoteMap);
    if (!q) { await sleep(500); continue; }

    const barsFill = await getCryptoBars1m(normalizedSymbol, 2);
    const lastVol = Array.isArray(barsFill) && barsFill.length ? (barsFill[barsFill.length - 1].vol || 0) : 0;
    const qFillEst = makerFillProb({ bsUnits: q.bs || 0, lastBarVolUnits: lastVol, campSec: SETTINGS.makerCampSec });
    const symSlot = (symStats[symbol] ||= {});
    symSlot.qMakerFillEwma = ewma(symSlot.qMakerFillEwma, qFillEst, 0.2);
    const qFillEff = symSlot.qMakerFillEwma;

    if (qFillEff < (SETTINGS.makerMinFillProb || 0.15) && SETTINGS.enableTakerFlip) {
      logTradeAction('taker_force_flip', normalizedSymbol, { reason: 'low_q_fill', q: Number(qFillEff ?? 0).toFixed(2) });
      break;
    }
    const bidNow = q.bid, askNow = q.ask;
    if (!Number.isFinite(bidNow) || bidNow <= 0) { await sleep(250); continue; }

    const spread = (Number.isFinite(askNow) && askNow > bidNow) ? (askNow - bidNow) : 0;
    let targetLimit = bidNow + Math.max(TICK, spread * 0.25);
    targetLimit = Math.max(targetLimit, TICK);
    if (Number.isFinite(askNow) && askNow > 0) {
      targetLimit = Math.min(targetLimit, Math.max(TICK, askNow - TICK));
    }
    targetLimit = Math.round(targetLimit / TICK) * TICK;
    targetLimit = Number.isFinite(targetLimit) && targetLimit > 0 ? targetLimit : bidNow;
    targetLimit = Math.max(targetLimit, TICK);
    if (Number.isFinite(askNow) && askNow > 0) {
      targetLimit = Math.min(targetLimit, Math.max(TICK, askNow - TICK));
    }
    const join = targetLimit;
    // Debug (optional): comment out if noisy
    // console.log('JOIN', symbol, { join, qty, usableBP });

    // Ensure we never exceed usable non‑marginable/crypto BP
    if (usableBP && join > 0) {
      const feeFracMaker = (SETTINGS.feeBpsMaker || 15) / 10000;
      // Max qty allowed at this limit including buy fees
      const maxQty = Math.floor(((usableBP / (join * (1 + feeFracMaker))) / QINC) + 1e-9) * QINC;
      qty = quantizeQty(Math.min(qty, maxQty));
      if (!(qty > 0)) return { filled: false, attempted, attemptsSent, attemptsFailed, ordersOpen, fillsCount };
    }

    const notionalPx = Number.isFinite(bidNow) && bidNow > 0
      ? bidNow
      : (Number.isFinite(askNow) && askNow > 0 ? askNow : join);
    if (Number.isFinite(notionalPx) && notionalPx > 0) {
      const sizingCheck = validateOrderCandidate({
        symbol: normalizedSymbol,
        side: 'buy',
        qty,
        price: notionalPx,
        computedNotional: qty * notionalPx,
        minNotional,
        buyingPower: usableBP,
      });
      if (sizingCheck.decision !== 'ATTEMPT') {
        emitDecisionSnapshot?.(sizingCheck.decision, {
          qty: sizingCheck.qty ?? qty,
          computedNotional: sizingCheck.computedNotional,
        });
        return { filled: false, attempted, attemptsSent, attemptsFailed, ordersOpen, fillsCount };
      }
      qty = sizingCheck.qty ?? qty;
    }

    if (isStock(symbol) && meta && meta._fractionable === false) {
      const px = notionalPx;
      if (!(qty > 0) || !(px > 0) || qty * px < 5) {
        return { filled: false, attempted, attemptsSent, attemptsFailed, ordersOpen, fillsCount };
      }
    }

    const nowTs = Date.now();
    const ticksDrift = placedLimit != null ? Math.abs(join - placedLimit) / TICK : Infinity;
    const needReplace = !lastOrderId || ticksDrift >= 2 || join < (placedLimit - TICK);
    if (needReplace && (nowTs - lastReplaceAt) > 1800) {
      if (lastOrderId) {
        try { await f(`${BACKEND_BASE_URL}/orders/${lastOrderId}`, { method: 'DELETE', headers: BACKEND_HEADERS }); } catch {}
        __openOrdersCache = { ts: 0, items: [] };
      }
      const order = {
        symbol: normalizedSymbol, qty, side: 'buy', type: 'limit', time_in_force: 'gtc',
        limit_price: formatLimit(join),
      };
      try {
        logOrderPayload('buy_limit', order);
        emitDecisionSnapshot?.('ATTEMPT', {
          qty,
          computedNotional: Number.isFinite(join) && join > 0 ? qty * join : null,
        });
        attemptsSent += 1;
        const res = await f(`${BACKEND_BASE_URL}/orders`, { method: 'POST', headers: BACKEND_HEADERS, body: JSON.stringify(order) });
        const raw = await res.text(); let data; try { data = JSON.parse(raw); } catch { data = { raw }; }
        logOrderResponse('buy_limit', order, res, data);
        const status = String(data?.status || '').toLowerCase();
        const orderOk = Boolean(res.ok && data?.ok && data?.orderId && status !== 'rejected');
        if (orderOk) {
          attempted = true;
          recordRecentOrder({ id: data.orderId, symbol: normalizedSymbol, status, submittedAt: data.submittedAt });
          if (status === 'filled') {
            fillsCount += 1;
          } else if (['new', 'accepted', 'open'].includes(status)) {
            ordersOpen += 1;
          }
        } else {
          attemptsFailed += 1;
          logOrderFailure({
            order,
            endpoint: `${BACKEND_BASE_URL}/orders`,
            status: res.status,
            body: data?.error?.message || data?.message || data?.raw || raw?.slice?.(0, 200) || '',
          });
        }
        if (orderOk) {
          lastOrderId = data.orderId; placedLimit = join; lastReplaceAt = nowTs;
          logTradeAction('buy_camped', normalizedSymbol, { limit: order.limit_price });
          __openOrdersCache = { ts: 0, items: [] };
        }
      } catch (e) {
        logOrderError('buy_limit', order, e);
        logTradeAction('quote_exception', normalizedSymbol, { error: e.message });
        attemptsFailed += 1;
        logOrderFailure({
          order,
          endpoint: `${BACKEND_BASE_URL}/orders`,
          status: null,
          body: e?.message || null,
          error: e,
        });
      }
    }

    const pos = await getPositionInfo(normalizedSymbol);
    if (pos && pos.qty > 0) {
      if (lastOrderId) {
        try { await f(`${BACKEND_BASE_URL}/orders/${lastOrderId}`, { method: 'DELETE', headers: BACKEND_HEADERS }); } catch {}
        __openOrdersCache = { ts: 0, items: [] };
      }
      logTradeAction('buy_success', normalizedSymbol, {
        qty: pos.qty, limit: placedLimit != null ? formatLimit(placedLimit) : placedLimit,
      });
      __positionsCache = { ts: 0, items: [] };
      __openOrdersCache = { ts: 0, items: [] };
      if (fillsCount === 0) fillsCount = 1;
      return { filled: true, entry: pos.basis ?? placedLimit, qty: pos.qty, liquidity: 'maker', attempted, attemptsSent, attemptsFailed, ordersOpen: 0, fillsCount };
    }
    await sleep(1200);
  }

  if (lastOrderId) {
    try { await f(`${BACKEND_BASE_URL}/orders/${lastOrderId}`, { method: 'DELETE', headers: BACKEND_HEADERS }); } catch {}
    __openOrdersCache = { ts: 0, items: [] };
    logTradeAction('buy_unfilled_canceled', normalizedSymbol, {});
  }

  if (SETTINGS.enableTakerFlip) {
    const q = await getQuoteSmart(normalizedSymbol, preQuoteMap);
    if (q && q.ask > 0) {
      let mQty = qty;
      if (isStock(symbol) && meta && meta._fractionable === false) {
        mQty = Math.floor(mQty);
      }
      mQty = quantizeQty(mQty);
      if (usableBP && q && q.ask > 0) {
        const feeFracTaker = (SETTINGS.feeBpsTaker || 25) / 10000;
        const pxRef = q.ask;
        const maxQty = Math.floor(((usableBP / (pxRef * (1 + feeFracTaker))) / QINC) + 1e-9) * QINC;
        mQty = quantizeQty(Math.min(mQty, maxQty));
        if (!(mQty > 0)) return { filled: false, attempted, attemptsSent, attemptsFailed, ordersOpen, fillsCount };
      }
      if (!(mQty > 0)) return { filled: false, attempted, attemptsSent, attemptsFailed, ordersOpen, fillsCount };
      const pxRef = Number.isFinite(q.bid) && q.bid > 0 ? q.bid : q.ask;
      if (Number.isFinite(pxRef) && pxRef > 0) {
        const sizingCheck = validateOrderCandidate({
          symbol: normalizedSymbol,
          side: 'buy',
          qty: mQty,
          price: pxRef,
          computedNotional: mQty * pxRef,
          minNotional,
          buyingPower: usableBP,
        });
        if (sizingCheck.decision !== 'ATTEMPT') {
          emitDecisionSnapshot?.(sizingCheck.decision, {
            qty: sizingCheck.qty ?? mQty,
            computedNotional: sizingCheck.computedNotional,
          });
          return { filled: false, attempted, attemptsSent, attemptsFailed, ordersOpen, fillsCount };
        }
        mQty = sizingCheck.qty ?? mQty;
      }
      const tif = isStock(symbol) ? 'day' : 'gtc';
      const order = { symbol: normalizedSymbol, qty: mQty, side: 'buy', type: 'market', time_in_force: tif };
      try {
        logOrderPayload('buy_market', order);
        emitDecisionSnapshot?.('ATTEMPT', {
          qty: mQty,
          computedNotional: Number.isFinite(q.ask) && q.ask > 0 ? mQty * q.ask : null,
        });
        attemptsSent += 1;
        const res = await f(`${BACKEND_BASE_URL}/orders`, { method: 'POST', headers: BACKEND_HEADERS, body: JSON.stringify(order) });
        const raw = await res.text(); let data; try { data = JSON.parse(raw); } catch { data = { raw }; }
        logOrderResponse('buy_market', order, res, data);
        const status = String(data?.status || '').toLowerCase();
        const orderOk = Boolean(res.ok && data?.ok && data?.orderId && status !== 'rejected');
        if (orderOk) {
          attempted = true;
          recordRecentOrder({ id: data.orderId, symbol: normalizedSymbol, status, submittedAt: data.submittedAt });
          if (status === 'filled') {
            fillsCount += 1;
          } else if (['new', 'accepted', 'open'].includes(status)) {
            ordersOpen += 1;
          }
        } else {
          attemptsFailed += 1;
          logOrderFailure({
            order,
            endpoint: `${BACKEND_BASE_URL}/orders`,
            status: res.status,
            body: data?.error?.message || data?.message || data?.raw || raw?.slice?.(0, 200) || '',
          });
        }
        if (orderOk) {
          logTradeAction('buy_success', normalizedSymbol, { qty: mQty, limit: 'mkt' });
          __positionsCache = { ts: 0, items: [] };
          __openOrdersCache = { ts: 0, items: [] };
          if (fillsCount === 0) fillsCount = 1;
          return {
            filled: true,
            entry: q.ask,
            qty: mQty,
            liquidity: 'taker',
            attempted,
            attemptsSent,
            attemptsFailed,
            ordersOpen: 0,
            fillsCount,
          };
        } else {
          logTradeAction('quote_exception', normalizedSymbol, { error: `BUY mkt ${res.status} ${data?.error?.message || data?.message || data?.raw?.slice?.(0, 80) || ''}` });
        }
      } catch (e) {
        logOrderError('buy_market', order, e);
        logTradeAction('quote_exception', normalizedSymbol, { error: e.message });
        attemptsFailed += 1;
        logOrderFailure({
          order,
          endpoint: `${BACKEND_BASE_URL}/orders`,
          status: null,
          body: e?.message || null,
          error: e,
        });
      }
    }
  }
  if (!attempted && attemptsSent === 0) {
    emitDecisionSnapshot?.('SKIP_no_attempt', { qty, computedNotional: null });
  }
  return { filled: false, attempted, attemptsSent, attemptsFailed, ordersOpen, fillsCount };
}

async function marketSell(symbol, qty) {
  const normalizedSymbol = toInternalSymbol(symbol);
  try { await cancelOpenOrdersForSymbol(normalizedSymbol, 'sell'); } catch {}
  // Re-check availability right before selling to avoid 403s
  let latest = null;
  try { latest = await getPositionInfo(normalizedSymbol); } catch {}
  const usableQty = Number(latest?.available ?? qty ?? 0);
  if (!(usableQty > 0)) {
    logTradeAction('tp_limit_error', normalizedSymbol, { error: 'SELL mkt skipped — no available qty' });
    return null;
  }
  const tif = isStock(symbol) ? 'day' : 'gtc';
  const mkt = { symbol: normalizedSymbol, qty: usableQty, side: 'sell', type: 'market', time_in_force: tif };
  if (DRY_RUN_STOPS) {
    logTradeAction('risk_stop', normalizedSymbol, { dryRun: true, qty: usableQty, order: mkt });
    return { dryRun: true, order: mkt };
  }
  try {
    logOrderPayload('sell_market', mkt);
    const res = await f(`${BACKEND_BASE_URL}/orders`, { method: 'POST', headers: BACKEND_HEADERS, body: JSON.stringify(mkt) });
    const raw = await res.text(); let data; try { data = JSON.parse(raw); } catch { data = { raw }; }
    logOrderResponse('sell_market', mkt, res, data);
    if (res.ok && data?.ok && data?.orderId) {
      __positionsCache = { ts: 0, items: [] };
      __openOrdersCache = { ts: 0, items: [] };
      return data;
    }
    logOrderFailure({
      order: mkt,
      endpoint: `${BACKEND_BASE_URL}/orders`,
      status: res.status,
      body: data?.error?.message || data?.message || data?.raw || raw?.slice?.(0, 200) || '',
    });
    logTradeAction('tp_limit_error', normalizedSymbol, { error: `SELL mkt ${res.status} ${data?.error?.message || data?.message || data?.raw?.slice?.(0, 120) || ''}` });
    return null;
  } catch (e) {
    logOrderError('sell_market', mkt, e);
    logTradeAction('tp_limit_error', normalizedSymbol, { error: `SELL mkt exception ${e.message}` });
    logOrderFailure({
      order: mkt,
      endpoint: `${BACKEND_BASE_URL}/orders`,
      status: null,
      body: e?.message || null,
      error: e,
    });
    return null;
  }
}

const SELL_EPS_BPS = 0.2;

/**
 * Dynamic, spread-aware stops with a grace window to avoid instant stop-outs.
 */
const ensureRiskExits = async (symbol, { tradeStateRef, pos } = {}) => {
  if (!SETTINGS.enableStops) return false;
  const state = tradeStateRef?.current?.[symbol];
  if (!state) return false;

  const posInfo = pos ?? await getPositionInfo(symbol);
  const qty = Number(posInfo?.available ?? 0);
  if (!(qty > 0)) return false;
  const entryPx = state.entry ?? posInfo?.basis ?? posInfo?.mark ?? 0;
  if (!(entryPx > 0)) return false;

  const q = await getQuoteSmart(symbol);
  if (!q || !(q.bid > 0)) return false;
  const bid = q.bid;

  const ageSec = (Date.now() - (state.entryTs || 0)) / 1000;
  const mid = Number.isFinite(q.bid) && Number.isFinite(q.ask) ? 0.5 * (q.bid + q.ask) : q.bid;
  const spreadBpsNow = (Number.isFinite(q.ask) && q.ask > q.bid && mid > 0)
    ? ((q.ask - q.bid) / mid) * 10000
    : 0;

  const bars = await getCryptoBars1m(symbol, 8);
  const closesStop = Array.isArray(bars) ? bars.map((b) => b.close) : [];
  const { sigmaBps: sigmaBpsStop } = ewmaSigmaFromCloses(closesStop.slice(-8), SETTINGS.volHalfLifeMin);

  const effStopBps = Math.max(
    eff(symbol, 'stopLossBps'),
    Math.floor((sigmaBpsStop || 0) * (SETTINGS.stopVolMult || 2.5)),
    Math.floor((spreadBpsNow * 0.5) + SETTINGS.feeBpsMaker + SETTINGS.feeBpsTaker + 6)
  );

  if (!state.stopPx) {
    const soft = entryPx * (1 - effStopBps / 10000);
    const hardBps = Math.max(SETTINGS.hardStopLossPct * 100, effStopBps + 20);
    const hard = entryPx * (1 - hardBps / 10000);
    state.stopPx = soft; state.hardStopPx = hard;
    logTradeAction('stop_arm', symbol, { stopPx: soft, hard: false });
    logTradeAction('stop_arm', symbol, { stopPx: hard, hard: true });
  } else {
    const newSoft = entryPx * (1 - effStopBps / 10000);
    if (newSoft < state.stopPx) {
      state.stopPx = newSoft;
      logTradeAction('stop_update', symbol, { stopPx: state.stopPx });
    }
  }

  if (ageSec < Math.max(0, SETTINGS.stopGraceSec || 0)) return false;

  if (bid <= (state.hardStopPx ?? 0)) {
    const res = await marketSell(symbol, qty);
    if (res) return true;
  }

  const trailStartEff = Math.max(
    SETTINGS.trailStartBps,
    Math.round((SETTINGS.trailArmVolMult || 0.8) * (sigmaBpsStop || 0))
  );

  if (SETTINGS.enableTrailing) {
    const armPx = entryPx * (1 + trailStartEff / 10000);
    if (!state.trailArmed && bid >= armPx) {
      state.trailArmed = true;
      state.trailPeak = bid;
      logTradeAction('trail_start', symbol, { startPx: armPx });
    }
    if (state.trailArmed) {
      if (bid > (state.trailPeak ?? 0)) {
        state.trailPeak = bid;
        logTradeAction('trail_peak', symbol, { peakPx: bid });
      }
      const trailStop = (state.trailPeak ?? armPx) * (1 - SETTINGS.trailingStopBps / 10000);
      state.stopPx = Math.max(state.stopPx ?? 0, trailStop);
      logTradeAction('stop_update', symbol, { stopPx: state.stopPx });
      if (bid <= trailStop) {
        const res = await marketSell(symbol, qty);
        if (res) return true;
      }
    }
  }

  if (bid <= (state.stopPx ?? 0)) {
    const res = await marketSell(symbol, qty);
    if (res) return true;
  }
  return false;
};

/**
 * Fee-aware TP posting + taker touch exit using *dynamic buy bps* (maker/taker).
 */
const ensureLimitTP = async (symbol, limitPrice, { tradeStateRef, touchMemoRef, openSellBySym, pos } = {}) => {
  const normalizedSymbol = normalizePair(symbol);
  const posInfo = pos ?? await getPositionInfo(symbol);
  if (!posInfo || posInfo.available <= 0) return;

  const state = (tradeStateRef?.current?.[normalizedSymbol]) || {};
  const entryPx = state.entry ?? posInfo.basis ?? posInfo.mark ?? 0;
  const qty = Number(posInfo.available ?? 0);
  if (!(entryPx > 0) || !(qty > 0)) return;

  const riskExited = await ensureRiskExits(normalizedSymbol, { tradeStateRef, pos: posInfo });
  if (riskExited) return;

  const heldMinutes = (Date.now() - (state.entryTs || 0)) / 60000;
  if (Number.isFinite(heldMinutes) && heldMinutes >= SETTINGS.maxHoldMin) {
    try {
      const q = await getQuoteSmart(normalizedSymbol);
      if (q && q.bid > 0) {
        const net = projectedNetPnlUSDWithBuy({
          symbol: normalizedSymbol, entryPx, qty, sellPx: q.bid, buyBpsOverride: state.buyBpsApplied
        });
        const feeFloor = minExitPriceFeeAwareDynamic({
          symbol: normalizedSymbol, entryPx, qty, buyBpsOverride: state.buyBpsApplied
        });
        const tick = isStock(symbol) ? 0.01 : 1e-5;
        const nearFeeFloor = q.bid >= (feeFloor - (2 * tick)); // prefer scratch
        if (net >= 0 || (nearFeeFloor && net >= -Math.abs(SETTINGS.netMinProfitUSD)) || net >= -Math.abs(SETTINGS.maxTimeLossUSD)) {
          try {
            const open = await getOpenOrdersCached();
            const ex = open.find(
              (o) =>
                (o.side || '').toLowerCase() === 'sell' &&
                (o.type || '').toLowerCase() === 'limit' &&
                normalizePair(o.symbol) === normalizedSymbol
            );
            if (ex) {
              await f(`${BACKEND_BASE_URL}/orders/${ex.id}`, { method: 'DELETE', headers: BACKEND_HEADERS }).catch(() => null);
              __openOrdersCache = { ts: 0, items: [] };
            }
          } catch {}
          const mkt = await marketSell(normalizedSymbol, qty);
          if (mkt) {
            logTradeAction('tp_limit_set', normalizedSymbol, { limit: `TIME_EXIT@~${q.bid.toFixed(isStock(symbol) ? 2 : 5)}` });
            return;
          }
        }
      }
    } catch {}
  }

  const feeFloor = minExitPriceFeeAwareDynamic({
    symbol: normalizedSymbol, entryPx, qty, buyBpsOverride: state.buyBpsApplied
  });
  let finalLimit = Math.max(limitPrice, feeFloor);
  const tickSize = isStock(symbol) ? 0.01 : 1e-5;
  finalLimit = roundToTick(finalLimit, tickSize);
  if (finalLimit > limitPrice + 1e-12) {
    logTradeAction('tp_fee_floor', normalizedSymbol, { limit: finalLimit.toFixed(isStock(symbol) ? 2 : 5) });
  }

  if (SETTINGS.takerExitOnTouch) {
    const q = await getQuoteSmart(normalizedSymbol);
    const memo = (touchMemoRef.current[normalizedSymbol]) || (touchMemoRef.current[normalizedSymbol] = { count: 0, lastTs: 0, firstTouchTs: 0 });
    if (q && q.bid > 0) {
      const tick = isStock(symbol) ? 0.01 : 1e-5;
      const barsE = await getCryptoBars1m(normalizedSymbol, 8);
      const closesE = Array.isArray(barsE) ? barsE.map((b) => b.close) : [];
      const { sigmaBps: sigmaBpsE } = ewmaSigmaFromCloses(closesE.slice(-8), SETTINGS.volHalfLifeMin);
      const epsBps = dynamicSellEpsBps({ symbol: normalizedSymbol, price: entryPx, tick, sigmaBps: sigmaBpsE, settings: SETTINGS });
      const touchPx = finalLimit * (1 - epsBps / 10000);
      const touching = q.bid >= touchPx;

      if (touching) {
        const now = Date.now();
        memo.count = now - memo.lastTs > 2000 * 5 ? 1 : memo.count + 1;
        memo.lastTs = now;
        if (!memo.firstTouchTs) memo.firstTouchTs = now;
        const ageSec = (now - memo.firstTouchTs) / 1000;
        logTradeAction('tp_touch_tick', normalizedSymbol, { count: memo.count, bid: q.bid });

        const guard = String(SETTINGS.takerExitGuard || 'fee').toLowerCase();
        const okByMin = meetsMinProfitWithBuy({
          symbol: normalizedSymbol, entryPx, qty, sellPx: q.bid, buyBpsOverride: state.buyBpsApplied
        });
        const okByFee = q.bid >= feeFloor * (1 - 1e-6);
        const okProfit = guard === 'min' ? okByMin : okByFee;

        const timedForce = ageSec >= Math.max(2, SETTINGS.touchFlipTimeoutSec) && okByFee;
        if ((memo.count >= SETTINGS.touchTicksRequired && okProfit) || timedForce) {
          try {
            const open = await getOpenOrdersCached();
            const ex = open.find(
              (o) =>
                (o.side || '').toLowerCase() === 'sell' &&
                (o.type || '').toLowerCase() === 'limit' &&
                normalizePair(o.symbol) === normalizedSymbol
            );
            if (ex) {
              await f(`${BACKEND_BASE_URL}/orders/${ex.id}`, { method: 'DELETE', headers: BACKEND_HEADERS }).catch(() => null);
              __openOrdersCache = { ts: 0, items: [] };
            }
          } catch {}
          const mkt = await marketSell(normalizedSymbol, qty);
          if (mkt) {
            touchMemoRef.current[normalizedSymbol] = { count: 0, lastTs: 0, firstTouchTs: 0 };
            logTradeAction(timedForce ? 'taker_force_flip' : 'tp_limit_set', normalizedSymbol, {
              limit: timedForce ? `FORCE@~${q.bid.toFixed?.(5) ?? q.bid}` : `TAKER@~${q.bid.toFixed?.(5) ?? q.bid}`
            });
            return;
          }
        } else if (memo.count >= SETTINGS.touchTicksRequired && !okProfit) {
          logTradeAction('taker_blocked_fee', normalizedSymbol, {});
        }
      } else {
        memo.count = 0;
        memo.lastTs = Date.now();
        memo.firstTouchTs = 0;
      }
    }
  }

  const limitTIF = isStock(symbol) ? 'day' : 'gtc';
  let existing = openSellBySym?.get?.(normalizedSymbol) || null;
  if (existing && (existing.type || '').toLowerCase() !== 'limit') existing = null;
  const now = Date.now();
  const lastTs = state.lastLimitPostTs || 0;
  const existingLimit = existing ? parseFloat(existing.limit_price ?? existing.limitPrice) : NaN;
  const priceDrift = Number.isFinite(existingLimit)
    ? Math.abs(existingLimit - finalLimit) / Math.max(1, finalLimit)
    : Infinity;
  const needsPost = !existing || priceDrift > 0.001 || now - lastTs > 1000 * 10;
  if (!needsPost) return;

  try {
    const decimals = isStock(symbol) ? 2 : 5;
    const order = {
      symbol: toInternalSymbol(normalizedSymbol),
      qty,
      side: 'sell',
      type: 'limit',
      time_in_force: limitTIF,
      limit_price: finalLimit.toFixed(decimals),
    };
    logOrderPayload('sell_limit', order);
    const res = await f(`${BACKEND_BASE_URL}/orders`, { method: 'POST', headers: BACKEND_HEADERS, body: JSON.stringify(order) });
    const raw = await res.text(); let data; try { data = JSON.parse(raw); } catch { data = { raw }; }
    logOrderResponse('sell_limit', order, res, data);
    if (res.ok && data?.ok && data?.orderId) {
      tradeStateRef.current[normalizedSymbol] = { ...(state || {}), lastLimitPostTs: now };
      if (existing) { await f(`${BACKEND_BASE_URL}/orders/${existing.id}`, { method: 'DELETE', headers: BACKEND_HEADERS }).catch(() => null); }
      logTradeAction('tp_limit_set', normalizedSymbol, { id: data.orderId, limit: order.limit_price });
    } else {
      const msg = data?.error?.message || data?.message || data?.raw?.slice?.(0, 100) || '';
      logOrderFailure({
        order,
        endpoint: `${BACKEND_BASE_URL}/orders`,
        status: res.status,
        body: msg,
      });
      logTradeAction('tp_limit_error', normalizedSymbol, { error: `POST ${res.status} ${msg}` });
    }
  } catch (e) {
    logOrderError('sell_limit', { symbol: normalizedSymbol, qty, side: 'sell', type: 'limit', time_in_force: limitTIF, limit_price: finalLimit }, e);
    logTradeAction('tp_limit_error', normalizedSymbol, { error: e.message });
    logOrderFailure({
      order: { symbol: normalizedSymbol, qty, side: 'sell', type: 'limit', time_in_force: limitTIF, limit_price: finalLimit },
      endpoint: `${BACKEND_BASE_URL}/orders`,
      status: null,
      body: e?.message || null,
      error: e,
    });
  }
};

const reconcileExits = async ({
  positions,
  openOrders,
  autoTrade,
  tradeStateRef,
  touchMemoRef,
  settings,
}) => {
  const openSellBySym = new Map(
    (openOrders || [])
      .filter((o) => (o.side || '').toLowerCase() === 'sell')
      .map((o) => [o.pairSymbol ?? normalizePair(o.symbol), o])
  );

  for (const pos of positions || []) {
    const symbol = pos.pairSymbol ?? normalizePair(pos.symbol);
    const qty = Number(pos.qty ?? pos.quantity ?? 0);
    const hasOpenSell = openSellBySym.has(symbol);
    console.log('EXIT_CHECK', { symbol, hasOpenSell });

    if (!autoTrade || hasOpenSell) continue;
    if (!(qty > 0)) continue;

    const meta = await fetchAssetMeta(symbol);
    const quantizeQty = createQtyQuantizer(symbol, meta);
    const plannedQty = quantizeQty(qty);
    if (!(plannedQty > 0) || plannedQty < MIN_TRADE_QTY) {
      console.warn('exit_reconcile_skip', { symbol, qty: plannedQty, reason: 'below_min_trade' });
      continue;
    }
    const minNotional = meta?._min_notional > 0 ? meta._min_notional : MIN_ORDER_NOTIONAL_USD;
    const state = tradeStateRef?.current?.[symbol] || {};
    const entryBase = Number(state.entry ?? pos.avg_entry_price ?? pos.basis ?? pos.mark ?? 0);
    if (!(entryBase > 0)) {
      console.warn('exit_reconcile_skip', { symbol, qty: plannedQty, reason: 'missing_entry' });
      continue;
    }
    const slipEw = symStats[symbol]?.slipEwmaBps ?? (settings?.slipBpsByRisk?.[settings?.riskLevel ?? SETTINGS.riskLevel] ?? 1);
    const needAdj = Math.max(
      requiredProfitBpsForSymbol(symbol, settings?.riskLevel ?? SETTINGS.riskLevel),
      exitFloorBps(symbol) + 0.5 + slipEw,
      eff(symbol, 'netMinProfitBps')
    );
    const tpBase = entryBase * (1 + needAdj / 10000);
    const feeFloor = minExitPriceFeeAwareDynamic({
      symbol,
      entryPx: entryBase,
      qty: plannedQty,
      buyBpsOverride: state.buyBpsApplied,
    });
    const tp = Math.max(tpBase, feeFloor);
    const sizingCheck = validateOrderCandidate({
      symbol,
      side: 'sell',
      qty: plannedQty,
      price: tp,
      computedNotional: plannedQty * tp,
      minNotional,
      quantizeQty,
      autoBumpMinNotional: false,
    });
    if (sizingCheck.decision !== 'ATTEMPT') {
      console.warn('exit_reconcile_skip', { symbol, qty: plannedQty, reason: sizingCheck.reason });
      continue;
    }
    await ensureLimitTP(symbol, tp, { tradeStateRef, touchMemoRef, openSellBySym, pos });
  }
};

/* ───────────────────────── 25) CONCURRENCY / PDT ───────────────────────────── */
function __recentHitRate() {
  let h = 0, t = 0;
  for (const s of Object.values(symStats)) {
    for (const b of (s.hitByHour || [])) { h += b.h || 0; t += b.t || 0; }
  }
  const r = t > 0 ? h / t : 0.5;
  return clamp(r, 0, 1);
}
const concurrencyCapBySpread = (avgBps) => {
  const base = SETTINGS.maxConcurrentPositions;
  const hit = __recentHitRate();
  let cap = base;
  if (Number.isFinite(avgBps)) {
    if (avgBps < 6) cap += 4;
    else if (avgBps < 10) cap += 2;
    else if (avgBps >= 16) cap -= 2;
  }
  if (hit < 0.45) cap -= 2;
  else if (hit > 0.60) cap += 1;
  return Math.max(2, cap);
};
function pdtBlockedForEquities(eq, flagged, dt) { return false; } // crypto-only

/* ─────────────────────────────── 26) APP ROOT ─────────────────────────────── */
export default function App() {
  const LOG_UI_LIMIT = 200;
  const LOG_LINE_HEIGHT = 14;

  const [tracked, setTracked] = useState(CRYPTO_CORE_TRACKED);
  const [univUpdatedAt, setUnivUpdatedAt] = useState(new Date().toISOString());
  const [data, setData] = useState([]);
  const [refreshing, setRefreshing] = useState(false);
  const [darkMode] = useState(true);
  const [autoTrade, setAutoTrade] = useState(true);
  const [notification, setNotification] = useState(null);
  const [logHistory, setLogHistory] = useState([]);
  const [connectivity, setConnectivity] = useState({ ok: null, checkedAt: null, error: null });
  const [haltState, setHaltState] = useState({ halted: TRADING_HALTED, reason: HALT_REASON });

  const [overrides, setOverrides] = useState({});
  const [lastSkips, setLastSkips] = useState({});
  const lastSkipsRef = useRef({});
  const [overrideSym, setOverrideSym] = useState(null);
  const overrideSymRef = useRef(overrideSym);
  const logUiBufferRef = useRef([]);
  const logFlushTimerRef = useRef(null);
  const notificationTimerRef = useRef(null);
  const scanTimerRef = useRef(null);
  const dustSweepTimerRef = useRef(null);
  const monitorControllersRef = useRef(new Map());
  const supportedCryptoSetRef = useRef(null);

  const skipHistoryRef = useRef(new Map());
  const lastAutoTuneRef = useRef(new Map());

  const coach = null;
  const setCoach = () => {};

  const [isUpdatingAcct, setIsUpdatingAcct] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [acctSummary, setAcctSummary] = useState({
    portfolioValue: null, buyingPower: null, dailyChangeUsd: null, dailyChangePct: null,
    patternDayTrader: null, daytradeCount: null, updatedAt: null,
    cryptoBuyingPower: null, stockBuyingPower: null, cash: null
  });
  const [pnlSnap, setPnlSnap] = useState({ last7Sum: null, last7UpDays: null, last7DownDays: null, last30Sum: null, fees30: null, fillsCount30: null, updatedAt: null, error: null });

  const [lastScanAt, setLastScanAt] = useState(null);
  const [openMeta, setOpenMeta] = useState({ positions: 0, orders: 0, allowed: CRYPTO_CORE_TRACKED.length, universe: CRYPTO_CORE_TRACKED.length });
  const [scanStats, setScanStats] = useState({
    ready: 0,
    attemptsSent: 0,
    attemptsFailed: 0,
    ordersOpen: 0,
    fillsCount: 0,
    watch: 0,
    skipped: 0,
    reasons: {},
  });

  const [settings, setSettings] = useState({ ...getEffectiveSettings() });
  const lastRiskChangeRef = useRef({ ts: 0, source: null, level: null });
  const effectiveSettings = useMemo(() => getEffectiveSettings(settings), [settings]);
  useEffect(() => {
    (async () => {
      try {
        const raw = await AsyncStorage.getItem(SETTINGS_STORAGE_KEY);
        if (raw) {
          const parsed = safeJsonParse(raw, null, 'settings');
          if (!parsed) return;
          const migrated = migrateSettings(parsed);
          setSettings(migrated);
          SETTINGS = { ...migrated };
        }
      } catch (err) {
        console.warn('Settings load failed', err?.message || err);
      }
    })();
  }, []);
  useEffect(() => {
    AsyncStorage.setItem(SETTINGS_STORAGE_KEY, JSON.stringify(settings)).catch(() => {});
  }, [settings]);
  useEffect(() => {
    SETTINGS = { ...settings };
  }, [settings]);

  const logRiskChange = ({ level, source, reason, spreadMax }) => {
    logTradeAction('risk_changed', 'SETTINGS', { level, spreadMax, source, reason });
    console.log(`SETTINGS — Risk→${level} (source=${source}, reason=${reason})`);
  };

  const applySettingsUpdate = (updater, { source = 'UI', reason = 'manual' } = {}) => {
    setSettings((s) => {
      const next = typeof updater === 'function' ? updater(s) : { ...s, ...updater };
      if (next.riskLevel !== s.riskLevel) {
        const now = Date.now();
        const last = lastRiskChangeRef.current || {};
        if (last.source && last.source !== source && now - last.ts < 2000) {
          return s;
        }
        lastRiskChangeRef.current = { ts: now, source, level: next.riskLevel };
        logRiskChange({
          level: next.riskLevel,
          source,
          reason,
          spreadMax: next.spreadMaxBps ?? s.spreadMaxBps,
        });
      }
      return next;
    });
  };

  const setRiskLevel = (level, { source = 'UI', reason = 'manual' } = {}) => {
    applySettingsUpdate((s) => ({ ...s, riskLevel: level }), { source, reason });
  };

  useEffect(() => {
    SETTINGS_OVERRIDES = { ...overrides };
  }, [overrides]);
  const [showSettings, setShowSettings] = useState(false);

  const [health, setHealth] = useState({ checkedAt: null, sections: {} });

  const scanningRef = useRef(false);
  const scanLockRef = useRef(false);
  const tradeStateRef = useRef({});
  const globalSpreadAvgRef = useRef(18);
  const touchMemoRef = useRef({});
  const stockPageRef = useRef(0);
  const cryptoPageRef = useRef(0);
  const riskTrailStateRef = useRef(new Map());
  const recentRiskExitRef = useRef(new Map());

  const lastAcctFetchRef = useRef(0);
  const getAccountSummaryThrottled = async (minMs = 30000) => {
    const now = Date.now();
    if (now - lastAcctFetchRef.current < minMs) return;
    lastAcctFetchRef.current = now;
    await getAccountSummary();
  };

  useEffect(() => {
    overrideSymRef.current = overrideSym;
  }, [overrideSym]);

  useEffect(() => {
    const cancel = bootConnectivityCheck(setConnectivity);
    return () => { if (typeof cancel === 'function') cancel(); };
  }, []);

  useEffect(() => {
    let active = true;
    const refreshSupportedPairs = async (force = false) => {
      const set = await fetchSupportedCryptoPairs({ force });
      if (!active) return;
      supportedCryptoSetRef.current = set;
    };
    refreshSupportedPairs(false);
    const interval = setInterval(() => {
      refreshSupportedPairs(true);
    }, SUPPORTED_CRYPTO_REFRESH_MS);
    return () => {
      active = false;
      clearInterval(interval);
    };
  }, []);

  useEffect(() => {
    return () => {
      for (const controller of monitorControllersRef.current.values()) {
        controller.abort();
      }
      monitorControllersRef.current.clear();
      if (notificationTimerRef.current) clearTimeout(notificationTimerRef.current);
      if (scanTimerRef.current) clearTimeout(scanTimerRef.current);
      if (dustSweepTimerRef.current) clearTimeout(dustSweepTimerRef.current);
    };
  }, []);

  useEffect(() => {
    registerLogSubscriber((entry) => {
      logUiBufferRef.current.push(entry);
    });
    const seed = logBuffer
      .slice(-LOG_UI_LIMIT)
      .reverse()
      .map((e) => {
        const f = friendlyLog(e);
        return { ts: e.timestamp, sev: f.sev, text: f.text, hint: null, raw: e };
      });
    if (seed.length) setLogHistory(seed);

    logFlushTimerRef.current = setInterval(() => {
      const buffer = logUiBufferRef.current;
      if (!buffer.length) return;
      logUiBufferRef.current = [];
      const batch = buffer.slice().reverse().map((entry) => {
        const f = friendlyLog(entry);
        return { ts: entry.timestamp, sev: f.sev, text: f.text, hint: null, raw: entry };
      });
      setLogHistory((prev) => [...batch, ...prev].slice(0, LOG_UI_LIMIT));

      let skipsChanged = false;
      for (const entry of buffer) {
        if (entry?.type === 'entry_skipped' && entry?.symbol) {
          lastSkipsRef.current[entry.symbol] = entry;
          skipsChanged = true;
          if (!overrideSymRef.current) setOverrideSym(entry.symbol);
          const sym = entry.symbol;
          const arr = skipHistoryRef.current.get(sym) || [];
          arr.push({
            ts: Date.now(),
            reason: entry.reason,
            spreadBps: entry.spreadBps,
            feeBps: entry.feeBps,
            mid: entry.mid,
            tpBps: entry.tpBps,
          });
          if (arr.length > 100) arr.splice(0, arr.length - 100);
          skipHistoryRef.current.set(sym, arr);
        }
      }
      if (skipsChanged) setLastSkips({ ...lastSkipsRef.current });
    }, 350);

    return () => {
      if (logFlushTimerRef.current) clearInterval(logFlushTimerRef.current);
      logFlushTimerRef.current = null;
      logUiBufferRef.current = [];
      registerLogSubscriber(null);
    };
  }, []);
  const showNotification = (msg) => {
    setNotification(msg);
    if (notificationTimerRef.current) clearTimeout(notificationTimerRef.current);
    notificationTimerRef.current = setTimeout(() => setNotification(null), 5000);
  };

  useEffect(() => {
    (async () => {
      const cryptoOnly = CRYPTO_CORE_TRACKED;
      setTracked(cryptoOnly);
      setUnivUpdatedAt(new Date().toISOString());
      setOpenMeta((m) => ({ ...m, universe: cryptoOnly.length, allowed: cryptoOnly.length }));
      logTradeAction('scan_start', 'UNIVERSE', { batch: cryptoOnly.length, stocks: 0, cryptos: cryptoOnly.length });
      await loadData();
    })();
  }, []);

  const getAccountSummary = async () => {
    setIsUpdatingAcct(true);
    try {
      const a = await getAccountSummaryRaw();
      setAcctSummary({
        portfolioValue: a.equity, buyingPower: a.buyingPower, dailyChangeUsd: a.changeUsd, dailyChangePct: a.changePct,
        patternDayTrader: a.patternDayTrader, daytradeCount: a.daytradeCount, updatedAt: new Date().toISOString(),
        cryptoBuyingPower: a.cryptoBuyingPower, stockBuyingPower: a.stockBuyingPower, cash: a.cash
      });
      if (shouldHaltTrading(a.changePct)) {
        TRADING_HALTED = true;
        logTradeAction('daily_halt', 'SYSTEM', { reason: HALT_REASON });
        showNotification(`⛔ Trading halted: ${HALT_REASON}`);
        syncHaltState(setHaltState);
      } else {
        TRADING_HALTED = false;
        syncHaltState(setHaltState);
      }
    } catch (e) {
      console.warn('Account fetch failed', { error: e?.message || e });
      logTradeAction('quote_exception', 'ACCOUNT', { error: e.message });
    } finally { setIsUpdatingAcct(false); }
  };

  async function checkAlpacaHealth() {
    const report = { checkedAt: new Date().toISOString(), sections: {} };

    try {
      const m = await getCryptoQuotesBatch(['BTC/USD', 'ETH/USD']);
      const bt = m.get('BTC/USD'), et = m.get('ETH/USD');
      const freshB = !!bt && !isStaleQuoteEntry(bt);
      const freshE = !!et && !isStaleQuoteEntry(et);
      report.sections.crypto = { ok: !!(freshB || freshE), detail: { 'BTC/USD': !!freshB, 'ETH/USD': !!freshE } };
      logTradeAction(freshB || freshE ? 'health_ok' : 'health_warn', 'SYSTEM', { section: 'crypto', note: freshB || freshE ? '' : 'no fresh quotes' });
    } catch (e) {
      report.sections.crypto = { ok: false, note: e.message };
      logTradeAction('health_err', 'SYSTEM', { section: 'crypto', note: e.message });
    }

    setHealth(report);
  }

  async function monitorOutcome(symbol, entryPx, v0, signal) {
    const HORIZ_MIN = 3, STEP_MS = 10000;
    let t0 = Date.now(), best = 0;
    while (Date.now() - t0 < HORIZ_MIN * 60 * 1000) {
      if (signal?.aborted) return;
      let price = null;
      const m = await getCryptoTradesBatch([toInternalSymbol(symbol)]);
      const one = m.get(toInternalSymbol(symbol));
      price = Number.isFinite(one?.price) ? one.price : null;
      if (Number.isFinite(price)) best = Math.max(best, price - entryPx);
      const waited = await sleepWithSignal(STEP_MS, signal);
      if (!waited) return;
    }
    if (v0 > 0 && best > 0) {
      const g_hat = (v0 * v0) / (2 * best);
      const s = (symStats[symbol] ||= { mfeHist: [], hitByHour: Array.from({ length: 24 }, () => ({ h: 0, t: 0 })) });
      s.drag_g = ewma(s.drag_g, g_hat, 0.2);
      pushMFE(symbol, best);
      const hr = new Date().getUTCHours();
      const need = (requiredProfitBpsForSymbol(symbol, effectiveSettings.riskLevel) / 10000) * entryPx;
      const hb = s.hitByHour[hr] || (s.hitByHour[hr] = { h: 0, t: 0 });
      hb.t += 1;
      if (best >= need) hb.h += 1;
    }
  }

  const logGateFail = (symbol, gate, detailText = '', details = null) => {
    const detailsPayload = details && typeof details === 'object' ? details : null;
    if (detailsPayload) {
      console.log(`${symbol} — GateFail — ${gate}`, detailsPayload);
      return;
    }
    const detailSuffix = detailText ? ` (${detailText})` : '';
    console.log(`${symbol} — GateFail — ${gate}${detailSuffix}`);
  };

  const logReadyAttempt = ({ symbol, orderUsd, price, spreadBps }) => {
    const spreadText = Number.isFinite(spreadBps) ? `${spreadBps.toFixed(1)}bps` : 'n/a';
    const orderText = Number.isFinite(orderUsd) ? orderUsd.toFixed(2) : orderUsd;
    const priceText = Number.isFinite(price) ? price.toFixed(8) : price;
    console.log(
      `${symbol} — READY — willAttemptBuy orderUsd=${orderText}, price=${priceText}, spread=${spreadText}`
    );
  };
  const logDecisionTrace = (symbol, reason, details = {}) => {
    console.log('decision_trace', { symbol, reason, ...details });
  };
  const logDecisionSnapshot = ({
    symbol,
    side,
    qty,
    computedNotional,
    minNotional,
    buyingPower,
    cash,
    reserve,
    maxPositions,
    currentOpenPositions,
    decision,
  }) => {
    console.log(
      `DECISION_SNAPSHOT ${JSON.stringify({
        symbol,
        side,
        qty,
        computedNotional,
        minNotional,
        buyingPower,
        cash,
        reserve,
        maxPositions,
        currentOpenPositions,
        decision,
      })}`
    );
  };

  async function computeEntrySignal(asset, d, riskLvl, preQuoteMap = null, preBarsMap = null, batchId = null, fundingCtx = null) {
    let bars1 = [];
    if (effectiveSettings.enforceMomentum) {
      if (preBarsMap && preBarsMap.has(asset.symbol)) {
        bars1 = preBarsMap.get(asset.symbol) || [];
      } else {
        bars1 = await getCryptoBars1m(asset.symbol, 6);
      }
    }
    const closes = Array.isArray(bars1) ? bars1.map((b) => b.close) : [];
    const { sigma, sigmaBps } = ewmaSigmaFromCloses(closes.slice(-16), effectiveSettings.volHalfLifeMin);
    const symEntry = (symStats[asset.symbol] ||= {});
    if (!Array.isArray(symEntry.mfeHist)) symEntry.mfeHist = [];
    if (!Array.isArray(symEntry.hitByHour)) symEntry.hitByHour = Array.from({ length: 24 }, () => ({ h: 0, t: 0 }));
    symEntry.sigmaEwmaBps = ewma(symEntry.sigmaEwmaBps, sigmaBps, 0.2);
    const sigmaBpsEff = symEntry.sigmaEwmaBps ?? sigmaBps;

    const q = await getQuoteSmart(asset.symbol, preQuoteMap);
    if (!q || !(q.bid > 0 && q.ask > 0)) {
      const normalized = toInternalSymbol(asset.symbol);
      const missingAt = lastQuoteBatchMissing.get(normalized);
      if (missingAt) {
        return { entryReady: false, why: 'no_quote', meta: { freshSec: null, lastSeenAgeSec: null } };
      }
      const cached = quoteCache.get(normalized);
      if (cached && isStaleQuoteEntry(cached)) {
        logStaleQuote(normalized, cached, { reason: 'stale_quote' });
        const lastSeenMs = getQuoteLastSeenMs(cached);
        const ageMs = Number.isFinite(lastSeenMs) ? Date.now() - lastSeenMs : null;
        return {
          entryReady: false,
          why: 'stale_quote',
          meta: { lastSeenAgeSec: formatLoggedAgeSeconds(ageMs) },
        };
      }
      return { entryReady: false, why: 'no_quote', meta: { freshSec: null } };
    }

    const mid = 0.5 * (q.bid + q.ask);
    const spreadBps = Number.isFinite(q.spreadBps)
      ? q.spreadBps
      : ((q.ask - q.bid) / mid) * 10000;
    const lastSeenMs = getQuoteLastSeenMs(q);
    const ageMs = Number.isFinite(lastSeenMs) ? Date.now() - lastSeenMs : null;
    const tsMs = Number.isFinite(q.tsMs) && q.tsMs > 0 ? q.tsMs : lastSeenMs;
    if (isStaleQuoteEntry(q)) {
      const bpsText = Number.isFinite(spreadBps) ? spreadBps.toFixed(1) : 'n/a';
      const ageText = Number.isFinite(ageMs) ? Math.round(ageMs) : 'n/a';
      const tsText = Number.isFinite(tsMs) ? tsMs : 'n/a';
      console.warn(`${asset.symbol} — Quote STALE (bps=${bpsText}, ageMs=${ageText}, ts=${tsText})`);
      logTradeAction('quote_stale', asset.symbol, { spreadBps: +Number(spreadBps || 0).toFixed(1), ageMs, tsMs, batchId });
      logStaleQuote(asset.symbol, q, { reason: 'stale_quote' });
      return { entryReady: false, why: 'stale_quote', meta: { lastSeenAgeSec: formatLoggedAgeSeconds(ageMs) } };
    }

    const mm = microMetrics(q);

    if (q.bs != null && Number.isFinite(q.bs) && (q.bs * q.bid) < MIN_BID_NOTIONAL_LOOSE_USD) {
      logGateFail(asset.symbol, 'illiquid', `bidNotional=${(q.bs * q.bid).toFixed(2)}, min=${MIN_BID_NOTIONAL_LOOSE_USD}`);
      return {
        entryReady: false,
        why: 'illiquid',
        meta: {
          bs: Number.isFinite(q.bs) && Number.isFinite(q.bid) ? q.bs * q.bid : 0,
          min: MIN_BID_NOTIONAL_LOOSE_USD
        }
      };
    }
    if (BLACKLIST.has(asset.symbol)) {
      logGateFail(asset.symbol, 'blacklist');
      return { entryReady: false, why: 'blacklist', meta: {} };
    }
    if (mid < eff(asset.symbol, 'minPriceUsd')) {
      logGateFail(asset.symbol, 'tiny_price', `mid=${mid.toFixed(6)}, min=${eff(asset.symbol, 'minPriceUsd')}`);
      return { entryReady: false, why: 'tiny_price', meta: { mid } };
    }

    if (fundingCtx && Number.isFinite(fundingCtx.availableUsd)) {
      const availableUsd = fundingCtx.availableUsd;
      const minNotional = Number.isFinite(fundingCtx.minNotional) ? fundingCtx.minNotional : MIN_ORDER_NOTIONAL_USD;
      const targetNotional = Number.isFinite(fundingCtx.targetNotional) ? fundingCtx.targetNotional : null;
      const requiredNotional = Number.isFinite(targetNotional)
        ? Math.max(minNotional, Math.min(targetNotional, availableUsd))
        : minNotional;
      if (availableUsd < requiredNotional) {
        const cash = fundingCtx.snapshot?.cash;
        const rawBuyingPower = fundingCtx.snapshot?.buyingPowerRaw;
        const nmbp = fundingCtx.snapshot?.nonMarginableBuyingPower;
        const reserve = fundingCtx.reserveUsd;
        const openOrderHold = fundingCtx.openOrderHold;
        const fmtMoney = (value) => (Number.isFinite(value) ? value.toFixed(2) : value);
        console.warn(
          `${asset.symbol} — Skip: insufficient_funding (availableUsd=${Number(availableUsd).toFixed(2)}, ` +
          `requiredNotional=${Number(requiredNotional).toFixed(2)}, cash=${fmtMoney(cash)}, ` +
          `buying_power=${fmtMoney(rawBuyingPower)}, ` +
          `non_marginable_buying_power=${fmtMoney(nmbp)}, ` +
          `reserve=${fmtMoney(reserve)}, openOrderHold=${fmtMoney(openOrderHold)})`
        );
        logTradeAction('skip_small_order', asset.symbol, {
          reason: 'insufficient_funding',
          availableUsd,
          requiredNotional,
          cash,
          buying_power: rawBuyingPower,
          non_marginable_buying_power: nmbp,
          reserve,
          openOrderHold,
          batchId,
        });
        return {
          entryReady: false,
          why: 'insufficient_funding',
          meta: { availableUsd, requiredNotional, reserve, openOrderHold },
        };
      }
    }

    if (spreadBps > d.spreadMax + SPREAD_EPS_BPS) {
      logTradeAction('skip_wide_spread', asset.symbol, { spreadBps: +spreadBps.toFixed(1) });
      logGateFail(
        asset.symbol,
        'spread_too_wide',
        `spread=${spreadBps.toFixed(1)}bps, max=${d.spreadMax}bps`
      );
      return { entryReady: false, why: 'spread', meta: { spreadBps, max: d.spreadMax } };
    }

    const feeBps = roundTripFeeBpsEstimate(asset.symbol);
    if (effectiveSettings.requireSpreadOverFees && spreadBps < feeBps + eff(asset.symbol, 'spreadOverFeesMinBps')) {
      const minBps = feeBps + eff(asset.symbol, 'spreadOverFeesMinBps');
      logGateFail(
        asset.symbol,
        'spread_fee_gate',
        `spread=${spreadBps.toFixed(1)}bps, min=${minBps.toFixed(1)}bps`
      );
      return { entryReady: false, why: 'spread_fee_gate', meta: { spreadBps, feeBps } };
    }

    const ema5 = emaArr(closes.slice(-6), 5);
    const slopeUp = ema5.length >= 2 ? ema5.at(-1) > ema5.at(-2) : true;
    const v0 = closes.length >= 2 ? closes.at(-1) - closes.at(-2) : 0;
    const breakout = closes.length >= 6 ? closes.at(-1) >= Math.max(...closes.slice(-6, -1)) * 1.001 : true;
    let _momentumPenalty = false;
    if (effectiveSettings.enforceMomentum && !(v0 >= 0 || slopeUp || breakout)) {
      // Momentum penalty: reduce pUp later instead of hard veto
      _momentumPenalty = true;
    }

    symEntry.spreadEwmaBps = ewma(symEntry.spreadEwmaBps, spreadBps, 0.2);
    const slipEw = symEntry.slipEwmaBps ?? (effectiveSettings.slipBpsByRisk?.[riskLvl] ?? 1);

    const stopBaseBps = eff(asset.symbol, 'stopLossBps');
    const effStopBps = Math.max(
      stopBaseBps,
      Math.round((effectiveSettings.stopVolMult || 2.5) * (sigmaBpsEff || 0))
    );

    const needDyn = Math.max(
      requiredProfitBpsForSymbol(asset.symbol, riskLvl),
      exitFloorBps(asset.symbol) + 0.5 + slipEw,
      eff(asset.symbol, 'netMinProfitBps')
    );
    const needBpsVol = Math.max(needDyn, Math.round((effectiveSettings.tpVolScale || 1.0) * (sigmaBpsEff || 0)));

    const aPrice = q.bid * (effStopBps / 10000);
    const bPrice = q.bid * (needBpsVol / 10000);
    let pUp = barrierPTouchUpDriftless(aPrice, bPrice);

    if (Number.isFinite(mm.microDrift) && q.bid > 0) {
      const driftBps = (mm.microDrift / q.bid) * 10000;
      pUp = clamp(pUp + 0.02 * Math.tanh(driftBps / Math.max(1, sigmaBpsEff)), 0.05, 0.95);
    }

    if (_momentumPenalty) {
      pUp = clamp(0.5 + 0.5 * (pUp - 0.5) * 0.6, 0.05, 0.95); // shrink toward 0.5
    }

    const sellBps = effectiveSettings.takerExitOnTouch ? effectiveSettings.feeBpsTaker : effectiveSettings.feeBpsMaker;
    const EVps = expectedValuePerShare({
      pUp,
      aPrice,
      bPrice,
      entryPx: q.bid,
      sellFeeBps: sellBps,
      buyFeeBps: effectiveSettings.feeBpsMaker,
      slippageBps: slipEw,
    });

    const spreadEw = symEntry.spreadEwmaBps ?? spreadBps;
    const needBpsCapped = Math.min(
      Math.max(needBpsVol, exitFloorBps(asset.symbol) + 1),
      Math.max(exitFloorBps(asset.symbol) + 1, Math.round((spreadEw || 0) * 1.6))
    );
    const tpBase = q.bid * (1 + needBpsCapped / 10000);
    if (!(tpBase > q.bid * 1.00005)) {
      logGateFail(
        asset.symbol,
        'edge_negative',
        `tpBps=${needBpsCapped.toFixed(1)}, bid=${q.bid}`
      );
      return { entryReady: false, why: 'edge_negative', meta: { tpBps: needBpsCapped, bid: q.bid, tp: tpBase } };
    }

    // ── EV guard (fail-open if disabled) ─────────────────────────────────────────
    if (effectiveSettings.evGuardEnabled) {
      // Compute EV in bps relative to entry
      const evBps = expectedValueBps({ symbol: asset.symbol, q, tpBps: needBpsCapped, buyBpsOverride: effectiveSettings.feeBpsMaker });
      const evUsd = (evBps / 10000) * q.bid; // per 1 unit; order sizing handled later

      if (effectiveSettings.evShowDebug && !isStaleQuoteEntry(q)) {
        logTradeAction('ev_debug', asset.symbol, { evBps: Number(evBps?.toFixed?.(2)), tpBps: Number(needBpsCapped?.toFixed?.(1)), batchId });
      }

      if (!(evBps >= (effectiveSettings.evMinBps ?? -1) || evUsd >= (effectiveSettings.evMinUSD ?? -0.02))) {
        logGateFail(
          asset.symbol,
          'edge_negative_ev',
          `evBps=${Number(evBps?.toFixed?.(2))}, evUsd=${Number(evUsd?.toFixed?.(4))}`
        );
        return { entryReady: false, why: 'edge_negative_ev', meta: { evBps, evUsd, tpBps: needBpsCapped } };
      }
    }
    // If EV guard is disabled → proceed (classic gates already passed)

    return {
      entryReady: true,
      spreadBps,
      quote: q,
      tpBps: needBpsCapped,
      tp: tpBase,
      v0,
      runway: 0,
      pUp,
      EVps,
      sigmaBps: sigmaBpsEff,
      microImb: mm.imbalance,
      meta: {},
      batchId,
    };
  }

  const placeOrder = async (symbol, ccSymbol = symbol, d, sigPre = null, preQuoteMap = null, refs, batchId = null) => {
    const normalizedSymbol = toInternalSymbol(symbol);
    if (TRADING_HALTED) {
      logTradeAction('daily_halt', normalizedSymbol, { reason: HALT_REASON || 'Rule' });
      logGateFail(normalizedSymbol, 'trading_halted', `reason=${HALT_REASON || 'Rule'}`);
      return { attempted: false, filled: false, attemptsSent: 0, attemptsFailed: 0, ordersOpen: 0, fillsCount: 0, decisionReason: 'trading_halted' };
    }
    const preflight = await preflightAccountCheck();
    if (preflight?.blocked) {
      logTradeAction('entry_skipped', normalizedSymbol, { entryReady: false, reason: 'preflight_blocked' });
      logGateFail(normalizedSymbol, 'preflight_blocked', preflight.reason || '');
      return { attempted: false, filled: false, attemptsSent: 0, attemptsFailed: 0, ordersOpen: 0, fillsCount: 0, decisionReason: 'preflight_blocked' };
    }
    if (FORCE_ONE_TEST_BUY && !forceTestBuyUsed && !STABLES.has(normalizedSymbol) && !BLACKLIST.has(normalizedSymbol)) {
      const notional = Math.max(1, Math.min(5, FORCE_ONE_TEST_BUY_NOTIONAL || 2));
      const order = { symbol: normalizedSymbol, notional, side: 'buy', type: 'market', time_in_force: 'gtc' };
      console.warn('FORCE_ONE_TEST_BUY', { symbol: normalizedSymbol, notional });
      let attemptsSent = 0;
      let attemptsFailed = 0;
      let ordersOpen = 0;
      let fillsCount = 0;
      try {
        logOrderPayload('force_test_buy', order);
        forceTestBuyUsed = true;
        attemptsSent += 1;
        const res = await f(`${BACKEND_BASE_URL}/orders`, { method: 'POST', headers: BACKEND_HEADERS, body: JSON.stringify(order) });
        const raw = await res.text(); let data; try { data = JSON.parse(raw); } catch { data = { raw }; }
        logOrderResponse('force_test_buy', order, res, data);
        const status = String(data?.status || '').toLowerCase();
        const orderOk = Boolean(res.ok && data?.ok && data?.orderId && status !== 'rejected');
        if (orderOk) {
          recordRecentOrder({ id: data.orderId, symbol: normalizedSymbol, status, submittedAt: data.submittedAt });
          if (status === 'filled') {
            fillsCount += 1;
          } else if (['new', 'accepted', 'open'].includes(status)) {
            ordersOpen += 1;
          }
          return { attempted: true, filled: true, attemptsSent, attemptsFailed, ordersOpen, fillsCount, decisionReason: 'force_test_buy' };
        }
        attemptsFailed += 1;
        logOrderFailure({
          order,
          endpoint: `${BACKEND_BASE_URL}/orders`,
          status: res.status,
          body: data?.error?.message || data?.message || data?.raw || raw?.slice?.(0, 200) || '',
        });
        return { attempted: true, filled: false, attemptsSent, attemptsFailed, ordersOpen, fillsCount, decisionReason: 'force_test_buy_failed' };
      } catch (e) {
        attemptsFailed += 1;
        logOrderError('force_test_buy', order, e);
        logOrderFailure({
          order,
          endpoint: `${BACKEND_BASE_URL}/orders`,
          status: null,
          body: e?.message || null,
          error: e,
        });
        return { attempted: true, filled: false, attemptsSent, attemptsFailed, ordersOpen, fillsCount, decisionReason: 'force_test_buy_failed' };
      }
    }
    if (STABLES.has(normalizedSymbol)) {
      return { attempted: false, filled: false, attemptsSent: 0, attemptsFailed: 0, ordersOpen: 0, fillsCount: 0, decisionReason: 'stable_asset' };
    }

    await cleanupStaleBuyOrders(30);

    let openOrdersCount = 0;
    try {
      const openOrders = await getOpenOrdersCached();
      openOrdersCount = Array.isArray(openOrders) ? openOrders.length : 0;
    } catch {}
    const cap = concurrencyCapBySpread(globalSpreadAvgRef.current);

    let currentOpenPositions = null;
    try {
      const allPos = await getAllPositionsCached();
      const nonStableOpen = (allPos || []).filter((p) => Number(p.qty) > 0 && Number(p.market_value || p.marketValue || 0) > 1).length;
      currentOpenPositions = nonStableOpen;
      if (nonStableOpen >= cap) {
        logTradeAction('concurrency_guard', normalizedSymbol, { cap, avg: globalSpreadAvgRef.current, hitRate: (function(){let h=0,t=0;for(const s of Object.values(symStats)){for(const b of (s.hitByHour||[])){h+=b.h||0;t+=b.t||0;}}return t>0?(h/t):null;})() });
        logGateFail(normalizedSymbol, 'max_positions', '', {
          open_positions_count: nonStableOpen,
          max_positions: cap,
          open_orders_count: openOrdersCount,
        });
        return { attempted: false, filled: false, attemptsSent: 0, attemptsFailed: 0, ordersOpen: 0, fillsCount: 0, decisionReason: 'cap_reached' };
      }
    } catch {}

    const held = await getPositionInfo(normalizedSymbol);
    const avail = Number(held?.available ?? 0);
    const mv    = Number(held?.marketValue ?? 0);
    const trulyHeld = avail > 0 || mv >= effectiveSettings.dustFlattenMaxUsd;
    if (trulyHeld) {
      logTradeAction('entry_skipped', normalizedSymbol, { entryReady: false, reason: 'held' });
      logGateFail(normalizedSymbol, 'held');
      return { attempted: false, filled: false, attemptsSent: 0, attemptsFailed: 0, ordersOpen: 0, fillsCount: 0, decisionReason: 'held' };
    }

    const sig = sigPre || (await computeEntrySignal({ symbol: normalizedSymbol, cc: ccSymbol }, d, effectiveSettings.riskLevel, preQuoteMap, null, batchId));
    if (!sig.entryReady) {
      return { attempted: false, filled: false, attemptsSent: 0, attemptsFailed: 0, ordersOpen: 0, fillsCount: 0 };
    }

    const meta = await fetchAssetMeta(normalizedSymbol);
    const minNotional = meta?._min_notional > 0 ? meta._min_notional : MIN_ORDER_NOTIONAL_USD;
    const quantizeQty = createQtyQuantizer(normalizedSymbol, meta);

    // Fetch fresh, pending-aware BP to avoid 403s
    const bpInfo = await getUsableBuyingPower({ forCrypto: isCrypto(normalizedSymbol) });
    let equity   = Number.isFinite(acctSummary.portfolioValue) ? acctSummary.portfolioValue : (bpInfo.snapshot?.equity ?? NaN);
    if (bpInfo.snapshot) {
      const a = bpInfo.snapshot;
      setAcctSummary((s) => ({
        portfolioValue: a.equity, buyingPower: a.buyingPower, dailyChangeUsd: a.changeUsd, dailyChangePct: a.changePct,
        patternDayTrader: a.patternDayTrader, daytradeCount: a.daytradeCount, updatedAt: new Date().toISOString(),
        cryptoBuyingPower: a.cryptoBuyingPower, stockBuyingPower: a.stockBuyingPower, cash: a.cash,
      }));
    }
    if (!Number.isFinite(equity) || equity <= 0) equity = 1000;

    const buyingPower = bpInfo.usable; // this is the only number we size from
    if (!Number.isFinite(buyingPower) || buyingPower <= 0) {
      const validation = validateOrderCandidate({
        symbol: normalizedSymbol,
        side: 'buy',
        qty: null,
        price: null,
        computedNotional: minNotional,
        minNotional,
        buyingPower,
        cash: bpInfo.snapshot?.cash,
        reserve: bpInfo.pending,
        maxPositions: cap,
        currentOpenPositions,
      });
      logDecisionSnapshot({
        symbol: normalizedSymbol,
        side: 'buy',
        qty: null,
        computedNotional: validation.computedNotional,
        minNotional: validation.minNotional,
        buyingPower,
        cash: bpInfo.snapshot?.cash,
        reserve: bpInfo.pending,
        maxPositions: cap,
        currentOpenPositions,
        decision: validation.decision,
      });
      logTradeAction('entry_skipped', normalizedSymbol, { entryReady: true, reason: 'skip_small_order', note: 'no usable BP' });
      logGateFail(normalizedSymbol, 'buying_power', '', {
        buying_power: Number.isFinite(buyingPower) ? Number(buyingPower.toFixed(2)) : buyingPower,
        min_notional: minNotional,
        open_orders_count: openOrdersCount,
        max_positions: cap,
      });
      return { attempted: false, filled: false, attemptsSent: 0, attemptsFailed: 0, ordersOpen: 0, fillsCount: 0, decisionReason: 'buying_power' };
    }

    let entryPx = Number.isFinite(sig?.quote?.bid) && sig.quote.bid > 0 ? sig.quote.bid : sig?.quote?.ask;
    if (!Number.isFinite(entryPx) || entryPx <= 0) entryPx = sig?.quote?.bid;

    let kellyNotional = null;
    if (effectiveSettings.kellyEnabled && sig && Number.isFinite(sig.pUp) && Number.isFinite(sig.EVps)) {
      const entryForKelly = Number.isFinite(entryPx) && entryPx > 0 ? entryPx : sig?.quote?.ask;
      const stopBps = eff(symbol, 'stopLossBps');
      const aPrice = Math.max(0, (entryForKelly || 0) * (stopBps / 10000));
      const bPrice = Math.max(0, (sig.tp ?? 0) - (entryForKelly || 0));
      const sellFee = (effectiveSettings.takerExitOnTouch ? effectiveSettings.feeBpsTaker : effectiveSettings.feeBpsMaker);
      const U = Math.max(1e-9, bPrice - ((sellFee * (entryForKelly || 0)) / 10000));
      const D = Math.max(1e-9, aPrice + ((effectiveSettings.feeBpsMaker * (entryForKelly || 0)) / 10000));
      const fKelly = ((sig.pUp * U) - ((1 - sig.pUp) * D)) / Math.max(1e-9, U * D);
      const f = clamp((effectiveSettings.kellyFraction || 0.5) * Math.max(0, fKelly), 0, effectiveSettings.maxPosPctEquity / 100);
      kellyNotional = f * equity;
    }

    // Cap per position
    const desired  = Math.min(buyingPower, (effectiveSettings.maxPosPctEquity / 100) * equity);
    const notionalBase = capNotional(normalizedSymbol, desired, equity);
    const notional = Number.isFinite(kellyNotional) && kellyNotional > 0
      ? Math.min(notionalBase, kellyNotional)
      : notionalBase;

    if (!Number.isFinite(entryPx) || entryPx <= 0) entryPx = sig.quote.bid;

    // Size using ask (or join) + fees + a tiny cushion
    const pxForSizing = (sig?.quote?.ask ?? sig?.quote?.bid);
    const feeFrac = (effectiveSettings.feeBpsMaker || 15) / 10000; // assume maker for entry
    const cushion = 0.0008; // 8 bps headroom
    const denom = Math.max(pxForSizing * (1 + feeFrac + cushion), 1e-9);
    let qty = quantizeQty(notional / denom);
    const computedNotional = Number.isFinite(pxForSizing) && pxForSizing > 0 ? qty * pxForSizing : notional;
    const validation = validateOrderCandidate({
      symbol: normalizedSymbol,
      side: 'buy',
      qty,
      price: pxForSizing,
      computedNotional,
      minNotional,
      buyingPower,
      cash: bpInfo.snapshot?.cash,
      reserve: bpInfo.pending,
      maxPositions: cap,
      currentOpenPositions,
      maxNotional: notionalBase,
      quantizeQty,
    });
    qty = validation.qty ?? qty;
    const finalNotional = Number.isFinite(validation.computedNotional) ? validation.computedNotional : computedNotional;
    if (validation.decision !== 'ATTEMPT') {
      logDecisionSnapshot({
        symbol: normalizedSymbol,
        side: 'buy',
        qty,
        computedNotional: finalNotional,
        minNotional: validation.minNotional,
        buyingPower,
        cash: bpInfo.snapshot?.cash,
        reserve: bpInfo.pending,
        maxPositions: cap,
        currentOpenPositions,
        decision: validation.decision,
      });
      const gateReason = validation.reason === 'insufficient_funding' ? 'insufficient_funding' : 'min_notional';
      logGateFail(normalizedSymbol, gateReason, '', {
        buying_power: Number.isFinite(buyingPower) ? Number(buyingPower.toFixed(2)) : buyingPower,
        min_notional: minNotional,
        open_orders_count: openOrdersCount,
        max_positions: cap,
        order_notional: Number.isFinite(finalNotional) ? Number(finalNotional.toFixed(2)) : finalNotional,
        order_qty: qty,
        reserve: Number.isFinite(bpInfo.pending) ? Number(bpInfo.pending.toFixed(2)) : bpInfo.pending,
      });
      return { attempted: false, filled: false, attemptsSent: 0, attemptsFailed: 0, ordersOpen: 0, fillsCount: 0, decisionReason: validation.reason || 'min_notional' };
    }

    console.log(`${normalizedSymbol} — GateCheck`, {
      buying_power: Number.isFinite(buyingPower) ? Number(buyingPower.toFixed(2)) : buyingPower,
      min_notional: minNotional,
      open_orders_count: openOrdersCount,
      max_positions: cap,
      order_notional: Number.isFinite(finalNotional) ? Number(finalNotional.toFixed(2)) : finalNotional,
      order_qty: qty,
    });

    // console.log('USABLE BP', symbol, buyingPower);
    const emitDecisionSnapshot = (() => {
      let logged = false;
      return (decision, overrides = {}) => {
        if (logged) return;
        logged = true;
        logDecisionSnapshot({
          symbol: normalizedSymbol,
          side: 'buy',
          qty: overrides.qty ?? qty,
          computedNotional: overrides.computedNotional ?? finalNotional,
          minNotional,
          buyingPower,
          cash: bpInfo.snapshot?.cash,
          reserve: bpInfo.pending,
          maxPositions: cap,
          currentOpenPositions,
          decision,
        });
      };
    })();
    logReadyAttempt({
      symbol: normalizedSymbol,
      orderUsd: finalNotional,
      price: entryPx,
      spreadBps: sig?.spreadBps,
    });
    const result = await placeMakerThenMaybeTakerBuy(
      normalizedSymbol,
      qty,
      preQuoteMap,
      buyingPower,
      {
        emitDecisionSnapshot,
        minNotional,
        buyingPower,
        reserve: bpInfo.pending,
      }
    );
    if (!result.filled) {
      return {
        attempted: result.attempted,
        filled: false,
        attemptsSent: result.attemptsSent ?? 0,
        attemptsFailed: result.attemptsFailed ?? 0,
        ordersOpen: result.ordersOpen ?? 0,
        fillsCount: result.fillsCount ?? 0,
        decisionReason: result.decisionReason || (result.attempted ? 'order_failed' : 'order_skipped'),
      };
    }

    const actualEntry = result.entry ?? entryPx;
    const actualQty = result.qty ?? qty;

    const buyBpsApplied = result.liquidity === 'taker' ? effectiveSettings.feeBpsTaker : effectiveSettings.feeBpsMaker;

    const barsR = await getCryptoBars1m(symbol, 12);
    const closesR = Array.isArray(barsR) ? barsR.map((b) => b.close) : [];
    const { sigma: sigmaRun } = ewmaSigmaFromCloses(closesR.slice(-12), effectiveSettings.volHalfLifeMin);
    const runwayUSD = robustRunwayUSD(symbol, actualEntry, sigmaRun, effectiveSettings.pTouchHorizonMin, symStats);

    const approxMid = sig && sig.quote ? 0.5 * (sig.quote.bid + sig.quote.ask) : actualEntry;
    const slipBpsVal = Number.isFinite(approxMid) && approxMid > 0 ? ((actualEntry - (sig?.quote?.bid ?? entryPx)) / approxMid) * 10000 : 0;
    const s = (refs.tradeStateRef.current[symbol] ||= { hitByHour: Array.from({ length: 24 }, () => ({ h: 0, t: 0 })), mfeHist: [] });
    s.slipEwmaBps = ewma(s.slipEwmaBps, Math.max(0, slipBpsVal), 0.2);

    const slipEw = s.slipEwmaBps ?? (effectiveSettings.slipBpsByRisk?.[effectiveSettings.riskLevel] ?? 1);
    const needBps0 = requiredProfitBpsForSymbol(symbol, effectiveSettings.riskLevel);
    const needBpsAdj = Math.max(needBps0, exitFloorBps(symbol) + 0.5 + slipEw, eff(symbol, 'netMinProfitBps'));
    const tpBase = Math.max(sig?.tp ?? 0, actualEntry * (1 + needBpsAdj / 10000));
    const feeFloor = minExitPriceFeeAwareDynamic({ symbol, entryPx: actualEntry, qty: actualQty, buyBpsOverride: buyBpsApplied });
    const tpCapped = Math.max(Math.min(tpBase, actualEntry + (runwayUSD || 0)), feeFloor);

    refs.tradeStateRef.current[normalizedSymbol] = {
      entry: actualEntry, qty: actualQty, tp: tpCapped, feeFloor,
      runway: runwayUSD ?? sig?.runway ?? 0, entryTs: Date.now(), lastLimitPostTs: 0,
      wasHolding: true, stopPx: null, hardStopPx: null, trailArmed: false, trailPeak: null,
      buyBpsApplied, // track actual buy fee (maker/taker)
    };
    await ensureLimitTP(normalizedSymbol, tpCapped, { tradeStateRef, touchMemoRef });

    const prevController = monitorControllersRef.current.get(normalizedSymbol);
    if (prevController) prevController.abort();
    const controller = new AbortController();
    monitorControllersRef.current.set(normalizedSymbol, controller);
    monitorOutcome(normalizedSymbol, actualEntry, sig?.v0 ?? 0, controller.signal)
      .catch(() => {})
      .finally(() => {
        if (monitorControllersRef.current.get(normalizedSymbol) === controller) {
          monitorControllersRef.current.delete(normalizedSymbol);
        }
      });
    return {
      attempted: true,
      filled: true,
      attemptsSent: result.attemptsSent ?? 0,
      attemptsFailed: result.attemptsFailed ?? 0,
      ordersOpen: result.ordersOpen ?? 0,
      fillsCount: result.fillsCount ?? 0,
      decisionReason: 'filled',
    };
  };

  useEffect(() => {
    let timer = null;
    const run = async () => {
      try {
        const [positions, openOrders] = await Promise.all([getAllPositionsCached(), getOpenOrdersCached()]);
        const posBySym = new Map((positions || []).map((p) => [p.symbol, p]));
        const openSellBySym = new Map(
          (openOrders || [])
            .filter((o) => (o.side || '').toLowerCase() === 'sell')
            .map((o) => [o.pairSymbol ?? normalizePair(o.symbol), o])
        );
        for (const p of positions || []) {
          const symbol = p.symbol;
          const basePos = posBySym.get(symbol) || p;
          const avail = Number(basePos.qty_available ?? basePos.available ?? basePos.qty ?? 0);
          const mv    = Number(basePos.market_value ?? basePos.marketValue ?? 0);
          if (!(avail > 0 || mv >= effectiveSettings.dustFlattenMaxUsd)) {
            // Not truly held → don’t maintain TP/stop; also clear stale trade state
            delete tradeStateRef.current[symbol];
            riskTrailStateRef.current.delete(symbol);
            continue;
          }

          const s = tradeStateRef.current[symbol] || {
            entry: Number(basePos.avg_entry_price || basePos.basis || 0),
            qty: Number(basePos.qty || 0),
            entryTs: Date.now(), lastLimitPostTs: 0, runway: 0, wasHolding: true, feeFloor: null,
          };
          tradeStateRef.current[symbol] = s;

          const slipEw = symStats[symbol]?.slipEwmaBps ?? (effectiveSettings.slipBpsByRisk?.[effectiveSettings.riskLevel] ?? 1);
          const needAdj = Math.max(requiredProfitBpsForSymbol(symbol, effectiveSettings.riskLevel), exitFloorBps(symbol) + 0.5 + slipEw, eff(symbol, 'netMinProfitBps'));
          const entryBase = Number(s.entry || basePos.avg_entry_price || basePos.mark || 0);
          const barsMaint = await getCryptoBars1m(symbol, 12);
          const closesMaint = Array.isArray(barsMaint) ? barsMaint.map((b) => b.close) : [];
          const { sigma: sigmaMaint } = ewmaSigmaFromCloses(closesMaint.slice(-12), effectiveSettings.volHalfLifeMin);
          const tpBase = entryBase * (1 + needAdj / 10000);
          const feeFloor = minExitPriceFeeAwareDynamic({ symbol, entryPx: entryBase, qty: avail, buyBpsOverride: s.buyBpsApplied });
          const runwayUSD = robustRunwayUSD(symbol, entryBase, sigmaMaint, effectiveSettings.pTouchHorizonMin, symStats);
          const tp = Math.max(Math.min(tpBase, entryBase + (runwayUSD || 0)), feeFloor);
          s.tp = tp; s.feeFloor = feeFloor; s.runway = runwayUSD;

          await ensureLimitTP(symbol, tp, { tradeStateRef, touchMemoRef, openSellBySym, pos: basePos });
          await ensureRiskExitsForPosition(basePos, { openOrders, preQuoteMap: null });
        }
      } finally {
        timer = setTimeout(run, 1000 * 5);
      }
    };
    run();
    return () => { if (timer) clearTimeout(timer); };
  }, []);

  useEffect(() => {
    let stopped = false;
    const sweep = async () => {
      try {
        const [positions, openOrders] = await Promise.all([getAllPositionsCached(), getOpenOrdersCached()]);
        const openSellBySym = new Map(
          (openOrders || [])
            .filter((o) => (o.side || '').toLowerCase() === 'sell')
            .map((o) => [o.pairSymbol ?? normalizePair(o.symbol), o])
        );
        for (const p of positions || []) {
          const sym = p.symbol;
          if (STABLES.has(sym) || BLACKLIST.has(sym)) continue;
          const mv = Number(p.market_value ?? p.marketValue ?? 0);
          const avail = Number(p.qty_available ?? p.available ?? p.qty ?? 0);
          if (mv > 0 && mv < effectiveSettings.dustFlattenMaxUsd && avail > 0 && !openSellBySym.has(sym)) {
            const mkt = { symbol: sym, qty: avail, side: 'sell', type: 'market', time_in_force: 'gtc' };
            try {
              const res = await f(`${BACKEND_BASE_URL}/orders`, { method: 'POST', headers: BACKEND_HEADERS, body: JSON.stringify(mkt) });
              if (res.ok) {
                __positionsCache = { ts: 0, items: [] };
                __openOrdersCache = { ts: 0, items: [] };
                logTradeAction('dust_flattened', sym, { usd: mv });
              }
            } catch {}
          }
        }
      } catch {}
      if (!stopped) {
        if (dustSweepTimerRef.current) clearTimeout(dustSweepTimerRef.current);
        dustSweepTimerRef.current = setTimeout(sweep, effectiveSettings.dustSweepMinutes * 60 * 1000);
      }
    };
    sweep();
    return () => {
      stopped = true;
      if (dustSweepTimerRef.current) clearTimeout(dustSweepTimerRef.current);
      dustSweepTimerRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!settings.autoTuneEnabled) return;
    const sweep = () => {
      try {
        if (typeof TRADING_HALTED !== 'undefined' && TRADING_HALTED) return;
        const now = Date.now();
        const windowMs = Math.max(1, settings.autoTuneWindowMin) * 60000;
        const thr = Math.max(1, settings.autoTuneThreshold);
        const cooled = (sym, key) => {
          const m = lastAutoTuneRef.current.get(sym) || {};
          const last = m[key] || 0;
          return (now - last) / 1000 >= Math.max(1, settings.autoTuneCooldownSec);
        };
        const mark = (sym, key) => {
          const m = lastAutoTuneRef.current.get(sym) || {};
          m[key] = now;
          lastAutoTuneRef.current.set(sym, m);
        };
        const entries = Array.from(skipHistoryRef.current.entries());
        const overrides = SETTINGS_OVERRIDES || {};
        let changed = 0;
        for (const [sym, arr] of entries) {
          const recent = arr.filter((a) => now - a.ts <= windowMs);
          if (!recent.length) continue;
          const count = (reason) => recent.filter((a) => a.reason === reason).length;
          if (count('spread') >= thr && cooled(sym, 'spread')) {
            const ov = overrides[sym] || {};
            const currentSpreadMax = ov.spreadMaxBps ?? settings.spreadMaxBps;
            if (currentSpreadMax < settings.autoTuneMaxSpreadBps) {
              bumpOv(sym, 'spreadMaxBps', +settings.autoTuneSpreadStepBps, { max: settings.autoTuneMaxSpreadBps });
              mark(sym, 'spread');
              changed++;
            }
          }
          if (count('spread_fee_gate') >= thr && cooled(sym, 'spread_fee_gate')) {
            const ov = overrides[sym] || {};
            const currentFeesGuard = ov.spreadOverFeesMinBps ?? settings.spreadOverFeesMinBps;
            if (currentFeesGuard > settings.autoTuneMinSpreadOverFeesBps) {
              bumpOv(sym, 'spreadOverFeesMinBps', -settings.autoTuneFeesGuardStepBps, { min: settings.autoTuneMinSpreadOverFeesBps });
              mark(sym, 'spread_fee_gate');
              changed++;
            }
          }
          if (count('edge_negative') >= thr && cooled(sym, 'edge_negative')) {
            bumpOv(sym, 'netMinProfitBps', -settings.autoTuneNetMinStepBps, { min: settings.autoTuneMinNetMinBps });
            mark(sym, 'edge_negative');
            changed++;
          }
          if (changed >= settings.autoTunePerSweepMaxSymbols) break;
        }
      } catch {}
    };
    const id = setInterval(sweep, 15000);
    return () => clearInterval(id);
  }, [
    settings.autoTuneEnabled,
    settings.autoTuneWindowMin,
    settings.autoTuneThreshold,
    settings.autoTuneCooldownSec,
    settings.autoTunePerSweepMaxSymbols,
    settings.autoTuneSpreadStepBps,
    settings.autoTuneFeesGuardStepBps,
    settings.autoTuneNetMinStepBps,
    settings.autoTuneMaxSpreadBps,
    settings.autoTuneMinSpreadOverFeesBps,
    settings.autoTuneMinNetMinBps,
  ]);

  const loadData = async () => {
    if (scanningRef.current || scanLockRef.current) return;
    scanningRef.current = true;
    scanLockRef.current = true;
    setIsLoading(true);
    if (isMarketDataCooldown()) {
      setIsLoading(false);
      setRefreshing(false);
      scanningRef.current = false;
      scanLockRef.current = false;
      return;
    }

    const effectiveTracked = tracked && tracked.length ? tracked : CRYPTO_CORE_TRACKED;
    setData((prev) =>
      prev && prev.length
        ? prev
        : effectiveTracked.map((t) => ({ ...t, price: null, entryReady: false, error: null, time: new Date().toLocaleTimeString(), spreadBps: null, tpBps: null }))
    );

    let results = [];
    try {
      await getAccountSummaryThrottled();
      await preflightAccountCheck();

      const scanBpInfo = await getUsableBuyingPower({ forCrypto: true });
      const reserveUsd = Number.isFinite(effectiveSettings.reserveUsd) ? effectiveSettings.reserveUsd : 0;
      const openOrderHold = Number.isFinite(scanBpInfo.pending) ? scanBpInfo.pending : 0;
      const baseUsd = Number.isFinite(scanBpInfo.base) ? scanBpInfo.base : 0;
      const computedAvailableUsd = Math.max(0, baseUsd - reserveUsd - openOrderHold);
      const equityForSizing = Number.isFinite(scanBpInfo.snapshot?.equity)
        ? scanBpInfo.snapshot.equity
        : (Number.isFinite(acctSummary.portfolioValue) ? acctSummary.portfolioValue : NaN);
      const targetNotional = Number.isFinite(equityForSizing)
        ? Math.min((effectiveSettings.maxPosPctEquity / 100) * equityForSizing, effectiveSettings.absMaxNotionalUSD ?? Infinity)
        : NaN;
      console.log('FUNDS', {
        cash: scanBpInfo.snapshot?.cash ?? null,
        buying_power: scanBpInfo.snapshot?.buyingPowerRaw ?? null,
        non_marginable_buying_power: scanBpInfo.snapshot?.nonMarginableBuyingPower ?? null,
        portfolio_value: scanBpInfo.snapshot?.portfolioValue ?? scanBpInfo.snapshot?.equity ?? null,
        account_status: scanBpInfo.snapshot?.accountStatus ?? null,
        computedAvailableUsd,
        reserve: reserveUsd,
        openOrderHold,
        minNotional: MIN_ORDER_NOTIONAL_USD,
        targetNotional,
        targetNotionalSettings: {
          maxPosPctEquity: effectiveSettings.maxPosPctEquity,
          absMaxNotionalUSD: effectiveSettings.absMaxNotionalUSD,
          kellyEnabled: effectiveSettings.kellyEnabled,
          kellyFraction: effectiveSettings.kellyFraction,
        },
      });
      const scanFundingCtx = {
        availableUsd: computedAvailableUsd,
        reserveUsd,
        openOrderHold,
        snapshot: scanBpInfo.snapshot,
        minNotional: MIN_ORDER_NOTIONAL_USD,
        targetNotional,
      };

      const [positions, allOpenOrders] = await Promise.all([getAllPositionsCached(), getOpenOrdersCached()]);
      console.log('ACCOUNT', scanBpInfo.snapshot ?? null);
      console.log('POSITIONS_RAW', positions);
      console.log('OPEN_ORDERS_RAW', allOpenOrders);
      await reconcileExits({
        positions,
        openOrders: allOpenOrders,
        autoTrade,
        tradeStateRef,
        touchMemoRef,
        settings: effectiveSettings,
      });
      const posBySym = new Map((positions || []).map((p) => [p.symbol, p]));
      const openCount = (positions || []).filter((p) => {
        const sym = p.symbol;
        if (STABLES.has(sym)) return false;
        const mv = parseFloat(p.market_value ?? p.marketValue ?? '0');
        const qty = parseFloat(p.qty ?? '0');
        return Number.isFinite(mv) && mv > 1 && Number.isFinite(qty) && qty > 0;
      }).length;

      let supportedSet = supportedCryptoSetRef.current;
      if (!supportedSet || supportedSet.size === 0) {
        supportedSet = await fetchSupportedCryptoPairs({ force: false });
        supportedCryptoSetRef.current = supportedSet;
      }
      const isSupportedSymbol = (sym) => {
        const internal = toInternalSymbol(sym);
        const normalized = normalizeCryptoSymbol(internal);
        if (!normalized) return false;
        return !isUnsupportedLocal(internal) && isSupportedCryptoSymbol(normalized, supportedSet);
      };
      const dropped = [];
      const supportedTracked = effectiveTracked.filter((t) => {
        const ok = isSupportedSymbol(t.symbol);
        if (!ok) dropped.push(toInternalSymbol(t.symbol));
        return ok;
      });
      console.log(`UNIVERSE_DROPPED count=${dropped.length} symbols=${JSON.stringify(dropped.slice(0, 20))}`);

      let cryptosAll = supportedTracked;
      const cryptoPages = Math.max(1, Math.ceil(Math.max(0, cryptosAll.length) / effectiveSettings.stockPageSize));
      const cIdx = cryptoPageRef.current % cryptoPages;
      const cStart = cIdx * effectiveSettings.stockPageSize;
      const cryptoSlice = cryptosAll.slice(cStart, Math.min(cStart + effectiveSettings.stockPageSize, cryptosAll.length));
      cryptoPageRef.current += 1;

      const cryptoSymbolsForQuotes = cryptoSlice.map((t) => toInternalSymbol(t.symbol));

      const barsMap = effectiveSettings.enforceMomentum ? await getCryptoBars1mBatch(cryptoSymbolsForQuotes, 6) : null;

      setOpenMeta({ positions: openCount, orders: (allOpenOrders || []).length, allowed: cryptosAll.length, universe: cryptosAll.length });
      logTradeAction('scan_start', 'STATIC', { batch: cryptoSlice.length });
      const sampleSymbol = cryptoSymbolsForQuotes[0];
      if (sampleSymbol) {
        console.log(`SYMBOL MAP: ${sampleSymbol} -> ${toAlpacaCryptoSymbol(sampleSymbol)}`);
      }

      const mixedSymbols = [...cryptoSymbolsForQuotes];
      const batchMap = await getQuotesBatch(mixedSymbols);

      for (const asset of cryptoSlice) {
        const qDisplay = batchMap.get(asset.symbol);
        if (qDisplay && qDisplay.bid > 0 && qDisplay.ask > 0) {
          const mid = 0.5 * (qDisplay.bid + qDisplay.ask);
          pushPriceHist(asset.symbol, mid);
        }
      }

      let readyCount = 0;
      let attemptsSent = 0;
      let attemptsFailed = 0;
      let ordersOpen = 0;
      let fillsCount = 0;
      let watchCount = 0;
      let skippedCount = 0;
      const reasonCounts = {};
      const spreadSamples = [];
      const scanBatchId = `scan_${Date.now()}_${cIdx}`;
      const submittedThisBatch = new Set();
      for (const asset of cryptoSlice) {
        const normalizedSymbol = toInternalSymbol(asset.symbol);
        const d = { spreadMax: eff(normalizedSymbol, 'spreadMaxBps') };
        const token = { ...asset, symbol: normalizedSymbol, price: null, entryReady: false, error: null, time: new Date().toLocaleTimeString(), spreadBps: null, tpBps: null };
        let decisionReason = null;
        try {
          const qDisplay = batchMap.get(normalizedSymbol);
          if (qDisplay && qDisplay.bid > 0 && qDisplay.ask > 0) token.price = 0.5 * (qDisplay.bid + qDisplay.ask);

          const prevState = tradeStateRef.current[normalizedSymbol] || {};
          const posNow = posBySym.get(normalizedSymbol);
          const isHolding = !!(posNow && Number(posNow.qty) > 0);
          tradeStateRef.current[normalizedSymbol] = { ...prevState, wasHolding: isHolding };

          const sig = await computeEntrySignal({ ...asset, symbol: normalizedSymbol }, d, effectiveSettings.riskLevel, batchMap, barsMap, scanBatchId, scanFundingCtx);
          token.entryReady = sig.entryReady;

          if (sig?.quote && sig.quote.bid > 0 && sig.quote.ask > 0) {
            const mid2 = 0.5 * (sig.quote.bid + sig.quote.ask);
            spreadSamples.push(((sig.quote.ask - sig.quote.bid) / mid2) * 10000);
          }

          if (sig.entryReady) {
            console.log(`${normalizedSymbol} — Quote OK (bps=${Number.isFinite(sig.spreadBps) ? sig.spreadBps.toFixed(1) : 'n/a'})`);
            logTradeAction('quote_ok', normalizedSymbol, { spreadBps: +Number(sig.spreadBps || 0).toFixed(1), batchId: scanBatchId });
          }

          const forceTest = FORCE_ONE_TEST_BUY && !forceTestBuyUsed && autoTrade;
          if (sig.entryReady || forceTest) {
            token.spreadBps = sig.spreadBps ?? null;
            token.tpBps = sig.tpBps ?? null;
            if (sig.entryReady) {
              readyCount++;
            }
            if (autoTrade) {
              if (submittedThisBatch.has(normalizedSymbol)) {
                console.log(`DUPLICATE_SUPPRESS ${normalizedSymbol}`);
                decisionReason = 'duplicate_suppress';
              } else {
                submittedThisBatch.add(normalizedSymbol);
                const result = await placeOrder(normalizedSymbol, asset.cc, d, sig, batchMap, { tradeStateRef, touchMemoRef }, scanBatchId);
                attemptsSent += result?.attemptsSent || 0;
                attemptsFailed += result?.attemptsFailed || 0;
                ordersOpen += result?.ordersOpen || 0;
                fillsCount += result?.fillsCount || 0;
                decisionReason = result?.decisionReason || (result?.filled ? 'filled' : (result?.attempted ? 'order_failed' : 'order_skipped'));
              }
            } else {
              logTradeAction('entry_skipped', normalizedSymbol, { entryReady: true, reason: 'auto_off' });
              decisionReason = 'auto_off';
            }
          } else {
            watchCount++;
            skippedCount++;
            if (sig?.why) reasonCounts[sig.why] = (reasonCounts[sig.why] || 0) + 1;
            logTradeAction('entry_skipped', normalizedSymbol, { entryReady: false, reason: sig.why, ...(sig.meta || {}) });
            decisionReason = sig?.why || 'signal_false';
          }
        } catch (err) {
          token.error = err?.message || String(err);
          logTradeAction('scan_error', normalizedSymbol, { error: token.error });
          watchCount++;
          skippedCount++;
          decisionReason = 'scan_error';
        }
        logDecisionTrace(normalizedSymbol, decisionReason || 'unknown');
        results.push(token);
      }

      const avg = spreadSamples.length ? spreadSamples.reduce((a, b) => a + b, 0) / spreadSamples.length : globalSpreadAvgRef.current;
      globalSpreadAvgRef.current = avg;

      setScanStats({
        ready: readyCount,
        attemptsSent,
        attemptsFailed,
        ordersOpen,
        fillsCount,
        watch: watchCount,
        skipped: skippedCount,
        reasons: reasonCounts,
      });
      logTradeAction('scan_summary', 'STATIC', { readyCount, attemptsSent, attemptsFailed, ordersOpen, fillsCount });
      const followUp = await pollRecentOrderStates();
      if (followUp && Number.isFinite(followUp?.ordersOpen)) {
        setScanStats((prev) => ({
          ...prev,
          ordersOpen: followUp.ordersOpen,
          fillsCount: (prev.fillsCount || 0) + (followUp.fillsCount || 0),
        }));
      }
    } catch (e) {
      logTradeAction('scan_error', 'ALL', { error: e?.message || String(e) });
    } finally {
      const bySym = new Map(results.map((r) => [r.symbol, r]));
      const display = (tracked && tracked.length ? tracked : CRYPTO_CORE_TRACKED).map(
        (t) =>
          bySym.get(t.symbol) || {
            ...t,
            price: null,
            entryReady: false,
            error: null,
            time: new Date().toLocaleTimeString(),
            spreadBps: null,
            tpBps: null,
          }
      );
      setData(display);
      setLastScanAt(Date.now());
      setRefreshing(false);
      setIsLoading(false);
      scanningRef.current = false;
      scanLockRef.current = false;
    }
  };

  // Auto-scan loop: run loadData repeatedly based on settings.scanMs.
  // Respects scanningRef so we never overlap scans.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        if (!scanningRef.current) {
          await loadData();
        }
      } finally {
        if (!cancelled) {
          if (scanTimerRef.current) clearTimeout(scanTimerRef.current);
          scanTimerRef.current = setTimeout(tick, Math.max(1000, effectiveSettings.scanMs));
        }
      }
    };
    tick();
    return () => {
      cancelled = true;
      if (scanTimerRef.current) clearTimeout(scanTimerRef.current);
      scanTimerRef.current = null;
    };
  }, [settings.scanMs, tracked?.length]);

  const onRefresh = () => {
    setRefreshing(true);
    loadData();
  };
  const bp = acctSummary.buyingPower, chPct = acctSummary.dailyChangePct;

  const okWindowMs = Math.max(effectiveSettings.scanMs * 3, 6000);
  const statusColor = isLoading ? '#7fd180' : !lastScanAt ? '#9aa0a6' : Date.now() - lastScanAt < okWindowMs ? '#7fd180' : '#f7b801';

  const bump = (key, delta, opts = {}) => {
    setSettings((s) => ({ ...s, [key]: clamp((s[key] ?? 0) + delta, opts.min ?? -1e9, opts.max ?? 1e9) }));
  };

  const bumpArr = (key, idx, delta, opts = {}) =>
    setSettings((s) => {
      const next = [...(s[key] || [])];
      next[idx] = clamp((next[idx] ?? 0) + delta, opts.min ?? -1e9, opts.max ?? 1e9);
      return { ...s, [key]: next };
    });

    const bumpOv = (sym, key, delta, bounds = {}) => {
      setOverrides((o) => {
        const cur = o?.[sym]?.[key];
        let nextVal = clamp((Number(cur) || 0) + delta, bounds.min ?? -1e9, bounds.max ?? 1e9);

        // Safety rails for auto‑tune
        const feesSum = (effectiveSettings.feeBpsMaker + effectiveSettings.feeBpsTaker);
        if (key === 'dynamicMinProfitBps' && nextVal < (feesSum + 5)) nextVal = feesSum + 5;
        if (key === 'spreadOverFeesMinBps' && nextVal < 0) nextVal = 0;
        if (key === 'netMinProfitBps' && nextVal < effectiveSettings.autoTuneMinNetMinBps) nextVal = effectiveSettings.autoTuneMinNetMinBps;

        const oo = { ...(o || {}) };
        const row = { ...(oo[sym] || {}) };
        row[key] = nextVal;
        oo[sym] = row;
        return oo;
      });
    };
  const clearOv = (sym) => {
    setOverrides((o) => {
      const oo = { ...(o || {}) };
      delete oo[sym];
      return oo;
    });
  };
  function lastSkipLine(sym) {
    const e = lastSkips?.[sym];
    if (!e) return '—';
    const why = e.reason || e.type || 'skip';
    if (why === 'spread' && e.spreadBps != null) {
      return `spread ${Number(e.spreadBps).toFixed(1)}bps > cap ${eff(sym, 'spreadMaxBps')}bps`;
    }
    if (why === 'spread_fee_gate') {
      const sb = e.spreadBps != null ? Number(e.spreadBps).toFixed(1) : '?';
      const fee = e.feeBps != null ? Number(e.feeBps).toFixed(1) : '?';
      return `spread ${sb}bps < fees ${fee}bps + guard ${eff(sym, 'spreadOverFeesMinBps')}bps`;
    }
    if (why === 'tiny_price' && e.mid != null) {
      return `mid≈${Number(e.mid).toPrecision(4)} < min ${eff(sym, 'minPriceUsd')}`;
    }
    if (why === 'edge_negative' && e.tpBps != null) {
      return `need ≥ ${Number(e.tpBps).toFixed(1)}bps`;
    }
    if (why === 'nomomo') return `momentum filter`;
    if (why === 'no_quote') return `no fresh quote`;
    if (why === 'stale_quote') return `stale quote`;
    return why;
  }

  const applyPreset = (name) => {
    const presets = {
      Safer:  { riskLevel: 3, spreadMaxBps: 50,  maxPosPctEquity: 10, absMaxNotionalUSD: 100, makerCampSec: 30, enableTakerFlip: false, takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 10, liveRequireQuote: true, liveFreshMsCrypto: 10000, liveFreshMsStock: 10000, enforceMomentum: true,  enableStops: true, stopLossPct: 2.0, stopLossBps: 80, hardStopLossPct: 1.8, stopGraceSec: 10, enableTrailing: true, trailStartPct: 1.0, trailDropPct: 1.0, trailStartBps: 20, trailingStopBps: 10, maxConcurrentPositions: 6, haltOnDailyLoss: true,  dailyMaxLossPct: 3.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
      Neutral:{ riskLevel: 2, spreadMaxBps: 70,  maxPosPctEquity: 15, absMaxNotionalUSD: 150, makerCampSec: 25, enableTakerFlip: false, takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 9,  liveRequireQuote: true, liveFreshMsCrypto: 15000, liveFreshMsStock: 15000, enforceMomentum: true,  enableStops: true, stopLossPct: 2.0, stopLossBps: 80, hardStopLossPct: 1.8, stopGraceSec: 10, enableTrailing: true, trailStartPct: 1.0, trailDropPct: 1.0, trailStartBps: 20, trailingStopBps: 10,  maxConcurrentPositions: 8, haltOnDailyLoss: true,  dailyMaxLossPct: 4.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
      Faster: { riskLevel: 1, spreadMaxBps: 100, maxPosPctEquity: 20, absMaxNotionalUSD: 200, makerCampSec: 20, enableTakerFlip: false, takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 8,  liveRequireQuote: true, liveFreshMsCrypto: 15000, liveFreshMsStock: 15000, enforceMomentum: true,  enableStops: true, stopLossPct: 2.0, stopLossBps: 80, hardStopLossPct: 1.8, stopGraceSec: 10, enableTrailing: true, trailStartPct: 1.0, trailDropPct: 1.0, trailStartBps: 20, trailingStopBps: 10,  maxConcurrentPositions: 8, haltOnDailyLoss: true,  dailyMaxLossPct: 5.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
      Aggro:  { riskLevel: 0, spreadMaxBps: 120, maxPosPctEquity: 25, absMaxNotionalUSD: 300, makerCampSec: 15, enableTakerFlip: true,  takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 7,  liveRequireQuote: true, liveFreshMsCrypto: 15000, liveFreshMsStock: 15000, enforceMomentum: false, enableStops: true, stopLossPct: 2.0, stopLossBps: 80, hardStopLossPct: 1.8, stopGraceSec: 10, enableTrailing: true, trailStartPct: 1.0, trailDropPct: 1.0, trailStartBps: 12, trailingStopBps: 6,  maxConcurrentPositions: 10,haltOnDailyLoss: true,  dailyMaxLossPct: 6.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
      Max:    { riskLevel: 0, spreadMaxBps: 150, maxPosPctEquity: 30, absMaxNotionalUSD: 500, makerCampSec: 10, enableTakerFlip: true,  takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 6,  liveRequireQuote: true, liveFreshMsCrypto: 15000, liveFreshMsStock: 15000, enforceMomentum: false, enableStops: true, stopLossPct: 2.0, stopLossBps: 80, hardStopLossPct: 2.0, stopGraceSec: 10, enableTrailing: true, trailStartPct: 1.0, trailDropPct: 1.0, trailStartBps: 10, trailingStopBps: 5,  maxConcurrentPositions: 12,haltOnDailyLoss: false, dailyMaxLossPct: 8.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
    };
    const p = presets[name];
    if (!p) return;
    applySettingsUpdate((s) => ({ ...s, ...p }), { source: 'UI', reason: `preset:${name}` });
  };

  function reasonKeyFromEntry(raw) {
    if (!raw) return null;
    if (raw.type === 'entry_skipped') return raw.reason || null;
    if (raw.type === 'skip_wide_spread') return 'spread';
    if (raw.type === 'taker_blocked_fee') return 'taker_blocked_fee';
    if (raw.type === 'concurrency_guard') return 'concurrency_guard';
    if (raw.type === 'skip_blacklist') return 'blacklist';
    if (raw.type === 'quote_http_error' || raw.type === 'unsupported_symbol') return 'no_quote';
    return null;
  }

  function openCoachForRaw(raw) { /* removed UI coach */ }

  function applyDelta(key, delta, bounds = {}) {
    setSettings((s) => {
      const next = { ...s };
      const cur = next[key];
      if (typeof cur === 'number' && typeof delta === 'number') {
        const v = clamp(cur + delta, bounds.min ?? -1e9, bounds.max ?? 1e9);
        next[key] = v;
      } else if (key === 'enforceMomentum' || key === 'liveRequireQuote') {
        next[key] = !cur;
      } else if (key === 'takerExitGuard' && bounds.cycle) {
        const cycle = bounds.cycle;
        const idx = Math.max(0, cycle.indexOf(String(cur)));
        next[key] = cycle[(idx + 1) % cycle.length];
      }
      return next;
    });
  }

  function renderLog(l, i) {
    const Tag = Text;
    const props = {
      key: `${l.ts}-${i}`,
      style: [
        styles.logLine,
        l.sev === 'success' ? styles.sevSuccess :
        l.sev === 'warn'    ? styles.sevWarn :
        l.sev === 'error'   ? styles.sevError : styles.sevInfo,
      ],
    };
    return (
      <Tag {...props}>
        {new Date(l.ts).toLocaleTimeString()} • {l.text}
      </Tag>
    );
  }

  return (
    <SafeAreaView style={styles.safe}>
      <ScrollView
        contentContainerStyle={[styles.container, darkMode && styles.containerDark]}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} />}
      >
        {/* Header */}
        <View style={styles.header}>
          <View style={styles.headerTopRow}>
            <View style={[styles.statusDot, { backgroundColor: statusColor }]} />
            <Text style={[styles.appTitle, darkMode && styles.titleDark]}>Magic $$</Text>
            <Text style={styles.versionTag}>{VERSION}</Text>
            <View style={[styles.pillToggle, { marginLeft: 6, backgroundColor: '#7fd180' }]}>
              <Text style={styles.pillText}>LIVE</Text>
            </View>
            <TouchableOpacity onPress={() => setShowSettings((v) => !v)} style={[styles.pillToggle, { marginLeft: 8 }]}>
              <Text style={styles.pillText}>⚙️ Settings</Text>
            </TouchableOpacity>
          </View>

          <Text style={styles.subTitle}>
            Open {openMeta.positions}/{openMeta.universe}
            <Text style={styles.dot}> • </Text>
            Orders {openMeta.orders}
            <Text style={styles.dot}> • </Text>
            Universe {openMeta.universe}
            {univUpdatedAt ? ` • U↑ ${new Date(univUpdatedAt).toLocaleTimeString()}` : ''}
          </Text>

          {notification && (
            <View style={styles.topBanner}>
              <Text style={styles.topBannerText}>{notification}</Text>
            </View>
          )}

          <View style={styles.riskHealthRow}>
            <View style={styles.riskIconGroup}>
              {RISK_LEVELS.map((icon, idx) => (
                <TouchableOpacity
                  key={idx}
                  onPress={() => setRiskLevel(idx, { source: 'UI', reason: 'tap' })}
                  style={[
                    styles.riskIconWrapper,
                    settings.riskLevel === idx && styles.riskIconWrapperActive,
                  ]}
                >
                  <Text
                    style={[
                      styles.riskIcon,
                      settings.riskLevel === idx && styles.riskIconActive,
                    ]}
                  >
                    {icon}
                  </Text>
                </TouchableOpacity>
              ))}
            </View>
            <View style={styles.healthIconGroup}>
              <View style={styles.healthIconItem}>
                <Text style={styles.healthIcon}>✅</Text>
                <Text style={styles.healthIconLabel}>Crypto</Text>
              </View>
              <TouchableOpacity onPress={checkAlpacaHealth} style={styles.chip}>
                <Text style={styles.chipText}>Re-check</Text>
              </TouchableOpacity>
            </View>
          </View>
        </View>

        {/* Settings Panel */}
        {showSettings && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>🛠️ Settings — Gates (matches logs)</Text>

            <View style={styles.rowSpace}>
              {['Safer', 'Neutral', 'Faster', 'Aggro', 'Max'].map((p) => (
                <TouchableOpacity key={p} style={styles.chip} onPress={() => applyPreset(p)}>
                  <Text style={styles.chipText}>{p}</Text>
                </TouchableOpacity>
              ))}
            </View>

            <View style={styles.line} />

            {/* Spread max */}
            <View style={styles.rowSpace}>
              <Text style={styles.label}>🧱 spread — Max spread (bps)</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('spreadMaxBps', -5, { min: 3 })}>
                  <Text style={styles.bumpBtnText}>-5</Text>
                </TouchableOpacity>
                <Text style={styles.value}>{settings.spreadMaxBps}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('spreadMaxBps', +5, { max: 200 })}>
                  <Text style={styles.bumpBtnText}>+5</Text>
                </TouchableOpacity>
              </View>
            </View>

            {/* Touch flip timeout */}
            <View style={styles.rowSpace}>
              <Text style={styles.label}>Touch flip timeout (s)</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('touchFlipTimeoutSec', -1, { min: 2 })}>
                  <Text style={styles.bumpBtnText}>-</Text>
                </TouchableOpacity>
                <Text style={styles.value}>{settings.touchFlipTimeoutSec}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('touchFlipTimeoutSec', +1, { max: 30 })}>
                  <Text style={styles.bumpBtnText}>+</Text>
                </TouchableOpacity>
              </View>
            </View>

            {!SIMPLE_SETTINGS_ONLY && (
            <View style={styles.rowSpace}>
              <Text style={styles.label}>⏱️ no_quote — require fresh quote</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity
                  style={[styles.chip, { backgroundColor: settings.liveRequireQuote ? '#4caf50' : '#2b2b2b' }]}
                  onPress={() => setSettings((s) => ({ ...s, liveRequireQuote: !s.liveRequireQuote }))}
                >
                  <Text style={styles.chipText}>{settings.liveRequireQuote ? 'ON' : 'OFF'}</Text>
                </TouchableOpacity>
              </View>
            </View>
            )}

            <View style={styles.line} />

            {/* Toggle: require spread ≥ fees + guard */}
            <View style={styles.rowSpace}>
              <Text style={styles.label}>💸 Require spread ≥ fees + guard</Text>
              <TouchableOpacity
                style={[styles.chip, { backgroundColor: settings.requireSpreadOverFees ? '#4caf50' : '#2b2b2b' }]}
                onPress={() => setSettings((s) => ({ ...s, requireSpreadOverFees: !s.requireSpreadOverFees }))}
              >
                <Text style={styles.chipText}>{settings.requireSpreadOverFees ? 'ON' : 'OFF'}</Text>
              </TouchableOpacity>
            </View>

            <View style={styles.line} />

            {/* Stops */}
            <View style={styles.rowSpace}>
              <Text style={styles.label}>🛑 Stops enabled</Text>
              <TouchableOpacity
                style={[styles.chip, { backgroundColor: settings.enableStops ? '#4caf50' : '#2b2b2b' }]}
                onPress={() => setSettings((s) => ({ ...s, enableStops: !s.enableStops }))}
              >
                <Text style={styles.chipText}>{settings.enableStops ? 'ON' : 'OFF'}</Text>
              </TouchableOpacity>
            </View>
            <View style={styles.rowSpace}>
              <Text style={styles.label}>Stop loss (%) below entry</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('stopLossPct', -0.25, { min: 0.2 })}>
                  <Text style={styles.bumpBtnText}>-</Text>
                </TouchableOpacity>
                <Text style={styles.value}>{Number(settings.stopLossPct ?? 0).toFixed(2)}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('stopLossPct', +0.25, { max: 10 })}>
                  <Text style={styles.bumpBtnText}>+</Text>
                </TouchableOpacity>
              </View>
            </View>

            {/* Trailing */}
            <View style={styles.rowSpace}>
              <Text style={styles.label}>📈 Trailing stop enabled</Text>
              <TouchableOpacity
                style={[styles.chip, { backgroundColor: settings.enableTrailing ? '#4caf50' : '#2b2b2b' }]}
                onPress={() => setSettings((s) => ({ ...s, enableTrailing: !s.enableTrailing }))}
              >
                <Text style={styles.chipText}>{settings.enableTrailing ? 'ON' : 'OFF'}</Text>
              </TouchableOpacity>
            </View>
            <View style={styles.rowSpace}>
              <Text style={styles.label}>Trail starts at profit (%)</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('trailStartPct', -0.25, { min: 0.1 })}>
                  <Text style={styles.bumpBtnText}>-</Text>
                </TouchableOpacity>
                <Text style={styles.value}>{Number(settings.trailStartPct ?? 0).toFixed(2)}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('trailStartPct', +0.25, { max: 20 })}>
                  <Text style={styles.bumpBtnText}>+</Text>
                </TouchableOpacity>
              </View>
            </View>
            <View style={styles.rowSpace}>
              <Text style={styles.label}>Sell if drop from peak (%)</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('trailDropPct', -0.25, { min: 0.1 })}>
                  <Text style={styles.bumpBtnText}>-</Text>
                </TouchableOpacity>
                <Text style={styles.value}>{Number(settings.trailDropPct ?? 0).toFixed(2)}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('trailDropPct', +0.25, { max: 20 })}>
                  <Text style={styles.bumpBtnText}>+</Text>
                </TouchableOpacity>
              </View>
            </View>

            {/* Fees (bps): Maker/Taker */}
            <View style={styles.rowSpace}>
              <Text style={styles.label}>Fees (bps): Maker / Taker</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('feeBpsMaker', -1, { min: 0 })}><Text style={styles.bumpBtnText}>-</Text></TouchableOpacity>
                <Text style={styles.value}>{settings.feeBpsMaker}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('feeBpsMaker', +1, { max: 200 })}><Text style={styles.bumpBtnText}>+</Text></TouchableOpacity>
                <Text style={[styles.value, { marginHorizontal: 6 }]}>/</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('feeBpsTaker', -1, { min: 0 })}><Text style={styles.bumpBtnText}>-</Text></TouchableOpacity>
                <Text style={styles.value}>{settings.feeBpsTaker}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('feeBpsTaker', +1, { max: 200 })}><Text style={styles.bumpBtnText}>+</Text></TouchableOpacity>
              </View>
            </View>

            {/* Gates */}
            <View style={{ marginTop: 4 }}><Text style={styles.subtle}>Gates</Text></View>

            <View style={styles.rowSpace}>
              <Text style={styles.label}>⚖️ spread_fee_gate — fees + guard (bps)</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('spreadOverFeesMinBps', -1, { min: 0 })}><Text style={styles.bumpBtnText}>-</Text></TouchableOpacity>
                <Text style={styles.value}>{settings.spreadOverFeesMinBps}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('spreadOverFeesMinBps', +1, { max: 100 })}><Text style={styles.bumpBtnText}>+</Text></TouchableOpacity>
              </View>
            </View>

            <View style={styles.rowSpace}>
              <Text style={styles.label}>📉 edge_negative — Dynamic min profit (bps)</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('dynamicMinProfitBps', -5, { min: 0 })}><Text style={styles.bumpBtnText}>-5</Text></TouchableOpacity>
                <Text style={styles.value}>{settings.dynamicMinProfitBps}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('dynamicMinProfitBps', +5, { max: 500 })}><Text style={styles.bumpBtnText}>+5</Text></TouchableOpacity>
              </View>
            </View>

            <View style={styles.rowSpace}>
              <Text style={styles.label}>📉 edge_negative — Extra over fees (bps)</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('extraOverFeesBps', -1, { min: 0 })}><Text style={styles.bumpBtnText}>-</Text></TouchableOpacity>
                <Text style={styles.value}>{settings.extraOverFeesBps}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('extraOverFeesBps', +1, { max: 100 })}><Text style={styles.bumpBtnText}>+</Text></TouchableOpacity>
              </View>
            </View>

            <View style={styles.rowSpace}>
              <Text style={styles.label}>📉 edge_negative — Absolute net min profit (bps)</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('netMinProfitBps', -0.5, { min: 0 })}><Text style={styles.bumpBtnText}>-</Text></TouchableOpacity>
                <Text style={styles.value}>{Number(settings.netMinProfitBps).toFixed(1)}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('netMinProfitBps', +0.5, { max: 100 })}><Text style={styles.bumpBtnText}>+</Text></TouchableOpacity>
              </View>
            </View>

            <View style={styles.rowSpace}>
              <Text style={styles.label}>🧪 tiny_price — Min price (USD)</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('minPriceUsd', -0.0005, { min: 0 })}><Text style={styles.bumpBtnText}>-</Text></TouchableOpacity>
                <Text style={styles.value}>{settings.minPriceUsd}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('minPriceUsd', +0.0005, { max: 10 })}><Text style={styles.bumpBtnText}>+</Text></TouchableOpacity>
              </View>
            </View>

            <View style={styles.rowSpace}>
              <Text style={styles.label}>🧠 nomomo — momentum filter</Text>
              <TouchableOpacity
                style={[styles.chip, { backgroundColor: settings.enforceMomentum ? '#4caf50' : '#2b2b2b' }]}
                onPress={() => setSettings((s) => ({ ...s, enforceMomentum: !s.enforceMomentum }))}
              >
                <Text style={styles.chipText}>{settings.enforceMomentum ? 'ON' : 'OFF'}</Text>
              </TouchableOpacity>
            </View>

            <View style={styles.rowSpace}>
              <Text style={styles.label}>🧮 concurrency_guard — Max positions</Text>
              <View style={styles.bumpGroup}>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('maxConcurrentPositions', -1, { min: 1 })}>
                  <Text style={styles.bumpBtnText}>-</Text>
                </TouchableOpacity>
                <Text style={styles.value}>{settings.maxConcurrentPositions}</Text>
                <TouchableOpacity style={styles.bumpBtn} onPress={() => bump('maxConcurrentPositions', +1, { max: 50 })}>
                  <Text style={styles.bumpBtnText}>+</Text></TouchableOpacity>
              </View>
            </View>
          </View>
        )}

        {/* Controls + Buying Power */}
        <View style={[styles.toolbar, darkMode && styles.toolbarDark]}>
          <View style={styles.topControlRow}>
            <TouchableOpacity onPress={onRefresh} style={[styles.pillToggle, styles.pillNeutral]}>
              <Text style={styles.pillText}>Refresh</Text>
            </TouchableOpacity>
            <TouchableOpacity onPress={cancelAllOrders} style={[styles.pillToggle, styles.btnWarn]}>
              <Text style={styles.pillText}>Cancel Orders</Text>
            </TouchableOpacity>
            <TouchableOpacity
              onPress={() => setAutoTrade((v) => !v)}
              style={[styles.pillToggle, { backgroundColor: autoTrade ? '#7fd180' : '#e0d8f6' }]}
            >
              <Text style={styles.pillText}>{autoTrade ? 'Auto-Trade: ON' : 'Auto-Trade: OFF'}</Text>
            </TouchableOpacity>
            <View style={styles.inlineBP}>
              <Text style={styles.bpLabel}>Buying Power</Text>
              <Text style={styles.bpValue}>
                {fmtUSD(acctSummary.buyingPower)} <Text style={styles.subtle}>crypto</Text>
                <Text style={styles.dot}> • </Text>
                {fmtUSD(acctSummary.stockBuyingPower)} <Text style={styles.subtle}>stk</Text>
                {isUpdatingAcct && <Text style={styles.badgeUpdating}>↻</Text>}
                <Text style={styles.dot}> • </Text>
                <Text style={styles.dayBadge}>Day {fmtPct(acctSummary.dailyChangePct)}</Text>
              </Text>
            </View>
          </View>
        </View>

        {/* Chart order per request: 1) Portfolio Percentage, 2) Holdings Percentage, 3) Portfolio Value */}
        {/* NEW: P&L + Benchmarks scoreboard */}
        <PnlScoreboard days={7} />
        <PortfolioChangeChart acctSummary={acctSummary} />
        <HoldingsChangeBarChart />
        <DailyPortfolioValueChart acctSummary={acctSummary} />
        <TxnHistoryCSVViewer />

        <LiveLogsCopyViewer logs={logHistory} />

        <View style={styles.card}>
          <Text style={styles.cardTitle}>Scan summary</Text>
          <View style={styles.scanRow}>
            <View style={styles.scanItem}>
              <Text style={styles.scanLabel}>Ready</Text>
              <Text style={styles.scanValue}>{scanStats.ready}</Text>
            </View>
            <View style={styles.scanItem}>
              <Text style={styles.scanLabel}>Sent</Text>
              <Text style={styles.scanValue}>{scanStats.attemptsSent}</Text>
            </View>
            <View style={styles.scanItem}>
              <Text style={styles.scanLabel}>Failed</Text>
              <Text style={styles.scanValue}>{scanStats.attemptsFailed}</Text>
            </View>
            <View style={styles.scanItem}>
              <Text style={styles.scanLabel}>Open</Text>
              <Text style={styles.scanValue}>{scanStats.ordersOpen}</Text>
            </View>
            <View style={styles.scanItem}>
              <Text style={styles.scanLabel}>Fills</Text>
              <Text style={styles.scanValue}>{scanStats.fillsCount}</Text>
            </View>
            <View style={styles.scanItem}>
              <Text style={styles.scanLabel}>Watch</Text>
              <Text style={styles.scanValue}>{scanStats.watch}</Text>
            </View>
          </View>
          {!!scanStats?.reasons && Object.keys(scanStats.reasons).length > 0 && (
            <>
              <View style={styles.line} />
              <Text style={styles.subtle}>Skipped by reason:</Text>
              {Object.entries(scanStats.reasons).map(([k, v]) => (
                <Text key={k} style={styles.subtle}>• {k}: {v}</Text>
              ))}
            </>
          )}
        </View>

        <View style={[styles.card, { flexShrink: 0 }]}>
          <Text style={styles.cardTitle}>Live logs</Text>
          <View style={{ minHeight: Math.min(logHistory.length, LOG_UI_LIMIT) * LOG_LINE_HEIGHT }}>
            {logHistory.slice(0, LOG_UI_LIMIT).map((l, i) => (
              <Text
                key={`${l.ts}-${i}`}
                style={[
                  styles.logLine,
                  l.sev === 'success' ? styles.sevSuccess :
                  l.sev === 'warn'    ? styles.sevWarn :
                  l.sev === 'error'   ? styles.sevError : styles.sevInfo
                ]}
              >
                {new Date(l.ts).toLocaleTimeString()} • {l.text}
              </Text>
            ))}
          </View>
        </View>

        <View style={{ height: 800 }} />
      </ScrollView>
    </SafeAreaView>
  );
}

/* ─────────────────────────────── 27) STYLES ─────────────────────────────── */
const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: '#e0f8ff' },
  container: { flexGrow: 1, paddingTop: 8, paddingHorizontal: 10, backgroundColor: '#e0f8ff' },
  containerDark: { backgroundColor: '#e0f8ff' },

  header: { alignItems: 'center', justifyContent: 'center', marginBottom: 6, marginTop: 6 },
  headerTopRow: { flexDirection: 'row', alignItems: 'center' },
  statusDot: { width: 8, height: 8, borderRadius: 4, marginRight: 6 },
  appTitle: { fontSize: 16, fontWeight: '800', color: '#355070' },
  versionTag: { marginLeft: 8, color: '#9f8ed7', fontWeight: '800', fontSize: 10 },
  subTitle: { marginTop: 2, fontSize: 11, color: '#657788' },
  titleDark: { color: '#355070' },
  dot: { color: '#657788', fontWeight: '800' },
  topBanner: { marginTop: 6, paddingVertical: 6, paddingHorizontal: 10, backgroundColor: '#ead5ff', borderRadius: 8, width: '100%' },
  topBannerText: { color: '#355070', textAlign: 'center', fontWeight: '700', fontSize: 12 },

  toolbar: { backgroundColor: '#e9faff', padding: 6, borderRadius: 8, marginBottom: 8 },
  toolbarDark: { backgroundColor: '#d7ecff' },
  topControlRow: { flexDirection: 'row', alignItems: 'center', flexWrap: 'wrap', gap: 8, justifyContent: 'space-between' },
  pillToggle: { backgroundColor: '#b29cd4', paddingVertical: 6, paddingHorizontal: 8, borderRadius: 8 },
  pillNeutral: { backgroundColor: '#c5dbee' },
  btnWarn: { backgroundColor: '#f7b801' },
  pillText: { color: '#355070', fontSize: 11, fontWeight: '800' },
  inlineBP: { flexDirection: 'row', alignItems: 'center', gap: 8, marginLeft: 'auto' },
  bpLabel: { fontSize: 11, fontWeight: '600', color: '#657788' },
  bpValue: { fontSize: 13, fontWeight: '800', color: '#355070' },
  dayBadge: { fontWeight: '800', color: '#355070' },
  badgeUpdating: { fontSize: 10, color: '#657788', fontWeight: '600' },

  card: { backgroundColor: '#ffffff', borderRadius: 10, padding: 10, marginBottom: 8 },
  cardTitle: { color: '#355070', fontWeight: '800', fontSize: 12, marginBottom: 6 },
  rowSpace: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6, flexWrap: 'wrap' },
  label: { color: '#657788', fontSize: 11, fontWeight: '600' },
  value: { color: '#355070', fontSize: 13, fontWeight: '800' },
  subtle: { color: '#657788', fontSize: 11 },
  smallNote: { color: '#657788', fontSize: 10, marginTop: 6 },
  line: { height: 1, backgroundColor: '#cde6f3', marginVertical: 6, borderRadius: 999 },

  chip: { backgroundColor: '#e0d8f6', paddingVertical: 6, paddingHorizontal: 8, borderRadius: 8, marginRight: 6, marginBottom: 6 },
  chipText: { color: '#355070', fontSize: 11, fontWeight: '800' },
  bumpGroup: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  bumpBtn: { backgroundColor: '#dcd3f7', paddingHorizontal: 10, paddingVertical: 4, borderRadius: 6 },
  bumpBtnText: { color: '#355070', fontWeight: '800' },

  grid2: { flexDirection: 'row', gap: 8 },
  statBox: { flex: 1, backgroundColor: '#e9faff', borderRadius: 8, padding: 10 },

  logLine: { fontSize: 11, marginBottom: 2 },
  sevInfo: { color: '#657788' },
  sevSuccess: { color: '#7fd180' },
  sevWarn: { color: '#f7b801' },
  sevError: { color: '#f37b7b' },

  txnBox: { backgroundColor: '#ffffff', borderRadius: 10, padding: 10, marginBottom: 8 },
  txnTitle: { color: '#355070', fontWeight: '800', fontSize: 12, marginBottom: 6 },
  txnBtnRow: { flexDirection: 'row', gap: 8, marginBottom: 8, flexWrap: 'wrap' },
  txnBtn: { backgroundColor: '#e0d8f6', paddingVertical: 6, paddingHorizontal: 8, borderRadius: 8 },
  txnBtnText: { color: '#355070', fontSize: 11, fontWeight: '800' },
  txnStatus: { color: '#657788', marginTop: 4, fontSize: 11 },
  csvHelp: { color: '#657788', fontSize: 11 },
  csvBox: {
    backgroundColor: '#e0f8ff',
    color: '#355070',
    borderRadius: 8,
    padding: 8,
    height: 240,
    fontSize: 12,
    fontFamily: Platform.OS === 'ios' ? 'Menlo' : 'monospace',
  },

  riskBar: { flexDirection: 'row', alignItems: 'center', justifyContent: 'center', marginTop: 6, marginBottom: 6 },
  riskIconWrapper: { padding: 4, borderRadius: 6, marginHorizontal: 4, backgroundColor: '#e0d8f6' },
  riskIconWrapperActive: { backgroundColor: '#c9b2e6' },
  riskIcon: { fontSize: 18, color: '#5e54a0' },
  riskIconActive: { color: '#3f327f' },

  healthIconsRow: { flexDirection: 'row', justifyContent: 'space-evenly', alignItems: 'center', marginBottom: 4 },
  healthIconItem: { alignItems: 'center', justifyContent: 'center', paddingHorizontal: 8 },
  healthIcon: { fontSize: 20, color: '#645fa0' },
  healthIconLabel: { color: '#657788', fontSize: 10, marginTop: 2 },

  scanRow: { flexDirection: 'row', justifyContent: 'space-between', marginBottom: 6 },
  scanItem: { flex: 1, alignItems: 'center' },
  scanLabel: { color: '#657788', fontSize: 11, fontWeight: '600' },
  scanValue: { color: '#355070', fontSize: 14, fontWeight: '800', marginTop: 2 },
  riskHealthRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'center', marginTop: 6, marginBottom: 6 },
  riskIconGroup: { flexDirection: 'row', alignItems: 'center', marginRight: 16 },
  healthIconGroup: { flexDirection: 'row', alignItems: 'center' },

  holdRow: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  holdLabel: { width: 72, color: '#355070', fontWeight: '800', fontSize: 11, textAlign: 'right' },
  legendSwatch: { width: 10, height: 10, borderRadius: 2, },
  holdBarWrap: {
    flex: 1,
    height: 12,
    backgroundColor: '#121212', /* mimic chart #1 dark track */
    borderRadius: 6,
    overflow: 'hidden',
    flexDirection: 'row',
    alignItems: 'center',
    position: 'relative',
  },
  holdFill: { height: '100%' },
  holdPct: { width: 70, textAlign: 'right', fontSize: 11, fontWeight: '800' },

  legendRow: { flexDirection: 'row', justifyContent: 'space-between', marginTop: 6 },
  coachCard: {
    backgroundColor: '#0f1a12',
    borderColor: '#214d32',
    borderWidth: 1,
    borderRadius: 10,
    padding: 10,
    marginTop: 8,
  },
  coachTitle: { color: '#e6f8ee', fontWeight: '800', fontSize: 12, marginLeft: 6 },
  coachText: { color: '#b7e1c8', fontSize: 11, marginTop: 2 },
  coachBtn: { backgroundColor: '#1d3a29', paddingVertical: 6, paddingHorizontal: 10, borderRadius: 8 },
  coachBadge: { fontSize: 16 },
});
