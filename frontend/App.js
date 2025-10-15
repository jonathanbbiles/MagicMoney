// app.js
import React, { useState, useEffect, useRef } from 'react';
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
import Constants from 'expo-constants';

/* ──────────────────────────────── 1) VERSION / CONFIG ──────────────────────────────── */
const VERSION = 'v1';
const EX = Constants?.expoConfig?.extra || Constants?.manifest?.extra || {};

// (per user request: do not change keys)
const ALPACA_KEY = 'AKANN0IP04IH45Z6FG3L';
const ALPACA_SECRET = 'qvaKRqP9Q3XMVMEYqVnq2BEgPGhQQQfWg1JT7bWV';
// Force LIVE trading endpoint, ignoring EX/APCA overrides
const ALPACA_BASE_URL = EX.APCA_API_BASE || 'https://api.alpaca.markets/v2';

const DATA_ROOT_CRYPTO = 'https://data.alpaca.markets/v1beta3/crypto';
// IMPORTANT: your account supports 'us' for crypto data. Do not call 'global' to avoid 400s.
const DATA_LOCATIONS = ['us'];
const DATA_ROOT_STOCKS_V2 = 'https://data.alpaca.markets/v2/stocks';

const HEADERS = {
  'APCA-API-KEY-ID': ALPACA_KEY,
  'APCA-API-SECRET-KEY': ALPACA_SECRET,
  Accept: 'application/json',
  'Content-Type': 'application/json',
};
// Safety guard: crash loudly if a paper endpoint is ever used
if (typeof ALPACA_BASE_URL === 'string' && /paper-api\.alpaca\.markets/i.test(ALPACA_BASE_URL)) {
  throw new Error('Paper trading endpoint detected. LIVE ONLY is enforced. Fix ALPACA_BASE_URL.');
}
console.log('[Alpaca ENV]', { base: ALPACA_BASE_URL, mode: 'LIVE' });

getAccountSummaryRaw().then(() => {
  console.log('✅ Connected to Alpaca LIVE endpoint:', ALPACA_BASE_URL);
}).catch((e) => {
  console.log('❌ Alpaca connection error:', e?.message || e);
});

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
const STABLES = new Set(['USDTUSD', 'USDCUSD']);
// You can remove SHIBUSD from blacklist if you want to include it despite tiny tick size.
const BLACKLIST = new Set(['SHIBUSD']);
const MIN_PRICE_FOR_TICK_SANE_USD = 0.001; // keep low, but still skip micro-price assets
const DUST_FLATTEN_MAX_USD = 0.75;
const DUST_SWEEP_MINUTES = 12;
const MIN_BID_SIZE_LOOSE = 1;
const MIN_BID_NOTIONAL_LOOSE_USD = 5; // gate by ~$ value, not raw size

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
  spreadMaxBps: 120,
  spreadOverFeesMinBps: 5,
  dynamicMinProfitBps: 60,
  extraOverFeesBps: 10,
  netMinProfitBps: 2.0,
  minPriceUsd: 0.001,
  slipBpsByRisk: [1, 2, 3, 4, 5],

  // Quote handling
  liveRequireQuote: true, // live quotes only; no synthetic fallback unless user disables this
  quoteTtlMs: 15000,
  liveFreshMsCrypto: 15000,
  liveFreshMsStock: 15000,
  liveFreshTradeMsCrypto: 180000,
  syntheticTradeSpreadBps: 12,

  // Momentum filter
  enforceMomentum: true,

  // Entry / exit behavior
  enableTakerFlip: false,
  takerExitOnTouch: true,
  takerExitGuard: 'min',
  makerCampSec: 18,
  touchTicksRequired: 2,
  touchFlipTimeoutSec: 8,
  maxHoldMin: 20,
  maxTimeLossUSD: -5.0,

  // Stops / trailing
  enableStops: true,
  stopLossBps: 80,
  hardStopLossPct: 1.8,
  stopGraceSec: 10, // NEW
  enableTrailing: true,
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
  requireSpreadOverFees: true,

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
  autoTuneMinNetMinBps: 1.0,
};
let SETTINGS = { ...DEFAULT_SETTINGS };

// Minimal UI switch: show only Gate settings inside Settings panel.
const SIMPLE_SETTINGS_ONLY = true;

// Per-symbol overrides live here. Example: { SOLUSD: { spreadMaxBps: 130 } }
let SETTINGS_OVERRIDES = {};

// Effective setting helper: tries per-symbol override, falls back to global setting
function eff(symbol, key) {
  const o = SETTINGS_OVERRIDES?.[symbol];
  const v = o && Object.prototype.hasOwnProperty.call(o, key) ? o[key] : undefined;
  return v != null ? v : SETTINGS[key];
}

/* ─────────────────────────────── 4) UTILITIES / HTTP ─────────────────────────────── */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --- Global request rate limiter (simple token bucket) ---
let __TOKENS = 180; // ~180 req/min default budget
let __LAST_REFILL = Date.now();
const __REFILL_RATE = 180 / 60000; // tokens/ms
const __MAX_TOKENS = 180;
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
        await sleep(400 * Math.pow(2, i) + Math.floor(Math.random() * 300));
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

/* ─────────────────────────── 5) TIME / PARSE / FORMATTING ─────────────────────────── */
const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const fmtUSD = (n) =>
  Number.isFinite(n)
    ? `$ ${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
    : '—';
const fmtPct = (n) => (Number.isFinite(n) ? `${n.toFixed(2)}%` : '—');

const parseTsMs = (t) => {
  if (t == null) return NaN;
  if (typeof t === 'string') {
    const ms = Date.parse(t);
    if (Number.isFinite(ms)) return ms;
    const n = +t;
    if (Number.isFinite(n)) {
      if (n > 1e15) return Math.floor(n / 1e6);
      if (n > 1e12) return Math.floor(n / 1e3);
      if (n > 1e10) return n;
      if (n > 1e9) return n * 1000;
    }
    return NaN;
  }
  if (typeof t === 'number') {
    if (t > 1e15) return Math.floor(t / 1e6);
    if (t > 1e12) return Math.floor(t / 1e3);
    if (t > 1e10) return t;
    if (t > 1e9) return t * 1000;
    return t;
  }
  return NaN;
};
const isFresh = (tsMs, ttlMs) => Number.isFinite(tsMs) && (Date.now() - tsMs <= ttlMs);

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
function toDataSymbol(sym) {
  if (!sym) return sym;
  if (sym.includes('/')) return sym;
  if (sym.endsWith('USD')) {
    const base = sym.slice(0, -3);
    if (!base || base.toUpperCase().endsWith('USD')) return `${sym}/USD`;
    return `${base}/USD`;
  }
  return sym;
}
function isCrypto(sym) { return /USD$/.test(sym); }
function isStock(sym) { return !isCrypto(sym); }

function synthQuoteFromTrade(price, bps = SETTINGS.syntheticTradeSpreadBps) {
  if (!(price > 0)) return null;
  const half = price * (bps / 20000);
  return { bid: price - half, ask: price + half, bs: null, as: null, tms: Date.now() };
}

/* ───────────────────────── 7) ACCOUNT / HISTORY / ACTIVITIES ───────────────────────── */
async function getPortfolioHistory({ period = '1M', timeframe = '1D' } = {}) {
  const url = `${ALPACA_BASE_URL}/account/portfolio/history?period=${encodeURIComponent(period)}&timeframe=${encodeURIComponent(timeframe)}&extended_hours=true`;
  const res = await f(url, { headers: HEADERS });
  if (!res.ok) return null;
  return res.json().catch(() => null);
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

  const url = `${ALPACA_BASE_URL}/account/activities?${params.toString()}`;
  const res = await f(url, { headers: HEADERS });
  let items = [];
  try { items = await res.json(); } catch {}
  const next = res.headers?.get?.('x-next-page-token') || null;
  return { items: Array.isArray(items) ? items : [], next };
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
    const r = await f(`${ALPACA_BASE_URL}/clock`, { headers: HEADERS });
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

/* ───────────────────────────── 10) STATIC UNIVERSES ───────────────────────────── */
const ORIGINAL_TOKENS = [
  { name: 'ETH/USD',  symbol: 'ETHUSD',  cc: 'ETH'  },
  { name: 'AAVE/USD', symbol: 'AAVEUSD', cc: 'AAVE' },
  { name: 'LTC/USD',  symbol: 'LTCUSD',  cc: 'LTC'  },
  { name: 'LINK/USD', symbol: 'LINKUSD', cc: 'LINK' },
  { name: 'UNI/USD',  symbol: 'UNIUSD',  cc: 'UNI'  },
  { name: 'SOL/USD',  symbol: 'SOLUSD',  cc: 'SOL'  },
  { name: 'BTC/USD',  symbol: 'BTCUSD',  cc: 'BTC'  },
  { name: 'AVAX/USD', symbol: 'AVAXUSD', cc: 'AVAX' },
  { name: 'ADA/USD',  symbol: 'ADAUSD',  cc: 'ADA'  },
  { name: 'MATIC/USD',symbol: 'MATICUSD',cc: 'MATIC'},
  { name: 'XRP/USD',  symbol: 'XRPUSD',  cc: 'XRP'  },
  { name: 'SHIB/USD', symbol: 'SHIBUSD', cc: 'SHIB' },
  { name: 'BCH/USD',  symbol: 'BCHUSD',  cc: 'BCH'  },
  { name: 'ETC/USD',  symbol: 'ETCUSD',  cc: 'ETC'  },
  { name: 'TRX/USD',  symbol: 'TRXUSD',  cc: 'TRX'  },
  { name: 'USDT/USD', symbol: 'USDTUSD', cc: 'USDT' },
  { name: 'USDC/USD', symbol: 'USDCUSD', cc: 'USDC' },
];
const CRYPTO_CORE_TRACKED = ORIGINAL_TOKENS.filter(t => !STABLES.has(t.symbol));

const TRAD_100 = [];
const CRYPTO_STOCKS_100 = [];
const STATIC_UNIVERSE = Array.from(
  new Map([...TRAD_100, ...CRYPTO_STOCKS_100].map((s) => [s, { name: s, symbol: s, cc: null }])).values()
);

/* ───────────────────────── 11) QUOTE CACHE / SUPPORT FLAGS ───────────────────────── */
const quoteCache = new Map();
const unsupportedSymbols = new Map();
const isUnsupported = (sym) => {
  const u = unsupportedSymbols.get(sym);
  if (!u) return false;
  if (Date.now() > u) { unsupportedSymbols.delete(sym); return false; }
  return true;
};
function markUnsupported(sym, mins = 120) { unsupportedSymbols.set(sym, Date.now() + mins * 60000); }

// -------------------------------------------------------------------------
// BAR CACHE
const barsCache = new Map();
const barsCacheTTL = 30000; // 30 seconds

/* ─────────────────────────────── 12) CRYPTO DATA API ─────────────────────────────── */
const buildURLCrypto = (loc, what, symbolsCSV, params = {}) => {
  const encoded = symbolsCSV.split(',').map((s) => encodeURIComponent(s)).join(',');
  const sp = new URLSearchParams();
  Object.entries(params || {}).forEach(([k, v]) => { if (v != null) sp.set(k, v); });
  const qs = sp.toString();
  return `${DATA_ROOT_CRYPTO}/${loc}/latest/${what}?symbols=${encoded}${qs ? '&' + qs : ''}`;
};

async function getCryptoQuotesBatch(dsyms = []) {
  if (!dsyms.length) return new Map();
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildURLCrypto(loc, 'quotes', dsyms.join(','));
      const r = await f(url, { headers: HEADERS });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        logTradeAction('quote_http_error', 'QUOTE', { status: r.status, loc, body: body?.slice?.(0, 120) });
        continue;
      }
      const j = await r.json().catch(() => null);
      const raw = j?.quotes || {};
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
          out.set(dsym, { bid, ask, bs: Number.isFinite(bs) ? bs : null, as: Number.isFinite(as) ? as : null, tms });
        }
      }
      if (out.size) return out;
    } catch (e) {
      logTradeAction('quote_http_error', 'QUOTE', { status: 'exception', loc, body: e?.message || '' });
    }
  }
  return new Map();
}
async function getCryptoTradesBatch(dsyms = []) {
  if (!dsyms.length) return new Map();
  for (const loc of DATA_LOCATIONS) {
    try {
      const url = buildURLCrypto(loc, 'trades', dsyms.join(','));
      const r = await f(url, { headers: HEADERS });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        logTradeAction('trade_http_error', 'TRADE', { status: r.status, loc, body: body?.slice?.(0, 120) });
        continue;
      }
      const j = await r.json().catch(() => null);
      const raw = j?.trades || {};
      const out = new Map();
      for (const dsym of dsyms) {
        const t = Array.isArray(raw[dsym]) ? raw[dsym][0] : raw[dsym];
        const p = Number(t?.p ?? t?.price);
        const tms = parseTsMs(t?.t);
        if (Number.isFinite(p) && p > 0) out.set(dsym, { price: p, tms });
      }
      if (out.size) return out;
    } catch (e) {
      logTradeAction('trade_http_error', 'TRADE', { status: 'exception', loc, body: e?.message || '' });
    }
  }
  return new Map();
}
async function getCryptoBars1m(symbol, limit = 6) {
  const dsym = toDataSymbol(symbol);
  const cached = barsCache.get(dsym);
  const now = Date.now();
  if (cached && (now - cached.ts) < barsCacheTTL) {
    return cached.bars.slice(0, limit);
  }
  for (const loc of DATA_LOCATIONS) {
    try {
      const sp = new URLSearchParams({ timeframe: '1Min', limit: String(limit), symbols: dsym });
      const url = `${DATA_ROOT_CRYPTO}/${loc}/bars?${sp.toString()}`;
      const r = await f(url, { headers: HEADERS });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        logTradeAction('quote_http_error', 'BARS', { status: r.status, loc, body: body?.slice?.(0, 120) });
        continue;
      }
      const j = await r.json().catch(() => null);
      const arr = j?.bars?.[dsym];
      if (Array.isArray(arr) && arr.length) {
        const bars = arr.map((b) => ({
          open: Number(b.o ?? b.open),
          high: Number(b.h ?? b.high),
          low: Number(b.l ?? b.low),
          close: Number(b.c ?? b.close),
          vol: Number(b.v ?? b.volume ?? 0),
          tms: parseTsMs(b.t),
        })).filter((x) => Number.isFinite(x.close) && x.close > 0);
        barsCache.set(dsym, { ts: now, bars: bars.slice() });
        return bars;
      }
    } catch (e) {
      logTradeAction('quote_http_error', 'BARS', { status: 'exception', loc, body: e?.message || '' });
    }
  }
  return [];
}

async function getCryptoBars1mBatch(symbols = [], limit = 6) {
  const uniqSyms = Array.from(new Set(symbols.filter(Boolean)));
  if (!uniqSyms.length) return new Map();
  const dsymList = uniqSyms.map((s) => toDataSymbol(s));
  const out = new Map();
  const now = Date.now();

  const missing = [];
  for (const dsym of dsymList) {
    const cached = barsCache.get(dsym);
    if (cached && (now - cached.ts) < barsCacheTTL) {
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
      const r = await f(url, { headers: HEADERS });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        logTradeAction('quote_http_error', 'BARS', { status: r.status, loc, body: body?.slice?.(0, 120) });
        continue;
      }
      const j = await r.json().catch(() => null);
      const raw = j?.bars || {};
      for (const dsym of missing) {
        const arr = raw[dsym];
        if (Array.isArray(arr) && arr.length) {
          const bars = arr.map((b) => ({
            open: Number(b.o ?? b.open),
            high: Number(b.h ?? b.high),
            low: Number(b.l ?? b.low),
            close: Number(b.c ?? b.close),
            vol: Number(b.v ?? b.volume ?? 0),
            tms: parseTsMs(b.t),
          })).filter((x) => Number.isFinite(x.close) && x.close > 0);
          barsCache.set(dsym, { ts: now, bars: bars.slice() });
          out.set(dsym.replace('/', ''), bars.slice(0, limit));
        }
      }
      break;
    } catch (e) {
      logTradeAction('quote_http_error', 'BARS', { status: 'exception', loc, body: e?.message || '' });
    }
  }
  return out;
}

/* ──────────────────────────────── 13) STOCKS DATA API ─────────────────────────────── */
async function stocksLatestQuotesBatch(symbols = []) {
  if (!symbols.length) return new Map();
  const csv = symbols.join(',');
  try {
    const r = await f(`${DATA_ROOT_STOCKS_V2}/quotes/latest?symbols=${encodeURIComponent(csv)}`, { headers: HEADERS });
    if (!r.ok) return new Map();
    const j = await r.json().catch(() => null);
    const out = new Map();
    for (const sym of symbols) {
      const qraw = j?.quotes?.[sym];
      const q = Array.isArray(qraw) ? qraw[0] : qraw;
      if (!q) continue;
      const bid = Number(q.bp ?? q.bid_price);
      const ask = Number(q.ap ?? q.ask_price);
      const bs = Number(q.bs ?? q.bid_size);
      const as = Number(q.as ?? q.ask_size);
      const tms = parseTsMs(q.t);
      if (bid > 0 && ask > 0) out.set(sym, { bid, ask, bs: Number.isFinite(bs) ? bs : null, as: Number.isFinite(as) ? as : null, tms });
    }
    return out;
  } catch { return new Map(); }
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
        if (Number.isFinite(d.freshSec)) return ` (fresh≤${Math.round(d.freshSec)}s)`;
        return '';
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

const logTradeAction = async (type, symbol, details = {}) => {
  const timestamp = new Date().toISOString();
  const entry = { timestamp, type, symbol, ...details };
  logBuffer.push(entry);
  if (logBuffer.length > MAX_LOGS) logBuffer.shift();
  if (typeof logSubscriber === 'function') {
    try { logSubscriber(entry); } catch {}
  }
};
const FRIENDLY = {
  quote_ok: { sev: 'info', msg: (d) => `Quote OK (${(d.spreadBps ?? 0).toFixed(1)} bps)` },
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
  scan_summary: { sev: 'info', msg: (d) => `Scan: ready ${d.readyCount} / attempts ${d.attemptCount} / fills ${d.successCount}` },
  scan_error: { sev: 'error', msg: (d) => `Scan error: ${d.error}` },
  skip_wide_spread: { sev: 'warn', msg: (d) => `Skip: spread ${d.spreadBps} bps > max` },
  skip_small_order: { sev: 'warn', msg: () => `Skip: below min notional or funding` },
  entry_skipped: { sev: 'info', msg: (d) => `Skip — ${d.reason}${fmtSkipDetail(d.reason, d)}` },
  risk_changed: { sev: 'info', msg: (d) => `Risk→${d.level} (spread≤${d.spreadMax}bps)` },
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
  const cryptos = symbols.filter((s) => isCrypto(s));
  const stocks = symbols.filter((s) => isStock(s));
  const out = new Map();
  const now = Date.now();

  if (cryptos.length) {
    const dsyms = Array.from(new Set(cryptos.map((s) => toDataSymbol(s)))).filter((dsym) => !isUnsupported(dsym.replace('/', '')));
    for (let i = 0; i < dsyms.length; i += 6) {
      const slice = dsyms.slice(i, i + 6);
      let qmap = await getCryptoQuotesBatch(slice);
      for (const dsym of slice) {
        const q = qmap.get(dsym);
        if (!q) continue;
        const fresh = isFresh(q.tms, SETTINGS.liveFreshMsCrypto);
        if (!fresh) continue;
        const sym = dsym.replace('/', '');
        out.set(sym, { bid: q.bid, ask: q.ask, bs: q.bs, as: q.as, tms: q.tms });
        quoteCache.set(sym, { ts: now, q: { bid: q.bid, ask: q.ask, bs: q.bs, as: q.as } });
      }
    }
  }

  if (stocks.length) {
    // no-op (crypto-only build keeps structure intact)
  }
  return out;
}

/* ─────────────────────────────── 17) SMART QUOTE ─────────────────────────────── */
async function getQuoteSmart(symbol, preloadedMap = null) {
  try {
    if (isUnsupported(symbol)) return null;

    // Always use cached real quotes if within TTL
    {
      const c = quoteCache.get(symbol);
      if (c && Date.now() - c.ts < SETTINGS.quoteTtlMs) return c.q;
    }

    // Use preloaded map if available and fresh
    if (preloadedMap && preloadedMap.has(symbol)) {
      const q = preloadedMap.get(symbol);
      if (q && isFresh(q.tms, SETTINGS.liveFreshMsCrypto)) return q;
    }

    const dsym = toDataSymbol(symbol);

    // Try to fetch a real quote
    const m = await getCryptoQuotesBatch([dsym]);
    const q0 = m.get(dsym);
    if (q0 && isFresh(q0.tms, SETTINGS.liveFreshMsCrypto)) {
      const qObj = { bid: q0.bid, ask: q0.ask, bs: q0.bs, as: q0.as, tms: q0.tms };
      quoteCache.set(symbol, { ts: Date.now(), q: qObj });
      return qObj;
    }

    // Fallback: synthesize a quote from last trade if allowed
    if (!SETTINGS.liveRequireQuote) {
      const tm = await getCryptoTradesBatch([dsym]);
      const t = tm.get(dsym);
      if (t && isFresh(t.tms, SETTINGS.liveFreshTradeMsCrypto)) {
        const synth = synthQuoteFromTrade(t.price, SETTINGS.syntheticTradeSpreadBps);
        if (synth) {
          quoteCache.set(symbol, { ts: Date.now(), q: synth });
          return synth;
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
const getPositionInfo = async (symbol) => {
  try {
    const res = await f(`${ALPACA_BASE_URL}/positions/${symbol}`, { headers: HEADERS });
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
    const r = await f(`${ALPACA_BASE_URL}/positions`, { headers: HEADERS });
    if (!r.ok) return [];
    const arr = await r.json();
    return Array.isArray(arr) ? arr : [];
  } catch { return []; }
};
const getOpenOrders = async () => {
  try {
    const r = await f(`${ALPACA_BASE_URL}/orders?status=open&nested=true&limit=100`, { headers: HEADERS });
    if (!r.ok) return [];
    const arr = await r.json();
    return Array.isArray(arr) ? arr : [];
  } catch { return []; }
};
let __openOrdersCache = { ts: 0, items: [] };
async function getOpenOrdersCached(ttlMs = 2000) {
  const now = Date.now();
  if (now - __openOrdersCache.ts < ttlMs) return __openOrdersCache.items.slice();
  const items = await getOpenOrders();
  __openOrdersCache = { ts: now, items };
  return items.slice();
}

let __positionsCache = { ts: 0, items: [] };
async function getAllPositionsCached(ttlMs = 2000) {
  const now = Date.now();
  if (now - __positionsCache.ts < ttlMs) return __positionsCache.items.slice();
  const items = await getAllPositions();
  __positionsCache = { ts: now, items };
  return items.slice();
}
const cancelOpenOrdersForSymbol = async (symbol, side = null) => {
  try {
    const open = await getOpenOrdersCached();
    const targets = (open || []).filter(
      (o) => o.symbol === symbol && (!side || (o.side || '').toLowerCase() === String(side).toLowerCase())
    );
    await Promise.all(
      targets.map((o) =>
        f(`${ALPACA_BASE_URL}/orders/${o.id}`, { method: 'DELETE', headers: HEADERS }).catch(() => null)
      )
    );
    __openOrdersCache = { ts: 0, items: [] };
  } catch {}
};
const cancelAllOrders = async () => {
  try {
    const orders = await getOpenOrdersCached();
    await Promise.all((orders || []).map((o) => f(`${ALPACA_BASE_URL}/orders/${o.id}`, { method: 'DELETE', headers: HEADERS }).catch(() => null)));
    __openOrdersCache = { ts: 0, items: [] };
  } catch {}
};

async function getAccountSummaryRaw() {
  const res = await f(`${ALPACA_BASE_URL}/account`, { headers: HEADERS });
  if (!res.ok) throw new Error(`Account ${res.status}`);
  const a = await res.json();
  const num = (x) => { const n = parseFloat(x); return Number.isFinite(n) ? n : NaN; };

  const equity = num(a.equity ?? a.portfolio_value);

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
    buyingPower: buyingPowerDisplay,
    changeUsd, changePct,
    patternDayTrader, daytradeCount,
    cryptoBuyingPower: cashish,
    stockBuyingPower: Number.isFinite(stockBP) ? stockBP : cashish,
    daytradeBuyingPower: dtbp,
    cash
  };
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
        await f(`${ALPACA_BASE_URL}/orders/${o.id}`, { method: 'DELETE', headers: HEADERS }).catch(() => null);
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
  const seededRef = useRef(false);

  useEffect(() => {
    (async () => {
      if (seededRef.current) return;
      seededRef.current = true;
      try {
        const hist = await getPortfolioHistory({ period: '1D', timeframe: '5Min' });
        if (!hist) return;

        const ts = (hist.timestamp || hist.timestamps || []).map((t) => parseTsMs(t)).filter(Number.isFinite);
        const rawPct = (hist.profit_loss_pct || hist.pnl_pct || []).map((x) => +x);
        let seeded = [];
        if (rawPct.length && ts.length) {
          // Alpaca returns decimals (e.g., 0.0048 for 0.48%). Convert to percent.
          seeded = rawPct.map((v, i) => ({
            t: ts[i] || (Date.now() - (rawPct.length - i) * 300000),
            pct: v * 100,
          }));
        } else if (Array.isArray(hist.equity)) {
          const eq = hist.equity.map((x) => +x).filter(Number.isFinite);
          const base = Number.isFinite(+hist.base_value) ? +hist.base_value : (eq[0] || 1);
          seeded = eq.map((e, i) => ({
            t: ts[i] || (Date.now() - (eq.length - i) * 300000),
            pct: ((e / base) - 1) * 100,
          }));
        }
        if (seeded.length) setPts(seeded.slice(-200));
      } catch {}
    })();
  }, []);

  useEffect(() => {
    const v = Number(acctSummary?.dailyChangePct);
    if (Number.isFinite(v)) {
      setPts((prev) => {
        const next = [...prev, { t: Date.now(), pct: v }];
        return next.length > 200 ? next.slice(next.length - 200) : next;
      });
    }
  }, [acctSummary?.dailyChangePct]);

  if (!pts.length) {
    return (
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Portfolio % Change (Today)</Text>
        <View style={{ height: 100, borderRadius: 8, backgroundColor: '#e0f8ff' }} />
        <Text style={styles.smallNote}>Waiting for account data…</Text>
      </View>
    );
  }

  const last = pts[pts.length - 1];

  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>Portfolio % Change (Today)</Text>
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
      <Text style={styles.smallNote}>
        Intraday % vs previous close. Seeded from <Text style={{ fontWeight: '800' }}>/account/portfolio/history</Text> (5‑min). P/L is calculated as equity/base_value − 1.
      </Text>
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
        <Text style={styles.cardTitle}>Portfolio Value (Daily)</Text>
        <Text style={styles.smallNote}>No history yet from /account/portfolio/history (1‑D).</Text>
      </View>
    );
  }

  const last = pts[pts.length - 1];

  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>Portfolio Value (Daily)</Text>
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
      <Text style={styles.smallNote}>
        Latest point = current equity; earlier points from <Text style={{ fontWeight: '800' }}>/account/portfolio/history</Text> (1‑D).
      </Text>
    </View>
  );
};

/* ─────────────────────────── 24) ENTRY / ORDERING / EXITS ─────────────────────────── */
async function fetchAssetMeta(symbol) {
  try {
    const r = await f(`${ALPACA_BASE_URL}/assets/${encodeURIComponent(symbol)}`, { headers: HEADERS });
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
async function placeMakerThenMaybeTakerBuy(symbol, qty, preQuoteMap = null, usableBP = null) {
  await cancelOpenOrdersForSymbol(symbol, 'buy');
  const meta = await fetchAssetMeta(symbol);
  const rawTick = meta?._price_inc ?? (isStock(symbol) ? 0.01 : 1e-5);
  const TICK = Number.isFinite(rawTick) && rawTick > 0 ? rawTick : (isStock(symbol) ? 0.01 : 1e-5);
  const rawQtyInc = meta?._qty_inc ?? 0.000001;
  const QINC = Number.isFinite(rawQtyInc) && rawQtyInc > 0 ? rawQtyInc : 0.000001;
  const qtyStepDecimals = Math.min(8, (QINC.toString().split('.')[1] || '').length || 6);
  const quantizeQty = (value) => {
    if (!(value > 0) || !(QINC > 0)) return 0;
    const scaled = Math.floor(value / QINC + 1e-9);
    if (!(scaled > 0)) return 0;
    return Number((scaled * QINC).toFixed(qtyStepDecimals));
  };
  let plannedQty = quantizeQty(qty);
  if (isStock(symbol) && meta && meta._fractionable === false) {
    plannedQty = Math.floor(plannedQty);
  }
  plannedQty = quantizeQty(plannedQty);
  if (!(plannedQty > 0)) return { filled: false };
  qty = plannedQty;

  let lastOrderId = null, placedLimit = null, lastReplaceAt = 0;
  const t0 = Date.now(), CAMP_SEC = SETTINGS.makerCampSec;
  const tickDecimals = (() => {
    const frac = (TICK.toString().split('.')[1] || '');
    if (!frac.length) return isStock(symbol) ? 2 : 5;
    return Math.min(6, Math.max(frac.length, isStock(symbol) ? 2 : 5));
  })();
  const formatLimit = (px) => Number(px).toFixed(tickDecimals);

  while ((Date.now() - t0) / 1000 < CAMP_SEC) {
    const q = await getQuoteSmart(symbol, preQuoteMap);
    if (!q) { await sleep(500); continue; }
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
      if (!(qty > 0)) return { filled: false };
    }

    const notionalPx = Number.isFinite(bidNow) && bidNow > 0
      ? bidNow
      : (Number.isFinite(askNow) && askNow > 0 ? askNow : join);
    if (meta && meta._min_notional > 0 && Number.isFinite(notionalPx) && notionalPx > 0) {
      if (qty * notionalPx < meta._min_notional) return { filled: false };
    }

    if (isStock(symbol) && meta && meta._fractionable === false) {
      const px = notionalPx;
      if (!(qty > 0) || !(px > 0) || qty * px < 5) return { filled: false };
    }

    const nowTs = Date.now();
    const ticksDrift = placedLimit != null ? Math.abs(join - placedLimit) / TICK : Infinity;
    const needReplace = !lastOrderId || ticksDrift >= 2 || join < (placedLimit - TICK);
    if (needReplace && (nowTs - lastReplaceAt) > 1800) {
      if (lastOrderId) {
        try { await f(`${ALPACA_BASE_URL}/orders/${lastOrderId}`, { method: 'DELETE', headers: HEADERS }); } catch {}
        __openOrdersCache = { ts: 0, items: [] };
      }
      const order = {
        symbol, qty, side: 'buy', type: 'limit', time_in_force: 'gtc',
        limit_price: formatLimit(join),
      };
      try {
        const res = await f(`${ALPACA_BASE_URL}/orders`, { method: 'POST', headers: HEADERS, body: JSON.stringify(order) });
        const raw = await res.text(); let data; try { data = JSON.parse(raw); } catch { data = { raw }; }
        if (res.ok && data.id) {
          lastOrderId = data.id; placedLimit = join; lastReplaceAt = nowTs;
          logTradeAction('buy_camped', symbol, { limit: order.limit_price });
          __openOrdersCache = { ts: 0, items: [] };
        }
      } catch (e) {
        logTradeAction('quote_exception', symbol, { error: e.message });
      }
    }

    const pos = await getPositionInfo(symbol);
    if (pos && pos.qty > 0) {
      if (lastOrderId) {
        try { await f(`${ALPACA_BASE_URL}/orders/${lastOrderId}`, { method: 'DELETE', headers: HEADERS }); } catch {}
        __openOrdersCache = { ts: 0, items: [] };
      }
      logTradeAction('buy_success', symbol, {
        qty: pos.qty, limit: placedLimit != null ? formatLimit(placedLimit) : placedLimit,
      });
      __positionsCache = { ts: 0, items: [] };
      __openOrdersCache = { ts: 0, items: [] };
      return { filled: true, entry: pos.basis ?? placedLimit, qty: pos.qty, liquidity: 'maker' };
    }
    await sleep(1200);
  }

  if (lastOrderId) {
    try { await f(`${ALPACA_BASE_URL}/orders/${lastOrderId}`, { method: 'DELETE', headers: HEADERS }); } catch {}
    __openOrdersCache = { ts: 0, items: [] };
    logTradeAction('buy_unfilled_canceled', symbol, {});
  }

  if (SETTINGS.enableTakerFlip) {
    const q = await getQuoteSmart(symbol, preQuoteMap);
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
        if (!(mQty > 0)) return { filled: false };
      }
      if (!(mQty > 0)) return { filled: false };
      if (meta && meta._min_notional > 0) {
        const pxRef = Number.isFinite(q.bid) && q.bid > 0 ? q.bid : q.ask;
        if (Number.isFinite(pxRef) && pxRef > 0 && mQty * pxRef < meta._min_notional) return { filled: false };
      }
      const tif = isStock(symbol) ? 'day' : 'gtc';
      const order = { symbol, qty: mQty, side: 'buy', type: 'market', time_in_force: tif };
      try {
        const res = await f(`${ALPACA_BASE_URL}/orders`, { method: 'POST', headers: HEADERS, body: JSON.stringify(order) });
        const raw = await res.text(); let data; try { data = JSON.parse(raw); } catch { data = { raw }; }
        if (res.ok && data.id) {
          logTradeAction('buy_success', symbol, { qty: mQty, limit: 'mkt' });
          __positionsCache = { ts: 0, items: [] };
          __openOrdersCache = { ts: 0, items: [] };
          return { filled: true, entry: q.ask, qty: mQty, liquidity: 'taker' };
        } else {
          logTradeAction('quote_exception', symbol, { error: `BUY mkt ${res.status} ${data?.message || data?.raw?.slice?.(0, 80) || ''}` });
        }
      } catch (e) {
        logTradeAction('quote_exception', symbol, { error: e.message });
      }
    }
  }
  return { filled: false };
}

async function marketSell(symbol, qty) {
  try { await cancelOpenOrdersForSymbol(symbol, 'sell'); } catch {}
  // Re-check availability right before selling to avoid 403s
  let latest = null;
  try { latest = await getPositionInfo(symbol); } catch {}
  const usableQty = Number(latest?.available ?? qty ?? 0);
  if (!(usableQty > 0)) {
    logTradeAction('tp_limit_error', symbol, { error: 'SELL mkt skipped — no available qty' });
    return null;
  }
  const tif = isStock(symbol) ? 'day' : 'gtc';
  const mkt = { symbol, qty: usableQty, side: 'sell', type: 'market', time_in_force: tif };
  try {
    const res = await f(`${ALPACA_BASE_URL}/orders`, { method: 'POST', headers: HEADERS, body: JSON.stringify(mkt) });
    const raw = await res.text(); let data; try { data = JSON.parse(raw); } catch { data = { raw }; }
    if (res.ok && data.id) {
      __positionsCache = { ts: 0, items: [] };
      __openOrdersCache = { ts: 0, items: [] };
      return data;
    }
    logTradeAction('tp_limit_error', symbol, { error: `SELL mkt ${res.status} ${data?.message || data?.raw?.slice?.(0, 120) || ''}` });
    return null;
  } catch (e) {
    logTradeAction('tp_limit_error', symbol, { error: `SELL mkt exception ${e.message}` });
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
  const qty = Number(posInfo?.available ?? posInfo?.qty ?? state.qty ?? 0);
  const entryPx = state.entry ?? posInfo?.basis ?? posInfo?.mark ?? 0;
  if (!(qty > 0) || !(entryPx > 0)) return false;

  const q = await getQuoteSmart(symbol);
  if (!q || !(q.bid > 0)) return false;
  const bid = q.bid;

  const ageSec = (Date.now() - (state.entryTs || 0)) / 1000;
  const mid = Number.isFinite(q.bid) && Number.isFinite(q.ask) ? 0.5 * (q.bid + q.ask) : q.bid;
  const spreadBpsNow = (Number.isFinite(q.ask) && q.ask > q.bid && mid > 0)
    ? ((q.ask - q.bid) / mid) * 10000
    : 0;

  const slipEw = (symStats[symbol]?.slipEwmaBps ?? (SETTINGS.slipBpsByRisk?.[SETTINGS.riskLevel] ?? 1));
  const effStopBps = Math.max(
    SETTINGS.stopLossBps,
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

  if (SETTINGS.enableTrailing) {
    const armPx = entryPx * (1 + SETTINGS.trailStartBps / 10000);
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
  const posInfo = pos ?? await getPositionInfo(symbol);
  if (!posInfo || posInfo.available <= 0) return;

  const state = (tradeStateRef?.current?.[symbol]) || {};
  const entryPx = state.entry ?? posInfo.basis ?? posInfo.mark ?? 0;
  const qty = Number(posInfo.available ?? posInfo.qty ?? state.qty ?? 0);
  if (!(entryPx > 0) || !(qty > 0)) return;

  const riskExited = await ensureRiskExits(symbol, { tradeStateRef, pos: posInfo });
  if (riskExited) return;

  const heldMinutes = (Date.now() - (state.entryTs || 0)) / 60000;
  if (Number.isFinite(heldMinutes) && heldMinutes >= SETTINGS.maxHoldMin) {
    try {
      const q = await getQuoteSmart(symbol);
      if (q && q.bid > 0) {
        const net = projectedNetPnlUSDWithBuy({
          symbol, entryPx, qty, sellPx: q.bid, buyBpsOverride: state.buyBpsApplied
        });
        const feeFloor = minExitPriceFeeAwareDynamic({
          symbol, entryPx, qty, buyBpsOverride: state.buyBpsApplied
        });
        const tick = isStock(symbol) ? 0.01 : 1e-5;
        const nearFeeFloor = q.bid >= (feeFloor - (2 * tick)); // prefer scratch
        if (net >= 0 || (nearFeeFloor && net >= -Math.abs(SETTINGS.netMinProfitUSD)) || net >= -Math.abs(SETTINGS.maxTimeLossUSD)) {
          try {
            const open = await getOpenOrdersCached();
            const ex = open.find((o) => (o.side || '').toLowerCase() === 'sell' && (o.type || '').toLowerCase() === 'limit' && o.symbol === symbol);
            if (ex) {
              await f(`${ALPACA_BASE_URL}/orders/${ex.id}`, { method: 'DELETE', headers: HEADERS }).catch(() => null);
              __openOrdersCache = { ts: 0, items: [] };
            }
          } catch {}
          const mkt = await marketSell(symbol, qty);
          if (mkt) {
            logTradeAction('tp_limit_set', symbol, { limit: `TIME_EXIT@~${q.bid.toFixed(isStock(symbol) ? 2 : 5)}` });
            return;
          }
        }
      }
    } catch {}
  }

  const feeFloor = minExitPriceFeeAwareDynamic({
    symbol, entryPx, qty, buyBpsOverride: state.buyBpsApplied
  });
  let finalLimit = Math.max(limitPrice, feeFloor);
  if (finalLimit > limitPrice + 1e-12) {
    logTradeAction('tp_fee_floor', symbol, { limit: finalLimit.toFixed(isStock(symbol) ? 2 : 5) });
  }

  if (SETTINGS.takerExitOnTouch) {
    const q = await getQuoteSmart(symbol);
    const memo = (touchMemoRef.current[symbol]) || (touchMemoRef.current[symbol] = { count: 0, lastTs: 0, firstTouchTs: 0 });
    if (q && q.bid > 0) {
      const touchPx = finalLimit * (1 - SELL_EPS_BPS / 10000);
      const touching = q.bid >= touchPx;

      if (touching) {
        const now = Date.now();
        memo.count = now - memo.lastTs > 2000 * 5 ? 1 : memo.count + 1;
        memo.lastTs = now;
        if (!memo.firstTouchTs) memo.firstTouchTs = now;
        const ageSec = (now - memo.firstTouchTs) / 1000;
        logTradeAction('tp_touch_tick', symbol, { count: memo.count, bid: q.bid });

        const guard = String(SETTINGS.takerExitGuard || 'fee').toLowerCase();
        const okByMin = meetsMinProfitWithBuy({
          symbol, entryPx, qty, sellPx: q.bid, buyBpsOverride: state.buyBpsApplied
        });
        const okByFee = q.bid >= feeFloor * (1 - 1e-6);
        const okProfit = guard === 'min' ? okByMin : okByFee;

        const timedForce = ageSec >= Math.max(2, SETTINGS.touchFlipTimeoutSec) && okByFee;
        if ((memo.count >= SETTINGS.touchTicksRequired && okProfit) || timedForce) {
          try {
            const open = await getOpenOrdersCached();
            const ex = open.find((o) => (o.side || '').toLowerCase() === 'sell' && (o.type || '').toLowerCase() === 'limit' && o.symbol === symbol);
            if (ex) {
              await f(`${ALPACA_BASE_URL}/orders/${ex.id}`, { method: 'DELETE', headers: HEADERS }).catch(() => null);
              __openOrdersCache = { ts: 0, items: [] };
            }
          } catch {}
          const mkt = await marketSell(symbol, qty);
          if (mkt) {
            touchMemoRef.current[symbol] = { count: 0, lastTs: 0, firstTouchTs: 0 };
            logTradeAction(timedForce ? 'taker_force_flip' : 'tp_limit_set', symbol, {
              limit: timedForce ? `FORCE@~${q.bid.toFixed?.(5) ?? q.bid}` : `TAKER@~${q.bid.toFixed?.(5) ?? q.bid}`
            });
            return;
          }
        } else if (memo.count >= SETTINGS.touchTicksRequired && !okProfit) {
          logTradeAction('taker_blocked_fee', symbol, {});
        }
      } else {
        memo.count = 0;
        memo.lastTs = Date.now();
        memo.firstTouchTs = 0;
      }
    }
  }

  const limitTIF = isStock(symbol) ? 'day' : 'gtc';
  let existing = openSellBySym?.get?.(symbol) || null;
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
    const order = { symbol, qty, side: 'sell', type: 'limit', time_in_force: limitTIF, limit_price: finalLimit.toFixed(decimals) };
    const res = await f(`${ALPACA_BASE_URL}/orders`, { method: 'POST', headers: HEADERS, body: JSON.stringify(order) });
    const raw = await res.text(); let data; try { data = JSON.parse(raw); } catch { data = { raw }; }
    if (res.ok && data.id) {
      tradeStateRef.current[symbol] = { ...(state || {}), lastLimitPostTs: now };
      if (existing) { await f(`${ALPACA_BASE_URL}/orders/${existing.id}`, { method: 'DELETE', headers: HEADERS }).catch(() => null); }
      logTradeAction('tp_limit_set', symbol, { id: data.id, limit: order.limit_price });
    } else {
      const msg = data?.message || data?.raw?.slice?.(0, 100) || '';
      logTradeAction('tp_limit_error', symbol, { error: `POST ${res.status} ${msg}` });
    }
  } catch (e) {
    logTradeAction('tp_limit_error', symbol, { error: e.message });
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

  const [overrides, setOverrides] = useState({});
  const [lastSkips, setLastSkips] = useState({});
  const lastSkipsRef = useRef({});
  const [overrideSym, setOverrideSym] = useState(null);

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
  const [scanStats, setScanStats] = useState({ ready: 0, attempted: 0, filled: 0, watch: 0, skipped: 0, reasons: {} });

  const [settings, setSettings] = useState({ ...SETTINGS });
  useEffect(() => {
    SETTINGS = { ...settings };
    logTradeAction('risk_changed', 'SETTINGS', { level: SETTINGS.riskLevel, spreadMax: SETTINGS.spreadMaxBps });
  }, [settings]);

  useEffect(() => {
    SETTINGS_OVERRIDES = { ...overrides };
  }, [overrides]);
  const [showSettings, setShowSettings] = useState(false);

  const [health, setHealth] = useState({ checkedAt: null, sections: {} });

  const scanningRef = useRef(false);
  const tradeStateRef = useRef({});
  const globalSpreadAvgRef = useRef(18);
  const touchMemoRef = useRef({});
  const stockPageRef = useRef(0);
  const cryptoPageRef = useRef(0);

  const lastAcctFetchRef = useRef(0);
  const getAccountSummaryThrottled = async (minMs = 30000) => {
    const now = Date.now();
    if (now - lastAcctFetchRef.current < minMs) return;
    lastAcctFetchRef.current = now;
    await getAccountSummary();
  };

  useEffect(() => {
    registerLogSubscriber((entry) => {
      const f = friendlyLog(entry);
      setLogHistory((prev) => [
        { ts: entry.timestamp, sev: f.sev, text: f.text, hint: null, raw: entry },
        ...prev,
      ].slice(0, LOG_UI_LIMIT));
      if (entry?.type === 'entry_skipped' && entry?.symbol) {
        lastSkipsRef.current[entry.symbol] = entry;
        setLastSkips({ ...lastSkipsRef.current });
        if (!overrideSym) setOverrideSym(entry.symbol);
        {
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
    });
    const seed = logBuffer
      .slice(-LOG_UI_LIMIT)
      .reverse()
      .map((e) => {
        const f = friendlyLog(e);
        return { ts: e.timestamp, sev: f.sev, text: f.text, hint: null, raw: e };
      });
    if (seed.length) setLogHistory(seed);
  }, []);
  const showNotification = (msg) => {
    setNotification(msg);
    setTimeout(() => setNotification(null), 5000);
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
      } else {
        TRADING_HALTED = false;
      }
    } catch (e) {
      logTradeAction('quote_exception', 'ACCOUNT', { error: e.message });
    } finally { setIsUpdatingAcct(false); }
  };

  async function checkAlpacaHealth() {
    const report = { checkedAt: new Date().toISOString(), sections: {} };

    try {
      const m = await getCryptoQuotesBatch(['BTC/USD','ETH/USD']);
      const bt = m.get('BTC/USD'), et = m.get('ETH/USD');
      const freshB = !!bt && isFresh(bt.tms, SETTINGS.liveFreshMsCrypto);
      const freshE = !!et && isFresh(et.tms, SETTINGS.liveFreshMsCrypto);
      report.sections.crypto = { ok: !!(freshB || freshE), detail: { 'BTC/USD': !!freshB, 'ETH/USD': !!freshE } };
      logTradeAction(freshB || freshE ? 'health_ok' : 'health_warn', 'SYSTEM', { section: 'crypto', note: freshB || freshE ? '' : 'no fresh quotes' });
    } catch (e) {
      report.sections.crypto = { ok: false, note: e.message };
      logTradeAction('health_err', 'SYSTEM', { section: 'crypto', note: e.message });
    }

    setHealth(report);
  }

  async function monitorOutcome(symbol, entryPx, v0) {
    const HORIZ_MIN = 3, STEP_MS = 10000;
    let t0 = Date.now(), best = 0;
    while (Date.now() - t0 < HORIZ_MIN * 60 * 1000) {
      let price = null;
      const m = await getCryptoTradesBatch([toDataSymbol(symbol)]);
      const one = m.get(toDataSymbol(symbol));
      price = Number.isFinite(one?.price) ? one.price : null;
      if (Number.isFinite(price)) best = Math.max(best, price - entryPx);
      await sleep(STEP_MS);
    }
    if (v0 > 0 && best > 0) {
      const g_hat = (v0 * v0) / (2 * best);
      const s = (symStats[symbol] ||= { mfeHist: [], hitByHour: Array.from({ length: 24 }, () => ({ h: 0, t: 0 })) });
      s.drag_g = ewma(s.drag_g, g_hat, 0.2);
      pushMFE(symbol, best);
      const hr = new Date().getUTCHours();
      const need = (requiredProfitBpsForSymbol(symbol, SETTINGS.riskLevel) / 10000) * entryPx;
      const hb = s.hitByHour[hr] || (s.hitByHour[hr] = { h: 0, t: 0 });
      hb.t += 1;
      if (best >= need) hb.h += 1;
    }
  }

  async function computeEntrySignal(asset, d, riskLvl, preQuoteMap = null, preBarsMap = null) {
    let bars1 = [];
    if (SETTINGS.enforceMomentum) {
      if (preBarsMap && preBarsMap.has(asset.symbol)) {
        bars1 = preBarsMap.get(asset.symbol) || [];
      } else {
        bars1 = await getCryptoBars1m(asset.symbol, 6);
      }
    }
    const closes = Array.isArray(bars1) ? bars1.map((b) => b.close) : [];

    const q = await getQuoteSmart(asset.symbol, preQuoteMap);
    if (!q || !(q.bid > 0 && q.ask > 0)) {
      let freshSec = null;
      try {
        const tm = await getCryptoTradesBatch([toDataSymbol(asset.symbol)]);
        const t = tm.get(toDataSymbol(asset.symbol));
        if (t?.tms) freshSec = (Date.now() - t.tms) / 1000;
      } catch {}
      return { entryReady: false, why: 'no_quote', meta: { freshSec } };
    }

    if (q.bs != null && Number.isFinite(q.bs) && (q.bs * q.bid) < MIN_BID_NOTIONAL_LOOSE_USD) {
      return { entryReady: false, why: 'illiquid', meta: { bsUSD: q.bs * q.bid, minUSD: MIN_BID_NOTIONAL_LOOSE_USD } };
    }

    const mid = 0.5 * (q.bid + q.ask);
    if (BLACKLIST.has(asset.symbol)) return { entryReady: false, why: 'blacklist', meta: {} };
    if (mid < eff(asset.symbol, 'minPriceUsd')) return { entryReady: false, why: 'tiny_price', meta: { mid } };

    const spreadBps = ((q.ask - q.bid) / mid) * 10000;
    logTradeAction('quote_ok', asset.symbol, { spreadBps: +spreadBps.toFixed(1) });

    if (spreadBps > d.spreadMax + SPREAD_EPS_BPS) {
      logTradeAction('skip_wide_spread', asset.symbol, { spreadBps: +spreadBps.toFixed(1) });
      return { entryReady: false, why: 'spread', meta: { spreadBps, max: d.spreadMax } };
    }

    const feeBps = roundTripFeeBpsEstimate(asset.symbol);
    if (SETTINGS.requireSpreadOverFees && spreadBps < feeBps + eff(asset.symbol, 'spreadOverFeesMinBps')) {
      return { entryReady: false, why: 'spread_fee_gate', meta: { spreadBps, feeBps } };
    }

    const ema5 = emaArr(closes.slice(-6), 5);
    const slopeUp = ema5.length >= 2 ? ema5.at(-1) > ema5.at(-2) : true;
    const v0 = closes.length >= 2 ? closes.at(-1) - closes.at(-2) : 0;
    const breakout = closes.length >= 6 ? closes.at(-1) >= Math.max(...closes.slice(-6, -1)) * 1.001 : true;
    if (SETTINGS.enforceMomentum && !(v0 >= 0 || slopeUp || breakout)) {
      return { entryReady: false, why: 'nomomo', meta: { v0, emaLast: ema5.at(-1), emaPrev: ema5.at(-2) } };
    }

    (symStats[asset.symbol] ||= {}).spreadEwmaBps = ewma(symStats[asset.symbol].spreadEwmaBps, spreadBps, 0.2);
    const slipEw = symStats[asset.symbol]?.slipEwmaBps ?? (SETTINGS.slipBpsByRisk?.[riskLvl] ?? 1);

    const needBps = Math.max(
      requiredProfitBpsForSymbol(asset.symbol, riskLvl),
      exitFloorBps(asset.symbol) + 0.5 + slipEw,
      eff(asset.symbol, 'netMinProfitBps')
    );
    const spreadEw = symStats[asset.symbol]?.spreadEwmaBps ?? spreadBps;
    const needBpsCapped = Math.min(needBps, Math.max(exitFloorBps(asset.symbol) + 1, Math.round((spreadEw || 0) * 1.6)));
    const tpBase = q.bid * (1 + needBpsCapped / 10000);
    if (!(tpBase > q.bid * 1.00005)) {
      return { entryReady: false, why: 'edge_negative', meta: { tpBps: needBpsCapped, bid: q.bid, tp: tpBase } };
    }

    const sst = symStats[asset.symbol] || {};
    const drag_g = Math.max(1e-6, sst.drag_g ?? 8);
    const runway = v0 > 0 ? (v0 * v0) / (2 * drag_g) : 0;

    return { entryReady: true, spreadBps, quote: q, tpBps: needBpsCapped, tp: tpBase, v0, runway, meta: {} };
  }

  const placeOrder = async (symbol, ccSymbol = symbol, d, sigPre = null, preQuoteMap = null, refs) => {
    if (TRADING_HALTED) {
      logTradeAction('daily_halt', symbol, { reason: HALT_REASON || 'Rule' });
      return false;
    }
    if (STABLES.has(symbol)) return false;

    await cleanupStaleBuyOrders(30);

    try {
      const allPos = await getAllPositionsCached();
      const nonStableOpen = (allPos || []).filter((p) => Number(p.qty) > 0 && Number(p.market_value || p.marketValue || 0) > 1).length;
      const cap = concurrencyCapBySpread(globalSpreadAvgRef.current);
      if (nonStableOpen >= cap) {
        logTradeAction('concurrency_guard', symbol, { cap, avg: globalSpreadAvgRef.current, hitRate: (function(){let h=0,t=0;for(const s of Object.values(symStats)){for(const b of (s.hitByHour||[])){h+=b.h||0;t+=b.t||0;}}return t>0?(h/t):null;})() });
        return false;
      }
    } catch {}

    const held = await getPositionInfo(symbol);
    if (held && Number(held.qty) > 0) {
      logTradeAction('entry_skipped', symbol, { entryReady: false, reason: 'held' });
      return false;
    }

    const sig = sigPre || (await computeEntrySignal({ symbol, cc: ccSymbol }, d, SETTINGS.riskLevel, preQuoteMap, null));
    if (!sig.entryReady) return false;

    let equity = acctSummary.portfolioValue;
    let buyingPower = isCrypto(symbol)
      ? (acctSummary.cryptoBuyingPower ?? acctSummary.buyingPower)
      : (acctSummary.stockBuyingPower  ?? acctSummary.buyingPower);
    if (!Number.isFinite(equity) || !Number.isFinite(buyingPower)) {
      try {
        const a = await getAccountSummaryRaw();
        equity = a.equity;
        buyingPower = isCrypto(symbol)
          ? (a.cryptoBuyingPower ?? a.buyingPower)
          : (a.stockBuyingPower  ?? a.buyingPower);
        setAcctSummary((s) => ({
          portfolioValue: a.equity, buyingPower: a.buyingPower, dailyChangeUsd: a.changeUsd, dailyChangePct: a.changePct,
          patternDayTrader: a.patternDayTrader, daytradeCount: a.daytradeCount, updatedAt: new Date().toISOString(),
          cryptoBuyingPower: a.cryptoBuyingPower, stockBuyingPower: a.stockBuyingPower, cash: a.cash
        }));
      } catch {}
    }
    if (!Number.isFinite(equity) || equity <= 0) equity = 1000;
    if (!Number.isFinite(buyingPower) || buyingPower <= 0) return false;

    const desired = Math.min(buyingPower, (SETTINGS.maxPosPctEquity / 100) * equity);
    const notional = capNotional(symbol, desired, equity);

    let entryPx = sig?.quote?.bid;
    if (!Number.isFinite(entryPx) || entryPx <= 0) entryPx = sig?.quote?.ask;
    if (!Number.isFinite(notional) || notional < 5) {
      logTradeAction('skip_small_order', symbol);
      return false;
    }

    if (!Number.isFinite(entryPx) || entryPx <= 0) entryPx = sig.quote.bid;

    // Size using ask (or join) + fees + a tiny cushion
    const pxForSizing = (sig?.quote?.ask ?? sig?.quote?.bid);
    const feeFrac = (SETTINGS.feeBpsMaker || 15) / 10000; // assume maker for entry
    const cushion = 0.0008; // 8 bps headroom
    const denom = Math.max(pxForSizing * (1 + feeFrac + cushion), 1e-9);
    let qty = +(notional / denom).toFixed(6);
    if (!Number.isFinite(qty) || qty <= 0) {
      logTradeAction('skip_small_order', symbol);
      return false;
    }

    // console.log('USABLE BP', symbol, buyingPower);
    const result = await placeMakerThenMaybeTakerBuy(symbol, qty, preQuoteMap, buyingPower);
    if (!result.filled) return false;

    const actualEntry = result.entry ?? entryPx;
    const actualQty = result.qty ?? qty;

    const buyBpsApplied = result.liquidity === 'taker' ? SETTINGS.feeBpsTaker : SETTINGS.feeBpsMaker;

    const approxMid = sig && sig.quote ? 0.5 * (sig.quote.bid + sig.quote.ask) : actualEntry;
    const slipBpsVal = Number.isFinite(approxMid) && approxMid > 0 ? ((actualEntry - (sig?.quote?.bid ?? entryPx)) / approxMid) * 10000 : 0;
    const s = (refs.tradeStateRef.current[symbol] ||= { hitByHour: Array.from({ length: 24 }, () => ({ h: 0, t: 0 })), mfeHist: [] });
    s.slipEwmaBps = ewma(s.slipEwmaBps, Math.max(0, slipBpsVal), 0.2);

    const slipEw = s.slipEwmaBps ?? (SETTINGS.slipBpsByRisk?.[SETTINGS.riskLevel] ?? 1);
    const needBps0 = requiredProfitBpsForSymbol(symbol, SETTINGS.riskLevel);
    const needBpsAdj = Math.max(needBps0, exitFloorBps(symbol) + 0.5 + slipEw, eff(symbol, 'netMinProfitBps'));
    const tpBase = actualEntry * (1 + needBpsAdj / 10000);
    const feeFloor = minExitPriceFeeAwareDynamic({ symbol, entryPx: actualEntry, qty: actualQty, buyBpsOverride: buyBpsApplied });
    const tpCapped = Math.max(Math.min(tpBase, actualEntry + (sig?.runway ?? 0)), feeFloor);

    refs.tradeStateRef.current[symbol] = {
      entry: actualEntry, qty: actualQty, tp: tpCapped, feeFloor,
      runway: sig?.runway ?? 0, entryTs: Date.now(), lastLimitPostTs: 0,
      wasHolding: true, stopPx: null, hardStopPx: null, trailArmed: false, trailPeak: null,
      buyBpsApplied, // track actual buy fee (maker/taker)
    };
    await ensureLimitTP(symbol, tpCapped, { tradeStateRef, touchMemoRef });

    monitorOutcome(symbol, actualEntry, sig?.v0 ?? 0).catch(() => {});
    return true;
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
            .map((o) => [o.symbol, o])
        );
        for (const p of positions || []) {
          const symbol = p.symbol;
          const basePos = posBySym.get(symbol) || p;
          const qty = Number(basePos.qty || 0);
          if (qty <= 0) continue;

          const s = tradeStateRef.current[symbol] || {
            entry: Number(basePos.avg_entry_price || basePos.basis || 0),
            qty: Number(basePos.qty || 0),
            entryTs: Date.now(), lastLimitPostTs: 0, runway: 0, wasHolding: true, feeFloor: null,
          };
          tradeStateRef.current[symbol] = s;

          const slipEw = symStats[symbol]?.slipEwmaBps ?? (SETTINGS.slipBpsByRisk?.[SETTINGS.riskLevel] ?? 1);
          const needAdj = Math.max(requiredProfitBpsForSymbol(symbol, SETTINGS.riskLevel), exitFloorBps(symbol) + 0.5 + slipEw, eff(symbol, 'netMinProfitBps'));
          const entryBase = Number(s.entry || basePos.avg_entry_price || basePos.mark || 0);
          const tpBase = entryBase * (1 + needAdj / 10000);
          const feeFloor = minExitPriceFeeAwareDynamic({ symbol, entryPx: entryBase, qty: Number(basePos.available ?? basePos.qty ?? 0), buyBpsOverride: s.buyBpsApplied });
          const tp = Math.max(Math.min(tpBase, entryBase + (s.runway ?? 0)), feeFloor);
          s.tp = tp; s.feeFloor = feeFloor;

          await ensureLimitTP(symbol, tp, { tradeStateRef, touchMemoRef, openSellBySym, pos: basePos });
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
            .map((o) => [o.symbol, o])
        );
        for (const p of positions || []) {
          const sym = p.symbol;
          if (STABLES.has(sym) || BLACKLIST.has(sym)) continue;
          const mv = Number(p.market_value ?? p.marketValue ?? 0);
          const avail = Number(p.qty_available ?? p.available ?? p.qty ?? 0);
          if (mv > 0 && mv < SETTINGS.dustFlattenMaxUsd && avail > 0 && !openSellBySym.has(sym)) {
            const mkt = { symbol: sym, qty: avail, side: 'sell', type: 'market', time_in_force: 'gtc' };
            try {
              const res = await f(`${ALPACA_BASE_URL}/orders`, { method: 'POST', headers: HEADERS, body: JSON.stringify(mkt) });
              if (res.ok) {
                __positionsCache = { ts: 0, items: [] };
                __openOrdersCache = { ts: 0, items: [] };
                logTradeAction('dust_flattened', sym, { usd: mv });
              }
            } catch {}
          }
        }
      } catch {}
      if (!stopped) setTimeout(sweep, SETTINGS.dustSweepMinutes * 60 * 1000);
    };
    sweep();
    return () => { stopped = true; };
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
    if (scanningRef.current) return;
    scanningRef.current = true;
    setIsLoading(true);

    const effectiveTracked = tracked && tracked.length ? tracked : CRYPTO_CORE_TRACKED;
    setData((prev) =>
      prev && prev.length
        ? prev
        : effectiveTracked.map((t) => ({ ...t, price: null, entryReady: false, error: null, time: new Date().toLocaleTimeString(), spreadBps: null, tpBps: null }))
    );

    let results = [];
    try {
      await getAccountSummaryThrottled();

      const [positions, allOpenOrders] = await Promise.all([getAllPositionsCached(), getOpenOrdersCached()]);
      const posBySym = new Map((positions || []).map((p) => [p.symbol, p]));
      const openCount = (positions || []).filter((p) => {
        const sym = p.symbol;
        if (STABLES.has(sym)) return false;
        const mv = parseFloat(p.market_value ?? p.marketValue ?? '0');
        const qty = parseFloat(p.qty ?? '0');
        return Number.isFinite(mv) && mv > 1 && Number.isFinite(qty) && qty > 0;
      }).length;

      let cryptosAll = effectiveTracked;
      const cryptoPages = Math.max(1, Math.ceil(Math.max(0, cryptosAll.length) / SETTINGS.stockPageSize));
      const cIdx = cryptoPageRef.current % cryptoPages;
      const cStart = cIdx * SETTINGS.stockPageSize;
      const cryptoSlice = cryptosAll.slice(cStart, Math.min(cStart + SETTINGS.stockPageSize, cryptosAll.length));
      cryptoPageRef.current += 1;

      const barsMap = SETTINGS.enforceMomentum ? await getCryptoBars1mBatch(cryptoSlice.map(t => t.symbol), 6) : null;

      setOpenMeta({ positions: openCount, orders: (allOpenOrders || []).length, allowed: cryptosAll.length, universe: cryptosAll.length });
      logTradeAction('scan_start', 'STATIC', { batch: cryptoSlice.length });

      const mixedSymbols = [...cryptoSlice.map((t) => t.symbol)];
      const batchMap = await getQuotesBatch(mixedSymbols);

      for (const asset of cryptoSlice) {
        const qDisplay = batchMap.get(asset.symbol);
        if (qDisplay && qDisplay.bid > 0 && qDisplay.ask > 0) {
          const mid = 0.5 * (qDisplay.bid + qDisplay.ask);
          pushPriceHist(asset.symbol, mid);
        }
      }

      let readyCount = 0, attemptCount = 0, successCount = 0, watchCount = 0, skippedCount = 0;
      const reasonCounts = {};
      const spreadSamples = [];
      for (const asset of cryptoSlice) {
        const d = { spreadMax: eff(asset.symbol, 'spreadMaxBps') };
        const token = { ...asset, price: null, entryReady: false, error: null, time: new Date().toLocaleTimeString(), spreadBps: null, tpBps: null };
        try {
          const qDisplay = batchMap.get(asset.symbol);
          if (qDisplay && qDisplay.bid > 0 && qDisplay.ask > 0) token.price = 0.5 * (qDisplay.bid + qDisplay.ask);

          const prevState = tradeStateRef.current[asset.symbol] || {};
          const posNow = posBySym.get(asset.symbol);
          const isHolding = !!(posNow && Number(posNow.qty) > 0);
          tradeStateRef.current[asset.symbol] = { ...prevState, wasHolding: isHolding };

          const sig = await computeEntrySignal(asset, d, SETTINGS.riskLevel, batchMap, barsMap);
          token.entryReady = sig.entryReady;

          if (sig?.quote && sig.quote.bid > 0 && sig.quote.ask > 0) {
            const mid2 = 0.5 * (sig.quote.bid + sig.quote.ask);
            spreadSamples.push(((sig.quote.ask - sig.quote.bid) / mid2) * 10000);
          }

          if (sig.entryReady) {
            token.spreadBps = sig.spreadBps ?? null;
            token.tpBps = sig.tpBps ?? null;
            readyCount++;
            attemptCount++;
            if (autoTrade) {
              const ok = await placeOrder(asset.symbol, asset.cc, d, sig, batchMap, { tradeStateRef, touchMemoRef });
              if (ok) successCount++;
            } else {
              logTradeAction('entry_skipped', asset.symbol, { entryReady: true, reason: 'auto_off' });
            }
          } else {
            watchCount++;
            skippedCount++;
            if (sig?.why) reasonCounts[sig.why] = (reasonCounts[sig.why] || 0) + 1;
            logTradeAction('entry_skipped', asset.symbol, { entryReady: false, reason: sig.why, ...(sig.meta || {}) });
          }
        } catch (err) {
          token.error = err?.message || String(err);
          logTradeAction('scan_error', asset.symbol, { error: token.error });
          watchCount++;
          skippedCount++;
        }
        results.push(token);
      }

      const avg = spreadSamples.length ? spreadSamples.reduce((a, b) => a + b, 0) / spreadSamples.length : globalSpreadAvgRef.current;
      globalSpreadAvgRef.current = avg;

      setScanStats({ ready: readyCount, attempted: attemptCount, filled: successCount, watch: watchCount, skipped: skippedCount, reasons: reasonCounts });
      logTradeAction('scan_summary', 'STATIC', { readyCount, attemptCount, successCount });
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
        if (!cancelled) setTimeout(tick, Math.max(1000, SETTINGS.scanMs));
      }
    };
    tick();
    return () => { cancelled = true; };
  }, [settings.scanMs, tracked?.length]);

  const onRefresh = () => {
    setRefreshing(true);
    loadData();
  };
  const bp = acctSummary.buyingPower, chPct = acctSummary.dailyChangePct;

  const okWindowMs = Math.max(SETTINGS.scanMs * 3, 6000);
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
        const feesSum = (SETTINGS.feeBpsMaker + SETTINGS.feeBpsTaker);
        if (key === 'dynamicMinProfitBps' && nextVal < (feesSum + 5)) nextVal = feesSum + 5;
        if (key === 'spreadOverFeesMinBps' && nextVal < 0) nextVal = 0;
        if (key === 'netMinProfitBps' && nextVal < SETTINGS.autoTuneMinNetMinBps) nextVal = SETTINGS.autoTuneMinNetMinBps;

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
    return why;
  }

  const applyPreset = (name) => {
    const presets = {
      Safer:  { riskLevel: 3, spreadMaxBps: 50,  maxPosPctEquity: 10, absMaxNotionalUSD: 100, makerCampSec: 30, enableTakerFlip: false, takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 10, liveRequireQuote: true, liveFreshMsCrypto: 10000, liveFreshMsStock: 10000, enforceMomentum: true,  enableStops: true, stopLossBps: 80, hardStopLossPct: 1.8, stopGraceSec: 10, enableTrailing: true, trailStartBps: 20, trailingStopBps: 10, maxConcurrentPositions: 6, haltOnDailyLoss: true,  dailyMaxLossPct: 3.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
      Neutral:{ riskLevel: 2, spreadMaxBps: 70,  maxPosPctEquity: 15, absMaxNotionalUSD: 150, makerCampSec: 25, enableTakerFlip: false, takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 9,  liveRequireQuote: true, liveFreshMsCrypto: 15000, liveFreshMsStock: 15000, enforceMomentum: true,  enableStops: true, stopLossBps: 80, hardStopLossPct: 1.8, stopGraceSec: 10, enableTrailing: true, trailStartBps: 20, trailingStopBps: 10,  maxConcurrentPositions: 8, haltOnDailyLoss: true,  dailyMaxLossPct: 4.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
      Faster: { riskLevel: 1, spreadMaxBps: 100, maxPosPctEquity: 20, absMaxNotionalUSD: 200, makerCampSec: 20, enableTakerFlip: false, takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 8,  liveRequireQuote: true, liveFreshMsCrypto: 15000, liveFreshMsStock: 15000, enforceMomentum: true,  enableStops: true, stopLossBps: 80, hardStopLossPct: 1.8, stopGraceSec: 10, enableTrailing: true, trailStartBps: 20, trailingStopBps: 10,  maxConcurrentPositions: 8, haltOnDailyLoss: true,  dailyMaxLossPct: 5.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
      Aggro:  { riskLevel: 0, spreadMaxBps: 120, maxPosPctEquity: 25, absMaxNotionalUSD: 300, makerCampSec: 15, enableTakerFlip: true,  takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 7,  liveRequireQuote: true, liveFreshMsCrypto: 15000, liveFreshMsStock: 15000, enforceMomentum: false, enableStops: true, stopLossBps: 80, hardStopLossPct: 1.8, stopGraceSec: 10, enableTrailing: true, trailStartBps: 12, trailingStopBps: 6,  maxConcurrentPositions: 10,haltOnDailyLoss: true,  dailyMaxLossPct: 6.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
      Max:    { riskLevel: 0, spreadMaxBps: 150, maxPosPctEquity: 30, absMaxNotionalUSD: 500, makerCampSec: 10, enableTakerFlip: true,  takerExitOnTouch: true, takerExitGuard: 'min', touchFlipTimeoutSec: 6,  liveRequireQuote: true, liveFreshMsCrypto: 15000, liveFreshMsStock: 15000, enforceMomentum: false, enableStops: true, stopLossBps: 80, hardStopLossPct: 2.0, stopGraceSec: 10, enableTrailing: true, trailStartBps: 10, trailingStopBps: 5,  maxConcurrentPositions: 12,haltOnDailyLoss: false, dailyMaxLossPct: 8.0, spreadOverFeesMinBps: 5, dynamicMinProfitBps: 60, extraOverFeesBps: 10, minPriceUsd: 0.001, feeBpsMaker: 15, feeBpsTaker: 25, slipBpsByRisk: [1,2,3,4,5] },
    };
    const p = presets[name];
    if (!p) return;
    setSettings((s) => ({ ...s, ...p }));
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
                  onPress={() => setSettings((s) => ({ ...s, riskLevel: idx }))}
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

        <PortfolioChangeChart acctSummary={acctSummary} />
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
              <Text style={styles.scanLabel}>Attempted</Text>
              <Text style={styles.scanValue}>{scanStats.attempted}</Text>
            </View>
            <View style={styles.scanItem}>
              <Text style={styles.scanLabel}>Filled</Text>
              <Text style={styles.scanValue}>{scanStats.filled}</Text>
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
