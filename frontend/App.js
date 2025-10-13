// App.js — Bullish or Bust — v1.9.3-MIXED-PAGER+ENTRY-ROLLBACK+EQUITY-FEE-FIX
// Summary of fixes vs 1.9.2-STOCKS-PAGER:
// • Re-enable mixed scanning (stocks + crypto) with rotating pagers (20 equities + 10 cryptos per tick)
// • Restore earlier, more permissive entry math (kept EMA slope & 3-bar momo; removed near-high & cooldown/pullback gates)
// • Correct equity fee floor in signal stage (no longer uses crypto bps globally)
// • Remove risk level UI slider; use single fixed RISK_LEVEL constant
// • FeeGuard/TP-on-touch/dust sweep/CSV remain intact. Alpaca keys untouched.

import React, { useState, useEffect, useRef } from 'react';
import { View, Text, ScrollView, StyleSheet, RefreshControl, TouchableOpacity, SafeAreaView, ActivityIndicator, TextInput } from 'react-native';
import Constants from 'expo-constants';

/* ===================== Meta / API ===================== */
const VERSION = 'v1.9.3-MIXED-PAGER+ENTRY-ROLLBACK+EQUITY-FEE-FIX';

const EX = (Constants?.expoConfig?.extra) || (Constants?.manifest?.extra) || {};
// (per user request: do not change)
const ALPACA_KEY    = 'AKANN0IP04IH45Z6FG3L';
const ALPACA_SECRET = 'qvaKRqP9Q3XMVMEYqVnq2BEgPGhQQQfWg1JT7bWV';
const ALPACA_BASE_URL = EX.APCA_API_BASE || 'https://api.alpaca.markets/v2';

const DATA_ROOT_CRYPTO = 'https://data.alpaca.markets/v1beta3/crypto';
const DATA_LOCATIONS   = ['us', 'global'];     // for crypto
const DATA_ROOT_STOCKS_V2 = 'https://data.alpaca.markets/v2/stocks';

const HEADERS = {
  'APCA-API-KEY-ID': ALPACA_KEY,
  'APCA-API-SECRET-KEY': ALPACA_SECRET,
  Accept: 'application/json',
  'Content-Type': 'application/json',
};

console.log('[Alpaca LIVE ENV]', {
  base: ALPACA_BASE_URL,
  keyPrefix: (ALPACA_KEY||'').slice(0,4),
  hasSecret: Boolean(ALPACA_SECRET),
});

/* ===================== HTTP helper (timeout + retry) ===================== */
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
async function f(url, opts={}, timeoutMs=8000, retries=2){
  let lastErr;
  for (let i=0;i<=retries;i++){
    const ac = new AbortController();
    const t  = setTimeout(()=>ac.abort(), timeoutMs);
    try{
      const res = await fetch(url, {...opts, signal: ac.signal});
      clearTimeout(t);
      if (res.status===429 || res.status>=500){
        if (i===retries) return res;
        await sleep(500*Math.pow(2,i));
        continue;
      }
      return res;
    }catch(e){
      clearTimeout(t);
      lastErr = e;
      if (i===retries) throw e;
      await sleep(350*Math.pow(2,i));
    }
  }
  if (lastErr) throw lastErr;
  return fetch(url, opts);
}

/* ===================== Monitoring (PnL & Fees) ===================== */
async function getPortfolioHistory({ period='1M', timeframe='1D' } = {}) {
  const url = `${ALPACA_BASE_URL}/account/portfolio/history?period=${encodeURIComponent(period)}&timeframe=${encodeURIComponent(timeframe)}&extended_hours=true`;
  const res = await f(url, { headers: HEADERS });
  if (!res.ok) return null;
  return res.json().catch(()=>null);
}
async function getActivities({ afterISO, untilISO, pageToken } = {}) {
  const params = new URLSearchParams({
    activity_types: 'FILL,FEE,CFEE,PTC',
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
const isoDaysAgo = (n)=>new Date(Date.now()-n*864e5).toISOString();

async function getPnLAndFeesSnapshot() {
  const hist1M = await getPortfolioHistory({ period: '1M', timeframe: '1D' });
  let last7Sum=null,last7DownDays=null,last7UpDays=null,last30Sum=null;
  if (hist1M?.profit_loss) {
    const pl = hist1M.profit_loss.map(Number).filter(Number.isFinite);
    const last7  = pl.slice(-7);
    const last30 = pl.slice(-30);
    last7Sum = last7.reduce((a,b)=>a+b,0);
    last30Sum = last30.reduce((a,b)=>a+b,0);
    last7UpDays = last7.filter(x=>x>0).length;
    last7DownDays = last7.filter(x=>x<0).length;
  }

  let fees30 = 0, fillsCount30 = 0;
  const afterISO = isoDaysAgo(30), untilISO = new Date().toISOString();
  let token = null;
  for (let i=0;i<10;i++){
    const { items, next } = await getActivities({ afterISO, untilISO, pageToken: token });
    for (const it of items) {
      const t = (it?.activity_type || it?.activityType || '').toUpperCase();
      if (t==='CFEE' || t==='FEE' || t==='PTC') {
        const raw = it.net_amount ?? it.amount ?? it.price ?? ((Number(it.per_share_amount)*Number(it.qty))||NaN);
        const amt = Number(raw);
        if (Number.isFinite(amt)) fees30 += amt;
      } else if (t==='FILL') fillsCount30 += 1;
    }
    if (!next) break;
    token = next;
  }
  return { last7Sum,last7UpDays,last7DownDays,last30Sum,fees30,fillsCount30 };
}

/* ===================== Stocks market clock ===================== */
async function getStockClock(){
  try{
    const r = await f(`${ALPACA_BASE_URL}/clock`, { headers: HEADERS });
    if (!r.ok) return { is_open:true };
    const j = await r.json();
    return { is_open: !!j.is_open, next_open: j.next_open, next_close: j.next_close };
  }catch{ return { is_open:true }; }
}

/* ========== Transaction History → CSV Viewer ========== */
const TxnHistoryCSVViewer = () => {
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState('');
  const [csv, setCsv] = useState('');
  const csvRef = useRef(null);

  const BASE_URL = (ALPACA_BASE_URL || 'https://api.alpaca.markets/v2').replace(/\/v2$/, '');
  const ACTIVITIES_URL = `${BASE_URL}/v2/account/activities`;

  async function fetchActivities({ days=7, types='FILL,CFEE,FEE,TRANS,PTC', max=1000 } = {}) {
    const until = new Date();
    const after = new Date(until.getTime()-days*864e5);
    const baseParams = new URLSearchParams();
    baseParams.set('direction','desc');
    baseParams.set('page_size','100');
    baseParams.set('activity_types', types);
    baseParams.set('after', after.toISOString());
    baseParams.set('until', until.toISOString());

    let pageToken=null, all=[];
    while (true) {
      const params = new URLSearchParams(baseParams);
      if (pageToken) params.set('page_token', pageToken);

      const res = await fetch(`${ACTIVITIES_URL}?${params.toString()}`, { headers: HEADERS });
      if (!res.ok) {
        const text = await res.text().catch(()=> '');
        throw new Error(`Activities HTTP ${res.status}${text ? ` - ${text}` : ''}`);
      }
      const arr = await res.json();
      if (!Array.isArray(arr) || arr.length===0) break;

      all = all.concat(arr);
      if (all.length >= max) break;

      const last = arr[arr.length-1];
      if (!last?.id) break;
      pageToken = last.id;
    }
    return all.slice(0,max);
  }

  function toCsv(rows) {
    const header = ['DateTime','Type','Side','Symbol','Qty','Price','CashFlowUSD','OrderID','ActivityID'];
    const escape = (v) => {
      if (v==null) return '';
      const s = String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
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
      if ((r.activity_type||'').toUpperCase()==='FILL') {
        const q = parseFloat(qty ?? '0');
        const p = parseFloat(price ?? '0');
        if (Number.isFinite(q)&&Number.isFinite(p)) {
          const signed = q*p*(side==='buy'?-1:1);
          cash = signed.toFixed(2);
        }
      } else {
        const net = parseFloat(r.net_amount ?? r.amount ?? '');
        cash = Number.isFinite(net) ? net.toFixed(2) : '';
      }
      const row = [local,r.activity_type,side,symbol,qty,price,cash,(r.order_id||''),(r.id||'')];
      lines.push(row.map(escape).join(','));
    }
    return lines.join('\n');
  }

  const buildRange = async (days) => {
    try{
      setBusy(true); setStatus('Fetching…'); setCsv('');
      const acts = await fetchActivities({ days });
      if (!acts.length) { setStatus('No activities found in range.'); return; }
      const out = toCsv(acts);
      setCsv(out);
      setStatus(`Built ${acts.length} activities (${days}d). Tap the box → Select All → Copy.`);
      setTimeout(()=>{ try{
        csvRef.current?.focus?.();
        csvRef.current?.setNativeProps?.({ selection: { start: 0, end: out.length } });
      } catch {} }, 150);
    }catch(err){ setStatus(`Error: ${err.message}`); }
    finally{ setBusy(false); }
  };

  return (
    <View style={styles.txnBox}>
      <Text style={styles.txnTitle}>Transaction History → CSV</Text>
      <View style={styles.txnBtnRow}>
        <TouchableOpacity style={styles.txnBtn} onPress={()=>buildRange(1)} disabled={busy}><Text style={styles.txnBtnText}>Build 24h CSV</Text></TouchableOpacity>
        <TouchableOpacity style={styles.txnBtn} onPress={()=>buildRange(7)} disabled={busy}><Text style={styles.txnBtnText}>Build 7d CSV</Text></TouchableOpacity>
        <TouchableOpacity style={styles.txnBtn} onPress={()=>buildRange(30)} disabled={busy}><Text style={styles.txnBtnText}>Build 30d CSV</Text></TouchableOpacity>
      </View>
      {busy ? <ActivityIndicator /> : null}
      <Text style={styles.txnStatus}>{status}</Text>
      {csv ? (
        <View style={{ marginTop: 8 }}>
          <Text style={styles.csvHelp}>Tap the box → Select All → Copy</Text>
          <TextInput ref={csvRef} style={styles.csvBox} value={csv} editable={false} multiline selectTextOnFocus scrollEnabled textBreakStrategy="highQuality" />
        </View>
      ) : null}
    </View>
  );
};

/* ===================== Strategy / constants ===================== */
const MAKER_ONLY        = true;
const ENABLE_TAKER_FLIP = true;

// Crypto fees (bps)
const FEE_BPS_MAKER = 15;
const FEE_BPS_TAKER = 25;

// ===== FeeGuard knobs =====
const NET_MIN_PROFIT_USD = 0.02;     // ≥ $0.02 per trade
const NET_MIN_PROFIT_BPS = 1.0;      // or ≥ 1bp on entry (whichever requires higher TP)
const EQUITY_SEC_FEE_BPS = 0.35;     // bps on SELLS only
const EQUITY_TAF_PER_SHARE = 0.000145; 
const EQUITY_TAF_CAP = 7.27;
const EQUITY_COMMISSION_PER_TRADE_USD = 0.00;

// === Risk: fixed level (UI removed). Lower = more entries, Higher = more conservative slip buffer.
const RISK_LEVEL = 2;                 // 0..4 (we used 2 historically as a good middle)
const SLIP_BUFFER_BPS_BY_RISK = [8, 10, 12, 14, 16];

// Scanner cadence & sizing caps
const SCAN_MS = 1000;
const ABS_MAX_NOTIONAL_USD = 85;

// Filter & guards
const STABLES = new Set(['USDTUSD','USDCUSD']);
const BLACKLIST = new Set(['SHIBUSD']);
const MIN_PRICE_FOR_TICK_SANE_USD = 0.05;
const DUST_FLATTEN_MAX_USD = 0.75;
const DUST_SWEEP_MINUTES = 12;
const TOUCH_TICKS_REQUIRED = 2;
const MIN_BID_SIZE_LOOSE = 1;

// Universe / buckets
const UNIVERSE_REFRESH_MIN = 15;      // rediscover symbols every 15 min
const MAX_EQUITIES = 160;
const MAX_CRYPTOS = 80;

// Mixed pagers
const STOCK_PAGE_SIZE  = 20;
const CRYPTO_PAGE_SIZE = 10;

// Base config used elsewhere
const CFG_BASE = {
  risk:  { maxPosPctEquity: 10, minNotionalUSD: 5 },
  exits: { limitReplaceSecs: 10, markRefreshSecs: 5 },
};

/* ===================== Discovery (NO static list) ===================== */
const isCrypto = (sym)=> /USD$/.test(sym);
const isStock  = (sym)=> !isCrypto(sym);

const FALLBACK_SEED = [
  { name:'AAPL', symbol:'AAPL', cc:null },
  { name:'MSFT', symbol:'MSFT', cc:null },
  { name:'NVDA', symbol:'NVDA', cc:null },
  { name:'ETH/USD', symbol:'ETHUSD', cc:'ETH' },
  { name:'BTC/USD', symbol:'BTCUSD', cc:'BTC' },
  { name:'SOL/USD', symbol:'SOLUSD', cc:'SOL' },
];

async function fetchAssetsEquities(){
  try{
    const url = `${ALPACA_BASE_URL}/assets?status=active&asset_class=us_equity`;
    const r = await f(url, { headers: HEADERS }, 30000, 2);
    if(!r.ok) throw new Error(String(r.status));
    const arr = await r.json();
    const pool = (arr||[])
      .filter(a => a.tradable && typeof a.symbol === 'string')
      .filter(a => /^[A-Z.\-]{1,5}$/.test(a.symbol));
    return pool.slice(0, MAX_EQUITIES).map(a => ({ name:a.symbol, symbol:a.symbol, cc:null }));
  }catch(e){ return []; }
}
async function fetchAssetsCrypto(){
  try{
    const url = `${ALPACA_BASE_URL}/assets?status=active&asset_class=crypto`;
    const r = await f(url, { headers: HEADERS }, 30000, 2);
    if (!r.ok) throw new Error(String(r.status));
    const arr = await r.json();
    const pool = (arr||[])
      .filter(a => a.tradable && /USD$/.test(a.symbol))
      .map(a => a.symbol);
    const uniq = Array.from(new Set(pool))
      .filter(sym => !STABLES.has(sym) && !BLACKLIST.has(sym));
    return uniq.slice(0, MAX_CRYPTOS).map(sym => ({ name: sym.replace('USD','')+'/USD', symbol: sym, cc: sym.replace('USD','') }));
  }catch{ return []; }
}
async function discoverUniverse(){
  try{
    let [eq, cc] = await Promise.all([fetchAssetsEquities(), fetchAssetsCrypto()]);
    let merged = [...eq, ...cc];
    if (merged.length < 40){
      await sleep(1000);
      const [eq2, cc2] = await Promise.all([fetchAssetsEquities(), fetchAssetsCrypto()]);
      merged = [...merged, ...eq2, ...cc2];
    }
    const m = new Map();
    for (const o of (merged||[])) {
      if (!o?.symbol) continue;
      if (!isStock(o.symbol) && STABLES.has(o.symbol)) continue;
      m.set(o.symbol, o);
    }
    const out = Array.from(m.values());
    return (out.length ? out : FALLBACK_SEED);
  }catch{ return FALLBACK_SEED; }
}

/* ===================== Crypto price fallbacks ===================== */
const CC_BARS = (cc, limit=5)=>`https://min-api.cryptocompare.com/data/v2/histominute?fsym=${cc}&tsym=USD&limit=${limit}&aggregate=1`;
const CC_PRICE = (cc)=>`https://min-api.cryptocompare.com/data/price?fsym=${cc}&tsyms=USD`;
const CG_IDS = { BTC:'bitcoin', ETH:'ethereum', SOL:'solana', AAVE:'aave', LTC:'litecoin', LINK:'chainlink', UNI:'uniswap', AVAX:'avalanche-2', ADA:'cardano', MATIC:'matic-network', XRP:'ripple', SHIB:'shiba-inu', BCH:'bitcoin-cash', ETC:'ethereum-classic', TRX:'tron', USDT:'tether', USDC:'usd-coin' };
const CG_PRICE = (id)=>`https://api.coingecko.com/api/v3/simple/price?ids=${id}&vs_currencies=usd`;

const BAR_TTL_MS_1M = 25000, QUOTE_TTL_MS = 4000, LAST_GOOD_TTL_MS = 15000;
const barsCache1m = new Map(), quoteCache = new Map(), lastGood = new Map();
let DISABLE_ALPACA_DATA_UNTIL = 0;
const unsupportedSymbols = new Map();
const isUnsupported = (sym)=>{ const u = unsupportedSymbols.get(sym); if (!u) return false; if (Date.now()>u){ unsupportedSymbols.delete(sym); return false; } return true; };
function markUnsupported(sym, mins=120){ unsupportedSymbols.set(sym, Date.now()+mins*60000); }

/* ===================== Utilities ===================== */
const clamp = (x,lo,hi)=>Math.max(lo,Math.min(hi,x));
const fmtUSD = (n)=>Number.isFinite(n)?`$ ${n.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}`:'—';
const fmtPct = (n)=>Number.isFinite(n)?`${n.toFixed(2)}%`:'—';
function toDataSymbol(sym){ if(!sym)return sym; if(sym.includes('/'))return sym; if(sym.endsWith('USD'))return sym.slice(0,-3)+'/USD'; return sym; }
const ccFromSymbol = (sym)=>(sym||'').replace('/','').replace('USD','');
function halfFromBps(price, bps){ return (bps/20000)*price; }
const emaArr = (arr,span)=>{ if(!arr?.length) return []; const k=2/(span+1); let prev=arr[0]; const out=[prev]; for(let i=1;i<arr.length;i++){ prev=arr[i]*k+prev*(1-k); out.push(prev);} return out; };
const roundToTick = (px, tick)=> (Math.ceil(px/tick)*tick);

/* ===================== FeeGuard (unified fee model) ===================== */
function feeModelFor(symbol){
  if (isStock(symbol)){
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
    buyBps: FEE_BPS_MAKER,
    sellBps: FEE_BPS_TAKER,
    tafPerShare: 0,
    tafCap: 0,
    commissionUSD: 0,
    tick: 1e-5,
  };
}
function perShareFeeOnBuy(entryPx, model){ return entryPx * (model.buyBps/10000); }
function perShareFixedOnSell(qty, model){
  if (model.cls!=='equity') return 0;
  const fixed = Math.min(model.tafPerShare*qty, model.tafCap) + model.commissionUSD;
  return fixed / Math.max(1, qty);
}
function minExitPriceFeeAware({ symbol, entryPx, qty }){
  const model = feeModelFor(symbol);
  const minNetPerShare = Math.max(NET_MIN_PROFIT_USD/Math.max(1,qty), (NET_MIN_PROFIT_BPS/10000)*entryPx);
  const buyFeePS = perShareFeeOnBuy(entryPx, model);
  const fixedSellPS = perShareFixedOnSell(qty, model);
  const sellBpsFrac = (model.sellBps/10000);
  const raw = (entryPx + buyFeePS + fixedSellPS + minNetPerShare) / Math.max(1e-9, (1 - sellBpsFrac));
  return roundToTick(raw, model.tick);
}
function projectedNetPnlUSD({ symbol, entryPx, qty, sellPx }){
  const m = feeModelFor(symbol);
  const buyFeesUSD = qty * perShareFeeOnBuy(entryPx, m);
  const sellFeesUSD = qty * sellPx * (m.sellBps/10000) + (m.cls==='equity' ? Math.min(m.tafPerShare*qty, m.tafCap) + m.commissionUSD : 0);
  return (sellPx*qty) - sellFeesUSD - (entryPx*qty) - buyFeesUSD;
}

/* ===================== Logging (friendly) ===================== */
let logSubscriber=null, logBuffer=[]; const MAX_LOGS=200;
export const registerLogSubscriber = (fn)=>{ logSubscriber = fn; };
const logTradeAction = async (type, symbol, details={})=>{
  const timestamp = new Date().toISOString();
  const entry = { timestamp, type, symbol, ...details };
  logBuffer.push(entry); if (logBuffer.length>MAX_LOGS) logBuffer.shift();
  if (typeof logSubscriber==='function') { try{ logSubscriber(entry); }catch{} }
};
const FRIENDLY = {
  quote_ok: { sev:'info', msg:(d)=>`Quote OK (${(d.spreadBps??0).toFixed(1)} bps${d.synthetic?' • synth':''})` },
  quote_from_trade: { sev:'info', msg:(d)=>`Fallback: trade→synth (${(d.spreadBps??0).toFixed(1)} bps)` },
  quote_from_cc: { sev:'info', msg:()=>`Fallback: CryptoCompare → synth` },
  quote_from_lastgood: { sev:'info', msg:()=>`Fallback: last-good → synth` },
  quote_empty: { sev:'error', msg:()=>`Quote empty` },
  quote_exception: { sev:'error', msg:(d)=>`Quote exception: ${d.error}` },
  trade_http_error: { sev:'warn', msg:(d)=>`Alpaca trades ${d.status}${d.loc?' • '+d.loc:''}` },
  quote_http_error: { sev:'warn', msg:(d)=>`Alpaca quotes ${d.status}${d.loc?' • '+d.loc:''}${d.body?' • '+d.body:''}` },
  unsupported_symbol: { sev:'warn', msg:(d)=>`Unsupported symbol: ${d.sym}` },
  buy_camped: { sev:'info', msg:(d)=>`Camping bid @ ${d.limit}` },
  buy_replaced: { sev:'info', msg:(d)=>`Replaced bid → ${d.limit}` },
  buy_success: { sev:'success', msg:(d)=>`BUY filled qty ${d.qty} @≤${d.limit}` },
  buy_unfilled_canceled:{ sev:'warn', msg:()=>`BUY unfilled — canceled bid` },
  buy_stale_cleared:{ sev:'warn', msg:(d)=>`Cleared stale BUY (${d.ageSec}s)` },
  tp_limit_set:{ sev:'success', msg:(d)=>`TP set @ ${d.limit}` },
  tp_limit_failed:{ sev:'error', msg:()=>`TP set failed` },
  tp_limit_error:{ sev:'error', msg:(d)=>`TP set error: ${d.error}` },
  scan_start:{ sev:'info', msg:(d)=>`Scan start (batch ${d.batch})` },
  scan_summary:{ sev:'info', msg:(d)=>`Scan: ready ${d.readyCount} / attempts ${d.attemptCount} / fills ${d.successCount}` },
  scan_error:{ sev:'error', msg:(d)=>`Scan error: ${d.error}` },
  skip_wide_spread:{ sev:'warn', msg:(d)=>`Skip: spread ${d.spreadBps} bps > max` },
  skip_small_order:{ sev:'warn', msg:()=>`Skip: below min notional or funding` },
  entry_skipped:{ sev:'info', msg:(d)=>`Entry ${d.entryReady ? 'ready' : 'not ready'}${d.reason ? ' — '+d.reason : ''}` },
  risk_changed:{ sev:'info', msg:(d)=>`Risk→${d.level} (spread≤${d.spreadMax}bps)` },
  concurrency_guard:{ sev:'warn', msg:(d)=>`Concurrency guard: cap ${d.cap} @ avg ${d.avg.toFixed?.(1) ?? d.avg} bps` },
  skip_blacklist:{ sev:'warn', msg:()=>`Skip: blacklisted` },
  coarse_tick_skip:{ sev:'warn', msg:()=>`Skip: coarse-tick/sub-$0.05` },
  dust_flattened:{ sev:'info', msg:(d)=>`Dust flattened (${d.usd?.toFixed?.(2) ?? d.usd} USD)` },
  tp_touch_tick:{ sev:'info', msg:(d)=>`Touch tick ${d.count}/${TOUCH_TICKS_REQUIRED} @bid≈${d.bid?.toFixed?.(5) ?? d.bid}` },
  tp_fee_floor: { sev:'info', msg:(d)=>`FeeGuard raised TP → ${d.limit}` },
  taker_blocked_fee: { sev:'warn', msg:(d)=>`Blocked taker exit (net ${fmtUSD(d.net)} < floor ${fmtUSD(d.floor)})` },
};
function friendlyLog(entry){
  const meta = FRIENDLY[entry.type];
  if (!meta) return { sev:'info', text:`${entry.type}${entry.symbol?' '+entry.symbol:''}`, hint:null };
  const text = typeof meta.msg==='function' ? meta.msg(entry) : meta.msg;
  return { sev: meta.sev, text: `${entry.symbol ? entry.symbol+' — ' : ''}${text}`, hint:null };
}

/* ===================== Crypto/Stocks data helpers ===================== */
async function getPriceUSD_CG(cc){
  const id = CG_IDS[(cc||'').toUpperCase()];
  if (!id) return NaN;
  try{
    const r = await f(CG_PRICE(id));
    const j = await r.json().catch(()=>null);
    const v = j?.[id]?.usd;
    return Number.isFinite(v) ? v : NaN;
  }catch{ return NaN; }
}
const getBars1m = async (cc, limit=6) => {
  const sym=ccFromSymbol(cc); const k=`${sym}-${limit}`;
  const c=barsCache1m.get(k); if(c && (Date.now()-c.ts)<BAR_TTL_MS_1M) return c.data;
  const r=await f(CC_BARS(sym, limit));
  const j=await r.json().catch(()=>({}));
  const arr=Array.isArray(j?.Data?.Data)?j.Data.Data:[];
  const data=arr.map(b=>({open:b.open,high:b.high,low:b.low,close:b.close,vol:(typeof b.volumefrom==='number'?b.volumefrom:(b.volumeto??0))}));
  barsCache1m.set(k,{ts:Date.now(),data}); return data;
};
async function getPriceUSD(ccOrSymbol){
  const sym=(ccOrSymbol||'').replace('/','').replace('USD','').toUpperCase();
  try{
    const r=await f(CC_PRICE(sym));
    const j=await r.json().catch(()=>({}));
    const v = parseFloat(j?.USD ?? 'NaN');
    if (Number.isFinite(v)) return v;
  }catch{}
  return await getPriceUSD_CG(sym);
}
const buildURLCrypto = (loc, what, symbolsCSV)=>`${DATA_ROOT_CRYPTO}/${loc}/latest/${what}?symbols=${encodeURIComponent(symbolsCSV)}`;
const MAX_SYMBOLS_PER_CALL = 6;

async function alpacaQuotesAnyCrypto(symbolsCSV){
  if (Date.now() < DISABLE_ALPACA_DATA_UNTIL) return null;
  for (const loc of DATA_LOCATIONS) {
    const url = buildURLCrypto(loc,'quotes',symbolsCSV);
    const r = await f(url,{ headers: HEADERS });
    if (!r.ok){
      const body = await r.text().catch(()=> '');
      logTradeAction('quote_http_error','DATA',{status:r.status, loc, body: body?.slice(0,120)});
      if (r.status===429) DISABLE_ALPACA_DATA_UNTIL = Date.now()+60000;
      if (r.status===400) return { badRequest:true, loc };
      continue;
    }
    const j = await r.json().catch(()=>null);
    if (j?.quotes && Object.keys(j.quotes).length) return { quotes: j.quotes, loc };
  }
  return null;
}
async function alpacaTradesAnyCrypto(symbolsCSV){
  if (Date.now() < DISABLE_ALPACA_DATA_UNTIL) return null;
  for (const loc of DATA_LOCATIONS) {
    const url = buildURLCrypto(loc,'trades',symbolsCSV);
    const r = await f(url,{ headers: HEADERS });
    if (!r.ok){
      const body = await r.text().catch(()=> '');
      logTradeAction('trade_http_error','DATA',{status:r.status, loc, body: body?.slice(0,120)});
      if (r.status===429) DISABLE_ALPACA_DATA_UNTIL = Date.now()+60000;
      if (r.status===400) return { badRequest:true, loc };
      continue;
    }
    const j = await r.json().catch(()=>null);
    if (j?.trades && Object.keys(j.trades).length) return { trades: j.trades, loc };
  }
  return null;
}
async function alpacaQuoteSingleCrypto(dsym){
  if (Date.now() < DISABLE_ALPACA_DATA_UNTIL) return null;
  const resQ = await alpacaQuotesAnyCrypto(dsym);
  if (resQ?.quotes?.[dsym]?.[0]) return { dsym, q: resQ.quotes[dsym][0] };
  const resT = await alpacaTradesAnyCrypto(dsym);
  if (resT?.trades?.[dsym]?.[0]) return { dsym, t: resT.trades[dsym][0] };
  return { dsym, unsupported:true };
}

/* ===================== Stocks v2 helpers ===================== */
async function stocksLatestQuote(symbol){
  try{
    const r = await f(`${DATA_ROOT_STOCKS_V2}/quotes/latest?symbols=${encodeURIComponent(symbol)}`, { headers: HEADERS });
    if (!r.ok) return null;
    const j = await r.json().catch(()=>null);
    const q = j?.quotes?.[symbol]?.[0] || j?.quotes?.[symbol] || null;
    if (!q) return null;
    const bid = Number(q.bp ?? q.bid_price), ask = Number(q.ap ?? q.ask_price);
    const bs  = Number(q.bs ?? q.bid_size),  as  = Number(q.as ?? q.ask_size);
    if (bid>0 && ask>0) return { bid, ask, bs:Number.isFinite(bs)?bs:null, as:Number.isFinite(as)?as:null };
    return null;
  }catch{ return null; }
}
async function stocksLatestTrade(symbol){
  try{
    const r = await f(`${DATA_ROOT_STOCKS_V2}/trades/latest?symbols=${encodeURIComponent(symbol)}`, { headers: HEADERS });
    if (!r.ok) return null;
    const j = await r.json().catch(()=>null);
    const t = j?.trades?.[symbol] || j?.trades?.[symbol]?.[0] || null;
    const p = Number(t?.p ?? t?.price);
    return Number.isFinite(p)&&p>0 ? p : null;
  }catch{ return null; }
}

/* ===================== Mixed batch quotes ===================== */
async function getQuotesBatch(symbols) {
  const cryptos = symbols.filter(s=>isCrypto(s));
  const stocks  = symbols.filter(s=>isStock(s));
  const out = new Map();

  // crypto slices
  if (cryptos.length){
    const uniq = Array.from(new Set(cryptos.map(s=>toDataSymbol(s))))
      .filter(dsym => !isUnsupported(dsym.replace('/','')) && !isUnsupported(dsym));
    for (let i=0; i<uniq.length; i+=MAX_SYMBOLS_PER_CALL) {
      const slice = uniq.slice(i, i+MAX_SYMBOLS_PER_CALL);
      const csv = slice.join(',');
      let resQ = await alpacaQuotesAnyCrypto(csv);
      if (resQ?.badRequest) {
        for (const dsym of slice){
          const one = await alpacaQuoteSingleCrypto(dsym);
          const sym = dsym.replace('/','');
          if (one?.q){
            const bid = Number(one.q?.bp), ask=Number(one.q?.ap), bs=Number(one.q?.bs), as=Number(one.q?.as);
            if (bid>0 && ask>0){
              const qObj = { bid, ask, bs:Number.isFinite(bs)?bs:null, as:Number.isFinite(as)?as:null };
              out.set(sym, qObj); quoteCache.set(sym, { ts:Date.now(), q:qObj }); lastGood.set(sym, { ts:Date.now(), mid:0.5*(bid+ask) });
            }
          } else if (one?.t){
            const p = Number(one.t?.p);
            if (Number.isFinite(p)&&p>0){
              const half = halfFromBps(p,6);
              const q2 = { bid:p-half, ask:p+half };
              out.set(sym, { ...q2, bs:null, as:null });
              quoteCache.set(sym, { ts:Date.now(), q:{ ...q2, bs:null, as:null } }); lastGood.set(sym, { ts:Date.now(), mid:p });
            }
          } else { markUnsupported(sym); }
        }
      } else {
        const quotes = resQ?.quotes || {};
        for (const dsym of slice) {
          const q = quotes?.[dsym]?.[0];
          if (!q) continue;
          const bid=Number(q?.bp), ask=Number(q?.ap), bs=Number(q?.bs), as=Number(q?.as);
          if (bid>0 && ask>0){
            const sym = dsym.replace('/','');
            const qObj = { bid, ask, bs:Number.isFinite(bs)?bs:null, as:Number.isFinite(as)?as:null };
            out.set(sym, qObj); quoteCache.set(sym,{ ts:Date.now(), q:qObj }); lastGood.set(sym,{ ts:Date.now(), mid:0.5*(q.bid+q.ask) });
          }
        }
        // trades fallback
        const misses = slice.filter(dsym => !out.has(dsym.replace('/','')));
        if (misses.length){
          let resT = await alpacaTradesAnyCrypto(misses.join(','));
          const trades = resT?.trades || {};
          for (const dsym of misses){
            const p = Number(trades?.[dsym]?.[0]?.p);
            if (Number.isFinite(p)&&p>0){
              const half=halfFromBps(p,6);
              const q2={bid:p-half, ask:p+half, bs:null, as:null};
              const sym = dsym.replace('/','');
              out.set(sym, { ...q2, bs:null, as:null });
              quoteCache.set(sym,{ts:Date.now(), q:{...q2, bs:null, as:null}}); lastGood.set(sym,{ts:Date.now(), mid:p});
            } else { markUnsupported(dsym.replace('/','')); }
          }
        }
      }
    }
  }

  // stocks one-by-one (pager keeps to ~20)
  for (const s of stocks){
    if (isUnsupported(s)) continue;
    const q = await stocksLatestQuote(s);
    if (q && q.bid>0 && q.ask>0) {
      out.set(s, q); quoteCache.set(s,{ts:Date.now(), q}); lastGood.set(s,{ts:Date.now(), mid:0.5*(q.bid+q.ask)});
    } else {
      const p = await stocksLatestTrade(s);
      if (Number.isFinite(p)&&p>0){
        const half=halfFromBps(p,4);
        const q2={ bid:p-half, ask:p+half, bs:null, as:null, synthetic:true };
        out.set(s, q2); quoteCache.set(s,{ts:Date.now(), q:q2}); lastGood.set(s,{ts:Date.now(), mid:p});
      } else { markUnsupported(s); }
    }
  }
  return out;
}

/* ===================== Smart quote (mixed) ===================== */
async function getQuoteSmart(symbol, preloadedMap=null){
  try{
    if (isUnsupported(symbol)) return null;
    if (preloadedMap && preloadedMap.has(symbol)) return preloadedMap.get(symbol);
    const c = quoteCache.get(symbol); if (c && (Date.now()-c.ts)<QUOTE_TTL_MS) return c.q;

    if (isStock(symbol)){
      const q = await stocksLatestQuote(symbol);
      if (q){ quoteCache.set(symbol,{ts:Date.now(), q}); lastGood.set(symbol,{ts:Date.now(), mid:0.5*(q.bid+q.ask)}); return q; }
      const p = await stocksLatestTrade(symbol);
      if (Number.isFinite(p)&&p>0){ const half=halfFromBps(p,4); return { bid:p-half, ask:p+half, bs:null, as:null, synthetic:true }; }
      return null;
    }

    const lg = lastGood.get(symbol);
    if (lg && (Date.now()-lg.ts)<LAST_GOOD_TTL_MS) {
      const mid=lg.mid, half=halfFromBps(mid,6);
      return { bid:mid-half, ask:mid+half, bs:null, as:null, synthetic:true };
    }

    const dataSym = toDataSymbol(symbol);
    const resQ = await alpacaQuotesAnyCrypto(dataSym);
    if (resQ?.badRequest){
      const one = await alpacaQuoteSingleCrypto(dataSym);
      if (one?.q){
        const bid=Number(one.q?.bp), ask=Number(one.q?.ap), bs=Number(one.q?.bs), as=Number(one.q?.as);
        if (bid>0 && ask>0){ const qObj={bid,ask,bs:Number.isFinite(bs)?bs:null,as:Number.isFinite(as)?as:null}; quoteCache.set(symbol,{ts:Date.now(), q:qObj}); lastGood.set(symbol,{ts:Date.now(), mid:0.5*(bid+ask)}); return qObj; }
      } else if (one?.t){
        const p = Number(one.t?.p);
        if (Number.isFinite(p)&&p>0){ const half=halfFromBps(p,6); return { bid:p-half, ask:p+half, bs:null, as:null, synthetic:true }; }
      } else { markUnsupported(symbol); }
    } else if (resQ?.quotes?.[dataSym]?.[0]) {
      const q = resQ.quotes[dataSym][0];
      const bid=Number(q?.bp), ask=Number(q?.ap), bs=Number(q?.bs), as=Number(q?.as);
      if (bid>0 && ask>0){ const qObj={bid,ask,bs:Number.isFinite(bs)?bs:null,as:Number.isFinite(as)?as:null}; quoteCache.set(symbol,{ts:Date.now(), q:qObj}); lastGood.set(symbol,{ts:Date.now(), mid:0.5*(bid+ask)}); return qObj; }
    }

    const px = await getPriceUSD(ccFromSymbol(symbol));
    if (Number.isFinite(px)&&px>0){ const half=halfFromBps(px,6); return { bid:px-half, ask:px+half, bs:null, as:null, synthetic:true }; }

    const bars1 = await getBars1m(ccFromSymbol(symbol),2);
    const close = bars1?.[bars1.length-1]?.close;
    if (Number.isFinite(close)&&close>0){ const half=halfFromBps(close,6); return { bid:close-half, ask:close+half, bs:null, as:null, synthetic:true }; }

    logTradeAction('quote_empty', symbol, {});
    return null;
  }catch(e){
    logTradeAction('quote_exception', symbol, { error: e.message });
    return null;
  }
}

/* ===================== Entry math (rollback/relaxed) ===================== */
// Spread limits: slightly higher to admit more entries; EMA slope + 3-bar momo retained.
const SPREAD_MAX_BPS_BASE = 30;        // was 26; allow a touch wider
const SPREAD_EPS_BPS = 0.30;

// IMPORTANT: crypto vs equity exit floors are different.
// Use crypto bps only for crypto; keep equity tiny (bps tied to slip via FeeGuard later).
const exitFloorBps = (symbol) => (isStock(symbol) ? 1.0 : (FEE_BPS_MAKER + FEE_BPS_TAKER));

function requiredProfitBpsForSymbol(symbol, riskLevel){
  const slip = SLIP_BUFFER_BPS_BY_RISK[riskLevel] ?? SLIP_BUFFER_BPS_BY_RISK[0];
  return exitFloorBps(symbol) + 0.5 + slip;  // tiny cushion (0.5bp) like earlier working builds
}

/* ===================== Account / Orders ===================== */
const getPositionInfo = async (symbol) => {
  try {
    const res = await f(`${ALPACA_BASE_URL}/positions/${symbol}`, { headers: HEADERS });
    if (!res.ok) return null;
    const info = await res.json();
    const qty = parseFloat(info.qty ?? '0');
    const available = parseFloat(info.qty_available ?? info.available ?? info.qty ?? '0');
    const marketValue = parseFloat(info.market_value ?? info.marketValue ?? 'NaN');
    const markFromMV = Number.isFinite(marketValue)&&qty>0 ? marketValue/qty : NaN;
    const markFallback = parseFloat(info.current_price ?? info.asset_current_price ?? 'NaN');
    const mark = Number.isFinite(markFromMV) ? markFromMV : (Number.isFinite(markFallback) ? markFallback : NaN);
    const basis = parseFloat(info.avg_entry_price ?? 'NaN');
    return { qty:+(qty||0), available:+(available||0), basis:Number.isFinite(basis)?basis:null, mark:Number.isFinite(mark)?mark:null, marketValue:Number.isFinite(marketValue)?marketValue:0 };
  } catch { return null; }
};
const getAllPositions = async ()=>{ try{ const r=await f(`${ALPACA_BASE_URL}/positions`,{headers:HEADERS}); if(!r.ok) return []; const arr=await r.json(); return Array.isArray(arr)?arr:[]; }catch{ return []; } };
const getOpenOrders = async ()=>{ try{ const r=await f(`${ALPACA_BASE_URL}/orders?status=open&nested=true&limit=100`,{headers:HEADERS}); if(!r.ok) return []; const arr=await r.json(); return Array.isArray(arr)?arr:[]; }catch{ return []; } };
const cancelOpenOrdersForSymbol = async (symbol, side=null)=>{
  try{
    const open=await getOpenOrders();
    const targets=(open||[]).filter(o =>
      o.symbol===symbol &&
      (!side || (o.side||'').toLowerCase() === String(side).toLowerCase())
    );
    await Promise.all(targets.map(o=>f(`${ALPACA_BASE_URL}/orders/${o.id}`,{method:'DELETE',headers:HEADERS}).catch(()=>null)));
  }catch{}
};
const cancelAllOrders = async ()=>{ try{ const orders=await getOpenOrders(); await Promise.all((orders||[]).map(o=>f(`${ALPACA_BASE_URL}/orders/${o.id}`,{method:'DELETE',headers:HEADERS}).catch(()=>null))); }catch{} };

async function getAccountSummaryRaw(){
  const res=await f(`${ALPACA_BASE_URL}/account`,{headers:HEADERS}); if(!res.ok) throw new Error(`Account ${res.status}`);
  const a=await res.json();
  const equity=parseFloat(a.equity ?? a.portfolio_value ?? 'NaN');
  const ref1=parseFloat(a.last_equity ?? 'NaN'); const ref2=parseFloat(a.equity_previous_close ?? 'NaN');
  const ref=Number.isFinite(ref1)?ref1:(Number.isFinite(ref2)?ref2:NaN);
  const changeUsd=Number.isFinite(equity)&&Number.isFinite(ref)?(equity-ref):NaN;
  const changePct=Number.isFinite(changeUsd)&&ref>0?(changeUsd/ref)*100:NaN;
  const nmbp=parseFloat(a.non_marginable_buying_power ?? 'NaN'); const bp=parseFloat(a.buying_power ?? 'NaN'); const cash=parseFloat(a.cash ?? 'NaN');
  const buyingPower=Number.isFinite(nmbp)?nmbp:(Number.isFinite(bp)?bp:(Number.isFinite(cash)?cash:NaN));
  return { equity, buyingPower, changeUsd, changePct };
}

function capNotional(symbol, proposed, equity){
  const hardCap = ABS_MAX_NOTIONAL_USD;
  const perSymbolDynCap = (CFG_BASE.risk.maxPosPctEquity/100)*equity;
  return Math.max(0, Math.min(proposed, hardCap, perSymbolDynCap));
}

async function cleanupStaleBuyOrders(maxAgeSec=30){
  try{
    const [open, positions] = await Promise.all([getOpenOrders(), getAllPositions()]);
    const held = new Set((positions||[]).map(p=>p.symbol));
    const now = Date.now();
    const tooOld = (o)=>{ const t=Date.parse(o.submitted_at || o.created_at || o.updated_at || ''); if(!Number.isFinite(t)) return false; return ((now-t)/1000) > maxAgeSec; };
    const stale = (open||[]).filter(o => (o.side||'').toLowerCase()==='buy' && !held.has(o.symbol) && tooOld(o));
    await Promise.all(stale.map(async (o)=>{ await f(`${ALPACA_BASE_URL}/orders/${o.id}`,{method:'DELETE',headers:HEADERS}).catch(()=>null); }));
  }catch{}
}

/* ===================== Stats / re-entry memo ===================== */
const symStats = {};
const ewma = (prev,x,a=0.2)=>Number.isFinite(prev)?(a*x+(1-a)*prev):x;
function pushMFE(sym,mfe,maxKeep=120){ const s=symStats[sym] || (symStats[sym]={mfeHist:[], hitByHour:Array.from({length:24},()=>({h:0,t:0}))}); s.mfeHist.push(mfe); if (s.mfeHist.length>maxKeep) s.mfeHist.shift(); }
const reentryMemo = {};

/* ===================== App ===================== */
export default function App(){
  // dynamic universe (no static list)
  const [tracked, setTracked] = useState([]);       // [{name, symbol, cc|null}]
  const [univUpdatedAt, setUnivUpdatedAt] = useState(null);

  const [data, setData] = useState([]);
  const [refreshing, setRefreshing] = useState(false);
  const [darkMode] = useState(true);
  const autoTrade = true;

  const [notification, setNotification] = useState(null);
  const [logHistory, setLogHistory] = useState([]);

  const [isUpdatingAcct, setIsUpdatingAcct] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [acctSummary, setAcctSummary] = useState({ portfolioValue:null, buyingPower:null, dailyChangeUsd:null, dailyChangePct:null, updatedAt:null });

  const [pnlSnap, setPnlSnap] = useState({ last7Sum:null,last7UpDays:null,last7DownDays:null,last30Sum:null,fees30:null,fillsCount30:null,updatedAt:null,error:null });

  // removed risk slider UI; fixed risk level constant used throughout
  // const [riskLevel, setRiskLevel] = useState(4);  <-- removed
  const [dialsOverride, setDialsOverride] = useState({ spreadMax:null });

  const [lastScanAt, setLastScanAt] = useState(null);
  const [openMeta, setOpenMeta] = useState({ positions: 0, orders: 0, allowed: 0, universe: 0 });
  const [scanStats, setScanStats] = useState({ ready:0, attempted:0, filled:0, watch:0, skipped:0, reasons:{} });

  const scanningRef = useRef(false);
  const tradeStateRef = useRef({});
  const globalSpreadAvgRef = useRef(18);
  const touchMemoRef = useRef({});
  const stockPageRef  = useRef(0);
  const cryptoPageRef = useRef(0);

  // logs → UI
  useEffect(()=>{
    registerLogSubscriber((entry)=>{
      const f = friendlyLog(entry);
      setLogHistory(prev => [{ ts: entry.timestamp, sev: f.sev, text: f.text, hint:null }, ...prev].slice(0,22));
    });
    const seed = logBuffer.slice(-14).reverse().map(e=>{ const f=friendlyLog(e); return { ts:e.timestamp, sev:f.sev, text:f.text, hint:null }; });
    if (seed.length) setLogHistory(seed);
  },[]);
  const showNotification = (msg)=>{ setNotification(msg); setTimeout(()=>setNotification(null), 5000); };

  // discover universe on boot + periodic refresh
  useEffect(()=>{
    let stopped=false, timer=null;
    const run = async ()=>{
      try{
        const uni0 = await discoverUniverse();
        const uniSafe = (Array.isArray(uni0) && uni0.length) ? uni0 : FALLBACK_SEED;
        if (!stopped){
          setTracked(uniSafe);
          setUnivUpdatedAt(new Date().toISOString());
          const uniCount = uniSafe.filter(u=>!STABLES.has(u.symbol)).length;
          setOpenMeta(m=>({ ...m, universe: uniCount, allowed: uniCount }));
          if (!uni0?.length) logTradeAction('scan_error','UNIVERSE',{error:'empty universe — using fallback'});
          logTradeAction('scan_start','UNIVERSE',{ batch: uniSafe.length });
        }
      }catch(e){
        if (!stopped){
          setTracked(FALLBACK_SEED);
          setUnivUpdatedAt(new Date().toISOString());
          const uniCount = FALLBACK_SEED.filter(u=>!STABLES.has(u.symbol)).length;
          setOpenMeta(m=>({ ...m, universe: uniCount, allowed: uniCount }));
          logTradeAction('scan_error','UNIVERSE',{error:`discover failed → fallback (${e?.message||'err'})`});
        }
      }
      if (!stopped) timer = setTimeout(run, UNIVERSE_REFRESH_MIN*60*1000);
    };
    run();
    return ()=>{ stopped=true; if (timer) clearTimeout(timer); };
  },[]);

  // account summary helper
  const getAccountSummary = async ()=>{
    setIsUpdatingAcct(true);
    try{
      const a = await getAccountSummaryRaw();
      setAcctSummary({ portfolioValue:a.equity, buyingPower:a.buyingPower, dailyChangeUsd:a.changeUsd, dailyChangePct:a.changePct, updatedAt:new Date().toISOString() });
    } catch (e) {
      logTradeAction('quote_exception','ACCOUNT',{error:e.message});
    } finally { setIsUpdatingAcct(false); }
  };

  /* ========== Outcome monitor ========== */
  async function monitorOutcome(symbol, entryPx, v0){
    const HORIZ_MIN=3, STEP_MS=10000; let t0=Date.now(), best=0;
    while ((Date.now()-t0) < HORIZ_MIN*60*1000){
      let price=null;
      if (isStock(symbol)){ const p=await stocksLatestTrade(symbol); price = Number.isFinite(p)?p:null; }
      else { price = await getPriceUSD(ccFromSymbol(symbol)); }
      if (Number.isFinite(price)) best=Math.max(best, price-entryPx);
      await sleep(STEP_MS);
    }
    if (v0>0 && best>0){
      const g_hat = (v0*v0)/(2*best);
      const s = (symStats[symbol] ||= { hitByHour:Array.from({length:24},()=>({h:0,t:0})), mfeHist:[] });
      s.drag_g = ewma(s.drag_g, g_hat, 0.2);
      pushMFE(symbol, best);
      const hr = new Date().getUTCHours();
      const need = (requiredProfitBpsForSymbol(symbol, RISK_LEVEL)/10000)*entryPx;
      const hb = s.hitByHour[hr] || (s.hitByHour[hr]={h:0,t:0});
      hb.t += 1; if (best>=need) hb.h += 1;
    }
  }

  /* ========== Entry signal (rolled-back guards) ========== */
  const DIALS = { spreadMax: clamp(SPREAD_MAX_BPS_BASE, 12, 32) };

  async function computeEntrySignal(asset, d, riskLvl, preQuoteMap=null){
    let closes=[], bars1=[];
    if (isStock(asset.symbol)){
      const pNow = await stocksLatestTrade(asset.symbol);
      if (Number.isFinite(pNow)) closes=[pNow,pNow,pNow];
      bars1 = closes.map(c=>({close:c,high:c,low:c,open:c}));
    } else {
      bars1 = await getBars1m(asset.cc || asset.symbol, 6);
      closes = bars1.map(b=>b.close);
    }

    let q = await getQuoteSmart(asset.symbol, preQuoteMap);
    if (!q || !(q.bid>0 && q.ask>0)) {
      const last = closes?.[closes.length-1];
      if (Number.isFinite(last) && last>0) {
        const half=halfFromBps(last, isStock(asset.symbol)?4:6);
        q = { bid:last-half, ask:last+half, synthetic:true };
        if (!isStock(asset.symbol)) logTradeAction('quote_from_cc', asset.symbol, {});
      }
    }
    if (!q) return { entryReady:false, why:'noquote' };

    const mid = 0.5*(q.bid+q.ask);

    if (!isStock(asset.symbol)) {
      if (BLACKLIST.has(asset.symbol)) { logTradeAction('skip_blacklist', asset.symbol, {}); return { entryReady:false, why:'blacklist' }; }
      if (mid < MIN_PRICE_FOR_TICK_SANE_USD) { logTradeAction('coarse_tick_skip', asset.symbol, {}); return { entryReady:false, why:'coarse_tick' }; }
    }

    const spreadBps = ((q.ask-q.bid)/mid)*10000;
    logTradeAction('quote_ok', asset.symbol, { spreadBps:+spreadBps.toFixed(1), synthetic:q.synthetic===true });
    if (spreadBps > (d.spreadMax + SPREAD_EPS_BPS)) { logTradeAction('skip_wide_spread', asset.symbol, { spreadBps:+spreadBps.toFixed(1) }); return { entryReady:false, why:'spread' }; }

    // Momentum: EMA(5) up or 3-bar up (as before when fills were coming in)
    const ema5 = emaArr(closes.slice(-6),5);
    const slopeUp = ema5.length>=2 ? (ema5.at(-1) > ema5.at(-2)) : true; // default true if thin bars (stocks)
    const momo3 = (closes.length>=4 ? ((closes.at(-3) < closes.at(-2)) && (closes.at(-2) < closes.at(-1))) : true);
    const v0 = (closes.length>=2) ? (closes.at(-1) - closes.at(-2)) : 0;
    const v1 = (closes.length>=3) ? (closes.at(-2) - closes.at(-3)) : 0;
    const accelOk = (v0>=0) && (slopeUp || v0>=v1);
    if (!(momo3 && accelOk)) return { entryReady:false, why:'nomomo', spreadBps, quote:q };

    // Stocks respect market hours (kept)
    if (isStock(asset.symbol)){
      const clk = await getStockClock();
      if (!clk.is_open) return { entryReady:false, why:'market_closed', spreadBps, quote:q };
    }

    // NOTE: We intentionally removed near_range_high & cooldown/pullback gates here (rollback).

    // Fee-aware TP feasibility (approx, refined at order+TP time)
    const sst = symStats[asset.symbol] || {};
    const slipEw = Number.isFinite(sst.slipEwmaBps) ? sst.slipEwmaBps : SLIP_BUFFER_BPS_BY_RISK[riskLvl];
    const needBps = Math.max(requiredProfitBpsForSymbol(asset.symbol, riskLvl), exitFloorBps(asset.symbol) + 0.5 + slipEw);
    const tpBase = q.bid * (1 + needBps/10000);
    const ok = tpBase > q.bid * 1.00005;
    if (!ok) return { entryReady:false, why:'edge_negative', spreadBps, quote:q, v0, tpBps:needBps, tp:tpBase };

    // Track spread EWMA for widening guard (kept but light)
    (symStats[asset.symbol] ||= {}).spreadEwmaBps = ewma(symStats[asset.symbol].spreadEwmaBps, spreadBps, 0.2);

    // Lightweight “runway” estimate retained
    const drag_g = Math.max(1e-6, sst.drag_g ?? 8);
    const runway = v0>0 ? (v0*v0)/(2*drag_g) : 0;

    return { entryReady:true, spreadBps, quote:q, tpBps:needBps, tp:tpBase, v0, runway };
  }

  /* ========== Maker-first entry with fraction rules ========== */
  async function fetchAssetMeta(symbol){
    try{
      const r = await f(`${ALPACA_BASE_URL}/assets/${encodeURIComponent(symbol)}`, { headers: HEADERS });
      if(!r.ok) return null;
      return await r.json();
    }catch{ return null; }
  }
  async function placeMakerThenMaybeTakerBuy(symbol, qty, preQuoteMap=null){
    await cancelOpenOrdersForSymbol(symbol, 'buy');
    let lastOrderId=null, placedLimit=null;
    const t0=Date.now(), CAMP_SEC=15;

    while ((Date.now()-t0)/1000 < CAMP_SEC){
      const q = await getQuoteSmart(symbol, preQuoteMap);
      if (!q){ await sleep(500); continue; }
      const bidNow=q.bid, askNow=q.ask;
      if (!Number.isFinite(bidNow) || bidNow<=0){ await sleep(250); continue; }

      const TICK = isStock(symbol) ? 0.01 : 1e-5;
      const join = Number.isFinite(askNow) && askNow>0 ? Math.min(askNow-TICK, bidNow+TICK) : (bidNow+TICK);

      if (isStock(symbol)){
        const meta = await fetchAssetMeta(symbol);
        if (meta && meta.fractionable===false){
          const px = bidNow || askNow || 0;
          const whole = Math.floor(qty);
          if (whole<=0 || whole*px<CFG_BASE.risk.minNotionalUSD) return { filled:false };
          qty = whole;
        }
      }

      if (!lastOrderId || Math.abs(join-placedLimit)/Math.max(1,join) > 0.0001){
        if (lastOrderId){ try{ await f(`${ALPACA_BASE_URL}/orders/${lastOrderId}`,{method:'DELETE',headers:HEADERS}); }catch{} }
        const order = { symbol, qty, side:'buy', type:'limit', time_in_force:'gtc', limit_price: join.toFixed(isStock(symbol)?2:5) };
        try{
          const res = await f(`${ALPACA_BASE_URL}/orders`, { method:'POST', headers: HEADERS, body: JSON.stringify(order) });
          const raw = await res.text(); let data; try{ data=JSON.parse(raw);}catch{ data={raw}; }
          if (res.ok && data.id){ lastOrderId=data.id; placedLimit=join; logTradeAction(placedLimit?'buy_replaced':'buy_camped',symbol,{limit:order.limit_price}); }
        }catch(e){ logTradeAction('quote_exception', symbol, { error:e.message }); }
      }

      const pos = await getPositionInfo(symbol);
      if (pos && pos.qty>0){
        if (lastOrderId){ try{ await f(`${ALPACA_BASE_URL}/orders/${lastOrderId}`,{method:'DELETE',headers:HEADERS}); }catch{} }
        logTradeAction('buy_success', symbol, { qty:pos.qty, limit: placedLimit?.toFixed ? placedLimit.toFixed(isStock(symbol)?2:5) : placedLimit });
        return { filled:true, entry: pos.basis ?? placedLimit, qty: pos.qty };
      }
      await sleep(1200);
    }

    if (lastOrderId) { try{ await f(`${ALPACA_BASE_URL}/orders/${lastOrderId}`,{method:'DELETE',headers:HEADERS}); }catch{} logTradeAction('buy_unfilled_canceled', symbol, {}); }

    if (ENABLE_TAKER_FLIP){
      const q = await getQuoteSmart(symbol, preQuoteMap);
      if (q && q.ask>0){
        let mQty = qty;
        if (isStock(symbol)){ const meta=await fetchAssetMeta(symbol); if (meta && meta.fractionable===false) mQty=Math.floor(qty); if (mQty<=0) return { filled:false }; }
        const order = { symbol, qty:mQty, side:'buy', type:'market', time_in_force:'gtc' };
        try{
          const res = await f(`${ALPACA_BASE_URL}/orders`, { method:'POST', headers: HEADERS, body: JSON.stringify(order) });
          const raw = await res.text(); let data; try{ data=JSON.parse(raw);}catch{ data={raw}; }
          if (res.ok && data.id){ logTradeAction('buy_success', symbol, { qty:mQty, limit:'mkt' }); return { filled:true, entry:q.ask, qty:mQty }; }
        }catch{}
      }
    }
    return { filled:false };
  }

  /* ========== Take-profit posting (FeeGuard enforced + safe taker on touch) ========== */
  const TP_TIF = 'ioc';
  const SELL_EPS_BPS = 1.0;
  const ENABLE_TAKER_TP_ON_TOUCH = true;

  const ensureLimitTP = async (symbol, limitPrice)=>{
    const pos = await getPositionInfo(symbol);
    if (!pos || pos.available<=0) return;

    const state = tradeStateRef.current[symbol] || {};
    const entryPx = state.entry ?? pos.basis ?? pos.mark ?? 0;
    const qty = Number(pos.available || pos.qty || state.qty || 0);
    if (!(entryPx>0) || !(qty>0)) return;

    const feeFloor = minExitPriceFeeAware({ symbol, entryPx, qty });
    let finalLimit = Math.max(limitPrice, feeFloor);

    if (finalLimit > limitPrice + 1e-12){
      logTradeAction('tp_fee_floor', symbol, { limit: finalLimit.toFixed(isStock(symbol)?2:5) });
    }

    if (ENABLE_TAKER_TP_ON_TOUCH){
      const q = await getQuoteSmart(symbol);
      const memo = touchMemoRef.current[symbol] || (touchMemoRef.current[symbol]={count:0,lastTs:0});
      if (q && q.bid>0){
        const touchPx = finalLimit * (1 - SELL_EPS_BPS/10000);
        const touching = q.bid >= touchPx;

        if (touching){
          memo.count = (Date.now()-memo.lastTs > (CFG_BASE.exits.markRefreshSecs*2000)) ? 1 : (memo.count+1);
          memo.lastTs = Date.now();
          logTradeAction('tp_touch_tick', symbol, { count:memo.count, bid:q.bid });

          const net = projectedNetPnlUSD({ symbol, entryPx, qty, sellPx: q.bid });
          const meetsFloor = net >= NET_MIN_PROFIT_USD;
          const sizeOk = (q.bs==null) ? true : (q.bs >= MIN_BID_SIZE_LOOSE);

          if (memo.count>=TOUCH_TICKS_REQUIRED && sizeOk && meetsFloor){
            try{
              const open = await getOpenOrders();
              const ex = open.find(o => (o.side||'').toLowerCase()==='sell' && (o.type||'').toLowerCase()==='limit' && o.symbol===symbol);
              if (ex){ await f(`${ALPACA_BASE_URL}/orders/${ex.id}`,{method:'DELETE',headers:HEADERS}).catch(()=>null); }
            }catch{}
            const mkt = { symbol, qty, side:'sell', type:'market', time_in_force:'gtc' };
            try{
              const res = await f(`${ALPACA_BASE_URL}/orders`, { method:'POST', headers: HEADERS, body: JSON.stringify(mkt) });
              const raw = await res.text(); let data; try{ data=JSON.parse(raw);}catch{ data={raw}; }
              if (res.ok && data.id){ touchMemoRef.current[symbol]={count:0,lastTs:0}; logTradeAction('tp_limit_set',symbol,{limit:`TAKER@~${q.bid.toFixed?.(isStock(symbol)?2:5) ?? q.bid}`}); return; }
            }catch(e){ logTradeAction('tp_limit_error',symbol,{error:e.message}); }
          } else if (memo.count>=TOUCH_TICKS_REQUIRED && !meetsFloor){
            logTradeAction('taker_blocked_fee', symbol, { net, floor: NET_MIN_PROFIT_USD });
          }
        } else { memo.count=0; memo.lastTs=Date.now(); }
      }
    }

    const open = await getOpenOrders();
    const existing = open.find(o => (o.side||'').toLowerCase()==='sell' && (o.type||'').toLowerCase()==='limit' && o.symbol===symbol);
    const now = Date.now();
    const lastTs = state.lastLimitPostTs || 0;
    const needsPost = !existing ||
      Math.abs(parseFloat(existing.limit_price)-finalLimit)/Math.max(1,finalLimit) > 0.001 ||
      now - lastTs > CFG_BASE.exits.limitReplaceSecs*1000;

    if (!needsPost) return;

    try{
      if (existing){ await f(`${ALPACA_BASE_URL}/orders/${existing.id}`,{method:'DELETE',headers:HEADERS}).catch(()=>null); }
      const order = { symbol, qty, side:'sell', type:'limit', time_in_force: TP_TIF, limit_price: finalLimit.toFixed(isStock(symbol)?2:5) };
      const res = await f(`${ALPACA_BASE_URL}/orders`, { method:'POST', headers: HEADERS, body: JSON.stringify(order) });
      const raw = await res.text(); let data; try{ data=JSON.parse(raw);}catch{ data={raw}; }
      if (res.ok && data.id){ tradeStateRef.current[symbol]={...(state||{}), lastLimitPostTs: now}; logTradeAction('tp_limit_set',symbol,{id:data.id, limit:order.limit_price}); }
      else { logTradeAction('tp_limit_failed',symbol,{}); }
    }catch(e){ logTradeAction('tp_limit_error',symbol,{error:e.message}); }
  };

  const hasOpenBuyForSymbol = async (symbol)=>{
    try{ const open=await getOpenOrders(); return (open||[]).some(o=>(o.symbol===symbol)&&((o.side||'').toLowerCase()==='buy')); }catch{ return false; }
  };

  const concurrencyCapBySpread = (avgBps)=>{ if(!Number.isFinite(avgBps)) return 1; if (avgBps<=10) return 3; if (avgBps<=15) return 2; return 1; };

  /* ========== placeOrder (sizing + guards) ========== */
  const placeOrder = async (symbol, ccSymbol=symbol, d, sigPre=null, preQuoteMap=null)=>{
    if (!isStock(symbol) && STABLES.has(symbol)) return false;
    if (!isStock(symbol) && BLACKLIST.has(symbol)) { logTradeAction('skip_blacklist',symbol,{}); return false; }

    await cleanupStaleBuyOrders(30);

    // concurrency guard
    try{
      const allPos = await getAllPositions();
      const nonStableOpen = (allPos||[]).filter(p => Number(p.qty)>0 && Number(p.market_value||p.marketValue||0)>1).length;
      const cap = concurrencyCapBySpread(globalSpreadAvgRef.current);
      if (nonStableOpen >= cap){ logTradeAction('concurrency_guard',symbol,{cap,avg:globalSpreadAvgRef.current}); return false; }
    }catch{}

    const held = await getPositionInfo(symbol);
    if (held && Number(held.qty)>0){ logTradeAction('entry_skipped',symbol,{entryReady:false,reason:'held'}); return false; }

    const sig = sigPre || await computeEntrySignal({ symbol, cc:ccSymbol }, DIALS, RISK_LEVEL, preQuoteMap);
    if (!sig.entryReady) return false;

    let equity=acctSummary.portfolioValue, buyingPower=acctSummary.buyingPower;
    if (!Number.isFinite(equity) || !Number.isFinite(buyingPower)){ try{ const a=await getAccountSummaryRaw(); equity=a.equity; buyingPower=a.buyingPower; }catch{} }
    if (!Number.isFinite(equity) || equity<=0) equity=1000;
    if (!Number.isFinite(buyingPower) || buyingPower<=0) return false;

    const desired = Math.min(buyingPower, (CFG_BASE.risk.maxPosPctEquity/100)*equity);
    const notional = capNotional(symbol, desired, equity);
    if (!isFinite(notional) || notional < CFG_BASE.risk.minNotionalUSD){ logTradeAction('skip_small_order',symbol); return false; }

    const entryPx = sig.quote.bid;
    let qty = +(notional/entryPx).toFixed(isStock(symbol)?4:6);
    if (isStock(symbol)){ const meta=await fetchAssetMeta(symbol); if (meta && meta.fractionable===false) qty=Math.floor(qty); }
    if (!Number.isFinite(qty) || qty<=0){ logTradeAction('skip_small_order',symbol); return false; }

    const result = await placeMakerThenMaybeTakerBuy(symbol, qty, preQuoteMap);
    if (!result.filled) return false;

    const actualEntry = result.entry ?? entryPx;
    const actualQty = result.qty ?? qty;

    const approxMid = sig && sig.quote ? 0.5*(sig.quote.bid + sig.quote.ask) : actualEntry;
    const slipBps = (Number.isFinite(approxMid)&&approxMid>0) ? (((actualEntry) - (sig?.quote?.bid ?? entryPx)) / approxMid) * 10000 : 0;
    const s = (symStats[symbol] ||= { hitByHour:Array.from({length:24},()=>({h:0,t:0})), mfeHist:[] });
    s.slipEwmaBps = ewma(s.slipEwmaBps, Math.max(0, slipBps), 0.2);

    const slipEw = s.slipEwmaBps ?? SLIP_BUFFER_BPS_BY_RISK[RISK_LEVEL];
    const needBps0 = requiredProfitBpsForSymbol(symbol, RISK_LEVEL);
    const needBpsAdj = Math.max(needBps0, exitFloorBps(symbol) + 0.5 + slipEw);
    const tpBase = (actualEntry) * (1 + needBpsAdj/10000);
    const feeFloor = minExitPriceFeeAware({ symbol, entryPx: actualEntry, qty: actualQty });
    const tpCapped = Math.max(Math.min(tpBase, (actualEntry) + (sig?.runway ?? 0)), feeFloor);

    tradeStateRef.current[symbol] = { entry: actualEntry, qty: actualQty, tp: tpCapped, feeFloor, runway:(sig?.runway ?? 0), entryTs:Date.now(), lastLimitPostTs:0, wasHolding:true };
    await ensureLimitTP(symbol, tpCapped);

    monitorOutcome(symbol, actualEntry, sig?.v0 ?? 0).catch(()=>{});
    return true;
  };

  /* ========== TP maintenance loop ========== */
  useEffect(()=>{
    let timer=null;
    const run = async ()=>{
      try{
        const positions = await getAllPositions();
        for (const p of (positions||[])){
          const symbol = p.symbol;
          const qty = Number(p.qty||0);
          if (qty<=0) continue;

          const s = tradeStateRef.current[symbol] || { entry: Number(p.avg_entry_price||p.basis||0), qty: Number(p.qty||0), entryTs: Date.now(), lastLimitPostTs:0, runway:0, wasHolding:true, feeFloor: null };
          tradeStateRef.current[symbol] = s;

          const slipEw = symStats[symbol]?.slipEwmaBps ?? SLIP_BUFFER_BPS_BY_RISK[RISK_LEVEL];
          const needAdj = Math.max(requiredProfitBpsForSymbol(symbol, RISK_LEVEL), exitFloorBps(symbol) + 0.5 + slipEw);
          const entryBase = Number(s.entry || p.avg_entry_price || p.mark || 0);
          const tpBase = entryBase*(1 + needAdj/10000);
          const feeFloor = minExitPriceFeeAware({ symbol, entryPx: entryBase, qty: Number(p.available ?? p.qty ?? 0) });
          const tp = Math.max(Math.min(tpBase, entryBase + (s.runway ?? 0)), feeFloor);
          s.tp = tp;
          s.feeFloor = feeFloor;

          await ensureLimitTP(symbol, tp);
        }
      } finally {
        timer = setTimeout(run, CFG_BASE.exits.markRefreshSecs*1000);
      }
    };
    run();
    return ()=>{ if (timer) clearTimeout(timer); };
  }, []); // fixed risk level; no dependency

  /* ========== Dust sweep ========== */
  useEffect(()=>{
    let stopped=false;
    const sweep = async ()=>{
      try{
        const [positions, openOrders] = await Promise.all([getAllPositions(), getOpenOrders()]);
        const openSellBySym = new Set((openOrders||[]).filter(o => (o.side||'').toLowerCase()==='sell').map(o=>o.symbol));
        for (const p of (positions||[])){
          const sym = p.symbol;
          if (!isStock(sym) && (STABLES.has(sym) || BLACKLIST.has(sym))) continue;
          const mv = Number(p.market_value ?? p.marketValue ?? 0);
          const avail = Number(p.qty_available ?? p.available ?? p.qty ?? 0);
          if (mv>0 && mv<DUST_FLATTEN_MAX_USD && avail>0 && !openSellBySym.has(sym)){
            const mkt = { symbol:sym, qty:avail, side:'sell', type:'market', time_in_force:'gtc' };
            try{ const res=await f(`${ALPACA_BASE_URL}/orders`,{method:'POST',headers:HEADERS,body:JSON.stringify(mkt)}); if (res.ok) logTradeAction('dust_flattened',sym,{usd:mv}); }catch{}
          }
        }
      }catch{}
      if (!stopped) setTimeout(sweep, DUST_SWEEP_MINUTES*60*1000);
    };
    sweep();
    return ()=>{ stopped=true; };
  },[]);

  /* ========== Scanner (mixed rotating pagers) ========== */
  const loadData = async ()=>{
    if (scanningRef.current) return;
    scanningRef.current = true;
    setIsLoading(true);

    const effectiveTracked = (tracked && tracked.length) ? tracked : FALLBACK_SEED;

    // seed display if empty
    setData(prev => (prev && prev.length ? prev :
      effectiveTracked.map(t => ({ ...t, price:null, entryReady:false, error:null, time:new Date().toLocaleTimeString(), spreadBps:null, tpBps:null }))
    ));

    let results=[];
    try{
      await getAccountSummary();

      const positions = await getAllPositions();
      const posBySym = new Map((positions||[]).map(p=>[p.symbol,p]));
      const allOpenOrders = await getOpenOrders();

      const openCount = (positions||[]).filter(p => {
        const sym = p.symbol;
        if (STABLES.has(sym)) return false;
        const mv = parseFloat(p.market_value ?? p.marketValue ?? '0');
        const qty = parseFloat(p.qty ?? '0');
        return Number.isFinite(mv) && mv>1 && Number.isFinite(qty) && qty>0;
      }).length;

      const equities = effectiveTracked.filter(t=>isStock(t.symbol));
      const cryptos  = effectiveTracked.filter(t=>isCrypto(t.symbol) && !STABLES.has(t.symbol));

      const stockPages = Math.max(1, Math.ceil(equities.length / STOCK_PAGE_SIZE));
      const cryptoPages = Math.max(1, Math.ceil(cryptos.length / CRYPTO_PAGE_SIZE));

      const sIdx = stockPageRef.current % stockPages;
      const cIdx = cryptoPageRef.current % cryptoPages;

      const sStart = sIdx * STOCK_PAGE_SIZE;
      const cStart = cIdx * CRYPTO_PAGE_SIZE;

      const stockSlice  = equities.slice(sStart, Math.min(sStart + STOCK_PAGE_SIZE, equities.length));
      const cryptoSlice = cryptos.slice(cStart, Math.min(cStart + CRYPTO_PAGE_SIZE, cryptos.length));
      const scanList = [...stockSlice, ...cryptoSlice];

      stockPageRef.current  = (stockPageRef.current  + 1);
      cryptoPageRef.current = (cryptoPageRef.current + 1);

      setOpenMeta({ positions:openCount, orders:(allOpenOrders||[]).length, allowed: equities.length+cryptos.length, universe: equities.length+cryptos.length });

      logTradeAction('scan_start','MIXED',{batch: scanList.length});

      const batchMap = await getQuotesBatch(scanList.map(t=>t.symbol));

      let readyCount=0, attemptCount=0, successCount=0, watchCount=0, skippedCount=0;
      const reasonCounts = {};
      const spreadSamples = [];
      const d = DIALS;

      for (const asset of scanList){
        const token = { ...asset, price:null, entryReady:false, error:null, time:new Date().toLocaleTimeString(), spreadBps:null, tpBps:null };
        try{
          const p = isStock(asset.symbol) ? await stocksLatestTrade(asset.symbol) : await getPriceUSD(asset.cc || asset.symbol);
          if (Number.isFinite(p)) token.price = p;

          const prevState = tradeStateRef.current[asset.symbol] || {};
          const posNow = posBySym.get(asset.symbol);
          const isHolding = !!posNow && Number(posNow.qty)>0;
          const wasHolding = !!prevState.wasHolding;
          if (wasHolding && !isHolding){
            const lastExitPx = (prevState.tp ?? prevState.entry ?? token.price ?? null);
            (reentryMemo[asset.symbol] ||= { lastExitTs:0, lastExitPx:null });
            reentryMemo[asset.symbol].lastExitTs = Date.now();
            reentryMemo[asset.symbol].lastExitPx = lastExitPx;
          }
          tradeStateRef.current[asset.symbol] = { ...prevState, wasHolding: isHolding };

          const sig = await computeEntrySignal(asset, d, RISK_LEVEL, batchMap);
          token.entryReady = sig.entryReady;

          if (sig?.quote && sig.quote.bid>0 && sig.quote.ask>0){
            const mid = 0.5*(sig.quote.bid + sig.quote.ask);
            spreadSamples.push(((sig.quote.ask - sig.quote.bid)/mid)*10000);
          }

          if (sig.entryReady){
            token.spreadBps = sig.spreadBps ?? null;
            token.tpBps = sig.tpBps ?? null;
            readyCount++; attemptCount++;
            if (autoTrade){
              const ok = await placeOrder(asset.symbol, asset.cc, d, sig, batchMap);
              if (ok){ successCount++; }
            } else {
              logTradeAction('entry_skipped',asset.symbol,{entryReady:true,reason:'auto_off'});
            }
          } else {
            watchCount++; skippedCount++;
            if (sig?.why) reasonCounts[sig.why]=(reasonCounts[sig.why]||0)+1;
            logTradeAction('entry_skipped',asset.symbol,{entryReady:false,reason:sig.why});
          }
        }catch(err){
          token.error = err?.message || String(err);
          logTradeAction('scan_error',asset.symbol,{error:token.error});
          watchCount++; skippedCount++;
        }
        results.push(token);
      }

      const avg = spreadSamples.length ? (spreadSamples.reduce((a,b)=>a+b,0)/spreadSamples.length) : globalSpreadAvgRef.current;
      globalSpreadAvgRef.current = avg;

      setScanStats({ ready:readyCount, attempted:attemptCount, filled:successCount, watch:watchCount, skipped:skippedCount, reasons:reasonCounts });
      logTradeAction('scan_summary','MIXED',{readyCount,attemptCount,successCount});
    }catch(e){
      logTradeAction('scan_error','ALL',{error:e.message||String(e)});
    }finally{
      const bySym = new Map(results.map(r=>[r.symbol,r]));
      const display = effectiveTracked.map(t => bySym.get(t.symbol) || ({ ...t, price:null, entryReady:false, error:null, time:new Date().toLocaleTimeString(), spreadBps:null, tpBps:null }));
      setData(display);
      setLastScanAt(Date.now());
      setRefreshing(false); setIsLoading(false);
      scanningRef.current = false;
    }
  };

  // boot & periodic scan
  useEffect(()=>{
    let stopped=false;
    const tick = async ()=>{ if (!stopped) await loadData(); if (!stopped) setTimeout(tick, SCAN_MS); };
    (async ()=>{
      await getAccountSummary();
      try{
        const res = await f(`${ALPACA_BASE_URL}/account`, { headers: HEADERS });
        const account = await res.json();
        console.log('[ALPACA CONNECTED]', account.account_number, 'Equity:', account.equity);
        showNotification('✅ Connected to Alpaca');
      }catch(err){
        console.error('[ALPACA CONNECTION FAILED]', err);
        showNotification('❌ Alpaca API Error');
      }
      await loadData();
      setTimeout(tick, SCAN_MS);
    })();
    return ()=>{ stopped=true; };
  },[]);

  // (Risk slider removed; no risk_changed effect)

  // PnL panel refresh every ~15 min
  useEffect(()=>{
    let timer=null, stopped=false;
    const run=async()=>{ try{ const s=await getPnLAndFeesSnapshot(); if(!stopped) setPnlSnap({ ...s, updatedAt:new Date().toISOString(), error:null }); }catch(e){ if(!stopped) setPnlSnap(p=>({ ...p, error:e?.message||String(e) })); } finally{ if(!stopped) timer=setTimeout(run, 15*60*1000); } };
    run();
    return ()=>{ stopped=true; if (timer) clearTimeout(timer); };
  },[]);

  const onRefresh = ()=>{ setRefreshing(true); loadData(); };

  const bp = acctSummary.buyingPower, chPct = acctSummary.dailyChangePct;
  const statusColor = !lastScanAt ? '#666' : (Date.now()-lastScanAt < (SCAN_MS*1.5) ? '#57e389' : '#ffd166');

  return (
    <SafeAreaView style={styles.safe}>
      <ScrollView contentContainerStyle={[styles.container, darkMode && styles.containerDark]} refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} />}>
        {/* Header */}
        <View style={styles.header}>
          <View style={styles.headerTopRow}>
            <View style={[styles.statusDot,{backgroundColor:statusColor}]} />
            <Text style={[styles.appTitle, darkMode && styles.titleDark]}>Bullish or Bust</Text>
            <Text style={styles.versionTag}>{VERSION}</Text>
          </View>
          <Text style={styles.subTitle}>
            Open {openMeta.positions}/{openMeta.universe} • Orders {openMeta.orders} • Universe {openMeta.universe}{univUpdatedAt ? ` • U↑ ${new Date(univUpdatedAt).toLocaleTimeString()}`:''}
          </Text>
          {notification && (<View style={styles.topBanner}><Text style={styles.topBannerText}>{notification}</Text></View>)}
        </View>

        {/* Controls + Buying Power */}
        <View style={[styles.toolbar, darkMode && styles.toolbarDark]}>
          <View style={styles.topControlRow}>
            <TouchableOpacity onPress={onRefresh} style={[styles.pillToggle, styles.pillNeutral]}><Text style={styles.pillText}>Refresh</Text></TouchableOpacity>
            <TouchableOpacity onPress={cancelAllOrders} style={[styles.pillToggle, styles.btnWarn]}><Text style={styles.pillText}>Cancel Orders</Text></TouchableOpacity>
            <View style={styles.inlineBP}>
              <Text style={[styles.bpLabel, darkMode && styles.titleDark]}>Buying Power</Text>
              <Text style={[styles.bpValue, darkMode && styles.titleDark]}>
                {fmtUSD(bp)} {isUpdatingAcct && <Text style={styles.badgeUpdating}>↻</Text>}
                <Text style={styles.dot}> • </Text><Text style={styles.dayBadge}>Day {fmtPct(chPct)}</Text>
              </Text>
            </View>
          </View>
        </View>

        {/* Transaction CSV viewer */}
        <TxnHistoryCSVViewer />

        {/* PnL & Fees */}
        <View style={[styles.toolbar, darkMode && styles.toolbarDark]}>
          <Text style={styles.sectionHeader}>📉 PnL & Fees</Text>
          {pnlSnap.error ? (
            <Text style={styles.noData}>Error: {pnlSnap.error}</Text>
          ) : (
            <View style={styles.pnlRow}>
              <View style={styles.pnlBox}>
                <Text style={styles.pnlLabel}>Last 7d P/L</Text>
                <Text style={styles.pnlValue}>{Number.isFinite(pnlSnap.last7Sum) ? fmtUSD(pnlSnap.last7Sum) : '—'}</Text>
                <Text style={styles.pnlTiny}>{Number.isFinite(pnlSnap.last7UpDays) ? `${pnlSnap.last7UpDays} up` : '—'} • {Number.isFinite(pnlSnap.last7DownDays) ? `${pnlSnap.last7DownDays} down` : '—'}</Text>
              </View>
              <View style={styles.pnlBox}>
                <Text style={styles.pnlLabel}>Last 30d P/L</Text>
                <Text style={styles.pnlValue}>{Number.isFinite(pnlSnap.last30Sum) ? fmtUSD(pnlSnap.last30Sum) : '—'}</Text>
                <Text style={styles.pnlTiny}>{Number.isFinite(pnlSnap.fillsCount30) ? `${pnlSnap.fillsCount30} fills` : '—'}</Text>
              </View>
              <View style={styles.pnlBox}>
                <Text style={styles.pnlLabel}>Fees (30d)</Text>
                <Text style={styles.pnlValue}>{Number.isFinite(pnlSnap.fees30) ? fmtUSD(pnlSnap.fees30) : '—'}</Text>
                <Text style={styles.pnlTiny}>{pnlSnap.updatedAt ? new Date(pnlSnap.updatedAt).toLocaleString() : ''}</Text>
              </View>
            </View>
          )}
        </View>

        {/* Compact scan summary */}
        <View style={[styles.toolbar, darkMode && styles.toolbarDark]}>
          <Text style={styles.sectionHeader}>📊 Scan Summary</Text>
          <View style={styles.pnlRow}>
            <View style={styles.pnlBox}><Text style={styles.pnlLabel}>Active Positions</Text><Text style={styles.pnlValue}>{openMeta.positions}</Text></View>
            <View style={styles.pnlBox}><Text style={styles.pnlLabel}>Ready Now</Text><Text style={styles.pnlValue}>{scanStats.ready}</Text><Text style={styles.pnlTiny}>{scanStats.attempted} attempted • {scanStats.filled} filled</Text></View>
            <View style={styles.pnlBox}><Text style={styles.pnlLabel}>Watching (Not Ready)</Text><Text style={styles.pnlValue}>{scanStats.watch}</Text><Text style={styles.pnlTiny}>{scanStats.skipped} skipped this scan</Text></View>
          </View>
          {Object.keys(scanStats.reasons||{}).length ? (
            <Text style={[styles.pnlTiny,{marginTop:6}]}>
              Top reasons: {Object.entries(scanStats.reasons).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k,v])=>`${k} (${v})`).join(' • ')}
            </Text>
          ) : null}
        </View>

        {/* Log */}
        <View style={[styles.logPanelTop, darkMode && { backgroundColor: '#1e1e1e' }]}>
          <Text style={styles.logTitle}>Running Log</Text>
          {logHistory.length===0 ? (
            <Text style={styles.noData}>No recent events yet…</Text>
          ) : (
            logHistory.map((l,i)=>(
              <View key={i} style={styles.logRow}>
                <Text style={[styles.sevBadge, l.sev==='success'&&styles.sevSuccess, l.sev==='warn'&&styles.sevWarn, l.sev==='error'&&styles.sevError]}>{l.sev.toUpperCase()}</Text>
                <Text style={styles.logText} numberOfLines={2}>{l.text}</Text>
              </View>
            ))
          )}
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

/* ===================== Styles ===================== */
const styles = StyleSheet.create({
  safe: { flex:1, backgroundColor:'#121212' },
  container: { flexGrow:1, paddingTop:8, paddingHorizontal:10, backgroundColor:'#fff' },
  containerDark: { backgroundColor:'#121212' },

  header: { alignItems:'center', justifyContent:'center', marginBottom:6, marginTop:6 },
  headerTopRow: { flexDirection:'row', alignItems:'center', gap:6 },
  statusDot: { width:8, height:8, borderRadius:4, marginRight:6 },
  appTitle: { fontSize:16, fontWeight:'800', color:'#000' },
  versionTag: { marginLeft:8, color:'#90caf9', fontWeight:'800', fontSize:10 },
  subTitle: { marginTop:2, fontSize:11, color:'#9aa0a6' },
  titleDark: { color:'#fff' },
  topBanner: { marginTop:6, paddingVertical:6, paddingHorizontal:10, backgroundColor:'#243b55', borderRadius:8, width:'100%' },
  topBannerText:{ color:'#fff', textAlign:'center', fontWeight:'700', fontSize:12 },

  toolbar: { backgroundColor:'#f2f2f2', padding:6, borderRadius:8, marginBottom:8 },
  toolbarDark: { backgroundColor:'#1b1b1b' },

  topControlRow: { flexDirection:'row', alignItems:'center', flexWrap:'wrap', gap:8, justifyContent:'space-between' },
  pillRow: { flexDirection:'row', alignItems:'center', flexWrap:'wrap', gap:8 },

  pillToggle: { backgroundColor:'#2b2b2b', paddingVertical:6, paddingHorizontal:8, borderRadius:8 },
  pillNeutral:{ backgroundColor:'#3a3a3a' },
  btnWarn: { backgroundColor:'#6b5e23' },
  pillText:{ color:'#fff', fontSize:11, fontWeight:'800' },

  inlineBP: { flexDirection:'row', alignItems:'center', gap:8, marginLeft:'auto' },
  bpLabel: { fontSize:11, fontWeight:'600', color:'#bbb' },
  bpValue: { fontSize:13, fontWeight:'800', color:'#e6f0ff' },
  dot: { color:'#999', fontWeight:'800' },
  dayBadge: { fontWeight:'800' },
  badgeUpdating: { fontSize:10, color:'#bbb', fontWeight:'600' },

  // Txn viewer
  txnBox:{ borderWidth:1, borderColor:'#2a2a2a', padding:12, borderRadius:8, marginVertical:8, backgroundColor:'#141414' },
  txnTitle:{ fontWeight:'600', fontSize:16, marginBottom:6, color:'#e6f0ff' },
  txnBtnRow:{ flexDirection:'row', flexWrap:'wrap', gap:8 },
  txnBtn:{ paddingVertical:8, paddingHorizontal:12, backgroundColor:'#2a2a2a', borderRadius:6, marginRight:8, marginBottom:8 },
  txnBtnText:{ fontWeight:'600', color:'#e6f0ff' },
  txnStatus:{ marginTop:6, fontSize:12, opacity:0.8, color:'#c7c7c7' },
  csvHelp:{ fontSize:11, color:'#9aa0a6', marginBottom:6 },
  csvBox:{ minHeight:160, maxHeight:260, borderWidth:1, borderColor:'#2a2a2a', backgroundColor:'#0f0f0f', color:'#e6f0ff', padding:8, borderRadius:6 },

  pnlRow:{ flexDirection:'row', gap:8, justifyContent:'space-between' },
  pnlBox:{ flex:1, backgroundColor:'#141414', borderRadius:8, padding:10, borderWidth:1, borderColor:'#2a2a2a' },
  pnlLabel:{ fontSize:11, color:'#aaa', fontWeight:'700', marginBottom:2 },
  pnlValue:{ fontSize:15, color:'#e6f0ff', fontWeight:'800' },
  pnlTiny:{ fontSize:10, color:'#9aa0a6', marginTop:2 },

  noData:{ textAlign:'center', marginTop:8, fontStyle:'italic', color:'#777' },

  sectionHeader:{ fontSize:14, fontWeight:'bold', marginBottom:6, marginTop:8, color:'#cfd8dc' },

  logPanelTop:{ backgroundColor:'#222', padding:10, borderRadius:8, marginBottom:8 },
  logTitle:{ color:'#fff', fontSize:13, fontWeight:'700' },
  logRow:{ flexDirection:'row', alignItems:'center', marginBottom:4, flexWrap:'wrap' },
  sevBadge:{ fontSize:10, color:'#111', backgroundColor:'#9e9e9e', paddingHorizontal:6, paddingVertical:2, borderRadius:6, marginRight:6, fontWeight:'800' },
  sevSuccess:{ backgroundColor:'#8be78b' },
  sevWarn:{ backgroundColor:'#ffd166' },
  sevError:{ backgroundColor:'#ff6b6b' },
  logText:{ color:'#fff', fontSize:12, flexShrink:1, maxWidth:'82%' },
});
