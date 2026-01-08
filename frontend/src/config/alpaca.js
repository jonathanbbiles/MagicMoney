import Constants from 'expo-constants';

export const VERSION = 'v1';
const EX = Constants?.expoConfig?.extra || Constants?.manifest?.extra || {};

const getEnvValue = (value) => {
  if (typeof value !== 'string') return '';
  const trimmed = value.trim();
  return trimmed ? trimmed : '';
};

const getExtraValue = (value) => {
  if (value == null) return '';
  const trimmed = String(value).trim();
  return trimmed ? trimmed : '';
};

export const getBackendBaseUrl = () => {
  const extraUrl = getExtraValue(EX.BACKEND_BASE_URL);
  const envUrl = getEnvValue(typeof process !== 'undefined' ? process?.env?.BACKEND_BASE_URL : '');
  const baseUrl = extraUrl || envUrl || 'https://magicmoney.onrender.com';
  return String(baseUrl || '').replace(/\/+$/, '');
};

export const BACKEND_BASE_URL = getBackendBaseUrl();

export const DATA_ROOT_CRYPTO = 'https://data.alpaca.markets/v1beta3/crypto';
// IMPORTANT: your account supports 'us' for crypto data. Do not call 'global' to avoid 400s.
export const DATA_LOCATIONS = ['us'];
export const DATA_ROOT_STOCKS_V2 = 'https://data.alpaca.markets/v2/stocks';

export const BACKEND_HEADERS = {
  Accept: 'application/json',
  'Content-Type': 'application/json',
};

export const getApiToken = () => {
  const extraToken = getExtraValue(EX.API_TOKEN);
  const envToken = getEnvValue(typeof process !== 'undefined' ? process?.env?.API_TOKEN : '');
  return extraToken || envToken || '';
};

export const getBackendHeaders = () => {
  const token = getApiToken();
  const h = { Accept: 'application/json', 'Content-Type': 'application/json' };
  if (token) {
    h.Authorization = `Bearer ${token}`;
    h['x-api-key'] = token;
  }
  return h;
};

export const FEE_BPS_MAKER = 15;
export const FEE_BPS_TAKER = 25;
export const EQUITY_SEC_FEE_BPS = 0.35;
export const EQUITY_TAF_PER_SHARE = 0.000145;
export const EQUITY_TAF_CAP = 7.27;
export const EQUITY_COMMISSION_PER_TRADE_USD = 0.0;

export const SLIP_BUFFER_BPS_BY_RISK = [1, 2, 3, 4, 5];
export const STABLES = new Set(['USDTUSD', 'USDCUSD']);
export const BLACKLIST = new Set(['SHIBUSD']);
export const MIN_PRICE_FOR_TICK_SANE_USD = 0.001;
export const DUST_FLATTEN_MAX_USD = 0.75;
export const DUST_SWEEP_MINUTES = 12;
export const MIN_BID_SIZE_LOOSE = 1;
export const MIN_BID_NOTIONAL_LOOSE_USD = 5;

export const MAX_EQUITIES = 400;
export const MAX_CRYPTOS = 400;
export const QUOTE_TTL_MS = 4000;

export const DYNAMIC_MIN_PROFIT_BPS = 60;
export const EXTRA_OVER_FEES_BPS = 10;
export const SPREAD_OVER_FEES_MIN_BPS = 5;

export const DEFAULT_SETTINGS = {
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
  spreadOverFeesMinBps: 0,
  dynamicMinProfitBps: 60,
  extraOverFeesBps: 10,
  netMinProfitBps: 2.0,
  minPriceUsd: 0.001,
  slipBpsByRisk: [1, 2, 3, 4, 5],

  // Quote handling
  liveRequireQuote: true,
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
  stopLossPct: 2.0,
  stopLossBps: 80,
  hardStopLossPct: 1.8,
  stopGraceSec: 10,
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
  requireSpreadOverFees: false,

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

export const SIMPLE_SETTINGS_ONLY = true;
