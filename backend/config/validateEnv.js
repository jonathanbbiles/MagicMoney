const path = require('path');
const { getDatasetPath } = require('../recorder');

const ALPACA_KEY_ENV_VARS = ['APCA_API_KEY_ID', 'ALPACA_KEY_ID', 'ALPACA_API_KEY_ID', 'ALPACA_API_KEY'];
const ALPACA_SECRET_ENV_VARS = ['APCA_API_SECRET_KEY', 'ALPACA_SECRET_KEY', 'ALPACA_API_SECRET_KEY'];

const maskSecret = (value) => {
  if (!value) return '';
  const str = String(value);
  if (str.length <= 4) return '****';
  return `${'*'.repeat(Math.min(6, str.length - 4))}${str.slice(-4)}`;
};

const parseCsv = (value) =>
  String(value || '')
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);

const assertValidUrl = (value, label, errors) => {
  if (!value) return;
  try {
    // eslint-disable-next-line no-new
    new URL(value);
  } catch (err) {
    errors.push(`${label} must be a valid URL (got "${value}")`);
  }
};

const validateOrigins = (origins, errors) => {
  for (const origin of origins) {
    try {
      // eslint-disable-next-line no-new
      new URL(origin);
    } catch (err) {
      errors.push(`CORS_ALLOWED_ORIGINS entry "${origin}" is not a valid URL`);
    }
  }
};

const checkApiTokenLength = (warnings) => {
  const token = String(process.env.API_TOKEN || '').trim();
  if (token && token.length < 12) {
    warnings.push('API_TOKEN is set but shorter than 12 characters. Consider using a longer token.');
  }
};

const detectAlpacaKeys = () => {
  const keyId = ALPACA_KEY_ENV_VARS.find((name) => Boolean(process.env[name]));
  const secretKey = ALPACA_SECRET_ENV_VARS.find((name) => Boolean(process.env[name]));
  return { keyId, secretKey };
};

const summarizeCors = ({ allowedOrigins, allowLan, originRegex }) => {
  return {
    allowedOriginsCount: allowedOrigins.length,
    allowLan,
    originRegexSet: Boolean(originRegex),
  };
};

const getResolvedTradeBase = () =>
  String(process.env.TRADE_BASE || process.env.ALPACA_API_BASE || '').trim();

const getResolvedDataBase = () =>
  String(process.env.DATA_BASE || '').trim();

const warnDatasetPath = ({ datasetPath, warnings }) => {
  const rawDatasetDir = String(process.env.DATASET_DIR || '').trim();
  if (!rawDatasetDir) return;
  const isAbsolute = path.isAbsolute(rawDatasetDir);
  const isRender = Boolean(process.env.RENDER || process.env.RENDER_EXTERNAL_URL);
  if (!isAbsolute && isRender) {
    warnings.push(
      `DATASET_DIR is relative ("${rawDatasetDir}"). Render filesystems are ephemeral unless you mount a disk.`
    );
  }
};

const validateEnv = () => {
  const errors = [];
  const warnings = [];

  const { keyId, secretKey } = detectAlpacaKeys();
  if (!keyId || !secretKey) {
    const missing = [];
    if (!keyId) missing.push('Alpaca API key id');
    if (!secretKey) missing.push('Alpaca secret key');
    warnings.push(
      `Missing ${missing.join(' and ')}. trade.js will fail fast if Alpaca credentials are absent.`
    );
  }

  const tradeBase = getResolvedTradeBase();
  const dataBase = getResolvedDataBase();
  assertValidUrl(tradeBase, 'TRADE_BASE/ALPACA_API_BASE', errors);
  assertValidUrl(dataBase, 'DATA_BASE', errors);

  const allowedOrigins = parseCsv(process.env.CORS_ALLOWED_ORIGINS);
  validateOrigins(allowedOrigins, errors);

  const originRegexRaw = String(process.env.CORS_ALLOWED_ORIGIN_REGEX || '').trim();
  let originRegex = null;
  if (originRegexRaw) {
    try {
      originRegex = new RegExp(originRegexRaw);
    } catch (err) {
      errors.push(`CORS_ALLOWED_ORIGIN_REGEX is invalid: ${err.message}`);
    }
  }

  checkApiTokenLength(warnings);

  const datasetPath = getDatasetPath();
  warnDatasetPath({ datasetPath, warnings });

  if (errors.length) {
    const summary = errors.map((err) => `- ${err}`).join('\n');
    throw new Error(`Invalid environment configuration:\n${summary}`);
  }

  const effectiveConfig = {
    tradeBase: tradeBase || '(default)',
    dataBase: dataBase || '(default)',
    apiToken: process.env.API_TOKEN ? maskSecret(process.env.API_TOKEN) : '(not set)',
    apiTokenLength: process.env.API_TOKEN ? String(process.env.API_TOKEN).length : 0,
    cors: summarizeCors({
      allowedOrigins,
      allowLan: String(process.env.CORS_ALLOW_LAN || '').toLowerCase() === 'true',
      originRegex,
    }),
    datasetPath,
  };

  console.log('effective_config', effectiveConfig);
  if (warnings.length) {
    warnings.forEach((warning) => console.warn('config_warning', warning));
  }
};

module.exports = { validateEnv };
