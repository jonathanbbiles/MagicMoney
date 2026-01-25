const parseCsv = (value) =>
  String(value || '')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);

const getAllowedOrigins = () => parseCsv(process.env.CORS_ALLOWED_ORIGINS);

const getOriginRegex = () => {
  const raw = String(process.env.CORS_ALLOWED_ORIGIN_REGEX || '').trim();
  if (!raw) return null;
  try {
    return new RegExp(raw);
  } catch (err) {
    console.warn('cors_origin_regex_invalid', { error: err.message });
    return null;
  }
};

const isLanOrigin = (origin) => {
  if (!origin) return false;
  let parsed;
  try {
    parsed = new URL(origin);
  } catch (err) {
    return false;
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    return false;
  }
  const hostname = parsed.hostname;
  if (hostname === 'localhost' || hostname === '127.0.0.1') return true;
  if (hostname.startsWith('10.')) return true;
  if (hostname.startsWith('192.168.')) return true;
  const match172 = hostname.match(/^172\.(\d+)\./);
  if (match172) {
    const octet = Number(match172[1]);
    if (octet >= 16 && octet <= 31) return true;
  }
  return false;
};

const isAllowedOrigin = ({ origin, allowedOrigins, originRegex, allowLan }) => {
  if (!origin) return true;
  if (!allowedOrigins.length && !originRegex && !allowLan) {
    return true;
  }
  if (allowLan && isLanOrigin(origin)) {
    return true;
  }
  if (allowedOrigins.includes(origin)) {
    return true;
  }
  if (originRegex && originRegex.test(origin)) {
    return true;
  }
  return false;
};

const buildCorsError = ({ origin, allowedOrigins, originRegex, allowLan }) => {
  const details = {
    origin,
    allowedOrigins,
    originRegex: originRegex ? originRegex.toString() : null,
    allowLan,
  };
  const err = new Error(
    `CORS blocked origin "${origin}". Update CORS_ALLOWED_ORIGINS, CORS_ALLOWED_ORIGIN_REGEX, or set CORS_ALLOW_LAN=true.`
  );
  err.cors = details;
  return err;
};

const corsOptionsDelegate = (origin, callback) => {
  const allowedOrigins = getAllowedOrigins();
  const originRegex = getOriginRegex();
  const allowLan = String(process.env.CORS_ALLOW_LAN || '').toLowerCase() === 'true';
  const allowed = isAllowedOrigin({ origin, allowedOrigins, originRegex, allowLan });
  if (allowed) {
    return callback(null, true);
  }
  return callback(buildCorsError({ origin, allowedOrigins, originRegex, allowLan }));
};

module.exports = { corsOptionsDelegate };
