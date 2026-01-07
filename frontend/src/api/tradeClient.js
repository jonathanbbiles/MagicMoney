import Constants from 'expo-constants';

function getBase() {
  const base = Constants.expoConfig?.extra?.BACKEND_BASE_URL || Constants.manifest?.extra?.BACKEND_BASE_URL;
  if (!base) throw new Error('BACKEND_BASE_URL missing');
  return base.replace(/\/+$/, '');
}

function getAuthHeader() {
  const token = Constants.expoConfig?.extra?.API_TOKEN || Constants.manifest?.extra?.API_TOKEN;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/**
 * Buy via backend /trade {symbol}, which will auto-attach OCO TP+SL server-side.
 * Returns parsed JSON response.
 */
export async function buyViaTrade(symbolRaw) {
  const symbol = String(symbolRaw).trim();
  const res = await fetch(`${getBase()}/trade`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...getAuthHeader(),
    },
    body: JSON.stringify({ symbol }),
  });
  if (res.status === 404) {
    // Allow legacy fallback (older backend) – do NOT throw. Caller will handle.
    return { _fallback: true };
  }
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`trade_buy_failed ${res.status} ${text}`);
  }
  return res.json();
}
