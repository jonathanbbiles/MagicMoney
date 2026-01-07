import Constants from 'expo-constants';

function getBase() {
  const base =
    (Constants.expoConfig && Constants.expoConfig.extra && Constants.expoConfig.extra.BACKEND_BASE_URL) ||
    (Constants.manifest && Constants.manifest.extra && Constants.manifest.extra.BACKEND_BASE_URL);
  if (!base) throw new Error('BACKEND_BASE_URL missing in Expo extra');
  return String(base).replace(/\/+$/, '');
}

function getAuthHeader() {
  const token =
    (Constants.expoConfig && Constants.expoConfig.extra && Constants.expoConfig.extra.API_TOKEN) ||
    (Constants.manifest && Constants.manifest.extra && Constants.manifest.extra.API_TOKEN);
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/** POST /trade {symbol}; returns JSON; {_fallback:true} if 404 */
export async function buyViaTrade(symbolRaw) {
  const symbol = String(symbolRaw || '').trim();
  if (!symbol) throw new Error('buyViaTrade: symbol required');

  const res = await fetch(`${getBase()}/trade`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeader() },
    body: JSON.stringify({ symbol }),
  });

  if (res.status === 404) return { _fallback: true };
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`trade_buy_failed ${res.status} ${text}`);
  }
  return res.json();
}
