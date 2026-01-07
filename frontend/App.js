import React, { useEffect, useState } from 'react';
import {
  SafeAreaView,
  View,
  Text,
  TextInput,
  TouchableOpacity,
  ScrollView,
  StyleSheet,
  ActivityIndicator,
} from 'react-native';
import Constants from 'expo-constants';
import { buyViaTrade } from 'src/api/tradeClient';

function getBase() {
  const base =
    (Constants.expoConfig && Constants.expoConfig.extra && Constants.expoConfig.extra.BACKEND_BASE_URL) ||
    (Constants.manifest && Constants.manifest.extra && Constants.manifest.extra.BACKEND_BASE_URL) ||
    '';
  return String(base).replace(/\/+$/, '');
}
function getHeaders() {
  const token =
    (Constants.expoConfig && Constants.expoConfig.extra && Constants.expoConfig.extra.API_TOKEN) ||
    (Constants.manifest && Constants.manifest.extra && Constants.manifest.extra.API_TOKEN);
  const h = { Accept: 'application/json' };
  if (token) h.Authorization = `Bearer ${token}`;
  return h;
}
async function getJson(url) {
  const res = await fetch(url, { headers: getHeaders() });
  if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
  return res.json();
}
const normalizePair = (sym) => {
  if (!sym) return '';
  const raw = String(sym).trim().toUpperCase();
  if (raw.includes('/')) return raw;
  if (raw.endsWith('USD') && raw.length > 3) return `${raw.slice(0, -3)}/USD`;
  return raw;
};

export default function App() {
  const [symbol, setSymbol] = useState('BTC/USD');
  const [status, setStatus] = useState('Checking backend…');
  const [busy, setBusy] = useState(false);
  const [orders, setOrders] = useState([]);
  const [err, setErr] = useState('');

  const BASE = getBase();

  async function refreshHealth() {
    setErr('');
    try {
      const res = await fetch(`${BASE}/health`);
      setStatus(res.ok ? 'Backend: OK' : `Backend: ${res.status}`);
    } catch (e) {
      setStatus('Backend: unreachable');
      setErr(String(e?.message || e));
    }
  }

  async function refreshOrders() {
    setErr('');
    try {
      const data = await getJson(`${BASE}/orders?status=open&nested=true`);
      setOrders(Array.isArray(data) ? data : []);
    } catch (e) {
      setErr(String(e?.message || e));
    }
  }

  async function onBuy() {
    const pair = normalizePair(symbol);
    if (!pair) return;
    setBusy(true);
    setErr('');
    try {
      const trade = await buyViaTrade(pair);
      if (trade && trade._fallback) {
        const r = await fetch(`${BASE}/orders`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...getHeaders() },
          body: JSON.stringify({ side: 'buy', symbol: pair, type: 'market', time_in_force: 'gtc' }),
        });
        if (!r.ok) throw new Error(`legacy_buy_failed ${r.status}`);
      }
      await refreshOrders();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    refreshHealth();
    refreshOrders();
  }, []);

  return (
    <SafeAreaView style={styles.safe}>
      <View style={styles.header}>
        <Text style={styles.title}>Bullish or Bust</Text>
        <Text style={styles.sub}>{status}</Text>
        {!!err && <Text style={styles.err}>{err}</Text>}
      </View>

      <View style={styles.row}>
        <TextInput
          style={styles.input}
          value={symbol}
          onChangeText={setSymbol}
          autoCapitalize="characters"
          autoCorrect={false}
          placeholder="e.g., BTC/USD"
          placeholderTextColor="#9aa4af"
        />
        <TouchableOpacity style={styles.btn} onPress={onBuy} disabled={busy}>
          <Text style={styles.btnText}>{busy ? 'Buying…' : 'Buy /trade'}</Text>
        </TouchableOpacity>
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Open Orders</Text>
        <TouchableOpacity onPress={refreshOrders} style={styles.smallBtn}>
          <Text style={styles.smallBtnText}>Refresh</Text>
        </TouchableOpacity>
      </View>

      <ScrollView style={styles.list}>
        {orders.length === 0 && <Text style={styles.empty}>No open orders</Text>}
        {orders.map((o) => (
          <View key={o.id} style={styles.card}>
            <Text style={styles.cardTitle}>
              {o.symbol} — {o.side} — {o.status}
            </Text>
            {!!o.order_class && <Text style={styles.cardText}>class: {o.order_class}</Text>}
            {Array.isArray(o.legs) &&
              o.legs.map((leg) => (
                <Text key={leg.id} style={styles.cardText}>
                  ↳ {leg.side} {leg.type} {leg.limit_price || leg.stop_price || ''}
                </Text>
              ))}
          </View>
        ))}
      </ScrollView>

      {busy && <ActivityIndicator style={{ margin: 12 }} />}
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: '#0b0d10' },
  header: { padding: 16 },
  title: { fontSize: 22, fontWeight: '700', color: 'white' },
  sub: { marginTop: 4, color: '#9aa4af' },
  err: { marginTop: 4, color: '#ff6b6b' },
  row: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 16, gap: 8 },
  input: {
    flex: 1,
    backgroundColor: '#101418',
    color: 'white',
    padding: 12,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#1b222b',
  },
  btn: {
    paddingHorizontal: 14,
    paddingVertical: 12,
    backgroundColor: '#3b82f6',
    borderRadius: 8,
    marginLeft: 8,
  },
  btnText: { color: 'white', fontWeight: '600' },
  section: {
    marginTop: 16,
    paddingHorizontal: 16,
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
  },
  sectionTitle: { color: 'white', fontWeight: '700' },
  smallBtn: { paddingHorizontal: 10, paddingVertical: 6, backgroundColor: '#1f2937', borderRadius: 6 },
  smallBtnText: { color: 'white' },
  list: { padding: 16 },
  empty: { color: '#9aa4af' },
  card: {
    marginBottom: 12,
    padding: 12,
    backgroundColor: '#111418',
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#1b222b',
  },
  cardTitle: { color: 'white', marginBottom: 6, fontWeight: '600' },
  cardText: { color: '#cbd5e1' },
});
