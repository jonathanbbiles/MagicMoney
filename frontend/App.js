import React, { useEffect, useMemo, useState } from 'react';
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
import { getBackendBaseUrl, getBackendHeaders } from './src/config/./alpaca';
import { LiveLogsCopyViewer, TxnHistoryCSVViewer } from './src/components/HistoryViewers';
import { getAccountSummaryRaw, getAllPositions, getOpenOrders, registerAlpacaLogger } from './src/services/alpacaClient';
import { fmtPct, fmtUSD } from './src/utils/format';
import { normalizePair } from './src/utils/symbols';
async function buyViaTrade(symbolRaw) {
  const symbol = String(symbolRaw || '').trim();
  if (!symbol) throw new Error('buyViaTrade: symbol required');

  const BASE = getBackendBaseUrl();
  if (!BASE) throw new Error('BACKEND_BASE_URL missing in Expo extra');

  const res = await fetch(`${BASE}/trade`, {
    method: 'POST',
    headers: getBackendHeaders(),
    body: JSON.stringify({ symbol }),
  });

  if (res.status === 404) return { _fallback: true };
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`trade_buy_failed ${res.status} ${text}`);
  }
  return res.json();
}
export default function App() {
  const [symbol, setSymbol] = useState('BTC/USD');
  const [status, setStatus] = useState('Checking backend…');
  const [busy, setBusy] = useState(false);
  const [orders, setOrders] = useState([]);
  const [positions, setPositions] = useState([]);
  const [account, setAccount] = useState(null);
  const [botStatus, setBotStatus] = useState(null);
  const [logHistory, setLogHistory] = useState([]);
  const [activityTab, setActivityTab] = useState('transactions');
  const [quickTradeCollapsed, setQuickTradeCollapsed] = useState(true);
  const [err, setErr] = useState('');

  const BASE = getBackendBaseUrl();
  const formatSignedPct = (n) => (Number.isFinite(n) ? `${n >= 0 ? '+' : ''}${fmtPct(n)}` : '—');
  const formatSignedUsd = (n) => {
    if (!Number.isFinite(n)) return '—';
    const abs = Math.abs(n);
    const formatted = abs.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return `$${n >= 0 ? '+' : '-'}${formatted}`;
  };
  const formatStatusTime = (value) => {
    if (!value) return '—';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '—';
    return date.toLocaleString();
  };
  const formatShortError = (value) => {
    if (!value) return '—';
    const text = typeof value === 'string' ? value : JSON.stringify(value);
    if (text.length <= 140) return text;
    return `${text.slice(0, 140)}…`;
  };

  async function refreshHealth() {
    setErr('');
    try {
      const res = await fetch(`${BASE}/health`, { headers: getBackendHeaders() });
      setStatus(res.ok ? 'Backend: OK' : `Backend: ${res.status}`);
    } catch (e) {
      setStatus('Backend: unreachable');
      setErr(String(e?.message || e));
    }
  }

  async function refreshOrders() {
    setErr('');
    try {
      const data = await getOpenOrders();
      setOrders(Array.isArray(data) ? data : []);
    } catch (e) {
      setErr(String(e?.message || e));
    }
  }

  async function refreshPositions() {
    setErr('');
    try {
      const data = await getAllPositions();
      setPositions(Array.isArray(data) ? data : []);
    } catch (e) {
      setErr(String(e?.message || e));
    }
  }

  async function refreshAccount() {
    setErr('');
    try {
      const data = await getAccountSummaryRaw();
      setAccount(data);
    } catch (e) {
      setErr(String(e?.message || e));
    }
  }

  async function refreshBotStatus() {
    setErr('');
    try {
      const res = await fetch(`${BASE}/debug/status`, { headers: getBackendHeaders() });
      if (!res.ok) throw new Error(`GET ${BASE}/debug/status -> ${res.status}`);
      const data = await res.json();
      setBotStatus(data);
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
          headers: getBackendHeaders(),
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
    refreshPositions();
    refreshAccount();
    refreshBotStatus();
  }, []);

  useEffect(() => {
    const handler = (type, symbolValue, details) => {
      const timestamp = new Date().toLocaleString();
      const payload = typeof details === 'string' ? details : JSON.stringify(details);
      const line = `${timestamp} • ${type || 'event'} • ${symbolValue || '—'} — ${payload}`;
      setLogHistory((prev) => {
        const next = prev.concat(line);
        if (next.length <= 300) return next;
        return next.slice(next.length - 300);
      });
    };
    registerAlpacaLogger(handler);
    return () => registerAlpacaLogger(() => {});
  }, []);

  const bpCrypto = Number(account?.cryptoBuyingPower ?? 0);
  const bpStock = Number(account?.stockBuyingPower ?? 0);
  const bpTotal = bpCrypto + bpStock;
  const summaryLine = useMemo(
    () => `Buying Power: ${fmtUSD(bpTotal)}   •   Day: ${formatSignedPct(account?.changePct)} (${formatSignedUsd(account?.changeUsd)})`,
    [bpTotal, account?.changePct, account?.changeUsd]
  );
  const tileItems = useMemo(
    () => [
      { label: 'Portfolio Value', value: fmtUSD(account?.equity) },
      { label: 'Buying Power', value: fmtUSD(bpTotal) },
      { label: 'Day %', value: formatSignedPct(account?.changePct) },
      { label: 'Day $', value: formatSignedUsd(account?.changeUsd) },
      { label: 'Open Positions', value: String(positions.length) },
      { label: 'Open Orders', value: String(orders.length) },
    ],
    [account?.equity, account?.changePct, account?.changeUsd, bpTotal, positions.length, orders.length]
  );

  return (
    <SafeAreaView style={styles.safe}>
      <View style={styles.header}>
        <Text style={styles.title}>Bullish or Bust</Text>
        <Text style={styles.centeredSummary}>{summaryLine}</Text>
        <Text style={styles.sub}>{status}</Text>
        {!!err && <Text style={styles.err}>{err}</Text>}
      </View>

      <ScrollView contentContainerStyle={styles.scrollContent}>
        <View style={styles.card}>
          <Text style={styles.cardTitle}>Dashboard</Text>
          <View style={styles.tileGrid}>
            {tileItems.map((tile) => (
              <View key={tile.label} style={styles.tile}>
                <Text style={styles.tileLabel}>{tile.label}</Text>
                <Text style={styles.tileValue}>{tile.value}</Text>
              </View>
            ))}
          </View>
        </View>

        <View style={styles.card}>
          <Text style={styles.cardTitle}>Scan / Bot Status</Text>
          <View style={styles.statusRow}>
            <Text style={styles.statusLabel}>Slots</Text>
            <Text style={styles.statusValue}>
              {botStatus?.activeSlotsUsed ?? '—'} / {botStatus?.capMax ?? '—'}
            </Text>
          </View>
          <View style={styles.statusRow}>
            <Text style={styles.statusLabel}>Open Positions</Text>
            <Text style={styles.statusValue}>{botStatus?.openPositions ?? '—'}</Text>
          </View>
          <View style={styles.statusRow}>
            <Text style={styles.statusLabel}>Open Orders</Text>
            <Text style={styles.statusValue}>{botStatus?.openOrders ?? '—'}</Text>
          </View>
          <View style={styles.statusRow}>
            <Text style={styles.statusLabel}>Last Scan</Text>
            <Text style={styles.statusValue}>{formatStatusTime(botStatus?.lastScanAt)}</Text>
          </View>
          <View style={styles.statusRow}>
            <Text style={styles.statusLabel}>Last Quote</Text>
            <Text style={styles.statusValue}>{formatStatusTime(botStatus?.lastQuoteAt)}</Text>
          </View>
          <View style={styles.statusRow}>
            <Text style={styles.statusLabel}>Alpaca Auth</Text>
            <Text style={styles.statusValue}>
              {typeof botStatus?.alpacaAuthOk === 'boolean' ? String(botStatus?.alpacaAuthOk) : '—'}
            </Text>
          </View>
          <View style={styles.statusRow}>
            <Text style={styles.statusLabel}>Last Error</Text>
            <Text style={styles.statusValue}>{formatShortError(botStatus?.lastHttpError)}</Text>
          </View>
        </View>

        <View style={styles.card}>
          <View style={styles.cardHeaderRow}>
            <Text style={styles.cardTitle}>Open Orders</Text>
            <TouchableOpacity onPress={refreshOrders} style={styles.chip}>
              <Text style={styles.chipText}>Refresh</Text>
            </TouchableOpacity>
          </View>
          {orders.length === 0 && <Text style={styles.empty}>No open orders</Text>}
          {orders.map((o) => (
            <View key={o.id} style={styles.orderRow}>
              <Text style={styles.orderTitle}>
                {o.symbol} • {o.side} • {o.status}
              </Text>
              <Text style={styles.orderMeta}>
                {o.qty || o.quantity ? `Qty ${o.qty || o.quantity}` : 'Qty —'} · {o.type || 'market'}
              </Text>
              {!!o.order_class && <Text style={styles.orderMeta}>Class: {o.order_class}</Text>}
              {Array.isArray(o.legs) &&
                o.legs.map((leg) => (
                  <Text key={leg.id} style={styles.orderMeta}>
                    ↳ {leg.side} {leg.type} {leg.limit_price || leg.stop_price || ''}
                  </Text>
                ))}
            </View>
          ))}
        </View>

        <View style={styles.card}>
          <View style={styles.cardHeaderRow}>
            <Text style={styles.cardTitle}>Activity</Text>
            <View style={styles.segmentedWrap}>
              <TouchableOpacity
                onPress={() => setActivityTab('transactions')}
                style={[styles.segmentBtn, activityTab === 'transactions' && styles.segmentBtnActive]}
              >
                <Text style={[styles.segmentText, activityTab === 'transactions' && styles.segmentTextActive]}>
                  Transactions
                </Text>
              </TouchableOpacity>
              <TouchableOpacity
                onPress={() => setActivityTab('logs')}
                style={[styles.segmentBtn, activityTab === 'logs' && styles.segmentBtnActive]}
              >
                <Text style={[styles.segmentText, activityTab === 'logs' && styles.segmentTextActive]}>Logs</Text>
              </TouchableOpacity>
            </View>
          </View>
          {activityTab === 'transactions' ? (
            <TxnHistoryCSVViewer styles={styles} />
          ) : (
            <View>
              <LiveLogsCopyViewer styles={styles} logs={logHistory} />
              <View style={styles.logList}>
                {logHistory.slice(-150).map((line, idx) => (
                  <Text key={`${idx}-${line.slice(0, 16)}`} style={styles.logLine}>
                    {line}
                  </Text>
                ))}
              </View>
            </View>
          )}
        </View>

        <View style={styles.card}>
          <TouchableOpacity
            style={styles.collapsibleHeader}
            onPress={() => setQuickTradeCollapsed((prev) => !prev)}
          >
            <Text style={styles.cardTitle}>Quick Trade</Text>
            <View style={styles.chip}>
              <Text style={styles.chipText}>{quickTradeCollapsed ? 'Expand' : 'Collapse'}</Text>
            </View>
          </TouchableOpacity>
          {!quickTradeCollapsed && (
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
          )}
          {busy && <ActivityIndicator style={{ marginTop: 12 }} />}
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: '#0b0d10' },
  header: { padding: 16 },
  title: { fontSize: 22, fontWeight: '700', color: 'white' },
  centeredSummary: { marginTop: 6, color: '#cbd5e1', textAlign: 'center', fontSize: 13 },
  sub: { marginTop: 6, color: '#9aa4af', textAlign: 'center' },
  err: { marginTop: 4, color: '#ff6b6b' },
  scrollContent: { padding: 16, paddingBottom: 32 },
  row: { flexDirection: 'row', alignItems: 'center', gap: 8, marginTop: 12 },
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
  },
  btnText: { color: 'white', fontWeight: '600' },
  card: {
    marginBottom: 12,
    padding: 12,
    backgroundColor: '#111418',
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#1b222b',
  },
  cardTitle: { color: 'white', marginBottom: 6, fontWeight: '600', fontSize: 16 },
  cardHeaderRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between' },
  empty: { color: '#9aa4af', marginTop: 6 },
  tileGrid: { flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'space-between' },
  tile: {
    width: '48%',
    padding: 10,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#1b222b',
    backgroundColor: '#0f1317',
    marginBottom: 10,
  },
  tileLabel: { color: '#9aa4af', fontSize: 12 },
  tileValue: { color: 'white', fontSize: 16, fontWeight: '600', marginTop: 4 },
  statusRow: { flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 4 },
  statusLabel: { color: '#9aa4af' },
  statusValue: { color: '#e2e8f0', flexShrink: 1, textAlign: 'right', marginLeft: 12 },
  orderRow: { paddingVertical: 8, borderBottomWidth: 1, borderBottomColor: '#1b222b' },
  orderTitle: { color: 'white', fontWeight: '600' },
  orderMeta: { color: '#cbd5e1', marginTop: 2 },
  segmentedWrap: {
    flexDirection: 'row',
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#1b222b',
    overflow: 'hidden',
  },
  segmentBtn: { paddingVertical: 6, paddingHorizontal: 12, backgroundColor: '#0f1317' },
  segmentBtnActive: { backgroundColor: '#1f2937' },
  segmentText: { color: '#9aa4af', fontWeight: '600' },
  segmentTextActive: { color: 'white' },
  collapsibleHeader: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between' },
  chip: { paddingHorizontal: 10, paddingVertical: 6, backgroundColor: '#1f2937', borderRadius: 999 },
  chipText: { color: 'white', fontSize: 12, fontWeight: '600' },
  txnBox: { marginTop: 8 },
  txnTitle: { color: 'white', fontWeight: '600' },
  txnBtnRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 8, marginTop: 8 },
  txnBtn: { paddingHorizontal: 10, paddingVertical: 6, backgroundColor: '#1f2937', borderRadius: 6 },
  txnBtnText: { color: 'white', fontSize: 12 },
  txnStatus: { color: '#9aa4af', marginTop: 6 },
  csvHelp: { color: '#9aa4af', marginTop: 6 },
  csvBox: {
    marginTop: 8,
    minHeight: 160,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#1b222b',
    color: '#e2e8f0',
    padding: 10,
    backgroundColor: '#0f1317',
  },
  logList: { marginTop: 12, borderTopWidth: 1, borderTopColor: '#1b222b', paddingTop: 8 },
  logLine: { color: '#cbd5e1', fontSize: 12, marginBottom: 4 },
});
