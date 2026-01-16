import React, { useEffect, useMemo, useRef, useState } from 'react';
import { SafeAreaView, View, Text, StyleSheet, TouchableOpacity, ActivityIndicator } from 'react-native';
import StatusPill from '../components/StatusPill';
import SummaryTile from '../components/SummaryTile';
import PagedHoldingsGrid from '../components/PagedHoldingsGrid';
import { apiGet, buildSymbolsParam, getBaseUrl } from '../api/backend';
import { formatAgo, formatUsd, pickTradePrice, safeUpper } from '../utils/format';

function useInterval(callback, delayMs) {
  const cbRef = useRef(callback);
  useEffect(() => { cbRef.current = callback; }, [callback]);
  useEffect(() => {
    if (!delayMs) return;
    const id = setInterval(() => cbRef.current && cbRef.current(), delayMs);
    return () => clearInterval(id);
  }, [delayMs]);
}

export default function DashboardScreen() {
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const [health, setHealth] = useState(null);
  const [status, setStatus] = useState(null);
  const [account, setAccount] = useState(null);
  const [positions, setPositions] = useState([]);
  const [error, setError] = useState(null);

  const lastUpdatedRef = useRef(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  const base = getBaseUrl();

  const portfolioValue = account?.portfolio_value ?? account?.equity ?? null;
  const buyingPower = account?.buying_power ?? null;

  const dayPl = useMemo(() => {
    const eq = Number(account?.equity);
    const last = Number(account?.last_equity);
    if (Number.isFinite(eq) && Number.isFinite(last)) return eq - last;
    return null;
  }, [account]);

  const holdings = useMemo(() => {
    return (positions || [])
      .map((p) => ({
        symbol: safeUpper(p.symbol),
        qty: p.qty,
        market_value: p.market_value,
        avg_entry_price: p.avg_entry_price,
        unrealized_pl: p.unrealized_pl,
        unrealized_plpc: p.unrealized_plpc,
        live_price: p.current_price, // may be overridden after trade fetch
        asset_class: p.asset_class,
      }))
      .sort((a, b) => Number(b.market_value || 0) - Number(a.market_value || 0));
  }, [positions]);

  async function refreshAll({ silent = false } = {}) {
    if (!silent) setRefreshing(true);
    setError(null);

    try {
      const [h, s, a, pos] = await Promise.all([
        apiGet('/health', { timeoutMs: 8000 }),
        apiGet('/debug/status', { timeoutMs: 10000 }),
        apiGet('/account', { timeoutMs: 12000 }),
        apiGet('/positions', { timeoutMs: 12000 }),
      ]);

      setHealth(h);
      setStatus(s);
      setAccount(a);
      setPositions(Array.isArray(pos) ? pos : []);

      // Batched live prices for holdings (via backend market data)
      const list = Array.isArray(pos) ? pos : [];
      const cryptoSyms = list.map((p) => String(p.symbol || '')).filter((s) => s.includes('/USD'));
      const stockSyms = list.map((p) => String(p.symbol || '')).filter((s) => s && !s.includes('/USD'));

      const updates = new Map(); // symbol -> price

      if (cryptoSyms.length) {
        const symbols = buildSymbolsParam(cryptoSyms);
        const resp = await apiGet(`/market/crypto/trades?symbols=${symbols}&loc=us`, { timeoutMs: 12000 }).catch(() => null);
        const trades = resp?.trades || {};
        for (const k of Object.keys(trades)) {
          const px = pickTradePrice(trades[k]);
          if (px != null) updates.set(safeUpper(k), px);
        }
      }

      if (stockSyms.length) {
        const symbols = buildSymbolsParam(stockSyms);
        const resp = await apiGet(`/market/stocks/trades?symbols=${symbols}`, { timeoutMs: 12000 }).catch(() => null);
        const trades = resp?.trades || {};
        for (const k of Object.keys(trades)) {
          const px = pickTradePrice(trades[k]);
          if (px != null) updates.set(safeUpper(k), px);
        }
      }

      if (updates.size) {
        setPositions((prev) =>
          (Array.isArray(prev) ? prev : []).map((p) => {
            const sym = safeUpper(p.symbol);
            const px = updates.get(sym);
            return px != null ? { ...p, current_price: px } : p;
          })
        );
      }

      lastUpdatedRef.current = new Date().toISOString();
      setLastUpdated(lastUpdatedRef.current);
    } catch (e) {
      setError(e?.message || String(e));
    } finally {
      if (!silent) setRefreshing(false);
      setLoading(false);
    }
  }

  useEffect(() => { refreshAll({ silent: false }); }, []);
  useInterval(() => refreshAll({ silent: true }), 8000);

  const backendOk = health?.ok === true;
  const authOk = status?.alpacaAuthOk === true;

  const headerPill = backendOk
    ? (authOk ? { tone: 'good', label: 'Backend OK' } : { tone: 'warn', label: 'Backend OK • Auth?' })
    : { tone: 'bad', label: 'Backend Down' };

  const lastError = status?.lastHttpError?.message || status?.lastHttpError || error || null;

  return (
    <SafeAreaView style={styles.safe}>
      <View style={styles.container}>
        {/* HEADER */}
        <View style={styles.header}>
          <View style={{ flex: 1 }}>
            <Text style={styles.title}>MagicMoney</Text>
            <Text style={styles.subtitle} numberOfLines={1}>
              {base} • {health?.version ? String(health.version).slice(0, 10) : '—'}
            </Text>
          </View>

          <View style={styles.headerRight}>
            <StatusPill tone={headerPill.tone} label={headerPill.label} />
            <View style={{ width: 10 }} />
            <TouchableOpacity style={styles.refreshBtn} onPress={() => refreshAll({ silent: false })} disabled={refreshing}>
              {refreshing ? <ActivityIndicator /> : <Text style={styles.refreshTxt}>Refresh</Text>}
            </TouchableOpacity>
          </View>
        </View>

        {/* SUMMARY */}
        <View style={styles.summaryRow}>
          <SummaryTile
            title="Portfolio"
            value={formatUsd(portfolioValue)}
            subValue={lastUpdated ? `Updated ${formatAgo(lastUpdated)}` : '—'}
          />
          <View style={{ width: 10 }} />
          <SummaryTile
            title="Day P/L"
            value={dayPl == null ? '—' : formatUsd(dayPl)}
            subValue={dayPl == null ? 'Needs last_equity' : (dayPl >= 0 ? 'Green day' : 'Red day')}
          />
          <View style={{ width: 10 }} />
          <SummaryTile
            title="Buying Power"
            value={formatUsd(buyingPower)}
            subValue={`Holdings ${holdings.length}`}
          />
          <View style={{ width: 10 }} />
          <SummaryTile
            title="Bot"
            value={`${status?.openPositions ?? 0} pos • ${status?.openOrders ?? 0} ord`}
            subValue={`Slots ${status?.activeSlotsUsed ?? 0}/${status?.capMax ?? '—'}`}
          />
        </View>

        {/* HOLDINGS */}
        <View style={styles.holdingsWrap}>
          <View style={styles.sectionRow}>
            <Text style={styles.sectionTitle}>Holdings</Text>
            <Text style={styles.sectionMeta} numberOfLines={1}>
              Scan {formatAgo(status?.lastScanAt)} • Quote {formatAgo(status?.lastQuoteAt)}
            </Text>
          </View>

          {loading ? (
            <View style={styles.center}>
              <ActivityIndicator size="large" />
              <Text style={styles.loadingTxt}>Loading dashboard…</Text>
            </View>
          ) : holdings.length ? (
            <PagedHoldingsGrid holdings={holdings.map((h) => ({ ...h, live_price: h.live_price }))} />
          ) : (
            <View style={styles.center}>
              <Text style={styles.emptyTitle}>No open positions</Text>
              <Text style={styles.emptySub}>Your backend reports zero holdings.</Text>
            </View>
          )}
        </View>

        {/* FOOTER */}
        <View style={styles.footer}>
          <StatusPill
            tone={lastError ? 'bad' : 'good'}
            label={lastError ? `Last error: ${String(lastError).slice(0, 120)}` : 'No backend errors reported'}
          />
        </View>
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: '#07080C' },
  container: { flex: 1, paddingHorizontal: 14, paddingTop: 10, paddingBottom: 12 },
  header: { flexDirection: 'row', alignItems: 'center', marginBottom: 12 },
  title: { color: '#F2F5FF', fontSize: 22, fontWeight: '900', letterSpacing: 0.2 },
  subtitle: { color: '#7F8AA8', marginTop: 4, fontSize: 12, fontWeight: '700' },
  headerRight: { flexDirection: 'row', alignItems: 'center' },
  refreshBtn: {
    backgroundColor: '#11131A',
    borderColor: '#23283A',
    borderWidth: 1,
    borderRadius: 14,
    paddingHorizontal: 12,
    paddingVertical: 10,
    minWidth: 92,
    alignItems: 'center',
  },
  refreshTxt: { color: '#EDEFF6', fontWeight: '900', fontSize: 12 },
  summaryRow: { flexDirection: 'row', marginBottom: 12 },
  holdingsWrap: { flex: 1, backgroundColor: '#0B0D12', borderColor: '#1E2231', borderWidth: 1, borderRadius: 18, padding: 12 },
  sectionRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 },
  sectionTitle: { color: '#F2F5FF', fontSize: 16, fontWeight: '900' },
  sectionMeta: { color: '#7F8AA8', fontSize: 12, fontWeight: '800' },
  footer: { marginTop: 10, alignItems: 'center' },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center' },
  loadingTxt: { marginTop: 12, color: '#AAB2C8', fontWeight: '800' },
  emptyTitle: { color: '#F2F5FF', fontSize: 18, fontWeight: '900' },
  emptySub: { marginTop: 6, color: '#7F8AA8', fontWeight: '800' },
});
