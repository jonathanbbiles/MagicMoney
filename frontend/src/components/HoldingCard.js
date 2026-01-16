import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { formatUsd, formatNum } from '../utils/format';

export default function HoldingCard({ item }) {
  const symbol = item?.symbol || '—';
  const qty = item?.qty;
  const mv = item?.market_value;
  const avg = item?.avg_entry_price;
  const upl = item?.unrealized_pl;
  const price = item?.live_price;

  const uplNum = Number(upl);
  const tone = Number.isFinite(uplNum) ? (uplNum > 0 ? styles.good : uplNum < 0 ? styles.bad : styles.neutral) : styles.neutral;

  return (
    <View style={styles.card}>
      <View style={styles.topRow}>
        <Text style={styles.symbol} numberOfLines={1}>{symbol}</Text>
        <Text style={[styles.pnl, tone]} numberOfLines={1}>
          {Number.isFinite(uplNum) ? formatUsd(uplNum) : '—'}
        </Text>
      </View>

      <View style={styles.grid}>
        <View style={styles.cell}>
          <Text style={styles.k}>Qty</Text>
          <Text style={styles.v} numberOfLines={1}>{formatNum(qty)}</Text>
        </View>
        <View style={styles.cell}>
          <Text style={styles.k}>Value</Text>
          <Text style={styles.v} numberOfLines={1}>{formatUsd(mv)}</Text>
        </View>
        <View style={styles.cell}>
          <Text style={styles.k}>Avg</Text>
          <Text style={styles.v} numberOfLines={1}>{Number.isFinite(Number(avg)) ? formatUsd(avg) : '—'}</Text>
        </View>
        <View style={styles.cell}>
          <Text style={styles.k}>Live</Text>
          <Text style={styles.v} numberOfLines={1}>{Number.isFinite(Number(price)) ? formatUsd(price) : '—'}</Text>
        </View>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: '#0E1016',
    borderColor: '#23283A',
    borderWidth: 1,
    borderRadius: 16,
    padding: 12,
  },
  topRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  symbol: { color: '#F2F5FF', fontSize: 16, fontWeight: '900' },
  pnl: { fontSize: 13, fontWeight: '900' },
  good: { color: '#62D28B' },
  bad: { color: '#FF6B6B' },
  neutral: { color: '#AAB2C8' },
  grid: { marginTop: 10, flexDirection: 'row', flexWrap: 'wrap' },
  cell: { width: '50%', paddingVertical: 6 },
  k: { color: '#7F8AA8', fontSize: 11, fontWeight: '800' },
  v: { color: '#EDEFF6', fontSize: 12, fontWeight: '800', marginTop: 2 },
});
