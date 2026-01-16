import React from 'react';
import { View, Text, StyleSheet } from 'react-native';

export default function SummaryTile({ title, value, subValue, right }) {
  return (
    <View style={styles.card}>
      <View style={styles.row}>
        <Text style={styles.title} numberOfLines={1}>{title}</Text>
        {right ? <Text style={styles.right} numberOfLines={1}>{right}</Text> : null}
      </View>
      <Text style={styles.value} numberOfLines={1}>{value}</Text>
      {subValue ? <Text style={styles.sub} numberOfLines={1}>{subValue}</Text> : <View style={{ height: 16 }} />}
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    flex: 1,
    backgroundColor: '#11131A',
    borderColor: '#23283A',
    borderWidth: 1,
    borderRadius: 16,
    padding: 12,
    minHeight: 86,
  },
  row: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  title: { color: '#AAB2C8', fontSize: 12, fontWeight: '700' },
  right: { color: '#7F8AA8', fontSize: 11, fontWeight: '700' },
  value: { color: '#F2F5FF', fontSize: 18, fontWeight: '900', marginTop: 8 },
  sub: { color: '#8E99B7', fontSize: 12, marginTop: 6, fontWeight: '700' },
});
