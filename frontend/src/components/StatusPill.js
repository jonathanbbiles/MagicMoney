import React from 'react';
import { View, Text, StyleSheet } from 'react-native';

export default function StatusPill({ label, tone = 'neutral' }) {
  const toneStyle =
    tone === 'good' ? styles.good :
    tone === 'bad' ? styles.bad :
    tone === 'warn' ? styles.warn :
    styles.neutral;

  return (
    <View style={[styles.pill, toneStyle]}>
      <Text style={styles.text} numberOfLines={1}>{label}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  pill: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 999,
    borderWidth: 1,
    maxWidth: 220,
  },
  text: {
    color: '#EDEFF6',
    fontSize: 12,
    fontWeight: '700',
    letterSpacing: 0.3,
  },
  good: { backgroundColor: '#10381F', borderColor: '#1F7A3D' },
  bad: { backgroundColor: '#3A1414', borderColor: '#9A2E2E' },
  warn: { backgroundColor: '#33240E', borderColor: '#B57B1A' },
  neutral: { backgroundColor: '#171A22', borderColor: '#2B3142' },
});
