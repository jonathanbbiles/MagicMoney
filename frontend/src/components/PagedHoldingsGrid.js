import React, { useMemo, useRef, useState } from 'react';
import { View, FlatList, StyleSheet, useWindowDimensions } from 'react-native';
import HoldingCard from './HoldingCard';

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export default function PagedHoldingsGrid({ holdings }) {
  const { width, height } = useWindowDimensions();
  const isTablet = Math.min(width, height) >= 768;

  const cols = isTablet ? 3 : 2;
  const rows = isTablet ? 3 : 3; // keep consistent, avoid cramped iPhone
  const perPage = cols * rows;

  const pages = useMemo(() => chunk(holdings || [], perPage), [holdings, perPage]);
  const [pageIndex, setPageIndex] = useState(0);
  const listRef = useRef(null);

  const cardGap = 10;
  const padding = 12;
  const cardWidth = (width - padding * 2 - cardGap * (cols - 1)) / cols;

  return (
    <View style={styles.wrap}>
      <FlatList
        ref={listRef}
        data={pages}
        keyExtractor={(_, idx) => `page_${idx}`}
        horizontal
        pagingEnabled
        showsHorizontalScrollIndicator={false}
        onMomentumScrollEnd={(e) => {
          const idx = Math.round(e.nativeEvent.contentOffset.x / width);
          setPageIndex(Math.max(0, Math.min(idx, pages.length - 1)));
        }}
        renderItem={({ item: page }) => (
          <View style={[styles.page, { width, paddingHorizontal: padding }]}>
            <View style={styles.grid}>
              {page.map((h, idx) => {
                const col = idx % cols;
                const mr = col === cols - 1 ? 0 : cardGap;
                return (
                  <View key={String(h.symbol)} style={{ width: cardWidth, marginRight: mr, marginBottom: cardGap }}>
                    <HoldingCard item={h} />
                  </View>
                );
              })}
              {/* Fill empty slots to keep layout stable */}
              {Array.from({ length: Math.max(0, perPage - page.length) }).map((_, i) => {
                const idx = page.length + i;
                const col = idx % cols;
                const mr = col === cols - 1 ? 0 : cardGap;
                return (
                  <View
                    key={`empty_${i}`}
                    style={{ width: cardWidth, marginRight: mr, marginBottom: cardGap, opacity: 0 }}
                  />
                );
              })}
            </View>
          </View>
        )}
      />
      <View style={styles.dots}>
        {pages.map((_, i) => (
          <View key={`dot_${i}`} style={[styles.dot, i === pageIndex ? styles.dotOn : styles.dotOff]} />
        ))}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: { flex: 1 },
  page: { flex: 1 },
  grid: { flex: 1, flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'flex-start' },
  dots: { flexDirection: 'row', justifyContent: 'center', marginTop: 10 },
  dot: { width: 8, height: 8, borderRadius: 99, marginHorizontal: 5 },
  dotOn: { backgroundColor: '#F2F5FF' },
  dotOff: { backgroundColor: '#2B3142' },
});
