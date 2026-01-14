const assert = require('assert/strict');

const {
  normalizeQuoteTsMs,
  computeQuoteAgeMs,
} = require('../shared/quoteUtils');

const NOW_MS = 1_700_000_000_000;

assert.equal(normalizeQuoteTsMs(1_700_000_000), 1_700_000_000 * 1000);
assert.equal(normalizeQuoteTsMs(1_700_000_000_000), 1_700_000_000_000);
assert.equal(normalizeQuoteTsMs(1_700_000_000_000_000), 1_700_000_000_000);
assert.equal(normalizeQuoteTsMs(1_700_000_000_000_000_000), 1_700_000_000_000);
assert.equal(
  normalizeQuoteTsMs('2024-01-01T00:00:00Z'),
  Date.parse('2024-01-01T00:00:00Z')
);
assert.equal(normalizeQuoteTsMs('not-a-date'), null);
assert.equal(normalizeQuoteTsMs(null), null);

assert.equal(
  computeQuoteAgeMs({ nowMs: NOW_MS, tsMs: NOW_MS - 5_000 }),
  5_000
);
assert.equal(
  computeQuoteAgeMs({ nowMs: NOW_MS, tsMs: NOW_MS + 6_000 }),
  0
);

console.log('quoteUtils tests passed');
