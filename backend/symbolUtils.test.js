const assert = require('assert/strict');

const { canonicalPair, canonicalAsset, normalizePair, alpacaSymbol } = require('./symbolUtils');

assert.equal(canonicalPair('BTCUSD'), 'BTC/USD');
assert.equal(canonicalPair('btc/usd'), 'BTC/USD');
assert.equal(canonicalPair('eth-usd'), 'ETH/USD');
assert.equal(canonicalPair('LINK'), 'LINK');

assert.equal(canonicalAsset('BTC/USD'), 'BTCUSD');
assert.equal(canonicalAsset('btc-usd'), 'BTCUSD');
assert.equal(canonicalAsset('AAPL'), 'AAPL');
assert.equal(normalizePair('BTCUSD'), 'BTC/USD');
assert.equal(normalizePair('ETH/USD'), 'ETH/USD');
assert.equal(alpacaSymbol('BTC/USD'), 'BTCUSD');
assert.equal(alpacaSymbol('ethusd'), 'ETHUSD');

console.log('symbolUtils tests passed');
