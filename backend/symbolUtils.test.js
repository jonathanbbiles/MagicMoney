const assert = require('assert/strict');

const { canonicalPair, canonicalAsset } = require('./symbolUtils');

assert.equal(canonicalPair('BTCUSD'), 'BTC/USD');
assert.equal(canonicalPair('btc/usd'), 'BTC/USD');
assert.equal(canonicalPair('eth-usd'), 'ETH/USD');
assert.equal(canonicalPair('LINK'), 'LINK');

assert.equal(canonicalAsset('BTC/USD'), 'BTCUSD');
assert.equal(canonicalAsset('btc-usd'), 'BTCUSD');
assert.equal(canonicalAsset('AAPL'), 'AAPL');

console.log('symbolUtils tests passed');
