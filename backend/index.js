require('dotenv').config();

const express = require('express');

const {
  placeMarketBuyThenSell,
  initializeInventoryFromPositions,
  submitOrder,
  fetchOrders,
  cancelOrder,
  startExitManager,
  getConcurrencyGuardStatus,
  getLastQuoteSnapshot,
  getAlpacaAuthStatus,
  getLastHttpError,
  getAlpacaConnectivityStatus,
  runDustCleanup,
  getLatestQuote,
  getLatestPrice,
  normalizeSymbolsParam,
  fetchCryptoQuotes,
  fetchCryptoTrades,
  fetchCryptoBars,
  fetchStockQuotes,
  fetchStockTrades,
  fetchStockBars,
} = require('./trade');
const { getLimiterStatus } = require('./limiters');
const { getFailureSnapshot } = require('./symbolFailures');

const app = express();

app.use(express.json());

 

// Sequentially place a limit buy order followed by a limit sell once filled

app.post('/trade', async (req, res) => {

  const { symbol } = req.body;

  try {

    const result = await placeMarketBuyThenSell(symbol);

    res.json(result);

  } catch (err) {

    console.error('Trade error:', err?.responseSnippet || err.message);

    res.status(500).json({ error: err.message });

  }

});

 

app.post('/buy', async (req, res) => {

  const { symbol, qty, side, type, time_in_force, limit_price } = req.body;

 

  try {

    const result = await submitOrder({
      symbol,
      qty,
      side,
      type,
      time_in_force,
      limit_price,
    });

    res.json(result);

  } catch (error) {

    console.error('Buy error:', error?.responseSnippet || error.message);

    res.status(500).json({ error: error.message });

  }

});

app.get('/orders', async (req, res) => {
  try {
    const orders = await fetchOrders(req.query || {});
    res.json(orders);
  } catch (error) {
    console.error('Orders fetch error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.post('/orders', async (req, res) => {
  try {
    const result = await submitOrder(req.body || {});
    res.json(result);
  } catch (error) {
    console.error('Order submit error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.delete('/orders/:id', async (req, res) => {
  try {
    const result = await cancelOrder(req.params.id);
    res.json(result || { canceled: true, id: req.params.id });
  } catch (error) {
    console.error('Order cancel error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/debug/status', async (req, res) => {
  try {
    const guardStatus = await getConcurrencyGuardStatus();
    const lastQuoteAt = getLastQuoteSnapshot();
    const authStatus = getAlpacaAuthStatus();
    res.json({
      openPositions: guardStatus.openPositions,
      openOrders: guardStatus.openOrders,
      activeSlotsUsed: guardStatus.activeSlotsUsed,
      capMax: guardStatus.capMax,
      lastScanAt: guardStatus.lastScanAt,
      lastQuoteAt,
      alpacaAuthOk: authStatus.alpacaAuthOk,
      alpacaKeyIdPresent: authStatus.alpacaKeyIdPresent,
      lastHttpError: getLastHttpError(),
      nodeVersion: process.version,
      portPresent: Boolean(process.env.PORT),
    });
  } catch (error) {
    console.error('Status debug error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/debug/net', (req, res) => {
  try {
    res.json({
      limiters: getLimiterStatus(),
      failures: getFailureSnapshot(),
    });
  } catch (error) {
    console.error('Net debug error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/debug/alpaca', async (req, res) => {
  try {
    const status = await getAlpacaConnectivityStatus();
    res.json(status);
  } catch (error) {
    console.error('Alpaca debug error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/health/alpaca', async (req, res) => {
  try {
    const status = await getAlpacaConnectivityStatus();
    res.json(status);
  } catch (error) {
    console.error('Alpaca health error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/market/quote', async (req, res) => {
  const { symbol } = req.query;
  if (!symbol) {
    return res.status(400).json({ error: 'symbol_required' });
  }
  try {
    const quote = await getLatestQuote(symbol);
    return res.json({ symbol, quote });
  } catch (error) {
    console.error('Market quote error:', error?.responseSnippet200 || error.message);
    const status = Number.isFinite(error?.statusCode) ? error.statusCode : 500;
    return res.status(status).json({
      error: error.message,
      statusCode: error?.statusCode ?? null,
      requestId: error?.requestId ?? null,
      urlHost: error?.urlHost ?? null,
      urlPath: error?.urlPath ?? null,
    });
  }
});

app.get('/market/trade', async (req, res) => {
  const { symbol } = req.query;
  if (!symbol) {
    return res.status(400).json({ error: 'symbol_required' });
  }
  try {
    const price = await getLatestPrice(symbol);
    return res.json({ symbol, price });
  } catch (error) {
    console.error('Market trade error:', error?.responseSnippet200 || error.message);
    const status = Number.isFinite(error?.statusCode) ? error.statusCode : 500;
    return res.status(status).json({
      error: error.message,
      statusCode: error?.statusCode ?? null,
      requestId: error?.requestId ?? null,
      urlHost: error?.urlHost ?? null,
      urlPath: error?.urlPath ?? null,
    });
  }
});

app.get('/market/crypto/quotes', async (req, res) => {
  const symbols = normalizeSymbolsParam(req.query.symbols);
  const location = req.query.location || req.query.loc || 'us';
  if (!symbols.length) {
    return res.status(400).json({ error: 'symbols_required' });
  }
  try {
    const payload = await fetchCryptoQuotes({ symbols, location });
    return res.json(payload || {});
  } catch (error) {
    console.error('Market crypto quotes error:', error?.responseSnippet200 || error.message);
    const status = Number.isFinite(error?.statusCode) ? error.statusCode : 500;
    return res.status(status).json({
      error: error.message,
      statusCode: error?.statusCode ?? null,
      requestId: error?.requestId ?? null,
      urlHost: error?.urlHost ?? null,
      urlPath: error?.urlPath ?? null,
    });
  }
});

app.get('/market/crypto/trades', async (req, res) => {
  const symbols = normalizeSymbolsParam(req.query.symbols);
  const location = req.query.location || req.query.loc || 'us';
  if (!symbols.length) {
    return res.status(400).json({ error: 'symbols_required' });
  }
  try {
    const payload = await fetchCryptoTrades({ symbols, location });
    return res.json(payload || {});
  } catch (error) {
    console.error('Market crypto trades error:', error?.responseSnippet200 || error.message);
    const status = Number.isFinite(error?.statusCode) ? error.statusCode : 500;
    return res.status(status).json({
      error: error.message,
      statusCode: error?.statusCode ?? null,
      requestId: error?.requestId ?? null,
      urlHost: error?.urlHost ?? null,
      urlPath: error?.urlPath ?? null,
    });
  }
});

app.get('/market/crypto/bars', async (req, res) => {
  const symbols = normalizeSymbolsParam(req.query.symbols);
  const location = req.query.location || req.query.loc || 'us';
  const limit = Number(req.query.limit || 6);
  const timeframe = req.query.timeframe || '1Min';
  if (!symbols.length) {
    return res.status(400).json({ error: 'symbols_required' });
  }
  try {
    const payload = await fetchCryptoBars({
      symbols,
      location,
      limit: Number.isFinite(limit) ? limit : 6,
      timeframe,
    });
    return res.json(payload || {});
  } catch (error) {
    console.error('Market crypto bars error:', error?.responseSnippet200 || error.message);
    const status = Number.isFinite(error?.statusCode) ? error.statusCode : 500;
    return res.status(status).json({
      error: error.message,
      statusCode: error?.statusCode ?? null,
      requestId: error?.requestId ?? null,
      urlHost: error?.urlHost ?? null,
      urlPath: error?.urlPath ?? null,
    });
  }
});

app.get('/market/stocks/quotes', async (req, res) => {
  const symbols = normalizeSymbolsParam(req.query.symbols);
  if (!symbols.length) {
    return res.status(400).json({ error: 'symbols_required' });
  }
  try {
    const payload = await fetchStockQuotes({ symbols });
    return res.json(payload || {});
  } catch (error) {
    console.error('Market stocks quotes error:', error?.responseSnippet200 || error.message);
    const status = Number.isFinite(error?.statusCode) ? error.statusCode : 500;
    return res.status(status).json({
      error: error.message,
      statusCode: error?.statusCode ?? null,
      requestId: error?.requestId ?? null,
      urlHost: error?.urlHost ?? null,
      urlPath: error?.urlPath ?? null,
    });
  }
});

app.get('/market/stocks/trades', async (req, res) => {
  const symbols = normalizeSymbolsParam(req.query.symbols);
  if (!symbols.length) {
    return res.status(400).json({ error: 'symbols_required' });
  }
  try {
    const payload = await fetchStockTrades({ symbols });
    return res.json(payload || {});
  } catch (error) {
    console.error('Market stocks trades error:', error?.responseSnippet200 || error.message);
    const status = Number.isFinite(error?.statusCode) ? error.statusCode : 500;
    return res.status(status).json({
      error: error.message,
      statusCode: error?.statusCode ?? null,
      requestId: error?.requestId ?? null,
      urlHost: error?.urlHost ?? null,
      urlPath: error?.urlPath ?? null,
    });
  }
});

app.get('/market/stocks/bars', async (req, res) => {
  const symbols = normalizeSymbolsParam(req.query.symbols);
  const limit = Number(req.query.limit || 6);
  const timeframe = req.query.timeframe || '1Min';
  if (!symbols.length) {
    return res.status(400).json({ error: 'symbols_required' });
  }
  try {
    const payload = await fetchStockBars({
      symbols,
      limit: Number.isFinite(limit) ? limit : 6,
      timeframe,
    });
    return res.json(payload || {});
  } catch (error) {
    console.error('Market stocks bars error:', error?.responseSnippet200 || error.message);
    const status = Number.isFinite(error?.statusCode) ? error.statusCode : 500;
    return res.status(status).json({
      error: error.message,
      statusCode: error?.statusCode ?? null,
      requestId: error?.requestId ?? null,
      urlHost: error?.urlHost ?? null,
      urlPath: error?.urlPath ?? null,
    });
  }
});

 

const PORT = process.env.PORT || 3000;

initializeInventoryFromPositions()

  .then((inventory) => {

    console.log(`Initialized inventory for ${inventory.size} symbols.`);

    return runDustCleanup();

  })

  .catch((err) => {

    console.error('Failed to initialize inventory', err?.responseSnippet || err.message);

  })

  .finally(() => {

    app.listen(PORT, () => {

      console.log(`Backend server running on port ${PORT}`);

    });

    startExitManager();

  });
