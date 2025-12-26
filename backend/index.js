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
