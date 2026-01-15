require('dotenv').config();

const express = require('express');
const cors = require('cors');
const { requireApiToken } = require('./auth');
const { rateLimit } = require('./rateLimit');

const {
  placeMakerLimitBuyThenSell,
  initializeInventoryFromPositions,
  submitOrder,
  fetchOrders,
  fetchOrderById,
  replaceOrder,
  cancelOrder,
  startEntryManager,
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
  fetchAccount,
  fetchPortfolioHistory,
  fetchActivities,
  fetchClock,
  fetchPositions,
  fetchPosition,
  fetchAsset,
  loadSupportedCryptoPairs,
  getSupportedCryptoPairsSnapshot,
  filterSupportedCryptoSymbols,
  scanOrphanPositions,
} = require('./trade');
const { getLimiterStatus } = require('./limiters');
const { getFailureSnapshot } = require('./symbolFailures');

const app = express();

app.set('trust proxy', 1);

const allowedOrigins = String(process.env.CORS_ALLOWED_ORIGINS || '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

app.use(cors({
  origin: (origin, callback) => {
    if (!origin) {
      return callback(null, true);
    }
    if (!allowedOrigins.length) {
      return callback(null, true);
    }
    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    return callback(new Error('Not allowed by CORS'));
  },
}));
app.use(express.json({ limit: '100kb' }));

const apiToken = String(process.env.API_TOKEN || '').trim();
if (!apiToken) {
  console.warn('SECURITY WARNING: API_TOKEN not set. Backend endpoints are unprotected.');
}

const VERSION =
  process.env.VERSION ||
  process.env.RENDER_GIT_COMMIT ||
  process.env.COMMIT_SHA ||
  'dev';

function extractOrderSummary(order) {
  if (!order) {
    return { orderId: null, status: null, submittedAt: null };
  }
  const orderId = order.id || order.order_id || null;
  const status = order.status || order.order_status || null;
  const submittedAt = order.submitted_at || order.submittedAt || null;
  return { orderId, status, submittedAt };
}

app.use((req, res, next) => {
  if (req.method === 'GET' && req.path === '/health') {
    return next();
  }
  return rateLimit(req, res, next);
});

app.use((req, res, next) => {
  if (req.method === 'OPTIONS') {
    return next();
  }
  if (req.method === 'GET' && req.path === '/health') {
    return next();
  }
  return requireApiToken(req, res, next);
});

app.get('/health', (req, res) => {
  res.json({ ok: true, ts: new Date().toISOString(), version: VERSION });
});

app.get('/account', async (req, res) => {
  try {
    const account = await fetchAccount();
    res.json(account);
  } catch (error) {
    console.error('Account fetch error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/account/portfolio/history', async (req, res) => {
  try {
    const history = await fetchPortfolioHistory(req.query || {});
    res.json(history);
  } catch (error) {
    console.error('Portfolio history error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/account/activities', async (req, res) => {
  try {
    const result = await fetchActivities(req.query || {});
    if (result?.nextPageToken) {
      res.set('x-next-page-token', result.nextPageToken);
    }
    res.json({ items: result?.items || [], nextPageToken: result?.nextPageToken || null });
  } catch (error) {
    console.error('Account activities error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/clock', async (req, res) => {
  try {
    const clock = await fetchClock();
    res.json(clock);
  } catch (error) {
    console.error('Clock fetch error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/positions', async (req, res) => {
  try {
    const positions = await fetchPositions();
    res.json(Array.isArray(positions) ? positions : []);
  } catch (error) {
    console.error('Positions fetch error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/diagnostics/orphans', async (req, res) => {
  try {
    const report = await scanOrphanPositions();
    res.json({
      ts: new Date().toISOString(),
      orphans: report?.orphans || [],
      positionsCount: report?.positionsCount ?? 0,
      openOrdersCount: report?.openOrdersCount ?? 0,
    });
  } catch (error) {
    console.error('Orphan diagnostics error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/positions/:symbol', async (req, res) => {
  try {
    const position = await fetchPosition(req.params.symbol);
    res.json(position || null);
  } catch (error) {
    console.error('Position fetch error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/assets/:symbol', async (req, res) => {
  try {
    const asset = await fetchAsset(req.params.symbol);
    res.json(asset || null);
  } catch (error) {
    console.error('Asset fetch error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/crypto/supported', async (req, res) => {
  try {
    await loadSupportedCryptoPairs();
    const snapshot = getSupportedCryptoPairsSnapshot();
    res.json({ pairs: snapshot.pairs || [], lastUpdated: snapshot.lastUpdated || null });
  } catch (error) {
    console.error('Supported crypto error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

// Sequentially place a limit buy order followed by a limit sell once filled

app.post('/trade', async (req, res) => {

  const { symbol } = req.body;

  try {

    const result = await placeMakerLimitBuyThenSell(symbol);

    res.json(result);

  } catch (err) {

    console.error('Trade error:', err?.responseSnippet || err.message);

    res.status(500).json({ error: err.message });

  }

});

 

app.post('/buy', async (req, res) => {

  const { symbol, qty, side, type, time_in_force, limit_price, desiredNetExitBps } = req.body;

 

  try {

    const result = await submitOrder({
      symbol,
      qty,
      side: side || 'buy',
      type,
      time_in_force,
      limit_price,
      desiredNetExitBps,
    });

    if (result?.ok) {
      const { orderId, status, submittedAt } = extractOrderSummary(result.buy);
      res.json({
        ok: true,
        orderId,
        status,
        submittedAt,
        buy: result.buy,
        sell: result.sell ?? null,
      });
      return;
    }

    if (result?.skipped) {
      res.json({
        ok: false,
        skipped: true,
        reason: result.reason,
        status: result.status ?? null,
        orderId: result.orderId ?? null,
      });
      return;
    }

    if (result?.id) {
      res.json({
        ok: true,
        orderId: result.id,
        status: result.status || result.order_status || 'accepted',
        submittedAt: result.submitted_at || result.submittedAt || null,
      });
    } else {
      res.status(500).json({
        ok: false,
        error: { message: 'Order rejected', code: null, status: 500 },
      });
    }

  } catch (error) {

    console.error('Buy error:', error?.responseSnippet || error.message);

    res.status(500).json({
      ok: false,
      error: {
        message: error.message,
        code: error?.errorCode ?? error?.code ?? null,
        status: error?.statusCode ?? 500,
      },
    });

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

app.get('/orders/:id', async (req, res) => {
  try {
    const order = await fetchOrderById(req.params.id);
    res.json(order || null);
  } catch (error) {
    console.error('Order fetch error:', error?.responseSnippet || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.post('/orders', async (req, res) => {
  try {
    const payload = req.body || {};
    const sideLower = String(payload.side || '').toLowerCase();
    const result = await submitOrder(payload);
    if (sideLower === 'buy') {
      if (result?.ok) {
        const { orderId, status, submittedAt } = extractOrderSummary(result.buy);
        res.json({
          ok: true,
          orderId,
          status,
          submittedAt,
          buy: result.buy,
          sell: result.sell ?? null,
        });
        return;
      }
      if (result?.skipped) {
        res.json({
          ok: false,
          skipped: true,
          reason: result.reason,
          status: result.status ?? null,
          orderId: result.orderId ?? null,
        });
        return;
      }
    }
    if (result?.id) {
      res.json({
        ok: true,
        orderId: result.id,
        status: result.status || result.order_status || 'accepted',
        submittedAt: result.submitted_at || result.submittedAt || null,
      });
    } else {
      res.status(500).json({
        ok: false,
        error: { message: 'Order rejected', code: null, status: 500 },
      });
    }
  } catch (error) {
    console.error('Order submit error:', error?.responseSnippet || error.message);
    res.status(500).json({
      ok: false,
      error: {
        message: error.message,
        code: error?.errorCode ?? error?.code ?? null,
        status: error?.statusCode ?? 500,
      },
    });
  }
});

app.patch('/orders/:id', async (req, res) => {
  try {
    const result = await replaceOrder(req.params.id, req.body || {});
    if (result?.id) {
      res.json({
        ok: true,
        orderId: result.id,
        status: result.status || result.order_status || 'accepted',
        order: result,
      });
    } else {
      res.status(500).json({
        ok: false,
        error: { message: 'Order replace rejected', code: null, status: 500 },
      });
    }
  } catch (error) {
    console.error('Order replace error:', error?.responseSnippet || error.message);
    res.status(500).json({
      ok: false,
      error: {
        message: error.message,
        code: error?.errorCode ?? error?.code ?? null,
        status: error?.statusCode ?? 500,
      },
    });
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
  const filtered = filterSupportedCryptoSymbols(symbols);
  if (!filtered.length) {
    return res.json({ quotes: {} });
  }
  try {
    const payload = await fetchCryptoQuotes({ symbols: filtered, location });
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
  const filtered = filterSupportedCryptoSymbols(symbols);
  if (!filtered.length) {
    return res.json({ trades: {} });
  }
  try {
    const payload = await fetchCryptoTrades({ symbols: filtered, location });
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
  const filtered = filterSupportedCryptoSymbols(symbols);
  if (!filtered.length) {
    return res.json({ bars: {} });
  }
  try {
    const payload = await fetchCryptoBars({
      symbols: filtered,
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

    return loadSupportedCryptoPairs()
      .catch((err) => {
        console.error('Supported crypto pairs preload failed', err?.message || err);
      })
      .then(() => runDustCleanup());

  })

  .catch((err) => {

    console.error('Failed to initialize inventory', err?.responseSnippet || err.message);

  })

  .finally(() => {

    app.listen(PORT, () => {

      console.log(`Backend server running on port ${PORT}`);

    });

    startEntryManager();
    startExitManager();
    console.log('exit_manager_start_attempted');

  });
