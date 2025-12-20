require('dotenv').config();

const express = require('express');

const {
  placeMarketBuyThenSell,
  initializeInventoryFromPositions,
  submitOrder,
  fetchOrders,
  cancelOrder,
} = require('./trade');

const app = express();

app.use(express.json());

 

// Sequentially place a limit buy order followed by a limit sell once filled

app.post('/trade', async (req, res) => {

  const { symbol } = req.body;

  try {

    const result = await placeMarketBuyThenSell(symbol);

    res.json(result);

  } catch (err) {

    console.error('Trade error:', err?.response?.data || err.message);

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

    console.error('Buy error:', error?.response?.data || error.message);

    res.status(500).json({ error: error.message });

  }

});

app.get('/orders', async (req, res) => {
  try {
    const orders = await fetchOrders(req.query || {});
    res.json(orders);
  } catch (error) {
    console.error('Orders fetch error:', error?.response?.data || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.post('/orders', async (req, res) => {
  try {
    const result = await submitOrder(req.body || {});
    res.json(result);
  } catch (error) {
    console.error('Order submit error:', error?.response?.data || error.message);
    res.status(500).json({ error: error.message });
  }
});

app.delete('/orders/:id', async (req, res) => {
  try {
    const result = await cancelOrder(req.params.id);
    res.json(result || { canceled: true, id: req.params.id });
  } catch (error) {
    console.error('Order cancel error:', error?.response?.data || error.message);
    res.status(500).json({ error: error.message });
  }
});

 

const PORT = process.env.PORT || 3000;

initializeInventoryFromPositions()

  .then((inventory) => {

    console.log(`Initialized inventory for ${inventory.size} symbols.`);

  })

  .catch((err) => {

    console.error('Failed to initialize inventory', err?.response?.data || err.message);

  })

  .finally(() => {

    app.listen(PORT, () => {

      console.log(`Backend server running on port ${PORT}`);

    });

  });
