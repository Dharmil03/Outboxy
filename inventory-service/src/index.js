const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const pool = require('./db');
const { startConsumer } = require('./consumer');

const app = express();
const PORT = process.env.PORT || 3002;

app.use(cors());
app.use(express.json());
app.use(morgan('dev'));

// GET /api/inventory - List all products
app.get('/api/inventory', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM products ORDER BY name');
    res.json({ products: result.rows, count: result.rowCount });
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch inventory' });
  }
});

// GET /api/inventory/:id - Single product
app.get('/api/inventory/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM products WHERE id = \$1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    res.json({ product: result.rows[0] });
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch product' });
  }
});

// GET /api/inventory/events/processed - Processed events log
app.get('/api/inventory/events/processed', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM processed_events ORDER BY processed_at DESC LIMIT 50'
    );
    res.json({ events: result.rows, count: result.rowCount });
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch events' });
  }
});

app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'healthy', service: 'inventory-service' });
  } catch (error) {
    res.status(500).json({ status: 'unhealthy', error: error.message });
  }
});

// Info
app.get('/', (req, res) => {
  res.json({
    service: 'Inventory Service',
    role: 'Kafka Consumer - processes CDC events from Debezium',
    endpoints: {
      'GET /api/inventory': 'List products + stock',
      'GET /api/inventory/:id': 'Single product',
      'GET /api/inventory/events/processed': 'Processed events log',
    },
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Inventory Service running on http://localhost:${PORT}`);
});

startConsumer().catch(console.error);