const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const ordersRouter = require('./routes/orders');
const pool = require('./db');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());
app.use(morgan('dev'));

// Routes
app.use('/api/orders', ordersRouter);

// Health check
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'healthy', service: 'order-service' });
  } catch (error) {
    res.status(500).json({ status: 'unhealthy', error: error.message });
  }
});

// Info
app.get('/', (req, res) => {
  res.json({
    service: 'Order Service',
    endpoints: {
      'POST /api/orders': 'Create order (writes order + outbox in single txn)',
      'POST /api/orders/:id/cancel': 'Cancel order',
      'GET /api/orders': 'List orders',
      'GET /api/orders/outbox/events': 'View outbox events',
    },
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Order Service running on http://localhost:${PORT}`);
});