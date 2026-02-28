const express = require('express');
const { v4: uuidv4 } = require('uuid');
const pool = require('../db');

const router = express.Router();

router.post('/', async (req, res) => {
  const client = await pool.connect();

  try {
    const { customerName, productId, productName, quantity, price } = req.body;

    // Validate
    if (!customerName || !productId || !productName || !quantity || !price) {
      return res.status(400).json({
        error: 'Missing required fields',
        required: ['customerName', 'productId', 'productName', 'quantity', 'price'],
      });
    }

    const orderId = uuidv4();
    const outboxId = uuidv4();

    //SINGLE TRANSACTION (no dual writes!)
    await client.query('BEGIN');

    // 1) Insert order
    const orderResult = await client.query(
      `INSERT INTO orders (id, customer_name, product_id, product_name, quantity, price, status)
       VALUES (\$1, \$2, \$3, \$4, \$5, \$6, \$7)
       RETURNING *`,
      [orderId, customerName, productId, productName, quantity, price, 'CREATED']
    );
    const order = orderResult.rows[0];

    // 2) Insert outbox event (same transaction!)
    const eventPayload = {
      orderId: order.id,
      customerName: order.customer_name,
      productId: order.product_id,
      productName: order.product_name,
      quantity: order.quantity,
      price: parseFloat(order.price),
      status: order.status,
      createdAt: order.created_at,
    };

    await client.query(
      `INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload)
       VALUES (\$1, \$2, \$3, \$4, \$5)`,
      [outboxId, 'Order', orderId, 'OrderCreated', JSON.stringify(eventPayload)]
    );

    await client.query('COMMIT');
    // ===== END TRANSACTION =====

    console.log(`✅ Order ${orderId} created with outbox event ${outboxId}`);

    res.status(201).json({
      message: 'Order created successfully',
      order,
      outboxEventId: outboxId,
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('❌ Create order failed:', error.message);
    res.status(500).json({ error: 'Failed to create order', details: error.message });
  } finally {
    client.release();
  }
});

router.post('/:id/cancel', async (req, res) => {
  const client = await pool.connect();

  try {
    const { id } = req.params;

    await client.query('BEGIN');

    // 1) Update order
    const orderResult = await client.query(
      `UPDATE orders SET status = 'CANCELLED', updated_at = CURRENT_TIMESTAMP
       WHERE id = \$1 AND status = 'CREATED'
       RETURNING *`,
      [id]
    );

    if (orderResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Order not found or already cancelled' });
    }

    const order = orderResult.rows[0];

    // 2) Write cancellation event to outbox
    const eventPayload = {
      orderId: order.id,
      productId: order.product_id,
      quantity: order.quantity,
      status: 'CANCELLED',
      cancelledAt: new Date().toISOString(),
    };

    await client.query(
      `INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload)
       VALUES (\$1, \$2, \$3, \$4, \$5)`,
      [uuidv4(), 'Order', id, 'OrderCancelled', JSON.stringify(eventPayload)]
    );

    await client.query('COMMIT');

    console.log(`🚫 Order ${id} cancelled`);
    res.json({ message: 'Order cancelled', order });
  } catch (error) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: 'Failed to cancel order', details: error.message });
  } finally {
    client.release();
  }
});


router.get('/', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM orders ORDER BY created_at DESC');
    res.json({ orders: result.rows, count: result.rowCount });
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch orders' });
  }
});

router.get('/outbox/events', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM outbox ORDER BY created_at DESC LIMIT 50');
    res.json({ events: result.rows, count: result.rowCount });
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch outbox events' });
  }
});

module.exports = router;