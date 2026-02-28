const { Kafka } = require('kafkajs');
const pool = require('./db');

const kafka = new Kafka({
  clientId: 'inventory-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: {
    initialRetryTime: 3000,
    retries: 10,
  },
});

const consumer = kafka.consumer({
  groupId: 'inventory-service-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

// ==========================================
// PROCESS DEBEZIUM CDC MESSAGE
// ==========================================
async function processMessage(message) {
  try {
    const rawValue = message.value.toString();
    const debeziumEvent = JSON.parse(rawValue);

    console.log('\n📨 ════════════════════════════════════════');
    console.log('📨 Received CDC event from Kafka');
    console.log('📨 ════════════════════════════════════════');

    // Debezium envelope: payload.op, payload.before, payload.after
    const operation = debeziumEvent.payload?.op || debeziumEvent.op;
    const after = debeziumEvent.payload?.after || debeziumEvent.after;

    if (!after) {
      console.log('⚠️  No "after" data (delete event?). Skipping.');
      return;
    }

    const eventId = after.id;
    const aggregateType = after.aggregatetype;
    const aggregateId = after.aggregateid;
    const eventType = after.type;

    // Parse payload (JSONB comes as string from Debezium)
    let eventPayload;
    if (typeof after.payload === 'string') {
      eventPayload = JSON.parse(after.payload);
    } else {
      eventPayload = after.payload;
    }

    console.log(`   Operation:  ${operation}`);
    console.log(`   Event ID:   ${eventId}`);
    console.log(`   Type:       ${eventType}`);
    console.log(`   Aggregate:  ${aggregateType} / ${aggregateId}`);
    console.log(`   Payload:    ${JSON.stringify(eventPayload)}`);

    // ===== IDEMPOTENCY CHECK =====
    const existing = await pool.query(
      'SELECT event_id FROM processed_events WHERE event_id = \$1',
      [eventId]
    );

    if (existing.rows.length > 0) {
      console.log(`⚠️  Event ${eventId} already processed. Skipping.`);
      return;
    }

    // ===== ROUTE TO HANDLER =====
    switch (eventType) {
      case 'OrderCreated':
        await handleOrderCreated(eventId, eventPayload);
        break;
      case 'OrderCancelled':
        await handleOrderCancelled(eventId, eventPayload);
        break;
      default:
        console.log(`⚠️  Unknown event type: ${eventType}`);
    }
  } catch (error) {
    console.error('❌ Error processing message:', error.message);
    console.error('   Raw:', message.value.toString().substring(0, 300));
  }
}

async function handleOrderCreated(eventId, payload) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const { productId, quantity, orderId } = payload;

    // Lock row for update
    const productResult = await client.query(
      'SELECT * FROM products WHERE id = \$1 FOR UPDATE',
      [productId]
    );

    if (productResult.rows.length === 0) {
      console.log(`⚠️  Product ${productId} not found!`);
      await client.query(
        'INSERT INTO processed_events (event_id, event_type, aggregate_id) VALUES (\$1, \$2, \$3)',
        [eventId, 'OrderCreated', orderId]
      );
      await client.query('COMMIT');
      return;
    }

    const product = productResult.rows[0];

    if (product.stock < quantity) {
      console.log(`⚠️  Insufficient stock! Available: ${product.stock}, Requested: ${quantity}`);
      await client.query(
        'INSERT INTO processed_events (event_id, event_type, aggregate_id) VALUES (\$1, \$2, \$3)',
        [eventId, 'OrderCreated', orderId]
      );
      await client.query('COMMIT');
      return;
    }

    // Deduct stock, increase reserved
    await client.query(
      `UPDATE products
       SET stock = stock - \$1, reserved = reserved + \$1, updated_at = CURRENT_TIMESTAMP
       WHERE id = \$2`,
      [quantity, productId]
    );

    // Mark processed
    await client.query(
      'INSERT INTO processed_events (event_id, event_type, aggregate_id) VALUES (\$1, \$2, \$3)',
      [eventId, 'OrderCreated', orderId]
    );

    await client.query('COMMIT');

    console.log(`\n INVENTORY UPDATED`);
    console.log(`   Product:  ${product.name} (${productId})`);
    console.log(`   Stock:    ${product.stock} → ${product.stock - quantity}`);
    console.log(`   Reserved: ${product.reserved} → ${product.reserved + quantity}`);
    console.log(`   Order:    ${orderId}\n`);
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('handleOrderCreated error:', error.message);
    throw error;
  } finally {
    client.release();
  }
}

async function handleOrderCancelled(eventId, payload) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const { productId, quantity, orderId } = payload;

    await client.query(
      `UPDATE products
       SET stock = stock + \$1, reserved = GREATEST(reserved - \$1, 0), updated_at = CURRENT_TIMESTAMP
       WHERE id = \$2`,
      [quantity, productId]
    );

    await client.query(
      'INSERT INTO processed_events (event_id, event_type, aggregate_id) VALUES (\$1, \$2, \$3)',
      [eventId, 'OrderCancelled', orderId]
    );

    await client.query('COMMIT');

    console.log(`\n🔄 INVENTORY RESTORED (Order Cancelled)`);
    console.log(`   Product: ${productId}, Qty restored: ${quantity}`);
    console.log(`   Order:   ${orderId}\n`);
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('❌ handleOrderCancelled error:', error.message);
    throw error;
  } finally {
    client.release();
  }
}

//CONSUMER
async function startConsumer() {
  try {
    console.log('🔄 Connecting to Kafka...');
    await consumer.connect();
    console.log('✅ Connected to Kafka');

    const topic = 'orders-db-connector.public.outbox';

    await consumer.subscribe({ topic, fromBeginning: true });
    console.log(`📡 Subscribed to topic: ${topic}`);
    console.log('👂 Waiting for CDC events...\n');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(`📥 Message on ${topic} [partition:${partition}, offset:${message.offset}]`);
        await processMessage(message);
      },
    });
  } catch (error) {
    console.error('❌ Consumer start failed:', error.message);
    console.log('🔄 Retrying in 10 seconds...');
    setTimeout(startConsumer, 10000);
  }
}

// Graceful shutdown
const shutdown = async () => {
  console.log('\n🛑 Shutting down consumer...');
  await consumer.disconnect();
  await pool.end();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

module.exports = { startConsumer };