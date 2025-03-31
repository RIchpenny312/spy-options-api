const { Client } = require("pg");
require("dotenv").config();

const DB_CONFIG = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: { rejectUnauthorized: false },
};

// ✅ Helper to create a partition if not exists
async function ensureSpyPartitionForDate(date) {
  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    const tableName = `spy_ohlc_${date.replace(/-/g, "_")}`;

    const createPartitionSQL = `
      CREATE TABLE IF NOT EXISTS ${tableName} PARTITION OF spy_ohlc
      FOR VALUES FROM ('${date} 00:00:00') TO ('${date} 23:59:59');
    `;

    await client.query(createPartitionSQL);
    console.log(`✅ Ensured partition exists for ${date}`);

  } catch (err) {
    console.error(`❌ Error ensuring partition for ${date}:`, err.message);
  } finally {
    await client.end();
  }
}

module.exports = {
  ensureSpyPartitionForDate,
};