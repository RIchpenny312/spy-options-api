// db/partitionHelpers.js

const { Client } = require('pg');
require("dotenv").config();
const dayjs = require('dayjs'); // For date manipulation
const { normalizeToBucket } = require('../utils/time'); // 5-min bucket helper
const { DEFAULT_TIMEZONE } = require('../config/marketHours'); // 🕒 Central Time config

const DB_CONFIG = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: { rejectUnauthorized: false }
};

// ✅ Create a partition table for a specific trading date
async function ensureSpyPartitionForDate(date) {
  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    const tableName = `spy_ohlc_${date.replace(/-/g, "_")}`;
    const nextDay = dayjs.tz(date, DEFAULT_TIMEZONE).add(1, 'day').format("YYYY-MM-DD");

    const createPartitionSQL = `
      CREATE TABLE IF NOT EXISTS ${tableName} PARTITION OF spy_ohlc
      FOR VALUES FROM ('${date} 00:00:00') TO ('${nextDay} 00:00:00');
    `;

    await client.query(createPartitionSQL);
    console.log(`✅ Partition ensured for ${date}`);

    // ✅ Ensure bucket_time column exists
    await client.query(`
      ALTER TABLE ${tableName}
      ADD COLUMN IF NOT EXISTS bucket_time TIMESTAMP;
    `);

    // ✅ Unique constraint on (bucket_time, start_time)
    await client.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint 
          WHERE conname = '${tableName}_bucket_start_unique'
        ) THEN
          ALTER TABLE ${tableName}
          ADD CONSTRAINT ${tableName}_bucket_start_unique UNIQUE (bucket_time, start_time);
        END IF;
      END
      $$;
    `);

    // ✅ Index on bucket_time for query performance
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_${tableName}_bucket_time ON ${tableName}(bucket_time);
    `);

    console.log(`✅ Index, constraint, and bucket_time column confirmed for ${tableName}`);

  } catch (err) {
    console.error(`❌ Error ensuring partition for ${date}:`, err.message);
  } finally {
    await client.end();
  }
}

// ✅ Example usage inside data processing
// const bucket_time = normalizeToBucket(someTimestamp);

module.exports = {
  ensureSpyPartitionForDate,
};
