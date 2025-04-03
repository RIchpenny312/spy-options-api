// partitionHelpers.js

const { Client } = require('pg');
require("dotenv").config();
const DB_CONFIG = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: { rejectUnauthorized: false }
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

    // ✅ Ensure `bucket_time` column exists in partition
    await client.query(`
      ALTER TABLE ${tableName}
      ADD COLUMN IF NOT EXISTS bucket_time TIMESTAMP;
    `);

    // ✅ Add unique constraint on bucket_time (partition-safe)
    await client.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint 
          WHERE conname = '${tableName}_bucket_time_unique'
        ) THEN
          ALTER TABLE ${tableName}
          ADD CONSTRAINT ${tableName}_bucket_time_unique UNIQUE (bucket_time);
        END IF;
      END
      $$;
    `);

    // ✅ Add index for performance
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_${tableName}_bucket_time ON ${tableName}(bucket_time);
    `);

    console.log(`✅ bucket_time column, constraint, and index added to ${tableName}`);

  } catch (err) {
    console.error(`❌ Error ensuring partition for ${date}:`, err.message);
  } finally {
    await client.end();
  }
}

module.exports = {
  ensureSpyPartitionForDate,
};