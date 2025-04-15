const { Client } = require("pg");

const DB_CONFIG = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: { rejectUnauthorized: false }
};

async function storeDarkPoolLevelsInDB({ trading_day, top_levels }) {
  const client = new Client(DB_CONFIG);
  await client.connect();

  console.log(`🔍 Storing ${top_levels.length} dark pool levels for ${trading_day}`);

  for (const level of top_levels) {
    try {
      console.log(`📊 Inserting level: ${JSON.stringify(level)}`);
      await client.query(`
        INSERT INTO spy_dark_pool_levels (
          trading_day, price, total_premium, total_volume, total_size, trade_count
        ) VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (trading_day, price) DO UPDATE
        SET total_premium = EXCLUDED.total_premium,
            total_volume = EXCLUDED.total_volume,
            total_size = EXCLUDED.total_size,
            trade_count = EXCLUDED.trade_count
      `, [
        trading_day,
        level.price,
        level.total_premium,
        level.total_volume,
        level.total_size,
        level.trade_count
      ]);
    } catch (err) {
      console.warn(`⚠️ Failed to insert level ${level.price}:`, err.message);
    }
  }

  await client.end();
  console.log(`✅ Stored ${top_levels.length} dark pool levels for ${trading_day}`);
}

module.exports = {
  storeDarkPoolLevelsInDB
};
