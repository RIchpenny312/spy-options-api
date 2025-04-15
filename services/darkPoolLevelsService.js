const { Client } = require("pg");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");
dayjs.extend(utc);
dayjs.extend(timezone);

const TIMEZONE = process.env.TIMEZONE || "America/Chicago";

const DB_CONFIG = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: { rejectUnauthorized: false }
};

// 🔍 Aggregate top price levels by total premium
async function getTopDarkPoolLevels({ date = null, limit = 10 } = {}) {
  const client = new Client(DB_CONFIG);
  await client.connect();

  // 🕒 Normalize and log trading day
  const tradingDay = date || dayjs().tz(TIMEZONE).format("YYYY-MM-DD");
  console.log("🔍 Fetching top dark pool levels for:", tradingDay);

  const query = `
    SELECT
      price,
      trade_count,
      total_size,
      total_volume,
      total_premium
    FROM spy_dark_pool_levels
    WHERE trading_day = $1::date
    ORDER BY total_premium DESC
    LIMIT $2
  `;

  const result = await client.query(query, [tradingDay, limit]);

  if (result.rows.length === 0) {
    console.warn(`⚠️ No dark pool levels found for ${tradingDay}. Falling back to previous trading day.`);
    const previousDay = dayjs(tradingDay).subtract(1, 'day').format("YYYY-MM-DD");
    const fallbackResult = await client.query(query, [previousDay, limit]);
    return {
      trading_day: previousDay,
      top_levels: fallbackResult.rows.map(row => ({
        price: parseFloat(row.price),
        total_premium: parseFloat(row.total_premium),
        total_volume: parseInt(row.total_volume) || 0,
        total_size: parseInt(row.total_size) || 0,
        trade_count: parseInt(row.trade_count)
      }))
    };
  }

  await client.end();

  return {
    trading_day: tradingDay,
    top_levels: result.rows.map(row => ({
      price: parseFloat(row.price),
      total_premium: parseFloat(row.total_premium),
      total_volume: parseInt(row.total_volume) || 0,
      total_size: parseInt(row.total_size) || 0,
      trade_count: parseInt(row.trade_count)
    }))
  };
}

module.exports = {
  getTopDarkPoolLevels
};
