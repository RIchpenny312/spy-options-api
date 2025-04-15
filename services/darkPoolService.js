// -----------------------------------------------
// Imports & Timezone Setup
// -----------------------------------------------
const axios = require("axios");
const { Client } = require("pg");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");
dayjs.extend(utc);
dayjs.extend(timezone);

const TIMEZONE = process.env.TIMEZONE || "America/Chicago";
dayjs.tz.setDefault(TIMEZONE);

// -----------------------------------------------
// Config: PostgreSQL Connection
// -----------------------------------------------
const DB_CONFIG = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: { rejectUnauthorized: false }
};

const BUCKET_INTERVAL_MINUTES = 5;

// -----------------------------------------------
// Utility: Normalize Time to 5-Minute Bucket
// -----------------------------------------------
function normalizeToBucket(timestampUtc, bucketSize = BUCKET_INTERVAL_MINUTES, timezone = TIMEZONE) {
  const local = dayjs.utc(timestampUtc).tz(timezone);
  const floored = Math.floor(local.minute() / bucketSize) * bucketSize;
  return local.minute(floored).second(0).millisecond(0).toISOString();
}

// -----------------------------------------------
// Utility: Fetch with Retry Logic
// -----------------------------------------------
async function fetchWithRetry(url, retries = 3, delay = 5000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios.get(url, {
        headers: { Authorization: `Bearer ${process.env.API_KEY}` },
        timeout: 10000,
      });
      return response;
    } catch (error) {
      const isRetryable = error.response?.status === 429 || error.code === 'ECONNABORTED';
      if (isRetryable && attempt < retries) {
        console.warn(`⚠️ Retry ${attempt}/${retries} for ${url} due to: ${error.message}`);
        await new Promise(res => setTimeout(res, delay));
      } else {
        throw error;
      }
    }
  }
}

// -----------------------------------------------
// Main Function: Fetch and Store Today's Dark Pool Data
// -----------------------------------------------
async function fetchAndStoreDarkPoolData() {
  console.log("🔍 Fetching Dark Pool Trades...");

  const response = await fetchWithRetry("https://api.unusualwhales.com/api/darkpool/SPY");
  const rawTrades = response.data?.data || [];

  const today = dayjs().tz(TIMEZONE).format("YYYY-MM-DD");
  const marketOpen = dayjs.tz(`${today} 08:00:00`, TIMEZONE); // Adjusted to 8:00 AM
  const marketClose = dayjs.tz(`${today} 16:00:00`, TIMEZONE); // Adjusted to 4:00 PM

  // Debugging: Log raw trades count
  console.log(`🔍 Raw trades fetched: ${rawTrades.length}`);

  // Filter trades to ensure they fall within market hours
  const todayTrades = rawTrades.filter(trade => {
    const executed = dayjs.utc(trade.executed_at).tz(TIMEZONE);
    const isValid = executed.isSame(today, 'day') &&
                    executed.isAfter(marketOpen) &&
                    executed.isBefore(marketClose);
    if (!isValid) {
      console.warn(`⚠️ Excluded trade: ${JSON.stringify(trade)}`);
    }
    return isValid;
  });

  // Debugging: Log filtered trades count
  console.log(`✅ Valid trades for today: ${todayTrades.length}`);

  if (todayTrades.length === 0) {
    console.log("⚠️ No valid dark pool trades for today.");
    return;
  }

  const parsedTrades = todayTrades.map(trade => {
    const executedLocal = dayjs.utc(trade.executed_at).tz(TIMEZONE);
    const bucketTime = normalizeToBucket(trade.executed_at);

    return {
      tracking_id: trade.tracking_id,
      ticker: trade.ticker,
      price: parseFloat(trade.price),
      size: parseInt(trade.size),
      premium: parseFloat(trade.premium),
      volume: parseInt(trade.volume),
      executed_at: executedLocal.toISOString(),
      bucket_time: bucketTime,
      nbbo_bid: parseFloat(trade.nbbo_bid),
      nbbo_ask: parseFloat(trade.nbbo_ask),
      bid_quantity: parseInt(trade.nbbo_bid_quantity),
      ask_quantity: parseInt(trade.nbbo_ask_quantity),
      market_center: trade.market_center,
      trade_day: executedLocal.format("YYYY-MM-DD") // Added trade_day for efficient filtering
    };
  });

  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    // Begin transaction for batch inserts
    await client.query("BEGIN");

    for (const trade of parsedTrades) {
      await client.query(`
        INSERT INTO spy_dark_pool (
          tracking_id, ticker, price, size, premium, volume,
          executed_at, bucket_time, nbbo_bid, nbbo_ask,
          bid_quantity, ask_quantity, market_center, trade_day
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (tracking_id) DO NOTHING
      `, [
        trade.tracking_id,
        trade.ticker,
        trade.price,
        trade.size,
        trade.premium,
        trade.volume,
        trade.executed_at,
        trade.bucket_time,
        trade.nbbo_bid,
        trade.nbbo_ask,
        trade.bid_quantity,
        trade.ask_quantity,
        trade.market_center,
        trade.trade_day
      ]);
    }

    // Commit transaction
    await client.query("COMMIT");
    console.log(`✅ Inserted ${parsedTrades.length} dark pool trades.`);
  } catch (err) {
    console.error("❌ Error during batch insert:", err.message);
    await client.query("ROLLBACK");
  } finally {
    await client.end();
  }
}

// -----------------------------------------------
// Export
// -----------------------------------------------
module.exports = {
  fetchAndStoreDarkPoolData
};