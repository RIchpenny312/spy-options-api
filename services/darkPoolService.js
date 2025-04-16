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
// Utility: Log Insert Summary
// -----------------------------------------------
function logInsertSummary(count, fallbackUsed) {
  const tag = fallbackUsed ? '📥 [Fallback]' : '✅';
  console.log(`${tag} Inserted ${count} dark pool trades into DB.`);
}

// -----------------------------------------------
// Main Function: Fetch and Store Today's Dark Pool Data
// -----------------------------------------------
async function fetchAndStoreDarkPoolData() {
  console.log("🔍 Fetching Dark Pool Trades...");

  const response = await fetchWithRetry("https://api.unusualwhales.com/api/darkpool/SPY");
  const rawTrades = response.data?.data || [];

  // Double-fallback guard: Skip processing if no trades are received
  if (rawTrades.length === 0) {
    console.warn("⚠️ No trades received from API — skipping insertion.");
    return;
  }

  // Sort rawTrades by executed_at to ensure the latest trade is on top
  rawTrades.sort((a, b) => new Date(b.executed_at) - new Date(a.executed_at));

  const today = dayjs().tz(TIMEZONE).format("YYYY-MM-DD");
  const marketOpen = dayjs.tz(`${today} 07:30:00`, TIMEZONE); // Adjusted to 7:30 AM
  const marketClose = dayjs.tz(`${today} 16:30:00`, TIMEZONE); // Adjusted to 4:30 PM

  // Log the most recent trade timestamp
  if (rawTrades.length > 0) {
    const mostRecent = dayjs.utc(rawTrades[0].executed_at).tz(TIMEZONE).format();
    console.log(`🕒 Most recent trade timestamp (local): ${mostRecent}`);
  }

  // Adjust time window logic to include boundary trades
  const todayTrades = rawTrades.filter(trade => {
    const executed = dayjs.utc(trade.executed_at).tz(TIMEZONE);
    const isValid = executed.isSame(today, 'day') &&
                    executed.isSameOrAfter(marketOpen) &&
                    executed.isSameOrBefore(marketClose);
    if (!isValid) {
      console.warn(`⚠️ Excluded trade: ${JSON.stringify(trade)}`);
    }
    return isValid;
  });

  // Log fallback explanation
  if (todayTrades.length === 0) {
    console.info("📥 Using fallback trades from most recent date due to absence of today’s entries.");
  }

  // Optional fallback logic for missing trades
  if (todayTrades.length === 0) {
    const mostRecentTrade = rawTrades[0];
    const mostRecentDate = dayjs.utc(mostRecentTrade.executed_at).tz(TIMEZONE).format("YYYY-MM-DD");
    console.warn(`⚠️ No trades for ${today}, falling back to most recent: ${mostRecentDate}`);
    todayTrades.push(...rawTrades);
  }

  // Debugging: Log filtered trades count
  console.log(`✅ Valid trades for today: ${todayTrades.length}`);

  // Analyze `off_vol` values
  const offVolValues = rawTrades.map(trade => parseInt(trade.off_vol) || 0).filter(val => val > 0);

  if (offVolValues.length > 0) {
    const sortedOffVol = [...offVolValues].sort((a, b) => b - a); // Descending order
    const top10 = sortedOffVol.slice(0, 10);
    const mean = (offVolValues.reduce((sum, val) => sum + val, 0) / offVolValues.length).toFixed(2);
    const median = sortedOffVol[Math.floor(offVolValues.length / 2)];
    const min = Math.min(...offVolValues);
    const max = Math.max(...offVolValues);

    console.log("📊 Off-Exchange Volume Analysis:");
    console.log(`   - Total Trades Analyzed: ${offVolValues.length}`);
    console.log(`   - Mean: ${mean}`);
    console.log(`   - Median: ${median}`);
    console.log(`   - Min: ${min}`);
    console.log(`   - Max: ${max}`);
    console.log(`   - Top 10 Off-Vol Values: ${top10.join(", ")}`);
  } else {
    console.warn("⚠️ No valid `off_vol` values found for analysis.");
  }

  // Handle missing fields gracefully
  const parsedTrades = todayTrades.map(trade => {
    const executedLocal = dayjs.utc(trade.executed_at).tz(TIMEZONE);
    const bucketTime = normalizeToBucket(trade.executed_at);

    return {
      tracking_id: trade.tracking_id || 'unknown',
      ticker: trade.ticker || 'SPY',
      price: parseFloat(trade.price) || 0,
      size: parseInt(trade.size) || 0,
      premium: parseFloat(trade.premium) || 0,
      volume: parseInt(trade.volume) || 0,
      executed_at: executedLocal.toISOString(),
      bucket_time: bucketTime,
      nbbo_bid: parseFloat(trade.nbbo_bid) || 0,
      nbbo_ask: parseFloat(trade.nbbo_ask) || 0,
      bid_quantity: parseInt(trade.nbbo_bid_quantity) || 0,
      ask_quantity: parseInt(trade.nbbo_ask_quantity) || 0,
      market_center: trade.market_center || 'unknown',
      trade_day: executedLocal.format("YYYY-MM-DD"), // Added trade_day for efficient filtering
      is_fallback: todayTrades.length === 0 // Tag fallback data
    };
  });

  // Log sample parsed trades for debugging
  console.table(parsedTrades.slice(0, 3));

  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    // Begin transaction for batch inserts
    await client.query("BEGIN");

    // Future-proofing: Add a note for batch insert optimization
    // TODO: Optimize batch inserts using COPY FROM or batched INSERT INTO for large datasets

    for (const trade of parsedTrades) {
      await client.query(`
        INSERT INTO spy_dark_pool (
          tracking_id, ticker, price, size, premium, volume,
          executed_at, bucket_time, nbbo_bid, nbbo_ask,
          bid_quantity, ask_quantity, market_center, trade_day, is_fallback
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
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
        trade.trade_day,
        trade.is_fallback
      ]);
    }

    // Commit transaction
    await client.query("COMMIT");
    logInsertSummary(parsedTrades.length, todayTrades.length === 0);
  } catch (err) {
    console.error("❌ Error during batch insert:", err.message);
    await client.query("ROLLBACK");
  } finally {
    await client.end();
  }
}

// -----------------------------------------------
// Get high confidence dark pool levels near a spot price
// -----------------------------------------------
async function getHighConfidenceLevelsNearSpot({ windowDays = 3, spotPrice, proximity = 1.0 }) {
  const client = new Client(DB_CONFIG);
  await client.connect();

  // 1. Get last N days of dark pool levels
  const query = `
    SELECT price, total_size, total_volume, total_premium, trade_day
    FROM spy_dark_pool_levels
    WHERE trade_day >= (CURRENT_DATE - INTERVAL '${windowDays - 1} days')
  `;
  const result = await client.query(query);
  await client.end();

  // 2. Aggregate by price
  const byPrice = {};
  for (const row of result.rows) {
    const price = parseFloat(row.price);
    if (!byPrice[price]) byPrice[price] = { price, total_size: 0, total_volume: 0, total_premium: 0, days: new Set() };
    byPrice[price].total_size += parseFloat(row.total_size);
    byPrice[price].total_volume += parseFloat(row.total_volume);
    byPrice[price].total_premium += parseFloat(row.total_premium);
    byPrice[price].days.add(row.trade_day);
  }

  // 3. Build summary and filter for high confidence near spot
  const summary = Object.values(byPrice).map(level => {
    const daysAppeared = level.days.size;
    return {
      price: level.price,
      avg_size: level.total_size / daysAppeared,
      total_volume: level.total_volume,
      total_premium: level.total_premium,
      days_appeared: daysAppeared,
      confidence: daysAppeared >= 3 ? 'High' : daysAppeared === 2 ? 'Medium' : 'Low'
    };
  });

  // 4. Filter for high confidence and near spot price
  return summary.filter(l => l.confidence === 'High' && Math.abs(l.price - spotPrice) <= proximity);
}

// -----------------------------------------------
// Export
// -----------------------------------------------
module.exports = {
  fetchAndStoreDarkPoolData,
  getHighConfidenceLevelsNearSpot
};