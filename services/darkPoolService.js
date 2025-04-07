const axios = require("axios");
const { Client } = require("pg");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");
const { storeDarkPoolLevelsInDB } = require("./storeDarkPoolLevels"); // adjust path if needed

dayjs.extend(utc);
dayjs.extend(timezone);

const TIMEZONE = process.env.TIMEZONE || "America/Chicago";
dayjs.tz.setDefault(TIMEZONE);

const DB_CONFIG = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: { rejectUnauthorized: false }
};

const BUCKET_INTERVAL_MINUTES = 5;

function normalizeToBucket(timestampUtc) {
  const local = dayjs.utc(timestampUtc).tz(TIMEZONE);
  const floored = Math.floor(local.minute() / BUCKET_INTERVAL_MINUTES) * BUCKET_INTERVAL_MINUTES;
  return local.minute(floored).second(0).millisecond(0).toISOString();
}

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

async function fetchAndStoreDarkPoolData() {
  console.log("🔍 Fetching Dark Pool Trades...");

  const response = await fetchWithRetry("https://api.unusualwhales.com/api/darkpool/SPY");
  const rawTrades = response.data?.data || [];

  const today = dayjs().tz(TIMEZONE).format("YYYY-MM-DD");
  const todayTrades = rawTrades.filter(trade => {
    const tradeDate = dayjs.utc(trade.executed_at).tz(TIMEZONE).format("YYYY-MM-DD");
    return tradeDate === today;
  });

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
      market_center: trade.market_center
    };
  });

  const client = new Client(DB_CONFIG);
  await client.connect();

  for (const trade of parsedTrades) {
    try {
      await client.query(`
        INSERT INTO spy_dark_pool (
          tracking_id, ticker, price, size, premium, volume,
          executed_at, bucket_time, nbbo_bid, nbbo_ask,
          bid_quantity, ask_quantity, market_center
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
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
        trade.market_center
      ]);
    } catch (err) {
      console.warn(`⚠️ Insert failed for ID ${trade.tracking_id}:`, err.message);
    }
  }

  await client.end();
  console.log(`✅ Inserted ${parsedTrades.length} raw dark pool trades.`);

  // --- Aggregate by price and insert summaries ---
  const summaryMap = new Map();

  for (const trade of parsedTrades) {
    const key = trade.price.toFixed(2); // round price for grouping

    if (!summaryMap.has(key)) {
      summaryMap.set(key, {
        trading_day: today,
        price: trade.price,
        total_premium: 0,
        total_volume: 0,
        total_size: 0,
        trade_count: 0
      });
    }

    const level = summaryMap.get(key);
    level.total_premium += trade.premium || 0;
    level.total_volume += trade.volume || 0;
    level.total_size += trade.size || 0;
    level.trade_count += 1;
  }

  const top_levels = Array.from(summaryMap.values());

  if (top_levels.length > 0) {
    await storeDarkPoolLevelsInDB({ trading_day: today, top_levels });
  } else {
    console.log("⚠️ No dark pool summary levels to insert.");
  }
}

module.exports = {
  fetchAndStoreDarkPoolData
};
