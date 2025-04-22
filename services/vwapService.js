// VWAP Service: Calculate and store SPY 5-min intraday VWAP (CT-aligned)
const db = require('./db');
const { normalizeToBucket } = require('../utils/time');
const dayjs = require('dayjs');

// --- Retry wrapper for axios.get ---
async function axiosGetWithRetry(url, options, retries = 3, delay = 2000) {
  const axios = require('axios');
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await axios.get(url, options);
    } catch (err) {
      if (attempt === retries) throw err;
      console.warn(`[VWAP Service][${new Date().toISOString()}] API request failed (attempt ${attempt}): ${err.message}`);
      await new Promise(res => setTimeout(res, delay));
    }
  }
}

// Fetch today's SPY OHLC 5-min data (from index.js logic)
async function fetchSpyOhlcDataToday() {
  const TIMEZONE = process.env.TIMEZONE || 'America/Chicago';
  const today = dayjs().tz(TIMEZONE).format('YYYY-MM-DD');
  const url = 'https://api.unusualwhales.com/api/stock/SPY/ohlc/5m';
  const API_KEY = process.env.API_KEY;
  const response = await axiosGetWithRetry(url, {
    headers: { Authorization: `Bearer ${API_KEY}` },
    timeout: 10000,
  });
  if (!response.data?.data || !Array.isArray(response.data.data)) return [];
  const filtered = response.data.data
    .filter(item => dayjs.utc(item.start_time).tz(TIMEZONE).format('YYYY-MM-DD') === today)
    .map(item => ({
      open: parseFloat(item.open) || 0,
      high: parseFloat(item.high) || 0,
      low: parseFloat(item.low) || 0,
      close: parseFloat(item.close) || 0,
      volume: parseInt(item.volume) || 0,
      bucket_time: normalizeToBucket(item.start_time),
    }));
  console.log(`[VWAP Service][${new Date().toISOString()}] Fetched OHLC rows: ${filtered.length}`);
  return filtered;
}

// Calculate cumulative VWAP series
async function calculateVwap(ohlcData) {
  // Defensive: filter out zero-volume candles
  const filtered = ohlcData.filter(row => row.volume > 0);
  let cumulativePV = 0;
  let cumulativeVolume = 0;
  const vwapSeries = [];
  for (const row of filtered) {
    const price = parseFloat(row.close);
    const volume = parseFloat(row.volume);
    cumulativePV += price * volume;
    cumulativeVolume += volume;
    const vwap = cumulativeVolume > 0 ? cumulativePV / cumulativeVolume : 0;
    vwapSeries.push({
      bucket_time: row.bucket_time,
      vwap: parseFloat(vwap.toFixed(4)),
    });
  }
  return vwapSeries;
}

// Store VWAP series in spy_intraday_vwap table
async function storeVwap(vwapSeries) {
  const insertQuery = `
    INSERT INTO spy_intraday_vwap (bucket_time, vwap)
    VALUES ($1, $2)
    ON CONFLICT (bucket_time)
    DO UPDATE SET vwap = EXCLUDED.vwap;
  `;
  let count = 0;
  for (const { bucket_time, vwap } of vwapSeries) {
    await db.query(insertQuery, [bucket_time, vwap]);
    count++;
  }
  console.log(`[VWAP Service][${new Date().toISOString()}] Stored VWAP points: ${count}`);
}

// Standalone main for cron jobs
async function main() {
  try {
    const now = new Date().toISOString();
    console.log(`[VWAP Service][${now}] Fetching today's OHLC data...`);
    const ohlcData = await fetchSpyOhlcDataToday();
    if (!ohlcData.length) {
      console.warn(`[VWAP Service][${new Date().toISOString()}] No OHLC data found for today.`);
      return;
    }
    console.log(`[VWAP Service][${new Date().toISOString()}] Calculating VWAP series...`);
    const vwapSeries = await calculateVwap(ohlcData);
    console.log(`[VWAP Service][${new Date().toISOString()}] Storing VWAP series into database...`);
    await storeVwap(vwapSeries);
    console.log(`[VWAP Service][${new Date().toISOString()}] VWAP calculation and storage complete.`);
  } catch (err) {
    console.error(`[VWAP Service][${new Date().toISOString()}] Error:`, err.message);
  }
}

if (require.main === module) {
  main();
}

module.exports = {
  calculateVwap,
  storeVwap,
};
