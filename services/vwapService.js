console.log("🧪 LIVE TEST: Using local DB for VWAP, no API call should occur");

const db = require('./db');

// Fetch today's SPY OHLC 5-min data from local DB
async function fetchOhlcFromDb() {
  const query = `
    SELECT bucket_time, close, volume
    FROM spy_ohlc
    WHERE bucket_time::date = CURRENT_DATE
    ORDER BY bucket_time ASC;
  `;
  try {
    const { rows } = await db.query(query);
    const filtered = rows.filter(row => Number(row.volume) > 0);
    console.log(`[VWAP Service][${new Date().toISOString()}] Fetched OHLC rows: ${filtered.length}`);
    return filtered;
  } catch (err) {
    console.error(`[VWAP Service][${new Date().toISOString()}] Error fetching OHLC:`, err.message);
    return [];
  }
}

// Calculate cumulative VWAP series
async function calculateVwap(ohlcData) {
  console.log(`[VWAP Service][${new Date().toISOString()}] Calculating VWAP series...`);
  let cumulativePV = 0;
  let cumulativeVolume = 0;
  const vwapSeries = [];
  for (const row of ohlcData) {
    const price = Number(row.close);
    const volume = Number(row.volume);
    cumulativePV += price * volume;
    cumulativeVolume += volume;
    const vwap = cumulativeVolume > 0 ? cumulativePV / cumulativeVolume : 0;
    vwapSeries.push({
      bucket_time: row.bucket_time,
      vwap: Number(vwap.toFixed(4)),
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
    console.log(`[VWAP Service][${new Date().toISOString()}] Fetching today's OHLC data...`);
    const ohlcData = await fetchOhlcFromDb();
    if (!ohlcData.length) {
      console.warn(`[VWAP Service][${new Date().toISOString()}] No OHLC data found for today.`);
      return;
    }
    const vwapSeries = await calculateVwap(ohlcData);
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
