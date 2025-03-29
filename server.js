require('dotenv').config();
const express = require('express');
const { Client } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;

// ✅ PostgreSQL Connection Config
const DB_CONFIG = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    ssl: { rejectUnauthorized: false }
};

// ✅ Function to Fetch Data from PostgreSQL
async function fetchData(query, params = []) {
    const client = new Client(DB_CONFIG);
    await client.connect();
    try {
        const result = await client.query(query, params);
        return result.rows;
    } catch (error) {
        console.error("❌ Database Error:", error.message);
        return [];
    } finally {
        await client.end();
    }
}

// ------------------------
// ✅ API Endpoints
// ------------------------

// 🔹 Fetch SPY OHLC Data (5m)
app.get('/api/spy/ohlc', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM spy_ohlc 
        ORDER BY start_time DESC 
        LIMIT 10
    `);
    res.json(data);
});

// 🔹 Fetch SPY Spot GEX (Latest)
app.get('/api/spy/spot-gex', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM spy_spot_gex 
        ORDER BY time DESC 
        LIMIT 1
    `);
    if (data.length === 0) {
        return res.status(404).json({ error: "No Spot GEX data available" });
    }
    res.json(data[0]);
});

// ✅ Fetch Last 10 Market Tide Entries
app.get("/api/spy/market-tide", async (req, res) => {
    try {
        const data = await fetchData(`
            SELECT * FROM market_tide_data 
            ORDER BY timestamp DESC 
            LIMIT 10
        `);
        res.json(data);
    } catch (error) {
        console.error("❌ Error fetching Market Tide data:", error.message);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// 🔹 Fetch Market Tide Rolling Averages (Last 12 & Last 48 Intervals)
app.get('/api/spy/market-tide/rolling-avg', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM market_tide_rolling_avg 
        ORDER BY timestamp DESC 
        LIMIT 1
    `);
    res.json(data);
});

// 🔹 Latest Market Tide (1 record)
app.get('/api/spy/market-tide/latest', async (req, res) => {
  const [latest] = await fetchData(`
    SELECT * FROM market_tide_data 
    WHERE date = CURRENT_DATE
    ORDER BY timestamp DESC 
    LIMIT 1
  `);
  if (!latest) return res.status(404).json({ error: "No Market Tide data available" });
  res.json(latest);
});

// 🔹 Today's Deltas
app.get('/api/spy/market-tide/deltas/today', async (req, res) => {
  const deltas = await fetchData(`
    SELECT * FROM market_tide_deltas 
    WHERE timestamp::date = CURRENT_DATE
    ORDER BY timestamp ASC
  `);
  res.json(deltas);
});

// 🔹 Combined Market Tide Snapshot
app.get('/api/spy/market-tide/snapshot', async (req, res) => {
  try {
    const [latestTide] = await fetchData(`
      SELECT * FROM market_tide_data 
      WHERE date = CURRENT_DATE
      ORDER BY timestamp DESC 
      LIMIT 1
    `);
    const [rollingAvg] = await fetchData(`
      SELECT * FROM market_tide_rolling_avg 
      ORDER BY timestamp DESC 
      LIMIT 1
    `);
    const [latestDelta] = await fetchData(`
      SELECT * FROM market_tide_deltas 
      WHERE timestamp::date = CURRENT_DATE
      ORDER BY timestamp DESC 
      LIMIT 1
    `);

    if (!latestTide || !rollingAvg || !latestDelta) {
      return res.status(404).json({ error: "Incomplete Market Tide snapshot" });
    }

    res.json({
      latest_tide: latestTide,
      rolling_avg: rollingAvg,
      latest_delta: latestDelta
    });
  } catch (error) {
    console.error("❌ Snapshot error:", error.message);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// 🔹 Fetch Bid/Ask Volume Data (Limited to 5)
app.get('/api/spy/bid-ask-volume', async (req, res) => {
    try {
        const data = await fetchData(`
            SELECT 
                COALESCE(symbol, ticker) AS ticker,  -- Ensures ticker is not NULL
                call_volume, put_volume, 
                call_volume_ask_side, put_volume_ask_side, 
                call_volume_bid_side, put_volume_bid_side, 
                date
            FROM bid_ask_volume_data
            WHERE symbol IS NOT NULL OR ticker IS NOT NULL
            ORDER BY date DESC
            LIMIT 5
        `);
        res.json(data);
    } catch (error) {
        console.error("❌ Error fetching Bid/Ask Volume:", error.message);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// 🔹 Fetch SPY Option Price Levels
app.get('/api/spy/option-price-levels/today', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM spy_option_price_levels
        WHERE time = CURRENT_DATE
        ORDER BY recorded_at DESC
        LIMIT 10
    `);
    res.json(data);
});

// 🔹 Fetch SPY Greeks by Strike
app.get('/api/spy/greeks', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM spy_greek_exposure_strike 
        ORDER BY time DESC 
        LIMIT 10
    `);
    res.json(data);
});

// 🔹 Fetch SPY/SPX Greek Exposure (Last 5 records)
app.get('/api/greek-exposure', async (req, res) => {
    const { symbol } = req.query;
    if (!symbol || !['SPY', 'SPX'].includes(symbol.toUpperCase())) {
        return res.status(400).json({ error: "Invalid symbol. Use 'SPY' or 'SPX'." });
    }
    const data = await fetchData(`
        SELECT * FROM greek_exposure 
        WHERE symbol = $1 
        ORDER BY date DESC 
        LIMIT 5
    `, [symbol.toUpperCase()]);
    res.json(data);
});

// ✅ Fetch the average SPY Spot GEX for the last 15 records
app.get('/api/spy/spot-gex/average', async (req, res) => {
    const client = new Client(DB_CONFIG);
    await client.connect();
    try {
        const result = await client.query(`
            SELECT 
                AVG(price) AS avg_price,
                AVG(charm_oi) AS avg_charm_oi,
                AVG(gamma_oi) AS avg_gamma_oi,
                AVG(vanna_oi) AS avg_vanna_oi
            FROM (
                SELECT price, charm_oi, gamma_oi, vanna_oi
                FROM spy_spot_gex
                ORDER BY time DESC
                LIMIT 15
            ) subquery;
        `);
        res.json(result.rows[0]);
    } catch (error) {
        console.error("❌ Error fetching average SPY Spot GEX:", error.message);
        res.status(500).json({ error: "Internal Server Error" });
    } finally {
        await client.end();
    }
});

// ✅ Fetch SPY IV Data from the Database (0 DTE)
app.get("/api/spy/iv/0dte", async (req, res) => {
    try {
        const client = new Client(DB_CONFIG);
        await client.connect();

        const result = await client.query(
            `SELECT * FROM spy_iv_0dte WHERE dte = 0 ORDER BY date DESC, recorded_at DESC LIMIT 5;`
        );

        await client.end();

        res.json({ latest: result.rows[0] || null, last_5: result.rows });
    } catch (error) {
        console.error("❌ Error fetching SPY IV 0 DTE from DB:", error.message);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// ✅ Fetch SPY SPY Intraday Summary
app.get('/api/spy/intraday-summary', async (req, res) => {
  try {
    // --- 1. Fetch daily OHLC summary ---
    const [summary] = await fetchData(`
      SELECT * FROM spy_ohlc_summary 
      WHERE trade_date = CURRENT_DATE
      LIMIT 1;
    `);

    if (!summary) {
      return res.status(404).json({ error: "No OHLC summary found for today." });
    }

    // --- 2. Fetch rolling average ---
    const [rollingAvg] = await fetchData(`
      SELECT * FROM spy_ohlc_averages 
      WHERE date = CURRENT_DATE
      LIMIT 1;
    `);

    // --- 3. Calculate VWAP from today's intraday candles ---
    const [vwapData] = await fetchData(`
      SELECT 
        SUM(((high + low + close) / 3) * volume)::float / NULLIF(SUM(volume), 0) AS vwap
      FROM spy_ohlc
      WHERE start_time::date = CURRENT_DATE;
    `);

    // --- 4. Analyze price structure (retests, consolidation zones, etc.) ---
    const priceStructure = await analyzePriceStructure();

    // --- 5. Compute % move from open to close ---
    const openPrice = parseFloat(summary.open);
    const closePrice = parseFloat(summary.close);
    const percentFromOpen = ((closePrice - openPrice) / openPrice) * 100;

    // --- 6. Respond with the full JSON summary ---
    res.json({
      date: summary.trade_date,
      rolling_avg_18: {
        avg_close: parseFloat(rollingAvg?.avg_close ?? 0),
        latest_close: parseFloat(rollingAvg?.latest_close ?? 0)
      },
      ohlc_summary: {
        open: openPrice,
        high: parseFloat(summary.high),
        low: parseFloat(summary.low),
        close: closePrice,
        total_volume: parseInt(summary.total_volume),
        percent_from_open: parseFloat(percentFromOpen.toFixed(2))  // ✅ new metric
      },
      dealer_exposure: {
        spot_price: parseFloat(summary.spot_price),
        gamma_oi: parseFloat(summary.spot_gamma_oi),
        charm_oi: parseFloat(summary.spot_charm_oi),
        vanna_oi: parseFloat(summary.spot_vanna_oi),
        implied_volatility: parseFloat(summary.implied_volatility)
      },
      vwap: parseFloat(vwapData?.vwap ?? 0),
      price_structure: priceStructure
    });

  } catch (error) {
    console.error("❌ Error generating intraday summary:", error.message);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// ------------------------
// ✅ Start Server
// ------------------------
app.listen(PORT, () => {
    console.log(`🚀 API Server running at http://localhost:${PORT}`);
});
