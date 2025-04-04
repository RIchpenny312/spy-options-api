require('dotenv').config();
const express = require('express');
const { Client } = require('pg');
const { getTimeContext } = require('./utils/time');

const app = express();
const PORT = process.env.PORT || 3000;
const { ensureSpyPartitionForDate } = require('./db/partitionHelpers');

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

// ✅ Exportable function for snapshot logic
async function getMarketTideSnapshot() {
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

  return {
    latest_tide: latestTide || null,
    rolling_avg: rollingAvg || null,
    latest_delta: latestDelta || null
  };
}

// ------------------------
// ✅ API Endpoints
// ------------------------

// 🔹 Fetch SPY OHLC Data (5m)
app.get('/api/spy/ohlc', async (req, res) => {
  try {
    const date = req.query.date || new Date().toISOString().split("T")[0];

    // ✅ Ensure partition exists before querying
    await ensureSpyPartitionForDate(date);

    const data = await fetchData(`
      SELECT *
      FROM spy_ohlc 
      WHERE start_time::date = $1
      ORDER BY start_time ASC
    `, [date]);

    if (!data || data.length === 0) {
      return res.status(404).json({ error: `No SPY OHLC data found for ${date}` });
    }

    res.json(data);

  } catch (error) {
    console.error(`❌ Error fetching SPY OHLC data:`, error.message);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// 🔹 SPY OHLC Daily 
app.get('/api/spy/ohlc/daily', async (req, res) => {
  const date = req.query.date;
  const data = await fetchData(`
    SELECT * FROM spy_ohlc
    WHERE start_time::date = $1
      AND start_time::time BETWEEN '14:30:00' AND '21:00:00'
    ORDER BY start_time;
  `, [date]);

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

// 🔹 Fetch Enhanced Bid/Ask Volume Data (Limited to 5)
app.get('/api/spy/bid-ask-volume-enhanced', async (req, res) => {
    try {
        const data = await fetchData(`
            SELECT 
                symbol, date,
                call_volume, put_volume,
                call_volume_bid_side, call_volume_ask_side,
                put_volume_bid_side, put_volume_ask_side,
                volume_delta_call, volume_delta_put,
                call_put_ratio_bid,
                spoof_flag_call, spoof_flag_put,
                price_open, price_close, price_change, price_direction,
                sentiment, confidence_level,
                recorded_at
            FROM bid_ask_volume_enhanced
            ORDER BY date DESC
            LIMIT 5
        `);
        res.json(data);
    } catch (error) {
        console.error("❌ Error fetching Enhanced Bid/Ask Volume:", error.message);
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

// ✅ Fetch SPY Intraday Summary
app.get('/api/spy/intraday-summary', async (req, res) => {
  const date = req.query.date || new Date().toISOString().split("T")[0];

  try {
    const [
      rollingAvg18,
      ohlcSummary,
      dealerExposure,
      vwapResult,
      priceStructure,
      marketTideLast3,
      marketTideRollingAvg,
      marketTideDeltasToday
    ] = await Promise.all([
      fetchData(`SELECT * FROM spy_rolling_avg_18 WHERE date = $1 LIMIT 1`, [date]),
      fetchData(`SELECT * FROM spy_ohlc_summary WHERE trade_date = $1 LIMIT 1`, [date]),
      fetchData(`SELECT * FROM dealer_exposure WHERE date = $1 LIMIT 1`, [date]),
      fetchData(`SELECT vwap FROM spy_vwap WHERE date = $1 LIMIT 1`, [date]),
      fetchData(`SELECT * FROM spy_price_structure WHERE date = $1 LIMIT 1`, [date]),
      fetchData(`
        SELECT timestamp, net_call_premium, net_put_premium, net_volume
        FROM market_tide_data
        WHERE date = $1
        ORDER BY timestamp DESC
        LIMIT 3
      `, [date]),
      fetchData(`
        SELECT *
        FROM market_tide_rolling_avg
        WHERE date = $1
        ORDER BY timestamp DESC
        LIMIT 1
      `, [date]),
      fetchData(`
        SELECT timestamp, delta_call, delta_put, delta_volume, sentiment
        FROM market_tide_deltas
        WHERE timestamp::date = $1
        ORDER BY timestamp ASC
      `, [date])
    ]);

    res.json({
      date,
      rolling_avg_18: rollingAvg18[0] || {},
      ohlc_summary: ohlcSummary[0] || {},
      dealer_exposure: dealerExposure[0] || {},
      vwap: vwapResult[0]?.vwap || null,
      price_structure: priceStructure[0] || {},
      market_tide: {
        last_3: marketTideLast3.reverse(), // show in ascending time
        rolling_avg: marketTideRollingAvg[0] || {}
      },
      market_tide_deltas: marketTideDeltasToday || []
    });

  } catch (error) {
    console.error("❌ Error building intraday summary:", error.message);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// 🔹 Get latest shift signal for each symbol
app.get('/api/bid-shift-signals/latest', async (req, res) => {
  const data = await fetchData(`
    SELECT DISTINCT ON (symbol) * 
    FROM bid_shift_signals
    ORDER BY symbol, recorded_at DESC
  `);
  res.json(data);
});

// 🔹 Get all continuation signals for today
app.get('/api/bid-shift-signals/continuations', async (req, res) => {
  const data = await fetchData(`
    SELECT * FROM bid_shift_signals
    WHERE continuation = TRUE
    AND recorded_at::date = CURRENT_DATE
    ORDER BY recorded_at DESC
  `);
  res.json(data);
});

// 🔹 Filter shift signals by symbol and confidence
app.get('/api/bid-shift-signals', async (req, res) => {
  const { symbol, confidence } = req.query;
  const params = [];
  let whereClause = "WHERE 1=1";

  if (symbol) {
    params.push(symbol.toUpperCase());
    whereClause += ` AND symbol = $${params.length}`;
  }

  if (confidence) {
    params.push(confidence);
    whereClause += ` AND confidence = $${params.length}`;
  }

  const data = await fetchData(`
    SELECT * FROM bid_shift_signals
    ${whereClause}
    ORDER BY recorded_at DESC
    LIMIT 25
  `, params);

  res.json(data);
});

// ✅ Analyze SPY intraday price structure
async function analyzePriceStructure(date = null) {
  const targetDate = date || new Date().toISOString().split("T")[0];

  const candles = await fetchData(`
    SELECT close FROM spy_ohlc
    WHERE start_time::date = $1
    ORDER BY start_time ASC
  `, [targetDate]);

  const closes = candles.map(c => parseFloat(c.close));
  const levels = {};

  closes.forEach(price => {
    const rounded = price.toFixed(2);
    levels[rounded] = (levels[rounded] || 0) + 1;
  });

  const keyLevels = Object.entries(levels)
    .filter(([_, count]) => count >= 3)
    .map(([price]) => parseFloat(price));

  const lastPrice = closes[closes.length - 1];
  const support = keyLevels.filter(p => p < lastPrice).slice(-3);
  const resistance = keyLevels.filter(p => p > lastPrice).slice(0, 3);

  return {
    retested_levels: keyLevels,
    support_zones: support,
    resistance_zones: resistance,
    consolidation_zones: [] // Add clustering logic later
  };
}

// 🔹 Get all Market Tide Snapshots for Today
app.get('/api/spy/market-tide/snapshot', async (req, res) => {
  try {
    const snapshot = await getMarketTideSnapshot();

    if (!snapshot.latest_tide || !snapshot.rolling_avg || !snapshot.latest_delta) {
      return res.status(404).json({ error: "Incomplete Market Tide snapshot" });
    }

    res.json(snapshot);
  } catch (error) {
    console.error("❌ Snapshot error:", error.message);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// 🔹 GPT Market Analysis Endpoint
app.post('/api/gpt-analysis', async (req, res) => {
  try {
    const today = new Date().toISOString().split("T")[0];

    const [ohlcData, deltas, greeks, bidVolume] = await Promise.all([
      fetchData(`
        SELECT open, high, low, close, total_volume, volume, start_time, end_time
        FROM spy_ohlc 
        WHERE start_time::date = $1 
        ORDER BY start_time ASC
      `, [today]),
      fetchData(`
        SELECT timestamp, delta_call, delta_put, delta_volume, sentiment
        FROM market_tide_deltas 
        WHERE timestamp::date = $1 
        ORDER BY timestamp ASC
      `, [today]),
      fetchData(`
        SELECT strike, call_gex, put_gex, call_delta, put_delta, call_vanna, put_vanna, call_charm, put_charm, price, time
        FROM spy_greek_exposure_strike 
        ORDER BY time DESC 
        LIMIT 10
      `),
      fetchData(`
        SELECT * FROM bid_ask_volume_data 
        WHERE symbol = 'SPY' 
        ORDER BY date DESC 
        LIMIT 1
      `)
    ]);

    const marketTide = await getMarketTideSnapshot();
    const timeContext = getTimeContext(); // Defaults to Chicago

    const gptPayload = {
      ohlc: ohlcData,
      market_tide: marketTide,
      deltas,
      greeks,
      bid_volume: bidVolume[0] || null,
      time_context: timeContext
    };

    const gptResult = await runGptAnalysis(gptPayload); // Stub for now
    res.json({ result: gptResult });

  } catch (error) {
    console.error("❌ GPT analysis failed:", error.message);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// 🔹 GPT Analysis Stub (placeholder for OpenAI or logic)
async function runGptAnalysis(payload) {
  const { time_context } = payload;
  return `Simulated GPT analysis result at ${time_context.time} ET on ${time_context.date}.`;
}

// ------------------------
// ✅ Start Server
// ------------------------
app.listen(PORT, () => {
  console.log(`🚀 API Server running at http://localhost:${PORT}`);
});

module.exports = {
  getMarketTideSnapshot,
  runGptAnalysis
};