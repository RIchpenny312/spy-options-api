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
        SELECT price, total_gex, call_gex, put_gex, call_volume, put_volume, time
        FROM spy_spot_gex
        ORDER BY time DESC
        LIMIT 1
    `);
    res.json(data[0] || { error: "No Spot GEX data available" });
});

// 🔹 Fetch SPY IV (5 DTE)
app.get('/api/spy/iv', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM spy_iv_5dte 
        ORDER BY recorded_at DESC 
        LIMIT 10
    `);
    res.json(data);
});

// 🔹 Fetch Market Tide Data
app.get('/api/spy/market-tide', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM market_tide_data 
        ORDER BY timestamp DESC 
        LIMIT 10
    `);
    res.json(data);
});

// 🔹 Fetch Bid/Ask Volume Data (Limited to 5)
app.get('/api/spy/bid-ask-volume', async (req, res) => {
    const data = await fetchData(`
        SELECT ticker, call_volume, put_volume, call_volume_ask_side, put_volume_ask_side, 
               call_volume_bid_side, put_volume_bid_side, date
        FROM bid_ask_volume_data
        ORDER BY date DESC
        LIMIT 5
    `);
    res.json(data);
});

// 🔹 Fetch SPY Option Price Levels
app.get('/api/spy/option-price-levels', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM spy_option_price_levels 
        ORDER BY time DESC 
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

// ------------------------
// ✅ Start Server
// ------------------------
app.listen(PORT, () => {
    console.log(`🚀 API Server running at http://localhost:${PORT}`);
});
