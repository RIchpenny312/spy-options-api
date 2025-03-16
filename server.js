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

// 🔹 Fetch Bid/Ask Volume Data
app.get('/api/spy/bid-ask', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM bid_ask_volume_data 
        ORDER BY recorded_at DESC 
        LIMIT 10
    `);
    res.json(data);
});

// 🔹 Fetch Option Price Levels
app.get('/api/spy/option-price-levels', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM spy_option_price_levels 
        ORDER BY time DESC 
        LIMIT 10
    `);
    res.json(data);
});

// 🔹 Fetch Greeks by Strike
app.get('/api/spy/greeks', async (req, res) => {
    const data = await fetchData(`
        SELECT * FROM spy_greek_exposure_strike 
        ORDER BY time DESC 
        LIMIT 10
    `);
    res.json(data);
});

// ------------------------
// ✅ Start Server
// ------------------------
app.listen(PORT, () => {
    console.log(`🚀 API Server running at http://localhost:${PORT}`);
});
