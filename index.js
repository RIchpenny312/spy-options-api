require("dotenv").config();
const axios = require("axios");
const { Client } = require("pg");
const dayjs = require("dayjs");

// ✅ Load environment variables
const API_KEY = process.env.API_KEY;
const DB_CONFIG = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl: { rejectUnauthorized: false }
};

// ✅ Function to handle API Rate Limits (Retry on 429 Errors)
async function fetchWithRetry(url, retries = 3, delay = 5000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios.get(url, {
        headers: { Authorization: `Bearer ${process.env.API_KEY}` },
        timeout: 10000, // ⏱ 10-second max wait time
      });
      return response;
    } catch (error) {
      const isRateLimit = error.response?.status === 429;
      const isRetryable =
        isRateLimit ||
        error.code === 'ECONNABORTED' ||
        error.message.includes("socket hang up");

      if (isRetryable && attempt < retries) {
        console.warn(`⚠️ Retry ${attempt}/${retries} for ${url} due to: ${error.message}`);
        await new Promise(res => setTimeout(res, delay));
      } else {
        console.error(`❌ Failed to fetch ${url}:`, error.message);
        throw error;
      }
    }
  }
}

// -----------------------
// Fetch Functions
// -----------------------

// ✅ Function to fetch SPY OHLC Data (5m) for Today
async function fetchSpyOhlcData() {
  try {
    console.log("🔍 Fetching OHLC Data...");
    const response = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/ohlc/5m");

    if (!response.data?.data || !Array.isArray(response.data.data)) {
      throw new Error("Invalid OHLC response format");
    }

    const today = dayjs().format("YYYY-MM-DD");
    const filteredData = response.data.data.filter(item => item.start_time.startsWith(today));

    if (filteredData.length === 0) {
      console.log("⚠️ No OHLC data found for today. Skipping insertion.");
      return [];
    }

    return filteredData.map(item => ({
      open: parseFloat(item.open) || 0,
      high: parseFloat(item.high) || 0,
      low: parseFloat(item.low) || 0,
      close: parseFloat(item.close) || 0,
      total_volume: parseInt(item.total_volume) || 0,
      volume: parseInt(item.volume) || 0,
      start_time: item.start_time,
      end_time: item.end_time
    }));
  } catch (error) {
    console.error("❌ Error fetching OHLC data:", error.message);
    return [];
  }
}

// ✅ Function to fetch SPY SPOT GEX
async function fetchSpySpotGex() {
    try {
        console.log("🔍 Fetching SPOT GEX...");
        const response = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/spot-exposures");

        // Debugging: Log API response
        console.log("SPY Spot GEX API Response:", response.data);

        if (!response.data?.data || !Array.isArray(response.data.data) || response.data.data.length === 0) {
            throw new Error("Invalid or empty SPOT GEX response format");
        }

        // ✅ Extract the most recent record
        const latestData = response.data.data.sort((a, b) => new Date(b.time) - new Date(a.time))[0];

        if (!latestData?.time || !latestData?.price) {
            console.error("❌ Missing critical Spot GEX fields in API response", latestData);
            return [];
        }

        return [{
            symbol: "SPY",
            date: latestData.time.split("T")[0], // Extract YYYY-MM-DD
            price: parseFloat(latestData.price) || 0,
            charm_oi: parseFloat(latestData.charm_per_one_percent_move_oi) || 0,
            gamma_oi: parseFloat(latestData.gamma_per_one_percent_move_oi) || 0,
            vanna_oi: parseFloat(latestData.vanna_per_one_percent_move_oi) || 0,
            time: latestData.time,
            ticker: latestData.ticker || "SPY"
        }];
    } catch (error) {
        console.error('❌ Error fetching SPY SPOT GEX:', error.message);
        return [];
    }
}

// ✅ Function to fetch SPY Greeks by Strike (Top 5 Call GEX & Top 5 Put GEX)
// The put_gex value is converted to its absolute value.
async function fetchSpyGreeksByStrike() {
  try {
    console.log("🔍 Fetching Greeks by Strike...");
    const response = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/greek-exposure/strike");
    if (!response.data?.data || !Array.isArray(response.data.data)) {
      throw new Error("Invalid Greeks by Strike response format");
    }
    // Filter top 5 call GEX and top 5 put GEX strikes
    const top5CallGex = response.data.data
      .filter(item => item.call_gex && parseFloat(item.call_gex) !== 0)
      .sort((a, b) => parseFloat(b.call_gex) - parseFloat(a.call_gex))
      .slice(0, 5);
    const top5PutGex = response.data.data
      .filter(item => item.put_gex && parseFloat(item.put_gex) !== 0)
      .sort((a, b) => parseFloat(b.put_gex) - parseFloat(a.put_gex))
      .slice(0, 5);
    const topStrikes = [...top5CallGex, ...top5PutGex];
    if (topStrikes.length === 0) {
      console.log("⚠️ No valid SPY Greeks data found for top 5 Call and Put GEX strikes.");
      return [];
    }
    console.log("📊 Processed Top 5 Call GEX & Top 5 Put GEX Strikes:", topStrikes);
    return topStrikes.map(item => ({
      strike: parseFloat(item.strike) || null,
      call_gex: parseFloat(item.call_gex) || 0,
      put_gex: Math.abs(parseFloat(item.put_gex)) || 0,
      call_delta: parseFloat(item.call_delta) || 0,
      put_delta: parseFloat(item.put_delta) || 0,
      call_vanna: parseFloat(item.call_vanna) || 0,
      put_vanna: parseFloat(item.put_vanna) || 0,
      call_charm: parseFloat(item.call_charm) || 0,
      put_charm: parseFloat(item.put_charm) || 0,
      price: parseFloat(item.price) || null,
      time: item.date || null
    }));
  } catch (error) {
    console.error('❌ Error fetching SPY Greeks by Strike:', error.message);
    return [];
  }
}

// ✅ Function to fetch SPY Option Price Levels (Top 10 by total volume)
async function fetchSpyOptionPriceLevels() {
  try {
    console.log("🔍 Fetching Today’s Option Price Levels...");
    const response = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/option/stock-price-levels");

    if (!response.data?.data || !Array.isArray(response.data.data)) {
      throw new Error("Invalid Option Price Levels response format");
    }

    const today = new Date().toISOString().split("T")[0]; // Get today's date

    // Assign today's date as the time if missing
    const processedData = response.data.data.map(item => ({
      price: parseFloat(item.price) || 0,
      call_volume: parseInt(item.call_volume) || 0,
      put_volume: parseInt(item.put_volume) || 0,
      total_volume: (parseInt(item.call_volume) || 0) + (parseInt(item.put_volume) || 0),
      time: today // Assign today's date since it's missing
    }));

    // Sort and take top 10 by total volume
    const top10Today = processedData
      .sort((a, b) => b.total_volume - a.total_volume)
      .slice(0, 10);

    console.log("📊 Processed Top 10 SPY Option Price Levels for Today:", top10Today);
    return top10Today;

  } catch (error) {
    console.error('❌ Error fetching SPY Option Price Levels:', error.message);
    return [];
  }
}

// ✅ Function to fetch Market Tide Data
async function fetchMarketTideData() {
    try {
        console.log("🔍 Fetching Market Tide Data...");
        const response = await fetchWithRetry("https://api.unusualwhales.com/api/market/market-tide?otm_only=false&interval_5m=true");

        if (!response.data?.data || !Array.isArray(response.data.data)) {
            throw new Error("Invalid Market Tide response format");
        }

        return response.data.data.map(item => ({
            date: item.date,
            timestamp: item.timestamp,
            net_call_premium: parseFloat(item.net_call_premium) || 0,
            net_put_premium: parseFloat(item.net_put_premium) || 0,
            net_volume: parseInt(item.net_volume) || 0
        }));
    } catch (error) {
        if (error.response && error.response.status === 404) {
            console.warn("⚠️ Market Tide API returned 404. Skipping this dataset.");
            return [];
        }
        console.error("❌ Error fetching Market Tide Data:", error.message);
        return [];
    }
}

// ✅ Function to fetch and store 1-hour & 4-hour rolling averages of Market Tide Data
async function fetchAndStoreMarketTideRollingAvg(client) {
    try {
        console.log("📊 Computing and inserting Market Tide Rolling Averages...");

        // SQL Query to calculate rolling averages for last 12 and last 48 intervals
        const query = `
            WITH last_12_intervals AS (
                SELECT 
                    AVG(net_call_premium) AS avg_net_call_premium_12_intervals,
                    AVG(net_put_premium) AS avg_net_put_premium_12_intervals,
                    AVG(net_volume) AS avg_net_volume_12_intervals
                FROM (
                    SELECT net_call_premium, net_put_premium, net_volume
                    FROM market_tide_data
                    ORDER BY timestamp DESC
                    LIMIT 12
                ) AS last_12
            ),
            last_48_intervals AS (
                SELECT 
                    AVG(net_call_premium) AS avg_net_call_premium_48_intervals,
                    AVG(net_put_premium) AS avg_net_put_premium_48_intervals,
                    AVG(net_volume) AS avg_net_volume_48_intervals
                FROM (
                    SELECT net_call_premium, net_put_premium, net_volume
                    FROM market_tide_data
                    ORDER BY timestamp DESC
                    LIMIT 48
                ) AS last_48
            )
            INSERT INTO market_tide_rolling_avg (
                date, timestamp, avg_net_call_premium_12_intervals, avg_net_put_premium_12_intervals, avg_net_volume_12_intervals,
                avg_net_call_premium_48_intervals, avg_net_put_premium_48_intervals, avg_net_volume_48_intervals, recorded_at
            )
            SELECT 
                CURRENT_DATE, NOW(),
                COALESCE(l12.avg_net_call_premium_12_intervals, 0.0),
                COALESCE(l12.avg_net_put_premium_12_intervals, 0.0),
                COALESCE(l12.avg_net_volume_12_intervals, 0),
                COALESCE(l48.avg_net_call_premium_48_intervals, 0.0),
                COALESCE(l48.avg_net_put_premium_48_intervals, 0.0),
                COALESCE(l48.avg_net_volume_48_intervals, 0),
                NOW()
            FROM last_12_intervals l12, last_48_intervals l48
            ON CONFLICT (timestamp) DO UPDATE SET 
                avg_net_call_premium_12_intervals = EXCLUDED.avg_net_call_premium_12_intervals,
                avg_net_put_premium_12_intervals = EXCLUDED.avg_net_put_premium_12_intervals,
                avg_net_volume_12_intervals = EXCLUDED.avg_net_volume_12_intervals,
                avg_net_call_premium_48_intervals = EXCLUDED.avg_net_call_premium_48_intervals,
                avg_net_put_premium_48_intervals = EXCLUDED.avg_net_put_premium_48_intervals,
                avg_net_volume_48_intervals = EXCLUDED.avg_net_volume_48_intervals,
                recorded_at = NOW();
        `;

        await client.query(query);
        console.log("✅ Market Tide Rolling Averages inserted successfully.");

    } catch (error) {
        console.error("❌ Error inserting Market Tide Rolling Averages:", error.message);
    }
}

// Function to fetch Today Market Tide Data
async function fetchTodayMarketTideDataFromDB() {
  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    const result = await client.query(`
      SELECT date, timestamp, net_call_premium, net_put_premium, net_volume
      FROM market_tide_data
      WHERE date = CURRENT_DATE
      ORDER BY timestamp ASC;
    `);
    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching today's Market Tide data:", error.message);
    return [];
  } finally {
    await client.end();
  }
}

// Function to fetch BID ASK Volume data for a given ticker
async function fetchBidAskVolumeData(ticker) {
    try {
        console.log(`🔍 Fetching BID ASK Volume for ${ticker}...`);
        const response = await fetchWithRetry(`https://api.unusualwhales.com/api/stock/${ticker}/options-volume`);

        if (!response.data?.data || response.data.data.length === 0) {
            throw new Error(`Invalid ${ticker} BID ASK Volume response format`);
        }

        const item = response.data.data[0];  // Extract first object
        return [{
            ticker: ticker.toUpperCase(),
            date: item.date || null,
            avg_30_day_call_volume: parseFloat(item.avg_30_day_call_volume) || 0,
            avg_30_day_put_volume: parseFloat(item.avg_30_day_put_volume) || 0,
            avg_3_day_call_volume: parseFloat(item.avg_3_day_call_volume) || 0,
            avg_3_day_put_volume: parseFloat(item.avg_3_day_put_volume) || 0,
            avg_7_day_call_volume: parseFloat(item.avg_7_day_call_volume) || 0,
            avg_7_day_put_volume: parseFloat(item.avg_7_day_put_volume) || 0,
            bearish_premium: parseFloat(item.bearish_premium) || 0,
            bullish_premium: parseFloat(item.bullish_premium) || 0,
            call_open_interest: parseInt(item.call_open_interest) || 0,
            put_open_interest: parseInt(item.put_open_interest) || 0,
            call_premium: parseFloat(item.call_premium) || 0,
            put_premium: parseFloat(item.put_premium) || 0,
            call_volume: parseInt(item.call_volume) || 0,
            put_volume: parseInt(item.put_volume) || 0,
            call_volume_ask_side: parseInt(item.call_volume_ask_side) || 0,
            put_volume_ask_side: parseInt(item.put_volume_ask_side) || 0,
            call_volume_bid_side: parseInt(item.call_volume_bid_side) || 0,
            put_volume_bid_side: parseInt(item.put_volume_bid_side) || 0,
            net_call_premium: parseFloat(item.net_call_premium) || 0,
            net_put_premium: parseFloat(item.net_put_premium) || 0
        }];
    } catch (error) {
        console.error(`❌ Error fetching ${ticker} BID ASK Volume:`, error.message);
        return [];
    }
}

// ✅ Function to fetch SPY IV for 0 DTE
async function fetchSpyIV0DTE() {
    try {
        console.log("🔍 Fetching SPY Implied Volatility for 0 DTE...");
        const response = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/volatility/term-structure");

        // ✅ Log the response
        console.log("Full API Response:", JSON.stringify(response.data, null, 2));

        if (!response.data?.data || !Array.isArray(response.data.data)) {
            throw new Error("Invalid SPY IV response format");
        }

        // ✅ Find `dte = 0`
        const ivData = response.data.data.find(item => item.dte === 0);

        if (!ivData) {
            console.warn("⚠️ No IV data found for DTE = 0");
            return [];
        }

        function safeParseFloat(value) {
            return isNaN(parseFloat(value)) ? null : parseFloat(value);
        }

        return [{
            ticker: ivData.ticker || "SPY",
            date: ivData.date || null,
            expiry: ivData.expiry || null,
            dte: ivData.dte || 0,
            implied_move: safeParseFloat(ivData.implied_move),
            implied_move_perc: safeParseFloat(ivData.implied_move_perc),
            volatility: safeParseFloat(ivData.volatility)
        }];
    } catch (error) {
        console.error("❌ Error fetching SPY IV 0 DTE data:", error.message);
        return [];
    }
}

// ✅ Fetch SPY and SPX Greek Exposure
async function fetchGreekExposure(symbol) {
  try {
    console.log(`🔍 Fetching ${symbol} Greek Exposure...`);
    const response = await fetchWithRetry(`https://api.unusualwhales.com/api/stock/${symbol}/greek-exposure`);
    
    if (!response.data?.data || !Array.isArray(response.data.data)) {
      throw new Error(`Invalid ${symbol} Greek Exposure response format`);
    }

    // Ensure each entry has a date, and sort by date (most recent first)
    const sortedData = response.data.data
      .filter(item => item.date) // Ensure we only process entries with a valid date
      .sort((a, b) => new Date(b.date) - new Date(a.date)) // Sort descending
      .slice(0, 5); // Select the last 5 records

    if (sortedData.length === 0) {
      console.warn(`⚠️ No valid data found for ${symbol} Greek Exposure.`);
      return [];
    }

    return sortedData.map(item => ({
      symbol,
      date: item.date, // ✅ Now explicitly using `date` instead of `time`
      call_charm: parseFloat(item.call_charm) || 0,
      call_delta: parseFloat(item.call_delta) || 0,
      call_gamma: parseFloat(item.call_gamma) || 0,
      call_vanna: parseFloat(item.call_vanna) || 0,
      put_charm: parseFloat(item.put_charm) || 0,
      put_delta: parseFloat(item.put_delta) || 0,
      put_gamma: parseFloat(item.put_gamma) || 0,
      put_vanna: parseFloat(item.put_vanna) || 0
    }));
  } catch (error) {
    console.error(`❌ Error fetching ${symbol} Greek Exposure:`, error.message);
    return [];
  }
}

// -----------------------
// Storage Functions
// -----------------------

// Store SPY OHLC Data in DB
async function storeSpyOhlcDataInDB(data) {
  if (!data.length) {
    console.warn("⚠️ No SPY OHLC data to insert.");
    return;
  }

  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    console.log("✅ Inserting SPY OHLC data into DB...");

    for (const item of data) {
      await client.query(
        `INSERT INTO spy_ohlc (open, high, low, close, total_volume, volume, start_time, end_time, recorded_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
         ON CONFLICT (start_time) DO UPDATE SET 
           open = EXCLUDED.open, 
           high = EXCLUDED.high, 
           low = EXCLUDED.low, 
           close = EXCLUDED.close, 
           total_volume = EXCLUDED.total_volume, 
           volume = EXCLUDED.volume, 
           recorded_at = NOW();`,
        [item.open, item.high, item.low, item.close, item.total_volume, item.volume, item.start_time, item.end_time]
      );
    }

    console.log("✅ SPY OHLC Data inserted successfully.");

    // ✅ Compute and Store the DT Average **AFTER** inserting
    await fetchAndStoreSpyOhlcAverages(client);

  } catch (error) {
    console.error("❌ Error inserting SPY OHLC data:", error.message);
  } finally {
    await client.end();
  }
}

// Store SPY SPOT GEX Data in DB
async function storeSpySpotGexInDB(data) {
    const client = new Client(DB_CONFIG);
    await client.connect();
    try {
        for (const item of data) {
            if (!item.time || item.price === 0) {
                console.warn("⚠️ Skipping invalid SPOT GEX entry:", item);
                continue;
            }

            await client.query(
                `INSERT INTO spy_spot_gex (symbol, date, price, charm_oi, gamma_oi, vanna_oi, time, ticker, recorded_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                 ON CONFLICT (symbol, time) 
                 DO UPDATE SET 
                     price = EXCLUDED.price,
                     charm_oi = EXCLUDED.charm_oi,
                     gamma_oi = EXCLUDED.gamma_oi,
                     vanna_oi = EXCLUDED.vanna_oi,
                     recorded_at = NOW();`,
                [item.symbol, item.date, item.price, item.charm_oi, item.gamma_oi, item.vanna_oi, item.time, item.ticker]
            );
        }
        console.log('✅ SPY SPOT GEX Data inserted successfully');
    } catch (error) {
        console.error('❌ Error inserting SPY SPOT GEX:', error.message);
    } finally {
        await client.end();
    }
}

// Store SPY Option Price Levels Data in DB
async function storeSpyOptionPriceLevelsInDB(data) {
  const client = new Client(DB_CONFIG);
  await client.connect();
  try {
    for (const item of data) {
      console.log("📊 Inserting Today’s Option Price Level:", JSON.stringify(item, null, 2));

      await client.query(
        `INSERT INTO spy_option_price_levels (
            price, call_volume, put_volume, total_volume, time, recorded_at
        ) VALUES (
            $1, $2, $3, $4, $5, NOW()
        )
        ON CONFLICT (price, time)
        DO UPDATE SET 
            call_volume = EXCLUDED.call_volume,
            put_volume = EXCLUDED.put_volume,
            total_volume = EXCLUDED.total_volume,
            recorded_at = NOW()
        WHERE EXCLUDED.time::date = CURRENT_DATE;`, // ✅ Ensures only today's records are updated
        [
          item.price,
          item.call_volume,
          item.put_volume,
          item.total_volume,
          item.time // ✅ Now always has today's date
        ]
      );
    }
    console.log('✅ SPY Option Price Levels Data (Only Today) inserted successfully');
  } catch (error) {
    console.error('❌ Error inserting SPY Option Price Levels:', error.message);
  } finally {
    await client.end();
  }
}

// Store SPY Greeks by Strike Data in DB
async function storeSpyGreeksByStrikeInDB(data) {
    const client = new Client(DB_CONFIG);
    await client.connect();
    try {
        for (const item of data) {
            console.log("📊 Inserting Greeks by Strike:", JSON.stringify(item, null, 2));

            await client.query(
                `INSERT INTO spy_greek_exposure_strike (
                    strike, price, call_gamma_oi, put_gamma_oi, 
                    call_gamma_vol, put_gamma_vol, call_vanna_oi, put_vanna_oi, 
                    call_vanna_vol, put_vanna_vol, call_charm_oi, put_charm_oi, 
                    call_charm_vol, put_charm_vol, call_gex, put_gex, 
                    call_delta, put_delta, total_oi, total_gex, time, recorded_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, 
                    $9, $10, $11, $12, $13, $14, $15, $16, 
                    $17, $18, $19, $20, $21, NOW()
                )
                ON CONFLICT (strike, time)
                DO UPDATE SET 
                    price = EXCLUDED.price,
                    call_gamma_oi = EXCLUDED.call_gamma_oi,
                    put_gamma_oi = EXCLUDED.put_gamma_oi,
                    call_gamma_vol = EXCLUDED.call_gamma_vol,
                    put_gamma_vol = EXCLUDED.put_gamma_vol,
                    call_vanna_oi = EXCLUDED.call_vanna_oi,
                    put_vanna_oi = EXCLUDED.put_vanna_oi,
                    call_vanna_vol = EXCLUDED.call_vanna_vol,
                    put_vanna_vol = EXCLUDED.put_vanna_vol,
                    call_charm_oi = EXCLUDED.call_charm_oi,
                    put_charm_oi = EXCLUDED.put_charm_oi,
                    call_charm_vol = EXCLUDED.call_charm_vol,
                    put_charm_vol = EXCLUDED.put_charm_vol,
                    call_gex = EXCLUDED.call_gex,
                    put_gex = EXCLUDED.put_gex,
                    call_delta = EXCLUDED.call_delta,
                    put_delta = EXCLUDED.put_delta,
                    total_oi = EXCLUDED.total_oi,
                    total_gex = EXCLUDED.total_gex,
                    recorded_at = NOW();`,
                [
                    parseFloat(item.strike) || 0,
                    parseFloat(item.price) || 0,
                    parseFloat(item.call_gamma_oi) || 0,
                    parseFloat(item.put_gamma_oi) || 0,
                    parseFloat(item.call_gamma_vol) || 0,
                    parseFloat(item.put_gamma_vol) || 0,
                    parseFloat(item.call_vanna_oi) || 0,
                    parseFloat(item.put_vanna_oi) || 0,
                    parseFloat(item.call_vanna_vol) || 0,
                    parseFloat(item.put_vanna_vol) || 0,
                    parseFloat(item.call_charm_oi) || 0,
                    parseFloat(item.put_charm_oi) || 0,
                    parseFloat(item.call_charm_vol) || 0,
                    parseFloat(item.put_charm_vol) || 0,
                    parseFloat(item.call_gex) || 0,
                    Math.abs(parseFloat(item.put_gex) || 0),  // Ensure put_gex is absolute
                    parseFloat(item.call_delta) || 0,
                    parseFloat(item.put_delta) || 0,
                    parseFloat(item.total_oi) || 0,
                    parseFloat(item.total_gex) || 0,
                    item.time || null
                ]
            );
        }
        console.log('✅ SPY Greeks by Strike Data inserted successfully');
    } catch (error) {
        console.error('❌ Error inserting SPY Greeks by Strike:', error.message);
    } finally {
        await client.end();
    }
}

// ✅ Function to compute and store Market Tide Averages
async function fetchAndStoreMarketTideAverages(client) {
    try {
        console.log("📊 Computing and inserting Market Tide Averages...");

        await client.query(`
            WITH last_18_intervals AS (
                SELECT * FROM market_tide_data ORDER BY timestamp DESC LIMIT 18
            )
            INSERT INTO market_tide_averages (
                date, latest_net_call_premium, latest_net_put_premium, latest_net_volume, 
                avg_net_call_premium, avg_net_put_premium, avg_net_volume, recorded_at
            )
            SELECT 
                CURRENT_DATE,
                (SELECT net_call_premium FROM market_tide_data ORDER BY timestamp DESC LIMIT 1),
                (SELECT net_put_premium FROM market_tide_data ORDER BY timestamp DESC LIMIT 1),
                (SELECT net_volume FROM market_tide_data ORDER BY timestamp DESC LIMIT 1),
                AVG(net_call_premium),
                AVG(net_put_premium),
                AVG(net_volume),
                NOW()
            FROM last_18_intervals
            ON CONFLICT (date) DO UPDATE SET 
                latest_net_call_premium = EXCLUDED.latest_net_call_premium,
                latest_net_put_premium = EXCLUDED.latest_net_put_premium,
                latest_net_volume = EXCLUDED.latest_net_volume,
                avg_net_call_premium = EXCLUDED.avg_net_call_premium,
                avg_net_put_premium = EXCLUDED.avg_net_put_premium,
                avg_net_volume = EXCLUDED.avg_net_volume,
                recorded_at = NOW();
        `);

        console.log("✅ Market Tide Averages inserted successfully.");
    } catch (error) {
        console.error("❌ Error inserting Market Tide Averages:", error.message);
    }
}

// ✅ Function to store Market Tide Data in DB
async function storeMarketTideDataInDB(data) {
    if (!data.length) {
        console.warn("⚠️ No Market Tide data to insert.");
        return;
    }

    const client = new Client(DB_CONFIG);
    await client.connect();

    try {
        console.log("✅ Inserting Market Tide data into DB...");

        for (const entry of data) {
            console.log("📊 Attempting to insert:", entry);

            await client.query(`
                INSERT INTO market_tide_data (date, timestamp, net_call_premium, net_put_premium, net_volume, recorded_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (timestamp) DO UPDATE SET 
                    net_call_premium = EXCLUDED.net_call_premium,
                    net_put_premium = EXCLUDED.net_put_premium,
                    net_volume = EXCLUDED.net_volume,
                    recorded_at = NOW();
            `, [
                entry.date,
                entry.timestamp,
                entry.net_call_premium,
                entry.net_put_premium,
                entry.net_volume
            ]);
        }

        console.log("✅ Market Tide data inserted successfully.");
        await fetchAndStoreMarketTideAverages(client);

    } catch (error) {
        console.error("❌ Error inserting Market Tide Data:", error.message);
    } finally {
        await client.end();
    }
}

// ✅ Function to store Market Tide Deltas
async function storeMarketTideDeltasInDB(deltas) {
  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    console.log("📊 Inserting Market Tide Deltas...");
    for (const d of deltas) {
      await client.query(`
        INSERT INTO market_tide_deltas (timestamp, delta_call, delta_put, delta_volume, sentiment, recorded_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (timestamp) DO UPDATE SET 
          delta_call = EXCLUDED.delta_call,
          delta_put = EXCLUDED.delta_put,
          delta_volume = EXCLUDED.delta_volume,
          sentiment = EXCLUDED.sentiment,
          recorded_at = NOW();
      `, [d.timestamp, d.delta_call, d.delta_put, d.delta_volume, d.sentiment]);
    }
    console.log("✅ Market Tide Deltas stored.");
  } catch (error) {
    console.error("❌ Error storing Market Tide Deltas:", error.message);
  } finally {
    await client.end();
  }
}

// Function to store BID ASK Volume Data in DB
async function storeBidAskVolumeDataInDB(data) {
  const client = new Client(DB_CONFIG);
  await client.connect();
  try {
    for (const item of data) {
      const symbol = item.ticker ? item.ticker.toUpperCase() : "UNKNOWN"; // Fix undefined symbol issue
      console.log(`📊 Inserting BID ASK Volume for ${symbol}:`, JSON.stringify(item, null, 2));

      await client.query(
        `INSERT INTO bid_ask_volume_data (
            symbol, date, avg_30_day_call_volume, avg_30_day_put_volume, 
            avg_3_day_call_volume, avg_3_day_put_volume, avg_7_day_call_volume, avg_7_day_put_volume,
            bearish_premium, bullish_premium, call_open_interest, put_open_interest,
            call_premium, put_premium, call_volume, put_volume, call_volume_ask_side, put_volume_ask_side,
            call_volume_bid_side, put_volume_bid_side, net_call_premium, net_put_premium, recorded_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
            $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, NOW()
        )
        ON CONFLICT (symbol, date)
        DO UPDATE SET 
            avg_30_day_call_volume = EXCLUDED.avg_30_day_call_volume,
            avg_30_day_put_volume = EXCLUDED.avg_30_day_put_volume,
            avg_3_day_call_volume = EXCLUDED.avg_3_day_call_volume,
            avg_3_day_put_volume = EXCLUDED.avg_3_day_put_volume,
            avg_7_day_call_volume = EXCLUDED.avg_7_day_call_volume,
            avg_7_day_put_volume = EXCLUDED.avg_7_day_put_volume,
            bearish_premium = EXCLUDED.bearish_premium,
            bullish_premium = EXCLUDED.bullish_premium,
            call_open_interest = EXCLUDED.call_open_interest,
            put_open_interest = EXCLUDED.put_open_interest,
            call_premium = EXCLUDED.call_premium,
            put_premium = EXCLUDED.put_premium,
            call_volume = EXCLUDED.call_volume,
            put_volume = EXCLUDED.put_volume,
            call_volume_ask_side = EXCLUDED.call_volume_ask_side,
            put_volume_ask_side = EXCLUDED.put_volume_ask_side,
            call_volume_bid_side = EXCLUDED.call_volume_bid_side,
            put_volume_bid_side = EXCLUDED.put_volume_bid_side,
            net_call_premium = EXCLUDED.net_call_premium,
            net_put_premium = EXCLUDED.net_put_premium,
            recorded_at = NOW();`,
        [
          symbol, item.date, item.avg_30_day_call_volume, item.avg_30_day_put_volume,
          item.avg_3_day_call_volume, item.avg_3_day_put_volume, item.avg_7_day_call_volume, item.avg_7_day_put_volume,
          item.bearish_premium, item.bullish_premium, item.call_open_interest, item.put_open_interest,
          item.call_premium, item.put_premium, item.call_volume, item.put_volume,
          item.call_volume_ask_side, item.put_volume_ask_side,
          item.call_volume_bid_side, item.put_volume_bid_side,
          item.net_call_premium, item.net_put_premium
        ]
      );
    }
    console.log('✅ BID ASK Volume Data inserted successfully');
  } catch (error) {
    console.error('❌ Error inserting BID ASK Volume Data:', error.message);
  } finally {
    await client.end();
  }
}

// Function to store BID Shift Signals
async function storeBidShiftSignals(bidAskData) {
  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    for (const current of bidAskData) {
      const { ticker, call_volume_bid_side, put_volume_bid_side, volume_delta_call, volume_delta_put } = current;
      const symbol = ticker.toUpperCase();
      const recorded_at = new Date(); // or use current.date if needed

      // Step 1: Determine current dominant side
      const dominant_side = put_volume_bid_side > call_volume_bid_side ? 'PUT' : 'CALL';

      // Step 2: Get previous dominant_side from DB
      const prevQuery = await client.query(
        `SELECT dominant_side FROM bid_shift_signals WHERE symbol = $1 ORDER BY recorded_at DESC LIMIT 1`,
        [symbol]
      );

      const previous_dominant_side = prevQuery.rows.length ? prevQuery.rows[0].dominant_side : null;

      // Step 3: Detect shift
      let shift_type = 'NONE';
      if (previous_dominant_side && dominant_side !== previous_dominant_side) {
        shift_type = `${previous_dominant_side}_TO_${dominant_side}`;
      }

      // Step 4: Flag continuation
      const continuation = previous_dominant_side === dominant_side;

      // Step 5: Confirm delta
      let delta_confirmation = false;
      if (
        (dominant_side === 'PUT' && volume_delta_put > 0) ||
        (dominant_side === 'CALL' && volume_delta_call > 0)
      ) {
        delta_confirmation = true;
      }

      // Step 6: Assign confidence
      let confidence = 'Low';
      if (shift_type !== 'NONE' && delta_confirmation) confidence = 'High';
      else if (continuation && delta_confirmation) confidence = 'Moderate';

      // Step 7: Insert into DB
      await client.query(
        `INSERT INTO bid_shift_signals (
          symbol, recorded_at, dominant_side, previous_dominant_side, shift_type, continuation, delta_confirmation, confidence
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8
        )`,
        [symbol, recorded_at, dominant_side, previous_dominant_side, shift_type, continuation, delta_confirmation, confidence]
      );

      console.log(`✅ Shift Signal Inserted for ${symbol}: ${shift_type} | Continuation: ${continuation} | Confidence: ${confidence}`);
    }
  } catch (error) {
    console.error("❌ Error inserting bid shift signals:", error.message);
  } finally {
    await client.end();
  }
}

// ✅ Function to store SPY IV Data (0 DTE) in DB
async function storeSpyIV0DTEDataInDB(data) {
    if (!data.length) {
        console.warn("⚠️ No SPY IV (0 DTE) data to insert.");
        return;
    }

    const client = new Client(DB_CONFIG);
    await client.connect();

    try {
        console.log("✅ Inserting SPY IV (0 DTE) data into DB...");

        function safeValue(val) {
            return (val === undefined || val === null) ? null : val;
        }

        for (const item of data) {
            console.log("📊 Inserting:", JSON.stringify(item, null, 2));

            await client.query(
                `INSERT INTO spy_iv_0dte (
                    symbol, date, expiry, dte, implied_move, implied_move_perc, volatility, recorded_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, NOW()
                )
                ON CONFLICT (symbol, date, dte)
                DO UPDATE SET 
                    implied_move = COALESCE(EXCLUDED.implied_move, spy_iv_0dte.implied_move),
                    implied_move_perc = COALESCE(EXCLUDED.implied_move_perc, spy_iv_0dte.implied_move_perc),
                    volatility = COALESCE(EXCLUDED.volatility, spy_iv_0dte.volatility),
                    recorded_at = NOW();`,
                [
                    safeValue(item.ticker),
                    safeValue(item.date),
                    safeValue(item.expiry),
                    safeValue(item.dte),
                    safeValue(item.implied_move),
                    safeValue(item.implied_move_perc),
                    safeValue(item.volatility)
                ]
            );
        }

        console.log("✅ SPY IV (0 DTE) Data inserted successfully.");

        // Fetch latest SPY IV data for Custom GPT Analysis
        const result = await client.query(
            `SELECT * FROM spy_iv_0dte ORDER BY date DESC, recorded_at DESC LIMIT 5;`
        );

        console.log("📊 Last 5 SPY IV (0 DTE) Records for Custom GPT Analysis:", result.rows);

    } catch (error) {
        console.error("❌ Error inserting SPY IV (0 DTE) Data:", error.message);
    } finally {
        await client.end();
    }
}

// ✅ Store SPY and SPX Greek Exposure Data in DB
async function storeGreekExposureInDB(data) {
  const client = new Client(DB_CONFIG);
  await client.connect();
  try {
    if (!data || data.length === 0) {
      console.warn("⚠️ No Greek Exposure data to insert. Skipping.");
      return;
    }

    // ✅ Keep only the last 5 records
    const latestData = data.slice(0, 5);

    for (const item of latestData) {
      console.log(`📊 Inserting ${item.symbol} Greek Exposure for ${item.date}:`, JSON.stringify(item, null, 2));

      await client.query(
        `INSERT INTO greek_exposure (
            symbol, date, call_charm, call_delta, call_gamma, call_vanna, 
            put_charm, put_delta, put_gamma, put_vanna, recorded_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW()
        )
        ON CONFLICT (symbol, date)
        DO UPDATE SET 
            call_charm = EXCLUDED.call_charm,
            call_delta = EXCLUDED.call_delta,
            call_gamma = EXCLUDED.call_gamma,
            call_vanna = EXCLUDED.call_vanna,
            put_charm = EXCLUDED.put_charm,
            put_delta = EXCLUDED.put_delta,
            put_gamma = EXCLUDED.put_gamma,
            put_vanna = EXCLUDED.put_vanna,
            recorded_at = NOW();`, // ✅ Updates timestamp
        [
          item.symbol,
          item.date,
          item.call_charm,
          item.call_delta,
          item.call_gamma,
          item.call_vanna,
          item.put_charm,
          item.put_delta,
          item.put_gamma,
          item.put_vanna,
        ]
      );
    }
    console.log('✅ Greek Exposure Data inserted successfully');
  } catch (error) {
    console.error('❌ Error inserting Greek Exposure:', error.message);
  } finally {
    await client.end();
  }
}

// -----------------------
// Fetch and Store Functions
// -----------------------

// ✅ Fetch and Store Enhanced Bid Ask Volume
async function fetchAndStoreEnhancedBidAsk(ticker, price_open, price_close) {
  try {
    console.log(`🔍 Fetching BID/ASK volume for ${ticker}...`);
    const response = await fetchWithRetry(`https://api.unusualwhales.com/api/stock/${ticker}/options-volume`);

    const raw = response?.data?.data?.[0];
    if (!raw) {
      console.warn(`⚠️ No volume data returned for ${ticker}`);
      return;
    }

    const bidCall = raw.call_volume_bid_side || 0;
    const askCall = raw.call_volume_ask_side || 0;
    const bidPut = raw.put_volume_bid_side || 0;
    const askPut = raw.put_volume_ask_side || 0;

    const volumeDeltaCall = bidCall - askCall;
    const volumeDeltaPut = bidPut - askPut;
    const callPutRatio = bidPut > 0 ? bidCall / bidPut : null;

    const priceChange = price_close - price_open;
    const priceDir = priceChange > 0 ? "up" : priceChange < 0 ? "down" : "flat";

    const spoofFlagCall = askCall > bidCall && priceDir === "up";
    const spoofFlagPut = bidPut > askPut && priceDir === "down";

    // Sentiment Logic
    let sentiment = "Neutral";
    if (bidPut > bidCall && volumeDeltaPut > 0) sentiment = "Bullish";
    else if (bidCall > bidPut && volumeDeltaCall > 0) sentiment = "Bearish";

    // Confidence logic — basic, to be upgraded if mult-ticker available
    let confidence = (Math.abs(volumeDeltaCall) > 100000 || Math.abs(volumeDeltaPut) > 100000) ? "High" : "Moderate";

    const data = {
      symbol: ticker,
      date: dayjs().format("YYYY-MM-DD"),
      recorded_at: new Date(),

      call_volume: raw.call_volume,
      put_volume: raw.put_volume,
      call_volume_bid_side: bidCall,
      call_volume_ask_side: askCall,
      put_volume_bid_side: bidPut,
      put_volume_ask_side: askPut,

      volume_delta_call: volumeDeltaCall,
      volume_delta_put: volumeDeltaPut,
      call_put_ratio_bid: callPutRatio,

      spoof_flag_call: spoofFlagCall,
      spoof_flag_put: spoofFlagPut,

      price_open,
      price_close,
      price_change: priceChange,
      price_direction: priceDir,

      sentiment,
      confidence_level: confidence
    };

    await insertEnhancedBidAskIntoDB(data);
  } catch (err) {
    console.error(`❌ Error processing bid/ask volume for ${ticker}:`, err.message);
  }
}

// ✅ Compute Bid Ask Volume
async function insertEnhancedBidAskIntoDB(data) {
  const client = new Client(DB_CONFIG);
  await client.connect();

  try {
    await client.query(`
      INSERT INTO bid_ask_volume_enhanced (
        symbol, date, recorded_at,
        call_volume, put_volume,
        call_volume_bid_side, call_volume_ask_side,
        put_volume_bid_side, put_volume_ask_side,
        volume_delta_call, volume_delta_put,
        call_put_ratio_bid,
        spoof_flag_call, spoof_flag_put,
        price_open, price_close, price_change, price_direction,
        sentiment, confidence_level
      ) VALUES (
        $1, $2, $3,
        $4, $5,
        $6, $7,
        $8, $9,
        $10, $11,
        $12,
        $13, $14,
        $15, $16, $17, $18,
        $19, $20
      )
      ON CONFLICT (symbol, date) DO UPDATE SET
        call_volume = EXCLUDED.call_volume,
        put_volume = EXCLUDED.put_volume,
        call_volume_bid_side = EXCLUDED.call_volume_bid_side,
        call_volume_ask_side = EXCLUDED.call_volume_ask_side,
        put_volume_bid_side = EXCLUDED.put_volume_bid_side,
        put_volume_ask_side = EXCLUDED.put_volume_ask_side,
        volume_delta_call = EXCLUDED.volume_delta_call,
        volume_delta_put = EXCLUDED.volume_delta_put,
        call_put_ratio_bid = EXCLUDED.call_put_ratio_bid,
        spoof_flag_call = EXCLUDED.spoof_flag_call,
        spoof_flag_put = EXCLUDED.spoof_flag_put,
        price_open = EXCLUDED.price_open,
        price_close = EXCLUDED.price_close,
        price_change = EXCLUDED.price_change,
        price_direction = EXCLUDED.price_direction,
        sentiment = EXCLUDED.sentiment,
        confidence_level = EXCLUDED.confidence_level,
        recorded_at = NOW();
    `, [
      data.symbol, data.date, data.recorded_at,
      data.call_volume, data.put_volume,
      data.call_volume_bid_side, data.call_volume_ask_side,
      data.put_volume_bid_side, data.put_volume_ask_side,
      data.volume_delta_call, data.volume_delta_put,
      data.call_put_ratio_bid,
      data.spoof_flag_call, data.spoof_flag_put,
      data.price_open, data.price_close, data.price_change, data.price_direction,
      data.sentiment, data.confidence_level
    ]);

    console.log(`✅ Stored enhanced BID/ASK volume for ${data.symbol}`);
  } catch (err) {
    console.error("❌ DB Insert Error:", err.message);
  } finally {
    await client.end();
  }
}

// -----------------------
// Compute and Store Functions
// -----------------------

// ✅ Compute and Store the DT Average for SPY OHLC
async function fetchAndStoreSpyOhlcAverages(client) {
  try {
    console.log("📊 Computing and inserting SPY OHLC DT Averages...");

    // Ensure client is connected
    if (!client._connected) {
      await client.connect();
    }

    const result = await client.query(`
      WITH last_18_intervals AS (
        SELECT close FROM spy_ohlc ORDER BY start_time DESC LIMIT 18
      )
      INSERT INTO spy_ohlc_averages (
        date, latest_close, avg_close, recorded_at
      )
      SELECT 
        CURRENT_DATE,
        (SELECT close FROM spy_ohlc ORDER BY start_time DESC LIMIT 1),
        COALESCE(AVG(close), 0),  -- Ensure we handle NULL values properly
        NOW()
      FROM last_18_intervals
      ON CONFLICT (date) DO UPDATE SET 
        latest_close = EXCLUDED.latest_close,
        avg_close = EXCLUDED.avg_close,
        recorded_at = NOW()
      RETURNING *; -- ✅ Debugging: See what gets inserted
    `);

    console.log("✅ SPY OHLC DT Averages inserted successfully:", result.rows);

  } catch (error) {
    console.error("❌ Error inserting SPY OHLC DT Averages:", error.message);
  }
}

async function storeDailyOhlcSummary() {
  const client = new Client({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    ssl: { rejectUnauthorized: false }
  });

  await client.connect();
  const today = dayjs().format("YYYY-MM-DD");

  // 1. Fetch Spot GEX
  const gexResponse = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/spot-exposures");
  const spot = gexResponse.data?.data?.sort((a, b) => new Date(b.time) - new Date(a.time))[0];

  // 2. Fetch IV
  const ivResponse = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/volatility/term-structure");
  const iv = ivResponse.data?.data?.find(d => d.dte === 0);

  // 3. Prepare values
  const values = [
    today,
    parseFloat(spot?.price ?? 0),
    parseFloat(spot?.gamma_per_one_percent_move_oi ?? 0),
    parseFloat(spot?.charm_per_one_percent_move_oi ?? 0),
    parseFloat(spot?.vanna_per_one_percent_move_oi ?? 0),
    parseFloat(iv?.volatility ?? 0)
  ];

  // 4. Fixed summaryQuery using window + aggregate separation
  const summaryQuery = `
    WITH base AS (
      SELECT *
      FROM spy_ohlc
      WHERE start_time::time BETWEEN '14:30:00' AND '21:00:00'
        AND start_time::date = $1
    ),
    windowed AS (
      SELECT 
        DATE(start_time) AS trade_date,
        FIRST_VALUE(open) OVER (PARTITION BY DATE(start_time) ORDER BY start_time ASC) AS open,
        FIRST_VALUE(close) OVER (PARTITION BY DATE(start_time) ORDER BY start_time DESC) AS close,
        high,
        low,
        volume
      FROM base
    ),
    summary AS (
      SELECT
        trade_date,
        MAX(high) AS high,
        MIN(low) AS low,
        MAX(open) AS open,
        MAX(close) AS close,
        SUM(volume) AS total_volume
      FROM windowed
      GROUP BY trade_date
    )
    INSERT INTO spy_ohlc_summary (
      trade_date, open, high, low, close, total_volume,
      spot_price, spot_gamma_oi, spot_charm_oi, spot_vanna_oi, implied_volatility,
      updated_at
    )
    SELECT 
      s.trade_date, s.open, s.high, s.low, s.close, s.total_volume,
      $2, $3, $4, $5, $6,
      now()
    FROM summary s
    ON CONFLICT (trade_date) DO UPDATE
    SET open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        total_volume = EXCLUDED.total_volume,
        spot_price = EXCLUDED.spot_price,
        spot_gamma_oi = EXCLUDED.spot_gamma_oi,
        spot_charm_oi = EXCLUDED.spot_charm_oi,
        spot_vanna_oi = EXCLUDED.spot_vanna_oi,
        implied_volatility = EXCLUDED.implied_volatility,
        updated_at = now();
  `;

  await client.query(summaryQuery, values);

  // 5. Version snapshot (same structure)
  const versionQuery = `
    WITH base AS (
      SELECT *
      FROM spy_ohlc
      WHERE start_time::time BETWEEN '14:30:00' AND '21:00:00'
        AND start_time::date = $1
    ),
    windowed AS (
      SELECT 
        DATE(start_time) AS trade_date,
        FIRST_VALUE(open) OVER (PARTITION BY DATE(start_time) ORDER BY start_time ASC) AS open,
        FIRST_VALUE(close) OVER (PARTITION BY DATE(start_time) ORDER BY start_time DESC) AS close,
        high,
        low,
        volume
      FROM base
    ),
    summary AS (
      SELECT
        trade_date,
        MAX(high) AS high,
        MIN(low) AS low,
        MAX(open) AS open,
        MAX(close) AS close,
        SUM(volume) AS total_volume
      FROM windowed
      GROUP BY trade_date
    )
    INSERT INTO spy_ohlc_summary_versions (
      trade_date, open, high, low, close, total_volume,
      spot_price, spot_gamma_oi, spot_charm_oi, spot_vanna_oi, implied_volatility
    )
    SELECT 
      s.trade_date, s.open, s.high, s.low, s.close, s.total_volume,
      $2, $3, $4, $5, $6
    FROM summary s;
  `;

  await client.query(versionQuery, values);

  await client.end();
  console.log("✅ Daily OHLC summary updated + version snapshot stored.");
}

// ✅ Compute Market Tide Deltas
function computeMarketTideDeltas(data) {
  return data.map((entry, index) => {
    if (index === 0) {
      return {
        timestamp: entry.timestamp,
        delta_call: 0,
        delta_put: 0,
        delta_volume: 0,
        sentiment: 'Neutral'
      };
    }

    const prev = data[index - 1];

    const delta_call = entry.net_call_premium - prev.net_call_premium;
    const delta_put = entry.net_put_premium - prev.net_put_premium;
    const delta_volume = entry.net_volume - prev.net_volume;

    let sentiment = 'Neutral';
    if (delta_put < 0 && entry.net_put_premium < 0) sentiment = 'Bullish Trend';
    else if (delta_call < 0 && entry.net_call_premium < 0) sentiment = 'Bearish Trend';

    return {
      timestamp: entry.timestamp,
      delta_call,
      delta_put,
      delta_volume,
      sentiment
    };
  });
}

// -----------------------
// Main function to fetch and store all SPY datasets
// -----------------------
async function main() {
  console.log("🚀 Fetching all datasets...");

  try {
    console.log("📢 Calling fetchSpyIV0DTE...");
    const spyIV0DTE = await fetchSpyIV0DTE();
    console.log("✅ SPY IV 0 DTE Data Fetched:", spyIV0DTE);

    // Fetch all datasets in parallel with error handling
    const [
      ohlcData,
      spotGexData,
      greeksByStrikeData,
      optionPriceLevelsData,
      marketTideData,
      bidAskSpy,
      bidAskSpx,
      bidAskQqq,
      bidAskNdx,
      greekSpy,
      greekSpx,
    ] = await Promise.all([
      fetchSpyOhlcData().catch(error => {
        console.error("❌ OHLC Fetch Error:", error.message);
        return [];
      }),
      fetchSpySpotGex().catch(error => {
        console.error("❌ Spot GEX Fetch Error:", error.message);
        return [];
      }),
      fetchSpyGreeksByStrike().catch(error => {
        console.error("❌ Greeks Fetch Error:", error.message);
        return [];
      }),
      fetchSpyOptionPriceLevels().catch(error => {
        console.error("❌ Option Price Levels Fetch Error:", error.message);
        return [];
      }),
      fetchMarketTideData().catch(error => {
        console.error("❌ Market Tide Fetch Error:", error.message);
        return [];
      }),
      fetchBidAskVolumeData("SPY").catch(error => {
        console.error("❌ Bid/Ask SPY Fetch Error:", error.message);
        return [];
      }),
      fetchBidAskVolumeData("SPX").catch(error => {
        console.error("❌ Bid/Ask SPX Fetch Error:", error.message);
        return [];
      }),
      fetchBidAskVolumeData("QQQ").catch(error => {
        console.error("❌ Bid/Ask QQQ Fetch Error:", error.message);
        return [];
      }),
      fetchBidAskVolumeData("NDX").catch(error => {
        console.error("❌ Bid/Ask NDX Fetch Error:", error.message);
        return [];
      }),
      fetchGreekExposure("SPY").catch(error => {
        console.error("❌ Greek SPY Fetch Error:", error.message);
        return [];
      }),
      fetchGreekExposure("SPX").catch(error => {
        console.error("❌ Greek SPX Fetch Error:", error.message);
        return [];
      }),
    ]);

    // ✅ Debugging: Check if market tide & bid ask data exist
    console.log("📌 Market Tide Data Before Storing:", marketTideData);
    console.log("📌 Bid/Ask SPY Data Before Storing:", bidAskSpy);

    // Store all datasets in parallel
    await Promise.all([
      ohlcData?.length > 0 ? storeSpyOhlcDataInDB(ohlcData) : null,
      spotGexData?.length > 0 ? storeSpySpotGexInDB(spotGexData) : null,
      optionPriceLevelsData?.length > 0 ? storeSpyOptionPriceLevelsInDB(optionPriceLevelsData) : null,
      greeksByStrikeData?.length > 0 ? storeSpyGreeksByStrikeInDB(greeksByStrikeData) : null,
      spyIV0DTE?.length > 0 ? storeSpyIV0DTEDataInDB(spyIV0DTE) : null,
      greekSpy?.length > 0 ? storeGreekExposureInDB(greekSpy) : null,
      greekSpx?.length > 0 ? storeGreekExposureInDB(greekSpx) : null,
      marketTideData?.length > 0 ? storeMarketTideDataInDB(marketTideData) : null,
      bidAskSpy?.length > 0 ? storeBidAskVolumeDataInDB(bidAskSpy) : null,
      bidAskSpx?.length > 0 ? storeBidAskVolumeDataInDB(bidAskSpx) : null,
      bidAskQqq?.length > 0 ? storeBidAskVolumeDataInDB(bidAskQqq) : null,
      bidAskNdx?.length > 0 ? storeBidAskVolumeDataInDB(bidAskNdx) : null
    ]);

    // ✅ NEW: Compute and store bid-side shift signals
    if (bidAskSpy?.length > 0) {
      await storeBidShiftSignals(bidAskSpy);
    }

    // Compute Delta Trends for Today
    const spyPriceOpen = 523.12;  // Replace with OHLC open
    const spyPriceClose = 525.65; // Replace with OHLC close
    const spxPriceOpen = 5263.44;
    const spxPriceClose = 5280.01;
    const qqqPriceOpen = 438.20;
    const qqqPriceClose = 440.70;
    const ndxPriceOpen = 18350.00;
    const ndxPriceClose = 18560.00;

    await fetchAndStoreEnhancedBidAsk("SPY", spyPriceOpen, spyPriceClose);
    await fetchAndStoreEnhancedBidAsk("SPX", spxPriceOpen, spxPriceClose);
    await fetchAndStoreEnhancedBidAsk("QQQ", qqqPriceOpen, qqqPriceClose);
    await fetchAndStoreEnhancedBidAsk("NDX", ndxPriceOpen, ndxPriceClose);

    // ✅ Compute Delta Trends for Today
    const todayMarketTideData = await fetchTodayMarketTideDataFromDB();
    if (todayMarketTideData.length > 1) {
      const deltas = computeMarketTideDeltas(todayMarketTideData);
      await storeMarketTideDeltasInDB(deltas);
    }

    // ✅ EOD summary + snapshot
    console.log("📦 Running daily OHLC summary + version snapshot...");
    await storeDailyOhlcSummary();

    console.log("✅ All data fetch, storage, and summary operations completed successfully.");

  } catch (error) {
    console.error("❌ Error in main function:", error.message);
  }
}

// Helper delay function
function pause(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ✅ Run main only if explicitly called
if (require.main === module) {
  main();
}