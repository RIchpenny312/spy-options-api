require('dotenv').config();
const axios = require('axios');
const { Client } = require('pg');
const dayjs = require('dayjs');

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
        headers: { Authorization: `Bearer ${API_KEY}` }
      });
      return response;
    } catch (error) {
      if (error.response && error.response.status === 429 && attempt < retries) {
        console.warn(`⚠️ API Rate Limit Exceeded (429). Retrying in ${delay / 1000} seconds... [Attempt ${attempt}/${retries}]`);
        await new Promise(res => setTimeout(res, delay));
      } else {
        throw error;
      }
    }
  }
}

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
    console.error('❌ Error fetching OHLC data:', error.message);
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
    console.log("🔍 Fetching Option Price Levels...");
    const response = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/option/stock-price-levels");
    if (!response.data?.data || !Array.isArray(response.data.data)) {
      throw new Error("Invalid Option Price Levels response format");
    }
    const top10Active = response.data.data
      .sort((a, b) => (parseInt(b.call_volume) + parseInt(b.put_volume)) - (parseInt(a.call_volume) + parseInt(a.put_volume)))
      .slice(0, 10);
    console.log("📊 Processed Top 10 SPY Option Price Levels:", top10Active);
    return top10Active.map(item => ({
      price: parseFloat(item.price) || 0,
      call_volume: parseInt(item.call_volume) || 0,
      put_volume: parseInt(item.put_volume) || 0,
      total_volume: (parseInt(item.call_volume) || 0) + (parseInt(item.put_volume) || 0),
      time: item.time || null
    }));
  } catch (error) {
    console.error('❌ Error fetching SPY Option Price Levels:', error.message);
    return [];
  }
}

// ✅ Function to fetch Market Tide Data
async function fetchMarketTideData() {
    try {
        console.log("🔍 Fetching Market Tide Data...");
        const response = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/market-tide");

        if (!response.data?.data) {
            throw new Error("Invalid Market Tide response format");
        }

        const marketTide = response.data.data;

        return [{
            gamma_tide: parseFloat(marketTide.gamma_tide) || 0,
            vanna_tide: parseFloat(marketTide.vanna_tide) || 0,
            charm_tide: parseFloat(marketTide.charm_tide) || 0,
            total_gamma: parseFloat(marketTide.total_gamma) || 0,
            total_vanna: parseFloat(marketTide.total_vanna) || 0,
            total_charm: parseFloat(marketTide.total_charm) || 0,
            price: parseFloat(marketTide.price) || 0,
            time: marketTide.time || null
        }];
    } catch (error) {
        if (error.response && error.response.status === 404) {
            console.warn("⚠️ Market Tide API returned 404. Skipping this dataset.");
            return []; // Return an empty array instead of failing
        }
        console.error('❌ Error fetching Market Tide Data:', error.message);
        return [];
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

// ✅ Function to fetch SPY IV for 5 DTE
async function fetchSpyIV() {
    try {
        console.log("🔍 Fetching SPY Implied Volatility for 5 DTE...");
        const response = await fetchWithRetry("https://api.unusualwhales.com/api/stock/SPY/volatility/term-structure");

        if (!response.data?.data || !Array.isArray(response.data.data)) {
            throw new Error("Invalid SPY IV response format");
        }

        // Filter only the entry with `dte = 5`
        const ivData = response.data.data.find(item => item.dte === 5);

        if (!ivData) {
            console.warn("⚠️ No IV data found for DTE = 5");
            return [];
        }

        return [{
            ticker: ivData.ticker || "SPY",
            date: ivData.date || null,
            expiry: ivData.expiry || null,
            dte: ivData.dte || 5,
            implied_move: parseFloat(ivData.implied_move) || 0,
            implied_move_perc: parseFloat(ivData.implied_move_perc) || 0,
            volatility: parseFloat(ivData.volatility) || 0
        }];
    } catch (error) {
        console.error("❌ Error fetching SPY IV data:", error.message);
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
  const client = new Client(DB_CONFIG);
  await client.connect();
  try {
    for (const item of data) {
      const checkQuery = `SELECT COUNT(*) FROM spy_ohlc WHERE start_time = $1`;
      const result = await client.query(checkQuery, [item.start_time]);
      if (parseInt(result.rows[0].count) > 0) {
        console.log(`⚠️ OHLC data for ${item.start_time} already exists. Skipping.`);
        continue;
      }
      await client.query(
        `INSERT INTO spy_ohlc (open, high, low, close, total_volume, volume, start_time, end_time, recorded_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())`,
        [item.open, item.high, item.low, item.close, item.total_volume, item.volume, item.start_time, item.end_time]
      );
    }
    console.log('✅ SPY OHLC Data inserted successfully');
  } catch (error) {
    console.error('❌ Error inserting OHLC data:', error.message);
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
      console.log("📊 Inserting Option Price Level:", JSON.stringify(item, null, 2));
      
      const timeValue = item.time ? item.time : new Date().toISOString(); // Handle null time values

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
            recorded_at = NOW();`,
        [
          parseFloat(item.price) || 0,
          parseInt(item.call_volume) || 0,
          parseInt(item.put_volume) || 0,
          (parseInt(item.call_volume) || 0) + (parseInt(item.put_volume) || 0),
          timeValue
        ]
      );
    }
    console.log('✅ SPY Option Price Levels Data inserted successfully');
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

// ✅ Function to store Market Tide Data in DB
async function storeMarketTideDataInDB(data) {
  const client = new Client(DB_CONFIG);
  await client.connect();
  try {
    for (const item of data) {
      console.log("📊 Inserting Market Tide Data:", JSON.stringify(item, null, 2));
      
      await client.query(
        `INSERT INTO spy_market_tide (
            gamma_tide, vanna_tide, charm_tide, 
            total_gamma, total_vanna, total_charm, 
            price, time, recorded_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, NOW()
        )
        ON CONFLICT (time) 
        DO UPDATE SET 
            gamma_tide = EXCLUDED.gamma_tide,
            vanna_tide = EXCLUDED.vanna_tide,
            charm_tide = EXCLUDED.charm_tide,
            total_gamma = EXCLUDED.total_gamma,
            total_vanna = EXCLUDED.total_vanna,
            total_charm = EXCLUDED.total_charm,
            price = EXCLUDED.price,
            recorded_at = NOW();`,
        [
          item.gamma_tide, item.vanna_tide, item.charm_tide, 
          item.total_gamma, item.total_vanna, item.total_charm, 
          item.price, item.time
        ]
      );
    }
    console.log('✅ Market Tide Data inserted successfully');
  } catch (error) {
    console.error('❌ Error inserting Market Tide Data:', error.message);
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

// ✅ Function to store SPY IV Data (5 DTE) in DB
async function storeSpyIVDataInDB(data) {
  const client = new Client(DB_CONFIG);
  await client.connect();
  try {
    for (const item of data) {
      console.log("📊 Inserting SPY IV (5 DTE):", JSON.stringify(item, null, 2));

      await client.query(
        `INSERT INTO spy_iv_5dte (
            symbol, date, expiry, dte, implied_move, implied_move_perc, volatility, recorded_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, NOW()
        )
        ON CONFLICT (symbol, date, dte)
        DO UPDATE SET 
            implied_move = EXCLUDED.implied_move,
            implied_move_perc = EXCLUDED.implied_move_perc,
            volatility = EXCLUDED.volatility,
            recorded_at = NOW();`,
        [
          item.ticker || "SPY", item.date, item.expiry, item.dte,
          item.implied_move, item.implied_move_perc, item.volatility
        ]
      );
    }
    console.log('✅ SPY IV (5 DTE) Data inserted successfully');
  } catch (error) {
    console.error('❌ Error inserting SPY IV (5 DTE) Data:', error.message);
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
// Main function to fetch and store all SPY datasets
// -----------------------
async function main() {
    console.log("🚀 Fetching all datasets...");

    // ✅ Fetch all datasets in parallel, including SPY & SPX Greek Exposure
    const [
        ohlcData, spotGexData, greeksByStrikeData, optionPriceLevelsData, marketTideData,
        bidAskSpy, bidAskSpx, bidAskQqq, bidAskNdx, spyIV5DTE,
        greekSpy, greekSpx // Fetch SPY & SPX Greek Exposure
    ] = await Promise.all([
        fetchSpyOhlcData(),
        fetchSpySpotGex(),
        fetchSpyGreeksByStrike(),
        fetchSpyOptionPriceLevels(),
        fetchMarketTideData(),
        fetchBidAskVolumeData("SPY"),
        fetchBidAskVolumeData("SPX"),
        fetchBidAskVolumeData("QQQ"),
        fetchBidAskVolumeData("NDX"),
        fetchSpyIV(), // Fetch SPY IV (5 DTE)
        fetchGreekExposure("SPY"), // Fetch SPY Greek Exposure
        fetchGreekExposure("SPX")  // Fetch SPX Greek Exposure
    ]);

    // ✅ Store all datasets in parallel, including SPY & SPX Greek Exposure
    await Promise.all([
        ohlcData.length > 0 ? storeSpyOhlcDataInDB(ohlcData) : null,
        spotGexData.length > 0 ? storeSpySpotGexInDB(spotGexData) : null,
        optionPriceLevelsData.length > 0 ? storeSpyOptionPriceLevelsInDB(optionPriceLevelsData) : null,
        greeksByStrikeData.length > 0 ? storeSpyGreeksByStrikeInDB(greeksByStrikeData) : null,
        marketTideData.length > 0 ? storeMarketTideDataInDB(marketTideData) : null,
        bidAskSpy.length > 0 ? storeBidAskVolumeDataInDB(bidAskSpy) : null,
        bidAskSpx.length > 0 ? storeBidAskVolumeDataInDB(bidAskSpx) : null,
        bidAskQqq.length > 0 ? storeBidAskVolumeDataInDB(bidAskQqq) : null,
        bidAskNdx.length > 0 ? storeBidAskVolumeDataInDB(bidAskNdx) : null,
        spyIV5DTE.length > 0 ? storeSpyIVDataInDB(spyIV5DTE) : null,
        greekSpy.length > 0 ? storeGreekExposureInDB(greekSpy) : null,  // Store SPY Greek Exposure
        greekSpx.length > 0 ? storeGreekExposureInDB(greekSpx) : null   // Store SPX Greek Exposure
    ]);

    console.log("✅ All data fetch and storage operations completed successfully.");
}

main();