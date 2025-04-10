const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");
dayjs.extend(utc);
dayjs.extend(timezone);

const { classifyDeltaTrend, suggestLabelExplanation } = require('./deltaTrendClassifier');

const TIMEZONE = process.env.TIMEZONE || "America/Chicago";

// Normalize timestamp to 5-minute Chicago-local bucket
function normalizeToBucket(timestampUtc) {
  if (!timestampUtc) {
    throw new Error("Invalid timestamp provided for bucketing");
  }
  const local = dayjs.utc(timestampUtc).tz(TIMEZONE);
  const floored = Math.floor(local.minute() / 5) * 5;
  return local.minute(floored).second(0).millisecond(0).toISOString();
}

function safeNumber(val) {
  const parsed = parseFloat(val);
  return isNaN(parsed) ? 0 : parsed;
}

function calculateSafeDeltas(latest, previous) {
  return {
    delta_call: safeNumber(latest.net_call_premium) - safeNumber(previous.net_call_premium),
    delta_put: safeNumber(latest.net_put_premium) - safeNumber(previous.net_put_premium),
    delta_volume: safeNumber(latest.net_volume) - safeNumber(previous.net_volume),
    previous_call: safeNumber(previous.net_call_premium),
    previous_put: safeNumber(previous.net_put_premium),
    previous_volume: safeNumber(previous.net_volume),
  };
}

function getDeltaChangeRates(call, put, vol, prevCall = 0, prevPut = 0, prevVol = 0) {
  const pctChange = (curr, prev) => (prev === 0 ? 0 : ((curr - prev) / Math.abs(prev)) * 100);
  return {
    delta_call_pct_change: pctChange(call, prevCall),
    delta_put_pct_change: pctChange(put, prevPut),
    delta_volume_pct_change: pctChange(vol, prevVol),
  };
}

function correlateWithMarketTide(net_call_premium, net_put_premium, delta_call, delta_put) {
  if (delta_call > delta_put && net_call_premium > net_put_premium) {
    return "Bullish Alignment";
  }
  if (delta_put > delta_call && net_put_premium > net_call_premium) {
    return "Bearish Alignment";
  }
  return "Neutral Alignment";
}

async function processAndInsertDeltaTrend(client, inputDate = null) {
  try {
    const now = inputDate ? dayjs.tz(inputDate, TIMEZONE) : dayjs().tz(TIMEZONE);
    const tradingDay = now.format("YYYY-MM-DD");

    const result = await client.query(`
      SELECT timestamp, net_call_premium, net_put_premium, net_volume
      FROM market_tide_data
      WHERE timestamp::date = $1
      ORDER BY timestamp ASC;
    `, [tradingDay]);

    if (!result.rows || result.rows.length < 2) {
      console.warn(`⚠️ Not enough market tide entries for ${tradingDay} (found ${result.rows.length})`);
      return;
    }

    const previous = result.rows[result.rows.length - 2];
    const latest = result.rows[result.rows.length - 1];

    if (!latest.timestamp) {
      console.error("❌ Latest tide record is missing a valid timestamp. Skipping delta processing.");
      return;
    }

    const {
      delta_call,
      delta_put,
      delta_volume,
      previous_call,
      previous_put,
      previous_volume,
    } = calculateSafeDeltas(latest, previous);

    const {
      delta_call_pct_change = 0,
      delta_put_pct_change = 0,
      delta_volume_pct_change = 0,
    } = getDeltaChangeRates(delta_call, delta_put, delta_volume, previous_call, previous_put, previous_volume);

    const bucketTimeIso = normalizeToBucket(latest.timestamp);
    const bucketTime = dayjs(bucketTimeIso).tz(TIMEZONE).toDate();
    const bucketTimeFormatted = dayjs(bucketTimeIso).format("YYYY-MM-DD HH:mm:ss");

    console.log(`🕒 Normalized Bucket Time: ${bucketTimeFormatted}`);

    // Refined sentiment logic
    const sentiment = Math.abs(delta_call - delta_put) < 500_000
      ? "Neutral"
      : delta_call > delta_put
      ? "Bullish"
      : "Bearish";

    // New classifier logic
    const flow_direction = classifyDeltaTrend(
      delta_call,
      delta_put,
      delta_call_pct_change,
      delta_put_pct_change
    );

    const alignment_label = correlateWithMarketTide(
      safeNumber(latest.net_call_premium),
      safeNumber(latest.net_put_premium),
      delta_call,
      delta_put
    );

    const ai_explanation = suggestLabelExplanation(flow_direction, alignment_label);

    const recordedAt = dayjs().toISOString();

    console.log("📊 Final Delta Payload:", {
      delta_call,
      delta_put,
      delta_volume,
      delta_call_pct_change,
      delta_put_pct_change,
      delta_volume_pct_change,
      flow_direction,
      alignment_label,
      ai_explanation,
    });

    // Optional: Add GPT-friendly debug log
    console.log(`[GPT] ${bucketTimeFormatted} | ${flow_direction} | ${alignment_label} → ${ai_explanation}`);

    // Optional: Add enhanced GPT-friendly debug log
    console.log(`[AI_LABEL]
      🕒 ${bucketTimeFormatted}
      Flow: ${flow_direction}
      Alignment: ${alignment_label}
      Explanation: ${ai_explanation}
    [/AI_LABEL]`);

    await client.query(`
      INSERT INTO market_tide_deltas (
        timestamp,
        bucket_time,
        delta_call,
        delta_put,
        delta_volume,
        sentiment,
        delta_call_pct_change,
        delta_put_pct_change,
        delta_volume_pct_change,
        flow_direction,
        alignment_label,
        ai_explanation,
        recorded_at
      )
      VALUES (
        $1,
        $2,
        $3, $4, $5, $6,
        $7, $8, $9,
        $10, $11, $12,
        $13
      )
      ON CONFLICT (bucket_time) DO UPDATE
      SET
        delta_call               = $3,
        delta_put                = $4,
        delta_volume             = $5,
        sentiment                = $6,
        delta_call_pct_change    = $7,
        delta_put_pct_change     = $8,
        delta_volume_pct_change  = $9,
        flow_direction           = $10,
        alignment_label          = $11,
        ai_explanation           = $12,
        recorded_at              = $13
    `, [
      latest.timestamp,
      bucketTime,
      delta_call,
      delta_put,
      delta_volume,
      sentiment,
      delta_call_pct_change,
      delta_put_pct_change,
      delta_volume_pct_change,
      flow_direction,
      alignment_label,
      ai_explanation,
      recordedAt,
    ]);

    console.log(`[Delta] ${bucketTimeIso} (${tradingDay}) → call: ${delta_call}, put: ${delta_put}, vol: ${delta_volume}, sentiment: ${sentiment}`);
    console.log(`[Pct] Δcall: ${delta_call_pct_change.toFixed(2)}%, Δput: ${delta_put_pct_change.toFixed(2)}%, Δvol: ${delta_volume_pct_change.toFixed(2)}%`);
    console.log("✅ Inserted delta trend with signals.");
  } catch (err) {
    console.error("❌ Error during delta trend processing:", err);
  }
}

module.exports = {
  processAndInsertDeltaTrend,
};
