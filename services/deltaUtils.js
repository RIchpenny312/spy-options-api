const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");
dayjs.extend(utc);
dayjs.extend(timezone);

const TIMEZONE = process.env.TIMEZONE || "America/Chicago";

function normalizeToBucket(timestampUtc) {
  if (!timestampUtc) throw new Error("Invalid timestamp provided for bucketing");
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

function detectPutClosingBounce({ delta_put, delta_put_pct_change, net_put_premium, delta_volume }) {
  return (
    net_put_premium < 0 &&
    delta_put < 0 &&
    delta_put_pct_change > -15 &&
    delta_volume >= 0
  );
}

function detectCallClosingBearish({ delta_call, delta_call_pct_change, net_call_premium, delta_volume }) {
  return (
    net_call_premium < 0 &&
    delta_call < 0 &&
    delta_call_pct_change > -15 &&
    delta_volume <= 0
  );
}

function evaluateSignalStrength(signalType, delta_pct_change, delta_volume) {
  if (signalType === "bounce") {
    return delta_pct_change > 0 && delta_volume > 0 ? "Strong Bounce Risk" : "Moderate Bounce Risk";
  }
  if (signalType === "bearish") {
    return delta_pct_change > 0 && delta_volume < 0 ? "Strong Bearish Signal" : "Moderate Bearish Signal";
  }
  return null;
}

module.exports = {
  normalizeToBucket,
  safeNumber,
  calculateSafeDeltas,
  getDeltaChangeRates,
  detectPutClosingBounce,
  detectCallClosingBearish,
  evaluateSignalStrength,
};
