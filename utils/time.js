// utils/time.js

const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");

dayjs.extend(utc);
dayjs.extend(timezone);

const DEFAULT_TIMEZONE = "America/Chicago"; // Default to Central Time (CT)

/**
 * Normalize a timestamp to the nearest bucket (e.g., 5-minute intervals).
 * @param {string|Date} timestamp - The timestamp to normalize.
 * @param {number} bucketSize - The bucket size in minutes (default: 5).
 * @param {string} timezone - The timezone for normalization (default: CT).
 * @returns {string} - The normalized bucket time as an ISO string.
 */
function normalizeToBucket(timestamp, bucketSize = 5, timezone = DEFAULT_TIMEZONE) {
  const time = dayjs(timestamp).tz(timezone);
  const minutes = Math.floor(time.minute() / bucketSize) * bucketSize; // Round down to the nearest bucket
  return time.minute(minutes).second(0).millisecond(0).toISOString(); // Return as ISO string
}

/**
 * Get the current time context for a specific timezone.
 * @param {string} timezone - The timezone for the time context (default: ET).
 * @param {object} marketHours - Market hours configuration (default: 9:00 AM to 4:00 PM ET).
 * @returns {object} - An object containing the time context.
 */
function getTimeContext(timezone = "America/New_York", marketHours = { open: 9, close: 16 }) {
  const now = dayjs().tz(timezone);

  return {
    iso: now.toISOString(),
    date: now.format("YYYY-MM-DD"),
    time: now.format("HH:mm:ss"),
    weekday: now.format("dddd"),
    hour: now.hour(),
    minute: now.minute(),
    isMarketOpen: now.hour() >= marketHours.open && now.hour() < marketHours.close,
    marketSession:
      now.hour() < marketHours.open ? "pre-market"
      : now.hour() < marketHours.close ? "regular"
      : "post-market"
  };
}

module.exports = { normalizeToBucket, getTimeContext };

