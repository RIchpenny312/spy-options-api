// utils/time.js

const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");
const moment = require('moment-timezone');

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
 * Normalize data to specific time buckets.
 * @param {Array} data - Array of data objects with timestamps.
 * @param {string} interval - Time interval (e.g., 'minute', 'hour', 'day').
 * @param {string} timezone - Target timezone (e.g., 'America/Chicago').
 * @param {string} startTime - Start time for the buckets (ISO string).
 * @param {string} endTime - End time for the buckets (ISO string).
 * @returns {Array} - Array of data aligned to time buckets.
 */
function normalizeDataToBuckets(data, interval, timezone, startTime, endTime) {
  // Validate inputs
  validateInputs(data, interval, timezone, startTime, endTime);

  // Create time buckets
  const buckets = createTimeBuckets(interval, timezone, startTime, endTime);

  // Aggregate data into buckets
  const aggregatedData = aggregateData(data, interval, timezone);

  // Map aggregated data to buckets
  return mapDataToBuckets(aggregatedData, buckets, timezone, interval);
}

/**
 * Validate inputs for the normalization process.
 * @param {Array} data - Array of data objects with timestamps.
 * @param {string} interval - Time interval (e.g., 'minute', 'hour', 'day').
 * @param {string} timezone - Target timezone (e.g., 'America/Chicago').
 * @param {string} startTime - Start time for the buckets (ISO string).
 * @param {string} endTime - End time for the buckets (ISO string).
 */
function validateInputs(data, interval, timezone, startTime, endTime) {
  if (!Array.isArray(data)) throw new Error('Data must be an array');
  if (!['minute', 'hour', 'day'].includes(interval)) throw new Error('Invalid interval');
  if (!moment.tz.zone(timezone)) throw new Error('Invalid timezone');
  if (!moment(startTime).isValid() || !moment(endTime).isValid()) throw new Error('Invalid start or end time');
}

/**
 * Create time buckets between a start and end time.
 * @param {string} interval - Time interval (e.g., 'minute', 'hour', 'day').
 * @param {string} timezone - Target timezone (e.g., 'America/Chicago').
 * @param {string} startTime - Start time for the buckets (ISO string).
 * @param {string} endTime - End time for the buckets (ISO string).
 * @returns {Array} - Array of bucket objects with timestamps.
 */
function createTimeBuckets(interval, timezone, startTime, endTime) {
  const buckets = [];
  const start = moment.tz(startTime, timezone).startOf(interval);
  const end = moment.tz(endTime, timezone).startOf(interval);

  while (start <= end) {
    buckets.push({ timestamp: start.toISOString() });
    start.add(1, interval);
  }

  return buckets;
}

/**
 * Aggregate data into buckets by averaging values for duplicate timestamps.
 * @param {Array} data - Array of data objects with timestamps.
 * @param {string} interval - Time interval (e.g., 'minute', 'hour', 'day').
 * @param {string} timezone - Target timezone (e.g., 'America/Chicago').
 * @returns {Array} - Array of aggregated data objects.
 */
function aggregateData(data, interval, timezone) {
  const aggregated = {};

  data.forEach((entry) => {
    const bucket = moment.tz(entry.timestamp, timezone).startOf(interval).toISOString();
    if (!aggregated[bucket]) {
      aggregated[bucket] = { ...entry, count: 1 };
    } else {
      aggregated[bucket].value = (aggregated[bucket].value * aggregated[bucket].count + entry.value) /
        (aggregated[bucket].count + 1);
      aggregated[bucket].count += 1;
    }
  });

  return Object.values(aggregated);
}

/**
 * Map aggregated data to time buckets, filling missing buckets with null.
 * @param {Array} data - Array of aggregated data objects.
 * @param {Array} buckets - Array of bucket objects with timestamps.
 * @param {string} timezone - Target timezone (e.g., 'America/Chicago').
 * @param {string} interval - Time interval (e.g., 'minute', 'hour', 'day').
 * @returns {Array} - Array of data aligned to time buckets.
 */
function mapDataToBuckets(data, buckets, timezone, interval) {
  const bucketMap = new Map();

  data.forEach((entry) => {
    const bucket = moment.tz(entry.timestamp, timezone).startOf(interval).toISOString();
    bucketMap.set(bucket, entry);
  });

  return buckets.map((bucket) => ({
    timestamp: bucket.timestamp,
    data: bucketMap.get(bucket.timestamp) || null,
  }));
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

module.exports = { normalizeToBucket, normalizeDataToBuckets, getTimeContext };

