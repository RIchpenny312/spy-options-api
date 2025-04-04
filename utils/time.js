const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
const timezone = require("dayjs/plugin/timezone");

dayjs.extend(utc);
dayjs.extend(timezone);

function getTimeContextET() {
  const nowET = dayjs().tz("America/New_York");

  return {
    iso: nowET.toISOString(),
    date: nowET.format("YYYY-MM-DD"),
    time: nowET.format("HH:mm:ss"),
    weekday: nowET.format("dddd"),
    hour: nowET.hour(),
    minute: nowET.minute(),
    isMarketOpen: nowET.hour() >= 9 && nowET.hour() < 16,
    marketSession:
      nowET.hour() < 9 ? "pre-market"
      : nowET.hour() < 16 ? "regular"
      : "post-market"
  };
}

module.exports = { getTimeContextET };
