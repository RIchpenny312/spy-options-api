const axios = require('axios');
const API_BASE = 'https://spy-options-api.onrender.com/api/spy';

async function getDeltaTrends() {
  const res = await axios.get(`${API_BASE}/market-tide-deltas-today`);
  return res.data.at(-1);
}

async function getGammaData() {
  const res = await axios.get(`${API_BASE}/spot-gex`);
  return res.data;
}

async function getOptionVolumeZones() {
  const res = await axios.get(`${API_BASE}/option-price-levels/today`);
  return res.data;
}

async function getOhlcPrice() {
  const res = await axios.get(`${API_BASE}/intraday-summary`);
  return res.data.ohlc_summary.close;
}

module.exports = {
  getDeltaTrends,
  getGammaData,
  getOptionVolumeZones,
  getOhlcPrice
};