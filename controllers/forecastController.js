const {
  classifyDeltaTrend,
  gammaRegimeAlert,
  checkDealerZoneBreach
} = require('../utils/forecastingUtils');

const {
  getDeltaTrends,
  getGammaData,
  getOptionVolumeZones,
  getOhlcPrice
} = require('../services/dataFetcher');

const db = require('../services/db');

function getCurrentBucket() {
  const now = new Date();
  now.setSeconds(0, 0);
  const minutes = Math.floor(now.getMinutes() / 5) * 5;
  now.setMinutes(minutes);
  return now.toISOString();
}

exports.getForecast = async (req, res) => {
  try {
    const currentBucket = getCurrentBucket();
    const delta = await getDeltaTrends();
    const gamma = await getGammaData();
    const optionLevels = await getOptionVolumeZones();
    const currentPrice = await getOhlcPrice();

    const dealerZones = optionLevels
      .filter(z => z.call_volume > 5000 || z.put_volume > 5000)
      .map(z => ({
        price: z.price,
        call_volume: z.call_volume,
        put_volume: z.put_volume
      }));

    const sentiment = classifyDeltaTrend(delta.delta_call, delta.delta_put, delta.delta_volume);
    const gammaRisk = gammaRegimeAlert(gamma.gamma_oi);
    const zoneAlert = checkDealerZoneBreach(currentPrice, dealerZones);

    await db.query(`
      INSERT INTO spy_forecast_snapshots
      (forecast_time, bucket, sentiment, gamma_risk, zone_alert, delta_call, delta_put, delta_volume, gamma_oi, current_price)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `, [
      new Date(), currentBucket, sentiment, gammaRisk, zoneAlert,
      delta.delta_call, delta.delta_put, delta.delta_volume,
      gamma.gamma_oi, currentPrice
    ]);

    res.json({
      bucket: currentBucket,
      sentiment,
      gammaRisk,
      zoneAlert: zoneAlert || 'No breach detected'
    });

  } catch (err) {
    console.error('Forecast error:', err.message);
    res.status(500).json({ error: 'Forecast failed', details: err.message });
  }
};
