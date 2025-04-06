function classifyDeltaTrend(deltaCall, deltaPut, deltaVolume) {
  if (deltaCall < 0 && deltaPut > 0) return 'Bearish Trend';
  if (deltaCall > 0 && deltaPut < 0) return 'Bullish Trend';
  return 'Neutral';
}

function gammaRegimeAlert(gammaValue) {
  return gammaValue < 0 ? 'Short Gamma – High Volatility Risk' : 'Long Gamma – Stability Expected';
}

function checkDealerZoneBreach(currentPrice, zones) {
  let alerts = [];
  zones.forEach(zone => {
    const isNear = Math.abs(currentPrice - zone.price) <= 0.20;
    if (isNear) {
      if (zone.put_volume > 50000) alerts.push(`Testing Dealer Put Zone at ${zone.price}`);
      if (zone.call_volume > 50000) alerts.push(`Testing Dealer Call Zone at ${zone.price}`);
    }
  });
  return alerts.length > 0 ? alerts.join(' | ') : null;
}

module.exports = {
  classifyDeltaTrend,
  gammaRegimeAlert,
  checkDealerZoneBreach
};
