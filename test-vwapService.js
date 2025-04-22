// Basic test for vwapService.js (calculateVwap)
const { calculateVwap } = require('./services/vwapService');

// Sample OHLC 5-min bucket data (CT-aligned)
const sampleOhlc = [
  { bucket_time: '2025-04-21T13:30:00.000Z', close: 500, volume: 100 },
  { bucket_time: '2025-04-21T13:35:00.000Z', close: 502, volume: 200 },
  { bucket_time: '2025-04-21T13:40:00.000Z', close: 504, volume: 300 },
  { bucket_time: '2025-04-21T13:45:00.000Z', close: 506, volume: 0 }, // Should be filtered out
  { bucket_time: '2025-04-21T13:50:00.000Z', close: 508, volume: 400 }
];

async function testCalculateVwap() {
  const vwapSeries = await calculateVwap(sampleOhlc);
  console.log('VWAP Series:', vwapSeries);
  // Simple assertions
  if (vwapSeries.length !== 4) {
    console.error('❌ Test failed: Expected 4 VWAP points (one zero-volume filtered)');
    process.exit(1);
  }
  if (Math.abs(vwapSeries[0].vwap - 500) > 0.0001) {
    console.error('❌ Test failed: First VWAP should equal first close');
    process.exit(1);
  }
  if (vwapSeries[3].bucket_time !== '2025-04-21T13:50:00.000Z') {
    console.error('❌ Test failed: Last VWAP bucket_time mismatch');
    process.exit(1);
  }
  console.log('✅ VWAP calculation test passed.');
}

testCalculateVwap();
