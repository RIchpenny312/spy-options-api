function classifyDeltaTrend(delta_call, delta_put, delta_call_pct, delta_put_pct) {
  if (Math.abs(delta_call - delta_put) < 2_000_000) return "Balanced Shift";
  if (delta_call > 5_000_000 && delta_call_pct > 5) return "Call Surge";
  if (delta_call < -5_000_000 && delta_call_pct < -5) return "Call Unwind";
  if (delta_put < -5_000_000 && delta_put_pct < -5) return "Put Absorption";
  if (delta_put > 5_000_000 && delta_put_pct > 5) return "Put Demand Spike";
  return "Unclassified";
}

function suggestLabelExplanation(flow_direction, alignment_label) {
  if (flow_direction === "Call Surge" && alignment_label === "Bullish Alignment") {
    return "Momentum ignition from both call demand and dealer alignment.";
  }
  if (flow_direction === "Put Demand Spike" && alignment_label === "Bearish Alignment") {
    return "Protective hedging spike confirms risk-off environment.";
  }
  if (flow_direction === "Put Absorption" && alignment_label === "Bullish Alignment") {
    return "Dealers aggressively unwinding puts, reducing downside pressure.";
  }
  if (flow_direction === "Call Unwind" && alignment_label === "Bearish Alignment") {
    return "Dealer shedding of call exposure signals breakdown risk.";
  }
  if (flow_direction === "Balanced Shift") {
    return "Flow activity is balanced with no dominant directional intent.";
  }
  return "No strong contextual alignment.";
}

module.exports = {
  classifyDeltaTrend,
  suggestLabelExplanation,
};
