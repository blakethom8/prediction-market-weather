# Insight: ColdMath Wins, Edge Strategy Loses

**Date discovered:** 2026-04-23
**Source:** Strategy performance breakdown across all settled bets
**Status:** Actionable — pivot strategy confirmed

---

## Performance Split

| Strategy | W | L | P&L | Verdict |
|---|---|---|---|---|
| **coldmath** | ✅ dominant | 2 | +$45.91 | **Keep and scale** |
| **baseline** | 9 | 12 | +$22.27 | Learning phase only, retire |
| **consensus** | 1 | 0 | +$2.40 | Tiny sample, watch |
| **edge** | 1 | 2 | **-$2.18** | **Broken. Do not use with real money.** |

## Why Edge Strategy Fails

The edge strategy buys YES at 1-30¢ on specific temperature buckets, betting we have a better model than the market. We don't. The problems:

1. **Point estimate vs probability:** `choose_best_market()` converts a single NWS forecast to a bucket pick with no uncertainty quantification. A point estimate isn't a probability.
2. **Market is smarter:** At 7,000+ contracts, the market has already corrected for airport station bias. When the market says 7¢ and we say 62¢, the market is right.
3. **Bucket specificity requires ±0.5°F accuracy.** We have ±3-5°F. The math doesn't work.

## Why ColdMath Works

ColdMath bets on near-certainties: outcomes that would require historically anomalous conditions to fail.

Examples:
- `CPI >1.0% MoM NO @ 10¢` → CPI at 1% would be a COVID/energy shock level. Market was 90% right; actual probability was 97%+. **+$45 on $5 notional.**
- `DC High >91.5°F in April NO @ 99¢` → DC hitting 91°F in April has happened once in 50 years.

The structural argument doesn't require forecast accuracy. It requires historical base rate knowledge.

## The Pivot

For real-money deployment:
- ✅ ColdMath only until 20 settled wins
- ✅ Near-certainty threshold: ≥85¢ contracts, ≥10°F forecast gap after bias correction
- ✅ Macro structural bets (CPI, GDP extremes) when narrative is clear
- ❌ No edge bucket plays at <50¢ until model uncertainty is properly quantified

## Key Metric to Watch

**ColdMath win rate should approach 90%+.** If it drops below 80% over 20 bets, the structural assumptions are wrong and we need to reassess.
