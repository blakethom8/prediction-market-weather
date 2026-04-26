# Inspiration: Strategy & Model Improvement Ideas

**Date added:** 2026-04-26
**Status:** Unvetted ideas — evaluate before implementing

---

## Forecast Accuracy

### ECMWF Ensemble Spread as Uncertainty Proxy
The European Centre for Medium-Range Weather Forecasts (ECMWF) publishes ensemble forecasts with spread data. High ensemble spread = high uncertainty = avoid betting. Low spread = model agreement = higher confidence.
- API: ECMWF Open Data (free for research, ERA5 for historical)
- Could replace our single-point NWS estimate with a distribution

### Intraday ASOS as Live Edge
By 11 AM local time, the observed maximum for the day is often already set (especially in summer). If `observed_max` > bet threshold, you have near-certain confirmation. Buy aggressively.
- Data: Iowa Environmental Mesonet (IEM) — free, real-time ASOS feeds
- Already partially implemented — just not wired into recommendations

### Kalshi Market Microstructure
High open interest + thin spread = informed market. Consider:
- If OI >10,000 and our model disagrees by >15¢ → market is right, skip
- If OI <500 → illiquid, market may be mispriced, opportunity exists
- Track OI trends: rising OI into close = smart money arriving

---

## Strategy Expansion

### Rain/Precipitation Markets
Temperature is hard (continuous, airport-specific). Rain is binary and sometimes easier:
- "Will it rain in NYC tomorrow?" settles YES/NO
- Historical base rates are well-documented by NOAA
- ColdMath applies: "no rain in Phoenix in June" is near-certain

### Wind Speed Markets (if Kalshi offers)
Same structural logic — extreme wind events are rare and predictable.

### Macro Event Calendar
Expand beyond CPI/GDP:
- **Fed rate decisions** — structural bets on extreme moves
- **PCE** — similar to CPI, tariff thesis applies
- **Jobs report** (NFP) — harder to structurally bet, but extreme misses are predictable
- **Election outcomes** — longer time horizon, extreme scenarios

### Arbitrage Watch
If Kalshi and Polymarket both list the same event, price differences are theoretically arbitrageable. Track systematically.

---

## Infrastructure / Meta

### Auto-Improvement Agent
A scheduled agent that:
1. Runs after each batch of settled bets
2. Reads BACKLOG.md + recent insights
3. Generates a code PR implementing the highest-priority fix
4. Human approves before merge

### Backtesting Framework
We have the weather data. Build a backtest that simulates our model's recommendations over the past 2 years against known outcomes. Would let us:
- Validate bias correction magnitude
- Tune ColdMath gap threshold
- Test volume filter impact

### P&L Dashboard
The live web app at `:8082` shows open bets. Extend it to show:
- Historical settled bet P&L by strategy type
- Win rate by city and season
- Edge accuracy: predicted edge vs realized outcome

---

## Reading List

- "Wisdom of Crowds" (Surowiecki) — why market prices beat models in aggregate
- Forecasting literature on NWS skill scores — how good is NWS really?
- Kalshi's settlement methodology docs — verify exact data sources per market
