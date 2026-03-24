# Betting Insights & Lessons Learned

A living document. Updated as we learn. The goal is to avoid repeating mistakes and to compound knowledge across sessions.

---

## Settlement & Data Sources

### The Airport Station Gap (Discovered: March 23, 2026)

**The most important thing to understand about these markets.**

Kalshi weather contracts settle against the **NWS Climatological Report (Daily)** for a specific weather station — typically the primary airport serving that city. This is NOT the same as the general metro area NWS forecast.

**Why it matters:** Airport weather stations can read meaningfully different from the surrounding metro area due to:
- Urban heat island effects (airports often on city periphery, but surrounded by tarmac)
- Coastal influence (LAX, Logan/BOS are coastal → cooler than inland)
- Elevation and terrain effects
- Station-specific microclimates

**Known biases by city:**

| City | Settlement Station | Known Bias vs Metro NWS |
|---|---|---|
| Washington DC | Reagan National Airport (DCA) | **+5 to +8°F warmer** than metro forecast. DCA is in a river valley with urban heat island. |
| Miami | Miami International Airport (MIA) | Roughly in line with metro. Less extreme bias. |
| Boston | Logan International (BOS) | Can run **warmer** than metro in winter due to ocean heat retention, **cooler** in summer. |
| Philadelphia | Philadelphia International (PHL) | Moderate warm bias vs metro (~2-4°F). |
| Los Angeles | LAX | **Cooler** than inland LA — coastal influence. Metro NWS may overstate highs by 5-10°F for LAX specifically. |
| Chicago | Midway (MDW) | Used for KXHIGHCHI. Midway is warmer than O'Hare (ORD) and runs closer to metro. |
| Denver | Denver International (DEN) | DEN is ~15 miles NE of downtown, slightly cooler. Check carefully. |

**The practical rule:**
> Never use generic metro lat/lon for the NWS forecast. Use the specific airport coordinates.

**Correct lat/lon for each settlement station:**

| City | Series | Station | Lat | Lon |
|---|---|---|---|---|
| DC | KXHIGHTDC | Reagan National (DCA) | 38.8521 | -77.0377 |
| Miami | KXHIGHMIA | Miami Intl (MIA) | 25.7959 | -80.2870 |
| Boston | KXHIGHTBOS | Logan Intl (BOS) | 42.3601 | -71.0105 |
| Philadelphia | KXHIGHPHIL | Philadelphia Intl (PHL) | 39.8721 | -75.2411 |
| Los Angeles | KXHIGHLAX | LAX | 33.9425 | -118.4081 |
| Chicago | KXHIGHCHI | Midway (MDW) | 41.7868 | -87.7522 |
| Denver | KXHIGHDEN | Denver Intl (DEN) | 39.8561 | -104.6737 |
| NYC | KXHIGHNY | Central Park (KNYC) | 40.7789 | -73.9692 |
| Seattle | KXHIGHTSEA | Seattle-Tacoma Intl (SEA) | 47.4502 | -122.3088 |
| Dallas | KXHIGHTDAL | Dallas Love Field (DAL) or DFW — verify | 32.8998 | -97.0403 |
| Houston | KXHIGHTHOU | Houston Hobby (HOU) — verify | 29.6454 | -95.2789 |
| Atlanta | KXHIGHTATL | Hartsfield-Jackson (ATL) | 33.6367 | -84.4281 |

> ⚠️ Always verify the settlement station by reading the contract's `rules_primary` field. Kalshi occasionally uses a different station than expected.

**What to fix in the model:**
- Replace generic metro lat/lon in `CITIES` dict with airport-specific coordinates
- Fetch NWS forecast for the specific station, not the city center
- Re-calibrate edge estimates after this fix — prior estimates were overconfident

---

## Market Pricing Patterns

### High-volume markets are usually right (March 23, 2026)

When a market has 7,000–25,000 contracts of volume, informed traders have likely already corrected for the airport station bias. Our model using metro NWS looked like it found huge edge (e.g. DC <67° at 1¢ when our metro forecast said 60°F) — but the market was pricing Reagan National's warmth correctly.

**Rule of thumb:**
> If the market price diverges sharply from your model AND volume is high, assume the market knows something you don't (usually the station-specific forecast). Investigate before betting.

Low-volume markets (<1,000 contracts) are more likely to be mispriced.

### Mutually exclusive buckets — probability must sum to ~1

Each city's market has mutually exclusive buckets (e.g. <79°, 79-80°, 81-82°, 83-84°, 85-86°, >86°). The sum of all YES prices should approximate $1.00. If they don't add up, there's arbitrage or illiquidity.

Use this as a sanity check: if you're modeling one bucket, your fair prob + the complement must be coherent.

### Price movement signals

- Prices moving toward YES → new data supports that outcome (updated forecast, early morning temps, etc.)
- Prices moving toward NO → contra-signal
- Rapid price movement in last 2 hours before close → something is known (preliminary NWS data, early readings)

---

## Forecast Models

### NWS vs Open-Meteo disagreement

When NWS and Open-Meteo disagree by more than 5°F:
- **Do not bet** unless you have a strong reason to trust one over the other
- Chicago and Dallas showed 9-13°F splits on March 23 — these were correctly avoided
- Large splits usually mean complex local weather (fronts, convection, terrain) where both models are uncertain

### When to trust NWS over Open-Meteo

- NWS is more accurate for precipitation events and associated temperature suppression (rain/snow cools temps dramatically)
- NWS is more accurate for rapidly changing conditions (frontal passages)
- Open-Meteo uses global models (GFS, ECMWF) and can miss local station behavior

### When to trust Open-Meteo over NWS

- Open-Meteo often has finer-grained ensemble data for clear-weather days
- For sunny, stable days with no fronts, Open-Meteo tends to be well-calibrated

### The NWS "near X°F" language

When NWS says "high near 79°" — this typically means the high will be within ±2°F of 79°. It could round to 78° or 80° in the final climatological report. For borderline bucket bets, this uncertainty matters.

---

## Workflow Lessons

### Paper bets before real money

Always run a paper session first for any new city, new model version, or new strategy variant. The paper system exists to build calibration before risking capital.

### Sizing discipline

- First bets: 1-7¢ range (cheap options, maximum learning per dollar)
- Scale only after validating model accuracy over ~20-30 settled bets
- Never bet large on a model you've just built — even if the edge looks enormous
- The 1¢ bets are not "too small to matter" — they're data collection at minimal cost

### The two-strategy comparison model

Running Strategy A (broad) and Strategy B (focused) in parallel on the same day is the correct way to learn which selection criteria actually work. The `strategy_variant` field in `ops.strategy_sessions` supports this natively. Use it.

### Settlement workflow (Monday morning routine)

1. `make sync-live-orders` — pull fill status updates from Kalshi
2. Check Kalshi app or NWS Climatological Reports for each city's actual high
3. For each position: `settle_live_order(kalshi_order_id, 'yes'/'no', settlement_note='NWS reported X°F')`
4. Review History page — realized P&L updates automatically
5. Write lessons to this file if anything new was learned

---

## Timing & When to Bet

### Don't bet at midnight on same-day markets (Lesson: March 23, 2026)

**This may be the most actionable lesson from our first real session.**

We placed all bets between midnight and 1:30 AM PDT on March 23 markets. Miami was forecast at 79°F by NWS at midnight. By afternoon, the NWS airport forecast updated to 83°F. The market was pricing 83-84° at 59¢ all along — it was already pricing in forecast information we didn't have.

**The problem:** Overnight NWS forecasts are the least reliable point in the forecast cycle. Models run at 00Z and 12Z UTC. By the time the 12Z (noon UTC = 5 AM PDT) model run assimilates overnight observations, the forecast often shifts materially. By 2-4 PM local time, forecasters have also issued afternoon updates incorporating the morning's actual temperature trajectory.

**Optimal betting window: 2–4 PM local time on the market day.**
- NWS has issued its afternoon forecast update
- Morning observations are assimilated into models
- You can see how temps are actually tracking intraday
- Only 4–6 hours to close instead of 17+
- Market may still misprice afternoon information — that's your edge window

**When early betting makes sense (exceptions):**
- Screaming structural mispricing that won't close (e.g. 1¢ on a city forecast to be 15°F below threshold)
- You have strong reason to believe the overnight forecast is stable (clear, calm, high-pressure day)
- You're buying very cheap options where the forecast shift risk is priced in

---

### Optimal Betting Windows by City Time Zone

**The core problem:** Daily high temps occur at different local times. By the time you bet, some cities' highs are already recorded. The market prices that in fast.

East Coast cities (EDT = PDT + 3h):
- Peak temp: 2–4 PM EDT = **11 AM – 1 PM PDT**
- Market prices it in: by ~2 PM EDT = **11 AM PDT**
- **Bet East Coast cities before 11 AM PDT**

Central cities (CDT = PDT + 2h):
- Peak temp: 2–4 PM CDT = **12 PM – 2 PM PDT**
- **Bet Chicago/Houston/Dallas/Atlanta before 12 PM PDT**

West Coast cities:
- Peak temp: 2–4 PM PDT
- Market prices it in: by ~4 PM PDT
- **Bet LA/Seattle before 2 PM PDT**

### Recommended daily workflow

| Time (PDT) | Action |
|---|---|
| **8–9 AM** | Morning scan — check NWS forecasts, identify candidates. Don't bet yet. Fix any data issues. |
| **9–10 AM** | Validate NWS via ASOS observed temps. If morning obs already 10°F off from forecast, flag it. |
| **10–11 AM** | **Primary East Coast betting window.** DC, Philly, NYC, Boston, Atlanta. Forecast reliable, high not yet recorded. |
| **11 AM – 12 PM** | **Central cities window.** Chicago, Houston, Dallas. |
| **12–1 PM** | **West Coast window.** LA, Seattle. Also last chance for Central. |
| **After 2 PM PDT** | East Coast highs are IN. Market has priced them. Don't bet East Coast. West Coast may still have small windows. |
| **After 4 PM PDT** | All highs recorded. Market fully priced. No new opportunities. |

### Forecast Reliability by Time of Day

| Time (PDT) | NWS Reliability | Market Efficiency | Sweet Spot? |
|---|---|---|---|
| Midnight | Low | Low | ❌ Avoid |
| 6 AM | Low-Medium | Low | ❌ Too early |
| 8–9 AM | Medium | Low | ✅ Scan only |
| 10–11 AM | High | Medium | ✅ Best for East Coast |
| 12–1 PM | High | Medium-High | ✅ Best for Central/West |
| 2–3 PM | Very High | High (East Coast done) | ⚠️ West Coast only |
| 4 PM+ | Certain | Near-certain | ❌ No edge left |

---

## Market Structure Lessons

### The 1¢ under-threshold bet structure (Discovered: March 23, 2026)

When the market prices a "high will be below X°F" contract at 1¢, it's saying there's roughly a 1% chance of that outcome. On cold/rainy days, NWS forecasts often point to temperatures well below major thresholds, and the market systematically underprices these because:
1. Retail traders focus on the "interesting" mid-range buckets
2. Low-volume contracts get less market-maker attention
3. The 1¢ floor means any non-zero probability bet gets the same price

**The opportunity:** On days where NWS clearly forecasts temps 10°F+ below a threshold, the 1¢ YES contracts may be the best risk/reward in the market. $2 can return $200.

**The caution:** The settlement station may differ from the forecast point. Airport stations sometimes run warmer. Always verify the specific station forecast, not just the metro.

**Example from March 23:**
- DC <67°: market at 1¢, NWS Reagan = 52°F (15°F below threshold) → likely WIN
- Philly <58°: market at 1¢, NWS PHL = 51°F (7°F below threshold) → likely WIN
- Boston <36°: market at 1¢, NWS Logan = 45°F (9°F ABOVE threshold) → LOSS
- The difference: Boston's Logan Airport coastal warmth made the metro NWS forecast irrelevant

---

### Market vs Model disagreement signals

**When market diverges from your model by >10¢:**
- If volume is HIGH (>5,000 contracts): market is almost certainly right. Find out why.
- If volume is LOW (<1,000 contracts): could be genuine mispricing. Investigate.
- Miami example: market priced 83-84° at 59¢ while our model said 79°F. High volume. Market was right — the afternoon NWS update confirmed 83°F.

**When market agrees with your model (<5¢ difference):**
- Confirmation, but lower expected edge
- DC/Philly at 1¢ were market-agreement bets: market said <1% chance, our model said ~40-60% chance — that's a genuine disagreement worth betting

---

## City-Specific Notes

### Boston / Logan Airport
- Logan sits on a peninsula in Boston Harbor
- Harbor heat retention makes Logan run **dramatically warmer** than metro in winter/spring
- On March 23: metro NWS said 35°F with sleet. Logan actually ~45°F. Gap = 10°F.
- **Rule: Never bet Boston below-threshold contracts in winter/spring precip events.** Logan will always be warmer than metro.
- In summer: Logan runs COOLER than metro (ocean influence reverses)

### Miami / MIA Airport
- Inland enough that airport temps are close to metro
- But: NWS morning forecasts can shift significantly by afternoon on sunny days as the sea breeze timing changes
- March 23: midnight NWS = 79°F → afternoon NWS = 83°F. The market knew before we did.
- **Rule: For Miami sunny day forecasts, trust the afternoon NWS over midnight. Market often prices the afternoon outcome correctly at midnight.**

### Washington DC / Reagan National (DCA)
- DCA runs warm in urban heat island — but on cold rainy days, this is less relevant
- Cold front days with rain: metro NWS and DCA tend to agree (both just cold)
- Warm sunny days: DCA runs 5-8°F warmer than metro
- March 23: both metro and DCA showed ~52°F — cold front day, bias minimal

### Houston / Settlement Station TBD
- Open question: Kalshi settles KXHIGHTHOU at Hobby (HOU) or Bush Intercontinental (IAH)?
- On March 23: NWS at HOU said 84°F but market priced >83° at ~0¢ — contradiction needs resolution
- **TODO: Verify settlement station from Kalshi contract rules before betting Houston again**

---

## Open Questions (to resolve with data)

- [ ] Does Reagan National (DCA) actually run 5-8°F warmer than metro NWS? **March 23 cold front day showed minimal bias — need warm sunny day data.**
- [ ] Is the PHL airport bias consistent or seasonal?
- [ ] Does LAX run 5-10°F cooler than inland LA metro? **March 23: NWS LAX said 70°F — need to compare to actual settlement.**
- [ ] Does NWS or Open-Meteo have better directional accuracy for our cities?
- [ ] At what volume threshold is a market "informed" enough to trust over our model?
- [ ] What is our fair probability model's actual calibration? (need 30+ settled bets)
- [ ] What is the Houston Kalshi settlement station — Hobby (HOU) or Bush Intercontinental (IAH)?
- [ ] How much does NWS forecast shift between midnight and afternoon on sunny vs rainy days? (Miami showed 4°F shift on sunny day)
- [ ] Is the 1¢ under-threshold structure systematically mispriced, or was March 23 an anomaly?

---

## NWS Data Sourcing Problem — DC (Discovered: March 23, 2026)

**Critical model bug.** Our NWS forecast for DC showed 52°F using Reagan National coordinates (38.8521, -77.0377). Actual high at settlement: **67-68°F**. That is a 15°F miss — not a forecast error, a data sourcing error.

**What likely happened:** The NWS `/points/{lat},{lon}` API returns the forecast office and grid cell for that location. Reagan National sits at a grid cell boundary, and the API may have been returning the forecast for a grid cell that doesn't correspond to the actual KDCA observation station. The NWS forecast grid and the official climatological observation station are different things.

**The fix:**
- Don't rely solely on `api.weather.gov/points` lat/lon lookup for settlement temperature
- Cross-reference with the actual ASOS/AWOS station ID (KDCA for Reagan, KPHL for Philly, etc.)
- Use the NWS hourly obs API or MesoWest for actual station readings: `https://api.weather.gov/stations/KDCA/observations/latest`
- Or use the station-specific forecast: `https://forecast.weather.gov/MapClick.php?CityName=Reagan+National&state=VA&site=LWX&textField1=38.8521&textField2=-77.0377`

**Station IDs for each settlement city:**

| City | Kalshi Series | ASOS Station | NWS Office |
|---|---|---|---|
| DC / Reagan National | KXHIGHTDC | KDCA | LWX |
| Miami / MIA | KXHIGHMIA | KMIA | MFL |
| Philadelphia / PHL | KXHIGHPHIL | KPHL | PHI |
| Boston / Logan | KXHIGHTBOS | KBOS | BOX |
| NYC / Central Park | KXHIGHNY | KNYC | OKX |
| Chicago / Midway | KXHIGHCHI | KMDW | LOT |
| LA / LAX | KXHIGHLAX | KLAX | LOX |
| Houston / (TBD) | KXHIGHTHOU | KHOU or KIAH | HGX |
| Seattle / Sea-Tac | KXHIGHTSEA | KSEA | SEW |
| Atlanta / Hartsfield | KXHIGHTATL | KATL | FFC |

**To get actual observed high for today:**
```
GET https://api.weather.gov/stations/KDCA/observations?start=2026-03-23T00:00:00Z&end=2026-03-23T23:59:59Z&limit=50
```
Parse `temperature.value` (Celsius, convert to °F) and take the max.

**Priority fix:** Update the model to validate forecast against observed temps at the ASOS station during the day. If the observed temp at 11 AM is already 62°F and NWS says high of 52°F — something is wrong, don't bet on that forecast.

---

## March 23, 2026 — Final Settlement Results

NWS market-implied outcomes (from Kalshi pricing at ~5:30 PM PDT):

| City | NWS Forecast (our model) | Actual High (market-implied) | Miss |
|---|---|---|---|
| Miami | 83°F | 81-82°F | -1 to -2°F (close) |
| DC | 52°F | **67-68°F** | **+15-16°F** ← data sourcing bug |
| Philly | 51°F | 58-59°F | +7-8°F (partial airport bias) |
| Boston | 45°F | 38-39°F | -6-7°F (Logan ran COOL, not warm) |
| NYC | 48°F | <54°F | Confirmed correct direction |
| Chicago | 52°F | 40-41°F | -11-12°F ← NWS badly wrong |
| LA | 70°F | <76°F | Correct direction |
| Houston | 84°F | 83-84°F | ~correct but threshold miss |

**Boston surprise:** Logan ran COOLER than metro NWS (38-39°F vs forecast of 45°F). The harbor heat retention thesis was wrong for this specific storm type. Sleet/wintry mix suppressed Logan more than the metro. Update our Boston mental model.

**Real money P&L (estimated):**
- Wins: LA <76° (Blake) — ~$0.40 profit
- Losses: everything else — ~-$28
- Net: approximately **-$28 on first session**

**Paper bet strategy winners (preliminary):**
- Strategy C (market follower) — would have won on Miami, lost on DC/Philly/Boston → mixed
- Strategy A/D (NWS-based) — would have won on DC/Philly IF model was correct, but model was wrong → all losses
- No strategy clearly "won" because the underlying data was flawed

---

## Session Log

| Date | Key Lesson | Bets | Result |
|---|---|---|---|
| 2026-03-23 | NWS data sourcing bug (DC miss = 15°F); midnight forecasts unreliable; Boston Logan ran COOL not warm in wintry mix; market was right on everything | 8 real + 21 paper | ~-$28 real money; LA <76 only win |

---

*Last updated: 2026-03-23 17:25 PDT*
*Add new insights after each settling cycle.*


## March 23, 2026 Settlement Review

- Philly: model missed by -6.0°F versus KPHL actual 52.0°F.
