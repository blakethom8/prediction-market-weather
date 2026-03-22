# Kalshi Weather Priority Roadmap

## Guiding Principle

Build the truth and replay layers before spending too much energy on clever gating, calibration, and configuration.

## Build Now

1. **Settlement / truth adapter**
   - official-source-shaped ingestion
   - final vs preliminary handling
   - station/date normalization
   - realistic fixtures + tests

2. **Source-backed decision replay**
   - take a parsed contract + forecast + market snapshot + settlement truth
   - materialize training row
   - run signal
   - generate rationale
   - write decision journal entry

## Build Next

3. **Strategy config layer**
   - min confidence
   - min edge
   - max open positions
   - daily loss caps
   - per-city limits
   - time-to-close gates

4. **Decision gate report**
   - did forecast gate pass?
   - did market gate pass?
   - did risk gate pass?
   - why was a bet skipped?

5. **Confidence tracking / city-specific calibration**
   - calibration by city
   - forecast error by city
   - realized edge by city
   - threshold review over time

## Build Later

6. **Paper trading / richer evaluator**
7. **Review dashboard / postmortem surface**
8. **Execution engine**

## Current Sprint Objective

Make the MVP testing environment more honest by strengthening:
- settlement truth
- source-backed replay
- tests and fixtures around both
- focus-city historical pipeline maturity (start with NYC + Chicago)
- archived forecast source plan centered on NDFD archive with IEM text fallback
- first real archived issued-time forecast ingestion path (`iem-zfp`) for NYC + Chicago

## Current Architectural Priority

The immediate product priority is now the **point-in-time betting architecture**:
- compare the full daily board before selecting a bet
- generate a daily strategy package/report
- capture rationale, forecast context, approvals, and abstentions
- settle outcomes and review paper performance
- improve daily edge selection through iteration
- keep live/app code structurally separate from research/ML code even while they share one repo

The biggest remaining honesty gap for research is still **"available data at the time"**.

That means:
- do not treat archive/reanalysis data as if it were a real historical forecast issuance
- use archive sources as proxy layers only
- build the first serious historical pipeline for a small number of cities before scaling out
- prefer explicit city-readiness diagnostics over vague confidence
