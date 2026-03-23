# Historical Forecast Archive Plan

## Focus Cities

Initial honest historical pipeline focus:
- `nyc`
- `chi`

These are the first cities we should treat as serious research targets for
"available-at-the-time" forecast reconstruction.

## Source Strategy

### 1. Primary target: archived NDFD
Reason:
- closest fit to issued-time forecast semantics
- better long-run path for point/gridded forecast reconstruction than reanalysis proxies
- likely best foundation for converting forecast issuances into threshold probabilities

### 2. Fallback / corroboration: IEM archived NWS text products
Reason:
- good archive surface for issued NWS text products
- useful for timing and qualitative cross-checks
- practical backup when structured archive access is awkward

### 3. Proxy only: Open-Meteo archive
Reason:
- useful for coverage and diagnostics
- not a true decision-time forecast source
- should not be treated as historical issuance truth

## Implementation Order

1. Lock NYC + Chicago as default historical-research focus cities.
2. Build source-specific metadata and diagnostics around those cities.
3. Implement the first real archived forecast ingestion path.
   - **Current implementation:** IEM archived Zone Forecast Products (`iem-zfp`) for NYC and Chicago
   - NYC: `ZFPOKX` Manhattan block
   - Chicago: `ZFPLOT` Central Cook block
4. Implement the richer NDFD archive path as the next upgrade.
5. Use IEM text products as support/cross-checks where helpful.
6. Keep Open-Meteo archive as a comparison/proxy layer only.

## Current Status

Working now:
- archived issued-time forecast fetch from IEM text archive
- issuance timestamp parsing
- city-specific block extraction for NYC and Chicago
- target-day section extraction
- approximate high-temperature parsing from NWS text phrases
- ingestion into `core.forecast_snapshots` as source `iem-zfp`

Still next:
- richer gridded NDFD archive ingestion
- cleaner precipitation / low-temp extraction
- deduping and preferred-source selection between multiple archived issuances

## Success Condition

For NYC and Chicago, we want training rows whose forecast component can answer:
- what forecast was issued?
- when did it become available?
- what was knowable before the Kalshi decision timestamp?

Only once that is working cleanly should the same architecture be scaled to more cities.

This focus-city plan is for historical archive rigor. The live betting board should still scan the full available market set by default.
