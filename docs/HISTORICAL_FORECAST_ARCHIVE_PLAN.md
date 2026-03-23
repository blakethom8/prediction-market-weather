# Historical Forecast Archive Plan

This is a research-track document. It supports the live platform by improving the forecast side of the board, but it is not the day-of operator runbook.

## Focus Cities

Initial honest historical pipeline focus:

- `nyc`
- `chi`

These are the first cities to treat as serious point-in-time forecast reconstruction targets. They are still research confidence anchors, not a hard filter on the live board.

## Source Strategy

### 1. Primary target: archived NDFD

Why:

- best fit for issued-time forecast semantics
- better long-run path for point and gridded forecast reconstruction
- best candidate for station-aware, settlement-aligned fair values

### 2. Fallback and corroboration: IEM archived NWS text products

Why:

- practical archive surface for issued NWS text products
- useful for issue-time validation and qualitative cross-checks
- already partially implemented for NYC and Chicago

### 3. Proxy only: Open-Meteo archive

Why:

- useful for coverage and diagnostics
- not a true decision-time forecast source
- should not be treated as historical issuance truth

## Current Implementation

Working now:

- archived issued-time forecast fetch from IEM text archive
- issuance timestamp parsing
- NYC and Chicago city-specific block extraction
- target-day section extraction
- approximate high-temperature parsing from NWS text phrases
- ingestion into `core.forecast_snapshots` as source `iem-zfp`

Still next:

- richer NDFD archive ingest
- better precipitation and low-temp extraction
- deduping and preferred-source selection across multiple issuances
- station-specific forecast mapping that lines up better with airport settlement rules

## Near-Term Goal

The next useful upgrade is not just "more archive coverage." It is better settlement alignment:

- tie forecast selection more directly to the station used by the contract
- reduce city-level proxy errors
- make live fair values and historical evaluation use the same station logic

## Success Condition

For NYC and Chicago, the forecast side of a training row should answer:

- what forecast was issued?
- when was it available?
- which station or settlement proxy did it correspond to?
- what was knowable before the Kalshi decision timestamp?

Only after that is clean should the same approach be scaled to more cities.
