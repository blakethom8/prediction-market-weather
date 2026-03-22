"""Promote raw Kalshi data into core normalized tables.

Reads raw.kalshi_markets, parses each title with the contract parser,
and upserts into core.weather_contracts.  Also promotes trade-level
rows from raw.kalshi_market_snapshots into core.market_snapshots.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from ..db import connect
from ..parse.contract_parser import parse_temperature_contract

logger = logging.getLogger(__name__)


@dataclass
class PromotionResult:
    contracts_promoted: int
    contracts_skipped: int
    snapshots_promoted: int


def promote_contracts(db_path: str | Path | None = None) -> PromotionResult:
    """Parse raw markets and load into core.weather_contracts + core.market_snapshots."""
    con = connect(db_path=db_path)
    try:
        # ------------------------------------------------------------------
        # Step 1: Promote raw.kalshi_markets → core.weather_contracts
        # ------------------------------------------------------------------
        raw_markets = con.execute('''
            SELECT market_ticker, event_ticker, title, subtitle,
                   status, result, open_time_utc, close_time_utc
            FROM raw.kalshi_markets
        ''').fetchall()

        logger.info('Parsing %d raw markets ...', len(raw_markets))

        promoted = 0
        skipped = 0
        batch = []

        # Pre-fetch city→station mapping
        city_station_map = {}
        for row in con.execute(
            'SELECT city_id, primary_station_id, timezone_name FROM core.cities'
        ).fetchall():
            city_station_map[row[0]] = (row[1], row[2])

        for market_ticker, event_ticker, title, subtitle, status, result, open_time, close_time in raw_markets:
            parsed = parse_temperature_contract(market_ticker, title)

            if parsed.parse_status == 'failed':
                skipped += 1
                continue

            station_id = None
            timezone_name = None
            if parsed.city_id and parsed.city_id in city_station_map:
                station_id, timezone_name = city_station_map[parsed.city_id]

            batch.append((
                market_ticker,          # contract_id = market_ticker for historical data
                'kalshi',
                market_ticker,
                event_ticker,
                parsed.city_id,
                station_id,
                parsed.market_date_local,
                timezone_name,
                parsed.measure,
                parsed.operator,
                parsed.threshold_low_f,
                parsed.threshold_high_f,
                parsed.parse_status,
                parsed.parse_confidence,
                title,
                subtitle,               # rules_text ← subtitle
                close_time,
                None,                   # settlement_time_utc
                status,
                result,
            ))
            promoted += 1

        # Bulk insert with upsert
        if batch:
            con.executemany(
                '''
                INSERT INTO core.weather_contracts (
                    contract_id, platform, market_ticker, event_ticker,
                    city_id, station_id, market_date_local, timezone_name,
                    measure, operator, threshold_low_f, threshold_high_f,
                    parse_status, parse_confidence, title, rules_text,
                    close_time_utc, settlement_time_utc, status, result
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_ticker) DO UPDATE SET
                    event_ticker = excluded.event_ticker,
                    city_id = excluded.city_id,
                    station_id = excluded.station_id,
                    market_date_local = excluded.market_date_local,
                    timezone_name = excluded.timezone_name,
                    measure = excluded.measure,
                    operator = excluded.operator,
                    threshold_low_f = excluded.threshold_low_f,
                    threshold_high_f = excluded.threshold_high_f,
                    parse_status = excluded.parse_status,
                    parse_confidence = excluded.parse_confidence,
                    title = excluded.title,
                    rules_text = excluded.rules_text,
                    close_time_utc = excluded.close_time_utc,
                    status = excluded.status,
                    result = excluded.result
                ''',
                batch,
            )

        logger.info(
            'Contracts: %d promoted, %d skipped (parse failed).',
            promoted, skipped,
        )

        # ------------------------------------------------------------------
        # Step 2: Promote raw.kalshi_market_snapshots → core.market_snapshots
        #         Only for tickers that made it into core.weather_contracts.
        # ------------------------------------------------------------------
        con.execute('''
            INSERT INTO core.market_snapshots (
                market_ticker, ts_utc,
                price_yes_bid, price_yes_ask, price_yes_mid,
                price_no_bid, price_no_ask, price_no_mid,
                last_price, spread_yes, volume, open_interest,
                minutes_to_close
            )
            SELECT
                s.market_ticker,
                s.ts_utc,
                s.yes_bid,
                s.yes_ask,
                CASE WHEN s.yes_bid IS NOT NULL AND s.yes_ask IS NOT NULL
                     THEN (s.yes_bid + s.yes_ask) / 2.0 END  AS price_yes_mid,
                s.no_bid,
                s.no_ask,
                CASE WHEN s.no_bid IS NOT NULL AND s.no_ask IS NOT NULL
                     THEN (s.no_bid + s.no_ask) / 2.0 END    AS price_no_mid,
                s.last_price,
                CASE WHEN s.yes_bid IS NOT NULL AND s.yes_ask IS NOT NULL
                     THEN s.yes_ask - s.yes_bid END           AS spread_yes,
                s.volume,
                s.open_interest,
                NULL AS minutes_to_close
            FROM raw.kalshi_market_snapshots s
            WHERE EXISTS (
                SELECT 1 FROM core.weather_contracts c
                WHERE c.market_ticker = s.market_ticker
            )
            ON CONFLICT(market_ticker, ts_utc) DO UPDATE SET
                last_price = excluded.last_price,
                volume = excluded.volume
        ''')

        snapshots_promoted = con.execute(
            'SELECT count(*) FROM core.market_snapshots'
        ).fetchone()[0]

        logger.info('Market snapshots promoted: %d', snapshots_promoted)

        return PromotionResult(
            contracts_promoted=promoted,
            contracts_skipped=skipped,
            snapshots_promoted=snapshots_promoted,
        )
    finally:
        con.close()


def infer_settlements(db_path: str | Path | None = None) -> int:
    """Derive settlement observations from resolved sibling buckets.

    For each event_ticker where exactly one 'between' contract resolved YES,
    we infer the observed high temperature as the midpoint of that bucket.
    This gives us training labels without needing NWS historical data.

    Returns the number of settlement observations inserted.
    """
    con = connect(db_path=db_path)
    try:
        # Find events where exactly one 'between' sibling resolved YES.
        # Use the midpoint of that bucket as the observed temperature.
        con.execute('''
            INSERT INTO core.settlement_observations (
                settlement_id, source, station_id, city_id,
                market_date_local, observed_high_temp_f,
                is_final
            )
            SELECT
                'settle_' || c.event_ticker   AS settlement_id,
                'kalshi-implied'              AS source,
                c.station_id,
                c.city_id,
                c.market_date_local,
                (c.threshold_low_f + c.threshold_high_f) / 2.0 AS observed_high_temp_f,
                true                          AS is_final
            FROM core.weather_contracts c
            WHERE c.result = 'yes'
              AND c.operator = 'between'
              AND c.parse_status = 'parsed'
              AND c.city_id IS NOT NULL
              AND c.market_date_local IS NOT NULL
              -- Only events where exactly one between-bucket resolved yes
              AND (
                  SELECT count(*)
                  FROM core.weather_contracts sib
                  WHERE sib.event_ticker = c.event_ticker
                    AND sib.operator = 'between'
                    AND sib.result = 'yes'
              ) = 1
            ON CONFLICT(settlement_id) DO UPDATE SET
                observed_high_temp_f = excluded.observed_high_temp_f
        ''')

        inserted = con.execute(
            'SELECT count(*) FROM core.settlement_observations'
        ).fetchone()[0]

        logger.info('Settlement observations inferred: %d', inserted)
        return inserted
    finally:
        con.close()


def promote_all(db_path: str | Path | None = None) -> None:
    """Run the full promotion pipeline: contracts → snapshots → settlements."""
    result = promote_contracts(db_path=db_path)
    settlements = infer_settlements(db_path=db_path)
    print(
        f'Promotion complete:\n'
        f'  Contracts:   {result.contracts_promoted:,} promoted, '
        f'{result.contracts_skipped:,} skipped\n'
        f'  Snapshots:   {result.snapshots_promoted:,}\n'
        f'  Settlements: {settlements:,}'
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    promote_all()


if __name__ == '__main__':
    main()
