from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ..db import connect
from ..parse.contract_parser import parse_temperature_contract
from ..utils.ids import new_id


def ingest_contract(*, market_ticker: str, event_ticker: str | None, title: str, rules_text: str = '', close_time_utc: datetime | None = None, settlement_time_utc: datetime | None = None, status: str = 'open', result: str | None = None, platform: str = 'kalshi', db_path: str | Path | None = None) -> str:
    parsed = parse_temperature_contract(market_ticker, title)
    contract_id = new_id('contract')
    con = connect(db_path=db_path)
    try:
        station_id = None
        timezone_name = None
        if parsed.city_id:
            row = con.execute(
                'select primary_station_id, timezone_name from core.cities where city_id = ?',
                [parsed.city_id],
            ).fetchone()
            if row:
                station_id, timezone_name = row
        con.execute(
            '''
            insert into core.weather_contracts (
                contract_id, platform, market_ticker, event_ticker, city_id, station_id,
                market_date_local, timezone_name, measure, operator, threshold_low_f, threshold_high_f,
                parse_status, parse_confidence, title, rules_text, close_time_utc, settlement_time_utc,
                status, result
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(market_ticker) do update set
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
                settlement_time_utc = excluded.settlement_time_utc,
                status = excluded.status,
                result = excluded.result
            ''',
            [
                contract_id,
                platform,
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
                rules_text,
                close_time_utc,
                settlement_time_utc,
                status,
                result,
            ],
        )
    finally:
        con.close()
    return contract_id
