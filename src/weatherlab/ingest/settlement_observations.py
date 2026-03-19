from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from ..db import connect
from ..utils.ids import new_id


def ingest_settlement_observation(*, source: str, station_id: str, city_id: str, market_date_local: date, observed_high_temp_f: float | None = None, observed_low_temp_f: float | None = None, observed_precip_in: float | None = None, report_published_at_utc: datetime | None = None, is_final: bool = True, db_path: str | Path | None = None) -> str:
    settlement_id = new_id('settlement')
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            insert into core.settlement_observations (
                settlement_id, source, station_id, city_id, market_date_local,
                observed_high_temp_f, observed_low_temp_f, observed_precip_in,
                report_published_at_utc, is_final, raw_ref
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                settlement_id,
                source,
                station_id,
                city_id,
                market_date_local,
                observed_high_temp_f,
                observed_low_temp_f,
                observed_precip_in,
                report_published_at_utc,
                is_final,
                None,
            ],
        )
    finally:
        con.close()
    return settlement_id
