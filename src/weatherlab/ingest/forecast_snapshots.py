from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from ..db import connect
from ..utils.ids import new_id


def ingest_forecast_snapshot(*, source: str, city_id: str, issued_at_utc: datetime, available_at_utc: datetime, target_date_local: date, pred_high_temp_f: float | None = None, pred_low_temp_f: float | None = None, pred_precip_prob: float | None = None, summary_text: str | None = None, distribution: dict[float, float] | None = None, db_path: str | Path | None = None) -> str:
    forecast_snapshot_id = new_id('forecast')
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            insert into core.forecast_snapshots (
                forecast_snapshot_id, source, city_id, issued_at_utc, available_at_utc,
                target_date_local, pred_high_temp_f, pred_low_temp_f, pred_precip_prob,
                summary_text, raw_ref
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                forecast_snapshot_id,
                source,
                city_id,
                issued_at_utc,
                available_at_utc,
                target_date_local,
                pred_high_temp_f,
                pred_low_temp_f,
                pred_precip_prob,
                summary_text,
                None,
            ],
        )
        if distribution:
            for threshold_f, prob_ge_threshold in distribution.items():
                con.execute(
                    '''
                    insert into core.forecast_distributions (
                        forecast_snapshot_id, threshold_f, prob_ge_threshold
                    ) values (?, ?, ?)
                    ''',
                    [forecast_snapshot_id, float(threshold_f), float(prob_ge_threshold)],
                )
    finally:
        con.close()
    return forecast_snapshot_id
