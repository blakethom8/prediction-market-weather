from __future__ import annotations

from pathlib import Path

from ..db import connect


def materialize_training_rows(db_path: str | Path | None = None) -> int:
    con = connect(db_path=db_path)
    try:
        con.execute('delete from features.contract_training_rows')
        con.execute(
            '''
            insert into features.contract_training_rows (
                market_ticker, ts_utc, city_id, market_date_local,
                price_yes_mid, price_yes_ask, price_yes_bid,
                fair_prob, edge_vs_mid, edge_vs_ask,
                minutes_to_close, sibling_rank, sibling_count, sibling_entropy,
                latest_forecast_snapshot_id, settlement_source, y_resolve_yes
            )
            select
                market_ticker, ts_utc, city_id, market_date_local,
                price_yes_mid, price_yes_ask, price_yes_bid,
                fair_prob, edge_vs_mid, edge_vs_ask,
                minutes_to_close,
                null as sibling_rank,
                null as sibling_count,
                null as sibling_entropy,
                latest_forecast_snapshot_id,
                settlement_source,
                y_resolve_yes
            from features.v_training_rows
            '''
        )
        inserted = con.execute('select count(*) from features.contract_training_rows').fetchone()[0]
    finally:
        con.close()
    return inserted
