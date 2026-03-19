from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ..db import connect


def ingest_market_snapshot(*, market_ticker: str, ts_utc: datetime, price_yes_bid: float | None, price_yes_ask: float | None, price_no_bid: float | None = None, price_no_ask: float | None = None, last_price: float | None = None, volume: float | None = None, open_interest: float | None = None, minutes_to_close: int | None = None, db_path: str | Path | None = None) -> None:
    price_yes_mid = None
    if price_yes_bid is not None and price_yes_ask is not None:
        price_yes_mid = (price_yes_bid + price_yes_ask) / 2
    price_no_mid = None
    if price_no_bid is not None and price_no_ask is not None:
        price_no_mid = (price_no_bid + price_no_ask) / 2
    spread_yes = None
    if price_yes_bid is not None and price_yes_ask is not None:
        spread_yes = price_yes_ask - price_yes_bid

    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            insert into core.market_snapshots (
                market_ticker, ts_utc, price_yes_bid, price_yes_ask, price_yes_mid,
                price_no_bid, price_no_ask, price_no_mid, last_price, spread_yes,
                volume, open_interest, minutes_to_close
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(market_ticker, ts_utc) do update set
                price_yes_bid = excluded.price_yes_bid,
                price_yes_ask = excluded.price_yes_ask,
                price_yes_mid = excluded.price_yes_mid,
                price_no_bid = excluded.price_no_bid,
                price_no_ask = excluded.price_no_ask,
                price_no_mid = excluded.price_no_mid,
                last_price = excluded.last_price,
                spread_yes = excluded.spread_yes,
                volume = excluded.volume,
                open_interest = excluded.open_interest,
                minutes_to_close = excluded.minutes_to_close
            ''',
            [
                market_ticker,
                ts_utc,
                price_yes_bid,
                price_yes_ask,
                price_yes_mid,
                price_no_bid,
                price_no_ask,
                price_no_mid,
                last_price,
                spread_yes,
                volume,
                open_interest,
                minutes_to_close,
            ],
        )
    finally:
        con.close()
