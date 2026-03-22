from __future__ import annotations

from pathlib import Path

from .backtest.decision_logger import log_decision
from .backtest.rationale import build_rationale
from .db import connect
from .signal import choose_action


def replay_decision_for_market(*, market_ticker: str, signal_version: str = 'baseline-v1', min_edge: float = 0.05, db_path: str | Path | None = None) -> str:
    con = connect(db_path=db_path)
    try:
        row = con.execute(
            '''
            select market_ticker, city_id, latest_forecast_snapshot_id, fair_prob, price_yes_mid, price_yes_ask, price_yes_bid, minutes_to_close
            from features.contract_training_rows
            where market_ticker = ?
            order by ts_utc desc
            limit 1
            ''',
            [market_ticker],
        ).fetchone()
    finally:
        con.close()

    if row is None:
        raise ValueError(f'No training row found for market {market_ticker}')

    market_ticker, city_id, forecast_snapshot_id, fair_prob, market_mid, yes_ask, yes_bid, minutes_to_close = row
    edge_vs_mid = fair_prob - market_mid if fair_prob is not None and market_mid is not None else None
    edge_vs_ask = fair_prob - yes_ask if fair_prob is not None and yes_ask is not None else None
    action = choose_action(fair_prob=fair_prob, tradable_yes_ask=yes_ask, min_edge=min_edge)
    abstain_reason = None if action != 'NO_TRADE' else 'edge_below_threshold'
    rationale = build_rationale(
        fair_prob=fair_prob,
        market_mid=market_mid,
        tradable_yes_ask=yes_ask,
        edge_vs_ask=edge_vs_ask,
        minutes_to_close=minutes_to_close,
        city_id=city_id,
        forecast_source='training-row',
        strategy_context={'mode': 'single_market_replay', 'forecast_snapshot_id': forecast_snapshot_id},
    )
    rationale['gate_summary'] = {
        'has_fair_prob': fair_prob is not None,
        'has_ask': yes_ask is not None,
        'edge_threshold': min_edge,
        'edge_vs_ask': edge_vs_ask,
    }

    return log_decision(
        market_ticker=market_ticker,
        signal_version=signal_version,
        fair_prob=fair_prob,
        market_mid=market_mid,
        tradable_yes_ask=yes_ask,
        tradable_yes_bid=yes_bid,
        edge_vs_mid=edge_vs_mid,
        edge_vs_ask=edge_vs_ask,
        confidence=0.5,
        action=action,
        abstain_reason=abstain_reason,
        rationale=rationale,
        db_path=db_path,
    )
