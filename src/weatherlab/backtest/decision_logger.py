from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ..db import connect
from ..utils.ids import new_id


def log_decision(*, market_ticker: str, signal_version: str, fair_prob: float, market_mid: float, tradable_yes_ask: float | None, tradable_yes_bid: float | None, edge_vs_mid: float, edge_vs_ask: float | None, confidence: float, action: str, abstain_reason: str | None, rationale: dict, db_path: str | Path | None = None) -> str:
    decision_id = new_id('decision')
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            insert into ops.decision_journal (
                decision_id, market_ticker, decided_at_utc, signal_version, fair_prob, market_mid,
                tradable_yes_ask, tradable_yes_bid, edge_vs_mid, edge_vs_ask, confidence,
                action, abstain_reason, rationale_json
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                decision_id,
                market_ticker,
                datetime.now(UTC),
                signal_version,
                fair_prob,
                market_mid,
                tradable_yes_ask,
                tradable_yes_bid,
                edge_vs_mid,
                edge_vs_ask,
                confidence,
                action,
                abstain_reason,
                json.dumps(rationale),
            ],
        )
    finally:
        con.close()
    return decision_id
