"""Fixed evaluator/backtester for weather market experiments."""

from __future__ import annotations

from dataclasses import dataclass

from signal import choose_action, compute_edge


@dataclass
class EvalRow:
    market_ticker: str
    fair_prob: float
    tradable_yes_ask: float
    resolved_yes: int


def score_row(row: EvalRow, min_edge: float = 0.05) -> dict:
    action = choose_action(
        fair_prob=row.fair_prob,
        tradable_yes_ask=row.tradable_yes_ask,
        min_edge=min_edge,
    )
    edge = compute_edge(row.fair_prob, row.tradable_yes_ask)
    pnl = 0.0
    if action == "BUY_YES":
        pnl = (1 - row.tradable_yes_ask) if row.resolved_yes else -row.tradable_yes_ask
    return {
        "market_ticker": row.market_ticker,
        "action": action,
        "edge": edge,
        "pnl": pnl,
    }


if __name__ == "__main__":
    rows = [
        EvalRow("TEST_A", 0.62, 0.50, 1),
        EvalRow("TEST_B", 0.53, 0.50, 0),
    ]
    for row in rows:
        print(score_row(row))
