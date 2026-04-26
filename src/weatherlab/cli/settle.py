"""Settlement check logic for the Chief CLI.

Walks open paper bets, looks up settlement observations, computes outcomes,
and closes the bets that can be resolved.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _compute_outcome(
    observed_high_f: float,
    operator: str | None,
    threshold_low_f: float | None,
    threshold_high_f: float | None,
) -> str | None:
    """Return 'YES' or 'NO' for a temperature contract outcome.

    Logic mirrors pipeline._markets.outcome_for_observed_high:
      between:  YES if threshold_low <= observed < threshold_high
      >=:       YES if observed >= threshold_low
      <=:       YES if observed < threshold_low   (Kalshi convention)
      >:        YES if observed > threshold_low
      <:        YES if observed < threshold_low
    """
    if operator is None or threshold_low_f is None:
        return None

    obs = float(observed_high_f)
    low = float(threshold_low_f)

    if operator == "between":
        if threshold_high_f is None:
            return None
        high = float(threshold_high_f)
        resolved = low <= obs < high
    elif operator == ">=":
        resolved = obs >= low
    elif operator == ">":
        resolved = obs > low
    elif operator == "<=":
        # Kalshi convention: YES means strictly below threshold
        resolved = obs < low
    elif operator == "<":
        resolved = obs < low
    else:
        return None

    return "YES" if resolved else "NO"


def _threshold_display(
    operator: str | None,
    threshold_low_f: float | None,
    threshold_high_f: float | None,
) -> str:
    if operator is None or threshold_low_f is None:
        return "?"
    if operator == "between" and threshold_high_f is not None:
        return f"{threshold_low_f}-{threshold_high_f}"
    return f"{operator}{threshold_low_f}"


def check_and_settle_open_bets(
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Check all open paper bets against settlement data and close resolved ones.

    Returns a list of result dicts — one per open bet — describing what happened.
    """
    from .queries import get_open_bets_with_contracts, get_settlement_observation
    from ..live.persistence import settle_paper_bet

    open_bets = get_open_bets_with_contracts(db_path=db_path)
    results: list[dict[str, Any]] = []

    for bet in open_bets:
        ticker = bet.get("market_ticker", "")
        station_id = bet.get("station_id")
        market_date_local = bet.get("market_date_local")
        measure = bet.get("measure", "temperature")

        result: dict[str, Any] = {
            "paper_bet_id": bet.get("paper_bet_id"),
            "market_ticker": ticker,
            "settled": False,
        }

        # Only handle temperature markets for now
        if measure and measure != "temperature":
            result["error"] = f"unsupported measure: {measure}"
            results.append(result)
            continue

        if not station_id or market_date_local is None:
            result["error"] = "missing station_id or market_date_local in contract"
            results.append(result)
            continue

        observation = get_settlement_observation(
            station_id=station_id,
            market_date_local=market_date_local,
            db_path=db_path,
        )

        if observation is None or observation.get("observed_high_temp_f") is None:
            results.append(result)
            continue

        observed_high = float(observation["observed_high_temp_f"])
        operator = bet.get("operator")
        threshold_low = bet.get("threshold_low_f")
        threshold_high = bet.get("threshold_high_f")

        outcome_label = _compute_outcome(observed_high, operator, threshold_low, threshold_high)
        if outcome_label is None:
            result["error"] = f"could not compute outcome (operator={operator})"
            results.append(result)
            continue

        side = bet.get("side", "")
        won = (side == "BUY_YES" and outcome_label == "YES") or (
            side == "BUY_NO" and outcome_label == "NO"
        )

        try:
            settle_paper_bet(
                paper_bet_id=bet["paper_bet_id"],
                outcome_label=outcome_label,
                review={
                    "settlement_source": observation.get("source"),
                    "observed_high_temp_f": observed_high,
                    "settlement_id": observation.get("settlement_id"),
                    "is_final": observation.get("is_final"),
                },
                db_path=db_path,
            )
        except Exception as exc:
            result["error"] = f"settle_paper_bet failed: {exc}"
            results.append(result)
            continue

        # Compute realized_pnl the same way persistence.py does
        limit_price = float(bet.get("limit_price") or 0)
        quantity = float(bet.get("quantity") or 0)
        if side == "BUY_YES":
            payout = quantity if outcome_label == "YES" else 0.0
        else:
            payout = quantity if outcome_label == "NO" else 0.0
        realized_pnl = payout - (limit_price * quantity)

        result.update({
            "settled": True,
            "outcome_label": outcome_label,
            "won": won,
            "observed_value": observed_high,
            "threshold_display": _threshold_display(operator, threshold_low, threshold_high),
            "realized_pnl": realized_pnl,
        })
        results.append(result)

    return results
