"""Plain-text formatters for Chief CLI output.

No ANSI color. Output must be clean for programmatic reading.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _age_label(ts: Any) -> str:
    """Return a human-readable staleness label like '18h ago — STALE'."""
    if ts is None:
        return "never"
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except ValueError:
            return str(ts)
    now = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = now - ts
    hours = int(delta.total_seconds() / 3600)
    if hours < 1:
        mins = int(delta.total_seconds() / 60)
        label = f"{mins}m ago"
    elif hours < 24:
        label = f"{hours}h ago"
    else:
        days = hours // 24
        label = f"{days}d ago"
    stale = " -- STALE" if hours >= 12 else ""
    return f"{label}{stale}"


def _fmt_ts(ts: Any) -> str:
    if ts is None:
        return "never"
    if isinstance(ts, str):
        return ts[:16].replace("T", " ")
    if hasattr(ts, "strftime"):
        return ts.strftime("%Y-%m-%d %H:%M UTC")
    return str(ts)


def _fmt_float(value: Any, decimals: int = 2, prefix: str = "") -> str:
    if value is None:
        return "N/A"
    return f"{prefix}{float(value):.{decimals}f}"


def _fmt_dollars(value: Any) -> str:
    if value is None:
        return "N/A"
    v = float(value)
    sign = "+" if v >= 0 else ""
    return f"{sign}${v:.2f}"


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"{float(value) * 100:.0f}%"


def _col_widths(headers: list[str], rows: list[list[str]]) -> list[int]:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(cell))
    return widths


def _table(headers: list[str], rows: list[list[str]]) -> str:
    widths = _col_widths(headers, rows)
    sep = "  "
    lines = [sep.join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    lines.append(sep.join("-" * w for w in widths))
    for row in rows:
        lines.append(sep.join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def format_status(
    sync_status: dict[str, Any],
    board_rows: list[dict[str, Any]],
    open_bets: list[dict[str, Any]],
    pnl: dict[str, Any],
    kill_switch: bool,
    today: str,
) -> str:
    lines: list[str] = []
    lines.append(f"=== CHIEF STATUS -- {today} ===")

    # SYNC
    lines.append("")
    lines.append("SYNC")
    kalshi_ts = sync_status.get("last_kalshi_ts")
    forecast_ts = sync_status.get("last_forecast_ts")
    lines.append(f"  Last Kalshi sync:   {_fmt_ts(kalshi_ts)}  ({_age_label(kalshi_ts)})")
    lines.append(f"  Last forecast sync: {_fmt_ts(forecast_ts)}  ({_age_label(forecast_ts)})")
    ks_label = "ON" if kill_switch else "OFF"
    lines.append(f"  Kill switch:        {ks_label}")

    # BOARD
    lines.append("")
    lines.append("BOARD (top candidates, today)")
    if not board_rows:
        lines.append("  [empty -- board not synced for today]")
    else:
        top = board_rows[:5]
        for row in top:
            edge = _fmt_float(row.get("edge_vs_ask"), 2)
            fair = _fmt_float(row.get("fair_prob"), 3)
            mkt = _fmt_float(row.get("price_yes_ask"), 3)
            bucket = row.get("candidate_bucket", "")
            lines.append(
                f"  {row.get('market_ticker', ''):<40}  fair={fair}  mkt={mkt}  edge={edge}  [{bucket}]"
            )

    # OPEN BETS
    lines.append("")
    n_open = len(open_bets)
    lines.append(f"OPEN BETS ({n_open})")
    if not open_bets:
        lines.append("  [no open bets]")
    else:
        for bet in open_bets:
            side = "YES" if bet.get("side") == "BUY_YES" else "NO"
            edge_str = _fmt_float(bet.get("expected_edge"), 2) if bet.get("expected_edge") is not None else "N/A"
            placed = _fmt_ts(bet.get("created_at_utc"))[:10]
            notional = _fmt_dollars(bet.get("notional_dollars"))
            lines.append(
                f"  {bet.get('market_ticker', ''):<42} {side:<4} {notional:<8}  edge={edge_str}  status=open  placed={placed}"
            )

    # P&L
    lines.append("")
    lines.append("P&L SUMMARY")
    lines.append(f"  Settled bets:     {pnl.get('total_settled', 0)}")
    wins = pnl.get("wins", 0)
    losses = pnl.get("losses", 0)
    if wins == 0 and losses == 0:
        lines.append(f"  Wins / Losses:    0 / 0  (outcome tracking incomplete)")
    else:
        lines.append(f"  Wins / Losses:    {wins} / {losses}")
    lines.append(f"  Realized P&L:     {_fmt_dollars(pnl.get('realized_pnl'))}")

    # CALIBRATION note
    lines.append("")
    lines.append("CALIBRATION")
    settled_with_outcomes = wins + losses
    if settled_with_outcomes == 0:
        lines.append("  [no settled bets with outcome labels -- calibration unavailable]")
    else:
        win_rate = wins / settled_with_outcomes if settled_with_outcomes > 0 else 0
        lines.append(f"  Win rate: {win_rate * 100:.0f}%  ({settled_with_outcomes} settled with outcomes)")
        lines.append("  Run: make chief -- calibration  for full report")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Board
# ---------------------------------------------------------------------------

def format_board(board_rows: list[dict[str, Any]], target_date: str) -> str:
    lines = [f"=== BOARD -- {target_date} ==="]
    if not board_rows:
        lines.append("  [No board data. Run: make fetch-live]")
        return "\n".join(lines)

    headers = ["RANK", "CITY", "TICKER", "FAIR", "MKT", "EDGE", "BUCKET"]
    rows = []
    for row in board_rows:
        rows.append([
            str(row.get("candidate_rank") or ""),
            str(row.get("city_id") or "").upper(),
            str(row.get("market_ticker") or ""),
            _fmt_float(row.get("fair_prob"), 3),
            _fmt_float(row.get("price_yes_ask"), 3),
            _fmt_float(row.get("edge_vs_ask"), 3),
            str(row.get("candidate_bucket") or ""),
        ])
    lines.append(_table(headers, rows))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Bets
# ---------------------------------------------------------------------------

def format_bets(bets: list[dict[str, Any]], status: str) -> str:
    lines = [f"=== BETS ({status}) ==="]
    if not bets:
        lines.append(f"  [no {status} bets]")
        return "\n".join(lines)

    if status == "open":
        headers = ["paper_bet_id", "ticker", "side", "notional", "edge", "status", "placed"]
        rows = []
        for bet in bets:
            side = "YES" if bet.get("side") == "BUY_YES" else "NO"
            rows.append([
                str(bet.get("paper_bet_id") or "")[:12],
                str(bet.get("market_ticker") or ""),
                side,
                _fmt_dollars(bet.get("notional_dollars")),
                _fmt_float(bet.get("expected_edge"), 2),
                str(bet.get("status") or ""),
                _fmt_ts(bet.get("created_at_utc"))[:10],
            ])
    else:
        headers = ["paper_bet_id", "ticker", "side", "notional", "outcome", "pnl", "closed"]
        rows = []
        for bet in bets:
            side = "YES" if bet.get("side") == "BUY_YES" else "NO"
            rows.append([
                str(bet.get("paper_bet_id") or "")[:12],
                str(bet.get("market_ticker") or ""),
                side,
                _fmt_dollars(bet.get("notional_dollars")),
                str(bet.get("outcome_label") or "pending"),
                _fmt_dollars(bet.get("realized_pnl")),
                _fmt_ts(bet.get("closed_at_utc"))[:10],
            ])
    lines.append(_table(headers, rows))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------

def format_calibration(
    by_city: list[dict[str, Any]],
    by_edge: list[dict[str, Any]],
    days: int,
) -> str:
    total_bets = sum(int(r.get("bet_count") or 0) for r in by_city)
    lines = ["=== CALIBRATION REPORT ==="]
    lines.append(f"  Period: last {days} days")
    lines.append(f"  Settled bets with outcomes: {total_bets}")

    if not by_city and not by_edge:
        lines.append("")
        lines.append("  [no settled bets with outcome labels]")
        return "\n".join(lines)

    if by_city:
        lines.append("")
        lines.append("  BY CITY:")
        headers = ["City", "Bets", "Win%", "Avg Edge", "Avg PnL", "Total PnL"]
        rows = []
        for r in by_city:
            rows.append([
                str(r.get("city_id") or "").upper(),
                str(int(r.get("bet_count") or 0)),
                _fmt_pct(r.get("win_rate")),
                _fmt_float(r.get("avg_edge"), 3),
                _fmt_dollars(r.get("avg_pnl")),
                _fmt_dollars(r.get("total_pnl")),
            ])
        for line in _table(headers, rows).splitlines():
            lines.append("  " + line)

    if by_edge:
        lines.append("")
        lines.append("  BY EDGE BAND:")
        headers = ["Edge", "Bets", "Win%", "Expected Win%", "Delta", "Total PnL"]
        rows = []
        for r in by_edge:
            actual_wr = float(r.get("win_rate") or 0)
            exp_wr = float(r.get("expected_win_rate") or 0.5)
            delta = actual_wr - exp_wr
            delta_str = f"{delta * 100:+.0f}%"
            rows.append([
                str(r.get("edge_band") or ""),
                str(int(r.get("bet_count") or 0)),
                _fmt_pct(r.get("win_rate")),
                _fmt_pct(r.get("expected_win_rate")),
                delta_str,
                _fmt_dollars(r.get("total_pnl")),
            ])
        for line in _table(headers, rows).splitlines():
            lines.append("  " + line)

    # Insight generation
    insights = _generate_calibration_insights(by_city, by_edge)
    if insights:
        lines.append("")
        for insight in insights:
            lines.append(f"  INSIGHT: {insight}")

    return "\n".join(lines)


def _generate_calibration_insights(
    by_city: list[dict[str, Any]],
    by_edge: list[dict[str, Any]],
) -> list[str]:
    insights = []
    for r in by_city:
        win_rate = float(r.get("win_rate") or 0)
        count = int(r.get("bet_count") or 0)
        city = str(r.get("city_id") or "").upper()
        if count >= 3 and win_rate < 0.35:
            insights.append(
                f"Model is weak on {city} (win rate {win_rate * 100:.0f}%, n={count}). "
                "Revisit station alignment."
            )
    for r in by_edge:
        band = str(r.get("edge_band") or "")
        actual_wr = float(r.get("win_rate") or 0)
        exp_wr = float(r.get("expected_win_rate") or 0.5)
        count = int(r.get("bet_count") or 0)
        delta = actual_wr - exp_wr
        if band == "0.20+" and count >= 3 and delta < -0.20:
            insights.append(
                f"Edge >0.20 may not be real (actual {actual_wr * 100:.0f}% vs expected {exp_wr * 100:.0f}%, "
                f"n={count}). Recommend paper-only until n>=20."
            )
    return insights


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def format_sync_result(result: dict[str, Any]) -> str:
    lines = []
    kalshi = result.get("kalshi", {})
    forecasts = result.get("forecasts", {})
    board = result.get("board", {})

    if kalshi.get("error"):
        lines.append(f"Syncing Kalshi markets...  ERROR  ({kalshi['error']})")
    else:
        contracts = kalshi.get("contracts_synced", 0)
        new_c = kalshi.get("new_contracts", 0)
        lines.append(f"Syncing Kalshi markets...  OK  ({contracts} contracts, {new_c} new)")

    if forecasts.get("error"):
        lines.append(f"Syncing forecasts...       ERROR  ({forecasts['error']})")
    else:
        cities = forecasts.get("cities_updated", 0)
        lines.append(f"Syncing forecasts...       OK  ({cities} cities updated)")

    if board.get("error"):
        lines.append(f"Rematerializing board...   ERROR  ({board['error']})")
    else:
        size = board.get("board_size", 0)
        date_label = board.get("date", "today")
        lines.append(f"Rematerializing board...   OK  (board refreshed for {date_label})")
        lines.append("")
        lines.append(f"Board now has {size} candidates. Run: make chief -- board")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Settle
# ---------------------------------------------------------------------------

def format_settle_result(
    results: list[dict[str, Any]],
    settled_count: int,
    open_count: int,
) -> str:
    total = len(results)
    lines = [f"Checking {total} open bets for settlement..."]
    for r in results:
        ticker = r.get("market_ticker", "")
        if r.get("settled"):
            obs = r.get("observed_value")
            threshold = r.get("threshold_display", "")
            outcome = r.get("outcome_label", "")
            pnl = _fmt_dollars(r.get("realized_pnl"))
            result_str = "YES" if outcome == "YES" else "NO"
            lines.append(
                f"  {ticker}: settlement={obs}F  threshold={threshold}  result={result_str}  PnL={pnl}  -> CLOSED {'WIN' if r.get('won') else 'LOSS'}"
            )
        elif r.get("error"):
            lines.append(f"  {ticker}: ERROR -- {r['error']}")
        else:
            lines.append(f"  {ticker}: no settlement data yet")

    lines.append("")
    lines.append(f"{settled_count} bet(s) settled. {open_count} still open.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Kill switch
# ---------------------------------------------------------------------------

def format_killswitch(old_state: bool, new_state: bool) -> str:
    old_label = "ON" if old_state else "OFF"
    new_label = "ON" if new_state else "OFF"
    lines = [f"Kill switch: {old_label} -> {new_label}"]
    if new_state:
        lines.append("Auto-betting is now DISABLED.")
    else:
        lines.append("Auto-betting is now ENABLED. Use with caution.")
    return "\n".join(lines)


def format_killswitch_status(state: bool) -> str:
    label = "ON" if state else "OFF"
    return f"Kill switch is currently: {label}"
