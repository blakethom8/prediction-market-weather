"""Settle the 21 open paper bets from March 23, 2026.

Uses known actual highs from Kalshi settlement data and calibration_log.
"""
from __future__ import annotations

import json
import hashlib
from datetime import datetime, UTC

import duckdb

DB_PATH = 'data/warehouse/weather_markets.duckdb'

# Known actual highs for March 23, 2026 (from Kalshi settlement / calibration_log)
ACTUAL_HIGHS = {
    'KXHIGHTDC': 67.5,   # DC: ~67.5°F
    'KXHIGHPHIL': 58.5,  # Philadelphia: ~58.5°F
    'KXHIGHTBOS': 38.5,  # Boston: ~38.5°F
    'KXHIGHTHOU': 83.5,  # Houston: ~83.5°F
    'KXHIGHMIA': 81.5,   # Miami: ~81.5°F
    'KXHIGHNY': 54.0,    # New York: ~54°F
    'KXHIGHTSEA': 52.0,  # Seattle: estimate ~52°F
}


def _parse_ticker(ticker: str) -> tuple[str, str]:
    """Returns (series_ticker, bucket_code) from e.g. KXHIGHTDC-26MAR23-T67."""
    parts = ticker.split('-')
    series = parts[0]
    bucket = parts[-1] if len(parts) >= 3 else ''
    return series, bucket


def _evaluate_outcome(ticker: str, side: str) -> tuple[str, bool]:
    """Determine market outcome (yes/no) and whether this bet won.

    Returns (market_outcome, bet_won).
    """
    series, bucket = _parse_ticker(ticker)
    actual_high = ACTUAL_HIGHS.get(series)
    if actual_high is None:
        # Unknown city — cannot settle
        return 'unknown', False

    # Parse bucket code: T67 means >=67, B79.5 means <79.5 (between/below)
    if bucket.startswith('T'):
        threshold = float(bucket[1:])
        # T{n} = "above n°F" — YES if actual >= threshold (Kalshi uses strict >)
        # Kalshi typically settles YES if actual > threshold
        market_outcome = 'yes' if actual_high > threshold else 'no'
    elif bucket.startswith('B'):
        threshold = float(bucket[1:])
        # B{n} = "below n°F" — YES if actual < threshold
        market_outcome = 'yes' if actual_high < threshold else 'no'
    else:
        return 'unknown', False

    # All paper bets were side=YES, so bet_won = (market_outcome == 'yes')
    bet_won = (market_outcome == side.lower())
    return market_outcome, bet_won


def _compute_pnl(bet_won: bool, quantity: float, limit_price: float) -> float:
    """Compute realized PnL for a paper bet.

    If won: payout ($1 per contract) minus cost
    If lost: negative cost
    """
    cost = round(quantity * limit_price, 4)
    if bet_won:
        return round(quantity * 1.0 - cost, 4)
    else:
        return round(-cost, 4)


def main():
    con = duckdb.connect(DB_PATH)

    # Fetch all open paper bets
    rows = con.execute(
        "SELECT * FROM ops.paper_bets WHERE status = 'open' ORDER BY market_ticker"
    ).fetchdf()

    print(f'Found {len(rows)} open paper bets to settle.\n')

    total_pnl = 0.0
    wins = 0
    losses = 0
    now_utc = datetime.now(UTC).replace(tzinfo=None)

    for _, row in rows.iterrows():
        paper_bet_id = row['paper_bet_id']
        ticker = row['market_ticker']
        side = row['side']
        limit_price = float(row['limit_price'])
        quantity = float(row['quantity'])
        notional = float(row['notional_dollars'])

        market_outcome, bet_won = _evaluate_outcome(ticker, side)
        if market_outcome == 'unknown':
            print(f'  SKIP {ticker}: unknown actual high for this city')
            continue

        pnl = _compute_pnl(bet_won, quantity, limit_price)
        total_pnl += pnl
        outcome_label = market_outcome
        status = 'closed'

        if bet_won:
            wins += 1
            result_str = 'WON'
        else:
            losses += 1
            result_str = 'LOST'

        print(f'  {ticker} ({side} @ {limit_price:.2f} x{int(quantity)}): '
              f'market={market_outcome.upper()} → {result_str} | PnL: ${pnl:+.4f}')

        # Update paper bet
        con.execute(
            '''
            UPDATE ops.paper_bets
            SET status = ?,
                outcome_label = ?,
                realized_pnl = ?,
                closed_at_utc = ?
            WHERE paper_bet_id = ?
            ''',
            [status, outcome_label, pnl, now_utc, paper_bet_id],
        )

        # Insert review into ops.paper_bet_reviews
        series, bucket = _parse_ticker(ticker)
        actual_high = ACTUAL_HIGHS.get(series, 'unknown')
        lesson = (
            f'Market settled {market_outcome.upper()}. '
            f'Actual high ~{actual_high}°F vs bucket {bucket}. '
            f'Bet {"won" if bet_won else "lost"} — PnL ${pnl:+.4f}.'
        )
        review_id = 'review_' + hashlib.md5(
            f'{paper_bet_id}-{now_utc.isoformat()}'.encode()
        ).hexdigest()[:12]

        con.execute(
            '''
            INSERT INTO ops.paper_bet_reviews (
                review_id, paper_bet_id, proposal_id, strategy_id,
                reviewed_at_utc, kalshi_outcome_label, realized_pnl,
                lesson_summary, review_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                review_id,
                paper_bet_id,
                None,
                row['strategy_id'],
                now_utc,
                market_outcome,
                pnl,
                lesson,
                json.dumps({
                    'actual_high_f': actual_high,
                    'bucket_code': bucket,
                    'bet_won': bet_won,
                }),
            ],
        )

    con.close()

    print(f'\n--- Summary ---')
    print(f'Settled: {wins + losses} / {len(rows)} paper bets')
    print(f'Wins: {wins}  Losses: {losses}')
    print(f'Total Paper PnL: ${total_pnl:+.4f}')


if __name__ == '__main__':
    main()
