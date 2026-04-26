#!/usr/bin/env python3
"""
arb_scan.py — Cross-platform arbitrage scanner: Kalshi vs Polymarket.

Fetches open markets from both platforms, matches them by keyword,
and flags pairs where the YES price differs by more than a threshold (default 5¢).

No auth required for either platform in read-only mode.
No recommendations generated — surfaces candidates for human review.

Usage:
    python scripts/arb_scan.py [--min-diff 0.05] [--min-volume 500] [--notify]
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from weatherlab.ingest.kalshi_live import KalshiClient, KalshiClientError
from weatherlab.ingest.polymarket import (
    days_to_close,
    fetch_open_markets,
    volume_from_market,
    yes_price_from_market,
)

# ── config ────────────────────────────────────────────────────────────────────
DEFAULT_MIN_DIFF = 0.05     # 5¢ minimum price divergence to flag
DEFAULT_MIN_VOLUME = 500    # minimum volume on Polymarket side
MAX_POLYMARKET_FETCH = 2000 # cap to avoid hammering the API

# Keywords that help match Kalshi tickers to Polymarket questions
# Format: (kalshi_series_prefix, [polymarket_keywords])
MATCH_HINTS: list[tuple[str, list[str]]] = [
    ('KXCPI',      ['CPI', 'consumer price', 'inflation']),
    ('KXGDP',      ['GDP', 'gross domestic product']),
    ('KXFED',      ['federal reserve', 'fed rate', 'FOMC', 'interest rate']),
    ('KXUNRATE',   ['unemployment rate', 'jobless']),
    ('KXNFP',      ['nonfarm payroll', 'jobs report', 'payrolls']),
    ('KXPCE',      ['PCE', 'personal consumption']),
    ('KXHIGH',     ['temperature', 'high temp', 'weather']),
]


# ── matching ──────────────────────────────────────────────────────────────────

def _kalshi_series(ticker: str) -> str:
    """Extract the series prefix from a Kalshi ticker, e.g. KXCPI from KXCPI-26MAR-T0.8"""
    return ticker.split('-')[0] if '-' in ticker else ticker


def _keywords_for_series(series: str) -> list[str]:
    for prefix, keywords in MATCH_HINTS:
        if series.startswith(prefix):
            return keywords
    return []


def _question_matches(question: str, keywords: list[str]) -> bool:
    q = question.lower()
    return any(kw.lower() in q for kw in keywords)


def match_markets(
    kalshi_markets: list[dict],
    poly_markets: list[dict],
) -> list[dict]:
    """
    For each Kalshi market, find Polymarket markets whose question matches
    on shared keywords. Returns a list of candidate pairs.
    """
    pairs: list[dict] = []
    for km in kalshi_markets:
        ticker = str(km.get('ticker') or '')
        series = _kalshi_series(ticker)
        keywords = _keywords_for_series(series)
        if not keywords:
            continue
        k_yes = _normalize_price(km.get('yes_ask'))
        if k_yes is None:
            continue
        for pm in poly_markets:
            question = str(pm.get('question') or '')
            if not _question_matches(question, keywords):
                continue
            p_yes = yes_price_from_market(pm)
            if p_yes is None:
                continue
            p_vol = volume_from_market(pm) or 0.0
            pairs.append({
                'kalshi_ticker': ticker,
                'kalshi_yes': round(k_yes, 4),
                'poly_slug': str(pm.get('slug') or pm.get('id') or ''),
                'poly_question': question,
                'poly_yes': round(p_yes, 4),
                'poly_volume': p_vol,
                'diff': round(abs(k_yes - p_yes), 4),
                'diff_direction': 'kalshi_higher' if k_yes > p_yes else 'poly_higher',
                'days_to_close': days_to_close(pm),
            })
    return pairs


def filter_arb_candidates(
    pairs: list[dict],
    min_diff: float = DEFAULT_MIN_DIFF,
    min_volume: float = DEFAULT_MIN_VOLUME,
) -> list[dict]:
    return [
        p for p in pairs
        if p['diff'] >= min_diff and p['poly_volume'] >= min_volume
    ]


def _normalize_price(value) -> float | None:
    if value in (None, ''):
        return None
    try:
        p = float(value)
        return p / 100.0 if p > 1.0 else p
    except (ValueError, TypeError):
        return None


# ── formatting ────────────────────────────────────────────────────────────────

def _fmt_price(p: float | None) -> str:
    if p is None:
        return 'n/a'
    return f'{round(p * 100)}c'


def _fmt_vol(v: float | None) -> str:
    if v is None:
        return 'n/a'
    return f'{int(v):,}'


def format_arb_report(
    candidates: list[dict],
    scan_time_utc: datetime,
    min_diff: float = DEFAULT_MIN_DIFF,
    min_volume: float = DEFAULT_MIN_VOLUME,
) -> str:
    lines = [
        f'⚡ KALSHI vs POLYMARKET ARB SCAN - {scan_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        f'Filter: diff >= {round(min_diff * 100)}c | poly_volume >= {int(min_volume):,}',
        'Human review only. No recommendations generated.',
        '',
    ]
    if not candidates:
        lines.append('No arbitrage candidates found above threshold.')
        return '\n'.join(lines)

    sorted_candidates = sorted(candidates, key=lambda r: r['diff'], reverse=True)
    for c in sorted_candidates:
        direction = '📈 Kalshi higher' if c['diff_direction'] == 'kalshi_higher' else '📉 Poly higher'
        dtc = f"{c['days_to_close']:.1f}d" if c.get('days_to_close') is not None else 'unknown'
        lines += [
            f"  {direction} | diff={_fmt_price(c['diff'])}",
            f"  Kalshi: {c['kalshi_ticker']} YES={_fmt_price(c['kalshi_yes'])}",
            f"  Poly:   {c['poly_slug']} YES={_fmt_price(c['poly_yes'])} | vol={_fmt_vol(c['poly_volume'])} | closes in {dtc}",
            f"  Q: {c['poly_question'][:100]}",
            '',
        ]
    return '\n'.join(lines).rstrip()


# ── main ─────────────────────────────────────────────────────────────────────

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Kalshi vs Polymarket arbitrage scanner')
    p.add_argument('--min-diff', type=float, default=DEFAULT_MIN_DIFF,
                   help=f'Minimum price divergence to flag (default {DEFAULT_MIN_DIFF})')
    p.add_argument('--min-volume', type=float, default=DEFAULT_MIN_VOLUME,
                   help=f'Minimum Polymarket volume (default {DEFAULT_MIN_VOLUME})')
    p.add_argument('--notify', action='store_true',
                   help='Send report via openclaw system event (suppressed if no candidates)')
    return p.parse_args(argv)


def _send_notification(text: str) -> int:
    result = subprocess.run(
        ['openclaw', 'system', 'event', '--text', text, '--mode', 'now'],
        check=False,
    )
    return result.returncode


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    scan_time = datetime.now(UTC)

    # Fetch Kalshi markets
    try:
        kalshi_markets = KalshiClient(timeout_seconds=10.0).fetch_open_weather_markets()
        # Also fetch non-weather Kalshi markets via direct API call
        raw_all = KalshiClient(timeout_seconds=10.0)._request_json(
            'GET', '/markets', params={'status': 'open', 'limit': 1000}
        )
        all_kalshi = raw_all.get('markets', []) if isinstance(raw_all, dict) else []
    except (KalshiClientError, Exception) as exc:
        print(f'Kalshi fetch failed: {exc}', file=sys.stderr)
        all_kalshi = []

    # Fetch Polymarket markets
    try:
        poly_markets = fetch_open_markets(limit=MAX_POLYMARKET_FETCH)
    except Exception as exc:
        print(f'Polymarket fetch failed: {exc}', file=sys.stderr)
        poly_markets = []

    pairs = match_markets(all_kalshi or kalshi_markets, poly_markets)
    candidates = filter_arb_candidates(pairs, min_diff=args.min_diff, min_volume=args.min_volume)

    report = format_arb_report(
        candidates,
        scan_time_utc=scan_time,
        min_diff=args.min_diff,
        min_volume=args.min_volume,
    )
    print(report)

    if args.notify:
        if not candidates:
            print('No arb candidates — notification suppressed.')
            return 0
        return _send_notification(report)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
