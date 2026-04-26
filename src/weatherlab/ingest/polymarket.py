"""Polymarket public API client — read-only market data, no auth required."""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

_GAMMA_BASE = 'https://gamma-api.polymarket.com'
_CLOB_BASE = 'https://clob.polymarket.com'
_REQUEST_TIMEOUT = 10.0
_PAGE_SIZE = 500


def _get(url: str, params: dict | None = None) -> Any:
    resp = requests.get(url, params=params, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_open_markets(
    limit: int = _PAGE_SIZE,
    active: bool = True,
) -> list[dict]:
    """
    Fetch open Polymarket markets from the Gamma API.
    Returns a flat list of market dicts each containing:
      question, conditionId, outcomePrices, volume, endDate, slug, id
    """
    markets: list[dict] = []
    offset = 0
    while True:
        try:
            batch = _get(
                f'{_GAMMA_BASE}/markets',
                params={
                    'active': str(active).lower(),
                    'closed': 'false',
                    'limit': min(limit, _PAGE_SIZE),
                    'offset': offset,
                },
            )
        except requests.RequestException as exc:
            logger.warning('Polymarket fetch failed at offset %d: %s', offset, exc)
            break
        if not batch:
            break
        markets.extend(batch)
        if len(batch) < _PAGE_SIZE:
            break
        offset += len(batch)
        if len(markets) >= limit:
            break
    return markets[:limit]


def yes_price_from_market(market: dict) -> float | None:
    """
    Extract the YES price from a Polymarket market dict.
    outcomePrices is a JSON-encoded list like '["0.73", "0.27"]'
    Index 0 = YES, index 1 = NO.
    """
    raw = market.get('outcomePrices')
    if not raw:
        return None
    try:
        import json
        prices = json.loads(raw) if isinstance(raw, str) else list(raw)
        return float(prices[0])
    except (ValueError, IndexError, TypeError):
        return None


def volume_from_market(market: dict) -> float | None:
    v = market.get('volume')
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def days_to_close(market: dict, now: datetime | None = None) -> float | None:
    end = market.get('endDate') or market.get('end_date_iso')
    if not end:
        return None
    try:
        end_dt = datetime.fromisoformat(str(end).replace('Z', '+00:00'))
        ref = now or datetime.now(UTC)
        delta = (end_dt - ref).total_seconds() / 86400
        return round(delta, 1)
    except (ValueError, TypeError):
        return None
