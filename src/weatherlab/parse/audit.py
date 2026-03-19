from __future__ import annotations

from dataclasses import asdict
from typing import Iterable

from .contract_parser import parse_temperature_contract


def audit_titles(rows: Iterable[dict]) -> list[dict]:
    audited = []
    for row in rows:
        parsed = parse_temperature_contract(
            market_ticker=row.get('market_ticker', ''),
            title=row.get('title', ''),
        )
        audited.append({
            **row,
            **asdict(parsed),
        })
    return audited


def summarize_audit(rows: Iterable[dict]) -> dict:
    rows = list(rows)
    summary = {'parsed': 0, 'partial': 0, 'failed': 0, 'total': len(rows)}
    for row in rows:
        status = row.get('parse_status', 'failed')
        summary[status] = summary.get(status, 0) + 1
    return summary
