from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional

CITY_ALIASES = {
    'new york': 'nyc',
    'nyc': 'nyc',
    'chicago': 'chi',
    'miami': 'mia',
    'los angeles': 'lax',
    'la': 'lax',
    'dallas': 'dal',
}


@dataclass
class ParsedContract:
    market_ticker: str
    city_id: Optional[str]
    measure: Optional[str]
    operator: Optional[str]
    threshold_low_f: Optional[float]
    threshold_high_f: Optional[float]
    parse_status: str
    parse_confidence: float
    parse_notes: Optional[str] = None


def parse_temperature_contract(market_ticker: str, title: str) -> ParsedContract:
    lowered = title.lower()
    city_id = None
    for alias, canonical in CITY_ALIASES.items():
        if alias in lowered:
            city_id = canonical
            break

    measure = 'daily_high_temp_f' if 'high temp' in lowered or 'high temperature' in lowered else None

    m_between = re.search(r'between\s+(\d+(?:\.\d+)?)\D+(\d+(?:\.\d+)?)', lowered)
    if m_between:
        return ParsedContract(
            market_ticker=market_ticker,
            city_id=city_id,
            measure=measure,
            operator='between',
            threshold_low_f=float(m_between.group(1)),
            threshold_high_f=float(m_between.group(2)),
            parse_status='parsed' if city_id and measure else 'partial',
            parse_confidence=0.9 if city_id and measure else 0.5,
        )

    m_ge = re.search(r'(?:above|over|greater than|>=?|hit)\s+(\d+(?:\.\d+)?)', lowered)
    if m_ge:
        return ParsedContract(
            market_ticker=market_ticker,
            city_id=city_id,
            measure=measure,
            operator='>=',
            threshold_low_f=float(m_ge.group(1)),
            threshold_high_f=None,
            parse_status='parsed' if city_id and measure else 'partial',
            parse_confidence=0.85 if city_id and measure else 0.45,
        )

    return ParsedContract(
        market_ticker=market_ticker,
        city_id=city_id,
        measure=measure,
        operator=None,
        threshold_low_f=None,
        threshold_high_f=None,
        parse_status='failed',
        parse_confidence=0.0,
        parse_notes='No supported threshold pattern matched',
    )
