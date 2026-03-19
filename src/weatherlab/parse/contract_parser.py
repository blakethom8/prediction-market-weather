from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

CITY_ALIASES = {
    'new york city': 'nyc',
    'new york': 'nyc',
    'nyc': 'nyc',
    'chicago': 'chi',
    'miami': 'mia',
    'los angeles': 'lax',
    'la': 'lax',
    'dallas': 'dal',
    'austin': 'aus',
    'denver': 'den',
    'philadelphia': 'phl',
    'philly': 'phl',
    'houston': 'hou',
}

MONTHS_PATTERN = r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*'
DATE_PATTERNS = [
    re.compile(rf'\bon\s+({MONTHS_PATTERN}\s+\d{{1,2}}(?:,\s*\d{{4}})?)\b', re.IGNORECASE),
    re.compile(rf'\bfor\s+({MONTHS_PATTERN}\s+\d{{1,2}}(?:,\s*\d{{4}})?)\b', re.IGNORECASE),
]
DATE_FORMATS = [
    '%b %d, %Y',
    '%B %d, %Y',
    '%b %d %Y',
    '%B %d %Y',
    '%b %d',
    '%B %d',
]


@dataclass
class ParsedContract:
    market_ticker: str
    city_id: Optional[str]
    measure: Optional[str]
    operator: Optional[str]
    threshold_low_f: Optional[float]
    threshold_high_f: Optional[float]
    market_date_local: Optional[date]
    parse_status: str
    parse_confidence: float
    parse_notes: Optional[str] = None


def _extract_city_id(text: str) -> Optional[str]:
    for alias, canonical in sorted(CITY_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if alias in text:
            return canonical
    return None


def _extract_market_date(text: str) -> Optional[date]:
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        raw = re.sub(r'\s+', ' ', match.group(1).replace(',', ', ')).strip()
        raw = re.sub(r',\s+', ', ', raw)
        for fmt in DATE_FORMATS:
            try:
                parsed = datetime.strptime(raw, fmt)
                if '%Y' not in fmt:
                    parsed = parsed.replace(year=date.today().year)
                return parsed.date()
            except ValueError:
                continue
    return None


def _build_result(
    *,
    market_ticker: str,
    city_id: Optional[str],
    measure: Optional[str],
    operator: Optional[str],
    threshold_low_f: Optional[float],
    threshold_high_f: Optional[float],
    market_date_local: Optional[date],
    confidence: float,
    notes: Optional[str] = None,
) -> ParsedContract:
    if city_id and measure and operator and threshold_low_f is not None:
        status = 'parsed'
    elif city_id or measure or operator or threshold_low_f is not None:
        status = 'partial'
    else:
        status = 'failed'
    return ParsedContract(
        market_ticker=market_ticker,
        city_id=city_id,
        measure=measure,
        operator=operator,
        threshold_low_f=threshold_low_f,
        threshold_high_f=threshold_high_f,
        market_date_local=market_date_local,
        parse_status=status,
        parse_confidence=confidence if status != 'failed' else 0.0,
        parse_notes=notes,
    )


def parse_temperature_contract(market_ticker: str, title: str) -> ParsedContract:
    lowered = title.lower()
    city_id = _extract_city_id(lowered)
    market_date_local = _extract_market_date(title)
    measure = 'daily_high_temp_f' if 'high temp' in lowered or 'high temperature' in lowered else None

    range_patterns = [
        re.compile(r'\bbe\s+(\d+(?:\.\d+)?)\s*(?:to|\-|–)\s*(\d+(?:\.\d+)?)\b', re.IGNORECASE),
        re.compile(r'\bbetween\s+(\d+(?:\.\d+)?)\D+(\d+(?:\.\d+)?)\b', re.IGNORECASE),
    ]
    for pattern in range_patterns:
        match = pattern.search(title)
        if match:
            return _build_result(
                market_ticker=market_ticker,
                city_id=city_id,
                measure=measure,
                operator='between',
                threshold_low_f=float(match.group(1)),
                threshold_high_f=float(match.group(2)),
                market_date_local=market_date_local,
                confidence=0.93 if city_id and measure else 0.55,
            )

    ge_patterns = [
        re.compile(r'\b(?:above|over|greater than|at least)\s+(\d+(?:\.\d+)?)\b', re.IGNORECASE),
        re.compile(r'\b(\d+(?:\.\d+)?)\s*(?:°|degrees?)?\s+or\s+higher\b', re.IGNORECASE),
        re.compile(r'>\s*(\d+(?:\.\d+)?)'),
        re.compile(r'\bhit\s+(\d+(?:\.\d+)?)\b', re.IGNORECASE),
    ]
    for pattern in ge_patterns:
        match = pattern.search(title)
        if match:
            return _build_result(
                market_ticker=market_ticker,
                city_id=city_id,
                measure=measure,
                operator='>=',
                threshold_low_f=float(match.group(1)),
                threshold_high_f=None,
                market_date_local=market_date_local,
                confidence=0.89 if city_id and measure else 0.5,
            )

    le_patterns = [
        re.compile(r'\b(?:below|under|less than|at most)\s+(\d+(?:\.\d+)?)\b', re.IGNORECASE),
        re.compile(r'\b(\d+(?:\.\d+)?)\s*(?:°|degrees?)?\s+or\s+lower\b', re.IGNORECASE),
        re.compile(r'\b<\s*(\d+(?:\.\d+)?)\b'),
    ]
    for pattern in le_patterns:
        match = pattern.search(title)
        if match:
            return _build_result(
                market_ticker=market_ticker,
                city_id=city_id,
                measure=measure,
                operator='<=',
                threshold_low_f=float(match.group(1)),
                threshold_high_f=None,
                market_date_local=market_date_local,
                confidence=0.89 if city_id and measure else 0.5,
            )

    return _build_result(
        market_ticker=market_ticker,
        city_id=city_id,
        measure=measure,
        operator=None,
        threshold_low_f=None,
        threshold_high_f=None,
        market_date_local=market_date_local,
        confidence=0.0,
        notes='No supported threshold pattern matched',
    )
