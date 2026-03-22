from __future__ import annotations

import re
from datetime import UTC, date, datetime
from pathlib import Path

from .settlement_observations import ingest_settlement_observation

REPORT_DATE_RE = re.compile(r'CLIMATE SUMMARY FOR\s+([A-Z]+)\s+(\d{1,2})\s+(\d{4})', re.IGNORECASE)
MAX_TEMP_RE = re.compile(r'^\s*MAXIMUM\s+(\d+(?:\.\d+)?)', re.MULTILINE)
MIN_TEMP_RE = re.compile(r'^\s*MINIMUM\s+(\d+(?:\.\d+)?)', re.MULTILINE)
PRECIP_RE = re.compile(r'^\s*YESTERDAY\s+(\d+(?:\.\d+)?)\s*$', re.MULTILINE)
PUBLISHED_RE = re.compile(r'^(\d{3,4}\s+[AP]M\s+[A-Z]{2,4}\s+\w+\s+' +
                          r'[A-Z]{3}\s+\d{1,2}\s+\d{4})$', re.MULTILINE)

MONTHS = {
    'JANUARY': 1, 'FEBRUARY': 2, 'MARCH': 3, 'APRIL': 4, 'MAY': 5, 'JUNE': 6,
    'JULY': 7, 'AUGUST': 8, 'SEPTEMBER': 9, 'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12,
}


def parse_nws_cli_text(text: str) -> dict:
    report_date_match = REPORT_DATE_RE.search(text)
    if not report_date_match:
        raise ValueError('Could not find climate summary date in NWS CLI text')

    month_name, day_str, year_str = report_date_match.groups()
    market_date_local = date(int(year_str), MONTHS[month_name.upper()], int(day_str))

    max_match = MAX_TEMP_RE.search(text)
    min_match = MIN_TEMP_RE.search(text)
    precip_matches = PRECIP_RE.findall(text)
    precip_value = float(precip_matches[-1]) if precip_matches else None

    published_match = PUBLISHED_RE.search(text)
    published_at = None
    if published_match:
        raw = published_match.group(1)
        try:
            published_at = datetime.strptime(raw.replace('EDT', '').replace('EST', '').strip(), '%I%M %p %a %b %d %Y').replace(tzinfo=UTC)
        except ValueError:
            published_at = None

    return {
        'market_date_local': market_date_local,
        'observed_high_temp_f': float(max_match.group(1)) if max_match else None,
        'observed_low_temp_f': float(min_match.group(1)) if min_match else None,
        'observed_precip_in': precip_value,
        'report_published_at_utc': published_at,
    }


def ingest_nws_cli_text(*, text: str, station_id: str, city_id: str, db_path: str | Path | None = None) -> str:
    parsed = parse_nws_cli_text(text)
    return ingest_settlement_observation(
        source='nws-cli',
        station_id=station_id,
        city_id=city_id,
        market_date_local=parsed['market_date_local'],
        observed_high_temp_f=parsed['observed_high_temp_f'],
        observed_low_temp_f=parsed['observed_low_temp_f'],
        observed_precip_in=parsed['observed_precip_in'],
        report_published_at_utc=parsed['report_published_at_utc'],
        is_final=True,
        db_path=db_path,
    )
