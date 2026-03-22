from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from ..db import connect
from ..settings import FOCUS_CITY_IDS
from .forecast_snapshots import ingest_forecast_snapshot
from .open_meteo import build_threshold_distribution

IEM_AFOS_URL = 'https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py'

ISSUED_RE = re.compile(
    r'^(\d{3,4}\s+[AP]M\s+[A-Z]{2,4}\s+\w{3}\s+\w{3}\s+\d{1,2}\s+\d{4})$',
    re.MULTILINE,
)
SECTION_RE = re.compile(
    r'^\.(?P<label>[A-Z /]+)\.\.\.(?P<body>.*?)(?=^\.[A-Z /]+\.\.\.|^\$\$|\Z)',
    re.MULTILINE | re.DOTALL,
)
HIGH_AROUND_RE = re.compile(r'highs?\s+(?:around|near)\s+(\d+)', re.IGNORECASE)
HIGH_BUCKET_RE = re.compile(r'highs?\s+in\s+the\s+([a-z\s]+?)\s+(\d{2})s', re.IGNORECASE)
TEMP_BUCKET_RE = re.compile(r'temperatures?\s+in\s+the\s+([a-z\s]+?)\s+(\d{2})s', re.IGNORECASE)

TZINFOS = {
    'UTC': timezone.utc,
    'EST': timezone(timedelta(hours=-5)),
    'EDT': timezone(timedelta(hours=-4)),
    'CST': timezone(timedelta(hours=-6)),
    'CDT': timezone(timedelta(hours=-5)),
}


@dataclass(frozen=True)
class ArchivedForecastConfig:
    city_id: str
    pil: str
    zone_label: str
    timezone_name: str


ARCHIVED_FORECAST_CONFIGS: dict[str, ArchivedForecastConfig] = {
    'nyc': ArchivedForecastConfig(
        city_id='nyc',
        pil='ZFPOKX',
        zone_label='New York (Manhattan)-',
        timezone_name='America/New_York',
    ),
    'chi': ArchivedForecastConfig(
        city_id='chi',
        pil='ZFPLOT',
        zone_label='Central Cook-',
        timezone_name='America/Chicago',
    ),
}


def _parse_issued_at_utc(text: str) -> datetime:
    match = ISSUED_RE.search(text)
    if not match:
        raise ValueError('Could not parse issued timestamp from archived forecast text')
    raw = match.group(1)
    parts = raw.split()
    tz_abbrev = parts[2]
    tzinfo = TZINFOS.get(tz_abbrev)
    if tzinfo is None:
        raise ValueError(f'Unsupported timezone abbreviation: {tz_abbrev}')
    dt = datetime.strptime(' '.join(parts[:2] + parts[3:]), '%I%M %p %a %b %d %Y')
    return dt.replace(tzinfo=tzinfo).astimezone(UTC)


def _split_products(raw_text: str) -> list[str]:
    parts = re.split(r'\x01', raw_text)
    products = [part.strip() for part in parts if part.strip()]
    return products


def _extract_zone_block(text: str, zone_label: str) -> str:
    for block in text.split('\n$$'):
        if zone_label in block:
            return block.strip()
    raise ValueError(f'Could not find zone block for {zone_label}')


def _label_candidates(*, target_date_local: date, issued_local_date: date) -> tuple[str, ...]:
    weekday = target_date_local.strftime('%A').upper()
    if target_date_local == issued_local_date:
        return (weekday, 'TODAY', 'THIS AFTERNOON', 'LATE THIS AFTERNOON', 'THIS MORNING')
    return (weekday,)


def _descriptor_to_temp(descriptor: str, decade: int) -> float:
    descriptor = descriptor.strip().lower().replace('-', ' ')
    mapping = {
        'lower': 2.0,
        'mid': 5.0,
        'upper': 8.0,
    }
    parts = [mapping[token] for token in descriptor.split() if token in mapping]
    if not parts:
        return float(decade)
    return decade + (sum(parts) / len(parts))


def _parse_high_temp_f(section_text: str) -> float | None:
    around_match = HIGH_AROUND_RE.search(section_text)
    if around_match:
        return float(around_match.group(1))

    bucket_match = HIGH_BUCKET_RE.search(section_text)
    if bucket_match:
        descriptor, decade = bucket_match.groups()
        return _descriptor_to_temp(descriptor, int(decade))

    temp_match = TEMP_BUCKET_RE.search(section_text)
    if temp_match:
        descriptor, decade = temp_match.groups()
        return _descriptor_to_temp(descriptor, int(decade))

    return None


def parse_archived_nws_zone_forecast(*, text: str, city_id: str, target_date_local: date, thresholds: list[float] | tuple[float, ...], sigma_f: float = 3.0) -> dict:
    config = ARCHIVED_FORECAST_CONFIGS[city_id]
    issued_at_utc = _parse_issued_at_utc(text)
    issued_local_date = issued_at_utc.astimezone(ZoneInfo(config.timezone_name)).date()
    zone_block = _extract_zone_block(text, config.zone_label)

    sections = {match.group('label').strip(): match.group('body').strip() for match in SECTION_RE.finditer(zone_block)}
    selected_text = None
    for label in _label_candidates(target_date_local=target_date_local, issued_local_date=issued_local_date):
        if label in sections:
            selected_text = sections[label]
            break

    if selected_text is None:
        raise ValueError(f'Could not find forecast section for {city_id} on {target_date_local}')

    pred_high_temp_f = _parse_high_temp_f(selected_text)
    distribution = {}
    if pred_high_temp_f is not None:
        distribution = build_threshold_distribution(
            point_temp_f=pred_high_temp_f,
            thresholds=thresholds,
            sigma_f=sigma_f,
        )

    return {
        'source': 'iem-zfp',
        'city_id': city_id,
        'issued_at_utc': issued_at_utc,
        'available_at_utc': issued_at_utc,
        'target_date_local': target_date_local,
        'pred_high_temp_f': pred_high_temp_f,
        'pred_low_temp_f': None,
        'pred_precip_prob': None,
        'summary_text': selected_text,
        'distribution': distribution,
        'raw_ref': config.pil,
    }


def fetch_archived_nws_text_products(*, pil: str, start_utc: datetime, end_utc: datetime, limit: int = 9999) -> list[str]:
    response = requests.get(
        IEM_AFOS_URL,
        params={
            'pil': pil,
            'fmt': 'text',
            'sdate': start_utc.strftime('%Y-%m-%dT%H:%MZ'),
            'edate': end_utc.strftime('%Y-%m-%dT%H:%MZ'),
            'limit': str(limit),
        },
        timeout=60,
    )
    response.raise_for_status()
    return _split_products(response.text)


def backfill_archived_nws_zone_forecasts(
    *,
    db_path: str | Path | None = None,
    city_ids: list[str] | tuple[str, ...] | None = None,
    sigma_f: float = 3.0,
) -> int:
    selected_city_ids = tuple(city_id.lower() for city_id in (city_ids or FOCUS_CITY_IDS) if city_id)
    con = connect(db_path=db_path)
    try:
        total_inserted = 0
        for city_id in selected_city_ids:
            config = ARCHIVED_FORECAST_CONFIGS.get(city_id)
            if config is None:
                continue

            rows = con.execute(
                '''
                select market_date_local, threshold_low_f, threshold_high_f, operator
                from core.weather_contracts
                where city_id = ?
                  and parse_status = 'parsed'
                  and market_date_local is not null
                  and threshold_low_f is not null
                order by market_date_local
                ''',
                [city_id],
            ).fetchall()
            if not rows:
                continue

            thresholds_by_date: dict[date, set[float]] = {}
            for market_date_local, threshold_low_f, threshold_high_f, operator in rows:
                thresholds_by_date.setdefault(market_date_local, set()).add(float(threshold_low_f))
                if operator == 'between' and threshold_high_f is not None:
                    thresholds_by_date[market_date_local].add(float(threshold_high_f) + 1.0)

            min_date = min(thresholds_by_date)
            max_date = max(thresholds_by_date)
            start_utc = datetime.combine(min_date - timedelta(days=2), time(0, 0), tzinfo=UTC)
            end_utc = datetime.combine(max_date + timedelta(days=1), time(23, 59), tzinfo=UTC)
            products = fetch_archived_nws_text_products(
                pil=config.pil,
                start_utc=start_utc,
                end_utc=end_utc,
            )

            for product in products:
                for target_date_local, thresholds in thresholds_by_date.items():
                    try:
                        parsed = parse_archived_nws_zone_forecast(
                            text=product,
                            city_id=city_id,
                            target_date_local=target_date_local,
                            thresholds=sorted(thresholds),
                            sigma_f=sigma_f,
                        )
                    except ValueError:
                        continue

                    ingest_forecast_snapshot(db_path=db_path, **parsed)
                    total_inserted += 1

        return total_inserted
    finally:
        con.close()
