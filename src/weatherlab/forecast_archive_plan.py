from __future__ import annotations

from dataclasses import dataclass

from .settings import FOCUS_CITY_IDS


@dataclass(frozen=True)
class ArchiveSourceCandidate:
    source_id: str
    source_class: str
    role: str
    notes: str


ARCHIVE_SOURCE_CANDIDATES: tuple[ArchiveSourceCandidate, ...] = (
    ArchiveSourceCandidate(
        source_id='ndfd-archive',
        source_class='gridded_forecast_archive',
        role='primary',
        notes=(
            'Primary target for honest historical forecast reconstruction. '
            'Supports issued-time semantics better than reanalysis proxies.'
        ),
    ),
    ArchiveSourceCandidate(
        source_id='iem-nws-text',
        source_class='text_forecast_archive',
        role='fallback',
        notes=(
            'Useful for archived NWS text products and cross-checking issuance timing '
            'when structured grid data is incomplete or operationally awkward.'
        ),
    ),
    ArchiveSourceCandidate(
        source_id='open-meteo-archive',
        source_class='reanalysis_proxy',
        role='proxy_only',
        notes=(
            'Coverage/diagnostic layer only. Not a true available-at-the-time forecast source.'
        ),
    ),
)


def get_focus_city_ids() -> tuple[str, ...]:
    return FOCUS_CITY_IDS or ('nyc', 'chi')


def get_focus_city_archive_plan() -> dict[str, dict[str, object]]:
    plan: dict[str, dict[str, object]] = {}
    for city_id in get_focus_city_ids():
        plan[city_id] = {
            'city_id': city_id,
            'primary_source': 'ndfd-archive',
            'fallback_source': 'iem-nws-text',
            'proxy_source': 'open-meteo-archive',
            'status': 'planned',
        }
    return plan
